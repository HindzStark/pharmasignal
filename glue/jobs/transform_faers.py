"""AWS Glue 6.0 job: TRANSFORM + DATA QUALITY + LOAD for PharmaSignal.

  bronze JSON (S3, bookmarked: only new files)  ->  merge with previous silver, dedupe on safetyreportid
  -> staging + star schema + marts (etl.transforms)  ->  28 DQ checks + Glue Data Quality (DQDL)
  -> Parquet (snappy) under a fresh run=<id>/ folder  ->  switch Glue Catalog tables to the new folder
  -> commit bookmark  ->  delete superseded run folders.

Nothing is published unless every check passes: the catalog keeps pointing at the previous run, the
bookmark is not committed, so the next run re-reads the same bronze files.

Arguments: --BUCKET <bucket> --DATABASE <glue database>; etl/ ships as --extra-py-files etl.zip.
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone

import boto3
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from etl import catalog, quality, transforms as T

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s", stream=sys.stdout)
log = logging.getLogger("pharmasignal")

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET", "DATABASE"])
BUCKET, DATABASE = args["BUCKET"], args["DATABASE"]
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
POINTER_KEY = "silver/_current_run.json"
S3 = f"s3://{BUCKET}"

sc = SparkContext.getOrCreate()
glue_ctx = GlueContext(sc)
spark = glue_ctx.spark_session
job = Job(glue_ctx)
job.init(args["JOB_NAME"], args)
s3 = boto3.client("s3")
glue = boto3.client("glue")

# Curated tables published to the catalog: name -> partition column (None = unpartitioned)
CURATED = {
    "dim_drug": None,
    "fct_drug_reactions": "therapy_area",
    "mart_drug_safety_summary": None,
    "mart_safety_signals": "therapy_area",
}
DESCRIPTIONS = {
    "dim_drug": "Tracked drugs (MSD brands and competitors).",
    "fct_drug_reactions": "One row per report x tracked suspect drug x reaction.",
    "mart_drug_safety_summary": "Brand-level safety KPIs for competitor benchmarking.",
    "mart_safety_signals": "PRR + chi-square disproportionality signals vs therapy-area peers.",
}


def previous_run() -> str | None:
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=POINTER_KEY)["Body"].read())["run_id"]
    except s3.exceptions.NoSuchKey:
        return None


def glue_data_quality(cur: T.Curated) -> None:
    """Evaluate the DQDL rulesets with AWS Glue Data Quality; any non-passing rule fails the job."""
    try:
        from awsgluedq.transforms import EvaluateDataQuality
    except ImportError:  # assertions in etl.quality already gate the load
        log.warning("awsgluedq not available in this runtime; relying on in-job assertions only")
        return
    for name, ruleset in quality.DQDL_RULESETS.items():
        frame = DynamicFrame.fromDF(getattr(cur, name), glue_ctx, f"dq_{name}")
        outcome = EvaluateDataQuality.apply(
            frame=frame, ruleset=ruleset,
            publishing_options={"dataQualityEvaluationContext": f"dq_{name}",
                                "enableDataQualityCloudWatchMetrics": False,
                                "enableDataQualityResultsPublishing": False},
        ).toDF()
        bad = outcome.where("Outcome != 'Passed'").select("Rule", "Outcome", "FailureReason").collect()
        for row in outcome.collect():
            log.info("GlueDQ %s | %s | %s", name, row["Outcome"], row["Rule"])
        if bad:
            raise quality.DataQualityError(f"Glue Data Quality failed for {name}: {[r.asDict() for r in bad]}")


def delete_superseded(prefix: str, keep: str) -> None:
    """Remove run=<id>/ folders under prefix other than `keep`."""
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            if cp["Prefix"].rstrip("/").endswith(f"run={keep}"):
                continue
            objs = [o["Key"] for p in paginator.paginate(Bucket=BUCKET, Prefix=cp["Prefix"]) for o in p.get("Contents", [])]
            for i in range(0, len(objs), 1000):
                s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in objs[i:i + 1000]]})


# ---- EXTRACT (from bronze): only files not yet processed, thanks to the job bookmark ----------
bronze = glue_ctx.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f"{S3}/bronze/"], "recurse": True},
    format="json",
    transformation_ctx="bronze_json",
)
new = T.flatten(T.explode_reports(T.conform_bronze(bronze.toDF())))

prev = previous_run()
old = None
if prev:
    old = T.Silver(*[spark.read.parquet(f"{S3}/silver/{t}/run={prev}/") for t in T.Silver._fields])
silver = T.dedupe(T.union_silver(old, new) if old else new)
silver = T.Silver(*[df.cache() for df in silver])
log.info("Silver: %d reports (previous run: %s)", silver.reports.count(), prev)

# ---- TRANSFORM ------------------------------------------------------------------------------
tracked = T.read_tracked_drugs(spark, f"{S3}/reference/tracked_drugs.csv").cache()
cur = T.Curated(*[df.cache() for df in T.build_curated(silver, tracked)])

# ---- DATA QUALITY: fail before writing anything ---------------------------------------------
quality.enforce(quality.run_checks(quality.build_checks(silver, cur, tracked)))
glue_data_quality(cur)

# ---- LOAD -----------------------------------------------------------------------------------
for name, df in silver._asdict().items():
    df.coalesce(1).write.mode("errorifexists").option("compression", "snappy").parquet(
        f"{S3}/silver/{name}/run={RUN_ID}/")

counts = {}
for name, part in CURATED.items():
    df = getattr(cur, name)
    location = f"{S3}/curated/{name}/run={RUN_ID}/"
    writer = (df.repartition(part) if part else df.coalesce(1)).write.mode("errorifexists").option("compression", "snappy")
    (writer.partitionBy(part) if part else writer).parquet(location)
    values = sorted(r[0] for r in df.select(part).distinct().collect()) if part else None
    table = catalog.table_input(name, df.schema, location, [part] if part else [], DESCRIPTIONS[name])
    catalog.publish_table(glue, DATABASE, table, values)
    counts[name] = df.count()

s3.put_object(Bucket=BUCKET, Key=POINTER_KEY, Body=json.dumps({"run_id": RUN_ID, "counts": counts}).encode())
job.commit()  # bookmark advances only after a fully successful publish

for name in list(T.Silver._fields) + list(CURATED):
    layer = "silver" if name in T.Silver._fields else "curated"
    delete_superseded(f"{layer}/{name}/", RUN_ID)

log.info("PHARMASIGNAL_METRICS %s", json.dumps({"run_id": RUN_ID, "rows": counts}))
