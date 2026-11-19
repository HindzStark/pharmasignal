"""Transform: bronze openFDA JSON -> silver (flattened, deduplicated) -> curated star schema + marts.

Ported 1:1 from the previous dbt models (stg_reports, stg_report_drugs, stg_reactions, dim_drug,
fct_drug_reactions, mart_drug_safety_summary, mart_safety_signals).

Written for Spark 4 with ANSI mode on (Glue 6.0 default): every cast/parse that can see dirty data
uses a try_* function so a bad value becomes NULL instead of failing the job.
"""
from __future__ import annotations

from typing import NamedTuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# --------------------------------------------------------------------------------------------
# Bronze schema: only the fields we use. openFDA records are large (~30 KB each, mostly the
# `openfda` enrichment block); an explicit schema skips everything else and avoids inference.
# --------------------------------------------------------------------------------------------
_S = T.StringType()


def _strings(*names: str) -> list[T.StructField]:
    return [T.StructField(n, _S) for n in names]


OPENFDA = T.StructType([T.StructField(n, T.ArrayType(_S)) for n in ("generic_name", "brand_name", "manufacturer_name")])
DRUG = T.StructType(_strings("medicinalproduct", "drugcharacterization") + [T.StructField("openfda", OPENFDA)])
REACTION = T.StructType(_strings("reactionmeddrapt", "reactionoutcome"))
PATIENT = T.StructType(
    _strings("patientsex", "patientonsetage", "patientonsetageunit")
    + [T.StructField("drug", T.ArrayType(DRUG)), T.StructField("reaction", T.ArrayType(REACTION))]
)
REPORT = T.StructType(
    _strings("safetyreportid", "safetyreportversion", "receivedate", "serious", "seriousnessdeath",
             "seriousnesshospitalization", "occurcountry")
    + [T.StructField("primarysource", T.StructType(_strings("qualification"))), T.StructField("patient", PATIENT)]
)
# One bronze file = {"_extracted_at": ..., "_drug": ..., "results": [report, ...]}
BRONZE_FILE = T.StructType(_strings("_extracted_at", "_drug") + [T.StructField("results", T.ArrayType(REPORT))])

THERAPY_AREAS = ["Oncology", "Diabetes"]
SEX_VALUES = ["Male", "Female", "Unknown"]
REPORTER_VALUES = ["Physician", "Pharmacist", "Other health professional", "Lawyer", "Consumer", "Unknown"]
HCP_REPORTERS = ["Physician", "Pharmacist", "Other health professional"]


class Silver(NamedTuple):
    """Flattened, deduplicated raw tables (persisted between runs so loads can be incremental)."""
    reports: DataFrame
    drugs: DataFrame
    reactions: DataFrame


class Curated(NamedTuple):
    stg_reports: DataFrame
    stg_report_drugs: DataFrame
    stg_reactions: DataFrame
    dim_drug: DataFrame
    fct_drug_reactions: DataFrame
    mart_drug_safety_summary: DataFrame
    mart_safety_signals: DataFrame


# --------------------------------------------------------------------------------------------
# Read / conform
# --------------------------------------------------------------------------------------------
def read_bronze_files(spark: SparkSession, path: str) -> DataFrame:
    """Read bronze files (.json or .json.gz, any depth) with the explicit schema. Used locally and in tests."""
    return (spark.read.schema(BRONZE_FILE).option("recursiveFileLookup", "true").json(path))


def conform_bronze(df: DataFrame) -> DataFrame:
    """Coerce a DataFrame with an *inferred* bronze schema (e.g. from a Glue DynamicFrame) to BRONZE_FILE.

    Missing fields become NULL, extra fields are dropped, scalar types are normalised to strings.
    """
    if not df.columns:
        return df.sparkSession.createDataFrame([], BRONZE_FILE)
    as_json = F.to_json(F.struct(*[F.col(f"`{c}`") for c in df.columns]))
    return df.select(F.from_json(as_json, BRONZE_FILE).alias("f")).select("f.*")


def explode_reports(bronze_files: DataFrame) -> DataFrame:
    """One row per report, carrying the extraction timestamp of the file it came from."""
    return (bronze_files
            .select(F.col("_extracted_at").alias("extracted_at"), F.explode("results").alias("r"))
            .select("r.*", "extracted_at"))


# --------------------------------------------------------------------------------------------
# Flatten + deduplicate (silver)
# --------------------------------------------------------------------------------------------
def flatten(reports: DataFrame) -> Silver:
    """Split nested reports into report / drug / reaction tables (same columns as the old raw.* tables)."""
    base = reports.where(F.col("safetyreportid").isNotNull())
    rep = base.select(
        "safetyreportid", "safetyreportversion", "receivedate", "serious", "seriousnessdeath",
        "seriousnesshospitalization", "occurcountry",
        F.col("primarysource.qualification").alias("reporter_qualification"),
        F.col("patient.patientsex").alias("patient_sex"),
        F.col("patient.patientonsetage").alias("patient_age"),
        F.col("patient.patientonsetageunit").alias("patient_age_unit"),
        "extracted_at",
    )
    keys = ["safetyreportid", "safetyreportversion", "extracted_at"]
    drugs = (base.select(*keys, F.posexplode("patient.drug").alias("drug_seq", "d"))
             .select(*keys, "drug_seq",
                     F.col("d.medicinalproduct").alias("medicinalproduct"),
                     F.col("d.drugcharacterization").alias("drugcharacterization"),
                     F.try_element_at("d.openfda.generic_name", F.lit(1)).alias("generic_name"),
                     F.try_element_at("d.openfda.brand_name", F.lit(1)).alias("brand_name"),
                     F.try_element_at("d.openfda.manufacturer_name", F.lit(1)).alias("manufacturer_name")))
    reactions = (base.select(*keys, F.posexplode("patient.reaction").alias("reaction_seq", "x"))
                 .select(*keys, "reaction_seq",
                         F.col("x.reactionmeddrapt").alias("reaction_pt"),
                         F.col("x.reactionoutcome").alias("reaction_outcome")))
    return Silver(rep, drugs, reactions)


def union_silver(*parts: Silver) -> Silver:
    parts = [p for p in parts if p is not None]
    return Silver(*[_union_all([getattr(p, f) for p in parts]) for f in Silver._fields])


def _union_all(dfs: list[DataFrame]) -> DataFrame:
    out = dfs[0]
    for df in dfs[1:]:
        out = out.unionByName(df)
    return out


def dedupe(silver: Silver) -> Silver:
    """Keep exactly one version of each report: highest safetyreportversion, then latest extraction.

    Child rows (drugs, reactions) are taken from the winning version only, so a report that was
    re-fetched or amended is replaced as a whole. Applying this twice is a no-op (idempotent).
    """
    w = Window.partitionBy("safetyreportid").orderBy(
        F.col("safetyreportversion").try_cast("int").desc_nulls_last(),
        F.col("extracted_at").desc_nulls_last(),
    )
    reports = silver.reports.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

    # Null-safe match on the winning version: version / extracted_at can be NULL in source data.
    def _key(df: DataFrame) -> DataFrame:
        return (df.withColumn("_v", F.coalesce("safetyreportversion", F.lit("")))
                  .withColumn("_e", F.coalesce("extracted_at", F.lit(""))))

    winners = _key(reports).select("safetyreportid", "_v", "_e")

    def children(df: DataFrame, seq: str) -> DataFrame:
        return (_key(df).join(winners, ["safetyreportid", "_v", "_e"], "left_semi")
                .select(*df.columns).dropDuplicates(["safetyreportid", seq]))

    return Silver(reports, children(silver.drugs, "drug_seq"), children(silver.reactions, "reaction_seq"))


# --------------------------------------------------------------------------------------------
# Staging (port of dbt staging models)
# --------------------------------------------------------------------------------------------
_REPORTER = {"1": "Physician", "2": "Pharmacist", "3": "Other health professional", "4": "Lawyer", "5": "Consumer"}
_SEX = {"1": "Male", "2": "Female"}
_AGE_TO_YEARS = {"800": 10.0, "801": 1.0, "802": 1 / 12, "803": 1 / 52, "804": 1 / 365, "805": 1 / 8760}


def _decode(col: str, mapping: dict, default: str) -> F.Column:
    expr = F.lit(default)
    for code, label in reversed(list(mapping.items())):
        expr = F.when(F.col(col) == code, F.lit(label)).otherwise(expr)
    return expr


def stg_reports(reports: DataFrame) -> DataFrame:
    received = F.to_date(F.try_to_timestamp(F.col("receivedate"), F.lit("yyyyMMdd")))
    age = F.col("patient_age").try_cast("double")
    age_years = F.lit(None).cast("double")
    for unit, factor in reversed(list(_AGE_TO_YEARS.items())):
        age_years = F.when(F.col("patient_age_unit") == unit, age * F.lit(factor)).otherwise(age_years)
    return (reports
            .withColumn("received_date", received)
            .where(F.col("received_date").isNotNull())
            .select(
                F.col("safetyreportid").alias("report_id"),
                "received_date",
                (F.col("serious") == "1").alias("is_serious"),
                F.coalesce(F.col("seriousnessdeath") == "1", F.lit(False)).alias("is_death"),
                F.coalesce(F.col("seriousnesshospitalization") == "1", F.lit(False)).alias("is_hospitalization"),
                F.upper(F.nullif(F.trim("occurcountry"), F.lit(""))).alias("country_code"),
                _decode("reporter_qualification", _REPORTER, "Unknown").alias("reporter_type"),
                _decode("patient_sex", _SEX, "Unknown").alias("patient_sex"),
                F.round(age_years, 1).alias("patient_age_years"),
            ))


def stg_report_drugs(drugs: DataFrame, tracked: DataFrame) -> DataFrame:
    """Suspect (1) / interacting (3) drugs only, restricted to tracked drugs. Concomitant (2) is noise."""
    return (drugs
            .where(F.col("drugcharacterization").isin("1", "3"))
            .select(F.col("safetyreportid").alias("report_id"), F.upper(F.trim("generic_name")).alias("generic_name"))
            .join(tracked.select("generic_name"), "generic_name")
            .select("report_id", "generic_name")
            .distinct())


def stg_reactions(reactions: DataFrame) -> DataFrame:
    return (reactions
            .where(F.nullif(F.trim("reaction_pt"), F.lit("")).isNotNull())
            .groupBy(F.col("safetyreportid").alias("report_id"), F.upper(F.trim("reaction_pt")).alias("reaction_pt"))
            .agg(F.max(F.col("reaction_outcome") == "5").alias("is_fatal_outcome")))


# --------------------------------------------------------------------------------------------
# Marts (port of dbt mart models)
# --------------------------------------------------------------------------------------------
def read_tracked_drugs(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.option("header", "true").csv(path)
    return df.select("generic_name", "brand_name", "company", "therapy_area",
                     F.lower(F.trim("is_msd")).isin("true", "1", "yes").alias("is_msd"))


def dim_drug(tracked: DataFrame) -> DataFrame:
    return tracked.select("generic_name", "brand_name", "company", "therapy_area", "is_msd")


def fct_drug_reactions(reports: DataFrame, report_drugs: DataFrame, dim: DataFrame, reactions: DataFrame) -> DataFrame:
    """Grain: one row per (report, tracked suspect drug, reaction)."""
    r, d, dd, x = reports.alias("r"), report_drugs.alias("d"), dim.alias("dd"), reactions.alias("x")
    return (r.join(d, F.col("r.report_id") == F.col("d.report_id"))
            .join(dd, F.col("d.generic_name") == F.col("dd.generic_name"))
            .join(x, F.col("r.report_id") == F.col("x.report_id"))
            .select(
                F.md5(F.concat_ws("|", "r.report_id", "d.generic_name", "x.reaction_pt")).alias("drug_reaction_key"),
                F.col("r.report_id").alias("report_id"),
                F.col("d.generic_name").alias("generic_name"),
                F.col("dd.therapy_area").alias("therapy_area"),
                F.col("x.reaction_pt").alias("reaction_pt"),
                F.col("r.received_date").alias("received_date"),
                F.trunc("r.received_date", "quarter").alias("received_quarter"),
                "r.is_serious", "r.is_death", "r.is_hospitalization",
                F.col("x.is_fatal_outcome").alias("is_fatal_outcome"),
                "r.patient_sex", "r.patient_age_years", "r.reporter_type", "r.country_code",
            ))


def _rate(cond: F.Column) -> F.Column:
    return F.round(F.avg(F.when(cond, 1).otherwise(0)), 4)


def mart_drug_safety_summary(fct: DataFrame, dim: DataFrame) -> DataFrame:
    # report-level (not reaction-level) so a report with 5 reactions counts once
    reports = fct.select("report_id", "generic_name", "is_serious", "is_death", "is_hospitalization",
                         "reporter_type").distinct()
    return (reports.join(dim, "generic_name")
            .groupBy("generic_name", "brand_name", "company", "therapy_area", "is_msd")
            .agg(F.count(F.lit(1)).alias("total_reports"),
                 _rate(F.col("is_serious")).alias("serious_rate"),
                 _rate(F.col("is_death")).alias("death_rate"),
                 _rate(F.col("is_hospitalization")).alias("hospitalization_rate"),
                 _rate(F.col("reporter_type").isin(*HCP_REPORTERS)).alias("hcp_reported_rate")))


def mart_safety_signals(fct: DataFrame, dim: DataFrame) -> DataFrame:
    """PRR + Yates chi-square of drug D vs the other tracked drugs in its therapy area.

                     reaction R    other reactions
      drug D             a               b
      comparators        c               d

    PRR = [a/(a+b)] / [c/(c+d)]; signal (Evans): a >= 3, PRR >= 2, chi2 >= 4.
    """
    base = fct.select("report_id", "generic_name", "therapy_area", "reaction_pt").distinct()
    n_drug = base.groupBy("generic_name", "therapy_area").agg(F.countDistinct("report_id").alias("n_drug"))
    n_area = base.groupBy("therapy_area").agg(F.countDistinct("report_id").alias("n_area"))
    pair = base.groupBy("generic_name", "therapy_area", "reaction_pt").agg(F.countDistinct("report_id").alias("a"))
    area_rx = base.groupBy("therapy_area", "reaction_pt").agg(F.countDistinct("report_id").alias("n_area_reaction"))

    ct = (pair.join(n_drug, ["generic_name", "therapy_area"])
          .join(area_rx, ["therapy_area", "reaction_pt"])
          .join(n_area, ["therapy_area"])
          .select("generic_name", "therapy_area", "reaction_pt",
                  F.col("a").cast("double").alias("a"),
                  (F.col("n_drug") - F.col("a")).cast("double").alias("b"),
                  (F.col("n_area_reaction") - F.col("a")).cast("double").alias("c"),
                  ((F.col("n_area") - F.col("n_drug")) - (F.col("n_area_reaction") - F.col("a"))).cast("double").alias("d")))

    a, b, c, d = (F.col(x) for x in "abcd")
    n = a + b + c + d
    prr = F.when((c > 0) & ((a + b) > 0) & ((c + d) > 0), (a / (a + b)) / (c / (c + d)))
    chi = F.when(((a + b) > 0) & ((c + d) > 0) & ((a + c) > 0) & ((b + d) > 0),
                 n * F.pow(F.greatest(F.abs(a * d - b * c) - n / 2, F.lit(0.0)), 2)
                 / ((a + b) * (c + d) * (a + c) * (b + d)))
    scored = ct.withColumn("prr", prr).withColumn("chi_square", chi)

    return (scored.join(dim.select("generic_name", "brand_name", "company", "is_msd"), "generic_name")
            .select(
                F.md5(F.concat_ws("|", "generic_name", "reaction_pt")).alias("signal_key"),
                "generic_name", "brand_name", "company", "therapy_area", "is_msd", "reaction_pt",
                F.col("a").cast("int").alias("reports_with_reaction"),
                (F.col("a") + F.col("b")).cast("int").alias("drug_total_reports"),
                F.round("prr", 3).alias("prr"),
                F.round("chi_square", 3).alias("chi_square"),
                F.coalesce((F.col("a") >= 3) & (F.col("prr") >= 2) & (F.col("chi_square") >= 4), F.lit(False))
                 .alias("is_signal"),
            ))


def build_curated(silver: Silver, tracked: DataFrame) -> Curated:
    reports = stg_reports(silver.reports)
    drugs = stg_report_drugs(silver.drugs, tracked)
    reactions = stg_reactions(silver.reactions)
    dim = dim_drug(tracked)
    fct = fct_drug_reactions(reports, drugs, dim, reactions)
    return Curated(reports, drugs, reactions, dim, fct,
                   mart_drug_safety_summary(fct, dim), mart_safety_signals(fct, dim))


def empty_silver(spark: SparkSession) -> Silver:
    """Silver tables with no rows (first run, before anything has been persisted)."""
    return flatten(explode_reports(spark.createDataFrame([], BRONZE_FILE)))
