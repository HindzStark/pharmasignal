"""Tests run on small synthetic openFDA-shaped records with a local SparkSession: no network, no AWS."""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from pipeline import config, upload

TRACKED_CSV = str(config.SEED_PATH)


def make_report(rid, drug, reactions, date="20250315", serious="1", death=None, sex="2", concomitant=None,
                version="1"):
    drugs = [{"medicinalproduct": drug, "drugcharacterization": "1", "openfda": {"generic_name": [drug]}}]
    if concomitant:
        drugs.append({"medicinalproduct": concomitant, "drugcharacterization": "2",
                      "openfda": {"generic_name": [concomitant]}})
    return {
        "safetyreportid": rid, "safetyreportversion": version, "receivedate": date, "serious": serious,
        "seriousnessdeath": death, "occurcountry": "US", "primarysource": {"qualification": "1"},
        "patient": {"patientsex": sex, "patientonsetage": "64", "patientonsetageunit": "801",
                    "drug": drugs, "reaction": [{"reactionmeddrapt": r, "reactionoutcome": "1"} for r in reactions]},
    }


def fixture_records():
    records = []
    # Keytruda: 6 reports, 5 with colitis -> should be flagged vs peers
    records += [make_report(f"K{i}", "PEMBROLIZUMAB", ["COLITIS", "FATIGUE"]) for i in range(5)]
    records += [make_report("K5", "PEMBROLIZUMAB", ["FATIGUE"], death="1")]
    # Peers: 20 reports, colitis only once
    records += [make_report(f"N{i}", "NIVOLUMAB", ["FATIGUE"], serious="2") for i in range(10)]
    records += [make_report(f"A{i}", "ATEZOLIZUMAB", ["NAUSEA"]) for i in range(9)]
    records += [make_report("A9", "ATEZOLIZUMAB", ["COLITIS"])]
    # Diabetes, plus a concomitant drug that must NOT count as a suspect
    records += [make_report(f"S{i}", "SITAGLIPTIN", ["PANCREATITIS"], concomitant="PEMBROLIZUMAB") for i in range(4)]
    records += [make_report(f"L{i}", "LINAGLIPTIN", ["NAUSEA"]) for i in range(6)]
    records += [make_report("BAD", "SAXAGLIPTIN", ["NAUSEA"], date="not-a-date")]  # dropped in staging
    return records


def write_bronze(root: Path, records, name="page_0000.json.gz", extracted_at="2025-01-01T00:00:00+00:00"):
    d = root / "drug=TEST" / "load_date=2025-01-01"
    d.mkdir(parents=True, exist_ok=True)
    body = json.dumps({"_extracted_at": extracted_at, "_drug": "TEST", "results": records}).encode()
    (d / name).write_bytes(gzip.compress(body) if name.endswith(".gz") else body)
    return root


@pytest.fixture
def bronze(tmp_path: Path) -> Path:
    return write_bronze(tmp_path / "raw", fixture_records())


def build(spark, bronze_path):
    from etl import transforms as T
    silver = T.dedupe(T.flatten(T.explode_reports(T.read_bronze_files(spark, str(bronze_path)))))
    tracked = T.read_tracked_drugs(spark, TRACKED_CSV)
    return silver, tracked, T.build_curated(silver, tracked)


# ---- transform ------------------------------------------------------------------------------
def test_flatten_extracts_all_three_tables(spark, tmp_path):
    from etl import transforms as T
    path = write_bronze(tmp_path / "raw", [make_report("X1", "PEMBROLIZUMAB", ["COLITIS", "RASH"])])
    silver = T.flatten(T.explode_reports(T.read_bronze_files(spark, str(path))))
    rep = silver.reports.collect()
    assert [(r.safetyreportid, r.receivedate) for r in rep] == [("X1", "20250315")]
    assert silver.drugs.collect()[0].generic_name == "PEMBROLIZUMAB"
    assert [r.reaction_pt for r in silver.reactions.orderBy("reaction_seq").collect()] == ["COLITIS", "RASH"]


def test_dedupe_is_idempotent(spark, bronze):
    from etl import transforms as T
    silver, _, _ = build(spark, bronze)
    assert silver.reports.count() == 37
    again = T.dedupe(T.union_silver(silver, silver))  # re-loading the same files must not duplicate
    assert [df.count() for df in again] == [df.count() for df in silver]


def test_dedupe_keeps_latest_report_version(spark, tmp_path):
    from etl import transforms as T
    root = tmp_path / "raw"
    write_bronze(root, [make_report("R1", "PEMBROLIZUMAB", ["RASH"], version="1")], name="a.json",
                 extracted_at="2025-01-01T00:00:00+00:00")
    write_bronze(root, [make_report("R1", "PEMBROLIZUMAB", ["COLITIS", "FATIGUE"], version="2")], name="b.json",
                 extracted_at="2025-01-02T00:00:00+00:00")
    silver = T.dedupe(T.flatten(T.explode_reports(T.read_bronze_files(spark, str(root)))))
    assert silver.reports.count() == 1
    assert sorted(r.reaction_pt for r in silver.reactions.collect()) == ["COLITIS", "FATIGUE"]


def test_conform_normalises_an_inferred_schema(spark, tmp_path):
    """Glue DynamicFrames infer the schema; conform_bronze must map that onto the explicit one."""
    from etl import transforms as T
    rec = make_report("C1", "NIVOLUMAB", ["RASH"])
    rec["safetyreportversion"] = 3            # number instead of string
    rec["unexpected_field"] = {"nested": 1}   # extra field
    del rec["occurcountry"]                   # missing field
    path = write_bronze(tmp_path / "raw", [rec], name="p.json")
    inferred = spark.read.option("recursiveFileLookup", "true").json(str(path))
    row = T.explode_reports(T.conform_bronze(inferred)).collect()[0]
    assert (row.safetyreportid, row.safetyreportversion, row.occurcountry) == ("C1", "3", None)


def test_transform_and_signal_logic(spark, bronze):
    _, _, cur = build(spark, bronze)
    assert cur.stg_reports.count() == 36  # unparseable receivedate dropped

    sig = {(r.generic_name, r.reaction_pt): r for r in cur.mart_safety_signals.collect()}
    colitis = sig[("PEMBROLIZUMAB", "COLITIS")]
    assert (colitis.is_signal, colitis.reports_with_reaction) == (True, 5)
    assert sig[("PEMBROLIZUMAB", "FATIGUE")].is_signal is False  # common across peers -> not disproportionate

    summary = {r.generic_name: r for r in cur.mart_drug_safety_summary.collect()}
    # concomitant Keytruda on Januvia reports must not inflate Keytruda's count
    assert summary["PEMBROLIZUMAB"].total_reports == 6
    assert summary["PEMBROLIZUMAB"].death_rate == pytest.approx(round(1 / 6, 4))


# ---- data quality -------------------------------------------------------------------------
def test_all_28_quality_checks_pass_on_clean_data(spark, bronze):
    from etl import quality
    silver, tracked, cur = build(spark, bronze)
    results = quality.run_checks(quality.build_checks(silver, cur, tracked))
    assert len(results) == 28
    assert [r.name for r in results if r.severity == "error" and not r.passed] == []
    # the BAD report's drug row points at a report dropped in staging: warning only (as in dbt)
    assert [r.name for r in results if not r.passed] == ["stg_report_drugs.report_id -> stg_reports"]
    quality.enforce(results)  # does not raise


def test_quality_gate_fails_the_job_on_bad_data(spark, bronze):
    from pyspark.sql import functions as F
    from etl import quality
    silver, tracked, cur = build(spark, bronze)
    broken = cur._replace(
        mart_drug_safety_summary=cur.mart_drug_safety_summary.withColumn("serious_rate", F.lit(1.5)),
        fct_drug_reactions=cur.fct_drug_reactions.unionByName(cur.fct_drug_reactions.limit(1)),
    )
    results = quality.run_checks(quality.build_checks(silver, broken, tracked))
    with pytest.raises(quality.DataQualityError, match="assert_rates_are_valid.*drug_reaction_key unique|"
                                                       "drug_reaction_key unique.*assert_rates_are_valid"):
        quality.enforce(results)


# ---- load (catalog) -----------------------------------------------------------------------
def test_catalog_table_input_for_partitioned_parquet(spark, bronze):
    from etl import catalog
    _, _, cur = build(spark, bronze)
    t = catalog.table_input("fct_drug_reactions", cur.fct_drug_reactions.schema,
                            "s3://b/curated/fct_drug_reactions/run=1", ["therapy_area"])
    cols = {c["Name"]: c["Type"] for c in t["StorageDescriptor"]["Columns"]}
    assert "therapy_area" not in cols and t["PartitionKeys"] == [{"Name": "therapy_area", "Type": "string"}]
    assert (cols["received_date"], cols["is_serious"], cols["patient_age_years"]) == ("date", "boolean", "double")
    assert t["StorageDescriptor"]["Location"] == "s3://b/curated/fct_drug_reactions/run=1/"
    parts = catalog.partition_inputs(t, ["Diabetes", "Oncology"])
    assert parts[1]["StorageDescriptor"]["Location"].endswith("/run=1/therapy_area=Oncology/")


# ---- upload --------------------------------------------------------------------------------
class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, **_):
        self.objects[Key] = Body

    def get_paginator(self, _name):
        store = self.objects

        class P:
            def paginate(self, Bucket, Prefix):
                yield {"Contents": [{"Key": k} for k in store if k.startswith(Prefix)]}
        return P()


def test_upload_gzips_and_is_idempotent(tmp_path):
    raw = tmp_path / "raw" / "drug=X" / "load_date=2026-01-01"
    raw.mkdir(parents=True)
    (raw / "page_0000.json").write_text('{"results": []}')
    s3 = FakeS3()
    assert upload.upload_bronze(s3, "b", tmp_path / "raw") == (1, 0)
    key = "bronze/drug=X/load_date=2026-01-01/page_0000.json.gz"
    assert gzip.decompress(s3.objects[key]) == b'{"results": []}'
    assert upload.upload_bronze(s3, "b", tmp_path / "raw") == (0, 1)  # second run uploads nothing


# ---- extract -------------------------------------------------------------------------------
class FakeResponse:
    def __init__(self, status, body=None):
        self.status_code, self._body, self.headers = status, body or {}, {"Retry-After": "0"}

    def json(self):
        return self._body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(self.status_code)


class FakeSession:
    def __init__(self, responses):
        self.responses, self.calls = list(responses), 0

    def get(self, *args, **kwargs):
        self.calls += 1
        return self.responses.pop(0)


def test_extract_retries_on_rate_limit_then_succeeds():
    from pipeline import extract
    session = FakeSession([FakeResponse(429), FakeResponse(503), FakeResponse(200, {"results": [{"x": 1}]})])
    assert extract.fetch_page(session, {}) == {"results": [{"x": 1}]}
    assert session.calls == 3


def test_extract_treats_404_as_no_data():
    from pipeline import extract
    assert extract.fetch_page(FakeSession([FakeResponse(404)]), {}) is None


def test_extract_fails_fast_on_403_with_api_reason():
    from pipeline import extract
    body = {"error": {"code": "API_KEY_MISSING", "message": "No api_key was supplied."}}
    session = FakeSession([FakeResponse(403, body)])
    with pytest.raises(RuntimeError, match="API_KEY_MISSING"):
        extract.fetch_page(session, {})
    assert session.calls == 1  # not retried: a 403 is not transient
