"""Data-quality gate: a port of all 28 tests from the previous dbt project.

The Glue job evaluates every check *before* writing anything. Any failing check with severity
"error" raises DataQualityError, so the job fails and leaves the published tables untouched.
Severity "warn" is logged only (same as the dbt config it replaces).

Simple column rules are also expressed in DQDL (DQDL_RULESETS) for AWS Glue Data Quality; the job
runs those on Glue in addition to these assertions. The assertions here are the source of truth
and are what the local test suite exercises.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from etl.transforms import REPORTER_VALUES, SEX_VALUES, THERAPY_AREAS, Curated, Silver

log = logging.getLogger(__name__)


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class Check:
    name: str
    failing_rows: Callable[[], int]   # number of rows violating the rule (0 = pass)
    severity: str = "error"


@dataclass(frozen=True)
class Result:
    name: str
    severity: str
    failing_rows: int

    @property
    def passed(self) -> bool:
        return self.failing_rows == 0


# ---- generic tests ------------------------------------------------------------------------
def not_null(df: DataFrame, col: str) -> Callable[[], int]:
    return lambda: df.where(F.col(col).isNull()).count()


def unique(df: DataFrame, *cols: str) -> Callable[[], int]:
    return lambda: df.groupBy(*cols).count().where("count > 1").count()


def accepted_values(df: DataFrame, col: str, values: list[str]) -> Callable[[], int]:
    # like dbt: NULLs are not checked by accepted_values (not_null covers them)
    return lambda: df.where(F.col(col).isNotNull() & ~F.col(col).isin(*values)).count()


def relationship(df: DataFrame, col: str, parent: DataFrame, parent_col: str) -> Callable[[], int]:
    def run() -> int:
        keys = parent.select(F.col(parent_col).alias(col)).distinct()
        return df.where(F.col(col).isNotNull()).join(keys, col, "left_anti").count()
    return run


def expression(df: DataFrame, failing_condition: str) -> Callable[[], int]:
    return lambda: df.where(failing_condition).count()


# ---- the 28 checks --------------------------------------------------------------------------
def build_checks(silver: Silver, cur: Curated, tracked: DataFrame) -> list[Check]:
    r, d, x = cur.stg_reports, cur.stg_report_drugs, cur.stg_reactions
    return [
        # source (silver) reports
        Check("silver_reports.safetyreportid unique", unique(silver.reports, "safetyreportid")),
        Check("silver_reports.safetyreportid not_null", not_null(silver.reports, "safetyreportid")),
        # stg_reports
        Check("stg_reports.report_id unique", unique(r, "report_id")),
        Check("stg_reports.report_id not_null", not_null(r, "report_id")),
        Check("stg_reports.received_date not_null", not_null(r, "received_date")),
        Check("stg_reports.patient_sex accepted_values", accepted_values(r, "patient_sex", SEX_VALUES)),
        Check("stg_reports.reporter_type accepted_values", accepted_values(r, "reporter_type", REPORTER_VALUES)),
        # stg_report_drugs
        Check("stg_report_drugs unique(report_id, generic_name)", unique(d, "report_id", "generic_name")),
        Check("stg_report_drugs.report_id not_null", not_null(d, "report_id")),
        Check("stg_report_drugs.report_id -> stg_reports", relationship(d, "report_id", r, "report_id"),
              severity="warn"),  # a report can fail date parsing upstream; warn rather than block
        Check("stg_report_drugs.generic_name not_null", not_null(d, "generic_name")),
        Check("stg_report_drugs.generic_name -> tracked_drugs",
              relationship(d, "generic_name", tracked, "generic_name")),
        # stg_reactions
        Check("stg_reactions unique(report_id, reaction_pt)", unique(x, "report_id", "reaction_pt")),
        Check("stg_reactions.reaction_pt not_null", not_null(x, "reaction_pt")),
        # dim_drug
        Check("dim_drug.generic_name unique", unique(cur.dim_drug, "generic_name")),
        Check("dim_drug.generic_name not_null", not_null(cur.dim_drug, "generic_name")),
        Check("dim_drug.therapy_area accepted_values", accepted_values(cur.dim_drug, "therapy_area", THERAPY_AREAS)),
        # fct_drug_reactions
        Check("fct_drug_reactions.drug_reaction_key unique", unique(cur.fct_drug_reactions, "drug_reaction_key")),
        Check("fct_drug_reactions.drug_reaction_key not_null", not_null(cur.fct_drug_reactions, "drug_reaction_key")),
        Check("fct_drug_reactions.generic_name -> dim_drug",
              relationship(cur.fct_drug_reactions, "generic_name", cur.dim_drug, "generic_name")),
        # mart_drug_safety_summary
        Check("mart_drug_safety_summary.generic_name unique", unique(cur.mart_drug_safety_summary, "generic_name")),
        Check("mart_drug_safety_summary.generic_name not_null", not_null(cur.mart_drug_safety_summary, "generic_name")),
        Check("mart_drug_safety_summary.total_reports not_null",
              not_null(cur.mart_drug_safety_summary, "total_reports")),
        # mart_safety_signals
        Check("mart_safety_signals.signal_key unique", unique(cur.mart_safety_signals, "signal_key")),
        Check("mart_safety_signals.signal_key not_null", not_null(cur.mart_safety_signals, "signal_key")),
        # singular tests
        Check("assert_rates_are_valid", expression(
            cur.mart_drug_safety_summary,
            "NOT (serious_rate BETWEEN 0 AND 1) OR NOT (death_rate BETWEEN 0 AND 1) "
            "OR NOT (hospitalization_rate BETWEEN 0 AND 1) OR NOT (hcp_reported_rate BETWEEN 0 AND 1)")),
        Check("assert_no_future_reports", expression(r, "received_date > current_date()")),
        Check("assert_contingency_is_consistent", expression(
            cur.mart_safety_signals,
            "reports_with_reaction > drug_total_reports OR prr < 0 OR chi_square < 0")),
    ]


def run_checks(checks: list[Check]) -> list[Result]:
    results = []
    for c in checks:
        res = Result(c.name, c.severity, c.failing_rows())
        level = logging.INFO if res.passed else (logging.WARNING if c.severity == "warn" else logging.ERROR)
        log.log(level, "DQ %-4s %-55s failing_rows=%d", "PASS" if res.passed else c.severity.upper(),
                c.name, res.failing_rows)
        results.append(res)
    return results


def enforce(results: list[Result]) -> None:
    errors = [r for r in results if r.severity == "error" and not r.passed]
    if errors:
        detail = "; ".join(f"{r.name} ({r.failing_rows} rows)" for r in errors)
        raise DataQualityError(f"{len(errors)} data-quality check(s) failed, nothing was written: {detail}")


# ---- DQDL rulesets for AWS Glue Data Quality (evaluated on Glue only) -------------------------
def _in(values: list[str]) -> str:
    return "[" + ", ".join(f'"{v}"' for v in values) + "]"


DQDL_RULESETS = {
    "dim_drug": f"""Rules = [
        IsComplete "generic_name", IsUnique "generic_name",
        ColumnValues "therapy_area" in {_in(THERAPY_AREAS)}
    ]""",
    "fct_drug_reactions": """Rules = [
        IsComplete "drug_reaction_key", IsUnique "drug_reaction_key",
        IsComplete "generic_name", IsComplete "report_id", IsComplete "received_date"
    ]""",
    "mart_drug_safety_summary": """Rules = [
        IsComplete "generic_name", IsUnique "generic_name", IsComplete "total_reports",
        ColumnValues "serious_rate" >= 0, ColumnValues "serious_rate" <= 1,
        ColumnValues "death_rate" >= 0, ColumnValues "death_rate" <= 1,
        ColumnValues "hospitalization_rate" >= 0, ColumnValues "hospitalization_rate" <= 1,
        ColumnValues "hcp_reported_rate" >= 0, ColumnValues "hcp_reported_rate" <= 1
    ]""",
    "mart_safety_signals": """Rules = [
        IsComplete "signal_key", IsUnique "signal_key", IsComplete "reports_with_reaction"
    ]""",
}
