"""Orchestrator: extract -> upload to S3 -> Glue transform + DQ + load -> Athena validation.

Usage:
    python -m pipeline.run_pipeline                 # full run
    python -m pipeline.run_pipeline --skip-extract  # reuse local bronze files, don't call openFDA

Fails fast: a failed Glue run (including a failed data-quality check) exits non-zero with a link to
its CloudWatch logs; the previously published tables stay untouched.
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from urllib.parse import quote

import boto3

from pipeline import aws, config, extract, upload

log = logging.getLogger("pipeline")
TERMINAL = {"SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT", "ERROR"}
G1X_DPU_PER_WORKER = 1  # G.1X worker = 1 DPU

VALIDATION_QUERIES = {
    "rows per curated table": """
        select 'dim_drug' as table_name, count(*) as row_count from dim_drug
        union all select 'fct_drug_reactions', count(*) from fct_drug_reactions
        union all select 'mart_drug_safety_summary', count(*) from mart_drug_safety_summary
        union all select 'mart_safety_signals', count(*) from mart_safety_signals
        order by 1""",
    "reports and flagged signals per brand": """
        select s.brand_name, s.total_reports, coalesce(g.signals, 0) as flagged_signals
        from mart_drug_safety_summary s
        left join (select generic_name, count(*) as signals from mart_safety_signals
                   where is_signal group by 1) g using (generic_name)
        order by s.therapy_area, s.total_reports desc""",
    "top 5 Keytruda signals": """
        select reaction_pt, reports_with_reaction, drug_total_reports, prr, chi_square
        from mart_safety_signals
        where generic_name = 'PEMBROLIZUMAB' and is_signal
        order by prr desc, reports_with_reaction desc limit 5""",
}


def step(name, fn):
    start = time.time()
    log.info("> %s", name)
    out = fn()
    log.info("ok %s (%.1fs)", name, time.time() - start)
    return out


def logs_url(run_id: str, region: str = config.AWS_REGION) -> str:
    group = quote(quote("/aws-glue/jobs/error", safe=""), safe="")  # console needs double-encoding
    return (f"https://{region}.console.aws.amazon.com/cloudwatch/home?region={region}"
            f"#logsV2:log-groups/log-group/{group}/log-events/{run_id}")


def run_glue_job(job_name: str, poll: int = 20) -> dict:
    glue = boto3.client("glue", region_name=config.AWS_REGION)
    run_id = glue.start_job_run(JobName=job_name)["JobRunId"]
    log.info("Glue run %s started", run_id)
    state = None
    while True:
        run = glue.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)["JobRun"]
        if run["JobRunState"] != state:
            state = run["JobRunState"]
            log.info("Glue run state: %s", state)
        if state in TERMINAL:
            break
        time.sleep(poll)
    if state != "SUCCEEDED":
        log.error("Glue job %s: %s", state, run.get("ErrorMessage", ""))
        log.error("Error logs: %s", logs_url(run_id))
        log.error("Output logs: %s", logs_url(run_id).replace("error", "output"))
        raise SystemExit(1)

    seconds = run.get("ExecutionTime", 0)
    workers = run.get("NumberOfWorkers") or 0
    dpu_seconds = run.get("DPUSeconds") or seconds * workers * G1X_DPU_PER_WORKER
    billed = max(dpu_seconds, 60 * workers * G1X_DPU_PER_WORKER)  # per-second billing, 1-minute minimum
    log.info("Glue run %s: %ss on %s x %s, %.0f DPU-seconds (billed %.0f = %.4f DPU-hours)",
             run_id, seconds, workers, run.get("WorkerType"), dpu_seconds, billed, billed / 3600)
    return run


def validate() -> None:
    for title, sql in VALIDATION_QUERIES.items():
        df = aws.athena_query(sql)
        print(f"\n== {title} ==")
        print(df.to_string(index=False))


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-extract", action="store_true", help="don't call openFDA; use local bronze")
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--validate-only", action="store_true", help="only run the Athena validation queries")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    out = aws.stack_outputs()
    if not args.validate_only:
        if not args.skip_extract:
            step("extract (openFDA -> local bronze)", extract.run)
        if not args.skip_upload:
            step("upload (local bronze -> S3 bronze)", lambda: upload.run(out["BucketName"]))
        step("transform + DQ + load (Glue)", lambda: run_glue_job(out["GlueJobName"]))
    step("validate (Athena)", validate)


if __name__ == "__main__":
    sys.exit(main())
