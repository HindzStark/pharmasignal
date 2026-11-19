"""Thin AWS helpers shared by the orchestrator and the dashboard (boto3 only, default credential chain)."""
from __future__ import annotations

import time
from functools import lru_cache

import boto3
import pandas as pd

from pipeline import config


@lru_cache(maxsize=1)
def stack_outputs(stack_name: str = config.STACK_NAME) -> dict[str, str]:
    cfn = boto3.client("cloudformation", region_name=config.AWS_REGION)
    stack = cfn.describe_stacks(StackName=stack_name)["Stacks"][0]
    return {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}


def sql_literal(value) -> str:
    """Athena inserts ExecutionParameters verbatim, so strings must arrive as quoted SQL literals."""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def athena_query(sql: str, params: list | None = None, workgroup: str | None = None,
                 database: str | None = None, poll: float = 1.0) -> pd.DataFrame:
    """Run a query in the stack's Athena workgroup and return the result as a DataFrame (all strings)."""
    out = stack_outputs()
    athena = boto3.client("athena", region_name=config.AWS_REGION)
    req = {"QueryString": sql, "WorkGroup": workgroup or out["AthenaWorkGroup"],
           "QueryExecutionContext": {"Database": database or out["GlueDatabase"]}}
    if params:
        req["ExecutionParameters"] = [sql_literal(p) for p in params]
    qid = athena.start_query_execution(**req)["QueryExecutionId"]
    while True:
        status = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(poll)
    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {status['State']}: {status.get('StateChangeReason')}\n{sql}")

    rows, header = [], None
    for page in athena.get_paginator("get_query_results").paginate(QueryExecutionId=qid):
        for r in page["ResultSet"]["Rows"]:
            vals = [c.get("VarCharValue") for c in r["Data"]]
            if header is None:
                header = vals
            else:
                rows.append(vals)
    return pd.DataFrame(rows, columns=header)
