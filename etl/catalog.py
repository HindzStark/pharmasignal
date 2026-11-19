"""Register curated Parquet tables in the Glue Data Catalog directly from the job (no crawlers).

`table_input` is pure (Spark schema -> Glue TableInput dict) so it is unit-tested locally;
`publish_table` takes a boto3 Glue client and is only called on AWS.
"""
from __future__ import annotations

from pyspark.sql import types as T

_PARQUET = {
    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
    "SerdeInfo": {"SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"},
}


def hive_type(dt: T.DataType) -> str:
    if isinstance(dt, T.StringType):
        return "string"
    if isinstance(dt, T.BooleanType):
        return "boolean"
    if isinstance(dt, T.IntegerType):
        return "int"
    if isinstance(dt, T.LongType):
        return "bigint"
    if isinstance(dt, T.DoubleType):
        return "double"
    if isinstance(dt, T.FloatType):
        return "float"
    if isinstance(dt, T.DateType):
        return "date"
    if isinstance(dt, T.TimestampType):
        return "timestamp"
    if isinstance(dt, T.DecimalType):
        return f"decimal({dt.precision},{dt.scale})"
    raise TypeError(f"No Hive/Athena type mapping for {dt!r}")


def table_input(name: str, schema: T.StructType, location: str, partition_cols: list[str],
                description: str = "") -> dict:
    cols = [{"Name": f.name, "Type": hive_type(f.dataType)} for f in schema.fields if f.name not in partition_cols]
    parts = [{"Name": f.name, "Type": hive_type(f.dataType)} for f in schema.fields if f.name in partition_cols]
    return {
        "Name": name,
        "Description": description,
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {"classification": "parquet", "parquet.compression": "SNAPPY", "EXTERNAL": "TRUE"},
        "PartitionKeys": parts,
        "StorageDescriptor": {"Columns": cols, "Location": location.rstrip("/") + "/", **_PARQUET},
    }


def partition_inputs(table: dict, values: list[str]) -> list[dict]:
    """One PartitionInput per value of a single partition key, pointing at Spark's key=value folders."""
    (key,) = [k["Name"] for k in table["PartitionKeys"]]
    sd = table["StorageDescriptor"]
    return [{"Values": [v],
             "StorageDescriptor": {**sd, "Location": f"{sd['Location']}{key}={v}/"}} for v in values]


def publish_table(glue, database: str, table: dict, partition_values: list[str] | None = None) -> None:
    """Create or replace a table definition and (re)point its partitions at the new data location."""
    try:
        glue.update_table(DatabaseName=database, TableInput=table)
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=database, TableInput=table)

    if not table["PartitionKeys"]:
        return
    existing = []
    for page in glue.get_paginator("get_partitions").paginate(DatabaseName=database, TableName=table["Name"]):
        existing += [{"Values": p["Values"]} for p in page["Partitions"]]
    for i in range(0, len(existing), 25):
        glue.batch_delete_partition(DatabaseName=database, TableName=table["Name"],
                                    PartitionsToDelete=existing[i:i + 25])
    new = partition_inputs(table, partition_values or [])
    for i in range(0, len(new), 100):
        resp = glue.batch_create_partition(DatabaseName=database, TableName=table["Name"],
                                           PartitionInputList=new[i:i + 100])
        if resp.get("Errors"):
            raise RuntimeError(f"Failed to register partitions for {table['Name']}: {resp['Errors']}")
