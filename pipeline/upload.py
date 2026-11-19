"""Upload: gzip local bronze JSON and copy it to s3://<bucket>/bronze/drug=.../load_date=.../

Idempotent: keys that already exist in S3 are skipped (bronze files are immutable, see extract.py),
so re-running never re-uploads or duplicates data. Also ships the tracked-drug reference list.
"""
from __future__ import annotations

import gzip
import logging
from pathlib import Path

from pipeline import config

log = logging.getLogger(__name__)
BRONZE_PREFIX = "bronze/"
REFERENCE_KEY = "reference/tracked_drugs.csv"


def bronze_key(path: Path, raw_dir: Path) -> str:
    return BRONZE_PREFIX + path.relative_to(raw_dir).as_posix() + ".gz"


def existing_keys(s3, bucket: str, prefix: str) -> set[str]:
    keys = set()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.update(o["Key"] for o in page.get("Contents", []))
    return keys


def upload_bronze(s3, bucket: str, raw_dir: Path = config.RAW_DIR) -> tuple[int, int]:
    """Returns (uploaded, skipped)."""
    have = existing_keys(s3, bucket, BRONZE_PREFIX)
    uploaded = skipped = 0
    for path in sorted(raw_dir.rglob("*.json")):
        key = bronze_key(path, raw_dir)
        if key in have:
            skipped += 1
            continue
        body = gzip.compress(path.read_bytes(), compresslevel=6, mtime=0)
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
        uploaded += 1
        log.info("uploaded %s (%.1f MB -> %.1f MB gz)", key, path.stat().st_size / 1e6, len(body) / 1e6)
    return uploaded, skipped


def upload_reference(s3, bucket: str) -> None:
    s3.put_object(Bucket=bucket, Key=REFERENCE_KEY, Body=config.SEED_PATH.read_bytes(), ContentType="text/csv")


def run(bucket: str, s3=None) -> None:
    if s3 is None:
        import boto3
        s3 = boto3.client("s3", region_name=config.AWS_REGION)
    upload_reference(s3, bucket)
    uploaded, skipped = upload_bronze(s3, bucket)
    log.info("bronze upload: %d new file(s), %d already in S3", uploaded, skipped)
