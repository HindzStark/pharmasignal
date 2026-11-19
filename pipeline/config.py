"""Central configuration for the PharmaSignal pipeline."""
import csv
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
RAW_DIR = DATA_DIR / "raw"            # bronze: untouched API responses (local copy, mirrored to S3)
STATE_PATH = DATA_DIR / "_state.json"  # high-water marks for incremental extracts

OPENFDA_URL = "https://api.fda.gov/drug/event.json"
# Optional API key (https://open.fda.gov/apis/authentication/). openFDA rejects limit=1000 without
# a key (HTTP 403 API_KEY_MISSING), so keyless runs use a smaller page.
OPENFDA_API_KEY = os.environ.get("OPENFDA_API_KEY") or None
PAGE_SIZE = 1000 if OPENFDA_API_KEY else 500
MAX_RECORDS_PER_DRUG = 5000  # cap per drug per run (~60 API calls total, well inside the no-key daily quota)

# Drugs tracked: MSD brands + direct competitors in the same therapy areas.
# Single source of truth: the extractor reads it here and the Glue job reads the copy shipped to
# s3://<bucket>/reference/tracked_drugs.csv, so the two never drift apart.
SEED_PATH = ROOT / "reference" / "tracked_drugs.csv"
with SEED_PATH.open(newline="") as _f:
    DRUGS = {row["generic_name"]: row for row in csv.DictReader(_f)}

# Default backfill window when no state exists yet (YYYYMMDD).
DEFAULT_START_DATE = "20250701"

# AWS (resources come from the CloudFormation stack's outputs; credentials from the default AWS profile)
AWS_REGION = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or "ap-south-1"
STACK_NAME = os.environ.get("PHARMASIGNAL_STACK", "pharmasignal")
