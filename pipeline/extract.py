"""Extract: pull adverse-event reports from the openFDA FAERS API into the bronze layer.

- Incremental: remembers the latest receivedate loaded per drug (data/_state.json)
  and only asks for newer reports on the next run.
- Resilient: retries with exponential backoff on 429/5xx and network errors.
- Bronze files are written untouched, partitioned by drug and load date:
  data/raw/drug=PEMBROLIZUMAB/load_date=2026-11-19/page_<HHMMSS>_0000.json
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone

import requests

from pipeline import config

log = logging.getLogger(__name__)


def load_state() -> dict:
    if config.STATE_PATH.exists():
        return json.loads(config.STATE_PATH.read_text())
    return {}


def save_state(state: dict) -> None:
    config.STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    config.STATE_PATH.write_text(json.dumps(state, indent=2))


def fetch_page(session: requests.Session, params: dict, max_retries: int = 5) -> dict | None:
    """GET one page from openFDA. Returns None when openFDA reports no matches."""
    if config.OPENFDA_API_KEY:
        params = {**params, "api_key": config.OPENFDA_API_KEY}
    for attempt in range(max_retries):
        try:
            resp = session.get(config.OPENFDA_URL, params=params, timeout=30)
        except requests.RequestException as exc:
            wait = 2 ** attempt
            log.warning("Network error (%s); retry %d in %ss", exc, attempt + 1, wait)
            time.sleep(wait)
            continue

        if resp.status_code == 404:  # openFDA returns 404 for "no matches"
            return None
        if resp.status_code in (401, 403):  # auth/quota problems are not transient: fail with the API's reason
            try:
                err = resp.json().get("error", {})
            except ValueError:
                err = {}
            raise RuntimeError(
                f"openFDA refused the request (HTTP {resp.status_code} {err.get('code', '')}: "
                f"{err.get('message', '')}). Set OPENFDA_API_KEY or lower PAGE_SIZE.")
        if resp.status_code in (429, 500, 502, 503, 504):
            wait = int(resp.headers.get("Retry-After", 2 ** attempt))
            log.warning("HTTP %s; retry %d in %ss", resp.status_code, attempt + 1, wait)
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"openFDA request failed after {max_retries} retries: {params}")


def extract_drug(session: requests.Session, generic_name: str, since: str, until: str) -> tuple[int, str | None]:
    """Pull reports for one drug with receivedate in [since, until]. Returns (count, max_receivedate)."""
    out_dir = config.RAW_DIR / f"drug={generic_name}" / f"load_date={date.today().isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)

    search = f'patient.drug.openfda.generic_name:"{generic_name}" AND receivedate:[{since} TO {until}]'
    total, page, max_date = 0, 0, None
    # Bronze files are immutable: a second run on the same day writes new names instead of
    # overwriting earlier pages (so the S3 upload can safely skip keys that already exist).
    run_tag = datetime.now(timezone.utc).strftime("%H%M%S")

    while total < config.MAX_RECORDS_PER_DRUG:
        params = {"search": search, "limit": config.PAGE_SIZE, "skip": total, "sort": "receivedate:asc"}
        payload = fetch_page(session, params)
        if not payload or not payload.get("results"):
            break

        results = payload["results"]
        (out_dir / f"page_{run_tag}_{page:04d}.json").write_text(json.dumps(
            {"_extracted_at": datetime.now(timezone.utc).isoformat(), "_drug": generic_name, "results": results}
        ))
        total += len(results)
        page += 1
        max_date = max([max_date or ""] + [r.get("receivedate", "") for r in results]) or None

        if len(results) < config.PAGE_SIZE:
            break
        time.sleep(0.3)  # stay well under the 240 req/min no-key limit

    # If we stopped at the cap, the last date may be only partly loaded. Step the
    # high-water mark back one day so the next run re-fetches it (upserts dedupe).
    if total >= config.MAX_RECORDS_PER_DRUG and max_date:
        max_date = (datetime.strptime(max_date, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
    return total, max_date


def run() -> None:
    state = load_state()
    until = date.today().strftime("%Y%m%d")
    with requests.Session() as session:
        for generic_name in config.DRUGS:
            last = state.get(generic_name)
            since = (datetime.strptime(last, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d") if last else config.DEFAULT_START_DATE
            if since > until:
                log.info("%s: up to date", generic_name)
                continue
            count, max_date = extract_drug(session, generic_name, since, until)
            log.info("%s: extracted %d reports (%s -> %s)", generic_name, count, since, max_date)
            if max_date:
                state[generic_name] = max_date
                save_state(state)  # save after each drug so a failure mid-run loses nothing


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
