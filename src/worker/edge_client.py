"""
HTTP client for invoking Supabase Edge Functions.

Two functions are targeted:

  job_b_live — fetches live fixture data from API-Football, writes Redis
               time buckets (when wired), returns per-fixture statuses.
               Does NOT write to Postgres.

  job_c      — post-match detail ingestion.  Fetches full fixture data +
               player stats from API-Football and upserts into Postgres
               (events, lineups, team stats, player stats, fixtures).

Expected response contracts:

  job_b_live → {
      "ok": true,
      "fixture_statuses": [
          {"fixture_id": 12345, "status_short": "2H"},
          {"fixture_id": 67890, "status_short": "FT"}
      ]
  }

  job_c → {
      "ok": true,
      "results": [
          {"fixture_id": 123, "status": "ok"},
          {"fixture_id": 456, "status": "error", "error": "..."}
      ]
  }

  On error:
      {"ok": false, "error": "...message..."}
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from . import config

log = logging.getLogger("worker.edge_client")

TIMEOUT_SEC = 25


def _build_url(fn_name: str) -> str:
    base = config.SUPABASE_URL.rstrip("/")
    return f"{base}/functions/v1/{fn_name}"


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {config.ORCHESTRATOR_SECRET}",
        "Content-Type": "application/json",
    }


def invoke_job_b(fixture_ids: list[int]) -> dict[str, Any]:
    """
    Invoke the job_b_live edge function with an explicit batch of
    fixture IDs.  Returns the parsed JSON response with per-fixture
    statuses, or an error dict if the call fails.
    """
    url = _build_url("job_b_live")
    payload = {"fixture_ids": fixture_ids}

    log.info("→ Job B tick: %d fixtures %s", len(fixture_ids), fixture_ids)

    if config.DRY_RUN:
        log.info("  [DRY RUN] Skipped POST to %s", url)
        return {
            "ok": True,
            "dry_run": True,
            "fixture_statuses": [
                {"fixture_id": fid, "status_short": "NS"} for fid in fixture_ids
            ],
        }

    try:
        resp = requests.post(url, json=payload, headers=_headers(), timeout=TIMEOUT_SEC)
        resp.raise_for_status()
        data = resp.json()
        log.info("← Job B response: %s", data)
        return data
    except requests.RequestException as exc:
        log.error("Job B invocation failed: %s", exc)
        return {"ok": False, "error": str(exc)}


JOB_C_TIMEOUT_SEC = 55


def invoke_job_c(fixture_ids: list[int]) -> dict[str, Any]:
    """
    Invoke the job_c edge function to run post-match detail ingestion
    for the given fixtures.  Each fixture triggers its own API-Football
    calls, so this can take a while — timeout is longer than job_b.
    """
    url = _build_url("job_c")
    payload = {"fixture_ids": fixture_ids}

    log.info("→ Job C: %d fixtures %s", len(fixture_ids), fixture_ids)

    if config.DRY_RUN:
        log.info("  [DRY RUN] Skipped POST to %s", url)
        return {
            "ok": True,
            "dry_run": True,
            "results": [
                {"fixture_id": fid, "status": "dry_run"} for fid in fixture_ids
            ],
        }

    try:
        resp = requests.post(url, json=payload, headers=_headers(), timeout=JOB_C_TIMEOUT_SEC)
        resp.raise_for_status()
        data = resp.json()
        log.info("← Job C response: %s", data)
        return data
    except requests.RequestException as exc:
        log.error("Job C invocation failed: %s", exc)
        return {"ok": False, "error": str(exc)}
