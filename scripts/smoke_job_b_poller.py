"""
Smoke test for the deployed Supabase Edge Function `job_b_live`.

Loads env from src/.env (via env_loader), then POSTs to:
  {SUPABASE_URL}/functions/v1/job_b_live

Requires:
  ORCHESTRATOR_SECRET — same value as Supabase Edge secrets and Railway worker
  SUPABASE_URL        — optional if SUPABASE_DB_URL is set (URL derived from host)

Run from repo root:
  python scripts/smoke_job_b_poller.py

Optional: pass one fixture id to exercise API + DB upsert:
  python scripts/smoke_job_b_poller.py 1234567
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from env_loader import load_repo_dotenv  # noqa: E402


def supabase_url_from_db(db_url: str) -> str:
    db_url = db_url.strip().strip("'\"")
    m = re.search(r"@db\.([a-z0-9]+)\.supabase\.co", db_url, re.I)
    if m:
        return f"https://{m.group(1)}.supabase.co"
    return ""


def main() -> int:
    load_repo_dotenv()

    secret = (os.environ.get("ORCHESTRATOR_SECRET") or "").strip()
    if not secret:
        print(
            "ERROR: ORCHESTRATOR_SECRET is not set.\n"
            "  Add it to src/.env (same value as `supabase secrets set`), or export it\n"
            "  in this shell.",
            file=sys.stderr,
        )
        return 1

    base = (os.environ.get("SUPABASE_URL") or "").strip().strip("'\"")
    if not base:
        db = (os.environ.get("SUPABASE_DB_URL") or "").strip().strip("'\"")
        base = supabase_url_from_db(db)
    if not base:
        print("ERROR: Set SUPABASE_URL or SUPABASE_DB_URL in src/.env", file=sys.stderr)
        return 1

    fixture_ids: list[int] = []
    if len(sys.argv) > 1:
        fixture_ids = [int(sys.argv[1])]
    else:
        try:
            import psycopg2

            db_url = (os.environ.get("SUPABASE_DB_URL") or "").strip().strip("'\"")
            if db_url:
                conn = psycopg2.connect(db_url)
                try:
                    with conn.cursor() as cur:
                        cur.execute("SELECT id FROM fixtures ORDER BY utc_kickoff DESC NULLS LAST LIMIT 1")
                        row = cur.fetchone()
                        if row:
                            fixture_ids = [int(row[0])]
                finally:
                    conn.close()
        except Exception as exc:
            print(f"Note: no fixture id from DB ({exc}); sending empty fixture_ids.")

    uri = f"{base.rstrip('/')}/functions/v1/job_b_live"
    print(f"POST {uri}")
    print(f"Body: {{'fixture_ids': {fixture_ids}}}")

    resp = requests.post(
        uri,
        json={"fixture_ids": fixture_ids},
        headers={
            "Authorization": f"Bearer {secret}",
            "Content-Type": "application/json",
        },
        timeout=60,
    )
    print(f"HTTP {resp.status_code}")
    try:
        print(json.dumps(resp.json(), indent=2))
    except Exception:
        print(resp.text)
    return 0 if resp.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
