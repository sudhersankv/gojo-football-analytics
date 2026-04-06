"""Fetch full API-Football JSON for Vancouver vs Portland (MLS) on a given date."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import requests

from env_loader import load_repo_dotenv

load_repo_dotenv()

BASE = "https://v3.football.api-sports.io"
MLS_LEAGUE_ID = 253


def _headers() -> dict[str, str]:
    key = os.environ.get("FOOTBALL_API_KEY", "")
    if not key:
        print("Missing FOOTBALL_API_KEY in src/.env", file=sys.stderr)
        sys.exit(1)
    return {"x-apisports-key": key}


def _match_van_por(item: dict) -> bool:
    teams = item.get("teams") or {}
    home = ((teams.get("home") or {}).get("name") or "").lower()
    away = ((teams.get("away") or {}).get("name") or "").lower()
    blob = f"{home} {away}"
    has_van = "vancouver" in blob
    has_por = "portland" in blob
    return has_van and has_por


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--date",
        help="YYYY-MM-DD (default: today UTC)",
        default=None,
    )
    parser.add_argument(
        "--season",
        type=int,
        help="MLS season year (default: year of --date)",
        default=None,
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Write JSON to this file instead of stdout",
        default=None,
    )
    args = parser.parse_args()

    if args.date:
        ymd = args.date
    else:
        ymd = datetime.now(timezone.utc).date().isoformat()

    season = args.season if args.season is not None else date.fromisoformat(ymd).year
    h = _headers()

    def get_fixtures(params: dict) -> dict:
        r = requests.get(f"{BASE}/fixtures", headers=h, params=params, timeout=45)
        r.raise_for_status()
        return r.json()

    data = get_fixtures({"league": MLS_LEAGUE_ID, "season": season, "date": ymd})
    if data.get("errors") or not (data.get("response") or []):
        # Retry previous season if empty (early-year edge cases)
        if season > 2020:
            data = get_fixtures({"league": MLS_LEAGUE_ID, "season": season - 1, "date": ymd})

    rows = data.get("response") or []
    picked = next((r for r in rows if _match_van_por(r)), None)

    if not picked and rows:
        # Try live=all if scheduled match not on date endpoint yet
        live = requests.get(f"{BASE}/fixtures", headers=h, params={"live": "all"}, timeout=45)
        live.raise_for_status()
        lj = live.json()
        for r in lj.get("response") or []:
            if (r.get("league") or {}).get("id") != MLS_LEAGUE_ID:
                continue
            if _match_van_por(r):
                picked = r
                break

    if not picked:
        err = {
            "error": "No Vancouver vs Portland fixture found",
            "searched_date": ymd,
            "season_tried": season,
            "fixtures_on_date": len(rows),
            "api_errors": data.get("errors"),
        }
        out = json.dumps(err, indent=2)
        if args.out:
            args.out.write_text(out, encoding="utf-8")
        else:
            print(out)
        sys.exit(1)

    fid = picked["fixture"]["id"]
    r = requests.get(f"{BASE}/fixtures", headers=h, params={"id": fid}, timeout=45)
    r.raise_for_status()
    full = r.json()

    out = json.dumps(full, indent=2, ensure_ascii=False)
    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(out, encoding="utf-8")
    else:
        print(out)


if __name__ == "__main__":
    main()
