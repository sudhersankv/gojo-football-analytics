"""Fetch full Premier League fixture list for one API season (single request — no `page` param)."""
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / "src" / ".env")

BASE = "https://v3.football.api-sports.io"
LEAGUE_ID = 39
SEASON = 2025  # 2025/26 — adjust when API rolls to next season

key = os.environ.get("FOOTBALL_API_KEY")
if not key:
    print("Missing FOOTBALL_API_KEY", file=sys.stderr)
    sys.exit(1)

headers = {"x-apisports-key": key}
r = requests.get(
    f"{BASE}/fixtures",
    headers=headers,
    params={"league": LEAGUE_ID, "season": SEASON},
    timeout=60,
)
r.raise_for_status()
data = r.json()
if data.get("errors"):
    print(data["errors"], file=sys.stderr)
    sys.exit(1)

all_rows = data.get("response") or []
out = {"results": len(all_rows), "fixtures": all_rows}
print(json.dumps(out, indent=2, ensure_ascii=False))
