"""Probe live MLS fixtures (e.g. LA Galaxy vs Minnesota) and player stats.

Writes full API payloads to project ``out/`` for inspection in the editor or a JSON viewer.
"""

import json
import os
import sys
from pathlib import Path

import requests

from env_loader import load_repo_dotenv

load_repo_dotenv()

BASE = "https://v3.football.api-sports.io"
HEADERS = {"x-apisports-key": os.environ.get("FOOTBALL_API_KEY", "")}

# MLS in API-Football is often league id 253 (verify via /leagues?country=USA&season=2025)
MLS_LEAGUE_ID = 253

# Repo root: .../Gojo ; script lives in .../Gojo/misc/
OUT_DIR = Path(__file__).resolve().parent.parent / "out"


def _write_json(path: Path, data: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> None:
    if not HEADERS["x-apisports-key"]:
        print("Missing FOOTBALL_API_KEY", file=sys.stderr)
        sys.exit(1)

    r = requests.get(f"{BASE}/status", headers=HEADERS, timeout=30)
    r.raise_for_status()
    st = r.json()
    print("=== /status ===")
    print(json.dumps(st.get("response", {}), indent=2))

    # All live fixtures
    r = requests.get(f"{BASE}/fixtures?live=all", headers=HEADERS, timeout=30)
    r.raise_for_status()
    live = r.json()
    _write_json(OUT_DIR / "live_all.json", live)
    print(f"\nWrote {OUT_DIR / 'live_all.json'}")

    print("\n=== /fixtures?live=all ===")
    print("errors:", live.get("errors"))
    print("results:", live.get("results"))

    items = live.get("response") or []
    galaxy_min = []
    for item in items:
        league = item.get("league") or {}
        teams = item.get("teams") or {}
        home = (teams.get("home") or {}).get("name", "")
        away = (teams.get("away") or {}).get("name", "")
        blob = f"{home} {away}".lower()
        if "galaxy" in blob and ("minnesota" in blob or "minn" in blob):
            galaxy_min.append(item)
        if league.get("id") == MLS_LEAGUE_ID:
            print(
                f"  MLS live: fixture={item.get('fixture', {}).get('id')} "
                f"{home} vs {away} | status={item.get('fixture', {}).get('status', {}).get('short')}"
            )

    target = galaxy_min[0] if galaxy_min else None
    if not target and items:
        # Fallback: first MLS match if Galaxyâ€“Minnesota not in list
        for item in items:
            if (item.get("league") or {}).get("id") == MLS_LEAGUE_ID:
                target = item
                print("\n(No Galaxy vs Minnesota in live=all; using first MLS live match.)")
                break

    if not target:
        print("\nNo matching live fixture right now. Try team search + today's fixtures.")
        # Search teams
        for q in ("Galaxy", "Minnesota"):
            tr = requests.get(f"{BASE}/teams?search={q}", headers=HEADERS, timeout=30).json()
            print(f"\nteams?search={q} -> results:", tr.get("results"), "errors:", tr.get("errors"))
            for row in (tr.get("response") or [])[:3]:
                t = row.get("team") or {}
                print(f"  id={t.get('id')} name={t.get('name')}")
        sys.exit(0)

    fid = target["fixture"]["id"]
    print(f"\n=== Selected fixture id={fid} ===")
    print(json.dumps(target, indent=2)[:4000])
    if len(json.dumps(target)) > 4000:
        print("... (truncated)")

    _write_json(OUT_DIR / f"live_fixture_{fid}_summary_from_live_all.json", target)

    # Full fixture (docs: includes events, lineups, stats for single id)
    r = requests.get(f"{BASE}/fixtures?id={fid}", headers=HEADERS, timeout=30)
    r.raise_for_status()
    full = r.json()
    _write_json(OUT_DIR / f"live_fixture_{fid}_fixtures_full.json", full)
    print(f"Wrote {OUT_DIR / f'live_fixture_{fid}_fixtures_full.json'}")

    print("\n=== /fixtures?id=... (keys in first response item) ===")
    if full.get("response"):
        keys = list(full["response"][0].keys())
        print("top-level keys:", keys)

    # Live player stats for this fixture
    r = requests.get(f"{BASE}/fixtures/players?fixture={fid}", headers=HEADERS, timeout=30)
    r.raise_for_status()
    fp = r.json()
    _write_json(OUT_DIR / f"live_fixture_{fid}_players.json", fp)
    print(f"Wrote {OUT_DIR / f'live_fixture_{fid}_players.json'}")

    print("\n=== /fixtures/players?fixture=... ===")
    print("errors:", fp.get("errors"), "results:", fp.get("results"))
    resp = fp.get("response") or []
    for grp in resp[:2]:
        team = grp.get("team") or {}
        players = grp.get("players") or []
        print(f"  Team {team.get('name')} ({len(players)} players with stats)")
        for p in players[:3]:
            pl = p.get("player") or {}
            stats = (p.get("statistics") or [{}])[0]
            games = stats.get("games") or {}
            print(
                f"    {pl.get('name')}: rating={games.get('rating')} "
                f"min={games.get('minutes')} pos={games.get('position')}"
            )

    print(f"\nAll JSON files under: {OUT_DIR.resolve()}")


if __name__ == "__main__":
    main()


