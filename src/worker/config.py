"""
Worker configuration — all settings from environment variables.

Required env vars:
  SUPABASE_DB_URL          Postgres connection string (for reading fixtures + ops tables)
  SUPABASE_URL             Supabase project URL (for edge function invocation)
  ORCHESTRATOR_SECRET      Shared secret sent to edge functions for auth

Optional env vars (defaults shown):
  LEAGUE_IDS               Comma-separated league IDs to track (default: "39")
  SEASON_YEAR              API season year (default: 2025)
  LEAGUE_TZ                IANA timezone for the league (default: Europe/London)
  TICK_INTERVAL_SEC        Seconds between Job B ticks (default: 30)
  PRE_KICKOFF_BUFFER_MIN   Minutes before earliest kickoff to open window (default: 30)
  POST_MATCH_BUFFER_MIN    Minutes after latest kickoff to close window (default: 150 = 2h30m)
  PLANNER_HOUR_LOCAL       Hour (in league TZ) to run daily planner (default: 6 = 06:00)
  DRY_RUN                  If "true", log actions but skip edge invocations (default: false)
"""

import os


def _env(key: str, default: str | None = None, required: bool = False) -> str:
    val = os.environ.get(key, default)
    if required and not val:
        raise SystemExit(f"Missing required env var: {key}")
    return val or ""


SUPABASE_DB_URL = _env("SUPABASE_DB_URL", required=True)
SUPABASE_URL = _env("SUPABASE_URL", required=True)
ORCHESTRATOR_SECRET = _env("ORCHESTRATOR_SECRET", required=True)

LEAGUE_IDS = [int(x.strip()) for x in _env("LEAGUE_IDS", "39").split(",") if x.strip()]
SEASON_YEAR = int(_env("SEASON_YEAR", "2025"))
LEAGUE_TZ = _env("LEAGUE_TZ", "Europe/London")

TICK_INTERVAL_SEC = int(_env("TICK_INTERVAL_SEC", "30"))
PRE_KICKOFF_BUFFER_MIN = int(_env("PRE_KICKOFF_BUFFER_MIN", "30"))
POST_MATCH_BUFFER_MIN = int(_env("POST_MATCH_BUFFER_MIN", "150"))
PLANNER_HOUR_LOCAL = int(_env("PLANNER_HOUR_LOCAL", "6"))

DRY_RUN = _env("DRY_RUN", "false").lower() == "true"
