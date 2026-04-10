"""
Daily planner — runs once per day (or on worker startup).

For each configured league, queries the fixtures table for today's
matches in the league timezone, computes the active window, writes
the day plan to ops.job_b_day_plan, and seeds one
ops.job_b_fixture_watch row per fixture.

If no fixtures exist for today, the plan is marked inactive and no
watch rows are created.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from . import config, db

log = logging.getLogger("worker.planner")


def plan_league_today(conn, league_id: int) -> dict | None:
    """
    Build today's plan for a single league.

    Window: first_kickoff - PRE_KICKOFF_BUFFER_MIN  →  last_kickoff + POST_MATCH_BUFFER_MIN
    """
    season_year = config.SEASON_YEAR
    tz = ZoneInfo(config.LEAGUE_TZ)

    season_id = db.get_season_id(conn, league_id, season_year)
    if season_id is None:
        log.error("No season for league=%d year=%d. Run Job A first.", league_id, season_year)
        return None

    today_local = datetime.now(tz).date()
    log.info("Planning league %d for %s (season=%d, tz=%s)", league_id, today_local, season_year, config.LEAGUE_TZ)

    db.cleanup_old_watch_rows(conn, keep_days=7)

    fixtures = db.get_fixtures_for_date(conn, season_id, today_local, config.LEAGUE_TZ)
    if not fixtures:
        log.info("League %d: no fixtures today — inactive.", league_id)
        db.upsert_day_plan(
            conn, league_id, season_year, today_local,
            fixture_count=0, status="inactive",
            window_start_utc=None, window_end_utc=None,
        )
        return None

    kickoffs = [f["utc_kickoff"] for f in fixtures if f.get("utc_kickoff")]
    if not kickoffs:
        log.warning("League %d: fixtures but no kickoff times — inactive.", league_id)
        db.upsert_day_plan(
            conn, league_id, season_year, today_local,
            fixture_count=len(fixtures), status="inactive",
            window_start_utc=None, window_end_utc=None,
        )
        return None

    earliest = min(kickoffs)
    latest = max(kickoffs)

    window_start = earliest - timedelta(minutes=config.PRE_KICKOFF_BUFFER_MIN)
    window_end = latest + timedelta(minutes=config.POST_MATCH_BUFFER_MIN)

    log.info(
        "League %d: %d fixtures. Kickoffs: %s → %s. Window: %s → %s",
        league_id, len(fixtures),
        earliest.isoformat(), latest.isoformat(),
        window_start.isoformat(), window_end.isoformat(),
    )

    db.upsert_day_plan(
        conn, league_id, season_year, today_local,
        fixture_count=len(fixtures), status="active",
        window_start_utc=window_start, window_end_utc=window_end,
    )

    db.seed_fixture_watch(conn, league_id, season_year, today_local, fixtures)

    fixture_ids = [f["id"] for f in fixtures]
    log.info("League %d plan complete. Fixture IDs: %s", league_id, fixture_ids)

    return {
        "league_id": league_id,
        "date_local": today_local,
        "fixture_count": len(fixtures),
        "window_start_utc": window_start,
        "window_end_utc": window_end,
        "fixture_ids": fixture_ids,
    }


def plan_all_leagues(conn) -> list[dict]:
    """Run the planner for every configured league. Returns list of active plans."""
    plans = []
    for league_id in config.LEAGUE_IDS:
        result = plan_league_today(conn, league_id)
        if result:
            plans.append(result)
    return plans
