"""
Database operations for the Railway worker.

Reads from:
  - public.fixtures           (today's schedule, for daily planner)
  - public.seasons            (resolve league_id + year → season_id)
  - ops.job_b_day_plan        (day-level window gate)
  - ops.job_b_fixture_watch   (per-fixture lifecycle state)

Writes to:
  - ops.job_b_day_plan        (daily planner output)
  - ops.job_b_fixture_watch   (planner seeds rows; worker updates after each tick)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

import psycopg2

log = logging.getLogger("worker.db")

LIVE_STATUSES = frozenset({"1H", "HT", "2H", "ET", "BT", "P", "LIVE"})

TERMINAL_STATUSES = frozenset({
    "FT", "AET", "PEN",
    "PST", "CANC", "ABD", "AWD", "WO",
    "INT", "SUSP",
})


def get_connection(db_url: str):
    return psycopg2.connect(db_url)


# ---------------------------------------------------------------------------
# Season resolution
# ---------------------------------------------------------------------------

def get_season_id(conn, league_id: int, season_year: int) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id FROM seasons WHERE league_id = %s AND year = %s",
            (league_id, season_year),
        )
        row = cur.fetchone()
        return int(row[0]) if row else None


# ---------------------------------------------------------------------------
# Fixture queries (read-only, from public.fixtures)
# ---------------------------------------------------------------------------

def get_fixtures_for_date(
    conn, season_id: int, target_date: date, tz_name: str,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, utc_kickoff, status_short
            FROM fixtures
            WHERE season_id = %s
              AND (utc_kickoff AT TIME ZONE %s)::date = %s
            ORDER BY utc_kickoff
            """,
            (season_id, tz_name, target_date),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Day plan (ops.job_b_day_plan)
# ---------------------------------------------------------------------------

def upsert_day_plan(
    conn, league_id: int, season_year: int, date_local: date,
    fixture_count: int, status: str,
    window_start_utc: datetime | None, window_end_utc: datetime | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ops.job_b_day_plan
                (league_id, season_year, date_local, fixture_count, status,
                 window_start_utc, window_end_utc)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (league_id, season_year, date_local) DO UPDATE SET
                fixture_count    = EXCLUDED.fixture_count,
                status           = EXCLUDED.status,
                window_start_utc = EXCLUDED.window_start_utc,
                window_end_utc   = EXCLUDED.window_end_utc
            """,
            (league_id, season_year, date_local, fixture_count, status,
             window_start_utc, window_end_utc),
        )
    conn.commit()


def get_day_plan(conn, league_id: int, season_year: int, date_local: date) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT league_id, season_year, date_local, fixture_count, status,
                   window_start_utc, window_end_utc
            FROM ops.job_b_day_plan
            WHERE league_id = %s AND season_year = %s AND date_local = %s
            """,
            (league_id, season_year, date_local),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


def mark_plan_ended(conn, league_id: int, season_year: int, date_local: date) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ops.job_b_day_plan SET status = 'ended'
            WHERE league_id = %s AND season_year = %s AND date_local = %s
            """,
            (league_id, season_year, date_local),
        )
    conn.commit()


# ---------------------------------------------------------------------------
# Fixture watch (ops.job_b_fixture_watch)
# ---------------------------------------------------------------------------

def seed_fixture_watch(
    conn, league_id: int, season_year: int, date_local: date,
    fixtures: list[dict[str, Any]],
) -> int:
    """
    Insert one watch row per fixture.  Existing rows are left untouched
    (ON CONFLICT DO NOTHING) so a re-plan doesn't reset mid-day state.
    """
    if not fixtures:
        return 0

    inserted = 0
    with conn.cursor() as cur:
        for f in fixtures:
            status = f.get("status_short", "NS")
            phase = _classify_phase(status)
            is_term = status in TERMINAL_STATUSES
            cur.execute(
                """
                INSERT INTO ops.job_b_fixture_watch
                    (fixture_id, league_id, season_year, date_local,
                     utc_kickoff, status_short, phase, is_terminal)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fixture_id) DO NOTHING
                """,
                (f["id"], league_id, season_year, date_local,
                 f.get("utc_kickoff"), status, phase, is_term),
            )
            inserted += cur.rowcount
    conn.commit()
    log.info("Seeded %d new fixture_watch rows (of %d fixtures).", inserted, len(fixtures))
    return inserted


def get_active_fixtures(conn, league_id: int, date_local: date) -> list[dict[str, Any]]:
    """Return all non-terminal fixtures for today — the batch for this tick."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT fixture_id, utc_kickoff, status_short, phase
            FROM ops.job_b_fixture_watch
            WHERE league_id = %s AND date_local = %s AND is_terminal = false
            ORDER BY utc_kickoff
            """,
            (league_id, date_local),
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_newly_terminal(conn, league_id: int, date_local: date) -> list[int]:
    """
    Return fixture IDs that are terminal but haven't been processed by Job C yet.
    Uses public.fixtures.detail_ingested_at IS NULL as the guard.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT w.fixture_id
            FROM ops.job_b_fixture_watch w
            JOIN fixtures f ON f.id = w.fixture_id
            WHERE w.league_id = %s
              AND w.date_local = %s
              AND w.is_terminal = true
              AND f.detail_ingested_at IS NULL
            ORDER BY w.fixture_id
            """,
            (league_id, date_local),
        )
        return [row[0] for row in cur.fetchall()]


def update_fixture_watch_batch(
    conn,
    fixture_statuses: list[dict[str, Any]],
) -> dict[str, int]:
    """
    After a Job B tick, update ops.job_b_fixture_watch with the per-fixture
    statuses returned by the edge function.

    `fixture_statuses`: [{"fixture_id": 12345, "status_short": "2H"}, ...]

    Returns counts: {"updated", "newly_live", "newly_terminal"}
    """
    now = datetime.now(timezone.utc)
    counts = {"updated": 0, "newly_live": 0, "newly_terminal": 0}

    with conn.cursor() as cur:
        for fs in fixture_statuses:
            fid = fs["fixture_id"]
            status = fs["status_short"]
            phase = _classify_phase(status)
            is_term = status in TERMINAL_STATUSES

            cur.execute(
                """
                UPDATE ops.job_b_fixture_watch SET
                    status_short = %s,
                    phase        = %s,
                    is_terminal  = %s,
                    terminal_at  = CASE WHEN %s AND terminal_at IS NULL THEN %s ELSE terminal_at END,
                    last_tick_at = %s,
                    last_error   = NULL
                WHERE fixture_id = %s
                """,
                (status, phase, is_term, is_term, now, now, fid),
            )
            counts["updated"] += cur.rowcount

            if phase == "live":
                counts["newly_live"] += 1
            if is_term:
                counts["newly_terminal"] += 1

    conn.commit()
    return counts


def set_fixture_watch_error(conn, fixture_id: int, error: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE ops.job_b_fixture_watch SET last_error = %s WHERE fixture_id = %s",
            (error, fixture_id),
        )
    conn.commit()


def all_fixtures_terminal(conn, league_id: int, date_local: date) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FILTER (WHERE is_terminal = false)
            FROM ops.job_b_fixture_watch
            WHERE league_id = %s AND date_local = %s
            """,
            (league_id, date_local),
        )
        return cur.fetchone()[0] == 0


def cleanup_old_watch_rows(conn, keep_days: int = 7) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM ops.job_b_fixture_watch WHERE date_local < CURRENT_DATE - %s::int",
            (keep_days,),
        )
        deleted = cur.rowcount
    conn.commit()
    if deleted:
        log.info("Cleaned up %d old fixture_watch rows.", deleted)
    return deleted


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _classify_phase(status_short: str) -> str:
    if status_short in LIVE_STATUSES:
        return "live"
    if status_short in TERMINAL_STATUSES:
        return "terminal"
    return "scheduled"
