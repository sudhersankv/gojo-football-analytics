"""
Railway worker entry point — Gojo Job-B orchestrator.

Architecture
============
Two threads run concurrently inside a single Python process:

  1. **Planner thread** — runs once on startup, then re-plans daily at
     PLANNER_HOUR_LOCAL in the league timezone.  For each configured
     league, writes the day plan (active/inactive + window) to
     ops.job_b_day_plan and seeds one ops.job_b_fixture_watch row
     per fixture.

  2. **Tick thread** — wakes every TICK_INTERVAL_SEC (default 30s).
     For each league with an active day plan whose window covers "now":
       a. Load non-terminal fixtures from ops.job_b_fixture_watch.
       b. Send all IDs as one batch to the job_b_live edge function.
       c. Update each fixture's watch row with the returned status.
       d. For newly-terminal fixtures, trigger Job C (placeholder).
       e. When all fixtures are terminal, mark the day plan "ended."

Key design:
  - Single tick loop, fixed cadence (30s default).
  - Batched work: one edge invocation per tick processes ALL active
    fixtures regardless of count.
  - Per-fixture terminal tracking: finished matches drop out of
    future ticks automatically.
  - Multi-league: the planner and tick loop iterate over LEAGUE_IDS.
  - Worker owns ops state; edge functions are stateless workers.

Usage
=====
  python -m worker

  Environment variables are documented in worker.config.
"""

from __future__ import annotations

import logging
import signal
import sys
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from . import config, db, edge_client
from .planner import plan_all_leagues

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("worker.main")

shutdown_event = threading.Event()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _today_local():
    tz = ZoneInfo(config.LEAGUE_TZ)
    return datetime.now(tz).date()


def _sleep_or_shutdown(seconds: float) -> bool:
    """Sleep for `seconds`, wake early on shutdown. Returns True if shutting down."""
    return shutdown_event.wait(timeout=seconds)


def _ensure_tz(dt: datetime | None) -> datetime | None:
    if dt is not None and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Job C trigger
# ---------------------------------------------------------------------------

def trigger_job_c(fixture_ids: list[int]) -> None:
    """
    Called when fixtures transition to terminal.  Invokes the job_c
    edge function for full post-match detail ingestion (events,
    lineups, player stats, team stats → Postgres).
    """
    if not fixture_ids:
        return
    log.info("Job C trigger: %d fixtures ready for detail ingestion: %s", len(fixture_ids), fixture_ids)

    result = edge_client.invoke_job_c(fixture_ids)
    if result.get("ok"):
        results = result.get("results", [])
        ok_count = sum(1 for r in results if r.get("status") == "ok")
        err_count = sum(1 for r in results if r.get("status") == "error")
        log.info("Job C finished: %d ok, %d errors", ok_count, err_count)
        for r in results:
            if r.get("status") == "error":
                log.warning("  fixture %s failed: %s", r.get("fixture_id"), r.get("error", "?"))
    else:
        log.error("Job C invocation failed: %s", result.get("error", "unknown"))


# ---------------------------------------------------------------------------
# Planner thread
# ---------------------------------------------------------------------------

def planner_loop(conn_factory):
    """Run the planner once immediately, then once per day at PLANNER_HOUR_LOCAL."""
    log.info("Planner thread started.")

    conn = conn_factory()
    try:
        plan_all_leagues(conn)
    except Exception:
        log.exception("Initial planning failed — will retry next cycle.")
    finally:
        conn.close()

    while not shutdown_event.is_set():
        tz = ZoneInfo(config.LEAGUE_TZ)
        now_local = datetime.now(tz)
        target = now_local.replace(
            hour=config.PLANNER_HOUR_LOCAL, minute=0, second=0, microsecond=0,
        )
        if target <= now_local:
            target += timedelta(days=1)

        wait_seconds = (target - now_local).total_seconds()
        log.info("Planner sleeping until %s (%d sec)", target.isoformat(), int(wait_seconds))

        if _sleep_or_shutdown(wait_seconds):
            break

        conn = conn_factory()
        try:
            plan_all_leagues(conn)
        except Exception:
            log.exception("Daily planning failed — will retry tomorrow.")
        finally:
            conn.close()

    log.info("Planner thread exiting.")


# ---------------------------------------------------------------------------
# Tick thread (single loop — replaces old poller + live threads)
# ---------------------------------------------------------------------------

def tick_loop(conn_factory):
    """
    Every TICK_INTERVAL_SEC, for each league with an active window:
      1. Load non-terminal fixtures from ops.job_b_fixture_watch.
      2. If none remain → mark plan ended, skip.
      3. Send all IDs as one batch to job_b_live edge function.
      4. Update each fixture's watch row with the returned status.
      5. For newly-terminal fixtures, trigger Job C.
      6. If all fixtures are now terminal → mark plan ended.
    """
    log.info("Tick thread started (interval=%ds).", config.TICK_INTERVAL_SEC)

    while not shutdown_event.is_set():
        if _sleep_or_shutdown(config.TICK_INTERVAL_SEC):
            break

        now = _now_utc()
        today = _today_local()
        conn = conn_factory()
        try:
            for league_id in config.LEAGUE_IDS:
                _tick_league(conn, league_id, today, now)
        except Exception:
            log.exception("Tick failed.")
        finally:
            conn.close()

    log.info("Tick thread exiting.")


def _tick_league(conn, league_id: int, today, now: datetime) -> None:
    """Process one tick for a single league."""

    plan = db.get_day_plan(conn, league_id, config.SEASON_YEAR, today)
    if not plan or plan["status"] != "active":
        return

    start = _ensure_tz(plan["window_start_utc"])
    end = _ensure_tz(plan["window_end_utc"])
    if not (start and end):
        return

    if not (start <= now <= end):
        log.debug("League %d: outside window (%s → %s) — skip.", league_id, start, end)
        return

    active = db.get_active_fixtures(conn, league_id, today)
    if not active:
        log.info("League %d: all fixtures terminal — marking plan ended.", league_id)
        db.mark_plan_ended(conn, league_id, config.SEASON_YEAR, today)
        return

    fixture_ids = [f["fixture_id"] for f in active]
    log.info("League %d: %d active fixtures — invoking job_b_live.", league_id, len(fixture_ids))

    result = edge_client.invoke_job_b(fixture_ids)

    if not result.get("ok"):
        log.warning("League %d: job_b_live failed: %s", league_id, result.get("error", "?"))
        return

    statuses = result.get("fixture_statuses", [])
    if statuses:
        counts = db.update_fixture_watch_batch(conn, statuses)
        log.info(
            "League %d: updated %d watch rows (live=%d, terminal=%d).",
            league_id, counts["updated"], counts["newly_live"], counts["newly_terminal"],
        )

    newly_terminal = db.get_newly_terminal(conn, league_id, today)
    if newly_terminal:
        trigger_job_c(newly_terminal)

    if db.all_fixtures_terminal(conn, league_id, today):
        log.info("League %d: all fixtures terminal — marking plan ended.", league_id)
        db.mark_plan_ended(conn, league_id, config.SEASON_YEAR, today)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main():
    log.info("=" * 60)
    log.info("Gojo Railway Worker starting")
    log.info("  Leagues: %s  Season: %d  TZ: %s", config.LEAGUE_IDS, config.SEASON_YEAR, config.LEAGUE_TZ)
    log.info("  Tick interval: %ds", config.TICK_INTERVAL_SEC)
    log.info("  Window: kickoff -%d min → +%d min", config.PRE_KICKOFF_BUFFER_MIN, config.POST_MATCH_BUFFER_MIN)
    log.info("  Planner runs daily at %02d:00 %s", config.PLANNER_HOUR_LOCAL, config.LEAGUE_TZ)
    log.info("  Dry run: %s", config.DRY_RUN)
    log.info("=" * 60)

    def conn_factory():
        return db.get_connection(config.SUPABASE_DB_URL)

    try:
        test_conn = conn_factory()
        test_conn.close()
        log.info("DB connection verified.")
    except Exception:
        log.exception("Cannot connect to DB — aborting.")
        sys.exit(1)

    def handle_signal(signum, frame):
        sig_name = signal.Signals(signum).name
        log.info("Received %s — shutting down gracefully...", sig_name)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    threads = [
        threading.Thread(target=planner_loop, args=(conn_factory,), name="planner", daemon=True),
        threading.Thread(target=tick_loop, args=(conn_factory,), name="tick", daemon=True),
    ]

    for t in threads:
        t.start()
        log.info("Started thread: %s", t.name)

    try:
        while not shutdown_event.is_set():
            shutdown_event.wait(timeout=5)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — shutting down.")
        shutdown_event.set()

    for t in threads:
        t.join(timeout=10)

    log.info("Worker stopped.")


if __name__ == "__main__":
    main()
