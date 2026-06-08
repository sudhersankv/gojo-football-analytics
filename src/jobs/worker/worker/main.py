"""
Railway worker entry point — Gojo Job-B orchestrator.

Architecture
============
Two threads run concurrently inside a single Python process:

  1. **Planner thread** — runs once on startup (with retry), then re-plans
     daily at PLANNER_HOUR_LOCAL in the league timezone.  For each configured
     league, writes the day plan (active/inactive + window) to
     ops.job_b_day_plan, seeds fixture_watch rows, and populates a shared
     in-memory cache so the tick thread can act without querying the DB.

  2. **Tick thread** — sleeps until the earliest active window opens, then
     ticks every TICK_INTERVAL_SEC (default 30s) while inside the window:
       a. Load non-terminal fixtures from ops.job_b_fixture_watch.
       b. Send all IDs as one batch to the job_b_live edge function.
       c. Update each fixture's watch row with the returned status.
       d. For newly-terminal fixtures, trigger Job C.
       e. When all fixtures are terminal, mark the day plan "ended."
     Outside any active window the tick thread sleeps (zero DB queries).

Key design:
  - Plan cache: planner writes, tick reads — no DB polling for plan/window.
  - Planner retries on startup failure with exponential backoff.
  - Tick thread wakes via threading.Event when cache is updated or on shutdown.
  - Batched work: one edge invocation per tick processes ALL active fixtures.
  - Per-fixture terminal tracking: finished matches drop out automatically.
  - Multi-league: planner and tick loop iterate over LEAGUE_IDS.

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

# ── Shared plan cache ─────────────────────────────────────────────────────
# Populated by the planner thread; read by the tick thread.
# { league_id: { "status": "active"|"inactive"|"ended",
#                "window_start": datetime|None, "window_end": datetime|None } }
_plan_cache: dict[int, dict] = {}
_cache_lock = threading.Lock()
_tick_wake = threading.Event()

# Planner retry backoff: 30s, 60s, 120s, then 300s indefinitely
_RETRY_DELAYS = [30, 60, 120, 300]


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


# ── Cache helpers ─────────────────────────────────────────────────────────

def _update_cache(plans: list[dict]) -> None:
    """Atomically refresh the plan cache after a successful planner run."""
    active_ids = {p["league_id"] for p in plans}
    with _cache_lock:
        for league_id in config.LEAGUE_IDS:
            if league_id in active_ids:
                p = next(x for x in plans if x["league_id"] == league_id)
                _plan_cache[league_id] = {
                    "status": "active",
                    "window_start": _ensure_tz(p["window_start_utc"]),
                    "window_end": _ensure_tz(p["window_end_utc"]),
                }
            else:
                _plan_cache[league_id] = {
                    "status": "inactive",
                    "window_start": None,
                    "window_end": None,
                }
    _tick_wake.set()


def _mark_league_ended(league_id: int) -> None:
    """Mark a league's cached plan as ended (called by tick thread)."""
    with _cache_lock:
        if league_id in _plan_cache:
            _plan_cache[league_id]["status"] = "ended"


def _get_cached_plan(league_id: int) -> dict | None:
    """Thread-safe snapshot of a league's cached plan."""
    with _cache_lock:
        plan = _plan_cache.get(league_id)
        return dict(plan) if plan else None


def _compute_sleep() -> float:
    """
    Determine how long the tick thread should sleep.
    Returns 0 if at least one league is inside its active window (tick now).
    Returns seconds-until-earliest-window if a window is upcoming.
    Returns a large value if no active plans exist.
    """
    now = _now_utc()
    min_wait: float | None = None

    with _cache_lock:
        for league_id in config.LEAGUE_IDS:
            plan = _plan_cache.get(league_id)
            if not plan or plan["status"] != "active":
                continue
            ws = plan.get("window_start")
            we = plan.get("window_end")
            if not ws or not we:
                continue
            if ws <= now <= we:
                return 0.0
            if now < ws:
                wait = (ws - now).total_seconds()
                if min_wait is None or wait < min_wait:
                    min_wait = wait

    return min_wait if min_wait is not None else 86400.0


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

def _run_planner_with_retry(conn_factory) -> None:
    """Run the planner, retrying with exponential backoff until success or shutdown."""
    attempt = 0
    while not shutdown_event.is_set():
        conn = conn_factory()
        try:
            plans = plan_all_leagues(conn)
            _update_cache(plans)
            if attempt > 0:
                log.info("Planner succeeded on retry %d. Active plans: %d", attempt, len(plans))
            else:
                log.info("Planner succeeded. Active plans: %d", len(plans))
            return
        except Exception:
            delay = _RETRY_DELAYS[min(attempt, len(_RETRY_DELAYS) - 1)]
            log.exception("Planner attempt %d failed — retrying in %ds.", attempt + 1, delay)
            attempt += 1
        finally:
            conn.close()

        if _sleep_or_shutdown(delay):
            return


def planner_loop(conn_factory):
    """Run the planner once immediately (with retry), then daily at PLANNER_HOUR_LOCAL."""
    log.info("Planner thread started.")

    _run_planner_with_retry(conn_factory)

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

        _run_planner_with_retry(conn_factory)

    log.info("Planner thread exiting.")


# ---------------------------------------------------------------------------
# Tick thread
# ---------------------------------------------------------------------------

def tick_loop(conn_factory):
    """
    Sleeps until an active window opens, then ticks every TICK_INTERVAL_SEC.
    Outside any window: zero DB queries — sleeps on _tick_wake event.
    Inside a window: queries fixture_watch and invokes job_b_live per tick.
    """
    log.info("Tick thread started (interval=%ds).", config.TICK_INTERVAL_SEC)

    while not shutdown_event.is_set():
        sleep_sec = _compute_sleep()

        if sleep_sec > 0:
            if sleep_sec > config.TICK_INTERVAL_SEC:
                log.info("Tick: sleeping %d sec until window opens.", int(sleep_sec))
            _tick_wake.clear()
            # Double-check after clear to catch cache updates that landed
            # between the first _compute_sleep and _tick_wake.clear().
            sleep_sec = _compute_sleep()
            if sleep_sec > 0:
                _tick_wake.wait(timeout=sleep_sec)
                if shutdown_event.is_set():
                    break
            continue

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

        _tick_wake.clear()
        _tick_wake.wait(timeout=config.TICK_INTERVAL_SEC)
        if shutdown_event.is_set():
            break

    log.info("Tick thread exiting.")


def _tick_league(conn, league_id: int, today, now: datetime) -> None:
    """Process one tick for a single league (reads plan from cache, not DB)."""

    plan = _get_cached_plan(league_id)
    if not plan or plan["status"] != "active":
        return

    ws = plan.get("window_start")
    we = plan.get("window_end")
    if not ws or not we:
        return

    if not (ws <= now <= we):
        return

    active = db.get_active_fixtures(conn, league_id, today)
    if not active:
        log.info("League %d: all fixtures terminal — marking plan ended.", league_id)
        db.mark_plan_ended(conn, league_id, config.SEASON_YEAR, today)
        _mark_league_ended(league_id)
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
        _mark_league_ended(league_id)


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
        _tick_wake.set()

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
        _tick_wake.set()

    for t in threads:
        t.join(timeout=10)

    log.info("Worker stopped.")


if __name__ == "__main__":
    main()
