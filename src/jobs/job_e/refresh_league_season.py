"""
Job E — Conditional refresh / DB sync for a league-season.

Unlike Job A (bootstrap from empty), Job E assumes the database is already
populated and performs a **conditional refresh**: it re-fetches data from
API-Football and applies updates only where column values have actually
changed.  This keeps `updated_at` accurate and avoids unnecessary write
amplification (WAL, triggers, index churn).

Core guarantee:
  - `created_at` is NEVER overwritten  (handled by DB default + trigger).
  - `detail_ingested_at`, `detail_ingest_attempts`, `detail_ingest_last_error`
    on fixtures are NEVER touched by the core refresh.  They are only updated
    when `--refresh-detail` is explicitly passed.
  - Every upsert uses a WHERE … IS DISTINCT FROM guard so the UPDATE fires
    only when at least one tracked column differs from the incoming value.
  - The `updated_at` trigger (`gojo_set_updated_at`) fires only on rows that
    are genuinely modified.

Phases (same logical order as Job A, same API endpoints):

  Phase 1 — Reference entities
    leagues, seasons, teams, league_season_teams

  Phase 2 — Players & squads
    players, squad_players, player_season_statistics

  Phase 3 — Fixtures (schedule + results)
    fixtures  (core columns only; detail markers left untouched)

  Phase 4 — Post-match detail  [only with --refresh-detail]
    fixture_events, fixture_team_statistics,
    fixture_team_lineups, fixture_lineup_players,
    player_fixture_statistics
    Updates detail_ingested_at on success.

  Phase 5 — Season aggregates
    standings

Flags:
  --league          League ID (default: 39 / Premier League)
  --season          API season year (default: 2025)
  --refresh-detail  Also re-ingest post-match detail tables (expensive)
  --skip-fixture-player-stats  Skip per-fixture /fixtures/players calls
  --sleep           Seconds between API calls (default: 0.15)

Requires env vars (in src/.env or .env):
  FOOTBALL_API_KEY  — API-Football key
  SUPABASE_DB_URL   — Postgres connection string

Run from repo root:
  python src/jobs/job_e/refresh_league_season.py --league 39 --season 2025
  python src/jobs/job_e/refresh_league_season.py --league 140 --season 2025
  python src/jobs/job_e/refresh_league_season.py --league 39 --season 2025 --refresh-detail
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

import psycopg2
import requests
from psycopg2.extras import Json, execute_values

# ---------------------------------------------------------------------------
# Resolve repo root so we can import env_loader from src/
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
from env_loader import load_repo_dotenv  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://v3.football.api-sports.io"
DEFAULT_LEAGUE_ID = 39
DEFAULT_SEASON_YEAR = 2025
FIXTURE_IDS_CHUNK = 20

TERMINAL_STATUSES = {"FT", "AET", "PEN", "AWD"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("job_e")

# ---------------------------------------------------------------------------
# Helpers  (shared utilities, same as Job A)
# ---------------------------------------------------------------------------


def _ensure_env(var: str) -> str:
    v = os.environ.get(var)
    if not v:
        raise SystemExit(f"Missing required env var: {var}")
    return v


def _parse_dt(value: str | None) -> datetime:
    if not value:
        raise ValueError("missing fixture date")
    return datetime.fromisoformat(value)


def _as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _stat_value_to_text(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _chunks(lst: list, n: int) -> list[list]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def _payload_league_id(payload: dict[str, Any]) -> int | None:
    lg = payload.get("league") or {}
    lid = lg.get("id")
    if lid is None:
        return None
    try:
        return int(lid)
    except (TypeError, ValueError):
        return None


def _is_target_league(payload: dict[str, Any], league_id: int) -> bool:
    lid = _payload_league_id(payload)
    if lid is None:
        return True
    return lid == int(league_id)


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


def api_get(url: str, headers: dict[str, str], sleep: float = 0.0) -> dict[str, Any]:
    resp = requests.get(url, headers=headers, timeout=90)
    resp.raise_for_status()
    data = resp.json()
    errors = data.get("errors")
    if errors:
        log.warning("API returned errors for %s: %s", url, errors)
    if sleep > 0:
        time.sleep(sleep)
    return data


# ---------------------------------------------------------------------------
# ConditionalWriter
#
# Every upsert follows the pattern:
#
#   INSERT INTO … VALUES (…)
#   ON CONFLICT (pk) DO UPDATE SET col = EXCLUDED.col, …
#   WHERE table.col1 IS DISTINCT FROM EXCLUDED.col1
#      OR table.col2 IS DISTINCT FROM EXCLUDED.col2
#      …
#
# If every tracked column is identical, no UPDATE is executed:
#   - updated_at trigger does NOT fire
#   - no WAL write for that row
#   - created_at is never mentioned → always preserved
#   - detail_ingested_at / attempts / last_error are never in the SET clause
#     for the core fixtures upsert → always preserved
# ---------------------------------------------------------------------------


class ConditionalWriter:
    """
    DB writer for Job E.  All upserts are conditional: the UPDATE fires only
    when at least one business column has actually changed.
    """

    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self.conn = conn

    def commit(self) -> None:
        self.conn.commit()

    # ── Phase 1: Reference entities ────────────────────────────────────────

    def upsert_league(self, league: dict[str, Any]) -> None:
        """
        Conditional upsert for leagues.
        Tracked columns: name, type, country_name, country_code, logo_url.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO leagues (id, name, type, country_name, country_code, logo_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                  name         = EXCLUDED.name,
                  type         = EXCLUDED.type,
                  country_name = EXCLUDED.country_name,
                  country_code = EXCLUDED.country_code,
                  logo_url     = EXCLUDED.logo_url
                WHERE
                  leagues.name         IS DISTINCT FROM EXCLUDED.name
                  OR leagues.type      IS DISTINCT FROM EXCLUDED.type
                  OR leagues.country_name IS DISTINCT FROM EXCLUDED.country_name
                  OR leagues.country_code IS DISTINCT FROM EXCLUDED.country_code
                  OR leagues.logo_url  IS DISTINCT FROM EXCLUDED.logo_url
                """,
                (
                    league.get("id"),
                    league.get("name"),
                    league.get("type"),
                    (league.get("country") or {}).get("name"),
                    (league.get("country") or {}).get("code"),
                    league.get("logo"),
                ),
            )

    def upsert_season(self, league_id: int, season: dict[str, Any]) -> int:
        """
        Conditional upsert for seasons.
        Returns the DB-generated season_id (serial PK).
        Tracked columns: start_date, end_date, current.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO seasons (league_id, year, start_date, end_date, current)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (league_id, year) DO UPDATE SET
                  start_date = EXCLUDED.start_date,
                  end_date   = EXCLUDED.end_date,
                  current    = EXCLUDED.current
                WHERE
                  seasons.start_date IS DISTINCT FROM EXCLUDED.start_date
                  OR seasons.end_date IS DISTINCT FROM EXCLUDED.end_date
                  OR seasons.current  IS DISTINCT FROM EXCLUDED.current
                """,
                (
                    league_id,
                    season.get("year"),
                    season.get("start"),
                    season.get("end"),
                    season.get("current", False),
                ),
            )
            cur.execute(
                "SELECT id FROM seasons WHERE league_id=%s AND year=%s",
                (league_id, season.get("year")),
            )
            row = cur.fetchone()
            if not row:
                raise RuntimeError("season_id not found after upsert")
            return int(row[0])

    def upsert_team_full(self, team: dict[str, Any], venue: dict[str, Any] | None) -> None:
        """
        Conditional upsert for teams (full data from /teams endpoint).
        Tracked columns: name, code, country, founded, national, logo_url, venue.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO teams (id, name, code, country, founded, national, logo_url, venue)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                  name     = EXCLUDED.name,
                  code     = EXCLUDED.code,
                  country  = EXCLUDED.country,
                  founded  = EXCLUDED.founded,
                  national = EXCLUDED.national,
                  logo_url = EXCLUDED.logo_url,
                  venue    = EXCLUDED.venue
                WHERE
                  teams.name     IS DISTINCT FROM EXCLUDED.name
                  OR teams.code  IS DISTINCT FROM EXCLUDED.code
                  OR teams.country IS DISTINCT FROM EXCLUDED.country
                  OR teams.founded IS DISTINCT FROM EXCLUDED.founded
                  OR teams.national IS DISTINCT FROM EXCLUDED.national
                  OR teams.logo_url IS DISTINCT FROM EXCLUDED.logo_url
                  OR teams.venue IS DISTINCT FROM EXCLUDED.venue
                """,
                (
                    team.get("id"),
                    team.get("name"),
                    team.get("code"),
                    team.get("country"),
                    team.get("founded"),
                    team.get("national", False),
                    team.get("logo"),
                    Json(venue) if venue else None,
                ),
            )

    def upsert_team_minimal(self, team: dict[str, Any]) -> None:
        """
        Conditional upsert for teams with minimal fields (from fixture payloads).
        Uses COALESCE so a minimal payload never overwrites richer /teams data.
        The WHERE guard checks if the COALESCE result would actually change the row.
        """
        tid = team.get("id")
        if not tid:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO teams (id, name, code, country, founded, national, logo_url, venue)
                VALUES (%s, %s, NULL, NULL, NULL, false, %s, NULL)
                ON CONFLICT (id) DO UPDATE SET
                  name     = COALESCE(EXCLUDED.name, teams.name),
                  logo_url = COALESCE(EXCLUDED.logo_url, teams.logo_url)
                WHERE
                  teams.name     IS DISTINCT FROM COALESCE(EXCLUDED.name, teams.name)
                  OR teams.logo_url IS DISTINCT FROM COALESCE(EXCLUDED.logo_url, teams.logo_url)
                """,
                (tid, team.get("name"), team.get("logo")),
            )

    def upsert_league_season_teams(self, season_id: int, team_ids: list[int]) -> None:
        """Junction table — insert-only, no update needed."""
        rows = [(season_id, tid) for tid in team_ids]
        if not rows:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO league_season_teams (season_id, team_id) VALUES %s ON CONFLICT DO NOTHING",
                rows,
                page_size=200,
            )

    # ── Phase 2: Players, squads, season stats ─────────────────────────────

    def upsert_players(self, players: list[dict[str, Any]]) -> None:
        """
        Conditional upsert for players.
        Tracked columns: name, firstname, lastname, birth_date, nationality, photo_url.
        """
        if not players:
            return
        rows = []
        for p in players:
            pid = p.get("id")
            if not pid:
                continue
            birth_date = None
            birth = p.get("birth")
            if isinstance(birth, dict):
                birth_date = birth.get("date")
            rows.append(
                (
                    pid,
                    p.get("name") or "?",
                    p.get("firstname"),
                    p.get("lastname"),
                    birth_date,
                    p.get("nationality"),
                    p.get("photo") or "",
                )
            )
        if not rows:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO players (id, name, firstname, lastname, birth_date, nationality, photo_url)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                  name        = EXCLUDED.name,
                  firstname   = EXCLUDED.firstname,
                  lastname    = EXCLUDED.lastname,
                  birth_date  = EXCLUDED.birth_date,
                  nationality = EXCLUDED.nationality,
                  photo_url   = EXCLUDED.photo_url
                WHERE
                  players.name        IS DISTINCT FROM EXCLUDED.name
                  OR players.firstname IS DISTINCT FROM EXCLUDED.firstname
                  OR players.lastname  IS DISTINCT FROM EXCLUDED.lastname
                  OR players.birth_date IS DISTINCT FROM EXCLUDED.birth_date
                  OR players.nationality IS DISTINCT FROM EXCLUDED.nationality
                  OR players.photo_url IS DISTINCT FROM EXCLUDED.photo_url
                """,
                rows,
                page_size=500,
            )

    def upsert_squad_players(self, season_id: int, player_team_pairs: list[tuple[int, int]]) -> None:
        """Squad membership — insert-only, no mutable columns to track."""
        if not player_team_pairs:
            return
        rows = [(season_id, tid, pid) for pid, tid in player_team_pairs]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO squad_players (season_id, team_id, player_id) VALUES %s ON CONFLICT DO NOTHING",
                rows,
                page_size=500,
            )

    def upsert_player_season_statistics(
        self, season_id: int, stat_rows: list[tuple[Any, ...]]
    ) -> None:
        """
        Conditional upsert for player_season_statistics.
        Tracked columns: position, rating, appearances, lineups, minutes,
                         number, goals, assists, extra.
        """
        if not stat_rows:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO player_season_statistics (
                    season_id, player_id, team_id, position, rating, appearances, lineups,
                    minutes, number, goals, assists, extra
                ) VALUES %s
                ON CONFLICT (season_id, player_id, team_id) DO UPDATE SET
                  position    = EXCLUDED.position,
                  rating      = EXCLUDED.rating,
                  appearances = EXCLUDED.appearances,
                  lineups     = EXCLUDED.lineups,
                  minutes     = EXCLUDED.minutes,
                  number      = EXCLUDED.number,
                  goals       = EXCLUDED.goals,
                  assists     = EXCLUDED.assists,
                  extra       = EXCLUDED.extra
                WHERE
                  player_season_statistics.position    IS DISTINCT FROM EXCLUDED.position
                  OR player_season_statistics.rating   IS DISTINCT FROM EXCLUDED.rating
                  OR player_season_statistics.appearances IS DISTINCT FROM EXCLUDED.appearances
                  OR player_season_statistics.lineups  IS DISTINCT FROM EXCLUDED.lineups
                  OR player_season_statistics.minutes  IS DISTINCT FROM EXCLUDED.minutes
                  OR player_season_statistics.number   IS DISTINCT FROM EXCLUDED.number
                  OR player_season_statistics.goals    IS DISTINCT FROM EXCLUDED.goals
                  OR player_season_statistics.assists  IS DISTINCT FROM EXCLUDED.assists
                  OR player_season_statistics.extra    IS DISTINCT FROM EXCLUDED.extra
                """,
                stat_rows,
                page_size=400,
            )

    # ── Phase 3: Fixtures ──────────────────────────────────────────────────

    def upsert_fixture(self, season_id: int, item: dict[str, Any]) -> int | None:
        """
        Conditional upsert for fixtures (core columns only).

        IMPORTANT: detail_ingested_at, detail_ingest_attempts, and
        detail_ingest_last_error are deliberately excluded from both the
        INSERT column list and the ON CONFLICT SET clause.  They are owned
        exclusively by the detail-ingestion path (Phase 4 / --refresh-detail).
        This means a core refresh can never accidentally reset or null out
        those markers.

        Tracked columns for the WHERE guard: status_short, home_goals,
        away_goals, utc_kickoff, elapsed, extra  (the columns most likely
        to change between refreshes — keeps the comparison cheap).
        """
        fx = item.get("fixture") or {}
        fid = fx.get("id")
        if not fid:
            return None

        league = item.get("league") or {}
        teams = item.get("teams") or {}
        goals = item.get("goals") or {}
        score = item.get("score") or {}
        status = fx.get("status") or {}
        venue = fx.get("venue") or {}
        ht = score.get("halftime") or {}
        ft = score.get("fulltime") or {}
        et = score.get("extratime") or {}
        pen = score.get("penalty") or {}
        home = teams.get("home") or {}
        away = teams.get("away") or {}

        row = (
            fid,
            season_id,
            home.get("id"),
            away.get("id"),
            _parse_dt(fx.get("date")),
            fx.get("timezone"),
            status.get("short"),
            league.get("round"),
            venue.get("id"),
            venue.get("name"),
            fx.get("referee"),
            goals.get("home"),
            goals.get("away"),
            ht.get("home"),
            ht.get("away"),
            ft.get("home"),
            ft.get("away"),
            et.get("home"),
            et.get("away"),
            pen.get("home"),
            pen.get("away"),
            home.get("winner"),
            away.get("winner"),
            status.get("elapsed"),
            Json(item),
        )
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO fixtures (
                    id, season_id, home_team_id, away_team_id, utc_kickoff, timezone,
                    status_short, round, venue_id, venue_name, referee,
                    home_goals, away_goals,
                    ht_home_goals, ht_away_goals, ft_home_goals, ft_away_goals,
                    et_home_goals, et_away_goals, pen_home_goals, pen_away_goals,
                    home_winner, away_winner, elapsed, extra
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT (id) DO UPDATE SET
                    season_id     = EXCLUDED.season_id,
                    home_team_id  = EXCLUDED.home_team_id,
                    away_team_id  = EXCLUDED.away_team_id,
                    utc_kickoff   = EXCLUDED.utc_kickoff,
                    timezone      = EXCLUDED.timezone,
                    status_short  = EXCLUDED.status_short,
                    round         = EXCLUDED.round,
                    venue_id      = EXCLUDED.venue_id,
                    venue_name    = EXCLUDED.venue_name,
                    referee       = EXCLUDED.referee,
                    home_goals    = EXCLUDED.home_goals,
                    away_goals    = EXCLUDED.away_goals,
                    ht_home_goals = EXCLUDED.ht_home_goals,
                    ht_away_goals = EXCLUDED.ht_away_goals,
                    ft_home_goals = EXCLUDED.ft_home_goals,
                    ft_away_goals = EXCLUDED.ft_away_goals,
                    et_home_goals = EXCLUDED.et_home_goals,
                    et_away_goals = EXCLUDED.et_away_goals,
                    pen_home_goals = EXCLUDED.pen_home_goals,
                    pen_away_goals = EXCLUDED.pen_away_goals,
                    home_winner   = EXCLUDED.home_winner,
                    away_winner   = EXCLUDED.away_winner,
                    elapsed       = EXCLUDED.elapsed,
                    extra         = EXCLUDED.extra
                WHERE
                    fixtures.status_short IS DISTINCT FROM EXCLUDED.status_short
                    OR fixtures.home_goals IS DISTINCT FROM EXCLUDED.home_goals
                    OR fixtures.away_goals IS DISTINCT FROM EXCLUDED.away_goals
                    OR fixtures.utc_kickoff IS DISTINCT FROM EXCLUDED.utc_kickoff
                    OR fixtures.elapsed    IS DISTINCT FROM EXCLUDED.elapsed
                    OR fixtures.referee    IS DISTINCT FROM EXCLUDED.referee
                    OR fixtures.home_winner IS DISTINCT FROM EXCLUDED.home_winner
                    OR fixtures.away_winner IS DISTINCT FROM EXCLUDED.away_winner
                    OR fixtures.ht_home_goals IS DISTINCT FROM EXCLUDED.ht_home_goals
                    OR fixtures.ht_away_goals IS DISTINCT FROM EXCLUDED.ht_away_goals
                    OR fixtures.ft_home_goals IS DISTINCT FROM EXCLUDED.ft_home_goals
                    OR fixtures.ft_away_goals IS DISTINCT FROM EXCLUDED.ft_away_goals
                    OR fixtures.extra IS DISTINCT FROM EXCLUDED.extra
                """,
                row,
            )
        return int(fid)

    # ── Phase 4: Post-match detail (opt-in only) ───────────────────────────

    def upsert_fixture_events(self, fixture_id: int, events: list[dict[str, Any]]) -> None:
        """
        Conditional upsert + orphan cleanup for fixture events.
        Tracked columns: team_id, minute, minute_extra, event_type, detail,
        comments, player_api_id, player_name, related_*, extra.
        """
        rows = []
        for i, ev in enumerate(events):
            team = ev.get("team") or {}
            tm = ev.get("time") or {}
            pl = ev.get("player") or {}
            rel = ev.get("assist") or {}
            tid = team.get("id")
            if not tid:
                continue
            rows.append(
                (
                    fixture_id, i, tid,
                    tm.get("elapsed") if tm.get("elapsed") is not None else 0,
                    tm.get("extra"),
                    ev.get("type") or "unknown",
                    ev.get("detail"),
                    ev.get("comments"),
                    pl.get("id"), pl.get("name"),
                    rel.get("id"), rel.get("name"),
                    Json(ev) if ev else None,
                )
            )
        if rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO fixture_events (
                        fixture_id, event_index, team_id, minute, minute_extra,
                        event_type, detail, comments,
                        player_api_id, player_name, related_player_api_id, related_player_name, extra
                    ) VALUES %s
                    ON CONFLICT (fixture_id, event_index) DO UPDATE SET
                      team_id       = EXCLUDED.team_id,
                      minute        = EXCLUDED.minute,
                      minute_extra  = EXCLUDED.minute_extra,
                      event_type    = EXCLUDED.event_type,
                      detail        = EXCLUDED.detail,
                      comments      = EXCLUDED.comments,
                      player_api_id = EXCLUDED.player_api_id,
                      player_name   = EXCLUDED.player_name,
                      related_player_api_id = EXCLUDED.related_player_api_id,
                      related_player_name   = EXCLUDED.related_player_name,
                      extra         = EXCLUDED.extra
                    WHERE
                      fixture_events.team_id       IS DISTINCT FROM EXCLUDED.team_id
                      OR fixture_events.minute     IS DISTINCT FROM EXCLUDED.minute
                      OR fixture_events.minute_extra IS DISTINCT FROM EXCLUDED.minute_extra
                      OR fixture_events.event_type IS DISTINCT FROM EXCLUDED.event_type
                      OR fixture_events.detail     IS DISTINCT FROM EXCLUDED.detail
                      OR fixture_events.comments   IS DISTINCT FROM EXCLUDED.comments
                      OR fixture_events.player_api_id IS DISTINCT FROM EXCLUDED.player_api_id
                      OR fixture_events.player_name IS DISTINCT FROM EXCLUDED.player_name
                      OR fixture_events.related_player_api_id IS DISTINCT FROM EXCLUDED.related_player_api_id
                      OR fixture_events.related_player_name IS DISTINCT FROM EXCLUDED.related_player_name
                      OR fixture_events.extra      IS DISTINCT FROM EXCLUDED.extra
                    """,
                    rows,
                    page_size=500,
                )
        # Orphan cleanup: remove events not in the incoming set
        valid_indices = [row[1] for row in rows]
        with self.conn.cursor() as cur:
            if valid_indices:
                cur.execute(
                    "DELETE FROM fixture_events WHERE fixture_id = %s AND NOT (event_index = ANY(%s))",
                    (fixture_id, valid_indices),
                )
            else:
                cur.execute("DELETE FROM fixture_events WHERE fixture_id = %s", (fixture_id,))

    def upsert_fixture_team_statistics(
        self, fixture_id: int, statistics: list[dict[str, Any]]
    ) -> None:
        """
        Conditional upsert for team-level match statistics.
        Tracked columns: stat_value.
        """
        rows = []
        for block in statistics:
            team = block.get("team") or {}
            tid = team.get("id")
            if not tid:
                continue
            for st in block.get("statistics") or []:
                rows.append((fixture_id, tid, st.get("type") or "", _stat_value_to_text(st.get("value"))))
        if rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO fixture_team_statistics (fixture_id, team_id, stat_type, stat_value)
                    VALUES %s
                    ON CONFLICT (fixture_id, team_id, stat_type) DO UPDATE SET
                      stat_value = EXCLUDED.stat_value
                    WHERE fixture_team_statistics.stat_value IS DISTINCT FROM EXCLUDED.stat_value
                    """,
                    rows,
                    page_size=500,
                )

    def upsert_lineups(self, fixture_id: int, lineups: list[dict[str, Any]]) -> None:
        """
        Conditional upsert for lineup data
        (fixture_team_lineups and fixture_lineup_players).
        """
        meta_rows: list[tuple[Any, ...]] = []
        player_rows: list[tuple[Any, ...]] = []

        for block in lineups:
            team = block.get("team") or {}
            tid = team.get("id")
            if not tid:
                continue
            coach = block.get("coach") or {}
            meta_rows.append(
                (fixture_id, tid, block.get("formation"),
                 coach.get("id"), coach.get("name"), coach.get("photo"),
                 Json(team.get("colors")))
            )
            for slot in block.get("startXI") or []:
                p = slot.get("player") or {}
                pid = p.get("id")
                if not pid:
                    continue
                player_rows.append(
                    (fixture_id, tid, pid, p.get("name"), True,
                     p.get("number"), p.get("pos"), p.get("grid"), None)
                )
            for slot in block.get("substitutes") or []:
                p = slot.get("player") or {}
                pid = p.get("id")
                if not pid:
                    continue
                player_rows.append(
                    (fixture_id, tid, pid, p.get("name"), False,
                     p.get("number"), p.get("pos"), p.get("grid"), None)
                )

        if meta_rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO fixture_team_lineups (
                        fixture_id, team_id, formation, coach_api_id, coach_name,
                        coach_photo_url, colors
                    ) VALUES %s
                    ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                      formation      = EXCLUDED.formation,
                      coach_api_id   = EXCLUDED.coach_api_id,
                      coach_name     = EXCLUDED.coach_name,
                      coach_photo_url = EXCLUDED.coach_photo_url,
                      colors         = EXCLUDED.colors
                    WHERE
                      fixture_team_lineups.formation      IS DISTINCT FROM EXCLUDED.formation
                      OR fixture_team_lineups.coach_api_id IS DISTINCT FROM EXCLUDED.coach_api_id
                      OR fixture_team_lineups.coach_name  IS DISTINCT FROM EXCLUDED.coach_name
                      OR fixture_team_lineups.colors      IS DISTINCT FROM EXCLUDED.colors
                    """,
                    meta_rows,
                    page_size=50,
                )

        if player_rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO fixture_lineup_players (
                        fixture_id, team_id, player_api_id, player_name, is_starter,
                        shirt_number, position_short, grid, player_id
                    ) VALUES %s
                    ON CONFLICT (fixture_id, team_id, player_api_id) DO UPDATE SET
                      player_name    = EXCLUDED.player_name,
                      is_starter     = EXCLUDED.is_starter,
                      shirt_number   = EXCLUDED.shirt_number,
                      position_short = EXCLUDED.position_short,
                      grid           = EXCLUDED.grid
                    WHERE
                      fixture_lineup_players.player_name    IS DISTINCT FROM EXCLUDED.player_name
                      OR fixture_lineup_players.is_starter  IS DISTINCT FROM EXCLUDED.is_starter
                      OR fixture_lineup_players.shirt_number IS DISTINCT FROM EXCLUDED.shirt_number
                      OR fixture_lineup_players.position_short IS DISTINCT FROM EXCLUDED.position_short
                      OR fixture_lineup_players.grid         IS DISTINCT FROM EXCLUDED.grid
                    """,
                    player_rows,
                    page_size=200,
                )

    def upsert_player_fixture_statistics(self, fixture_id: int, rows: list[tuple[Any, ...]]) -> None:
        """
        Conditional upsert for per-player per-fixture statistics.
        Tracked columns: all stat fields (position through extra).
        """
        if rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO player_fixture_statistics (
                        fixture_id, season_id, player_id, team_id, position, rating, minutes, number,
                        starter, substitute, goals, assists, shots_total, shots_on,
                        passes_total, passes_key, passes_acc, tackles_total, interceptions,
                        yellow_cards, red_cards, extra
                    ) VALUES %s
                    ON CONFLICT (fixture_id, player_id, team_id) DO UPDATE SET
                      position      = EXCLUDED.position,
                      rating        = EXCLUDED.rating,
                      minutes       = EXCLUDED.minutes,
                      number        = EXCLUDED.number,
                      starter       = EXCLUDED.starter,
                      substitute    = EXCLUDED.substitute,
                      goals         = EXCLUDED.goals,
                      assists       = EXCLUDED.assists,
                      shots_total   = EXCLUDED.shots_total,
                      shots_on      = EXCLUDED.shots_on,
                      passes_total  = EXCLUDED.passes_total,
                      passes_key    = EXCLUDED.passes_key,
                      passes_acc    = EXCLUDED.passes_acc,
                      tackles_total = EXCLUDED.tackles_total,
                      interceptions = EXCLUDED.interceptions,
                      yellow_cards  = EXCLUDED.yellow_cards,
                      red_cards     = EXCLUDED.red_cards,
                      extra         = EXCLUDED.extra
                    WHERE
                      player_fixture_statistics.rating   IS DISTINCT FROM EXCLUDED.rating
                      OR player_fixture_statistics.goals IS DISTINCT FROM EXCLUDED.goals
                      OR player_fixture_statistics.assists IS DISTINCT FROM EXCLUDED.assists
                      OR player_fixture_statistics.minutes IS DISTINCT FROM EXCLUDED.minutes
                      OR player_fixture_statistics.shots_total IS DISTINCT FROM EXCLUDED.shots_total
                      OR player_fixture_statistics.passes_total IS DISTINCT FROM EXCLUDED.passes_total
                      OR player_fixture_statistics.tackles_total IS DISTINCT FROM EXCLUDED.tackles_total
                      OR player_fixture_statistics.yellow_cards IS DISTINCT FROM EXCLUDED.yellow_cards
                      OR player_fixture_statistics.red_cards IS DISTINCT FROM EXCLUDED.red_cards
                      OR player_fixture_statistics.extra IS DISTINCT FROM EXCLUDED.extra
                    """,
                    rows,
                    page_size=400,
                )

    def mark_fixture_detail_done(self, fixture_id: int) -> None:
        """
        Mark a fixture as having its detail successfully refreshed.
        Only called when --refresh-detail is active.
        Moves detail_ingested_at forward to now(); never clears it.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE fixtures
                SET detail_ingested_at     = now(),
                    detail_ingest_attempts = 0,
                    detail_ingest_last_error = NULL
                WHERE id = %s
                """,
                (fixture_id,),
            )

    def mark_fixture_detail_error(self, fixture_id: int, error_msg: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE fixtures
                SET detail_ingest_attempts   = detail_ingest_attempts + 1,
                    detail_ingest_last_error = %s
                WHERE id = %s
                """,
                (error_msg[:500], fixture_id),
            )

    # ── Phase 5: Standings ─────────────────────────────────────────────────

    def upsert_standings(self, season_id: int, rows_data: list[dict[str, Any]]) -> None:
        """
        Conditional upsert for standings.
        Tracked columns: rank, points, goals_diff, form, all_played, goals_for, goals_against.
        """
        rows: list[tuple[Any, ...]] = []
        for r in rows_data:
            team = r.get("team") or {}
            all_ = r.get("all") or {}
            goals = all_.get("goals") or {}
            rows.append(
                (
                    season_id,
                    team.get("id"),
                    r.get("rank"),
                    r.get("points"),
                    r.get("goalsDiff"),
                    r.get("form"),
                    r.get("description"),
                    all_.get("played"),
                    all_.get("win"),
                    all_.get("draw"),
                    all_.get("lose"),
                    goals.get("for"),
                    goals.get("against"),
                )
            )
        if not rows:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO standings (
                    season_id, team_id, rank, points, goals_diff, form, description,
                    all_played, all_win, all_draw, all_lose, goals_for, goals_against
                ) VALUES %s
                ON CONFLICT (season_id, team_id) DO UPDATE SET
                  rank        = EXCLUDED.rank,
                  points      = EXCLUDED.points,
                  goals_diff  = EXCLUDED.goals_diff,
                  form        = EXCLUDED.form,
                  description = EXCLUDED.description,
                  all_played  = EXCLUDED.all_played,
                  all_win     = EXCLUDED.all_win,
                  all_draw    = EXCLUDED.all_draw,
                  all_lose    = EXCLUDED.all_lose,
                  goals_for   = EXCLUDED.goals_for,
                  goals_against = EXCLUDED.goals_against
                WHERE
                  standings.rank        IS DISTINCT FROM EXCLUDED.rank
                  OR standings.points   IS DISTINCT FROM EXCLUDED.points
                  OR standings.goals_diff IS DISTINCT FROM EXCLUDED.goals_diff
                  OR standings.form     IS DISTINCT FROM EXCLUDED.form
                  OR standings.all_played IS DISTINCT FROM EXCLUDED.all_played
                  OR standings.goals_for IS DISTINCT FROM EXCLUDED.goals_for
                  OR standings.goals_against IS DISTINCT FROM EXCLUDED.goals_against
                """,
                rows,
                page_size=200,
            )

    # ── Watermark ──────────────────────────────────────────────────────────

    def upsert_watermark(self, key: str, value: str) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_watermarks (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                WHERE ingestion_watermarks.value IS DISTINCT FROM EXCLUDED.value
                """,
                (key, value),
            )

    # ── Orphan cleanup (phase-level) ──────────────────────────────────────

    def cleanup_squad_players(self, season_id: int, valid_pairs: list[tuple[int, int]]) -> None:
        """Remove squad_players rows not in the incoming (player_id, team_id) set."""
        with self.conn.cursor() as cur:
            if valid_pairs:
                valid_pids = [p[0] for p in valid_pairs]
                valid_tids = [p[1] for p in valid_pairs]
                cur.execute(
                    """DELETE FROM squad_players
                       WHERE season_id = %s
                       AND (player_id, team_id) NOT IN (
                           SELECT unnest(%s::bigint[]), unnest(%s::bigint[])
                       )""",
                    (season_id, valid_pids, valid_tids),
                )
            else:
                cur.execute("DELETE FROM squad_players WHERE season_id = %s", (season_id,))

    def cleanup_standings(self, season_id: int, valid_team_ids: list[int]) -> None:
        with self.conn.cursor() as cur:
            if valid_team_ids:
                cur.execute(
                    "DELETE FROM standings WHERE season_id = %s AND NOT (team_id = ANY(%s))",
                    (season_id, valid_team_ids),
                )
            else:
                cur.execute("DELETE FROM standings WHERE season_id = %s", (season_id,))



# ---------------------------------------------------------------------------
# Squad helpers (same parsing as Job A)
# ---------------------------------------------------------------------------


def _parse_squad_pairs(team_id: int, data: dict[str, Any]) -> list[tuple[int, int]]:
    pairs: list[tuple[int, int]] = []
    raw = data.get("response") or []
    blocks: list[dict[str, Any]] = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        team = block.get("team") or {}
        tid = team.get("id") or team_id
        for pl in block.get("players") or []:
            if not isinstance(pl, dict):
                continue
            inner = pl.get("player")
            pid = inner.get("id") if isinstance(inner, dict) else pl.get("id")
            if tid and pid:
                pairs.append((int(pid), int(tid)))
    return pairs


def _parse_squad_players(data: dict[str, Any]) -> list[dict[str, Any]]:
    players: list[dict[str, Any]] = []
    raw = data.get("response") or []
    blocks: list[dict[str, Any]] = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []
    for block in blocks:
        if not isinstance(block, dict):
            continue
        for pl in block.get("players") or []:
            if not isinstance(pl, dict):
                continue
            inner = pl.get("player")
            src = inner if isinstance(inner, dict) else pl
            pid = src.get("id")
            if not pid:
                continue
            players.append(
                {
                    "id": pid,
                    "name": src.get("name"),
                    "firstname": src.get("firstname"),
                    "lastname": src.get("lastname"),
                    "birth": src.get("birth") or {},
                    "nationality": src.get("nationality"),
                    "photo": src.get("photo") or "",
                }
            )
    return players


# ---------------------------------------------------------------------------
# Phase orchestration
# ---------------------------------------------------------------------------


def phase1_reference(
    w: ConditionalWriter, headers: dict[str, str],
    league_id: int, season_year: int, sleep: float,
) -> tuple[int, list[int]]:
    """Phase 1: Conditionally refresh league, season, teams, league_season_teams."""
    log.info("Phase 1: Reference entities (league=%d, season=%d)", league_id, season_year)

    data = api_get(f"{BASE_URL}/leagues?id={league_id}", headers, sleep)
    if not data.get("response"):
        raise SystemExit(f"No league found for id={league_id}")
    league_obj = data["response"][0]
    league = league_obj.get("league") or {}
    w.upsert_league(league)

    seasons = league_obj.get("seasons") or []
    season_obj = next((s for s in seasons if s.get("year") == season_year), None)
    if not season_obj:
        raise SystemExit(f"Season {season_year} not found for league {league_id}")
    season_id = w.upsert_season(league_id, season_obj)
    w.commit()
    log.info("  league=%d, season_id=%d (year=%d)", league_id, season_id, season_year)

    teams_data = api_get(
        f"{BASE_URL}/teams?league={league_id}&season={season_year}", headers, sleep
    )
    team_ids: list[int] = []
    for item in teams_data.get("response") or []:
        team = item.get("team") or {}
        venue = item.get("venue") or {}
        if team.get("id"):
            w.upsert_team_full(team, venue)
            team_ids.append(int(team["id"]))
    w.upsert_league_season_teams(season_id, team_ids)
    w.commit()
    log.info("  Teams: %d", len(team_ids))

    return season_id, team_ids


def phase2_players_and_squads(
    w: ConditionalWriter, headers: dict[str, str],
    league_id: int, season_year: int, season_id: int,
    team_ids: list[int], sleep: float,
) -> None:
    """Phase 2: Conditionally refresh players, squad_players, player_season_statistics."""
    log.info("Phase 2: Players, squads, season statistics")

    page = 1
    total_pages = 1
    total_players = 0

    while page <= total_pages:
        data = api_get(
            f"{BASE_URL}/players?league={league_id}&season={season_year}&page={page}",
            headers, sleep,
        )
        paging = data.get("paging") or {}
        total_pages = int(paging.get("total") or 1)
        response = data.get("response") or []

        player_objs: list[dict[str, Any]] = []
        stat_rows: list[tuple[Any, ...]] = []

        for item in response:
            player = item.get("player") or {}
            pid = player.get("id")
            if not pid:
                continue
            player_objs.append(
                {
                    "id": pid,
                    "name": player.get("name"),
                    "firstname": player.get("firstname"),
                    "lastname": player.get("lastname"),
                    "birth": player.get("birth") or {},
                    "nationality": player.get("nationality"),
                    "photo": player.get("photo") or "",
                }
            )
            for st in item.get("statistics") or []:
                st_league = st.get("league") or {}
                if st_league.get("id") != league_id:
                    continue
                team = st.get("team") or {}
                tid = team.get("id")
                if not tid:
                    continue
                games = st.get("games") or {}
                goals_obj = st.get("goals") or {}
                stat_rows.append(
                    (
                        season_id, pid, tid,
                        games.get("position"),
                        _as_decimal(games.get("rating")),
                        games.get("appearences"),
                        games.get("lineups"),
                        games.get("minutes"),
                        games.get("number"),
                        goals_obj.get("total"),
                        goals_obj.get("assists"),
                        Json(st),
                    )
                )

        w.upsert_players(player_objs)
        w.upsert_player_season_statistics(season_id, stat_rows)
        w.commit()
        total_players += len(response)
        log.info("  Players page %d/%d (%d rows)", page, total_pages, len(response))
        page += 1

    log.info("  Total players processed: %d", total_players)

    log.info("Phase 2 (squads): /players/squads per team → squad_players")
    all_squad_pairs: list[tuple[int, int]] = []
    all_squad_players: list[dict[str, Any]] = []
    for tid in team_ids:
        squads_data = api_get(f"{BASE_URL}/players/squads?team={tid}", headers, sleep)
        if squads_data.get("errors"):
            log.warning("  squads errors for team=%s: %s", tid, squads_data.get("errors"))
            continue
        all_squad_pairs.extend(_parse_squad_pairs(tid, squads_data))
        all_squad_players.extend(_parse_squad_players(squads_data))

    w.upsert_players(all_squad_players)
    all_squad_pairs = sorted(set(all_squad_pairs))
    if all_squad_pairs:
        w.upsert_squad_players(season_id, all_squad_pairs)
    w.cleanup_squad_players(season_id, all_squad_pairs)
    w.commit()
    log.info("  squad_players pairs upserted: %d", len(all_squad_pairs))


def phase3_fixtures(
    w: ConditionalWriter, headers: dict[str, str],
    league_id: int, season_year: int, season_id: int, sleep: float,
) -> list[dict[str, Any]]:
    """Phase 3: Conditionally refresh all fixtures (core columns only)."""
    log.info("Phase 3: Fixtures (all statuses)")

    list_data = api_get(
        f"{BASE_URL}/fixtures?league={league_id}&season={season_year}", headers, sleep
    )
    all_items: list[dict[str, Any]] = list_data.get("response") or []
    target_items = [it for it in all_items if _is_target_league(it, league_id)]
    if len(target_items) != len(all_items):
        log.warning(
            "  Dropped %d fixtures not in league %d",
            len(all_items) - len(target_items), league_id,
        )

    fids = []
    for it in target_items:
        fid = (it.get("fixture") or {}).get("id")
        if fid:
            fids.append(int(fid))
    fids = sorted(set(fids))
    log.info("  Fixture IDs found: %d", len(fids))

    all_detail_items: list[dict[str, Any]] = []
    for bi, group in enumerate(_chunks(fids, FIXTURE_IDS_CHUNK)):
        id_str = "-".join(str(x) for x in group)
        detail = api_get(f"{BASE_URL}/fixtures?ids={id_str}", headers, sleep)

        for item in detail.get("response") or []:
            if not _is_target_league(item, league_id):
                continue
            home = (item.get("teams") or {}).get("home") or {}
            away = (item.get("teams") or {}).get("away") or {}
            w.upsert_team_minimal(home)
            w.upsert_team_minimal(away)
            w.upsert_fixture(season_id, item)
            all_detail_items.append(item)

        w.commit()
        log.info("  Fixture batch %d: %d fixtures", bi + 1, len(group))

    log.info("  Total fixtures upserted: %d", len(all_detail_items))
    return all_detail_items


def phase4_detail_refresh(
    w: ConditionalWriter, headers: dict[str, str],
    league_id: int, season_id: int,
    fixture_items: list[dict[str, Any]], sleep: float,
    skip_player_stats: bool = False,
) -> None:
    """
    Phase 4 (opt-in): Re-ingest post-match detail for terminal fixtures.
    Only runs when --refresh-detail is passed.
    Updates detail_ingested_at on success.
    """
    terminal_items = [
        it for it in fixture_items
        if ((it.get("fixture") or {}).get("status") or {}).get("short") in TERMINAL_STATUSES
        and _is_target_league(it, league_id)
    ]
    log.info(
        "Phase 4: Detail refresh for %d terminal fixtures (of %d total)",
        len(terminal_items), len(fixture_items),
    )

    success_count = 0
    error_count = 0

    for item in terminal_items:
        fx = item.get("fixture") or {}
        fid = fx.get("id")
        if not fid:
            continue
        fid = int(fid)

        try:
            w.upsert_fixture_events(fid, item.get("events") or [])
            w.upsert_fixture_team_statistics(fid, item.get("statistics") or [])
            w.upsert_lineups(fid, item.get("lineups") or [])

            lineup_players: list[dict[str, Any]] = []
            for block in item.get("lineups") or []:
                for key in ("startXI", "substitutes"):
                    for slot in block.get(key) or []:
                        p = slot.get("player") or {}
                        if p.get("id"):
                            lineup_players.append(
                                {"id": p["id"], "name": p.get("name") or "?",
                                 "firstname": None, "lastname": None,
                                 "birth": {}, "nationality": None, "photo": ""}
                            )
            w.upsert_players(lineup_players)

            if not skip_player_stats:
                _ingest_fixture_player_stats(w, season_id, fid, headers, sleep)

            w.mark_fixture_detail_done(fid)
            w.commit()
            success_count += 1

        except Exception as exc:
            log.warning("  Fixture %d detail refresh failed: %s", fid, exc)
            w.conn.rollback()
            try:
                w.mark_fixture_detail_error(fid, str(exc))
                w.commit()
            except Exception:
                w.conn.rollback()
            error_count += 1

        if (success_count + error_count) % 25 == 0:
            log.info(
                "  Progress: %d/%d (errors: %d)",
                success_count + error_count, len(terminal_items), error_count,
            )

    log.info("  Phase 4 complete: %d succeeded, %d failed", success_count, error_count)


def _ingest_fixture_player_stats(
    w: ConditionalWriter, season_id: int, fixture_id: int,
    headers: dict[str, str], sleep: float,
) -> None:
    """Fetch /fixtures/players for a single fixture and conditionally upsert."""
    data = api_get(f"{BASE_URL}/fixtures/players?fixture={fixture_id}", headers, sleep)
    player_objs: list[dict[str, Any]] = []
    stat_rows: list[tuple[Any, ...]] = []

    for grp in data.get("response") or []:
        team = grp.get("team") or {}
        team_id = team.get("id")
        for p in grp.get("players") or []:
            player = p.get("player") or {}
            pid = player.get("id")
            if not pid or not team_id:
                continue
            player_objs.append(
                {"id": pid, "name": player.get("name"),
                 "firstname": player.get("firstname"), "lastname": player.get("lastname"),
                 "birth": player.get("birth") or {},
                 "nationality": player.get("nationality"), "photo": player.get("photo") or ""}
            )
            for st in (p.get("statistics") or [])[:1]:
                games = st.get("games") or {}
                goals_obj = st.get("goals") or {}
                shots = st.get("shots") or {}
                passes = st.get("passes") or {}
                tackles = st.get("tackles") or {}
                cards = st.get("cards") or {}
                sub = games.get("substitute")
                starter = (not bool(sub)) if sub is not None else None
                stat_rows.append(
                    (
                        fixture_id, season_id, pid, team_id,
                        games.get("position"), _as_decimal(games.get("rating")),
                        games.get("minutes"), games.get("number"),
                        starter, sub,
                        goals_obj.get("total"), goals_obj.get("assists"),
                        shots.get("total"), shots.get("on"),
                        passes.get("total"), passes.get("key"), passes.get("accuracy"),
                        tackles.get("total"), tackles.get("interceptions"),
                        cards.get("yellow"), cards.get("red"),
                        Json(st),
                    )
                )

    w.upsert_players(player_objs)
    w.upsert_player_fixture_statistics(fixture_id, stat_rows)


def phase5_standings(
    w: ConditionalWriter, headers: dict[str, str],
    league_id: int, season_year: int, season_id: int, sleep: float,
) -> None:
    """Phase 5: Conditionally refresh standings."""
    log.info("Phase 5: Standings")

    data = api_get(
        f"{BASE_URL}/standings?league={league_id}&season={season_year}", headers, sleep
    )
    resp = data.get("response") or []
    row_count = 0
    standings_team_ids: list[int] = []
    if resp:
        first = resp[0]
        if not _is_target_league(first, league_id):
            log.warning(
                "  Standings league_id=%s (expected %s); skipping",
                _payload_league_id(first), league_id,
            )
        else:
            groups = first.get("league", {}).get("standings") or []
            first_group = groups[0] if groups else []
            row_count = len(first_group)
            w.upsert_standings(season_id, first_group)
            for r in first_group:
                tid = (r.get("team") or {}).get("id")
                if tid:
                    standings_team_ids.append(int(tid))
    w.cleanup_standings(season_id, standings_team_ids)
    w.commit()
    log.info("  Standings rows: %d", row_count)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Job E: Conditional refresh / DB sync for a league-season"
    )
    parser.add_argument(
        "--league", type=int, default=DEFAULT_LEAGUE_ID,
        help=f"League ID (default: {DEFAULT_LEAGUE_ID})",
    )
    parser.add_argument(
        "--season", type=int, default=DEFAULT_SEASON_YEAR,
        help=f"API season year (default: {DEFAULT_SEASON_YEAR})",
    )
    parser.add_argument(
        "--refresh-detail", action="store_true",
        help="Also re-ingest post-match detail tables (events, lineups, player stats). "
             "Without this flag, detail_ingested_at and related columns are never touched.",
    )
    parser.add_argument(
        "--skip-fixture-player-stats", action="store_true",
        help="Skip /fixtures/players per-match calls (saves many API requests). "
             "Only relevant when --refresh-detail is active.",
    )
    parser.add_argument(
        "--sleep", type=float, default=0.15,
        help="Seconds to sleep between API calls (default: 0.15)",
    )
    args = parser.parse_args()

    load_repo_dotenv()
    api_key = _ensure_env("FOOTBALL_API_KEY")
    db_url = _ensure_env("SUPABASE_DB_URL")
    headers = {"x-apisports-key": api_key}

    conn = psycopg2.connect(db_url)
    try:
        w = ConditionalWriter(conn)

        season_id, team_ids = phase1_reference(
            w, headers, args.league, args.season, args.sleep
        )

        phase2_players_and_squads(
            w, headers, args.league, args.season, season_id, team_ids, args.sleep
        )

        fixture_items = phase3_fixtures(
            w, headers, args.league, args.season, season_id, args.sleep
        )

        if args.refresh_detail:
            phase4_detail_refresh(
                w, headers, args.league, season_id, fixture_items, args.sleep,
                skip_player_stats=args.skip_fixture_player_stats,
            )
        else:
            log.info("Phase 4: Skipped (detail refresh not requested; use --refresh-detail)")

        phase5_standings(w, headers, args.league, args.season, season_id, args.sleep)

        w.upsert_watermark(
            f"job_e:{args.league}:{args.season}",
            datetime.now(timezone.utc).isoformat(),
        )
        w.commit()

        log.info("Job E complete (league=%d, season=%d).", args.league, args.season)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
