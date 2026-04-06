"""
Job A — Bootstrap / full snapshot for a league-season.

Populates every table with all data currently available from API-Football:

  Phase 1: Reference entities
    - leagues, seasons, teams, league_season_teams

  Phase 2: Players & squads
    - players (from /players endpoint, all pages)
    - squad_players (derived from player-season stats: each player→team link)
    - player_season_statistics (full season aggregates)

  Phase 3: Fixtures (schedule + completed)
    - fixtures (all — NS through FT; schedule skeleton + results)

  Phase 4: Post-match detail for already-completed fixtures
    - fixture_events
    - fixture_team_statistics
    - fixture_team_lineups, fixture_lineup_players
    - player_fixture_statistics (per-match player stats)
    - Sets detail_ingested_at on successfully processed terminal fixtures

  Phase 5: Season aggregates
    - standings

Design principles:
  - Idempotent: all writes are INSERT ... ON CONFLICT upserts; safe to re-run.
  - No truncate: existing data is preserved and updated. Use --truncate for clean start.
  - Terminal status set: FT, AET, PEN, AWD (configurable via TERMINAL_STATUSES).
  - Rate-limited: configurable sleep between API calls (default 0.15s).
  - Batched fixture detail: fetches fixture detail in chunks of 20 (API limit).
  - Per-fixture error isolation: one bad fixture does not stop the batch.

Requires env vars (in src/.env or .env):
  FOOTBALL_API_KEY  — API-Football key (x-apisports-key header)
  SUPABASE_DB_URL   — Postgres connection string

Run from repo root:
  .\\.venv\\Scripts\\python.exe src\\jobs\\job_a\\bootstrap_league_season.py
  .\\.venv\\Scripts\\python.exe src\\jobs\\job_a\\bootstrap_league_season.py --league 39 --season 2025
  .\\.venv\\Scripts\\python.exe src\\jobs\\job_a\\bootstrap_league_season.py --truncate
  .\\.venv\\Scripts\\python.exe src\\jobs\\job_a\\bootstrap_league_season.py --skip-fixture-player-stats
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from datetime import datetime
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
DEFAULT_LEAGUE_ID = 39       # Premier League
DEFAULT_SEASON_YEAR = 2025   # API season year (e.g. 2025 = 2025/26 campaign)
FIXTURE_IDS_CHUNK = 20       # API supports up to 20 fixture IDs per request

# Fixtures in these statuses are considered "completed" and eligible for
# post-match detail ingestion in Phase 4.
TERMINAL_STATUSES = {"FT", "AET", "PEN", "AWD"}

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("job_a")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _ensure_env(var: str) -> str:
    """Read a required environment variable or exit."""
    v = os.environ.get(var)
    if not v:
        raise SystemExit(f"Missing required env var: {var}")
    return v


def _parse_dt(value: str | None) -> datetime:
    """Parse an ISO-8601 date string from the API into a Python datetime."""
    if not value:
        raise ValueError("missing fixture date")
    return datetime.fromisoformat(value)


def _as_decimal(value: Any) -> Decimal | None:
    """Safely convert a value to Decimal (for ratings)."""
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _stat_value_to_text(value: Any) -> str | None:
    """Normalize a stat value to text (API returns mixed types)."""
    if value is None:
        return None
    return str(value)


def _chunks(lst: list, n: int) -> list[list]:
    """Split a list into sublists of at most n elements."""
    return [lst[i : i + n] for i in range(0, len(lst), n)]


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------


def api_get(url: str, headers: dict[str, str], sleep: float = 0.0) -> dict[str, Any]:
    """
    Make a GET request to API-Football with error handling.
    Optionally sleeps after the call to respect rate limits.
    """
    resp = requests.get(url, headers=headers, timeout=90)
    resp.raise_for_status()
    data = resp.json()

    errors = data.get("errors")
    if errors:
        # API-Football returns errors as a dict or list; log but don't always crash
        log.warning("API returned errors for %s: %s", url, errors)

    if sleep > 0:
        time.sleep(sleep)

    return data


# ---------------------------------------------------------------------------
# Database writer (idempotent upserts for every table)
# ---------------------------------------------------------------------------


class Writer:
    """Encapsulates all DB write operations as idempotent upserts."""

    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self.conn = conn

    def commit(self) -> None:
        self.conn.commit()

    # -- Phase 1: Reference entities ----------------------------------------

    def upsert_league(self, league: dict[str, Any]) -> None:
        """Upsert a single league row from /leagues API response."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO leagues (id, name, type, country_name, country_code, logo_url)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                  name = EXCLUDED.name, type = EXCLUDED.type,
                  country_name = EXCLUDED.country_name, country_code = EXCLUDED.country_code,
                  logo_url = EXCLUDED.logo_url
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
        Upsert a season row and return its DB-generated season_id.
        The season_id is the internal serial PK, not the API year.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO seasons (league_id, year, start_date, end_date, current)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (league_id, year) DO UPDATE SET
                  start_date = EXCLUDED.start_date, end_date = EXCLUDED.end_date,
                  current = EXCLUDED.current
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
        """Upsert a team with all fields from /teams endpoint."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO teams (id, name, code, country, founded, national, logo_url, venue)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                  name = EXCLUDED.name, code = EXCLUDED.code, country = EXCLUDED.country,
                  founded = EXCLUDED.founded, national = EXCLUDED.national,
                  logo_url = EXCLUDED.logo_url, venue = EXCLUDED.venue
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
        Upsert a team with minimal fields (from fixture/lineup payloads).
        Uses COALESCE to avoid overwriting richer data from /teams endpoint.
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
                  name = COALESCE(EXCLUDED.name, teams.name),
                  logo_url = COALESCE(EXCLUDED.logo_url, teams.logo_url)
                """,
                (tid, team.get("name"), team.get("logo")),
            )

    def upsert_league_season_teams(self, season_id: int, team_ids: list[int]) -> None:
        """Link teams to a league-season (junction table)."""
        rows = [(season_id, tid) for tid in team_ids]
        if not rows:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO league_season_teams (season_id, team_id) VALUES %s
                ON CONFLICT DO NOTHING
                """,
                rows,
                page_size=200,
            )

    # -- Phase 2: Players, squads, season stats -----------------------------

    def upsert_players(self, players: list[dict[str, Any]]) -> None:
        """
        Upsert player rows from /players API response.
        Works for both full player objects and minimal lineup-derived objects.
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
                  name = EXCLUDED.name, firstname = EXCLUDED.firstname, lastname = EXCLUDED.lastname,
                  birth_date = EXCLUDED.birth_date, nationality = EXCLUDED.nationality,
                  photo_url = EXCLUDED.photo_url
                """,
                rows,
                page_size=500,
            )

    def upsert_squad_players(self, season_id: int, player_team_pairs: list[tuple[int, int]]) -> None:
        """
        Upsert squad_players rows. Each pair is (player_id, team_id).
        Derived from player-season statistics: if a player has stats for a team
        in this season, they belong to that squad.
        """
        if not player_team_pairs:
            return
        rows = [(season_id, tid, pid) for pid, tid in player_team_pairs]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO squad_players (season_id, team_id, player_id) VALUES %s
                ON CONFLICT DO NOTHING
                """,
                rows,
                page_size=500,
            )

    def upsert_player_season_statistics(
        self, season_id: int, stat_rows: list[tuple[Any, ...]]
    ) -> None:
        """Upsert player_season_statistics from /players API (all pages)."""
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
                  position = EXCLUDED.position, rating = EXCLUDED.rating,
                  appearances = EXCLUDED.appearances, lineups = EXCLUDED.lineups,
                  minutes = EXCLUDED.minutes, number = EXCLUDED.number,
                  goals = EXCLUDED.goals, assists = EXCLUDED.assists, extra = EXCLUDED.extra
                """,
                stat_rows,
                page_size=400,
            )

    # -- Phase 3: Fixtures --------------------------------------------------

    def upsert_fixture(self, season_id: int, item: dict[str, Any]) -> int | None:
        """
        Upsert a single fixture row from the /fixtures API response.
        Returns the fixture_id on success, None if the payload is unusable.
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
                    season_id = EXCLUDED.season_id,
                    home_team_id = EXCLUDED.home_team_id, away_team_id = EXCLUDED.away_team_id,
                    utc_kickoff = EXCLUDED.utc_kickoff, timezone = EXCLUDED.timezone,
                    status_short = EXCLUDED.status_short, round = EXCLUDED.round,
                    venue_id = EXCLUDED.venue_id, venue_name = EXCLUDED.venue_name,
                    referee = EXCLUDED.referee, home_goals = EXCLUDED.home_goals,
                    away_goals = EXCLUDED.away_goals,
                    ht_home_goals = EXCLUDED.ht_home_goals, ht_away_goals = EXCLUDED.ht_away_goals,
                    ft_home_goals = EXCLUDED.ft_home_goals, ft_away_goals = EXCLUDED.ft_away_goals,
                    et_home_goals = EXCLUDED.et_home_goals, et_away_goals = EXCLUDED.et_away_goals,
                    pen_home_goals = EXCLUDED.pen_home_goals, pen_away_goals = EXCLUDED.pen_away_goals,
                    home_winner = EXCLUDED.home_winner, away_winner = EXCLUDED.away_winner,
                    elapsed = EXCLUDED.elapsed, extra = EXCLUDED.extra
                """,
                row,
            )
        return int(fid)

    # -- Phase 4: Post-match detail -----------------------------------------

    def replace_fixture_events(self, fixture_id: int, events: list[dict[str, Any]]) -> None:
        """
        Replace all events for a fixture (delete + insert).
        Events are ordered by event_index to preserve timeline.
        """
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM fixture_events WHERE fixture_id = %s", (fixture_id,))
        if not events:
            return
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
                    fixture_id,
                    i,
                    tid,
                    tm.get("elapsed") if tm.get("elapsed") is not None else 0,
                    tm.get("extra"),
                    ev.get("type") or "unknown",
                    ev.get("detail"),
                    ev.get("comments"),
                    pl.get("id"),
                    pl.get("name"),
                    rel.get("id"),
                    rel.get("name"),
                    Json(ev) if ev else None,
                )
            )
        if not rows:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO fixture_events (
                    fixture_id, event_index, team_id, minute, minute_extra,
                    event_type, detail, comments,
                    player_api_id, player_name, related_player_api_id, related_player_name, extra
                ) VALUES %s
                """,
                rows,
                page_size=500,
            )

    def replace_fixture_team_statistics(
        self, fixture_id: int, statistics: list[dict[str, Any]]
    ) -> None:
        """Replace team-level match statistics for a fixture."""
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM fixture_team_statistics WHERE fixture_id = %s", (fixture_id,)
            )
        rows = []
        for block in statistics:
            team = block.get("team") or {}
            tid = team.get("id")
            if not tid:
                continue
            for st in block.get("statistics") or []:
                rows.append(
                    (
                        fixture_id,
                        tid,
                        st.get("type") or "",
                        _stat_value_to_text(st.get("value")),
                    )
                )
        if not rows:
            return
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO fixture_team_statistics (fixture_id, team_id, stat_type, stat_value)
                VALUES %s
                ON CONFLICT (fixture_id, team_id, stat_type) DO UPDATE SET
                  stat_value = EXCLUDED.stat_value
                """,
                rows,
                page_size=500,
            )

    def replace_lineups(self, fixture_id: int, lineups: list[dict[str, Any]]) -> None:
        """
        Replace lineup data for a fixture:
          - fixture_team_lineups (formation, coach)
          - fixture_lineup_players (startXI + substitutes)
        """
        with self.conn.cursor() as cur:
            cur.execute(
                "DELETE FROM fixture_lineup_players WHERE fixture_id = %s", (fixture_id,)
            )
            cur.execute(
                "DELETE FROM fixture_team_lineups WHERE fixture_id = %s", (fixture_id,)
            )

        meta_rows: list[tuple[Any, ...]] = []
        player_rows: list[tuple[Any, ...]] = []

        for block in lineups:
            team = block.get("team") or {}
            tid = team.get("id")
            if not tid:
                continue
            coach = block.get("coach") or {}
            meta_rows.append(
                (
                    fixture_id,
                    tid,
                    block.get("formation"),
                    coach.get("id"),
                    coach.get("name"),
                    coach.get("photo"),
                    Json(team.get("colors")),
                )
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
                      formation = EXCLUDED.formation, coach_api_id = EXCLUDED.coach_api_id,
                      coach_name = EXCLUDED.coach_name, coach_photo_url = EXCLUDED.coach_photo_url,
                      colors = EXCLUDED.colors
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
                      player_name = EXCLUDED.player_name, is_starter = EXCLUDED.is_starter,
                      shirt_number = EXCLUDED.shirt_number, position_short = EXCLUDED.position_short,
                      grid = EXCLUDED.grid
                    """,
                    player_rows,
                    page_size=200,
                )

    def upsert_player_fixture_statistics(self, rows: list[tuple[Any, ...]]) -> None:
        """Upsert per-player per-fixture statistics from /fixtures/players."""
        if not rows:
            return
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
                  position = EXCLUDED.position, rating = EXCLUDED.rating, minutes = EXCLUDED.minutes,
                  number = EXCLUDED.number, starter = EXCLUDED.starter, substitute = EXCLUDED.substitute,
                  goals = EXCLUDED.goals, assists = EXCLUDED.assists, shots_total = EXCLUDED.shots_total,
                  shots_on = EXCLUDED.shots_on, passes_total = EXCLUDED.passes_total,
                  passes_key = EXCLUDED.passes_key, passes_acc = EXCLUDED.passes_acc,
                  tackles_total = EXCLUDED.tackles_total, interceptions = EXCLUDED.interceptions,
                  yellow_cards = EXCLUDED.yellow_cards, red_cards = EXCLUDED.red_cards,
                  extra = EXCLUDED.extra
                """,
                rows,
                page_size=400,
            )

    def mark_fixture_detail_done(self, fixture_id: int) -> None:
        """
        Mark a fixture as having its post-match detail successfully ingested.
        Clears any previous error and resets attempt counter.
        """
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE fixtures
                SET detail_ingested_at = now(),
                    detail_ingest_attempts = 0,
                    detail_ingest_last_error = NULL
                WHERE id = %s
                """,
                (fixture_id,),
            )

    def mark_fixture_detail_error(self, fixture_id: int, error_msg: str) -> None:
        """Record a failed detail ingest attempt for a fixture."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE fixtures
                SET detail_ingest_attempts = detail_ingest_attempts + 1,
                    detail_ingest_last_error = %s
                WHERE id = %s
                """,
                (error_msg[:500], fixture_id),
            )

    # -- Phase 5: Season aggregates -----------------------------------------

    def upsert_standings(self, season_id: int, rows_data: list[dict[str, Any]]) -> None:
        """Upsert standings table from /standings API response."""
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
                  rank = EXCLUDED.rank, points = EXCLUDED.points, goals_diff = EXCLUDED.goals_diff,
                  form = EXCLUDED.form, description = EXCLUDED.description,
                  all_played = EXCLUDED.all_played, all_win = EXCLUDED.all_win,
                  all_draw = EXCLUDED.all_draw, all_lose = EXCLUDED.all_lose,
                  goals_for = EXCLUDED.goals_for, goals_against = EXCLUDED.goals_against
                """,
                rows,
                page_size=200,
            )

    # -- Ingestion watermark ------------------------------------------------

    def upsert_watermark(self, key: str, value: str) -> None:
        """Record an ingestion checkpoint for operational visibility."""
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO ingestion_watermarks (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, value),
            )


# ---------------------------------------------------------------------------
# Truncate (optional clean-start mode)
# ---------------------------------------------------------------------------


def truncate_all(conn: psycopg2.extensions.connection) -> None:
    """Remove all football data. FK-safe via single TRUNCATE ... CASCADE."""
    sql = """
    TRUNCATE TABLE
      fixture_events,
      fixture_team_statistics,
      fixture_team_lineups,
      fixture_lineup_players,
      player_fixture_statistics,
      fixtures,
      standings,
      league_season_teams,
      squad_players,
      player_season_statistics,
      seasons,
      teams,
      leagues,
      players,
      ingestion_watermarks
    RESTART IDENTITY;
    """
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    log.info("All Gojo tables truncated.")


# ---------------------------------------------------------------------------
# Phase orchestration functions
# ---------------------------------------------------------------------------


def phase1_reference(
    w: Writer, headers: dict[str, str], league_id: int, season_year: int, sleep: float
) -> tuple[int, list[int]]:
    """
    Phase 1: League, season, teams, league_season_teams.
    Returns (season_id, team_ids).
    """
    log.info("Phase 1: Reference entities (league=%d, season=%d)", league_id, season_year)

    # League + season metadata
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

    # Teams + league_season_teams
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
    w: Writer,
    headers: dict[str, str],
    league_id: int,
    season_year: int,
    season_id: int,
    sleep: float,
) -> None:
    """
    Phase 2: Paginate /players endpoint → players, squad_players, player_season_statistics.
    """
    log.info("Phase 2: Players, squads, season statistics")

    page = 1
    total_pages = 1
    total_players = 0

    while page <= total_pages:
        data = api_get(
            f"{BASE_URL}/players?league={league_id}&season={season_year}&page={page}",
            headers,
            sleep,
        )
        paging = data.get("paging") or {}
        total_pages = int(paging.get("total") or 1)
        response = data.get("response") or []

        player_objs: list[dict[str, Any]] = []
        stat_rows: list[tuple[Any, ...]] = []
        squad_pairs: list[tuple[int, int]] = []

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
                team = st.get("team") or {}
                tid = team.get("id")
                if not tid:
                    continue
                games = st.get("games") or {}
                goals_obj = st.get("goals") or {}
                stat_rows.append(
                    (
                        season_id,
                        pid,
                        tid,
                        games.get("position"),
                        _as_decimal(games.get("rating")),
                        games.get("appearences"),  # API typo is intentional
                        games.get("lineups"),
                        games.get("minutes"),
                        games.get("number"),
                        goals_obj.get("total"),
                        goals_obj.get("assists"),
                        Json(st),
                    )
                )
                squad_pairs.append((pid, tid))

        w.upsert_players(player_objs)
        w.upsert_player_season_statistics(season_id, stat_rows)
        w.upsert_squad_players(season_id, squad_pairs)
        w.commit()
        total_players += len(response)
        log.info("  Players page %d/%d (%d rows)", page, total_pages, len(response))
        page += 1

    log.info("  Total players processed: %d", total_players)


def phase3_fixtures(
    w: Writer,
    headers: dict[str, str],
    league_id: int,
    season_year: int,
    season_id: int,
    sleep: float,
) -> list[dict[str, Any]]:
    """
    Phase 3: Fetch all fixtures for the league-season and upsert into fixtures table.
    Returns the full list of fixture items (used by Phase 4 to identify terminal ones).
    """
    log.info("Phase 3: Fixtures (all statuses)")

    # Collect all fixture IDs first
    list_data = api_get(
        f"{BASE_URL}/fixtures?league={league_id}&season={season_year}", headers, sleep
    )
    if list_data.get("errors"):
        log.error("API errors on fixture list: %s", list_data["errors"])

    all_items: list[dict[str, Any]] = list_data.get("response") or []
    fids = []
    for it in all_items:
        fid = (it.get("fixture") or {}).get("id")
        if fid:
            fids.append(int(fid))
    fids = sorted(set(fids))
    log.info("  Fixture IDs found: %d", len(fids))

    # Fetch full detail in batches of FIXTURE_IDS_CHUNK (API supports up to 20 IDs)
    all_detail_items: list[dict[str, Any]] = []
    for bi, group in enumerate(_chunks(fids, FIXTURE_IDS_CHUNK)):
        id_str = "-".join(str(x) for x in group)
        detail = api_get(f"{BASE_URL}/fixtures?ids={id_str}", headers, sleep)

        for item in detail.get("response") or []:
            # Ensure teams exist before fixture FK insert
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


def phase4_postmatch_detail(
    w: Writer,
    headers: dict[str, str],
    season_id: int,
    fixture_items: list[dict[str, Any]],
    sleep: float,
    skip_player_stats: bool = False,
) -> None:
    """
    Phase 4: For fixtures in terminal status, ingest post-match detail:
      - events, team statistics, lineups, lineup players
      - player fixture statistics (optional, expensive: one API call per fixture)
      - mark detail_ingested_at on success

    Errors are isolated per fixture — one failure does not stop the batch.
    """
    terminal_items = [
        it for it in fixture_items
        if ((it.get("fixture") or {}).get("status") or {}).get("short") in TERMINAL_STATUSES
    ]
    log.info(
        "Phase 4: Post-match detail for %d terminal fixtures (of %d total)",
        len(terminal_items),
        len(fixture_items),
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
            # Events, team stats, lineups from the fixture detail payload
            w.replace_fixture_events(fid, item.get("events") or [])
            w.replace_fixture_team_statistics(fid, item.get("statistics") or [])
            w.replace_lineups(fid, item.get("lineups") or [])

            # Seed players from lineups so FKs resolve
            lineup_players: list[dict[str, Any]] = []
            for block in item.get("lineups") or []:
                for key in ("startXI", "substitutes"):
                    for slot in block.get(key) or []:
                        p = slot.get("player") or {}
                        if p.get("id"):
                            lineup_players.append(
                                {
                                    "id": p["id"],
                                    "name": p.get("name") or "?",
                                    "firstname": None,
                                    "lastname": None,
                                    "birth": {},
                                    "nationality": None,
                                    "photo": "",
                                }
                            )
            w.upsert_players(lineup_players)

            # Per-fixture player statistics (expensive: one API call per fixture)
            if not skip_player_stats:
                _ingest_fixture_player_stats(w, season_id, fid, headers, sleep)

            w.mark_fixture_detail_done(fid)
            w.commit()
            success_count += 1

        except Exception as exc:
            log.warning("  Fixture %d detail failed: %s", fid, exc)
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
                success_count + error_count,
                len(terminal_items),
                error_count,
            )

    log.info(
        "  Phase 4 complete: %d succeeded, %d failed", success_count, error_count
    )


def _ingest_fixture_player_stats(
    w: Writer, season_id: int, fixture_id: int, headers: dict[str, str], sleep: float
) -> None:
    """
    Fetch /fixtures/players for a single fixture and upsert into
    player_fixture_statistics. Also upserts player rows for FK safety.
    """
    data = api_get(
        f"{BASE_URL}/fixtures/players?fixture={fixture_id}", headers, sleep
    )
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
                        fixture_id,
                        season_id,
                        pid,
                        team_id,
                        games.get("position"),
                        _as_decimal(games.get("rating")),
                        games.get("minutes"),
                        games.get("number"),
                        starter,
                        sub,
                        goals_obj.get("total"),
                        goals_obj.get("assists"),
                        shots.get("total"),
                        shots.get("on"),
                        passes.get("total"),
                        passes.get("key"),
                        passes.get("accuracy"),
                        tackles.get("total"),
                        tackles.get("interceptions"),
                        cards.get("yellow"),
                        cards.get("red"),
                        Json(st),
                    )
                )

    w.upsert_players(player_objs)
    w.upsert_player_fixture_statistics(stat_rows)


def phase5_standings(
    w: Writer,
    headers: dict[str, str],
    league_id: int,
    season_year: int,
    season_id: int,
    sleep: float,
) -> None:
    """Phase 5: Refresh standings from /standings endpoint."""
    log.info("Phase 5: Standings")

    data = api_get(
        f"{BASE_URL}/standings?league={league_id}&season={season_year}", headers, sleep
    )
    resp = data.get("response") or []
    row_count = 0
    if resp:
        groups = resp[0].get("league", {}).get("standings") or []
        first_group = groups[0] if groups else []
        row_count = len(first_group)
        w.upsert_standings(season_id, first_group)
    w.commit()
    log.info("  Standings rows: %d", row_count)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Job A: Bootstrap / full snapshot for a league-season"
    )
    parser.add_argument(
        "--league", type=int, default=DEFAULT_LEAGUE_ID,
        help=f"API-Football league ID (default: {DEFAULT_LEAGUE_ID} = Premier League)",
    )
    parser.add_argument(
        "--season", type=int, default=DEFAULT_SEASON_YEAR,
        help=f"API season year (default: {DEFAULT_SEASON_YEAR})",
    )
    parser.add_argument(
        "--truncate", action="store_true",
        help="Truncate all tables before ingesting (clean start)",
    )
    parser.add_argument(
        "--skip-fixture-player-stats", action="store_true",
        help="Skip /fixtures/players per-match calls (saves many API requests)",
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
        if args.truncate:
            truncate_all(conn)

        w = Writer(conn)

        # Phase 1: Reference entities
        season_id, team_ids = phase1_reference(
            w, headers, args.league, args.season, args.sleep
        )

        # Phase 2: Players, squads, season statistics
        phase2_players_and_squads(
            w, headers, args.league, args.season, season_id, args.sleep
        )

        # Phase 3: Fixtures (all statuses: NS through FT)
        fixture_items = phase3_fixtures(
            w, headers, args.league, args.season, season_id, args.sleep
        )

        # Phase 4: Post-match detail for terminal fixtures
        phase4_postmatch_detail(
            w,
            headers,
            season_id,
            fixture_items,
            args.sleep,
            skip_player_stats=args.skip_fixture_player_stats,
        )

        # Phase 5: Standings
        phase5_standings(w, headers, args.league, args.season, season_id, args.sleep)

        # Record watermark
        w.upsert_watermark(
            f"job_a:{args.league}:{args.season}",
            datetime.utcnow().isoformat(),
        )
        w.commit()

        log.info("Job A complete (league=%d, season=%d).", args.league, args.season)

    finally:
        conn.close()


if __name__ == "__main__":
    main()
