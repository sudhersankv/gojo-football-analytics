"""
Full ingest: Premier League (league=39) API season=2025 (2025/26 campaign).

1) Truncates all Gojo football tables (fresh start).
2) Leagues, season, teams, standings.
3) All fixtures for the season (paged list), then detail in batches of 20 (API limit for `ids`).
4) Normalized: fixtures (+ score breakdown), fixture_events, fixture_team_statistics,
   fixture_team_lineups, fixture_lineup_players.
5) All pages of /players?league=39&season=2025 (season stats).
6) /fixtures/players for each fixture (per-match player stats).

Requires: FOOTBALL_API_KEY, SUPABASE_DB_URL in ``src/.env`` (or repo ``.env``).

Run (repo root):
  .\\.venv\\Scripts\\python.exe misc\\ingest_pl_2025_full.py
  .\\.venv\\Scripts\\python.exe misc\\ingest_pl_2025_full.py --no-truncate
  .\\.venv\\Scripts\\python.exe misc\\ingest_pl_2025_full.py --skip-fixture-player-stats
"""

from __future__ import annotations

import argparse
import os
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

import psycopg2
import requests
from psycopg2.extras import Json, execute_values

from env_loader import load_repo_dotenv

BASE_URL = "https://v3.football.api-sports.io"
DEFAULT_LEAGUE_ID = 39
DEFAULT_SEASON_YEAR = 2025
FIXTURE_IDS_CHUNK = 20


def _ensure_env(var: str) -> str:
    v = os.environ.get(var)
    if not v:
        raise SystemExit(f"Missing {var} in src/.env")
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


def http_get(url: str, headers: dict[str, str]) -> dict[str, Any]:
    r = requests.get(url, headers=headers, timeout=90)
    r.raise_for_status()
    return r.json()


def truncate_all(conn: psycopg2.extensions.connection) -> None:
    """Remove all football data in one statement (FK-safe)."""
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
    print("Database truncated (all Gojo tables cleared).")


class Writer:
    def __init__(self, conn: psycopg2.extensions.connection) -> None:
        self.conn = conn

    def commit(self) -> None:
        self.conn.commit()

    def fetch_one(self, sql: str, params: tuple[Any, ...]) -> Any:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def upsert_league(self, league: dict[str, Any]) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO leagues (id, name, type, country_name, country_code, logo_url, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                  name = EXCLUDED.name, type = EXCLUDED.type,
                  country_name = EXCLUDED.country_name, country_code = EXCLUDED.country_code,
                  logo_url = EXCLUDED.logo_url, updated_at = now()
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

    def upsert_team_minimal(self, team: dict[str, Any]) -> None:
        """Insert/update team from fixture/lineup (minimal fields)."""
        tid = team.get("id")
        if not tid:
            return
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO teams (id, name, code, country, founded, national, logo_url, venue, updated_at)
                VALUES (%s, %s, NULL, NULL, NULL, false, %s, NULL, now())
                ON CONFLICT (id) DO UPDATE SET
                  name = COALESCE(EXCLUDED.name, teams.name),
                  logo_url = COALESCE(EXCLUDED.logo_url, teams.logo_url),
                  updated_at = now()
                """,
                (tid, team.get("name"), team.get("logo")),
            )

    def upsert_team_full(self, team: dict[str, Any], venue: dict[str, Any] | None) -> None:
        t = dict(team)
        if venue:
            t = {**team, "venue": venue}
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO teams (id, name, code, country, founded, national, logo_url, venue, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                  name = EXCLUDED.name, code = EXCLUDED.code, country = EXCLUDED.country,
                  founded = EXCLUDED.founded, national = EXCLUDED.national,
                  logo_url = EXCLUDED.logo_url, venue = EXCLUDED.venue, updated_at = now()
                """,
                (
                    t.get("id"),
                    t.get("name"),
                    t.get("code"),
                    t.get("country"),
                    t.get("founded"),
                    t.get("national", False),
                    t.get("logo"),
                    Json(t.get("venue")),
                ),
            )

    def upsert_league_season_teams(self, season_id: int, team_ids: list[int]) -> None:
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

    def upsert_standings(self, season_id: int, rows_data: list[dict[str, Any]]) -> None:
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

    def upsert_fixture_full(self, season_id: int, item: dict[str, Any]) -> None:
        fx = item.get("fixture") or {}
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
            fx.get("id"),
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
                    elapsed = EXCLUDED.elapsed, extra = EXCLUDED.extra, updated_at = now()
                """,
                row,
            )

    def replace_fixture_events(self, fixture_id: int, events: list[dict[str, Any]]) -> None:
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

    def replace_fixture_team_statistics(self, fixture_id: int, statistics: list[dict[str, Any]]) -> None:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM fixture_team_statistics WHERE fixture_id = %s", (fixture_id,))
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
                  stat_value = EXCLUDED.stat_value, updated_at = now()
                """,
                rows,
                page_size=500,
            )

    def replace_lineups(self, fixture_id: int, lineups: list[dict[str, Any]]) -> None:
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM fixture_lineup_players WHERE fixture_id = %s", (fixture_id,))
            cur.execute("DELETE FROM fixture_team_lineups WHERE fixture_id = %s", (fixture_id,))

        meta_rows = []
        player_rows = []

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
                    (
                        fixture_id,
                        tid,
                        pid,
                        p.get("name"),
                        True,
                        p.get("number"),
                        p.get("pos"),
                        p.get("grid"),
                        None,
                    )
                )
            for slot in block.get("substitutes") or []:
                p = slot.get("player") or {}
                pid = p.get("id")
                if not pid:
                    continue
                player_rows.append(
                    (
                        fixture_id,
                        tid,
                        pid,
                        p.get("name"),
                        False,
                        p.get("number"),
                        p.get("pos"),
                        p.get("grid"),
                        None,
                    )
                )

        if meta_rows:
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO fixture_team_lineups (
                        fixture_id, team_id, formation, coach_api_id, coach_name, coach_photo_url, colors
                    ) VALUES %s
                    ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                      formation = EXCLUDED.formation, coach_api_id = EXCLUDED.coach_api_id,
                      coach_name = EXCLUDED.coach_name, coach_photo_url = EXCLUDED.coach_photo_url,
                      colors = EXCLUDED.colors, updated_at = now()
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
                      grid = EXCLUDED.grid, updated_at = now()
                    """,
                    player_rows,
                    page_size=200,
                )

    def upsert_players_minimal(self, players: list[dict[str, Any]]) -> None:
        if not players:
            return
        rows = []
        for p in players:
            pid = p.get("id")
            if not pid:
                continue
            rows.append(
                (
                    pid,
                    p.get("name") or "?",
                    p.get("firstname"),
                    p.get("lastname"),
                    (p.get("birth") or {}).get("date") if isinstance(p.get("birth"), dict) else None,
                    p.get("nationality"),
                    p.get("photo") or "",
                )
            )
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO players (id, name, firstname, lastname, birth_date, nationality, photo_url)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                  name = EXCLUDED.name, firstname = EXCLUDED.firstname, lastname = EXCLUDED.lastname,
                  birth_date = EXCLUDED.birth_date, nationality = EXCLUDED.nationality,
                  photo_url = EXCLUDED.photo_url, updated_at = now()
                """,
                rows,
                page_size=500,
            )

    def upsert_player_season_statistics(self, season_id: int, stat_rows: list[tuple[Any, ...]]) -> None:
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
                  goals = EXCLUDED.goals, assists = EXCLUDED.assists, extra = EXCLUDED.extra,
                  updated_at = now()
                """,
                stat_rows,
                page_size=400,
            )

    def upsert_player_fixture_statistics(self, rows: list[tuple[Any, ...]]) -> None:
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
                  extra = EXCLUDED.extra, updated_at = now()
                """,
                rows,
                page_size=400,
            )


def collect_fixture_ids(league_id: int, season_year: int, headers: dict[str, str]) -> list[int]:
    """List all fixture ids for league+season (single call — API does not support `page` here)."""
    url = f"{BASE_URL}/fixtures?league={league_id}&season={season_year}"
    data = http_get(url, headers)
    if data.get("errors"):
        raise RuntimeError(f"fixtures list errors: {data.get('errors')}")
    ids: list[int] = []
    for it in data.get("response") or []:
        fid = (it.get("fixture") or {}).get("id")
        if fid:
            ids.append(int(fid))
    return sorted(set(ids))


def chunks(lst: list[int], n: int) -> list[list[int]]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def ingest_fixture_player_stats(
    w: Writer,
    season_id: int,
    fixture_id: int,
    headers: dict[str, str],
) -> None:
    url = f"{BASE_URL}/fixtures/players?fixture={fixture_id}"
    data = http_get(url, headers)
    players_payload: list[dict[str, Any]] = []
    stat_rows: list[tuple[Any, ...]] = []

    for grp in data.get("response") or []:
        team = grp.get("team") or {}
        team_id = team.get("id")
        for p in grp.get("players") or []:
            player = p.get("player") or {}
            pid = player.get("id")
            if not pid or not team_id:
                continue
            players_payload.append(
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
                goals = st.get("goals") or {}
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
                        goals.get("total"),
                        goals.get("assists"),
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

    w.upsert_players_minimal(players_payload)
    w.upsert_player_fixture_statistics(stat_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Full PL ingest for API season year (2025 = 2025/26)")
    parser.add_argument("--league", type=int, default=DEFAULT_LEAGUE_ID)
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON_YEAR, help="API season year, e.g. 2025")
    parser.add_argument("--no-truncate", action="store_true", help="Keep existing rows (upsert only)")
    parser.add_argument(
        "--skip-fixture-player-stats",
        action="store_true",
        help="Skip /fixtures/players per match (saves many API calls)",
    )
    parser.add_argument("--sleep", type=float, default=0.15, help="Seconds between API calls")
    args = parser.parse_args()

    load_repo_dotenv()
    api_key = _ensure_env("FOOTBALL_API_KEY")
    db_url = _ensure_env("SUPABASE_DB_URL")
    headers = {"x-apisports-key": api_key}

    conn = psycopg2.connect(db_url)
    try:
        if not args.no_truncate:
            truncate_all(conn)

        w = Writer(conn)

        # League + season
        leagues_data = http_get(f"{BASE_URL}/leagues?id={args.league}", headers)
        if not leagues_data.get("response"):
            raise SystemExit(f"No league {args.league}")
        league_obj = leagues_data["response"][0]
        league = league_obj.get("league") or {}
        w.upsert_league(league)
        seasons = league_obj.get("seasons") or []
        season_obj = next((s for s in seasons if s.get("year") == args.season), None)
        if not season_obj:
            raise SystemExit(f"Season {args.season} not found on league {args.league}")
        season_id = w.upsert_season(args.league, season_obj)
        w.commit()
        print(f"season_id={season_id} (league={args.league}, year={args.season})")

        # Teams
        teams_data = http_get(f"{BASE_URL}/teams?league={args.league}&season={args.season}", headers)
        team_ids: list[int] = []
        for item in teams_data.get("response") or []:
            team = item.get("team") or {}
            venue = item.get("venue") or {}
            if team.get("id"):
                w.upsert_team_full(team, venue)
                team_ids.append(int(team["id"]))
        w.upsert_league_season_teams(season_id, team_ids)
        w.commit()
        print(f"Teams: {len(team_ids)}")
        time.sleep(args.sleep)

        # Standings
        st_data = http_get(f"{BASE_URL}/standings?league={args.league}&season={args.season}", headers)
        st_resp = st_data.get("response") or []
        st_rows_count = 0
        if st_resp:
            groups = st_resp[0].get("league", {}).get("standings") or []
            first = groups[0] if groups else []
            st_rows_count = len(first)
            w.upsert_standings(season_id, first)
        w.commit()
        print(f"Standings rows: {st_rows_count}")
        time.sleep(args.sleep)

        # All fixture ids
        fids = collect_fixture_ids(args.league, args.season, headers)
        print(f"Fixture ids to fetch detail: {len(fids)}")
        time.sleep(args.sleep)

        # Detail batches
        for bi, group in enumerate(chunks(fids, FIXTURE_IDS_CHUNK)):
            id_str = "-".join(str(x) for x in group)
            detail = http_get(f"{BASE_URL}/fixtures?ids={id_str}", headers)
            for item in detail.get("response") or []:
                home = (item.get("teams") or {}).get("home") or {}
                away = (item.get("teams") or {}).get("away") or {}
                w.upsert_team_minimal(home)
                w.upsert_team_minimal(away)
                w.upsert_fixture_full(season_id, item)
                fid = (item.get("fixture") or {}).get("id")
                if not fid:
                    continue
                w.replace_fixture_events(int(fid), item.get("events") or [])
                w.replace_fixture_team_statistics(int(fid), item.get("statistics") or [])
                w.replace_lineups(int(fid), item.get("lineups") or [])
                # Seed players from lineups for FK
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
                w.upsert_players_minimal(lineup_players)
            w.commit()
            print(f"  Detail batch {bi + 1}: {len(group)} fixtures")
            time.sleep(args.sleep)

        # Season player statistics (all pages)
        page = 1
        total_pages = 1
        while page <= total_pages:
            pdata = http_get(
                f"{BASE_URL}/players?league={args.league}&season={args.season}&page={page}",
                headers,
            )
            paging = pdata.get("paging") or {}
            total_pages = int(paging.get("total") or 1)
            response = pdata.get("response") or []
            players_to: list[dict[str, Any]] = []
            stat_rows: list[tuple[Any, ...]] = []
            for item in response:
                player = item.get("player") or {}
                pid = player.get("id")
                if not pid:
                    continue
                players_to.append(
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
                    goals = st.get("goals") or {}
                    stat_rows.append(
                        (
                            season_id,
                            pid,
                            tid,
                            games.get("position"),
                            _as_decimal(games.get("rating")),
                            games.get("appearences"),
                            games.get("lineups"),
                            games.get("minutes"),
                            games.get("number"),
                            goals.get("total"),
                            goals.get("assists"),
                            Json(st),
                        )
                    )
            w.upsert_players_minimal(players_to)
            w.upsert_player_season_statistics(season_id, stat_rows)
            w.commit()
            print(f"  Players page {page}/{total_pages} ({len(response)} rows)")
            page += 1
            time.sleep(args.sleep)

        # Per-fixture player stats
        if not args.skip_fixture_player_stats:
            for i, fid in enumerate(fids):
                ingest_fixture_player_stats(w, season_id, fid, headers)
                w.commit()
                if (i + 1) % 25 == 0:
                    print(f"  Fixture player stats: {i + 1}/{len(fids)}")
                time.sleep(args.sleep)
            print(f"Fixture player stats done: {len(fids)} fixtures")

        print("Ingest complete.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
