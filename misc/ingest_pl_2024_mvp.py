"""
Ingest Premier League (league=39) season=2024 into Supabase Postgres (Supabase).

Free-plan-aware MVP strategy:
  - Pull season-level player statistics (player ratings) via `GET /players?league=39&season=2024&page=N`
  - Pull a recent window of completed fixtures via `GET /fixtures?league=39&season=2024&from=...&to=...&status=ft`
  - For each fixture in that window, pull per-player match ratings via
      `GET /fixtures/players?fixture=<fixture_id>`

Run (from repo root):
  .\.venv\Scripts\python.exe misc\ingest_pl_2024_mvp.py --player-page-start 1 --player-page-end 3
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Iterable

import psycopg2
from psycopg2.extras import Json, execute_values

from env_loader import load_repo_dotenv

import requests


BASE_URL = "https://v3.football.api-sports.io"
LEAGUE_ID = 39
SEASON_YEAR = 2024


def _as_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _parse_dt(value: str) -> datetime:
    # API returns ISO8601 with offset, e.g. 2025-05-01T18:30:00+00:00
    return datetime.fromisoformat(value)


def _ensure_env(var: str) -> str:
    v = os.environ.get(var)
    if not v:
        raise SystemExit(f"Missing {var} in src/.env")
    return v


class SupabaseWriter:
    def __init__(self, db_url: str) -> None:
        self.conn = psycopg2.connect(db_url)
        self.conn.autocommit = False

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def commit(self) -> None:
        self.conn.commit()

    def fetch_one(self, sql: str, params: tuple[Any, ...]) -> Any:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return row

    def upsert_league(self, league: dict[str, Any]) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO leagues (id, name, type, country_name, country_code, logo_url, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                  name = EXCLUDED.name,
                  type = EXCLUDED.type,
                  country_name = EXCLUDED.country_name,
                  country_code = EXCLUDED.country_code,
                  logo_url = EXCLUDED.logo_url,
                  updated_at = now()
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
                  start_date = EXCLUDED.start_date,
                  end_date = EXCLUDED.end_date,
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
                raise RuntimeError("Failed to resolve season_id after upsert")
            return int(row[0])

    def upsert_team(self, team: dict[str, Any]) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO teams (id, name, code, country, founded, national, logo_url, venue, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (id) DO UPDATE SET
                  name = EXCLUDED.name,
                  code = EXCLUDED.code,
                  country = EXCLUDED.country,
                  founded = EXCLUDED.founded,
                  national = EXCLUDED.national,
                  logo_url = EXCLUDED.logo_url,
                  venue = EXCLUDED.venue,
                  updated_at = now()
                """,
                (
                    team.get("id"),
                    team.get("name"),
                    team.get("code"),
                    team.get("country"),
                    team.get("founded"),
                    team.get("national", False),
                    team.get("logo"),
                    Json(team.get("venue")),
                ),
            )

    def upsert_league_season_teams(
        self,
        season_id: int,
        team_ids: Iterable[int],
    ) -> None:
        rows = [(season_id, tid) for tid in team_ids]
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO league_season_teams (season_id, team_id)
                VALUES %s
                ON CONFLICT (season_id, team_id) DO NOTHING
                """,
                rows,
                page_size=200,
            )

    def upsert_standings(
        self,
        season_id: int,
        standings_rows: list[dict[str, Any]],
    ) -> None:
        # Each row in standings_rows represents a team in the table.
        rows: list[tuple[Any, ...]] = []
        for r in standings_rows:
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

        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO standings (
                    season_id, team_id, rank, points, goals_diff, form, description,
                    all_played, all_win, all_draw, all_lose, goals_for, goals_against
                )
                VALUES %s
                ON CONFLICT (season_id, team_id) DO UPDATE SET
                  rank = EXCLUDED.rank,
                  points = EXCLUDED.points,
                  goals_diff = EXCLUDED.goals_diff,
                  form = EXCLUDED.form,
                  description = EXCLUDED.description,
                  all_played = EXCLUDED.all_played,
                  all_win = EXCLUDED.all_win,
                  all_draw = EXCLUDED.all_draw,
                  all_lose = EXCLUDED.all_lose,
                  goals_for = EXCLUDED.goals_for,
                  goals_against = EXCLUDED.goals_against
                """,
                rows,
                page_size=200,
            )

    def upsert_players(self, players: list[dict[str, Any]]) -> None:
        # players are unique by player id
        rows = []
        for p in players:
            rows.append(
                (
                    p.get("id"),
                    p.get("name"),
                    p.get("firstname"),
                    p.get("lastname"),
                    p.get("birth", {}).get("date"),
                    p.get("nationality"),
                    p.get("photo"),
                )
            )

        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO players (id, name, firstname, lastname, birth_date, nationality, photo_url)
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                  name = EXCLUDED.name,
                  firstname = EXCLUDED.firstname,
                  lastname = EXCLUDED.lastname,
                  birth_date = EXCLUDED.birth_date,
                  nationality = EXCLUDED.nationality,
                  photo_url = EXCLUDED.photo_url,
                  updated_at = now()
                """,
                rows,
                page_size=500,
            )

    def upsert_player_season_statistics(
        self,
        season_id: int,
        rows: list[tuple[Any, ...]],
    ) -> None:
        # rows columns must match insert below
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO player_season_statistics (
                    season_id, player_id, team_id,
                    position, rating, appearances, lineups, minutes, number,
                    goals, assists,
                    extra
                )
                VALUES %s
                ON CONFLICT (season_id, player_id, team_id) DO UPDATE SET
                  position = EXCLUDED.position,
                  rating = EXCLUDED.rating,
                  appearances = EXCLUDED.appearances,
                  lineups = EXCLUDED.lineups,
                  minutes = EXCLUDED.minutes,
                  number = EXCLUDED.number,
                  goals = EXCLUDED.goals,
                  assists = EXCLUDED.assists,
                  extra = EXCLUDED.extra,
                  updated_at = now()
                """,
                rows,
                page_size=500,
            )

    def upsert_fixtures(self, season_id: int, fixtures: list[dict[str, Any]]) -> None:
        rows = []
        for item in fixtures:
            fx = item.get("fixture") or {}
            league = item.get("league") or {}
            teams = item.get("teams") or {}
            goals = item.get("goals") or {}
            status = fx.get("status") or {}
            venue = fx.get("venue") or {}
            rows.append(
                (
                    fx.get("id"),
                    season_id,
                    (teams.get("home") or {}).get("id"),
                    (teams.get("away") or {}).get("id"),
                    _parse_dt(fx.get("date")),
                    status.get("short"),
                    league.get("round"),
                    venue.get("id"),
                    venue.get("name"),
                    fx.get("referee"),
                    goals.get("home"),
                    goals.get("away"),
                    status.get("elapsed"),
                    Json(item),
                )
            )

        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO fixtures (
                    id, season_id, home_team_id, away_team_id,
                    utc_kickoff, status_short, round, venue_id, venue_name, referee,
                    home_goals, away_goals, elapsed, extra
                )
                VALUES %s
                ON CONFLICT (id) DO UPDATE SET
                  season_id = EXCLUDED.season_id,
                  home_team_id = EXCLUDED.home_team_id,
                  away_team_id = EXCLUDED.away_team_id,
                  utc_kickoff = EXCLUDED.utc_kickoff,
                  status_short = EXCLUDED.status_short,
                  round = EXCLUDED.round,
                  venue_id = EXCLUDED.venue_id,
                  venue_name = EXCLUDED.venue_name,
                  referee = EXCLUDED.referee,
                  home_goals = EXCLUDED.home_goals,
                  away_goals = EXCLUDED.away_goals,
                  elapsed = EXCLUDED.elapsed,
                  extra = EXCLUDED.extra,
                  updated_at = now()
                """,
                rows,
                page_size=200,
            )

    def upsert_player_fixture_statistics(
        self,
        rows: list[tuple[Any, ...]],
    ) -> None:
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                """
                INSERT INTO player_fixture_statistics (
                    fixture_id, season_id, player_id, team_id,
                    position, rating, minutes, number,
                    starter, substitute,
                    goals, assists, shots_total, shots_on,
                    passes_total, passes_key, passes_acc,
                    tackles_total, interceptions,
                    yellow_cards, red_cards,
                    extra
                )
                VALUES %s
                ON CONFLICT (fixture_id, player_id, team_id) DO UPDATE SET
                  position = EXCLUDED.position,
                  rating = EXCLUDED.rating,
                  minutes = EXCLUDED.minutes,
                  number = EXCLUDED.number,
                  starter = EXCLUDED.starter,
                  substitute = EXCLUDED.substitute,
                  goals = EXCLUDED.goals,
                  assists = EXCLUDED.assists,
                  shots_total = EXCLUDED.shots_total,
                  shots_on = EXCLUDED.shots_on,
                  passes_total = EXCLUDED.passes_total,
                  passes_key = EXCLUDED.passes_key,
                  passes_acc = EXCLUDED.passes_acc,
                  tackles_total = EXCLUDED.tackles_total,
                  interceptions = EXCLUDED.interceptions,
                  yellow_cards = EXCLUDED.yellow_cards,
                  red_cards = EXCLUDED.red_cards,
                  extra = EXCLUDED.extra,
                  updated_at = now()
                """,
                rows,
                page_size=500,
            )


def http_get(url: str, headers: dict[str, str]) -> dict[str, Any]:
    r = requests.get(url, headers=headers, timeout=60)
    r.raise_for_status()
    return r.json()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--player-page-start", type=int, default=1)
    parser.add_argument("--player-page-end", type=int, default=3)
    parser.add_argument("--fixtures-from", type=str, default="2025-05-10")
    parser.add_argument("--fixtures-to", type=str, default="2025-05-25")
    parser.add_argument("--sleep-seconds", type=float, default=0.2)
    parser.add_argument("--skip-reference", action="store_true", help="Skip leagues/seasons/teams/standings upserts")
    parser.add_argument("--skip-fixtures", action="store_true", help="Skip fixtures and fixture-level player ratings ingestion")
    args = parser.parse_args()

    load_repo_dotenv()

    api_key = _ensure_env("FOOTBALL_API_KEY")
    db_url = _ensure_env("SUPABASE_DB_URL")
    headers = {"x-apisports-key": api_key}

    writer = SupabaseWriter(db_url)
    try:
        # Reference: league + season + teams + standings
        if not args.skip_reference:
            leagues_url = f"{BASE_URL}/leagues?id={LEAGUE_ID}"
            leagues_data = http_get(leagues_url, headers)
            if not leagues_data.get("response"):
                raise RuntimeError(f"No league data returned for league={LEAGUE_ID}")

            league_obj = leagues_data["response"][0]
            # response[0] structure: { "league": {...}, "seasons": [...] }
            league = league_obj.get("league") or {}

            writer.upsert_league(league)

            seasons = league_obj.get("seasons") or []
            season_obj = next((s for s in seasons if s.get("year") == SEASON_YEAR), None)
            if not season_obj:
                raise RuntimeError(f"Season {SEASON_YEAR} not found for league {LEAGUE_ID}")

            season_id = writer.upsert_season(LEAGUE_ID, season_obj)
            print("Upserted season_id:", season_id)

            # Teams for league-season
            teams_url = f"{BASE_URL}/teams?league={LEAGUE_ID}&season={SEASON_YEAR}"
            teams_data = http_get(teams_url, headers)
            team_rows = teams_data.get("response") or []
            if not team_rows:
                print("WARN: teams response empty.")
                print("WARN: teams errors:", teams_data.get("errors"))

            team_ids: list[int] = []
            for item in team_rows:
                team = item.get("team") or {}
                venue = item.get("venue") or {}
                if venue:
                    team = {**team, "venue": venue}
                team_ids.append(int(team["id"]))
                writer.upsert_team(team)

            writer.upsert_league_season_teams(season_id, team_ids)
            writer.commit()
            print("Upserted teams:", len(team_ids))

            # Standings
            standings_url = f"{BASE_URL}/standings?league={LEAGUE_ID}&season={SEASON_YEAR}"
            standings_data = http_get(standings_url, headers)
            standings_response = standings_data.get("response") or []
            if not standings_response:
                print("WARN: standings response empty.")
                print("WARN: standings errors:", standings_data.get("errors"))
                raise RuntimeError("No standings response (check WARN output above).")

            standings_groups = standings_response[0]["league"].get("standings") or []
            first_group = standings_groups[0] if standings_groups else []
            writer.upsert_standings(season_id, first_group)
            writer.commit()
            print("Upserted standings rows:", len(first_group))
        else:
            # Resolve season_id without calling API for reference data.
            row = writer.fetch_one(
                "SELECT id FROM seasons WHERE league_id=%s AND year=%s",
                (LEAGUE_ID, SEASON_YEAR),
            )
            if not row:
                raise RuntimeError("skip-reference requested but seasons row not found")
            season_id = int(row[0])
            print("Resolved season_id:", season_id)

        # Fixtures + match-level player ratings
        if not args.skip_fixtures:
            fixtures_url = (
                f"{BASE_URL}/fixtures?league={LEAGUE_ID}&season={SEASON_YEAR}"
                f"&from={args.fixtures_from}&to={args.fixtures_to}&status=ft"
            )
            fixtures_data = http_get(fixtures_url, headers)
            fixtures = fixtures_data.get("response") or []
            print("Fixtures in window:", len(fixtures))

            writer.upsert_fixtures(season_id, fixtures)
            writer.commit()

            fixture_ids = [
                int(f.get("fixture", {}).get("id"))
                for f in fixtures
                if f.get("fixture", {}).get("id")
            ]
            fixture_ids = fixture_ids[:200]  # safety

            # Pull players per fixture (expensive; controlled by window size)
            for i, fixture_id in enumerate(fixture_ids, start=1):
                fp_url = f"{BASE_URL}/fixtures/players?fixture={fixture_id}"
                fp_data = http_get(fp_url, headers)
                fp_response = fp_data.get("response") or []

                players_to_upsert: dict[int, dict[str, Any]] = {}
                stats_rows: list[tuple[Any, ...]] = []

                for team_group in fp_response:
                    team = team_group.get("team") or {}
                    team_id = team.get("id")
                    for p in team_group.get("players") or []:
                        player = p.get("player") or {}
                        player_id = player.get("id")
                        if not player_id or not team_id:
                            continue

                        players_to_upsert[int(player_id)] = {
                            "id": player_id,
                            "name": player.get("name"),
                            "firstname": player.get("firstname"),
                            "lastname": player.get("lastname"),
                            "birth": player.get("birth") or {},
                            "nationality": player.get("nationality"),
                            "photo": player.get("photo") or "",
                        }

                        stats_list = p.get("statistics") or []
                        if not stats_list:
                            continue

                        for st in stats_list[:1]:
                            games = (st.get("games") or {}) if isinstance(st, dict) else {}
                            goals = st.get("goals") or {}
                            shots = st.get("shots") or {}
                            passes = st.get("passes") or {}
                            tackles = st.get("tackles") or {}
                            cards = st.get("cards") or {}

                            rating = _as_decimal(games.get("rating"))

                            substitute = games.get("substitute")
                            starter = None
                            if substitute is not None:
                                starter = not bool(substitute)

                            stats_rows.append(
                                (
                                    fixture_id,
                                    season_id,
                                    player_id,
                                    team_id,
                                    games.get("position"),
                                    rating,
                                    games.get("minutes"),
                                    games.get("number"),
                                    starter,
                                    substitute,
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

                if players_to_upsert:
                    writer.upsert_players(list(players_to_upsert.values()))
                if stats_rows:
                    writer.upsert_player_fixture_statistics(stats_rows)
                writer.commit()

                if i % 5 == 0:
                    print(f"Processed {i}/{len(fixture_ids)} fixtures")
                time.sleep(args.sleep_seconds)

        # 5) Season-level player ratings (paged)
        for page in range(args.player_page_start, args.player_page_end + 1):
            players_url = f"{BASE_URL}/players?league={LEAGUE_ID}&season={SEASON_YEAR}&page={page}"
            pdata = http_get(players_url, headers)
            response = pdata.get("response") or []
            if not response:
                print(f"No players returned for page={page} (stopping)")
                break

            players_to_upsert: dict[int, dict[str, Any]] = {}
            stats_rows: list[tuple[Any, ...]] = []

            for item in response:
                player = item.get("player") or {}
                player_id = player.get("id")
                if not player_id:
                    continue

                # Upsert player base row.
                players_to_upsert[int(player_id)] = {
                    "id": player_id,
                    "name": player.get("name"),
                    "firstname": player.get("firstname"),
                    "lastname": player.get("lastname"),
                    "birth": player.get("birth") or {},
                    "nationality": player.get("nationality"),
                    "photo": player.get("photo") or "",
                }

                for st in item.get("statistics") or []:
                    team = st.get("team") or {}
                    team_id = team.get("id")
                    if not team_id:
                        continue

                    games = st.get("games") or {}
                    goals = st.get("goals") or {}

                    stats_rows.append(
                        (
                            season_id,
                            player_id,
                            team_id,
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

            writer.upsert_players(list(players_to_upsert.values()))
            if stats_rows:
                writer.upsert_player_season_statistics(season_id, stats_rows)
            writer.commit()
            print(f"Upserted player season stats page={page}")

            time.sleep(args.sleep_seconds)

        writer.commit()
        print("MVP ingestion finished.")
    finally:
        writer.close()


if __name__ == "__main__":
    main()

