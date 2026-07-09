-- Gojo — PostgreSQL schema (Supabase-compatible)
-- Source of truth: API-Football IDs (stable across requests).
-- Run in Supabase: SQL Editor → New query → paste → Run.
--
-- Conventions:
--   created_at / updated_at on all tables that represent mutable domain rows
--   updated_at maintained automatically via trigger (see gojo_set_updated_at)
--   fixture_events: append/replace per fixture; still has updated_at for uniformity
--   Incremental ingest: fixtures.detail_* columns for post-match worker state
--
-- Covers: leagues/seasons/teams/players/squads, fixtures (+ score breakdown),
-- standings, player season & per-fixture stats, match events, team match stats,
-- lineups (formation + XI/subs), ingestion watermarks.

-- ---------------------------------------------------------------------------
-- Leagues & seasons
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS leagues (
    id           BIGINT PRIMARY KEY,
    name         TEXT NOT NULL,
    type         TEXT,
    country_name TEXT,
    country_code TEXT,
    logo_url     TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS seasons (
    id         BIGSERIAL PRIMARY KEY,
    league_id  BIGINT NOT NULL REFERENCES leagues (id) ON DELETE CASCADE,
    year       INT NOT NULL,
    start_date DATE,
    end_date   DATE,
    current    BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (league_id, year)
);

CREATE INDEX IF NOT EXISTS idx_seasons_league ON seasons (league_id);

-- ---------------------------------------------------------------------------
-- Teams & squads (global teams; participation per league-season)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS teams (
    id          BIGINT PRIMARY KEY,
    name        TEXT NOT NULL,
    code        TEXT,
    country     TEXT,
    founded     INT,
    national    BOOLEAN NOT NULL DEFAULT false,
    logo_url    TEXT,
    venue       JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS league_season_teams (
    season_id BIGINT NOT NULL REFERENCES seasons (id) ON DELETE CASCADE,
    team_id   BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (season_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_lst_team ON league_season_teams (team_id);

CREATE TABLE IF NOT EXISTS players (
    id           BIGINT PRIMARY KEY,
    name         TEXT NOT NULL,
    firstname    TEXT,
    lastname     TEXT,
    birth_date   DATE,
    nationality  TEXT,
    photo_url    TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per player per team in a given league season (squad).
CREATE TABLE IF NOT EXISTS squad_players (
    season_id BIGINT NOT NULL REFERENCES seasons (id) ON DELETE CASCADE,
    team_id   BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,
    player_id BIGINT NOT NULL REFERENCES players (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (season_id, team_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_squad_player ON squad_players (player_id);

-- ---------------------------------------------------------------------------
-- Player performance statistics
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS player_season_statistics (
    season_id     BIGINT NOT NULL REFERENCES seasons (id) ON DELETE CASCADE,
    player_id     BIGINT NOT NULL REFERENCES players (id) ON DELETE CASCADE,
    team_id       BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,

    position      TEXT,
    rating        NUMERIC(6, 3),

    appearances   INT,
    lineups       INT,
    minutes       INT,
    number        INT,

    goals         INT,
    assists       INT,

    extra         JSONB,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (season_id, player_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_player_season_stats_season ON player_season_statistics (season_id);
CREATE INDEX IF NOT EXISTS idx_player_season_stats_rating ON player_season_statistics (season_id, rating);

-- ---------------------------------------------------------------------------
-- Fixtures & results
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fixtures (
    id            BIGINT PRIMARY KEY,
    season_id     BIGINT NOT NULL REFERENCES seasons (id) ON DELETE CASCADE,
    home_team_id  BIGINT NOT NULL REFERENCES teams (id),
    away_team_id  BIGINT NOT NULL REFERENCES teams (id),
    utc_kickoff   TIMESTAMPTZ NOT NULL,
    timezone      TEXT,
    status_short  TEXT,
    round         TEXT,
    venue_id      BIGINT,
    venue_name    TEXT,
    referee       TEXT,
    home_goals    INT,
    away_goals    INT,
    ht_home_goals   INT,
    ht_away_goals   INT,
    ft_home_goals   INT,
    ft_away_goals   INT,
    et_home_goals   INT,
    et_away_goals   INT,
    pen_home_goals  INT,
    pen_away_goals  INT,
    home_winner     BOOLEAN,
    away_winner     BOOLEAN,
    elapsed       INT,
    extra         JSONB,
    detail_ingested_at      TIMESTAMPTZ,
    detail_ingest_attempts  INT NOT NULL DEFAULT 0,
    detail_ingest_last_error TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fixtures_season ON fixtures (season_id);
CREATE INDEX IF NOT EXISTS idx_fixtures_kickoff ON fixtures (utc_kickoff);
CREATE INDEX IF NOT EXISTS idx_fixtures_status ON fixtures (status_short);
CREATE INDEX IF NOT EXISTS idx_fixtures_home ON fixtures (home_team_id);
CREATE INDEX IF NOT EXISTS idx_fixtures_away ON fixtures (away_team_id);

CREATE INDEX IF NOT EXISTS idx_fixtures_postmatch_pending
ON fixtures (utc_kickoff ASC, id)
WHERE detail_ingested_at IS NULL
  AND status_short IN ('FT', 'AET', 'PEN', 'AWD');

CREATE TABLE IF NOT EXISTS player_fixture_statistics (
    fixture_id    BIGINT NOT NULL REFERENCES fixtures (id) ON DELETE CASCADE,
    season_id     BIGINT NOT NULL REFERENCES seasons (id) ON DELETE CASCADE,
    player_id     BIGINT NOT NULL REFERENCES players (id) ON DELETE CASCADE,
    team_id       BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,

    position      TEXT,
    rating        NUMERIC(6, 3),
    minutes       INT,
    number        INT,

    starter        BOOLEAN,
    substitute    BOOLEAN,

    goals         INT,
    assists       INT,
    shots_total   INT,
    shots_on      INT,

    passes_total  INT,
    passes_key    INT,
    passes_acc    TEXT,

    tackles_total INT,
    interceptions INT,

    yellow_cards  INT,
    red_cards     INT,

    extra         JSONB,

    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (fixture_id, player_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_player_fixture_stats_fixture ON player_fixture_statistics (fixture_id);
CREATE INDEX IF NOT EXISTS idx_player_fixture_stats_season ON player_fixture_statistics (season_id);

-- ---------------------------------------------------------------------------
-- Match timeline (API-Football `events` array)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fixture_events (
    id              BIGSERIAL PRIMARY KEY,
    fixture_id      BIGINT NOT NULL REFERENCES fixtures (id) ON DELETE CASCADE,
    event_index     INT NOT NULL,
    team_id         BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,
    minute          INT NOT NULL,
    minute_extra    INT,
    event_type      TEXT NOT NULL,
    detail          TEXT,
    comments        TEXT,
    player_api_id   BIGINT,
    player_name     TEXT,
    related_player_api_id BIGINT,
    related_player_name   TEXT,
    extra           JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (fixture_id, event_index)
);

CREATE INDEX IF NOT EXISTS idx_fixture_events_fixture ON fixture_events (fixture_id);
CREATE INDEX IF NOT EXISTS idx_fixture_events_minute ON fixture_events (fixture_id, minute);
CREATE INDEX IF NOT EXISTS idx_fixture_events_type ON fixture_events (fixture_id, event_type);
CREATE INDEX IF NOT EXISTS idx_fixture_events_player ON fixture_events (player_api_id);

-- ---------------------------------------------------------------------------
-- Team-level match statistics
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fixture_team_statistics (
    fixture_id   BIGINT NOT NULL REFERENCES fixtures (id) ON DELETE CASCADE,
    team_id      BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,
    stat_type    TEXT NOT NULL,
    stat_value   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (fixture_id, team_id, stat_type)
);

CREATE INDEX IF NOT EXISTS idx_fixture_team_stats_fixture ON fixture_team_statistics (fixture_id);
CREATE INDEX IF NOT EXISTS idx_fixture_team_stats_team ON fixture_team_statistics (team_id);

-- ---------------------------------------------------------------------------
-- Lineups
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS fixture_team_lineups (
    fixture_id      BIGINT NOT NULL REFERENCES fixtures (id) ON DELETE CASCADE,
    team_id         BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,
    formation       TEXT,
    coach_api_id    BIGINT,
    coach_name      TEXT,
    coach_photo_url TEXT,
    colors          JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (fixture_id, team_id)
);

CREATE TABLE IF NOT EXISTS fixture_lineup_players (
    fixture_id       BIGINT NOT NULL REFERENCES fixtures (id) ON DELETE CASCADE,
    team_id          BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,
    player_api_id    BIGINT NOT NULL,
    player_name      TEXT,
    is_starter       BOOLEAN NOT NULL DEFAULT false,
    shirt_number     INT,
    position_short   TEXT,
    grid             TEXT,
    player_id        BIGINT REFERENCES players (id) ON DELETE SET NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (fixture_id, team_id, player_api_id)
);

CREATE INDEX IF NOT EXISTS idx_lineup_players_fixture ON fixture_lineup_players (fixture_id);
CREATE INDEX IF NOT EXISTS idx_lineup_players_player ON fixture_lineup_players (player_id);
CREATE INDEX IF NOT EXISTS idx_lineup_players_api ON fixture_lineup_players (player_api_id);

-- ---------------------------------------------------------------------------
-- Standings
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS standings (
    season_id   BIGINT NOT NULL REFERENCES seasons (id) ON DELETE CASCADE,
    team_id     BIGINT NOT NULL REFERENCES teams (id) ON DELETE CASCADE,
    rank        INT NOT NULL,
    points      INT NOT NULL,
    goals_diff  INT,
    form        TEXT,
    description TEXT,
    all_played  INT,
    all_win     INT,
    all_draw    INT,
    all_lose    INT,
    goals_for   INT,
    goals_against INT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (season_id, team_id)
);

CREATE INDEX IF NOT EXISTS idx_standings_rank ON standings (season_id, rank);

-- ---------------------------------------------------------------------------
-- Ingestion bookkeeping
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    key          TEXT PRIMARY KEY,
    value        TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------------
-- Forward-compatible ALTERs (existing databases; safe to re-run)
-- ---------------------------------------------------------------------------

ALTER TABLE IF EXISTS player_season_statistics ADD COLUMN IF NOT EXISTS extra JSONB;
ALTER TABLE IF EXISTS player_fixture_statistics ADD COLUMN IF NOT EXISTS extra JSONB;

ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS timezone TEXT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS ht_home_goals INT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS ht_away_goals INT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS ft_home_goals INT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS ft_away_goals INT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS et_home_goals INT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS et_away_goals INT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS pen_home_goals INT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS pen_away_goals INT;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS home_winner BOOLEAN;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS away_winner BOOLEAN;

ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS detail_ingested_at TIMESTAMPTZ;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS detail_ingest_attempts INT NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS fixtures ADD COLUMN IF NOT EXISTS detail_ingest_last_error TEXT;

ALTER TABLE IF EXISTS seasons ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE IF EXISTS seasons ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE IF EXISTS league_season_teams ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE IF EXISTS league_season_teams ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE IF EXISTS squad_players ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE IF EXISTS squad_players ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE IF EXISTS standings ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE IF EXISTS standings ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE IF EXISTS fixture_events ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE IF EXISTS ingestion_watermarks ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE IF EXISTS ingestion_watermarks ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_fixtures_postmatch_pending
ON fixtures (utc_kickoff ASC, id)
WHERE detail_ingested_at IS NULL
  AND status_short IN ('FT', 'AET', 'PEN', 'AWD');

-- ---------------------------------------------------------------------------
-- updated_at triggers (automatic bump on UPDATE)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION gojo_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS tr_gojo_leagues_updated_at ON leagues;
CREATE TRIGGER tr_gojo_leagues_updated_at
    BEFORE UPDATE ON leagues
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_seasons_updated_at ON seasons;
CREATE TRIGGER tr_gojo_seasons_updated_at
    BEFORE UPDATE ON seasons
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_teams_updated_at ON teams;
CREATE TRIGGER tr_gojo_teams_updated_at
    BEFORE UPDATE ON teams
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_league_season_teams_updated_at ON league_season_teams;
CREATE TRIGGER tr_gojo_league_season_teams_updated_at
    BEFORE UPDATE ON league_season_teams
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_players_updated_at ON players;
CREATE TRIGGER tr_gojo_players_updated_at
    BEFORE UPDATE ON players
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_squad_players_updated_at ON squad_players;
CREATE TRIGGER tr_gojo_squad_players_updated_at
    BEFORE UPDATE ON squad_players
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_player_season_statistics_updated_at ON player_season_statistics;
CREATE TRIGGER tr_gojo_player_season_statistics_updated_at
    BEFORE UPDATE ON player_season_statistics
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_fixtures_updated_at ON fixtures;
CREATE TRIGGER tr_gojo_fixtures_updated_at
    BEFORE UPDATE ON fixtures
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_player_fixture_statistics_updated_at ON player_fixture_statistics;
CREATE TRIGGER tr_gojo_player_fixture_statistics_updated_at
    BEFORE UPDATE ON player_fixture_statistics
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_fixture_events_updated_at ON fixture_events;
CREATE TRIGGER tr_gojo_fixture_events_updated_at
    BEFORE UPDATE ON fixture_events
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_fixture_team_statistics_updated_at ON fixture_team_statistics;
CREATE TRIGGER tr_gojo_fixture_team_statistics_updated_at
    BEFORE UPDATE ON fixture_team_statistics
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_fixture_team_lineups_updated_at ON fixture_team_lineups;
CREATE TRIGGER tr_gojo_fixture_team_lineups_updated_at
    BEFORE UPDATE ON fixture_team_lineups
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_fixture_lineup_players_updated_at ON fixture_lineup_players;
CREATE TRIGGER tr_gojo_fixture_lineup_players_updated_at
    BEFORE UPDATE ON fixture_lineup_players
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_standings_updated_at ON standings;
CREATE TRIGGER tr_gojo_standings_updated_at
    BEFORE UPDATE ON standings
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_gojo_ingestion_watermarks_updated_at ON ingestion_watermarks;
CREATE TRIGGER tr_gojo_ingestion_watermarks_updated_at
    BEFORE UPDATE ON ingestion_watermarks
    FOR EACH ROW
    EXECUTE FUNCTION gojo_set_updated_at();