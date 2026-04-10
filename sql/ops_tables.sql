-- Ops tables for Job B orchestration (no poller table).
CREATE SCHEMA IF NOT EXISTS ops;

-- 1) One row per league-day, planned by the worker
CREATE TABLE IF NOT EXISTS ops.job_b_day_plan (
  league_id        BIGINT NOT NULL,
  season_year      INT    NOT NULL,
  date_local       DATE   NOT NULL,

  fixture_count    INT    NOT NULL DEFAULT 0,
  status           TEXT   NOT NULL DEFAULT 'inactive',  -- inactive | active | ended

  window_start_utc TIMESTAMPTZ,
  window_end_utc   TIMESTAMPTZ,

  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (league_id, season_year, date_local)
);

-- 2) One row per fixture for that day (seeded by planner; updated each tick)
CREATE TABLE IF NOT EXISTS ops.job_b_fixture_watch (
  fixture_id    BIGINT PRIMARY KEY,

  league_id     BIGINT NOT NULL,
  season_year   INT    NOT NULL,
  date_local    DATE   NOT NULL,
  utc_kickoff   TIMESTAMPTZ,

  status_short  TEXT   NOT NULL DEFAULT 'NS',
  phase         TEXT   NOT NULL DEFAULT 'scheduled',     -- scheduled | live | terminal
  is_terminal   BOOLEAN NOT NULL DEFAULT false,
  terminal_at   TIMESTAMPTZ,

  last_tick_at  TIMESTAMPTZ,  -- “job_b tick” timestamp (single stage)
  last_error    TEXT,

  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_job_b_fixture_watch_day
  ON ops.job_b_fixture_watch (league_id, season_year, date_local);

CREATE INDEX IF NOT EXISTS idx_job_b_fixture_watch_active
  ON ops.job_b_fixture_watch (league_id, date_local)
  WHERE is_terminal = false;

-- updated_at triggers (uses your existing public.gojo_set_updated_at())
DROP TRIGGER IF EXISTS tr_ops_day_plan_updated_at ON ops.job_b_day_plan;
CREATE TRIGGER tr_ops_day_plan_updated_at
  BEFORE UPDATE ON ops.job_b_day_plan
  FOR EACH ROW
  EXECUTE FUNCTION public.gojo_set_updated_at();

DROP TRIGGER IF EXISTS tr_ops_fixture_watch_updated_at ON ops.job_b_fixture_watch;
CREATE TRIGGER tr_ops_fixture_watch_updated_at
  BEFORE UPDATE ON ops.job_b_fixture_watch
  FOR EACH ROW
  EXECUTE FUNCTION public.gojo_set_updated_at();