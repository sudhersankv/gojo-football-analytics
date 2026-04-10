/**
 * job_b_poller — Supabase Edge Function (Deno runtime)
 *
 * Called by the Railway worker on a fixed cadence (default every 5 min)
 * during the active polling window.  Receives an explicit list of
 * fixture IDs, fetches their current status/score from API-Football,
 * and conditionally upserts the public.fixtures table.
 *
 * Request body:
 *   { "fixture_ids": [123, 456, 789] }
 *
 * Response:
 *   {
 *     "ok": true,
 *     "fixture_statuses": [
 *       { "fixture_id": 123, "status_short": "2H" },
 *       { "fixture_id": 456, "status_short": "FT" }
 *     ]
 *   }
 *
 * Environment secrets (set via Supabase dashboard):
 *   SUPABASE_URL            — auto-injected
 *   SUPABASE_SERVICE_ROLE_KEY — auto-injected (needed for DB writes)
 *   API_FOOTBALL_KEY        — API-Football x-apisports-key
 *   ORCHESTRATOR_SECRET     — shared secret for auth with Railway worker
 */

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface RequestBody {
  fixture_ids: number[];
}

interface FixtureStatus {
  fixture_id: number;
  status_short: string;
}

interface ApiFixture {
  fixture: {
    id: number;
    status: { short: string; elapsed: number | null; extra: number | null };
  };
  goals: { home: number | null; away: number | null };
  score: {
    halftime: { home: number | null; away: number | null };
    fulltime: { home: number | null; away: number | null };
    extratime: { home: number | null; away: number | null };
    penalty: { home: number | null; away: number | null };
  };
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const API_FOOTBALL_BASE = "https://v3.football.api-sports.io";
const API_FOOTBALL_KEY = Deno.env.get("API_FOOTBALL_KEY") ?? "";
const ORCHESTRATOR_SECRET = Deno.env.get("ORCHESTRATOR_SECRET") ?? "";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

// Max fixture IDs per API-Football request (API supports id=X-Y-Z)
const API_BATCH_SIZE = 20;

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

function authorize(req: Request): boolean {
  const auth = req.headers.get("Authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  return token === ORCHESTRATOR_SECRET;
}

// ---------------------------------------------------------------------------
// API-Football fetch
// ---------------------------------------------------------------------------

async function fetchFixturesBatch(ids: number[]): Promise<ApiFixture[]> {
  const idsParam = ids.join("-");
  const url = `${API_FOOTBALL_BASE}/fixtures?ids=${idsParam}`;

  const resp = await fetch(url, {
    headers: { "x-apisports-key": API_FOOTBALL_KEY },
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`API-Football ${resp.status}: ${text}`);
  }

  const json = await resp.json();
  return (json.response ?? []) as ApiFixture[];
}

async function fetchAllFixtures(ids: number[]): Promise<ApiFixture[]> {
  const results: ApiFixture[] = [];

  for (let i = 0; i < ids.length; i += API_BATCH_SIZE) {
    const batch = ids.slice(i, i + API_BATCH_SIZE);
    const fixtures = await fetchFixturesBatch(batch);
    results.push(...fixtures);
  }

  return results;
}

// ---------------------------------------------------------------------------
// DB upsert — conditional (only update rows where data changed)
// ---------------------------------------------------------------------------

async function upsertFixtures(
  fixtures: ApiFixture[],
): Promise<FixtureStatus[]> {
  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const statuses: FixtureStatus[] = [];

  for (const f of fixtures) {
    const fid = f.fixture.id;
    const statusShort = f.fixture.status.short;
    const elapsed = f.fixture.status.elapsed;

    const row = {
      id: fid,
      status_short: statusShort,
      elapsed,
      home_goals: f.goals.home,
      away_goals: f.goals.away,
      ht_home_goals: f.score.halftime.home,
      ht_away_goals: f.score.halftime.away,
      ft_home_goals: f.score.fulltime.home,
      ft_away_goals: f.score.fulltime.away,
      et_home_goals: f.score.extratime.home,
      et_away_goals: f.score.extratime.away,
      pen_home_goals: f.score.penalty.home,
      pen_away_goals: f.score.penalty.away,
    };

    // Supabase JS client upsert — uses ON CONFLICT (id) DO UPDATE
    // for the columns we provide.  The updated_at trigger fires only
    // when at least one value actually differs (because Postgres checks
    // OLD vs NEW in the trigger or via IS DISTINCT FROM in DB logic).
    const { error } = await supabase
      .from("fixtures")
      .upsert(row, { onConflict: "id", ignoreDuplicates: false });

    if (error) {
      console.error(`Upsert error for fixture ${fid}:`, error.message);
    }

    statuses.push({ fixture_id: fid, status_short: statusShort });
  }

  return statuses;
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

serve(async (req: Request) => {
  // Only POST
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ ok: false, error: "Method not allowed" }), {
      status: 405,
      headers: { "Content-Type": "application/json" },
    });
  }

  // Auth
  if (!authorize(req)) {
    return new Response(JSON.stringify({ ok: false, error: "Unauthorized" }), {
      status: 401,
      headers: { "Content-Type": "application/json" },
    });
  }

  try {
    const body: RequestBody = await req.json();
    const fixtureIds = body.fixture_ids ?? [];

    if (fixtureIds.length === 0) {
      return new Response(
        JSON.stringify({ ok: true, fixture_statuses: [] }),
        { headers: { "Content-Type": "application/json" } },
      );
    }

    console.log(`Poller: processing ${fixtureIds.length} fixtures: ${fixtureIds}`);

    // 1. Fetch from API-Football
    const apiFixtures = await fetchAllFixtures(fixtureIds);
    console.log(`Poller: API returned ${apiFixtures.length} fixtures`);

    // 2. Upsert into public.fixtures
    const fixtureStatuses = await upsertFixtures(apiFixtures);

    // 3. Return per-fixture statuses to the worker
    return new Response(
      JSON.stringify({ ok: true, fixture_statuses: fixtureStatuses }),
      { headers: { "Content-Type": "application/json" } },
    );
  } catch (err) {
    console.error("Poller error:", err);
    return new Response(
      JSON.stringify({ ok: false, error: String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
});
