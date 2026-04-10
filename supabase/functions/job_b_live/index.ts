/**
 * job_b_live — Supabase Edge Function (Deno runtime)
 *
 * Called by the Railway worker every 30s during an active match window.
 * Receives an explicit list of fixture IDs, fetches live data from
 * API-Football, writes snapshots to Upstash Redis, and returns
 * per-fixture statuses so the worker can update ops state.
 *
 * This function does NOT write to Postgres.  DB ingestion happens in
 * Job C after the match reaches terminal status.
 *
 * Two-tier Redis writes:
 *   LIGHT (every tick / 30s):
 *     gojo:live:fixture:{id}:latest       → compact JSON snapshot
 *     gojo:live:fixture:{id}:m:{minute}   → compact JSON snapshot (NX)
 *     gojo:live:fixture:{id}:minutes      → ZSET of minute numbers
 *
 *   HEAVY (every 5 elapsed minutes):
 *     gojo:live:fixture:{id}:detail:m:{m} → full events + team stats + lineups
 *     gojo:live:fixture:{id}:players:m:{m}→ per-player stats (separate API call)
 *
 * Request body:
 *   { "fixture_ids": [123, 456, 789] }
 *
 * Response:
 *   {
 *     "ok": true,
 *     "fixture_statuses": [...],
 *     "redis_writes": 12,
 *     "heavy_fetches": 2
 *   }
 *
 * Environment secrets:
 *   API_FOOTBALL_KEY            — API-Football x-apisports-key
 *   ORCHESTRATOR_SECRET         — shared secret for auth with Railway worker
 *   UPSTASH_REDIS_REST_URL     — Upstash REST endpoint
 *   UPSTASH_REDIS_REST_TOKEN   — Upstash bearer token
 */

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface RequestBody {
  fixture_ids: number[];
}

interface FixtureStatus {
  fixture_id: number;
  status_short: string;
  elapsed: number | null;
}

// deno-lint-ignore no-explicit-any
interface ApiFixture {
  fixture: {
    id: number;
    status: { short: string; elapsed: number | null; extra: number | null };
  };
  teams: {
    home: { id: number; name: string };
    away: { id: number; name: string };
  };
  goals: { home: number | null; away: number | null };
  score: {
    halftime: { home: number | null; away: number | null };
    fulltime: { home: number | null; away: number | null };
    extratime: { home: number | null; away: number | null };
    penalty: { home: number | null; away: number | null };
  };
  // deno-lint-ignore no-explicit-any
  events?: any[];
  // deno-lint-ignore no-explicit-any
  statistics?: any[];
  // deno-lint-ignore no-explicit-any
  lineups?: any[];
  // deno-lint-ignore no-explicit-any
  players?: any[];
}

interface Snapshot {
  fixture_id: number;
  status_short: string;
  elapsed: number | null;
  elapsed_extra: number | null;
  captured_at: string;
  score: {
    home: number | null;
    away: number | null;
    ht: { home: number | null; away: number | null };
    ft: { home: number | null; away: number | null };
    et: { home: number | null; away: number | null };
    pen: { home: number | null; away: number | null };
  };
  teams: {
    home: { id: number; name: string };
    away: { id: number; name: string };
  };
  // deno-lint-ignore no-explicit-any
  stats: Record<string, any>;
  events_count: number;
  // deno-lint-ignore no-explicit-any
  last_event: any | null;
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const API_FOOTBALL_BASE = "https://v3.football.api-sports.io";
const API_FOOTBALL_KEY = Deno.env.get("API_FOOTBALL_KEY") ?? "";
const ORCHESTRATOR_SECRET = Deno.env.get("ORCHESTRATOR_SECRET") ?? "";
const REDIS_URL = Deno.env.get("UPSTASH_REDIS_REST_URL") ?? "";
const REDIS_TOKEN = Deno.env.get("UPSTASH_REDIS_REST_TOKEN") ?? "";

const API_BATCH_SIZE = 20;
const REDIS_TTL_SEC = 21600; // 6 hours
const KEY_PREFIX = "gojo:live:fixture";
const HEAVY_INTERVAL_MIN = 5;

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

// deno-lint-ignore no-explicit-any
async function fetchPlayerStats(fixtureId: number): Promise<any[]> {
  const url = `${API_FOOTBALL_BASE}/fixtures/players?fixture=${fixtureId}`;
  const resp = await fetch(url, {
    headers: { "x-apisports-key": API_FOOTBALL_KEY },
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`API-Football players ${resp.status}: ${text}`);
  }

  const json = await resp.json();
  return json.response ?? [];
}

// ---------------------------------------------------------------------------
// Extract statuses from API response
// ---------------------------------------------------------------------------

function extractStatuses(fixtures: ApiFixture[]): FixtureStatus[] {
  return fixtures.map((f) => ({
    fixture_id: f.fixture.id,
    status_short: f.fixture.status.short,
    elapsed: f.fixture.status.elapsed,
  }));
}

// ---------------------------------------------------------------------------
// Upstash Redis REST helpers
// ---------------------------------------------------------------------------

async function redisPipeline(commands: (string | number)[][]): Promise<unknown[]> {
  const resp = await fetch(`${REDIS_URL}/pipeline`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${REDIS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(commands),
  });

  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`Redis pipeline ${resp.status}: ${text}`);
  }

  const data = await resp.json();
  return data as unknown[];
}

// Check if a Redis key already exists (used to skip duplicate heavy fetches)
async function redisExists(key: string): Promise<boolean> {
  const resp = await fetch(`${REDIS_URL}`, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${REDIS_TOKEN}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify(["EXISTS", key]),
  });
  if (!resp.ok) return false;
  const data = await resp.json();
  return data.result === 1;
}

// ---------------------------------------------------------------------------
// Build compact snapshot from API fixture (light write)
// ---------------------------------------------------------------------------

function buildSnapshot(f: ApiFixture): Snapshot {
  const events = f.events ?? [];
  const lastEvent = events.length > 0 ? events[events.length - 1] : null;

  // deno-lint-ignore no-explicit-any
  const stats: Record<string, any> = {};
  for (const block of (f.statistics ?? [])) {
    const teamId = block.team?.id;
    const side = teamId === f.teams?.home?.id ? "home" : "away";
    // deno-lint-ignore no-explicit-any
    const teamStats: Record<string, any> = {};
    for (const s of (block.statistics ?? [])) {
      teamStats[s.type] = s.value;
    }
    stats[side] = teamStats;
  }

  return {
    fixture_id: f.fixture.id,
    status_short: f.fixture.status.short,
    elapsed: f.fixture.status.elapsed,
    elapsed_extra: f.fixture.status.extra,
    captured_at: new Date().toISOString(),
    score: {
      home: f.goals.home,
      away: f.goals.away,
      ht: f.score.halftime,
      ft: f.score.fulltime,
      et: f.score.extratime,
      pen: f.score.penalty,
    },
    teams: {
      home: { id: f.teams?.home?.id, name: f.teams?.home?.name },
      away: { id: f.teams?.away?.id, name: f.teams?.away?.name },
    },
    stats,
    events_count: events.length,
    last_event: lastEvent
      ? {
          minute: lastEvent.time?.elapsed,
          extra: lastEvent.time?.extra,
          type: lastEvent.type,
          detail: lastEvent.detail,
          team: lastEvent.team?.name,
          player: lastEvent.player?.name,
        }
      : null,
  };
}

// ---------------------------------------------------------------------------
// Build detail snapshot (heavy write — events + team stats + lineups)
// ---------------------------------------------------------------------------

// deno-lint-ignore no-explicit-any
function buildDetailSnapshot(f: ApiFixture): Record<string, any> {
  return {
    fixture_id: f.fixture.id,
    status_short: f.fixture.status.short,
    elapsed: f.fixture.status.elapsed,
    captured_at: new Date().toISOString(),
    score: {
      home: f.goals.home,
      away: f.goals.away,
      ht: f.score.halftime,
      ft: f.score.fulltime,
      et: f.score.extratime,
      pen: f.score.penalty,
    },
    teams: {
      home: f.teams?.home,
      away: f.teams?.away,
    },
    events: f.events ?? [],
    statistics: f.statistics ?? [],
    lineups: f.lineups ?? [],
  };
}

// ---------------------------------------------------------------------------
// Determine if this tick is a heavy-write tick for a fixture
// ---------------------------------------------------------------------------

function isHeavyTick(elapsed: number | null): boolean {
  if (elapsed == null || elapsed <= 0) return false;
  return elapsed % HEAVY_INTERVAL_MIN === 0;
}

// ---------------------------------------------------------------------------
// Write light snapshots to Redis (every tick)
// ---------------------------------------------------------------------------

async function writeLightBuckets(fixtures: ApiFixture[]): Promise<number> {
  if (fixtures.length === 0) return 0;

  const commands: (string | number)[][] = [];

  for (const f of fixtures) {
    const fid = f.fixture.id;
    const snapshot = buildSnapshot(f);
    const json = JSON.stringify(snapshot);
    const minute = f.fixture.status.elapsed;

    const latestKey = `${KEY_PREFIX}:${fid}:latest`;
    const minutesKey = `${KEY_PREFIX}:${fid}:minutes`;

    commands.push(["SET", latestKey, json, "EX", REDIS_TTL_SEC]);

    if (minute != null) {
      const bucketKey = `${KEY_PREFIX}:${fid}:m:${minute}`;
      commands.push(["SET", bucketKey, json, "EX", REDIS_TTL_SEC, "NX"]);
      commands.push(["ZADD", minutesKey, minute, String(minute)]);
      commands.push(["EXPIRE", minutesKey, REDIS_TTL_SEC]);
    }
  }

  if (commands.length === 0) return 0;

  await redisPipeline(commands);
  return commands.length;
}

// ---------------------------------------------------------------------------
// Write heavy snapshots to Redis (every 5 elapsed minutes)
// Fetches /fixtures/players per fixture (1 API call each)
// ---------------------------------------------------------------------------

async function writeHeavyBuckets(fixtures: ApiFixture[]): Promise<{ commands: number; fetches: number }> {
  const heavyFixtures = fixtures.filter((f) => isHeavyTick(f.fixture.status.elapsed));
  if (heavyFixtures.length === 0) return { commands: 0, fetches: 0 };

  const commands: (string | number)[][] = [];
  let apiFetches = 0;

  for (const f of heavyFixtures) {
    const fid = f.fixture.id;
    const minute = f.fixture.status.elapsed!;

    const detailKey = `${KEY_PREFIX}:${fid}:detail:m:${minute}`;
    const playersKey = `${KEY_PREFIX}:${fid}:players:m:${minute}`;

    // Skip if this 5-min bucket was already written (dedup across 30s ticks)
    const alreadyExists = await redisExists(detailKey);
    if (alreadyExists) {
      console.log(`Heavy: fixture ${fid} m:${minute} already written — skip`);
      continue;
    }

    // Detail snapshot (events/stats/lineups — already in hand, no extra API call)
    const detail = buildDetailSnapshot(f);
    commands.push(["SET", detailKey, JSON.stringify(detail), "EX", REDIS_TTL_SEC]);

    // Player stats — requires a separate API call
    try {
      const playerData = await fetchPlayerStats(fid);
      apiFetches++;
      const playersSnapshot = {
        fixture_id: fid,
        elapsed: minute,
        captured_at: new Date().toISOString(),
        teams: playerData,
      };
      commands.push(["SET", playersKey, JSON.stringify(playersSnapshot), "EX", REDIS_TTL_SEC]);
    } catch (err) {
      console.error(`Heavy: player stats fetch failed for fixture ${fid}:`, err);
    }
  }

  if (commands.length === 0) return { commands: 0, fetches: apiFetches };

  await redisPipeline(commands);
  return { commands: commands.length, fetches: apiFetches };
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

serve(async (req: Request) => {
  if (req.method !== "POST") {
    return new Response(JSON.stringify({ ok: false, error: "Method not allowed" }), {
      status: 405,
      headers: { "Content-Type": "application/json" },
    });
  }

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
        JSON.stringify({ ok: true, fixture_statuses: [], redis_writes: 0, heavy_fetches: 0 }),
        { headers: { "Content-Type": "application/json" } },
      );
    }

    console.log(`job_b_live: processing ${fixtureIds.length} fixtures: ${fixtureIds}`);

    // 1. Fetch from API-Football (batched, light)
    const apiFixtures = await fetchAllFixtures(fixtureIds);
    console.log(`job_b_live: API returned ${apiFixtures.length} fixtures`);

    if (!REDIS_URL || !REDIS_TOKEN) {
      console.warn("Redis not configured — skipping all writes");
      return new Response(
        JSON.stringify({
          ok: true,
          fixture_statuses: extractStatuses(apiFixtures),
          redis_writes: 0,
          heavy_fetches: 0,
        }),
        { headers: { "Content-Type": "application/json" } },
      );
    }

    // 2. Light Redis writes (every tick)
    const lightCount = await writeLightBuckets(apiFixtures);
    console.log(`Light writes: ${lightCount} commands`);

    // 3. Heavy Redis writes (every 5 elapsed minutes — extra API calls)
    const heavy = await writeHeavyBuckets(apiFixtures);
    console.log(`Heavy writes: ${heavy.commands} commands, ${heavy.fetches} API fetches`);

    // 4. Extract and return per-fixture statuses
    const fixtureStatuses = extractStatuses(apiFixtures);

    return new Response(
      JSON.stringify({
        ok: true,
        fixture_statuses: fixtureStatuses,
        redis_writes: lightCount + heavy.commands,
        heavy_fetches: heavy.fetches,
      }),
      { headers: { "Content-Type": "application/json" } },
    );
  } catch (err) {
    console.error("job_b_live error:", err);
    return new Response(
      JSON.stringify({ ok: false, error: String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
});
