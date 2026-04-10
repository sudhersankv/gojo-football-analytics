/**
 * job_c — Supabase Edge Function (Deno runtime)
 *
 * Post-match detail ingestion.  Called by the Railway worker when a
 * fixture transitions to terminal status.  For each fixture ID:
 *
 *   1. Fetch full fixture detail from API-Football (/fixtures?id=X)
 *   2. Fetch per-player stats from API-Football (/fixtures/players?fixture=X)
 *   3. Upsert into Postgres:
 *      - fixtures (final score / status)
 *      - fixture_events
 *      - fixture_team_statistics
 *      - fixture_team_lineups + fixture_lineup_players
 *      - players (seed from lineups + player stats for FK safety)
 *      - player_fixture_statistics
 *   4. Mark fixtures.detail_ingested_at = now()
 *
 * This mirrors Job A Phase 4 but runs per-fixture on demand.
 *
 * Request body:
 *   { "fixture_ids": [123, 456] }
 *
 * Response:
 *   {
 *     "ok": true,
 *     "results": [
 *       { "fixture_id": 123, "status": "ok" },
 *       { "fixture_id": 456, "status": "error", "error": "..." }
 *     ]
 *   }
 *
 * Environment secrets:
 *   SUPABASE_URL              — auto-injected
 *   SUPABASE_SERVICE_ROLE_KEY — auto-injected
 *   API_FOOTBALL_KEY          — API-Football x-apisports-key
 *   ORCHESTRATOR_SECRET       — shared secret for auth with Railway worker
 */

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const API_FOOTBALL_BASE = "https://v3.football.api-sports.io";
const API_FOOTBALL_KEY = Deno.env.get("API_FOOTBALL_KEY") ?? "";
const ORCHESTRATOR_SECRET = Deno.env.get("ORCHESTRATOR_SECRET") ?? "";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

function authorize(req: Request): boolean {
  const auth = req.headers.get("Authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  return token === ORCHESTRATOR_SECRET;
}

// ---------------------------------------------------------------------------
// API-Football helpers
// ---------------------------------------------------------------------------

// deno-lint-ignore no-explicit-any
async function apiFetch(path: string): Promise<any> {
  const url = `${API_FOOTBALL_BASE}${path}`;
  const resp = await fetch(url, {
    headers: { "x-apisports-key": API_FOOTBALL_KEY },
  });
  if (!resp.ok) {
    const text = await resp.text();
    throw new Error(`API-Football ${resp.status}: ${text}`);
  }
  return await resp.json();
}

// ---------------------------------------------------------------------------
// Per-fixture detail ingestion
// ---------------------------------------------------------------------------

interface IngestResult {
  fixture_id: number;
  status: "ok" | "error";
  error?: string;
}

async function ingestFixtureDetail(
  sb: SupabaseClient,
  fixtureId: number,
): Promise<IngestResult> {
  try {
    // ── 1. Fetch full fixture detail ──
    const fixtureData = await apiFetch(`/fixtures?id=${fixtureId}`);
    const items = fixtureData.response ?? [];
    if (items.length === 0) {
      throw new Error("No fixture data returned from API");
    }
    const item = items[0];
    const fx = item.fixture ?? {};
    const goals = item.goals ?? {};
    const score = item.score ?? {};

    // ── 2. Upsert fixtures (final score / status) ──
    const fixtureRow = {
      id: fixtureId,
      status_short: fx.status?.short,
      elapsed: fx.status?.elapsed,
      referee: fx.referee,
      home_goals: goals.home,
      away_goals: goals.away,
      ht_home_goals: score.halftime?.home,
      ht_away_goals: score.halftime?.away,
      ft_home_goals: score.fulltime?.home,
      ft_away_goals: score.fulltime?.away,
      et_home_goals: score.extratime?.home,
      et_away_goals: score.extratime?.away,
      pen_home_goals: score.penalty?.home,
      pen_away_goals: score.penalty?.away,
      home_winner: item.teams?.home?.winner ?? null,
      away_winner: item.teams?.away?.winner ?? null,
    };

    await upsert(sb, "fixtures", fixtureRow, "id");

    // ── 3. Fixture events ──
    const events: unknown[] = item.events ?? [];
    // Delete existing events first, then insert
    await sb.from("fixture_events").delete().eq("fixture_id", fixtureId);

    if (events.length > 0) {
      // deno-lint-ignore no-explicit-any
      const eventRows = events.map((ev: any, idx: number) => ({
        fixture_id: fixtureId,
        event_index: idx,
        team_id: ev.team?.id,
        minute: ev.time?.elapsed ?? 0,
        minute_extra: ev.time?.extra ?? null,
        event_type: ev.type ?? "Unknown",
        detail: ev.detail ?? null,
        comments: ev.comments ?? null,
        player_api_id: ev.player?.id ?? null,
        player_name: ev.player?.name ?? null,
        related_player_api_id: ev.assist?.id ?? null,
        related_player_name: ev.assist?.name ?? null,
      }));

      const { error } = await sb.from("fixture_events").insert(eventRows);
      if (error) console.error(`Events insert error for ${fixtureId}:`, error.message);
    }

    // ── 4. Team statistics ──
    const statistics: unknown[] = item.statistics ?? [];
    await sb.from("fixture_team_statistics").delete().eq("fixture_id", fixtureId);

    // deno-lint-ignore no-explicit-any
    const statRows: any[] = [];
    // deno-lint-ignore no-explicit-any
    for (const block of statistics as any[]) {
      const teamId = block.team?.id;
      if (!teamId) continue;
      // deno-lint-ignore no-explicit-any
      for (const s of (block.statistics ?? []) as any[]) {
        statRows.push({
          fixture_id: fixtureId,
          team_id: teamId,
          stat_type: s.type ?? "unknown",
          stat_value: s.value != null ? String(s.value) : null,
        });
      }
    }
    if (statRows.length > 0) {
      const { error } = await sb.from("fixture_team_statistics").insert(statRows);
      if (error) console.error(`Team stats insert error for ${fixtureId}:`, error.message);
    }

    // ── 5. Lineups ──
    const lineups: unknown[] = item.lineups ?? [];
    await sb.from("fixture_lineup_players").delete().eq("fixture_id", fixtureId);
    await sb.from("fixture_team_lineups").delete().eq("fixture_id", fixtureId);

    // deno-lint-ignore no-explicit-any
    const lineupMetaRows: any[] = [];
    // deno-lint-ignore no-explicit-any
    const lineupPlayerRows: any[] = [];
    // deno-lint-ignore no-explicit-any
    const seedPlayers: any[] = [];

    // deno-lint-ignore no-explicit-any
    for (const block of lineups as any[]) {
      const teamId = block.team?.id;
      if (!teamId) continue;

      lineupMetaRows.push({
        fixture_id: fixtureId,
        team_id: teamId,
        formation: block.formation ?? null,
        coach_api_id: block.coach?.id ?? null,
        coach_name: block.coach?.name ?? null,
        coach_photo_url: block.coach?.photo ?? null,
        colors: block.team?.colors ?? null,
      });

      for (const key of ["startXI", "substitutes"]) {
        // deno-lint-ignore no-explicit-any
        for (const slot of (block[key] ?? []) as any[]) {
          const p = slot.player ?? {};
          if (!p.id) continue;

          seedPlayers.push({
            id: p.id,
            name: p.name ?? "?",
          });

          lineupPlayerRows.push({
            fixture_id: fixtureId,
            team_id: teamId,
            player_api_id: p.id,
            player_name: p.name ?? null,
            is_starter: key === "startXI",
            shirt_number: p.number ?? null,
            position_short: p.pos ?? null,
            grid: p.grid ?? null,
            player_id: p.id,
          });
        }
      }
    }

    // Seed players for FK safety
    for (const sp of seedPlayers) {
      await upsert(sb, "players", sp, "id");
    }

    if (lineupMetaRows.length > 0) {
      const { error } = await sb.from("fixture_team_lineups").insert(lineupMetaRows);
      if (error) console.error(`Lineups insert error for ${fixtureId}:`, error.message);
    }
    if (lineupPlayerRows.length > 0) {
      const { error } = await sb.from("fixture_lineup_players").insert(lineupPlayerRows);
      if (error) console.error(`Lineup players insert error for ${fixtureId}:`, error.message);
    }

    // ── 6. Player fixture statistics (separate API call) ──
    const playerData = await apiFetch(`/fixtures/players?fixture=${fixtureId}`);

    // Resolve season_id once (same for all players in this fixture)
    const { data: fixRow } = await sb.from("fixtures").select("season_id").eq("id", fixtureId).single();
    const seasonId = fixRow?.season_id;

    // deno-lint-ignore no-explicit-any
    const playerStatRows: any[] = [];

    if (seasonId) {
      // deno-lint-ignore no-explicit-any
      for (const grp of (playerData.response ?? []) as any[]) {
        const teamId = grp.team?.id;
        // deno-lint-ignore no-explicit-any
        for (const p of (grp.players ?? []) as any[]) {
          const player = p.player ?? {};
          const pid = player.id;
          if (!pid || !teamId) continue;

          // Seed player row
          await upsert(sb, "players", {
            id: pid,
            name: player.name ?? "?",
            photo_url: player.photo ?? null,
          }, "id");

          const st = (p.statistics ?? [])[0];
          if (!st) continue;

          const games = st.games ?? {};
          const goalsObj = st.goals ?? {};
          const shots = st.shots ?? {};
          const passes = st.passes ?? {};
          const tackles = st.tackles ?? {};
          const cards = st.cards ?? {};
          const sub = games.substitute;

          playerStatRows.push({
            fixture_id: fixtureId,
            season_id: seasonId,
            player_id: pid,
            team_id: teamId,
            position: games.position ?? null,
            rating: games.rating ? parseFloat(games.rating) : null,
            minutes: games.minutes ?? null,
            number: games.number ?? null,
            starter: sub != null ? !sub : null,
            substitute: sub ?? null,
            goals: goalsObj.total ?? null,
            assists: goalsObj.assists ?? null,
            shots_total: shots.total ?? null,
            shots_on: shots.on ?? null,
            passes_total: passes.total ?? null,
            passes_key: passes.key ?? null,
            passes_acc: passes.accuracy ?? null,
            tackles_total: tackles.total ?? null,
            interceptions: tackles.interceptions ?? null,
            yellow_cards: cards.yellow ?? null,
            red_cards: cards.red ?? null,
            extra: st,
          });
        }
      }
    } else {
      console.warn(`job_c: fixture ${fixtureId} has no season_id — skipping player stats`);
    }

    // Delete existing player stats for this fixture, then insert fresh
    await sb.from("player_fixture_statistics").delete().eq("fixture_id", fixtureId);
    if (playerStatRows.length > 0) {
      const { error } = await sb.from("player_fixture_statistics").insert(playerStatRows);
      if (error) console.error(`Player stats insert error for ${fixtureId}:`, error.message);
    }

    // ── 7. Mark detail_ingested_at ──
    await sb.from("fixtures").update({
      detail_ingested_at: new Date().toISOString(),
      detail_ingest_attempts: 0,
      detail_ingest_last_error: null,
    }).eq("id", fixtureId);

    console.log(`job_c: fixture ${fixtureId} ingested successfully`);
    return { fixture_id: fixtureId, status: "ok" };

  } catch (err) {
    const errorMsg = String(err);
    console.error(`job_c: fixture ${fixtureId} failed:`, errorMsg);

    // Record the failure — increment attempts, store error
    try {
      const { data: row } = await sb
        .from("fixtures")
        .select("detail_ingest_attempts")
        .eq("id", fixtureId)
        .single();
      const prevAttempts = row?.detail_ingest_attempts ?? 0;

      await sb.from("fixtures").update({
        detail_ingest_attempts: prevAttempts + 1,
        detail_ingest_last_error: errorMsg.substring(0, 500),
      }).eq("id", fixtureId);
    } catch (e2) {
      console.error(`job_c: could not record error for ${fixtureId}:`, e2);
    }

    return { fixture_id: fixtureId, status: "error", error: errorMsg };
  }
}

// ---------------------------------------------------------------------------
// Upsert helper
// ---------------------------------------------------------------------------

async function upsert(
  sb: SupabaseClient,
  table: string,
  // deno-lint-ignore no-explicit-any
  row: Record<string, any>,
  onConflict: string,
): Promise<void> {
  const { error } = await sb.from(table).upsert(row, { onConflict, ignoreDuplicates: false });
  if (error) {
    console.error(`Upsert ${table} error:`, error.message);
  }
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
    const body = await req.json();
    const fixtureIds: number[] = body.fixture_ids ?? [];

    if (fixtureIds.length === 0) {
      return new Response(
        JSON.stringify({ ok: true, results: [] }),
        { headers: { "Content-Type": "application/json" } },
      );
    }

    console.log(`job_c: processing ${fixtureIds.length} fixtures: ${fixtureIds}`);

    const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
    const results: IngestResult[] = [];

    for (const fid of fixtureIds) {
      const result = await ingestFixtureDetail(sb, fid);
      results.push(result);
    }

    const okCount = results.filter((r) => r.status === "ok").length;
    const errCount = results.filter((r) => r.status === "error").length;
    console.log(`job_c: done. ${okCount} ok, ${errCount} errors`);

    return new Response(
      JSON.stringify({ ok: errCount === 0, results }),
      { headers: { "Content-Type": "application/json" } },
    );
  } catch (err) {
    console.error("job_c error:", err);
    return new Response(
      JSON.stringify({ ok: false, error: String(err) }),
      { status: 500, headers: { "Content-Type": "application/json" } },
    );
  }
});
