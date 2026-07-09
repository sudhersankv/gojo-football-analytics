/**
 * job_d — Supabase Edge Function (Deno runtime)
 *
 * Refreshes aggregate season tables after match detail ingestion (Job C):
 *   - standings
 *   - player_season_statistics
 *
 * Mirrors Job E Phase 5 (standings) + Phase 2 player stats mapping from
 * refresh_league_season.py. No row deletes — inserts new rows and updates
 * rows whose business fields changed (avoid churning updated_at when unchanged).
 *
 * Request body:
 *   { "league_id": 39, "season_year": 2025 }
 *
 * Environment secrets:
 *   SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
 *   API_FOOTBALL_KEY
 *   ORCHESTRATOR_SECRET (Bearer auth, same as worker / job_c)
 */

import { serve } from "https://deno.land/std@0.177.0/http/server.ts";
import { createClient, SupabaseClient } from "https://esm.sh/@supabase/supabase-js@2";

const API_FOOTBALL_BASE = "https://v3.football.api-sports.io";
const API_FOOTBALL_KEY = Deno.env.get("API_FOOTBALL_KEY") ?? "";
const ORCHESTRATOR_SECRET = Deno.env.get("ORCHESTRATOR_SECRET") ?? "";
const SUPABASE_URL = Deno.env.get("SUPABASE_URL") ?? "";
const SUPABASE_SERVICE_KEY = Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? "";

const JOB_D_MAX_ATTEMPTS = 4;
const JOB_D_RETRY_DELAY_MS = 900;

interface JobDRequest {
  league_id: number;
  season_year: number;
}

function authorize(req: Request): boolean {
  const auth = req.headers.get("Authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  return token === ORCHESTRATOR_SECRET;
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

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

function payloadLeagueId(payload: Record<string, unknown>): number | null {
  const lg = (payload["league"] ?? {}) as Record<string, unknown>;
  const lid = lg["id"];
  if (lid == null) return null;
  const n = Number(lid);
  return Number.isFinite(n) ? n : null;
}

function isTargetLeague(payload: Record<string, unknown>, leagueId: number): boolean {
  const lid = payloadLeagueId(payload);
  if (lid == null) return true;
  return lid === leagueId;
}

function normNum(v: unknown): number | null {
  if (v == null) return null;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : null;
}

function ratingsClose(a: unknown, b: unknown): boolean {
  const x = normNum(a);
  const y = normNum(b);
  if (x == null && y == null) return true;
  if (x == null || y == null) return false;
  return Math.abs(x - y) < 1e-9;
}

function jsonEqual(a: unknown, b: unknown): boolean {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
}

/** Equality for numeric DB/API fields and plain equality otherwise. */
function fieldEq(a: unknown, b: unknown): boolean {
  const na = normNum(a);
  const nb = normNum(b);
  if (na != null || nb != null) return na === nb;
  return (a ?? null) === (b ?? null);
}

// deno-lint-ignore no-explicit-any
function parseStandingsRows(seasonId: number, leagueId: number, apiResp: any[]): any[] {
  const rows: Record<string, unknown>[] = [];
  if (!apiResp?.length) return rows;
  const first = apiResp[0];
  if (!isTargetLeague(first as Record<string, unknown>, leagueId)) {
    console.warn(
      `job_d: standings payload league mismatch (got ${payloadLeagueId(first as Record<string, unknown>)}, want ${leagueId})`,
    );
    return rows;
  }
  const groups = first?.league?.standings ?? [];
  const firstGroup = Array.isArray(groups) && groups.length > 0 ? groups[0] : [];
  // deno-lint-ignore no-explicit-any
  for (const r of firstGroup as any[]) {
    const team = r.team ?? {};
    const tid = team.id;
    if (!tid) continue;
    const all = r.all ?? {};
    const goals = all.goals ?? {};
    rows.push({
      season_id: seasonId,
      team_id: tid,
      rank: r.rank,
      points: r.points,
      goals_diff: r.goalsDiff,
      form: r.form ?? null,
      description: r.description ?? null,
      all_played: all.played ?? null,
      all_win: all.win ?? null,
      all_draw: all.draw ?? null,
      all_lose: all.lose ?? null,
      goals_for: goals.for ?? null,
      goals_against: goals.against ?? null,
    });
  }
  return rows;
}

// deno-lint-ignore no-explicit-any
function parsePlayerSeasonRows(seasonId: number, leagueId: number, responseItems: any[]): {
  players: Record<string, unknown>[];
  stats: Record<string, unknown>[];
} {
  const players: Record<string, unknown>[] = [];
  const stats: Record<string, unknown>[] = [];

  // deno-lint-ignore no-explicit-any
  for (const item of responseItems as any[]) {
    const player = item.player ?? {};
    const pid = player.id;
    if (!pid) continue;

    players.push({
      id: pid,
      name: player.name ?? "?",
      firstname: player.firstname ?? null,
      lastname: player.lastname ?? null,
      nationality: player.nationality ?? null,
      photo_url: player.photo ?? null,
    });

    // deno-lint-ignore no-explicit-any
    for (const st of (item.statistics ?? []) as any[]) {
      const stLeague = st.league ?? {};
      if (stLeague.id != null && Number(stLeague.id) !== leagueId) continue;
      const team = st.team ?? {};
      const tid = team.id;
      if (!tid) continue;
      const games = st.games ?? {};
      const goalsObj = st.goals ?? {};
      stats.push({
        season_id: seasonId,
        player_id: pid,
        team_id: tid,
        position: games.position ?? null,
        rating: normNum(games.rating),
        appearances: games.appearences ?? games.appearances ?? null,
        lineups: games.lineups ?? null,
        minutes: games.minutes ?? null,
        number: games.number ?? null,
        goals: goalsObj.total ?? null,
        assists: goalsObj.assists ?? null,
        extra: st,
      });
    }
  }

  return { players, stats };
}

function standingChanged(
  existing: Record<string, unknown> | undefined,
  row: Record<string, unknown>,
): boolean {
  if (!existing) return true;
  const keys = [
    "rank",
    "points",
    "goals_diff",
    "form",
    "description",
    "all_played",
    "all_win",
    "all_draw",
    "all_lose",
    "goals_for",
    "goals_against",
  ];
  return keys.some((k) => !fieldEq(existing[k], row[k]));
}

function playerStatChanged(
  existing: Record<string, unknown> | undefined,
  row: Record<string, unknown>,
): boolean {
  if (!existing) return true;
  if (!ratingsClose(existing["rating"], row["rating"])) return true;
  const keys = [
    "position",
    "appearances",
    "lineups",
    "minutes",
    "number",
    "goals",
    "assists",
  ];
  for (const k of keys) {
    if (!fieldEq(existing[k], row[k])) return true;
  }
  if (!jsonEqual(existing["extra"], row["extra"])) return true;
  return false;
}

async function upsertPlayersIgnoreDuplicates(
  sb: SupabaseClient,
  rows: Record<string, unknown>[],
): Promise<void> {
  if (rows.length === 0) return;
  const chunk = 80;
  for (let i = 0; i < rows.length; i += chunk) {
    const part = rows.slice(i, i + chunk);
    const { error } = await sb.from("players").upsert(part, {
      onConflict: "id",
      ignoreDuplicates: true,
    });
    if (error) throw new Error(`players upsert (seed): ${error.message}`);
  }
}

async function loadStandingsMap(sb: SupabaseClient, seasonId: number): Promise<Map<number, Record<string, unknown>>> {
  const { data, error } = await sb.from("standings").select("*").eq("season_id", seasonId);
  if (error) throw new Error(`standings select: ${error.message}`);
  const m = new Map<number, Record<string, unknown>>();
  for (const r of data ?? []) {
    const tid = r.team_id as number;
    m.set(tid, r as Record<string, unknown>);
  }
  return m;
}

async function loadPlayerStatsMap(
  sb: SupabaseClient,
  seasonId: number,
): Promise<Map<string, Record<string, unknown>>> {
  const { data, error } = await sb.from("player_season_statistics").select("*").eq("season_id", seasonId);
  if (error) throw new Error(`player_season_statistics select: ${error.message}`);
  const m = new Map<string, Record<string, unknown>>();
  for (const r of data ?? []) {
    const pid = r.player_id as number;
    const tid = r.team_id as number;
    m.set(`${pid}:${tid}`, r as Record<string, unknown>);
  }
  return m;
}

async function runJobDOnce(
  sb: SupabaseClient,
  leagueId: number,
  seasonYear: number,
): Promise<{
  standings_inserted: number;
  standings_updated: number;
  standings_skipped: number;
  player_stats_inserted: number;
  player_stats_updated: number;
  player_stats_skipped: number;
  touched: number;
}> {
  const { data: seasonRow, error: seasonErr } = await sb
    .from("seasons")
    .select("id")
    .eq("league_id", leagueId)
    .eq("year", seasonYear)
    .single();

  if (seasonErr || !seasonRow?.id) {
    throw new Error(`season not found for league_id=${leagueId} year=${seasonYear}: ${seasonErr?.message ?? ""}`);
  }
  const seasonId = seasonRow.id as number;

  const standingsData = await apiFetch(`/standings?league=${leagueId}&season=${seasonYear}`);
  const standingRows = parseStandingsRows(seasonId, leagueId, standingsData.response ?? []);

  const existingStandings = await loadStandingsMap(sb, seasonId);
  const standingUpserts: Record<string, unknown>[] = [];
  let standings_inserted = 0;
  let standings_updated = 0;
  let standings_skipped = 0;

  for (const row of standingRows) {
    const tid = row.team_id as number;
    const ex = existingStandings.get(tid);
    if (!ex) {
      standingUpserts.push(row);
      standings_inserted++;
    } else if (standingChanged(ex, row)) {
      standingUpserts.push(row);
      standings_updated++;
    } else {
      standings_skipped++;
    }
  }

  if (standingUpserts.length > 0) {
    const { error } = await sb.from("standings").upsert(standingUpserts, {
      onConflict: "season_id,team_id",
    });
    if (error) throw new Error(`standings upsert: ${error.message}`);
  }

  const playersOut: { players: Record<string, unknown>[]; stats: Record<string, unknown>[] } = {
    players: [],
    stats: [],
  };

  let page = 1;
  let totalPages = 1;
  while (page <= totalPages) {
    const pdata = await apiFetch(`/players?league=${leagueId}&season=${seasonYear}&page=${page}`);
    const paging = pdata.paging ?? {};
    totalPages = Number(paging.total ?? 1);
    const response = pdata.response ?? [];
    const parsed = parsePlayerSeasonRows(seasonId, leagueId, response);
    playersOut.players.push(...parsed.players);
    playersOut.stats.push(...parsed.stats);
    page++;
  }

  const playerDedupe = new Map<number, Record<string, unknown>>();
  for (const p of playersOut.players) {
    const id = p.id as number;
    if (!playerDedupe.has(id)) playerDedupe.set(id, p);
  }
  await upsertPlayersIgnoreDuplicates(sb, [...playerDedupe.values()]);

  const existingStats = await loadPlayerStatsMap(sb, seasonId);
  const statUpserts: Record<string, unknown>[] = [];
  let player_stats_inserted = 0;
  let player_stats_updated = 0;
  let player_stats_skipped = 0;

  for (const row of playersOut.stats) {
    const pid = row.player_id as number;
    const tid = row.team_id as number;
    const key = `${pid}:${tid}`;
    const ex = existingStats.get(key);
    if (!ex) {
      statUpserts.push(row);
      player_stats_inserted++;
    } else if (playerStatChanged(ex, row)) {
      statUpserts.push(row);
      player_stats_updated++;
    } else {
      player_stats_skipped++;
    }
  }

  const chunk = 150;
  for (let i = 0; i < statUpserts.length; i += chunk) {
    const part = statUpserts.slice(i, i + chunk);
    const { error } = await sb.from("player_season_statistics").upsert(part, {
      onConflict: "season_id,player_id,team_id",
    });
    if (error) throw new Error(`player_season_statistics upsert: ${error.message}`);
  }

  const touched =
    standings_inserted +
    standings_updated +
    player_stats_inserted +
    player_stats_updated;

  return {
    standings_inserted,
    standings_updated,
    standings_skipped,
    player_stats_inserted,
    player_stats_updated,
    player_stats_skipped,
    touched,
  };
}

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
    const body = (await req.json()) as JobDRequest;
    const leagueId = body.league_id;
    const seasonYear = body.season_year;

    if (!leagueId || !seasonYear) {
      return new Response(JSON.stringify({ ok: false, error: "league_id and season_year required" }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
    }

    const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

    let lastErr: string | null = null;
    let lastCounts: Record<string, unknown> | null = null;

    for (let attempt = 1; attempt <= JOB_D_MAX_ATTEMPTS; attempt++) {
      try {
        const counts = await runJobDOnce(sb, leagueId, seasonYear);
        lastCounts = counts as unknown as Record<string, unknown>;

        const standingsTouched =
          counts.standings_inserted + counts.standings_updated;
        const playerStatsTouched =
          counts.player_stats_inserted + counts.player_stats_updated;
        const everythingFresh =
          standingsTouched > 0 && playerStatsTouched > 0;

        if (everythingFresh || attempt === JOB_D_MAX_ATTEMPTS) {
          console.log(
            `job_d done league=${leagueId} season=${seasonYear} attempt=${attempt} counts=${JSON.stringify(counts)}`,
          );
          return new Response(
            JSON.stringify({
              ok: true,
              league_id: leagueId,
              season_year: seasonYear,
              attempts: attempt,
              ...counts,
              no_change_after_retries: counts.touched === 0,
              not_fully_fresh_after_retries: !everythingFresh,
            }),
            { headers: { "Content-Type": "application/json" } },
          );
        }

        console.log(
          `job_d: attempt ${attempt}/${JOB_D_MAX_ATTEMPTS} — standings_touched=${standingsTouched} player_stats_touched=${playerStatsTouched}, retrying…`,
        );
        await delay(JOB_D_RETRY_DELAY_MS * attempt);
      } catch (e) {
        lastErr = String(e);
        console.error(`job_d: attempt ${attempt} failed:`, lastErr);
        if (attempt === JOB_D_MAX_ATTEMPTS) {
          return new Response(
            JSON.stringify({
              ok: false,
              error: lastErr,
              attempts: attempt,
              last_counts: lastCounts,
            }),
            { status: 500, headers: { "Content-Type": "application/json" } },
          );
        }
        await delay(JOB_D_RETRY_DELAY_MS * attempt);
      }
    }

    return new Response(JSON.stringify({ ok: false, error: "unexpected job_d exit" }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  } catch (err) {
    console.error("job_d error:", err);
    return new Response(JSON.stringify({ ok: false, error: String(err) }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
});
