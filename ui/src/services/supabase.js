import { createClient } from "@supabase/supabase-js";

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL;
const supabaseKey = import.meta.env.VITE_SUPABASE_ANON_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error(
    "Missing VITE_SUPABASE_URL or VITE_SUPABASE_ANON_KEY. Copy .env.example to .env and fill in values.",
  );
}

export const supabase = createClient(supabaseUrl || "", supabaseKey || "");

// ── Seasons ─────────────────────────────────────────────────────────

export async function getCurrentSeason(leagueId) {
  const { data, error } = await supabase
    .from("seasons")
    .select("*")
    .eq("league_id", leagueId)
    .eq("current", true)
    .limit(1)
    .maybeSingle();

  if (error) throw error;
  if (data) return data;

  const { data: fallback, error: fbErr } = await supabase
    .from("seasons")
    .select("*")
    .eq("league_id", leagueId)
    .order("year", { ascending: false })
    .limit(1)
    .maybeSingle();
  if (fbErr) throw fbErr;
  return fallback;
}

// ── Fixtures ────────────────────────────────────────────────────────

export async function getMatchesByLeague(seasonId) {
  const { data, error } = await supabase
    .from("fixtures")
    .select(
      `
      *,
      home_team:teams!home_team_id(id, name, logo_url),
      away_team:teams!away_team_id(id, name, logo_url)
    `,
    )
    .eq("season_id", seasonId)
    .order("utc_kickoff", { ascending: false });

  if (error) throw error;
  return data || [];
}

export async function getMatchById(fixtureId) {
  const { data, error } = await supabase
    .from("fixtures")
    .select(
      `
      *,
      home_team:teams!home_team_id(id, name, logo_url),
      away_team:teams!away_team_id(id, name, logo_url)
    `,
    )
    .eq("id", fixtureId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

// ── Match Events ────────────────────────────────────────────────────

export async function getMatchEvents(fixtureId) {
  const { data, error } = await supabase
    .from("fixture_events")
    .select("*")
    .eq("fixture_id", fixtureId)
    .order("minute", { ascending: true })
    .order("event_index", { ascending: true });

  if (error) throw error;
  return data || [];
}

// ── Team Statistics ─────────────────────────────────────────────────

export async function getMatchStats(fixtureId) {
  const { data, error } = await supabase
    .from("fixture_team_statistics")
    .select(
      `
      *,
      team:teams!team_id(id, name, logo_url)
    `,
    )
    .eq("fixture_id", fixtureId);

  if (error) throw error;
  return data || [];
}

// ── Player Fixture Statistics ───────────────────────────────────────

export async function getPlayerStats(fixtureId) {
  const { data, error } = await supabase
    .from("player_fixture_statistics")
    .select(
      `
      *,
      player:players!player_id(id, name, photo_url),
      team:teams!team_id(id, name, logo_url)
    `,
    )
    .eq("fixture_id", fixtureId)
    .order("rating", { ascending: false, nullsFirst: false });

  if (error) throw error;
  return data || [];
}

// ── Standings ───────────────────────────────────────────────────────

export async function getStandings(seasonId) {
  const { data, error } = await supabase
    .from("standings")
    .select(
      `
      *,
      team:teams!team_id(id, name, logo_url)
    `,
    )
    .eq("season_id", seasonId)
    .order("rank", { ascending: true });

  if (error) throw error;
  return data || [];
}

// ── Lineups ─────────────────────────────────────────────────────────

export async function getLineups(fixtureId) {
  const { data, error } = await supabase
    .from("fixture_team_lineups")
    .select(
      `
      *,
      team:teams!team_id(id, name, logo_url)
    `,
    )
    .eq("fixture_id", fixtureId);

  if (error) throw error;
  return data || [];
}

export async function getLineupPlayers(fixtureId) {
  const { data, error } = await supabase
    .from("fixture_lineup_players")
    .select("*")
    .eq("fixture_id", fixtureId);

  if (error) throw error;
  return data || [];
}

// ── Player Detail ───────────────────────────────────────────────────

export async function getPlayerById(playerId) {
  const { data, error } = await supabase
    .from("players")
    .select("*")
    .eq("id", playerId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

export async function getPlayerSeasonStats(playerId) {
  const { data, error } = await supabase
    .from("player_season_statistics")
    .select(
      `
      *,
      team:teams!team_id(id, name, logo_url),
      season:seasons!season_id(id, year, league_id)
    `,
    )
    .eq("player_id", playerId)
    .order("season_id", { ascending: false });

  if (error) throw error;
  return data || [];
}

export async function getPlayerMatchStats(playerId, fixtureId) {
  const { data, error } = await supabase
    .from("player_fixture_statistics")
    .select(
      `
      *,
      player:players!player_id(id, name, photo_url),
      team:teams!team_id(id, name, logo_url),
      fixture:fixtures!fixture_id(id, utc_kickoff, status_short, home_goals, away_goals, round,
        home_team:teams!home_team_id(id, name, logo_url),
        away_team:teams!away_team_id(id, name, logo_url)
      )
    `,
    )
    .eq("player_id", playerId)
    .eq("fixture_id", fixtureId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

// ── Team Detail ─────────────────────────────────────────────────────

export async function getTeamById(teamId) {
  const { data, error } = await supabase
    .from("teams")
    .select("*")
    .eq("id", teamId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

export async function getTeamStanding(seasonId, teamId) {
  const { data, error } = await supabase
    .from("standings")
    .select("*")
    .eq("season_id", seasonId)
    .eq("team_id", teamId)
    .maybeSingle();

  if (error) throw error;
  return data;
}

export async function getTeamSquad(seasonId, teamId) {
  const { data, error } = await supabase
    .from("player_season_statistics")
    .select(
      `
      *,
      player:players!player_id(id, name, photo_url)
    `,
    )
    .eq("season_id", seasonId)
    .eq("team_id", teamId)
    .order("appearances", { ascending: false, nullsFirst: false });

  if (error) throw error;
  return data || [];
}

export async function getTeamFixtures(seasonId, teamId, limit = 10) {
  const { data, error } = await supabase
    .from("fixtures")
    .select(
      `
      *,
      home_team:teams!home_team_id(id, name, logo_url),
      away_team:teams!away_team_id(id, name, logo_url)
    `,
    )
    .eq("season_id", seasonId)
    .or(`home_team_id.eq.${teamId},away_team_id.eq.${teamId}`)
    .order("utc_kickoff", { ascending: false })
    .limit(limit);

  if (error) throw error;
  return data || [];
}

// ── Season Stats (leaderboards) ─────────────────────────────────────

export async function getTopScorers(seasonId, limit = 20) {
  const { data, error } = await supabase
    .from("player_season_statistics")
    .select(
      `*, player:players!player_id(id, name, photo_url), team:teams!team_id(id, name, logo_url)`,
    )
    .eq("season_id", seasonId)
    .gt("goals", 0)
    .order("goals", { ascending: false })
    .order("appearances", { ascending: true })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

export async function getTopAssists(seasonId, limit = 20) {
  const { data, error } = await supabase
    .from("player_season_statistics")
    .select(
      `*, player:players!player_id(id, name, photo_url), team:teams!team_id(id, name, logo_url)`,
    )
    .eq("season_id", seasonId)
    .gt("assists", 0)
    .order("assists", { ascending: false })
    .order("appearances", { ascending: true })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

export async function getTopRated(seasonId, limit = 20) {
  const { data, error } = await supabase
    .from("player_season_statistics")
    .select(
      `*, player:players!player_id(id, name, photo_url), team:teams!team_id(id, name, logo_url)`,
    )
    .eq("season_id", seasonId)
    .not("rating", "is", null)
    .gte("appearances", 5)
    .order("rating", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

export async function getMostAppearances(seasonId, limit = 20) {
  const { data, error } = await supabase
    .from("player_season_statistics")
    .select(
      `*, player:players!player_id(id, name, photo_url), team:teams!team_id(id, name, logo_url)`,
    )
    .eq("season_id", seasonId)
    .gt("appearances", 0)
    .order("appearances", { ascending: false })
    .limit(limit);
  if (error) throw error;
  return data || [];
}

export async function getPlayerRecentMatches(playerId, limit = 10) {
  const { data, error } = await supabase
    .from("player_fixture_statistics")
    .select(
      `
      *,
      fixture:fixtures!fixture_id(id, utc_kickoff, status_short, home_goals, away_goals, round,
        home_team:teams!home_team_id(id, name, logo_url),
        away_team:teams!away_team_id(id, name, logo_url)
      ),
      team:teams!team_id(id, name, logo_url)
    `,
    )
    .eq("player_id", playerId)
    .order("fixture_id", { ascending: false })
    .limit(limit);

  if (error) throw error;
  return data || [];
}
