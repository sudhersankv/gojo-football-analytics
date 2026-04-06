/**
 * Extract deep stats from the `extra` JSONB column of player_fixture_statistics
 * or player_season_statistics. The JSONB mirrors the raw API-Football payload.
 *
 * Schema (per-fixture `extra`):
 *   games: { captain, offsides, ... }
 *   goals: { total, conceded, saves }
 *   shots: { total, on }
 *   passes: { total, key, accuracy }
 *   tackles: { total, blocks, interceptions }
 *   duels: { total, won }
 *   dribbles: { attempts, success, past }
 *   fouls: { drawn, committed }
 *   cards: { yellow, yellowred, red }
 *   penalty: { won, committed, scored, missed, saved }
 *
 * Season `extra` wraps in categories like:
 *   games, goals, shots, passes, tackles, duels, dribbles, fouls, cards, penalty, substitutes
 */

function num(val) {
  if (val == null) return 0;
  const n = Number(val);
  return isNaN(n) ? 0 : n;
}

export function extractMatchExtra(extra) {
  if (!extra) return {};
  const games = extra.games || {};
  const goals = extra.goals || {};
  const shots = extra.shots || {};
  const passes = extra.passes || {};
  const tackles = extra.tackles || {};
  const duels = extra.duels || {};
  const dribbles = extra.dribbles || {};
  const fouls = extra.fouls || {};
  const cards = extra.cards || {};
  const penalty = extra.penalty || {};

  return {
    offsides: num(games.offsides),
    captain: !!games.captain,

    goals_conceded: num(goals.conceded),
    saves: num(goals.saves),

    shots_total: num(shots.total),
    shots_on: num(shots.on),

    passes_total: num(passes.total),
    passes_key: num(passes.key),
    passes_accuracy: passes.accuracy,

    tackles_total: num(tackles.total),
    tackles_blocks: num(tackles.blocks),
    interceptions: num(tackles.interceptions),

    duels_total: num(duels.total),
    duels_won: num(duels.won),
    duels_pct: duels.total > 0 ? Math.round((num(duels.won) / num(duels.total)) * 100) : null,

    dribbles_attempts: num(dribbles.attempts),
    dribbles_success: num(dribbles.success),
    dribbles_past: num(dribbles.past),

    fouls_drawn: num(fouls.drawn),
    fouls_committed: num(fouls.committed),

    yellow_cards: num(cards.yellow),
    red_cards: num(cards.red),

    penalty_won: num(penalty.won),
    penalty_scored: num(penalty.scored),
    penalty_missed: num(penalty.missed),
  };
}

export function extractSeasonExtra(extra) {
  if (!extra) return {};
  const games = extra.games || {};
  const goals = extra.goals || {};
  const shots = extra.shots || {};
  const passes = extra.passes || {};
  const tackles = extra.tackles || {};
  const duels = extra.duels || {};
  const dribbles = extra.dribbles || {};
  const fouls = extra.fouls || {};
  const cards = extra.cards || {};
  const penalty = extra.penalty || {};
  const substitutes = extra.substitutes || {};

  return {
    captain: !!games.captain,

    total_goals: num(goals.total),
    goals_conceded: num(goals.conceded),
    saves: num(goals.saves),

    shots_total: num(shots.total),
    shots_on: num(shots.on),

    passes_total: num(passes.total),
    passes_key: num(passes.key),
    passes_accuracy: num(passes.accuracy),

    tackles_total: num(tackles.total),
    tackles_blocks: num(tackles.blocks),
    interceptions: num(tackles.interceptions),

    duels_total: num(duels.total),
    duels_won: num(duels.won),

    dribbles_attempts: num(dribbles.attempts),
    dribbles_success: num(dribbles.success),

    fouls_drawn: num(fouls.drawn),
    fouls_committed: num(fouls.committed),

    yellow_cards: num(cards.yellow),
    red_cards: num(cards.red),

    penalty_won: num(penalty.won),
    penalty_scored: num(penalty.scored),
    penalty_missed: num(penalty.missed),

    sub_in: num(substitutes.in),
    sub_out: num(substitutes.out),
    sub_bench: num(substitutes.bench),
  };
}

export function ratingColor(r) {
  const n = parseFloat(r);
  if (isNaN(n)) return "text-gray-400";
  if (n >= 8) return "text-green-600";
  if (n >= 7) return "text-blue-600";
  if (n >= 6) return "text-yellow-600";
  return "text-red-600";
}

export function ratingBg(r) {
  const n = parseFloat(r);
  if (isNaN(n)) return "bg-gray-100 text-gray-500";
  if (n >= 8) return "bg-green-100 text-green-700";
  if (n >= 7) return "bg-blue-100 text-blue-700";
  if (n >= 6) return "bg-yellow-100 text-yellow-700";
  return "bg-red-100 text-red-700";
}
