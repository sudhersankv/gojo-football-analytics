import { useEffect, useState } from "react";
import { useParams, useSearchParams, Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import {
  getPlayerById, getPlayerSeasonStats, getPlayerRecentMatches, getPlayerMatchStats,
} from "../services/supabase";
import { extractMatchExtra, extractSeasonExtra, ratingBg } from "../utils/statsHelpers";
import LeagueHeader from "../components/LeagueHeader";
import ImageWithFallback from "../components/ImageWithFallback";
import PlayerRadarChart from "../components/PlayerRadarChart";
import Spinner from "../components/Spinner";

function StatCard({ label, value, highlight, small }) {
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-3 text-center">
      <div
        className={`font-bold ${small ? "text-base" : "text-xl"} ${highlight ? "" : "text-gray-900"}`}
        style={highlight ? { color: "#37003c" } : undefined}
      >
        {value ?? "—"}
      </div>
      <div className="text-[10px] font-medium text-gray-500 uppercase tracking-wider mt-0.5">
        {label}
      </div>
    </div>
  );
}

export default function PlayerDetail() {
  const { id } = useParams();
  const [searchParams] = useSearchParams();
  const fixtureId = searchParams.get("fixture_id");
  const { currentTheme, setCurrentTheme } = useTheme();
  const [player, setPlayer] = useState(null);
  const [seasonStats, setSeasonStats] = useState([]);
  const [recentMatches, setRecentMatches] = useState([]);
  const [matchStats, setMatchStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [view, setView] = useState(fixtureId ? "match" : "season");

  useEffect(() => {
    if (currentTheme === "gojo") setCurrentTheme("premier-league");
  }, [currentTheme, setCurrentTheme]);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        const promises = [
          getPlayerById(id),
          getPlayerSeasonStats(id),
          getPlayerRecentMatches(id, 10),
        ];
        if (fixtureId) promises.push(getPlayerMatchStats(id, fixtureId));

        const results = await Promise.all(promises);
        if (cancelled) return;
        setPlayer(results[0]);
        setSeasonStats(results[1]);
        setRecentMatches(results[2]);
        if (results[3]) setMatchStats(results[3]);
      } catch (err) {
        if (!cancelled) setError(err.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => { cancelled = true; };
  }, [id, fixtureId]);

  if (loading) return <><LeagueHeader /><Spinner /></>;
  if (error) return <><LeagueHeader /><div className="mx-auto max-w-3xl px-4 py-16 text-center text-red-600 text-sm">{error}</div></>;
  if (!player) return <><LeagueHeader /><p className="py-16 text-center text-gray-500">Player not found.</p></>;

  const mainSeason = seasonStats[0];

  const backTo = fixtureId
    ? `/league/${currentTheme}/match/${fixtureId}`
    : `/league/${currentTheme}`;

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <LeagueHeader />

      <main className="mx-auto max-w-3xl px-4 py-6">
        <Link
          to={backTo}
          className="mb-5 inline-flex items-center gap-1 text-sm text-gray-500 hover:text-pl-purple transition-colors"
        >
          ← {fixtureId ? "Back to Match" : "Back"}
        </Link>

        {/* Player header */}
        <div className="mb-6 rounded-xl text-white p-6" style={{ backgroundColor: "#37003c" }}>
          <div className="flex items-center gap-4">
            <ImageWithFallback
              src={player.photo_url}
              type="player"
              className="h-20 w-20 rounded-full object-cover border-2 border-white/20"
            />
            <div>
              <h1 className="text-2xl font-bold">{player.name}</h1>
              {mainSeason?.team && (
                <Link
                  to={`/league/${currentTheme}/team/${mainSeason.team.id}`}
                  className="flex items-center gap-2 mt-1 opacity-80 hover:opacity-100 transition-opacity"
                >
                  <ImageWithFallback src={mainSeason.team.logo_url} type="team" className="h-5 w-5 object-contain" />
                  <span className="text-sm">{mainSeason.team.name}</span>
                </Link>
              )}
              <div className="flex items-center gap-3 mt-1.5 text-xs opacity-60">
                {player.nationality && <span>{player.nationality}</span>}
                {player.birth_date && <span>Born: {new Date(player.birth_date).toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" })}</span>}
                {mainSeason?.position && <span>{mainSeason.position === "Attacker" ? "Forward" : mainSeason.position}</span>}
                {mainSeason?.number && <span>#{mainSeason.number}</span>}
              </div>
            </div>
          </div>
        </div>

        {/* View toggle */}
        {fixtureId && matchStats && (
          <div className="mb-5 flex gap-1 bg-gray-100 rounded-lg p-1">
            {[
              { key: "match", label: "Match Performance" },
              { key: "season", label: "Season Overview" },
            ].map((t) => (
              <button
                key={t.key}
                onClick={() => setView(t.key)}
                className={`flex-1 rounded-md px-4 py-2 text-sm font-medium transition-all ${
                  view === t.key
                    ? "bg-white text-gray-900 shadow-sm"
                    : "text-gray-500 hover:text-gray-700"
                }`}
              >
                {t.label}
              </button>
            ))}
          </div>
        )}

        {/* Match Performance */}
        {view === "match" && matchStats && <MatchView stats={matchStats} currentTheme={currentTheme} />}

        {/* Season Stats */}
        {view === "season" && mainSeason && <SeasonView stats={mainSeason} />}

        {/* Fallback */}
        {view === "match" && !matchStats && mainSeason && (
          <section className="mb-6">
            <p className="text-sm text-gray-500 mb-4">No match-specific stats available. Showing season stats.</p>
            <SeasonView stats={mainSeason} />
          </section>
        )}

        {/* Recent Matches */}
        {recentMatches.length > 0 && (
          <section>
            <h3 className="text-sm font-semibold text-gray-900 mb-3 uppercase tracking-wider">
              Recent Matches
            </h3>
            <div className="rounded-lg border border-gray-200 bg-white overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                    <th className="py-2.5 px-3 text-left font-semibold">Match</th>
                    <th className="py-2.5 px-3 text-center font-semibold">Min</th>
                    <th className="py-2.5 px-3 text-center font-semibold">G</th>
                    <th className="py-2.5 px-3 text-center font-semibold">A</th>
                    <th className="py-2.5 px-3 text-center font-semibold">Sh</th>
                    <th className="py-2.5 px-3 text-center font-semibold">KP</th>
                    <th className="py-2.5 px-3 text-center font-semibold">Rating</th>
                  </tr>
                </thead>
                <tbody>
                  {recentMatches.map((rm) => {
                    const fix = rm.fixture;
                    if (!fix) return null;
                    const homeName = fix.home_team?.name || "?";
                    const awayName = fix.away_team?.name || "?";
                    const score = `${fix.home_goals ?? "-"} — ${fix.away_goals ?? "-"}`;
                    const rating = rm.rating != null ? Number(rm.rating).toFixed(1) : "—";
                    const extra = extractMatchExtra(rm.extra);

                    return (
                      <tr key={rm.fixture_id} className="border-t border-gray-100 hover:bg-gray-50 transition-colors">
                        <td className="py-2 px-3">
                          <Link
                            to={`/league/${currentTheme}/player/${id}?fixture_id=${fix.id}`}
                            className="hover:text-pl-purple transition-colors"
                          >
                            <div className="flex items-center gap-1.5">
                              <ImageWithFallback src={fix.home_team?.logo_url} type="team" className="h-4 w-4 object-contain" />
                              <span className="text-xs font-medium text-gray-800">{homeName}</span>
                              <span className="text-xs text-gray-500 font-bold mx-0.5">{score}</span>
                              <span className="text-xs font-medium text-gray-800">{awayName}</span>
                              <ImageWithFallback src={fix.away_team?.logo_url} type="team" className="h-4 w-4 object-contain" />
                            </div>
                          </Link>
                        </td>
                        <td className="py-2 px-3 text-center text-gray-700 tabular-nums">{rm.minutes ?? "—"}</td>
                        <td className="py-2 px-3 text-center text-gray-700 tabular-nums font-semibold">{rm.goals ?? 0}</td>
                        <td className="py-2 px-3 text-center text-gray-700 tabular-nums">{rm.assists ?? 0}</td>
                        <td className="py-2 px-3 text-center text-gray-700 tabular-nums">{extra.shots_total || rm.shots_total || "—"}</td>
                        <td className="py-2 px-3 text-center text-gray-700 tabular-nums">{extra.passes_key || rm.passes_key || "—"}</td>
                        <td className="py-2 px-3 text-center">
                          <span className={`inline-block px-2 py-0.5 rounded text-xs font-bold ${ratingBg(rating)}`}>
                            {rating}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>
        )}
      </main>
    </div>
  );
}

function MatchView({ stats, currentTheme }) {
  const extra = extractMatchExtra(stats.extra);
  const merged = { ...stats, ...extra };

  return (
    <section className="mb-6">
      <h3 className="text-sm font-semibold text-gray-900 mb-3 uppercase tracking-wider">
        Match Performance
      </h3>
      {stats.fixture && (
        <Link
          to={`/league/${currentTheme}/match/${stats.fixture.id}`}
          className="mb-3 flex items-center gap-2 text-sm text-gray-600 hover:text-pl-purple transition-colors"
        >
          <ImageWithFallback src={stats.fixture.home_team?.logo_url} type="team" className="h-4 w-4 object-contain" />
          <span>{stats.fixture.home_team?.name}</span>
          <span className="font-bold text-gray-800">{stats.fixture.home_goals} — {stats.fixture.away_goals}</span>
          <span>{stats.fixture.away_team?.name}</span>
          <ImageWithFallback src={stats.fixture.away_team?.logo_url} type="team" className="h-4 w-4 object-contain" />
        </Link>
      )}

      {/* Core stats */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Minutes" value={merged.minutes} />
        <StatCard label="Goals" value={merged.goals} highlight />
        <StatCard label="Assists" value={merged.assists} highlight />
        <StatCard label="Rating" value={stats.rating ? Number(stats.rating).toFixed(1) : null} highlight />
      </div>

      {/* Shooting & Passing */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Shots" value={merged.shots_total} small />
        <StatCard label="On Target" value={merged.shots_on} small />
        <StatCard label="Passes" value={merged.passes_total} small />
        <StatCard label="Key Passes" value={merged.passes_key} small />
      </div>

      {/* Defensive & Physical */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Tackles" value={merged.tackles_total} small />
        <StatCard label="Interceptions" value={merged.interceptions} small />
        <StatCard label="Duels Won" value={merged.duels_won} small />
        <StatCard label="Duels %" value={merged.duels_pct != null ? `${merged.duels_pct}%` : null} small />
      </div>

      {/* Extras */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Dribbles" value={`${merged.dribbles_success}/${merged.dribbles_attempts}`} small />
        <StatCard label="Fouls Drawn" value={merged.fouls_drawn} small />
        <StatCard label="Fouls Committed" value={merged.fouls_committed} small />
        <StatCard label="Offsides" value={merged.offsides} small />
      </div>

      <PlayerRadarChart stats={stats} mode="match" position={stats.position} />
    </section>
  );
}

function SeasonView({ stats }) {
  const extra = extractSeasonExtra(stats.extra);
  const merged = { ...stats, ...extra };

  return (
    <section className="mb-6">
      <h3 className="text-sm font-semibold text-gray-900 mb-3 uppercase tracking-wider">
        Season {stats.season?.year || ""}
      </h3>

      {/* Core */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Appearances" value={merged.appearances} />
        <StatCard label="Goals" value={merged.goals} highlight />
        <StatCard label="Assists" value={merged.assists} highlight />
        <StatCard label="Rating" value={stats.rating ? Number(stats.rating).toFixed(2) : null} highlight />
      </div>

      {/* Playing time */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Minutes" value={merged.minutes} small />
        <StatCard label="Lineups" value={merged.lineups} small />
        <StatCard label="Sub In" value={merged.sub_in} small />
        <StatCard label="Sub Out" value={merged.sub_out} small />
      </div>

      {/* Shooting & Passing */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Shots" value={merged.shots_total} small />
        <StatCard label="On Target" value={merged.shots_on} small />
        <StatCard label="Passes" value={merged.passes_total} small />
        <StatCard label="Key Passes" value={merged.passes_key} small />
      </div>

      {/* Defensive */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Tackles" value={merged.tackles_total} small />
        <StatCard label="Interceptions" value={merged.interceptions} small />
        <StatCard label="Duels Won" value={merged.duels_won} small />
        <StatCard label="Dribbles" value={`${merged.dribbles_success}/${merged.dribbles_attempts}`} small />
      </div>

      {/* Cards & Discipline */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
        <StatCard label="Yellow Cards" value={merged.yellow_cards} small />
        <StatCard label="Red Cards" value={merged.red_cards} small />
        <StatCard label="Fouls Drawn" value={merged.fouls_drawn} small />
        <StatCard label="Fouls Committed" value={merged.fouls_committed} small />
      </div>

      {/* Penalties */}
      {(merged.penalty_scored > 0 || merged.penalty_missed > 0 || merged.penalty_won > 0) && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 mb-4">
          <StatCard label="Pen Won" value={merged.penalty_won} small />
          <StatCard label="Pen Scored" value={merged.penalty_scored} small />
          <StatCard label="Pen Missed" value={merged.penalty_missed} small />
          {merged.saves > 0 && <StatCard label="Saves" value={merged.saves} small />}
        </div>
      )}

      <PlayerRadarChart stats={stats} mode="season" position={stats.position} />
    </section>
  );
}
