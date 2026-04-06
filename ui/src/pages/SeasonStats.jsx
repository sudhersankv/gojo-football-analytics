import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import {
  getCurrentSeason, getTopScorers, getTopAssists, getTopRated, getMostAppearances,
} from "../services/supabase";
import LeagueHeader from "../components/LeagueHeader";
import ImageWithFallback from "../components/ImageWithFallback";
import Spinner from "../components/Spinner";
import { ratingBg } from "../utils/statsHelpers";

const TABS = [
  { key: "goals", label: "Top Scorers" },
  { key: "assists", label: "Top Assists" },
  { key: "rating", label: "Top Rated" },
  { key: "appearances", label: "Most Apps" },
];

function posLabel(p) {
  if (p === "Attacker") return "FWD";
  if (p === "Midfielder") return "MID";
  if (p === "Defender") return "DEF";
  if (p === "Goalkeeper") return "GK";
  return p || "—";
}

export default function SeasonStats() {
  const { league, currentTheme, setCurrentTheme } = useTheme();
  const [tab, setTab] = useState("goals");
  const [data, setData] = useState({ goals: [], assists: [], rating: [], appearances: [] });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    if (currentTheme === "gojo") setCurrentTheme("premier-league");
  }, [currentTheme, setCurrentTheme]);

  useEffect(() => {
    if (!league) return;
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        const season = await getCurrentSeason(league.leagueId);
        if (!season) throw new Error("No season found.");
        const [scorers, assisters, rated, apps] = await Promise.all([
          getTopScorers(season.id),
          getTopAssists(season.id),
          getTopRated(season.id),
          getMostAppearances(season.id),
        ]);
        if (!cancelled) {
          setData({ goals: scorers, assists: assisters, rating: rated, appearances: apps });
        }
      } catch (err) {
        if (!cancelled) setError(err.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => { cancelled = true; };
  }, [league]);

  const rows = data[tab] || [];

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <LeagueHeader />
      <div className="h-1" style={{ background: "linear-gradient(to right, #37003c, #37003c, #00ff85)" }} />

      <main className="mx-auto max-w-3xl px-4 py-6">
        <h2 className="text-xl font-bold text-gray-900 mb-5">Season Stats</h2>

        <div className="mb-5 flex flex-wrap gap-1.5">
          {TABS.map((t) => (
            <button
              key={t.key}
              onClick={() => setTab(t.key)}
              className={`rounded-md px-3.5 py-1.5 text-xs font-semibold uppercase tracking-wider transition-colors ${
                tab === t.key
                  ? "text-white"
                  : "bg-gray-100 text-gray-600 hover:text-gray-900 hover:bg-gray-200"
              }`}
              style={tab === t.key ? { backgroundColor: "#37003c" } : undefined}
            >
              {t.label}
            </button>
          ))}
        </div>

        {loading && <Spinner />}
        {error && (
          <div className="rounded-lg bg-red-50 border border-red-200 p-4 text-center text-red-600 text-sm">
            {error}
          </div>
        )}

        {!loading && !error && rows.length === 0 && (
          <p className="py-16 text-center text-gray-500 text-sm">No data available.</p>
        )}

        {!loading && rows.length > 0 && (
          <div className="overflow-x-auto rounded-lg border border-gray-200 bg-white">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                  <th className="py-3 px-3 text-center font-semibold w-10">#</th>
                  <th className="py-3 px-3 text-left font-semibold">Player</th>
                  <th className="py-3 px-3 text-left font-semibold">Team</th>
                  <th className="py-3 px-3 text-center font-semibold">Pos</th>
                  <th className="py-3 px-3 text-center font-semibold">App</th>
                  {tab === "goals" && <th className="py-3 px-3 text-center font-semibold">Goals</th>}
                  {tab === "assists" && <th className="py-3 px-3 text-center font-semibold">Assists</th>}
                  {tab === "rating" && <th className="py-3 px-3 text-center font-semibold">Rating</th>}
                  {tab === "appearances" && <th className="py-3 px-3 text-center font-semibold">Minutes</th>}
                </tr>
              </thead>
              <tbody>
                {rows.map((row, i) => {
                  const player = row.player;
                  const team = row.team;
                  const pid = player?.id || row.player_id;

                  return (
                    <tr
                      key={`${row.player_id}-${row.team_id}`}
                      className="border-t border-gray-100 hover:bg-gray-50 transition-colors"
                    >
                      <td className="py-2.5 px-3 text-center font-bold text-gray-400 tabular-nums">
                        {i + 1}
                      </td>
                      <td className="py-2.5 px-3">
                        <Link
                          to={`/league/${currentTheme}/player/${pid}`}
                          className="flex items-center gap-2 group"
                        >
                          <ImageWithFallback
                            src={player?.photo_url}
                            type="player"
                            className="h-7 w-7 rounded-full object-cover"
                          />
                          <span className="font-semibold text-gray-800 group-hover:text-pl-purple transition-colors">
                            {player?.name || "—"}
                          </span>
                        </Link>
                      </td>
                      <td className="py-2.5 px-3">
                        <Link
                          to={`/league/${currentTheme}/team/${team?.id}`}
                          className="flex items-center gap-1.5 group"
                        >
                          <ImageWithFallback
                            src={team?.logo_url}
                            type="team"
                            className="h-5 w-5 object-contain"
                          />
                          <span className="text-gray-600 text-xs group-hover:text-pl-purple transition-colors truncate max-w-[100px]">
                            {team?.name || "—"}
                          </span>
                        </Link>
                      </td>
                      <td className="py-2.5 px-3 text-center text-xs text-gray-500 font-medium">
                        {posLabel(row.position)}
                      </td>
                      <td className="py-2.5 px-3 text-center text-gray-700 tabular-nums">
                        {row.appearances ?? 0}
                      </td>

                      {tab === "goals" && (
                        <td className="py-2.5 px-3 text-center font-bold tabular-nums" style={{ color: "#37003c" }}>
                          {row.goals ?? 0}
                        </td>
                      )}
                      {tab === "assists" && (
                        <td className="py-2.5 px-3 text-center font-bold tabular-nums" style={{ color: "#37003c" }}>
                          {row.assists ?? 0}
                        </td>
                      )}
                      {tab === "rating" && (
                        <td className="py-2.5 px-3 text-center">
                          <span className={`inline-block px-2 py-0.5 rounded text-xs font-bold ${ratingBg(row.rating ? Number(row.rating).toFixed(2) : "—")}`}>
                            {row.rating ? Number(row.rating).toFixed(2) : "—"}
                          </span>
                        </td>
                      )}
                      {tab === "appearances" && (
                        <td className="py-2.5 px-3 text-center text-gray-700 tabular-nums">
                          {row.minutes ?? 0}
                        </td>
                      )}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </main>
    </div>
  );
}
