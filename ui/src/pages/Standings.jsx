import { useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import { getCurrentSeason, getStandings } from "../services/supabase";
import LeagueHeader from "../components/LeagueHeader";
import ImageWithFallback from "../components/ImageWithFallback";
import Spinner from "../components/Spinner";

export default function Standings() {
  const { league, currentTheme, setCurrentTheme } = useTheme();
  const [rows, setRows] = useState([]);
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
        const data = await getStandings(season.id);
        if (!cancelled) setRows(data);
      } catch (err) {
        if (!cancelled) setError(err.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => { cancelled = true; };
  }, [league]);

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <LeagueHeader />
      <div className="h-1" style={{ background: "linear-gradient(to right, #37003c, #37003c, #00ff85)" }} />

      <main className="mx-auto max-w-4xl px-4 py-6">
        <h2 className="text-xl font-bold text-gray-900 mb-5">Standings</h2>

        {loading && <Spinner />}
        {error && (
          <div className="rounded-lg bg-red-50 border border-red-200 p-4 text-center text-red-600 text-sm">
            {error}
          </div>
        )}

        {!loading && !error && rows.length === 0 && (
          <p className="py-16 text-center text-gray-500 text-sm">No standings data available.</p>
        )}

        {!loading && rows.length > 0 && (
          <div className="overflow-x-auto rounded-lg border border-gray-200 bg-white">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                  <th className="py-3 px-3 text-center font-semibold w-10">#</th>
                  <th className="py-3 px-3 text-left font-semibold">Team</th>
                  <th className="py-3 px-3 text-center font-semibold">P</th>
                  <th className="py-3 px-3 text-center font-semibold">W</th>
                  <th className="py-3 px-3 text-center font-semibold">D</th>
                  <th className="py-3 px-3 text-center font-semibold">L</th>
                  <th className="py-3 px-3 text-center font-semibold">GF</th>
                  <th className="py-3 px-3 text-center font-semibold">GA</th>
                  <th className="py-3 px-3 text-center font-semibold">GD</th>
                  <th className="py-3 px-3 text-center font-semibold">Pts</th>
                  <th className="py-3 px-3 text-center font-semibold hidden sm:table-cell">Form</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row, i) => {
                  const team = row.team;
                  const isTop4 = i < 4;
                  return (
                    <tr
                      key={row.team_id}
                      className={`border-t border-gray-100 transition-colors hover:bg-gray-50 border-l-[3px] ${
                        isTop4 ? "" : "border-l-transparent"
                      }`}
                      style={isTop4 ? { borderLeftColor: "#37003c" } : undefined}
                    >
                      <td className="py-2.5 px-3 text-center font-bold text-gray-500 tabular-nums">
                        {row.rank}
                      </td>
                      <td className="py-2.5 px-3">
                        <Link
                          to={`/league/${currentTheme}/team/${row.team_id}`}
                          className="flex items-center gap-2 group"
                        >
                          <ImageWithFallback src={team?.logo_url} type="team" className="h-5 w-5 object-contain" />
                          <span className="font-semibold text-gray-800 group-hover:text-pl-purple transition-colors">
                            {team?.name || "—"}
                          </span>
                        </Link>
                      </td>
                      <td className="py-2.5 px-3 text-center text-gray-700 tabular-nums">{row.all_played ?? "—"}</td>
                      <td className="py-2.5 px-3 text-center text-gray-700 tabular-nums">{row.all_win ?? "—"}</td>
                      <td className="py-2.5 px-3 text-center text-gray-700 tabular-nums">{row.all_draw ?? "—"}</td>
                      <td className="py-2.5 px-3 text-center text-gray-700 tabular-nums">{row.all_lose ?? "—"}</td>
                      <td className="py-2.5 px-3 text-center text-gray-700 tabular-nums">{row.goals_for ?? "—"}</td>
                      <td className="py-2.5 px-3 text-center text-gray-700 tabular-nums">{row.goals_against ?? "—"}</td>
                      <td className="py-2.5 px-3 text-center font-semibold text-gray-800 tabular-nums">
                        {row.goals_diff != null ? (row.goals_diff > 0 ? `+${row.goals_diff}` : row.goals_diff) : "—"}
                      </td>
                      <td className="py-2.5 px-3 text-center font-bold tabular-nums" style={{ color: "#37003c" }}>{row.points}</td>
                      <td className="py-2.5 px-3 text-center hidden sm:table-cell">
                        <div className="flex justify-center gap-0.5">
                          {(row.form || "").split("").map((ch, fi) => (
                            <span
                              key={fi}
                              className={`inline-block h-5 w-5 rounded text-[10px] font-bold leading-5 text-center ${
                                ch === "W"
                                  ? "bg-green-100 text-green-700"
                                  : ch === "D"
                                    ? "bg-yellow-100 text-yellow-700"
                                    : ch === "L"
                                      ? "bg-red-100 text-red-700"
                                      : "bg-gray-100 text-gray-500"
                              }`}
                            >
                              {ch}
                            </span>
                          ))}
                        </div>
                      </td>
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
