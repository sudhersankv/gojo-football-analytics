import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import {
  getTeamById, getCurrentSeason, getTeamStanding, getTeamSquad, getTeamFixtures,
} from "../services/supabase";
import LeagueHeader from "../components/LeagueHeader";
import ImageWithFallback from "../components/ImageWithFallback";
import Spinner from "../components/Spinner";

function StatBox({ label, value, highlight }) {
  return (
    <div className="rounded-lg border border-gray-200 bg-white p-3 text-center">
      <div
        className={`text-xl font-bold ${highlight ? "" : "text-gray-900"}`}
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

function ratingColor(r) {
  const n = parseFloat(r);
  if (isNaN(n)) return "text-gray-400";
  if (n >= 8) return "text-green-600";
  if (n >= 7) return "text-blue-600";
  if (n >= 6) return "text-yellow-600";
  return "text-red-600";
}

const POS_ORDER = { G: 0, D: 1, M: 2, F: 3 };

export default function TeamDetail() {
  const { id } = useParams();
  const { currentTheme, setCurrentTheme, league } = useTheme();
  const [team, setTeam] = useState(null);
  const [standing, setStanding] = useState(null);
  const [squad, setSquad] = useState([]);
  const [fixtures, setFixtures] = useState([]);
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
        const [t, season] = await Promise.all([
          getTeamById(id),
          getCurrentSeason(league.leagueId),
        ]);
        if (cancelled) return;
        setTeam(t);

        if (season) {
          const [st, sq, fx] = await Promise.all([
            getTeamStanding(season.id, id),
            getTeamSquad(season.id, id),
            getTeamFixtures(season.id, id, 10),
          ]);
          if (cancelled) return;
          setStanding(st);
          setSquad(sq);
          setFixtures(fx);
        }
      } catch (err) {
        if (!cancelled) setError(err.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => { cancelled = true; };
  }, [id, league]);

  if (loading) return <><LeagueHeader /><Spinner /></>;
  if (error) return <><LeagueHeader /><div className="mx-auto max-w-3xl px-4 py-16 text-center text-red-600 text-sm">{error}</div></>;
  if (!team) return <><LeagueHeader /><p className="py-16 text-center text-gray-500">Team not found.</p></>;

  const groupedSquad = {};
  squad.forEach((ps) => {
    const pos = ps.position?.charAt(0)?.toUpperCase() || "?";
    if (!groupedSquad[pos]) groupedSquad[pos] = [];
    groupedSquad[pos].push(ps);
  });
  const positionSections = Object.entries(groupedSquad).sort(
    ([a], [b]) => (POS_ORDER[a] ?? 9) - (POS_ORDER[b] ?? 9),
  );
  const POS_FULL = { G: "Goalkeepers", D: "Defenders", M: "Midfielders", F: "Forwards" };

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <LeagueHeader />

      <main className="mx-auto max-w-3xl px-4 py-6">
        <Link
          to={`/league/${currentTheme}/standings`}
          className="mb-5 inline-flex items-center gap-1 text-sm text-gray-500 hover:text-pl-purple transition-colors"
        >
          ← Standings
        </Link>

        {/* Team header */}
        <div className="mb-6 rounded-xl text-white p-6 flex items-center gap-5" style={{ backgroundColor: "#37003c" }}>
          <ImageWithFallback src={team.logo_url} type="team" className="h-20 w-20 object-contain drop-shadow-lg" />
          <div>
            <h1 className="text-2xl font-bold">{team.name}</h1>
            {team.country && <span className="text-sm opacity-70">{team.country}</span>}
            {team.venue_name && <span className="text-sm opacity-60 ml-2">· {team.venue_name}</span>}
          </div>
        </div>

        {/* Season standing */}
        {standing && (
          <section className="mb-6">
            <h3 className="text-sm font-semibold text-gray-900 mb-3 uppercase tracking-wider">Season Overview</h3>
            <div className="grid grid-cols-3 sm:grid-cols-6 gap-2">
              <StatBox label="Position" value={`#${standing.rank}`} highlight />
              <StatBox label="Points" value={standing.points} highlight />
              <StatBox label="Played" value={standing.all_played} />
              <StatBox label="Won" value={standing.all_win} />
              <StatBox label="Drawn" value={standing.all_draw} />
              <StatBox label="Lost" value={standing.all_lose} />
              <StatBox label="GF" value={standing.goals_for} />
              <StatBox label="GA" value={standing.goals_against} />
              <StatBox label="GD" value={standing.goals_diff != null ? (standing.goals_diff > 0 ? `+${standing.goals_diff}` : standing.goals_diff) : null} />
            </div>
          </section>
        )}

        {/* Recent fixtures */}
        {fixtures.length > 0 && (
          <section className="mb-6">
            <h3 className="text-sm font-semibold text-gray-900 mb-3 uppercase tracking-wider">Recent Matches</h3>
            <div className="space-y-1.5">
              {fixtures.map((fx) => {
                const isHome = fx.home_team_id === Number(id);
                const opponent = isHome ? fx.away_team : fx.home_team;
                const goalsFor = isHome ? fx.home_goals : fx.away_goals;
                const goalsAg = isHome ? fx.away_goals : fx.home_goals;
                const result = fx.status_short === "NS" ? null
                  : goalsFor > goalsAg ? "W" : goalsFor < goalsAg ? "L" : "D";
                const resultBg = result === "W" ? "bg-green-100 text-green-700"
                  : result === "L" ? "bg-red-100 text-red-700"
                  : result === "D" ? "bg-yellow-100 text-yellow-700"
                  : "bg-gray-100 text-gray-500";

                return (
                  <Link
                    key={fx.id}
                    to={`/league/${currentTheme}/match/${fx.id}`}
                    className="flex items-center gap-3 rounded-lg border border-gray-200 bg-white p-3 hover:border-pl-purple/30 hover:shadow-sm transition-all"
                  >
                    <ImageWithFallback src={opponent?.logo_url} type="team" className="h-6 w-6 object-contain" />
                    <span className="text-sm font-medium text-gray-800 flex-1 truncate">
                      {isHome ? "vs" : "@"} {opponent?.name || "—"}
                    </span>
                    <span className="text-sm font-bold text-gray-700 tabular-nums">
                      {fx.home_goals ?? "-"} — {fx.away_goals ?? "-"}
                    </span>
                    {result && (
                      <span className={`text-xs font-bold w-6 text-center rounded ${resultBg}`}>{result}</span>
                    )}
                  </Link>
                );
              })}
            </div>
          </section>
        )}

        {/* Squad */}
        {positionSections.length > 0 && (
          <section>
            <h3 className="text-sm font-semibold text-gray-900 mb-3 uppercase tracking-wider">Squad</h3>
            {positionSections.map(([pos, players]) => (
              <div key={pos} className="mb-4">
                <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">
                  {POS_FULL[pos] || pos}
                </h4>
                <div className="rounded-lg border border-gray-200 bg-white overflow-hidden">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="bg-gray-50 text-gray-500 text-xs uppercase tracking-wider">
                        <th className="py-2 px-3 text-left font-semibold">Player</th>
                        <th className="py-2 px-3 text-center font-semibold">#</th>
                        <th className="py-2 px-3 text-center font-semibold">App</th>
                        <th className="py-2 px-3 text-center font-semibold">G</th>
                        <th className="py-2 px-3 text-center font-semibold">A</th>
                        <th className="py-2 px-3 text-center font-semibold">Rtg</th>
                      </tr>
                    </thead>
                    <tbody>
                      {players.map((ps) => {
                        const p = ps.player;
                        const pid = p?.id || ps.player_id;
                        const rtg = ps.rating ? Number(ps.rating).toFixed(1) : "—";
                        return (
                          <tr key={ps.player_id} className="border-t border-gray-100 hover:bg-gray-50 transition-colors">
                            <td className="py-2 px-3">
                              <Link
                                to={`/league/${currentTheme}/player/${pid}`}
                                className="flex items-center gap-2 group"
                              >
                                <ImageWithFallback
                                  src={p?.photo_url}
                                  type="player"
                                  className="h-6 w-6 rounded-full object-cover"
                                />
                                <span className="font-medium text-gray-800 group-hover:text-pl-purple transition-colors">
                                  {p?.name || "—"}
                                </span>
                              </Link>
                            </td>
                            <td className="py-2 px-3 text-center text-gray-500 tabular-nums">{ps.number ?? "—"}</td>
                            <td className="py-2 px-3 text-center text-gray-700 tabular-nums">{ps.appearances ?? 0}</td>
                            <td className="py-2 px-3 text-center text-gray-700 tabular-nums font-semibold">{ps.goals ?? 0}</td>
                            <td className="py-2 px-3 text-center text-gray-700 tabular-nums">{ps.assists ?? 0}</td>
                            <td className={`py-2 px-3 text-center tabular-nums font-bold ${ratingColor(rtg)}`}>{rtg}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            ))}
          </section>
        )}
      </main>
    </div>
  );
}
