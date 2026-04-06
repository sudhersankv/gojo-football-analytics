import { useEffect, useState, useMemo, useRef, useCallback } from "react";
import { Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import { getCurrentSeason, getMatchesByLeague, getStandings } from "../services/supabase";
import LeagueHeader from "../components/LeagueHeader";
import MatchCard from "../components/MatchCard";
import ImageWithFallback from "../components/ImageWithFallback";
import Spinner from "../components/Spinner";

function groupByRound(matches) {
  const groups = {};
  matches.forEach((m) => {
    const key = m.round || "Other";
    if (!groups[key]) groups[key] = [];
    groups[key].push(m);
  });
  return groups;
}

function findCurrentRoundIndex(sortedRounds, grouped) {
  const now = new Date();
  let best = 0;
  let bestDiff = Infinity;
  for (let i = 0; i < sortedRounds.length; i++) {
    const matches = grouped[sortedRounds[i]] || [];
    for (const m of matches) {
      if (!m.utc_kickoff) continue;
      const diff = Math.abs(new Date(m.utc_kickoff) - now);
      if (diff < bestDiff) {
        bestDiff = diff;
        best = i;
      }
    }
  }
  return best;
}

export default function LeagueDashboard() {
  const { league, currentTheme, setCurrentTheme } = useTheme();
  const [matches, setMatches] = useState([]);
  const [standings, setStandings] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filter, setFilter] = useState("all");
  const currentRoundRef = useRef(null);
  const scrolledRef = useRef(false);

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
        if (!season) throw new Error("No season found for this league.");
        const [matchData, standingsData] = await Promise.all([
          getMatchesByLeague(season.id),
          getStandings(season.id),
        ]);
        if (!cancelled) {
          setMatches(matchData);
          setStandings(standingsData);
          scrolledRef.current = false;
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

  const filtered = useMemo(() => {
    if (filter === "all") return matches;
    if (filter === "live") return matches.filter((m) => ["1H", "2H", "HT", "ET", "LIVE"].includes(m.status_short));
    if (filter === "results") return matches.filter((m) => ["FT", "AET", "PEN"].includes(m.status_short));
    if (filter === "upcoming") return matches.filter((m) => m.status_short === "NS");
    return matches;
  }, [matches, filter]);

  const grouped = useMemo(() => groupByRound(filtered), [filtered]);

  const sortedRounds = useMemo(() => {
    return Object.keys(grouped).sort((a, b) => {
      const aDate = grouped[a][0]?.utc_kickoff || "";
      const bDate = grouped[b][0]?.utc_kickoff || "";
      return aDate.localeCompare(bDate);
    });
  }, [grouped]);

  const currentRoundIdx = useMemo(
    () => (filter === "all" ? findCurrentRoundIndex(sortedRounds, grouped) : -1),
    [sortedRounds, grouped, filter],
  );

  const setCurrentRoundNode = useCallback(
    (node) => {
      currentRoundRef.current = node;
      if (node && !scrolledRef.current) {
        scrolledRef.current = true;
        requestAnimationFrame(() => {
          node.scrollIntoView({ behavior: "smooth", block: "start" });
        });
      }
    },
    [],
  );

  const topStandings = standings.slice(0, 5);

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <LeagueHeader />
      <div className="h-1" style={{ background: "linear-gradient(to right, #37003c, #37003c, #00ff85)" }} />

      <main className="mx-auto max-w-5xl px-4 py-6">
        <div className="flex flex-col lg:flex-row gap-6">
          <div className="flex-1 min-w-0">
            <div className="mb-5 flex flex-wrap gap-1.5">
              {["all", "live", "results", "upcoming"].map((f) => (
                <button
                  key={f}
                  onClick={() => setFilter(f)}
                  className={`rounded-md px-3.5 py-1.5 text-xs font-semibold uppercase tracking-wider transition-colors ${
                    filter === f
                      ? "text-white"
                      : "bg-gray-100 text-gray-600 hover:text-gray-900 hover:bg-gray-200"
                  }`}
                  style={filter === f ? { backgroundColor: "#37003c" } : undefined}
                >
                  {f === "results" ? "Results" : f}
                </button>
              ))}
            </div>

            {loading && <Spinner />}
            {error && (
              <div className="rounded-lg bg-red-50 border border-red-200 p-4 text-center text-red-600 text-sm">
                {error}
              </div>
            )}
            {!loading && !error && filtered.length === 0 && (
              <p className="py-16 text-center text-gray-500 text-sm">No matches found.</p>
            )}

            {sortedRounds.map((round, idx) => {
              const isCurrent = idx === currentRoundIdx;
              return (
                <section
                  key={round}
                  className="mb-6"
                  ref={isCurrent ? setCurrentRoundNode : undefined}
                >
                  <h3
                    className={`mb-2 text-xs font-semibold uppercase tracking-wider ${
                      isCurrent ? "text-pl-purple" : "text-gray-500"
                    }`}
                  >
                    {round}
                    {isCurrent && (
                      <span className="ml-2 inline-block px-1.5 py-0.5 rounded text-[9px] bg-pl-purple/10 text-pl-purple font-bold">
                        Current
                      </span>
                    )}
                  </h3>
                  <div className="space-y-1.5">
                    {grouped[round].map((m) => (
                      <MatchCard key={m.id} match={m} />
                    ))}
                  </div>
                </section>
              );
            })}
          </div>

          {topStandings.length > 0 && (
            <aside className="lg:w-64 shrink-0">
              <div className="rounded-lg border border-gray-200 bg-white p-3 sticky top-14">
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-xs font-semibold uppercase tracking-wider text-gray-500">
                    Standings
                  </h3>
                  <Link
                    to={`/league/${currentTheme}/standings`}
                    className="text-[10px] font-semibold hover:underline uppercase"
                    style={{ color: "#37003c" }}
                  >
                    View all
                  </Link>
                </div>
                <table className="w-full text-xs">
                  <thead>
                    <tr className="text-gray-400">
                      <th className="pb-1.5 text-left font-medium w-5">#</th>
                      <th className="pb-1.5 text-left font-medium">Team</th>
                      <th className="pb-1.5 text-center font-medium">P</th>
                      <th className="pb-1.5 text-center font-medium">Pts</th>
                    </tr>
                  </thead>
                  <tbody>
                    {topStandings.map((row) => (
                      <tr key={row.team_id} className="border-t border-gray-100">
                        <td className="py-1.5 font-bold text-gray-400">{row.rank}</td>
                        <td className="py-1.5">
                          <Link
                            to={`/league/${currentTheme}/team/${row.team_id}`}
                            className="flex items-center gap-1.5 group"
                          >
                            <ImageWithFallback src={row.team?.logo_url} type="team" className="h-4 w-4 object-contain" />
                            <span className="font-medium text-gray-700 truncate group-hover:text-pl-purple transition-colors">
                              {row.team?.name}
                            </span>
                          </Link>
                        </td>
                        <td className="py-1.5 text-center text-gray-500">{row.all_played}</td>
                        <td className="py-1.5 text-center font-bold" style={{ color: "#37003c" }}>{row.points}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </aside>
          )}
        </div>
      </main>
    </div>
  );
}
