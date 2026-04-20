import { useEffect, useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import {
  getMatchById, getMatchEvents, getMatchStats, getPlayerStats,
  getLineups, getLineupPlayers,
} from "../services/supabase";
import LeagueHeader from "../components/LeagueHeader";
import MatchTimeline from "../components/MatchTimeline";
import TeamStats from "../components/TeamStats";
import TeamComparisonChart from "../components/TeamComparisonChart";
import PlayerTable from "../components/PlayerTable";
import PitchView from "../components/PitchView";
import ImageWithFallback from "../components/ImageWithFallback";
import Spinner from "../components/Spinner";

const PL_PURPLE = "var(--color-primary)";

const STATUS_LABELS = {
  FT: "Full Time", AET: "After Extra Time", PEN: "Penalties",
  "1H": "1st Half", "2H": "2nd Half", HT: "Half Time", NS: "Not Started",
};

const TABS = [
  { key: "overview", label: "Overview" },
  { key: "lineups", label: "Lineups" },
  { key: "stats", label: "Stats" },
];

export default function MatchDetail() {
  const { id } = useParams();
  const { currentTheme, setCurrentTheme } = useTheme();
  const [match, setMatch] = useState(null);
  const [events, setEvents] = useState([]);
  const [stats, setStats] = useState([]);
  const [players, setPlayers] = useState([]);
  const [lineups, setLineups] = useState([]);
  const [lineupPlayers, setLineupPlayers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [tab, setTab] = useState("overview");

  useEffect(() => {
    if (currentTheme === "gojo") setCurrentTheme("premier-league");
  }, [currentTheme, setCurrentTheme]);

  useEffect(() => {
    let cancelled = false;

    async function load() {
      try {
        setLoading(true);
        const [m, ev, st, pl, lu, lup] = await Promise.all([
          getMatchById(id),
          getMatchEvents(id),
          getMatchStats(id),
          getPlayerStats(id),
          getLineups(id),
          getLineupPlayers(id),
        ]);
        if (cancelled) return;
        setMatch(m);
        setEvents(ev);
        setStats(st);
        setPlayers(pl);
        setLineups(lu);
        setLineupPlayers(lup);
      } catch (err) {
        if (!cancelled) setError(err.message);
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    load();
    return () => { cancelled = true; };
  }, [id]);

  if (loading) return <><LeagueHeader /><Spinner /></>;
  if (error) return <><LeagueHeader /><div className="mx-auto max-w-3xl px-4 py-16 text-center text-red-600 text-sm">{error}</div></>;
  if (!match) return <><LeagueHeader /><p className="py-16 text-center text-gray-500">Match not found.</p></>;

  const home = match.home_team;
  const away = match.away_team;
  const status = match.status_short || "NS";
  const kickoff = match.utc_kickoff
    ? new Date(match.utc_kickoff).toLocaleDateString("en-US", {
        weekday: "short", month: "short", day: "numeric", hour: "2-digit", minute: "2-digit",
      })
    : "";

  return (
    <div className="min-h-screen bg-gray-50 text-gray-900">
      <LeagueHeader />

      <main className="mx-auto max-w-3xl px-4 py-6">
        <Link
          to={`/league/${currentTheme}`}
          className="mb-5 inline-flex items-center gap-1 text-sm text-gray-500 hover:text-gray-800 transition-colors"
        >
          ← Matches
        </Link>

        {/* Score header */}
        <div
          className="mb-6 rounded-xl p-6 sm:p-8"
          style={{ backgroundColor: PL_PURPLE, color: "#ffffff" }}
        >
          <div className="flex items-center justify-center gap-6 sm:gap-12">
            <Link to={`/league/${currentTheme}/team/${home?.id}`} className="flex flex-col items-center gap-2 min-w-0 hover:opacity-80 transition-opacity">
              <ImageWithFallback src={home?.logo_url} type="team" className="h-14 w-14 sm:h-16 sm:w-16 object-contain drop-shadow-md" />
              <span className="text-center text-xs sm:text-sm font-semibold" style={{ color: "rgba(255,255,255,0.9)" }}>{home?.name}</span>
            </Link>

            <div className="text-center">
              <div className="text-4xl sm:text-5xl font-black tracking-tight" style={{ color: "#ffffff" }}>
                {match.home_goals ?? "-"} — {match.away_goals ?? "-"}
              </div>
              <p className="mt-1 text-xs font-semibold uppercase tracking-wider" style={{ color: "rgba(255,255,255,0.75)" }}>
                {STATUS_LABELS[status] || status}
              </p>
              {match.ht_home_goals != null && (
                <p className="mt-0.5 text-[11px]" style={{ color: "rgba(255,255,255,0.5)" }}>
                  HT: {match.ht_home_goals} — {match.ht_away_goals}
                </p>
              )}
            </div>

            <Link to={`/league/${currentTheme}/team/${away?.id}`} className="flex flex-col items-center gap-2 min-w-0 hover:opacity-80 transition-opacity">
              <ImageWithFallback src={away?.logo_url} type="team" className="h-14 w-14 sm:h-16 sm:w-16 object-contain drop-shadow-md" />
              <span className="text-center text-xs sm:text-sm font-semibold" style={{ color: "rgba(255,255,255,0.9)" }}>{away?.name}</span>
            </Link>
          </div>

          {/* xG */}
          {(() => {
            const byTeam = {};
            stats.forEach((s) => { if (!byTeam[s.team_id]) byTeam[s.team_id] = {}; byTeam[s.team_id][s.stat_type] = s.stat_value; });
            const hxg = byTeam[match.home_team_id]?.expected_goals;
            const axg = byTeam[match.away_team_id]?.expected_goals;
            if (!hxg && !axg) return null;
            return (
              <p className="mt-2 text-center text-sm font-medium tracking-wide" style={{ color: "rgba(255,255,255,0.7)" }}>
                xG: {hxg ?? "—"} — {axg ?? "—"}
              </p>
            );
          })()}

          <div className="mt-3 flex flex-wrap items-center justify-center gap-3 text-[11px]" style={{ color: "rgba(255,255,255,0.6)" }}>
            {kickoff && <span>{kickoff}</span>}
            {match.venue_name && <span>📍 {match.venue_name}</span>}
            {match.referee && <span>{match.referee}</span>}
            {match.round && <span>{match.round}</span>}
          </div>
        </div>

        {/* Tabs */}
        <div className="mb-5 flex gap-1 bg-gray-100 rounded-lg p-1">
          {TABS.map((t) => {
            const active = tab === t.key;
            return (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={`flex-1 rounded-md px-4 py-2 text-sm font-semibold transition-all ${
                  active
                    ? "text-white shadow-sm"
                    : "text-gray-500 hover:text-gray-700"
                }`}
                style={active ? { backgroundColor: PL_PURPLE } : undefined}
              >
                {t.label}
              </button>
            );
          })}
        </div>

        {/* Tab content */}
        <div className="rounded-xl border border-gray-200 bg-white p-4 sm:p-6">
          {tab === "overview" && (
            <div className="space-y-8">
              <div>
                <h3 className="text-sm font-semibold text-gray-900 mb-4 uppercase tracking-wider">
                  Match Events
                </h3>
                <MatchTimeline events={events} homeTeamId={match.home_team_id} />
              </div>

              {stats.length > 0 && (
                <>
                  <div>
                    <h3 className="text-sm font-semibold text-gray-900 mb-4 uppercase tracking-wider">
                      Team Statistics
                    </h3>
                    <TeamStats
                      stats={stats}
                      homeTeamId={match.home_team_id}
                      awayTeamId={match.away_team_id}
                      homeTeam={home}
                      awayTeam={away}
                    />
                  </div>
                  <div>
                    <h3 className="text-sm font-semibold text-gray-900 mb-4 uppercase tracking-wider">
                      Comparison
                    </h3>
                    <TeamComparisonChart
                      stats={stats}
                      homeTeamId={match.home_team_id}
                      awayTeamId={match.away_team_id}
                      homeTeamName={home?.name}
                      awayTeamName={away?.name}
                    />
                  </div>
                </>
              )}
            </div>
          )}

          {tab === "lineups" && (
            <PitchView
              lineups={lineups}
              lineupPlayers={lineupPlayers}
              homeTeamId={match.home_team_id}
              awayTeamId={match.away_team_id}
              fixtureId={match.id}
            />
          )}

          {tab === "stats" && (
            <PlayerTable
              players={players}
              homeTeamId={match.home_team_id}
              awayTeamId={match.away_team_id}
              homeTeamName={home?.name}
              awayTeamName={away?.name}
              fixtureId={match.id}
            />
          )}
        </div>
      </main>
    </div>
  );
}
