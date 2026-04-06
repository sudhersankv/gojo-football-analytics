import { Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import ImageWithFallback from "./ImageWithFallback";

function parseGrid(grid) {
  if (!grid) return null;
  const parts = grid.split(":");
  if (parts.length !== 2) return null;
  return { row: parseInt(parts[0], 10), col: parseInt(parts[1], 10) };
}

function PlayerDot({ player, themeSlug, fixtureId }) {
  const pid = player.player_id || player.player_api_id;
  const name = player.player_name || "?";
  const shortName = name.split(" ").pop();
  const number = player.shirt_number;

  const dot = (
    <div className="flex flex-col items-center gap-0.5 group cursor-pointer">
      <div className="h-9 w-9 rounded-full bg-white/90 border-2 border-white/40 flex items-center justify-center
                      text-xs font-bold text-gray-800 shadow-sm group-hover:scale-110 transition-transform">
        {number || "—"}
      </div>
      <span className="text-[10px] font-medium text-white text-center leading-tight max-w-[64px] truncate drop-shadow-md">
        {shortName}
      </span>
    </div>
  );

  if (pid) {
    const href = fixtureId
      ? `/league/${themeSlug}/player/${pid}?fixture_id=${fixtureId}`
      : `/league/${themeSlug}/player/${pid}`;
    return (
      <Link to={href} className="no-underline">
        {dot}
      </Link>
    );
  }
  return dot;
}

function FormationHalf({ players, formation, side, teamName, teamLogo, coachName, coachPhoto, themeSlug, fixtureId }) {
  const starters = players
    .filter((p) => p.is_starter)
    .map((p) => ({ ...p, gridPos: parseGrid(p.grid) }))
    .filter((p) => p.gridPos);

  const maxRow = Math.max(...starters.map((p) => p.gridPos.row), 1);
  const rows = [];
  for (let r = 1; r <= maxRow; r++) {
    rows.push(starters.filter((p) => p.gridPos.row === r));
  }

  if (side === "away") rows.reverse();

  return (
    <div className="flex-1 flex flex-col items-center justify-between py-4 px-2 min-h-[260px]">
      <div className="flex items-center gap-1.5 mb-2">
        {teamLogo && <img src={teamLogo} alt="" className="h-4 w-4 object-contain" />}
        <span className="text-[10px] font-semibold text-white/80 uppercase tracking-wider">
          {teamName} {formation && `· ${formation}`}
        </span>
      </div>

      <div className="flex-1 flex flex-col justify-evenly w-full gap-1">
        {rows.map((rowPlayers, ri) => (
          <div key={ri} className="flex items-center justify-center gap-3">
            {rowPlayers
              .sort((a, b) => a.gridPos.col - b.gridPos.col)
              .map((p) => (
                <PlayerDot
                  key={p.player_api_id}
                  player={p}
                  themeSlug={themeSlug}
                  fixtureId={fixtureId}
                />
              ))}
          </div>
        ))}
      </div>
    </div>
  );
}

export default function PitchView({ lineups, lineupPlayers, homeTeamId, awayTeamId, fixtureId }) {
  const { currentTheme } = useTheme();

  if (!lineupPlayers?.length) {
    return <p className="py-10 text-center text-gray-500 text-sm">Lineup data not available.</p>;
  }

  const homePlayers = lineupPlayers.filter((p) => p.team_id === homeTeamId);
  const awayPlayers = lineupPlayers.filter((p) => p.team_id === awayTeamId);
  const homeLineup = lineups?.find((l) => l.team_id === homeTeamId);
  const awayLineup = lineups?.find((l) => l.team_id === awayTeamId);

  return (
    <div>
      {/* Coach info */}
      {(homeLineup?.coach_name || awayLineup?.coach_name) && (
        <div className="flex items-center justify-between mb-4 px-2">
          <CoachBadge name={homeLineup?.coach_name} photo={homeLineup?.coach_photo_url} />
          <span className="text-[10px] font-semibold text-gray-400 uppercase tracking-widest">Coaches</span>
          <CoachBadge name={awayLineup?.coach_name} photo={awayLineup?.coach_photo_url} align="right" />
        </div>
      )}

      {/* Pitch */}
      <div
        className="relative mx-auto max-w-lg rounded-xl overflow-hidden"
        style={{
          background: "linear-gradient(180deg, #1a8a3f 0%, #15742f 50%, #1a8a3f 100%)",
        }}
      >
        <div className="absolute inset-0 pointer-events-none">
          <div className="absolute left-0 right-0 top-1/2 h-px bg-white/20" />
          <div className="absolute left-1/2 top-1/2 h-16 w-16 -translate-x-1/2 -translate-y-1/2 rounded-full border border-white/20" />
          <div className="absolute left-1/2 top-0 h-12 w-36 -translate-x-1/2 border-b border-l border-r border-white/15 rounded-b-sm" />
          <div className="absolute left-1/2 bottom-0 h-12 w-36 -translate-x-1/2 border-t border-l border-r border-white/15 rounded-t-sm" />
          <div className="absolute inset-2 border border-white/20 rounded-sm" />
        </div>

        <div className="relative z-10 flex flex-col">
          <FormationHalf
            players={homePlayers}
            formation={homeLineup?.formation}
            side="home"
            teamName={homeLineup?.team?.name || "Home"}
            teamLogo={homeLineup?.team?.logo_url}
            coachName={homeLineup?.coach_name}
            coachPhoto={homeLineup?.coach_photo_url}
            themeSlug={currentTheme}
            fixtureId={fixtureId}
          />
          <div className="h-px mx-6" />
          <FormationHalf
            players={awayPlayers}
            formation={awayLineup?.formation}
            side="away"
            teamName={awayLineup?.team?.name || "Away"}
            teamLogo={awayLineup?.team?.logo_url}
            coachName={awayLineup?.coach_name}
            coachPhoto={awayLineup?.coach_photo_url}
            themeSlug={currentTheme}
            fixtureId={fixtureId}
          />
        </div>
      </div>

      {/* Substitutes */}
      {(homePlayers.some((p) => !p.is_starter) || awayPlayers.some((p) => !p.is_starter)) && (
        <div className="mt-6 grid grid-cols-2 gap-4">
          <SubList
            players={homePlayers.filter((p) => !p.is_starter)}
            teamName={homeLineup?.team?.name}
            themeSlug={currentTheme}
            fixtureId={fixtureId}
          />
          <SubList
            players={awayPlayers.filter((p) => !p.is_starter)}
            teamName={awayLineup?.team?.name}
            themeSlug={currentTheme}
            fixtureId={fixtureId}
          />
        </div>
      )}
    </div>
  );
}

function CoachBadge({ name, photo, align = "left" }) {
  if (!name) return <div />;
  return (
    <div className={`flex items-center gap-2 ${align === "right" ? "flex-row-reverse" : ""}`}>
      <ImageWithFallback src={photo} type="player" className="h-7 w-7 rounded-full object-cover" />
      <span className="text-xs font-medium text-gray-700">{name}</span>
    </div>
  );
}

function SubList({ players, teamName, themeSlug, fixtureId }) {
  if (!players?.length) return <div />;
  return (
    <div>
      <h4 className="text-xs font-semibold text-gray-500 uppercase tracking-wide mb-2">
        {teamName} — Subs
      </h4>
      <div className="space-y-1">
        {players.map((p) => {
          const pid = p.player_id || p.player_api_id;
          const name = p.player_name || "?";
          const inner = (
            <div className="flex items-center gap-2 text-sm">
              <span className="text-xs font-bold text-gray-400 w-5 text-right tabular-nums">
                {p.shirt_number || "—"}
              </span>
              <span className="text-gray-800">{name}</span>
              {p.position_short && (
                <span className="text-[10px] text-gray-400 uppercase">{p.position_short}</span>
              )}
            </div>
          );
          if (pid) {
            const href = fixtureId
              ? `/league/${themeSlug}/player/${pid}?fixture_id=${fixtureId}`
              : `/league/${themeSlug}/player/${pid}`;
            return (
              <Link
                key={p.player_api_id}
                to={href}
                className="block hover:bg-gray-50 rounded px-1.5 py-0.5 transition-colors"
              >
                {inner}
              </Link>
            );
          }
          return <div key={p.player_api_id} className="px-1.5 py-0.5">{inner}</div>;
        })}
      </div>
    </div>
  );
}
