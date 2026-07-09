import ImageWithFallback from "./ImageWithFallback";

const STAT_ORDER = [
  "Ball Possession",
  "expected_goals",
  "Total Shots",
  "Shots on Goal",
  "Shots off Goal",
  "Blocked Shots",
  "Shots insidebox",
  "Shots outsidebox",
  "Corner Kicks",
  "Offsides",
  "Fouls",
  "Yellow Cards",
  "Red Cards",
  "Goalkeeper Saves",
  "Total passes",
  "Passes accurate",
  "Passes %",
];

const STAT_LABELS = {
  "Ball Possession": "Possession",
  expected_goals: "Expected Goals (xG)",
  "Total Shots": "Total Shots",
  "Shots on Goal": "On Target",
  "Shots off Goal": "Off Target",
  "Blocked Shots": "Blocked Shots",
  "Shots insidebox": "Inside Box",
  "Shots outsidebox": "Outside Box",
  "Corner Kicks": "Corners",
  "Goalkeeper Saves": "Saves",
  "Total passes": "Passes",
  "Passes accurate": "Accurate Passes",
  "Passes %": "Pass Accuracy",
};

function parseNum(val) {
  if (val == null) return 0;
  const s = String(val).replace("%", "");
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

function isHighlight(label) {
  return label === "expected_goals" || label === "Ball Possession";
}

function StatBar({ label, homeVal, awayVal }) {
  const hNum = parseNum(homeVal);
  const aNum = parseNum(awayVal);
  const total = hNum + aNum || 1;
  const hPct = (hNum / total) * 100;
  const hBetter = hNum >= aNum;
  const highlight = isHighlight(label);

  return (
    <div className={`mb-4 ${highlight ? "p-3 rounded-lg bg-gray-50 border border-gray-100" : ""}`}>
      <div className="flex items-center justify-between mb-1.5">
        <span className={`tabular-nums ${highlight ? "text-base" : "text-sm"} ${hBetter ? "font-bold text-gray-900" : "font-semibold text-gray-500"}`}>
          {homeVal ?? "0"}
        </span>
        <span
          className={`font-medium text-gray-500 uppercase tracking-wide ${highlight ? "text-xs font-bold" : "text-xs"}`}
          style={highlight ? { color: "var(--color-primary)" } : undefined}
        >
          {STAT_LABELS[label] || label}
        </span>
        <span className={`tabular-nums ${highlight ? "text-base" : "text-sm"} ${!hBetter ? "font-bold text-gray-900" : "font-semibold text-gray-500"}`}>
          {awayVal ?? "0"}
        </span>
      </div>
      <div className="flex h-2 gap-0.5 rounded-full overflow-hidden bg-gray-100">
        <div
          className="rounded-l-full transition-all duration-700 ease-out"
          style={{ width: `${hPct}%`, backgroundColor: "var(--color-primary)" }}
        />
        <div
          className="rounded-r-full transition-all duration-700 ease-out"
          style={{ width: `${100 - hPct}%`, backgroundColor: "var(--color-secondary)" }}
        />
      </div>
    </div>
  );
}

export default function TeamStats({ stats, homeTeamId, awayTeamId, homeTeam, awayTeam }) {
  if (!stats?.length) {
    return <p className="py-10 text-center text-gray-500 text-sm">No statistics available.</p>;
  }

  const byTeam = {};
  stats.forEach((row) => {
    if (!byTeam[row.team_id]) byTeam[row.team_id] = {};
    byTeam[row.team_id][row.stat_type] = row.stat_value;
  });

  const home = byTeam[homeTeamId] || {};
  const away = byTeam[awayTeamId] || {};
  const statTypes = STAT_ORDER.filter((s) => s in home || s in away);

  return (
    <div>
      {(homeTeam || awayTeam) && (
        <div className="flex items-center justify-between mb-5">
          <div className="flex items-center gap-2">
            <ImageWithFallback src={homeTeam?.logo_url} type="team" className="h-6 w-6 object-contain" />
            <span className="text-sm font-semibold text-gray-800">{homeTeam?.name}</span>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-gray-800">{awayTeam?.name}</span>
            <ImageWithFallback src={awayTeam?.logo_url} type="team" className="h-6 w-6 object-contain" />
          </div>
        </div>
      )}

      <div className="mx-auto max-w-md">
        {statTypes.map((st) => (
          <StatBar key={st} label={st} homeVal={home[st]} awayVal={away[st]} />
        ))}
      </div>
    </div>
  );
}
