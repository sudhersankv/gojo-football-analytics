import { ResponsiveContainer, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend } from "recharts";

const CHART_STATS = [
  { key: "Ball Possession", label: "Possession" },
  { key: "expected_goals", label: "xG" },
  { key: "Total Shots", label: "Shots" },
  { key: "Shots on Goal", label: "On Target" },
  { key: "Shots off Goal", label: "Off Target" },
  { key: "Blocked Shots", label: "Blocked" },
  { key: "Corner Kicks", label: "Corners" },
  { key: "Total passes", label: "Passes" },
  { key: "Passes accurate", label: "Acc. Passes" },
  { key: "Fouls", label: "Fouls" },
];

function parseNum(val) {
  if (val == null) return 0;
  const s = String(val).replace("%", "");
  const n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null;
  return (
    <div className="rounded-lg bg-white border border-gray-200 px-3 py-2 shadow-lg text-xs">
      <p className="font-semibold text-gray-800 mb-1">{label}</p>
      {payload.map((p) => (
        <p key={p.name} style={{ color: p.color }} className="font-medium">
          {p.name}: <span className="font-bold">{p.value}</span>
        </p>
      ))}
    </div>
  );
}

export default function TeamComparisonChart({ stats, homeTeamId, awayTeamId, homeTeamName, awayTeamName }) {
  if (!stats?.length) return null;

  const byTeam = {};
  stats.forEach((row) => {
    if (!byTeam[row.team_id]) byTeam[row.team_id] = {};
    byTeam[row.team_id][row.stat_type] = row.stat_value;
  });

  const home = byTeam[homeTeamId] || {};
  const away = byTeam[awayTeamId] || {};

  const data = CHART_STATS
    .filter(({ key }) => key in home || key in away)
    .map(({ key, label }) => ({
      stat: label,
      [homeTeamName || "Home"]: parseNum(home[key]),
      [awayTeamName || "Away"]: parseNum(away[key]),
    }));

  if (!data.length) return null;

  const hName = homeTeamName || "Home";
  const aName = awayTeamName || "Away";

  return (
    <div className="w-full h-80">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={data} barGap={2} margin={{ top: 5, right: 10, left: -10, bottom: 5 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" vertical={false} />
          <XAxis
            dataKey="stat"
            tick={{ fill: "#6b7280", fontSize: 10 }}
            axisLine={{ stroke: "#e5e7eb" }}
            tickLine={false}
            interval={0}
            angle={-25}
            textAnchor="end"
            height={50}
          />
          <YAxis
            tick={{ fill: "#9ca3af", fontSize: 10 }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip content={<CustomTooltip />} />
          <Legend wrapperStyle={{ fontSize: "12px", color: "#374151" }} />
          <Bar dataKey={hName} radius={[4, 4, 0, 0]} fill="#37003c" />
          <Bar dataKey={aName} radius={[4, 4, 0, 0]} fill="#02d76a" />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
