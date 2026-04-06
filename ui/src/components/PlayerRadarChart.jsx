import { ResponsiveContainer, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, Tooltip } from "recharts";
import { extractMatchExtra, extractSeasonExtra } from "../utils/statsHelpers";

const MATCH_AXES = [
  { key: "goals", label: "Goals", max: 3 },
  { key: "assists", label: "Assists", max: 3 },
  { key: "shots_total", label: "Shots", max: 8 },
  { key: "passes_key", label: "Key Pass", max: 5 },
  { key: "dribbles_success", label: "Dribbles", max: 5 },
  { key: "tackles_total", label: "Tackles", max: 8 },
  { key: "interceptions", label: "Int", max: 5 },
  { key: "duels_won", label: "Duels Won", max: 15 },
];

const SEASON_AXES = [
  { key: "total_goals", label: "Goals", max: 25 },
  { key: "assists", label: "Assists", max: 15 },
  { key: "shots_on", label: "Shots OT", max: 40 },
  { key: "passes_key", label: "Key Pass", max: 50 },
  { key: "dribbles_success", label: "Dribbles", max: 40 },
  { key: "tackles_total", label: "Tackles", max: 60 },
  { key: "interceptions", label: "Int", max: 30 },
  { key: "duels_won", label: "Duels Won", max: 150 },
];

function normalize(val, max) {
  const n = Number(val) || 0;
  return Math.min((n / max) * 100, 100);
}

function CustomTooltip({ active, payload }) {
  if (!active || !payload?.[0]) return null;
  const d = payload[0].payload;
  return (
    <div className="rounded-lg bg-white border border-gray-200 px-3 py-2 shadow-lg text-xs">
      <p className="font-semibold text-gray-800">{d.stat}</p>
      <p className="text-gray-600">Value: <span className="font-bold" style={{ color: "#37003c" }}>{d.raw}</span></p>
    </div>
  );
}

export default function PlayerRadarChart({ stats, mode = "match" }) {
  if (!stats) return null;

  const extra = mode === "season"
    ? extractSeasonExtra(stats.extra)
    : extractMatchExtra(stats.extra);

  const merged = { ...stats, ...extra };

  const axes = mode === "season" ? SEASON_AXES : MATCH_AXES;

  const data = axes.map(({ key, label, max }) => ({
    stat: label,
    value: normalize(merged[key], max),
    raw: merged[key] ?? 0,
  }));

  const hasValues = data.some((d) => d.raw > 0);
  if (!hasValues) return null;

  return (
    <div className="w-full h-72">
      <ResponsiveContainer width="100%" height="100%">
        <RadarChart data={data} cx="50%" cy="50%" outerRadius="68%">
          <PolarGrid stroke="#e5e7eb" strokeDasharray="3 3" />
          <PolarAngleAxis
            dataKey="stat"
            tick={{ fill: "#6b7280", fontSize: 11, fontWeight: 500 }}
          />
          <PolarRadiusAxis
            tick={false}
            axisLine={false}
            domain={[0, 100]}
          />
          <Radar
            name="Performance"
            dataKey="value"
            stroke="#37003c"
            fill="#37003c"
            fillOpacity={0.12}
            strokeWidth={2}
            dot={{ fill: "#37003c", r: 3 }}
          />
          <Tooltip content={<CustomTooltip />} />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}
