import { ResponsiveContainer, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar, Tooltip } from "recharts";
import { extractMatchExtra, extractSeasonExtra } from "../utils/statsHelpers";

const MATCH_AXES_BY_POS = {
  Goalkeeper: [
    { key: "saves", label: "Saves", max: 8 },
    { key: "goals_conceded", label: "Conceded", max: 4 },
    { key: "passes_total", label: "Passes", max: 40 },
    { key: "passes_accuracy", label: "Pass Acc", max: 100 },
    { key: "duels_won", label: "Duels Won", max: 8 },
    { key: "tackles_total", label: "Tackles", max: 3 },
  ],
  Defender: [
    { key: "tackles_total", label: "Tackles", max: 8 },
    { key: "interceptions", label: "Int", max: 5 },
    { key: "duels_won", label: "Duels Won", max: 15 },
    { key: "passes_total", label: "Passes", max: 80 },
    { key: "passes_key", label: "Key Pass", max: 3 },
    { key: "fouls_committed", label: "Fouls", max: 5 },
    { key: "goals", label: "Goals", max: 2 },
    { key: "assists", label: "Assists", max: 2 },
  ],
  Midfielder: [
    { key: "passes_total", label: "Passes", max: 80 },
    { key: "passes_key", label: "Key Pass", max: 5 },
    { key: "assists", label: "Assists", max: 3 },
    { key: "goals", label: "Goals", max: 3 },
    { key: "dribbles_success", label: "Dribbles", max: 5 },
    { key: "tackles_total", label: "Tackles", max: 8 },
    { key: "interceptions", label: "Int", max: 5 },
    { key: "duels_won", label: "Duels Won", max: 15 },
  ],
  Attacker: [
    { key: "goals", label: "Goals", max: 3 },
    { key: "assists", label: "Assists", max: 3 },
    { key: "shots_total", label: "Shots", max: 8 },
    { key: "shots_on", label: "On Target", max: 5 },
    { key: "dribbles_success", label: "Dribbles", max: 5 },
    { key: "passes_key", label: "Key Pass", max: 5 },
    { key: "duels_won", label: "Duels Won", max: 15 },
    { key: "offsides", label: "Offsides", max: 5 },
  ],
};

const SEASON_AXES_BY_POS = {
  Goalkeeper: [
    { key: "saves", label: "Saves", max: 80 },
    { key: "goals_conceded", label: "Conceded", max: 40 },
    { key: "passes_total", label: "Passes", max: 800 },
    { key: "passes_accuracy", label: "Pass Acc", max: 100 },
    { key: "duels_won", label: "Duels Won", max: 30 },
    { key: "penalty_saved", label: "Pen Saved", max: 5 },
  ],
  Defender: [
    { key: "tackles_total", label: "Tackles", max: 60 },
    { key: "interceptions", label: "Int", max: 30 },
    { key: "duels_won", label: "Duels Won", max: 150 },
    { key: "passes_total", label: "Passes", max: 1500 },
    { key: "passes_key", label: "Key Pass", max: 30 },
    { key: "total_goals", label: "Goals", max: 8 },
    { key: "assists", label: "Assists", max: 8 },
    { key: "yellow_cards", label: "Yellows", max: 12 },
  ],
  Midfielder: [
    { key: "passes_total", label: "Passes", max: 2000 },
    { key: "passes_key", label: "Key Pass", max: 50 },
    { key: "assists", label: "Assists", max: 15 },
    { key: "total_goals", label: "Goals", max: 15 },
    { key: "dribbles_success", label: "Dribbles", max: 40 },
    { key: "tackles_total", label: "Tackles", max: 60 },
    { key: "interceptions", label: "Int", max: 30 },
    { key: "duels_won", label: "Duels Won", max: 150 },
  ],
  Attacker: [
    { key: "total_goals", label: "Goals", max: 25 },
    { key: "assists", label: "Assists", max: 15 },
    { key: "shots_on", label: "Shots OT", max: 40 },
    { key: "dribbles_success", label: "Dribbles", max: 40 },
    { key: "passes_key", label: "Key Pass", max: 50 },
    { key: "duels_won", label: "Duels Won", max: 100 },
    { key: "fouls_drawn", label: "Fouls Drawn", max: 40 },
    { key: "penalty_scored", label: "Pen Scored", max: 8 },
  ],
};

const DEFAULT_MATCH_AXES = MATCH_AXES_BY_POS.Midfielder;
const DEFAULT_SEASON_AXES = SEASON_AXES_BY_POS.Midfielder;

function resolvePosition(position) {
  if (!position) return null;
  const p = position.trim();
  if (p === "Goalkeeper" || p === "G") return "Goalkeeper";
  if (p === "Defender" || p === "D") return "Defender";
  if (p === "Midfielder" || p === "M") return "Midfielder";
  if (p === "Attacker" || p === "Forward" || p === "A" || p === "F") return "Attacker";
  return null;
}

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
      <p className="text-gray-600">Value: <span className="font-bold" style={{ color: "var(--color-primary)" }}>{d.raw}</span></p>
    </div>
  );
}

export default function PlayerRadarChart({ stats, mode = "match", position }) {
  if (!stats) return null;

  const extra = mode === "season"
    ? extractSeasonExtra(stats.extra)
    : extractMatchExtra(stats.extra);

  const merged = { ...stats, ...extra };

  const pos = resolvePosition(position || stats.position);
  let axes;
  if (mode === "season") {
    axes = (pos && SEASON_AXES_BY_POS[pos]) || DEFAULT_SEASON_AXES;
  } else {
    axes = (pos && MATCH_AXES_BY_POS[pos]) || DEFAULT_MATCH_AXES;
  }

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
            stroke="var(--color-primary)"
            fill="var(--color-primary)"
            fillOpacity={0.12}
            strokeWidth={2}
            dot={{ fill: "var(--color-primary)", r: 3 }}
          />
          <Tooltip content={<CustomTooltip />} />
        </RadarChart>
      </ResponsiveContainer>
    </div>
  );
}
