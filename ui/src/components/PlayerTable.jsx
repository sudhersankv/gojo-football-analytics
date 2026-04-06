import { useState } from "react";
import { Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import { extractMatchExtra, ratingBg } from "../utils/statsHelpers";
import ImageWithFallback from "./ImageWithFallback";

const COLUMNS = [
  { key: "name", label: "Player", align: "left" },
  { key: "position", label: "Pos", align: "center" },
  { key: "minutes", label: "Min", align: "center" },
  { key: "rating", label: "Rtg", align: "center" },
  { key: "goals", label: "G", align: "center" },
  { key: "assists", label: "A", align: "center" },
  { key: "shots_total", label: "Sh", align: "center" },
  { key: "shots_on", label: "SoT", align: "center" },
  { key: "passes_total", label: "Pas", align: "center" },
  { key: "passes_key", label: "KP", align: "center" },
  { key: "tackles_total", label: "Tkl", align: "center" },
  { key: "interceptions", label: "Int", align: "center" },
  { key: "duels_won", label: "DW", align: "center" },
  { key: "dribbles_success", label: "Drb", align: "center" },
  { key: "fouls_committed", label: "Fls", align: "center" },
  { key: "cards", label: "Cards", align: "center" },
];

export default function PlayerTable({ players, homeTeamId, awayTeamId, homeTeamName, awayTeamName, fixtureId }) {
  const [selectedTeam, setSelectedTeam] = useState("home");
  const { currentTheme } = useTheme();

  if (!players?.length) {
    return <p className="py-10 text-center text-gray-500 text-sm">No player stats available.</p>;
  }

  const teamId = selectedTeam === "home" ? homeTeamId : awayTeamId;
  const filtered = players.filter((p) => p.team_id === teamId);

  function playerLink(pid) {
    const base = `/league/${currentTheme}/player/${pid}`;
    return fixtureId ? `${base}?fixture_id=${fixtureId}` : base;
  }

  return (
    <div>
      <div className="mb-4 flex justify-center gap-1.5">
        {[
          { key: "home", label: homeTeamName },
          { key: "away", label: awayTeamName },
        ].map(({ key, label }) => (
          <button
            key={key}
            onClick={() => setSelectedTeam(key)}
            className={`rounded-md px-4 py-1.5 text-sm font-medium transition-colors ${
              selectedTeam === key
                ? "text-white"
                : "bg-gray-100 text-gray-600 hover:text-gray-900 hover:bg-gray-200"
            }`}
            style={selectedTeam === key ? { backgroundColor: "#37003c" } : undefined}
          >
            {label || key}
          </button>
        ))}
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-gray-200 text-gray-500">
              {COLUMNS.map((col) => (
                <th
                  key={col.key}
                  className={`py-2 px-1.5 font-medium uppercase tracking-wider whitespace-nowrap ${
                    col.align === "left" ? "text-left" : "text-center"
                  }`}
                >
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {filtered.map((row) => {
              const extra = extractMatchExtra(row.extra);
              const merged = { ...row, ...extra };
              return (
                <tr
                  key={`${row.player_id}-${row.team_id}`}
                  className="border-b border-gray-100 hover:bg-gray-50 transition-colors"
                >
                  {COLUMNS.map((col) => {
                    if (col.key === "name") {
                      const name = row.player?.name || "—";
                      const pid = row.player?.id || row.player_id;
                      const photo = row.player?.photo_url;
                      return (
                        <td key={col.key} className="py-1.5 px-1.5 text-left">
                          {pid ? (
                            <Link
                              to={playerLink(pid)}
                              className="flex items-center gap-1.5 group"
                            >
                              <ImageWithFallback
                                src={photo}
                                type="player"
                                className="h-5 w-5 rounded-full object-cover"
                              />
                              <span className="font-medium text-gray-800 group-hover:text-pl-purple transition-colors whitespace-nowrap">
                                {name}
                              </span>
                            </Link>
                          ) : (
                            <span className="font-medium text-gray-800">{name}</span>
                          )}
                        </td>
                      );
                    }
                    if (col.key === "rating") {
                      const r = row.rating != null ? Number(row.rating).toFixed(1) : "—";
                      return (
                        <td key={col.key} className="py-1.5 px-1.5 text-center">
                          <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-bold ${ratingBg(r)}`}>
                            {r}
                          </span>
                        </td>
                      );
                    }
                    if (col.key === "cards") {
                      const yc = merged.yellow_cards || 0;
                      const rc = merged.red_cards || 0;
                      if (!yc && !rc) return <td key={col.key} className="py-1.5 px-1.5 text-center text-gray-400">—</td>;
                      return (
                        <td key={col.key} className="py-1.5 px-1.5 text-center">
                          <span className="inline-flex gap-0.5">
                            {yc > 0 && <span className="inline-block h-3.5 w-2.5 rounded-sm bg-yellow-400" title={`${yc} yellow`} />}
                            {rc > 0 && <span className="inline-block h-3.5 w-2.5 rounded-sm bg-red-500" title={`${rc} red`} />}
                          </span>
                        </td>
                      );
                    }
                    return (
                      <td key={col.key} className="py-1.5 px-1.5 text-center text-gray-700 tabular-nums">
                        {merged[col.key] ?? "—"}
                      </td>
                    );
                  })}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
    </div>
  );
}
