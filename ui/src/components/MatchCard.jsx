import { Link } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";
import ImageWithFallback from "./ImageWithFallback";

const STATUS_LABELS = {
  FT: "FT", AET: "AET", PEN: "PEN",
  "1H": "LIVE", "2H": "LIVE", HT: "HT",
  NS: "—", PST: "PST", CANC: "CANC", SUSP: "SUSP", INT: "INT", LIVE: "LIVE",
};

function isLive(status) {
  return ["1H", "2H", "HT", "ET", "BT", "P", "LIVE", "INT", "SUSP"].includes(status);
}

function formatKickoff(utc) {
  if (!utc) return "";
  const d = new Date(utc);
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

export default function MatchCard({ match }) {
  const { currentTheme } = useTheme();
  const slug = currentTheme;

  const home = match.home_team;
  const away = match.away_team;
  const status = match.status_short || "NS";
  const live = isLive(status);
  const finished = ["FT", "AET", "PEN"].includes(status);

  return (
    <Link
      to={`/league/${slug}/match/${match.id}`}
      className="group block rounded-lg border border-gray-200 bg-white p-3.5
                 hover:shadow-md hover:border-primary/30 transition-all duration-200"
    >
      <div className="flex items-center">
        {/* Home */}
        <div className="flex flex-1 items-center gap-2.5 min-w-0">
          <ImageWithFallback src={home?.logo_url} type="team" className="h-7 w-7 shrink-0 object-contain" />
          <span className={`truncate text-sm font-semibold ${finished && match.home_winner ? "text-gray-900" : "text-gray-700"}`}>
            {home?.name || "TBD"}
          </span>
        </div>

        {/* Score / Time / Status */}
        <div className="shrink-0 text-center mx-3 min-w-[72px]">
          {finished || live ? (
            <div className="flex flex-col items-center">
              <span className="text-lg font-bold text-gray-900 tracking-wide">
                {match.home_goals ?? "-"} — {match.away_goals ?? "-"}
              </span>
              <span
                className={`mt-0.5 text-[10px] font-bold uppercase tracking-wider ${
                  live ? "text-red-500" : "text-gray-400"
                }`}
              >
                {live && match.elapsed ? `${match.elapsed}'` : STATUS_LABELS[status] || status}
              </span>
            </div>
          ) : (
            <span className="text-xs text-gray-500 leading-tight">
              {formatKickoff(match.utc_kickoff)}
            </span>
          )}
        </div>

        {/* Away */}
        <div className="flex flex-1 items-center justify-end gap-2.5 min-w-0">
          <span className={`truncate text-right text-sm font-semibold ${finished && match.away_winner ? "text-gray-900" : "text-gray-700"}`}>
            {away?.name || "TBD"}
          </span>
          <ImageWithFallback src={away?.logo_url} type="team" className="h-7 w-7 shrink-0 object-contain" />
        </div>
      </div>
    </Link>
  );
}
