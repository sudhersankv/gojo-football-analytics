function eventIcon(type, detail) {
  if (type === "Goal") return "⚽";
  if (type === "Card" && detail?.toLowerCase().includes("red")) return "🟥";
  if (type === "Card") return "🟨";
  if (type === "subst") return "🔄";
  if (type === "Var") return "📺";
  return "•";
}

export default function MatchTimeline({ events, homeTeamId }) {
  if (!events?.length) {
    return <p className="py-10 text-center text-gray-500 text-sm">No events recorded.</p>;
  }

  return (
    <div className="relative mx-auto max-w-xl">
      <div className="absolute left-1/2 top-0 bottom-0 w-px -translate-x-1/2 bg-gray-200" />

      {events.map((ev) => {
        const isHome = ev.team_id === homeTeamId;
        const minute = ev.minute_extra
          ? `${ev.minute}+${ev.minute_extra}'`
          : `${ev.minute}'`;

        return (
          <div
            key={ev.id}
            className={`relative mb-3 flex items-start ${isHome ? "flex-row" : "flex-row-reverse"}`}
          >
            <div
              className={`w-[44%] rounded-lg bg-gray-50 border border-gray-200 p-2.5 ${
                isHome ? "ml-auto text-right" : "mr-auto text-left"
              }`}
            >
              <div
                className="flex items-center gap-1.5 text-xs mb-0.5"
                style={{ justifyContent: isHome ? "flex-end" : "flex-start" }}
              >
                <span className="font-mono font-bold" style={{ color: "var(--color-primary)" }}>{minute}</span>
                <span className="text-sm">{eventIcon(ev.event_type, ev.detail)}</span>
              </div>
              <p className="text-sm font-semibold text-gray-800">{ev.player_name || "Unknown"}</p>
              {ev.detail && ev.event_type !== "Goal" && (
                <p className="text-xs text-gray-500 mt-0.5">
                  {ev.detail}
                  {ev.related_player_name ? ` — ${ev.related_player_name}` : ""}
                </p>
              )}
              {ev.event_type === "Goal" && ev.related_player_name && (
                <p className="text-xs text-gray-500 mt-0.5">
                  Assist: {ev.related_player_name}
                </p>
              )}
            </div>

            <div
              className="absolute left-1/2 top-2.5 h-2.5 w-2.5 -translate-x-1/2 rounded-full border-2 border-white z-10"
              style={{ backgroundColor: "var(--color-primary)" }}
            />

            <div className="w-[44%]" />
          </div>
        );
      })}
    </div>
  );
}
