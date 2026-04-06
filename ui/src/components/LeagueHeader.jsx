import { Link, useLocation } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";

export default function LeagueHeader() {
  const { league, currentTheme } = useTheme();
  const location = useLocation();
  if (!league) return null;

  const slug = currentTheme;
  const tabs = [
    { to: `/league/${slug}`, label: "Matches" },
    { to: `/league/${slug}/standings`, label: "Standings" },
  ];

  return (
    <header className="sticky top-0 z-40 border-b border-gray-200 bg-white/95 backdrop-blur-md">
      <div className="mx-auto flex max-w-5xl items-center gap-3 px-4 py-2.5">
        <Link
          to="/"
          className="text-gray-500 hover:text-gray-800 transition-colors text-sm font-medium"
        >
          <img src="/gojo-logo.png" alt="Gojo" className="h-6 object-contain" />
        </Link>

        <div className="h-5 w-px bg-gray-200" />

        <img src={league.logo} alt={league.name} className="h-7 w-7 object-contain" />
        <span className="text-base font-bold text-gray-900">{league.name}</span>

        <nav className="ml-auto flex gap-1">
          {tabs.map((t) => {
            const active = location.pathname === t.to;
            return (
              <Link
                key={t.to}
                to={t.to}
                className={`px-3.5 py-1.5 text-sm font-medium rounded-md transition-colors ${
                  active
                    ? "text-white"
                    : "text-gray-500 hover:text-gray-800 hover:bg-gray-100"
                }`}
                style={active ? { backgroundColor: "#37003c" } : undefined}
              >
                {t.label}
              </Link>
            );
          })}
        </nav>
      </div>
    </header>
  );
}
