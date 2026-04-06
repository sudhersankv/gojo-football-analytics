import { useNavigate } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";

const leagues = [
  {
    slug: "premier-league",
    name: "Premier League",
    logo: "https://media.api-sports.io/football/leagues/39.png",
    country: "England",
  },
];

export default function Home() {
  const navigate = useNavigate();
  const { setCurrentTheme } = useTheme();

  function handleLeague(slug) {
    setCurrentTheme(slug);
    navigate(`/league/${slug}`);
  }

  return (
    <div className="flex min-h-screen flex-col items-center justify-center px-4 bg-[#050508] text-gray-100">
      {/* Ambient glow */}
      <div className="pointer-events-none fixed inset-0 overflow-hidden">
        <div className="absolute left-1/2 top-1/3 h-[500px] w-[500px] -translate-x-1/2 -translate-y-1/2 rounded-full bg-purple-600/6 blur-[120px]" />
        <div className="absolute right-1/4 bottom-1/4 h-[300px] w-[300px] rounded-full bg-indigo-500/5 blur-[100px]" />
      </div>

      {/* Hero */}
      <div className="relative mb-16 text-center">
        <img
          src="/gojo-logo.png"
          alt="Gojo"
          className="h-24 sm:h-32 mx-auto mb-6 drop-shadow-2xl"
        />
        <p className="text-base text-gray-400 tracking-wide">
          AI-Powered Football Analytics Platform
        </p>
        <div className="mt-4 h-px w-32 mx-auto bg-gradient-to-r from-transparent via-indigo-500/30 to-transparent" />
      </div>

      {/* League selector */}
      <div className="relative w-full max-w-sm space-y-3">
        <p className="text-center text-[11px] font-semibold uppercase tracking-[0.2em] text-gray-500 mb-4">
          Select League
        </p>
        {leagues.map((lg) => (
          <button
            key={lg.slug}
            onClick={() => handleLeague(lg.slug)}
            className="group relative w-full flex items-center gap-4 rounded-xl border border-gray-700/50 bg-gray-900/60
                       backdrop-blur-sm p-5 text-left transition-all duration-300
                       hover:border-indigo-500/30 hover:bg-gray-800/80 hover:shadow-lg hover:shadow-indigo-500/5"
          >
            <div className="absolute inset-0 rounded-xl opacity-0 group-hover:opacity-100 transition-opacity duration-500
                            bg-gradient-to-r from-indigo-500/5 via-transparent to-purple-500/5" />
            <img src={lg.logo} alt={lg.name} className="relative h-11 w-11 object-contain" />
            <div className="relative flex-1">
              <p className="text-base font-bold text-gray-200 group-hover:text-white transition-colors">
                {lg.name}
              </p>
              <p className="text-xs text-gray-500">{lg.country}</p>
            </div>
            <span className="relative text-gray-600 group-hover:text-indigo-400 transition-colors text-lg">
              →
            </span>
          </button>
        ))}
      </div>

      <p className="relative mt-24 text-[10px] text-gray-600 tracking-wide">
        More leagues coming soon
      </p>
    </div>
  );
}
