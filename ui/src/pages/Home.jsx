import { useNavigate } from "react-router-dom";
import { useTheme } from "../context/ThemeContext";

const leagues = [
  {
    slug: "premier-league",
    name: "Premier League",
    logo: "https://media.api-sports.io/football/leagues/39.png",
    country: "England",
    season: "2025/26",
  },
  {
    slug: "la-liga",
    name: "La Liga",
    logo: "https://media.api-sports.io/football/leagues/140.png",
    country: "Spain",
    season: "2025/26",
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
    <div className="flex min-h-screen flex-col items-center justify-center px-4 bg-white">
      <div className="relative mb-14 text-center">
        <img
          src="/gojo-logo.png"
          alt="Gojo"
          className="h-20 sm:h-24 mx-auto mb-5"
        />
        <h1 className="text-2xl sm:text-3xl font-extrabold tracking-tight text-gray-900">
          Gojo
        </h1>
        <p className="mt-2 text-sm text-gray-400 font-medium tracking-wide">
          Football Analytics
        </p>
      </div>

      <div className="w-full max-w-sm">
        <p className="text-center text-[10px] font-semibold uppercase tracking-[0.25em] text-gray-400 mb-5">
          Select League
        </p>

        <div className="space-y-3">
          {leagues.map((lg) => (
            <button
              key={lg.slug}
              onClick={() => handleLeague(lg.slug)}
              className="group w-full flex items-center gap-4 rounded-xl border border-gray-200 bg-white
                         p-5 text-left transition-all duration-200
                         hover:border-gray-300 hover:shadow-md active:scale-[0.99]"
            >
              <img
                src={lg.logo}
                alt={lg.name}
                className="h-11 w-11 object-contain"
              />
              <div className="flex-1 min-w-0">
                <p className="text-sm font-bold text-gray-900 group-hover:text-black transition-colors">
                  {lg.name}
                </p>
                <p className="text-xs text-gray-400 mt-0.5">
                  {lg.country} &middot; {lg.season}
                </p>
              </div>
              <span className="text-gray-300 group-hover:text-gray-500 transition-colors text-lg">
                &rarr;
              </span>
            </button>
          ))}
        </div>
      </div>

      <p className="mt-20 text-[10px] text-gray-300 tracking-wider">
        More leagues coming soon
      </p>
    </div>
  );
}
