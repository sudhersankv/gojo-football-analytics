import { createContext, useContext, useState, useEffect, useMemo } from "react";

const themes = {
  gojo: {
    "--color-bg": "#050508",
    "--color-primary": "#6d28d9",
    "--color-accent": "#818cf8",
    "--color-surface": "#0f0f18",
    "--color-surface-alt": "#1a1a2e",
    "--color-text": "#f1f5f9",
    "--color-text-secondary": "#94a3b8",
    "--color-border": "#1e1e3a",
  },
  "premier-league": {
    "--color-bg": "#f8f9fa",
    "--color-primary": "#37003c",
    "--color-accent": "#00ff85",
    "--color-surface": "#ffffff",
    "--color-surface-alt": "#f0f0f5",
    "--color-text": "#1a1a2e",
    "--color-text-secondary": "#6b7280",
    "--color-border": "#e5e7eb",
  },
};

const leagueMeta = {
  "premier-league": {
    name: "Premier League",
    leagueId: 39,
    seasonYear: 2025,
    logo: "https://media.api-sports.io/football/leagues/39.png",
  },
};

const ThemeContext = createContext();

export function ThemeProvider({ children }) {
  const [currentTheme, setCurrentTheme] = useState("gojo");

  useEffect(() => {
    const vars = themes[currentTheme] || themes.gojo;
    const root = document.documentElement;
    Object.entries(vars).forEach(([prop, val]) => root.style.setProperty(prop, val));
  }, [currentTheme]);

  const league = useMemo(
    () => (currentTheme === "gojo" ? null : leagueMeta[currentTheme] || null),
    [currentTheme],
  );

  const isDark = currentTheme === "gojo";

  const value = useMemo(
    () => ({ currentTheme, setCurrentTheme, league, isDark, themes: Object.keys(themes) }),
    [currentTheme, league, isDark],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}

export function useTheme() {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used inside ThemeProvider");
  return ctx;
}
