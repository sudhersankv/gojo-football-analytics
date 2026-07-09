import { createContext, useContext, useState, useEffect, useMemo } from "react";

const themes = {
  gojo: {
    "--color-bg": "#ffffff",
    "--color-primary": "#111827",
    "--color-accent": "#6b7280",
    "--color-secondary": "#6b7280",
    "--color-surface": "#ffffff",
    "--color-surface-alt": "#f9fafb",
    "--color-text": "#111827",
    "--color-text-secondary": "#6b7280",
    "--color-border": "#e5e7eb",
  },
  "premier-league": {
    "--color-bg": "#ffffff",
    "--color-primary": "#37003c",
    "--color-accent": "#00ff85",
    "--color-secondary": "#02d76a",
    "--color-surface": "#ffffff",
    "--color-surface-alt": "#f9fafb",
    "--color-text": "#111827",
    "--color-text-secondary": "#6b7280",
    "--color-border": "#e5e7eb",
  },
  "la-liga": {
    "--color-bg": "#ffffff",
    "--color-primary": "#ee8707",
    "--color-accent": "#1a3c6e",
    "--color-secondary": "#1a3c6e",
    "--color-surface": "#ffffff",
    "--color-surface-alt": "#f9fafb",
    "--color-text": "#111827",
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
  "la-liga": {
    name: "La Liga",
    leagueId: 140,
    seasonYear: 2025,
    logo: "https://media.api-sports.io/football/leagues/140.png",
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

  const isDark = false;

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
