/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--color-bg)",
        primary: "var(--color-primary)",
        accent: "var(--color-accent)",
        secondary: "var(--color-secondary)",
        surface: "var(--color-surface)",
        "surface-alt": "var(--color-surface-alt)",
        "text-main": "var(--color-text)",
        "text-secondary": "var(--color-text-secondary)",
        border: "var(--color-border)",
        "league-primary": "var(--color-primary)",
        "league-secondary": "var(--color-secondary)",
      },
      fontFamily: {
        sans: ['"Inter"', "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};
