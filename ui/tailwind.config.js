/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,jsx}"],
  theme: {
    extend: {
      colors: {
        bg: "var(--color-bg)",
        primary: "var(--color-primary)",
        accent: "var(--color-accent)",
        surface: "var(--color-surface)",
        "surface-alt": "var(--color-surface-alt)",
        "text-main": "var(--color-text)",
        "text-secondary": "var(--color-text-secondary)",
        border: "var(--color-border)",
        pl: {
          purple: "#37003c",
          green: "#00ff85",
          "green-dark": "#02d76a",
        },
      },
      fontFamily: {
        sans: ['"Inter"', "system-ui", "sans-serif"],
      },
    },
  },
  plugins: [],
};
