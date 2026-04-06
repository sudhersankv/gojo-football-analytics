import { BrowserRouter, Routes, Route } from "react-router-dom";
import { ThemeProvider } from "./context/ThemeContext";
import Home from "./pages/Home";
import LeagueDashboard from "./pages/LeagueDashboard";
import MatchDetail from "./pages/MatchDetail";
import Standings from "./pages/Standings";
import PlayerDetail from "./pages/PlayerDetail";
import TeamDetail from "./pages/TeamDetail";

export default function App() {
  return (
    <ThemeProvider>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Home />} />
          <Route path="/league/:slug" element={<LeagueDashboard />} />
          <Route path="/league/:slug/match/:id" element={<MatchDetail />} />
          <Route path="/league/:slug/standings" element={<Standings />} />
          <Route path="/league/:slug/player/:id" element={<PlayerDetail />} />
          <Route path="/league/:slug/team/:id" element={<TeamDetail />} />
        </Routes>
      </BrowserRouter>
    </ThemeProvider>
  );
}
