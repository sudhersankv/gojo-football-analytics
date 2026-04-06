import { useState } from "react";

const FALLBACK_TEAM = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 40 40'%3E%3Crect width='40' height='40' rx='8' fill='%23e5e7eb'/%3E%3Ctext x='50%25' y='54%25' text-anchor='middle' dominant-baseline='middle' font-size='16' fill='%239ca3af'%3E⚽%3C/text%3E%3C/svg%3E";
const FALLBACK_PLAYER = "data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 40 40'%3E%3Crect width='40' height='40' rx='20' fill='%23e5e7eb'/%3E%3Ctext x='50%25' y='54%25' text-anchor='middle' dominant-baseline='middle' font-size='14' fill='%239ca3af'%3E👤%3C/text%3E%3C/svg%3E";

export default function ImageWithFallback({ src, alt = "", type = "team", className = "" }) {
  const [failed, setFailed] = useState(false);
  const fallback = type === "player" ? FALLBACK_PLAYER : FALLBACK_TEAM;
  const imgSrc = !src || failed ? fallback : src;

  return (
    <img
      src={imgSrc}
      alt={alt}
      className={className}
      onError={() => setFailed(true)}
      loading="lazy"
    />
  );
}
