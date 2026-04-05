"""
Redis responsibilities and key naming for Gojo.

Postgres holds durable, queryable football data (teams, fixtures, standings, squads).
Redis holds ephemeral / hot-path data only — never the only copy of facts you care about.

Key patterns use a single prefix so you can flush by namespace in dev.
"""

from __future__ import annotations

PREFIX = "gojo"

# --- Live & near-live (short TTL) ---
# Current match snapshots for fast UI / router "live" path.
# Values: JSON from API or a slim normalized dict. TTL: 30–120s, refresh while NS/LIVE.
def live_fixture(fixture_id: int) -> str:
    return f"{PREFIX}:live:fixture:{fixture_id}"


def live_league_day(league_id: int, date_ymd: str) -> str:
    """All fixture ids or payloads for a league on a calendar day (poll window)."""
    return f"{PREFIX}:live:league:{league_id}:day:{date_ymd}"


# --- API response cache (protect daily quota) ---
# Same GET params → same cached JSON. TTL: 6–24h for reference data; 1–5m for fixtures.
def cache_api(endpoint_slug: str, params_fingerprint: str) -> str:
    return f"{PREFIX}:cache:api:{endpoint_slug}:{params_fingerprint}"


# --- Optional: cached analytics / NL query results ---
def cache_query_result(query_hash: str) -> str:
    return f"{PREFIX}:cache:query:{query_hash}"


# Suggested TTLs (seconds) — tune when you measure traffic.
TTL_LIVE_FIXTURE = 60
TTL_LIVE_LEAGUE_DAY = 120
TTL_CACHE_REFERENCE = 86_400
TTL_CACHE_FIXTURES_LIST = 300
TTL_CACHE_QUERY = 300
