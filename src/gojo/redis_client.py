"""Upstash Redis (REST) — load ``src/.env``, then ``Redis.from_env()``."""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv
from upstash_redis import Redis


def _load_repo_env() -> None:
    root = Path(__file__).resolve().parent.parent.parent
    load_dotenv(root / "src" / ".env")
    load_dotenv(root / ".env", override=False)


def _apply_redis_env_aliases() -> None:
    """Map optional names so ``Redis.from_env()`` works (it only reads Upstash's names)."""
    if not os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip():
        token = os.environ.get("REDIS_API_KEY", "").strip()
        if token:
            os.environ["UPSTASH_REDIS_REST_TOKEN"] = token
    if not os.environ.get("UPSTASH_REDIS_REST_URL", "").strip():
        for key in ("REDIS_URL", "REDIS_REST_URL"):
            url = os.environ.get(key, "").strip()
            if url:
                os.environ["UPSTASH_REDIS_REST_URL"] = url
                break


@lru_cache(maxsize=1)
def get_redis() -> Redis:
    """Shared client: same as ``Redis.from_env()`` after loading dotenv and aliases."""
    _load_repo_env()
    _apply_redis_env_aliases()
    try:
        return Redis.from_env()
    except KeyError as e:
        missing = str(e).strip("'\"")
        raise RuntimeError(
            "Add to src/.env: UPSTASH_REDIS_REST_URL (https://….upstash.io) and "
            "UPSTASH_REDIS_REST_TOKEN — or REDIS_URL + REDIS_API_KEY (token). "
            f"Missing: {missing}"
        ) from e
