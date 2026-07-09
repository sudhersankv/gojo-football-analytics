"""Resolve repo root and load secrets from ``src/.env`` then optional root ``.env``."""

from pathlib import Path

from dotenv import load_dotenv


def repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def load_repo_dotenv() -> Path:
    root = repo_root()
    load_dotenv(root / "src" / ".env")
    load_dotenv(root / ".env", override=False)
    return root
