"""Minimal API-Football (api-sports) request - checks key and quota via /status."""

import json
import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from env_loader import load_repo_dotenv  # noqa: E402

load_repo_dotenv()

BASE_URL = "https://v3.football.api-sports.io"


def main() -> None:
    key = os.environ.get("FOOTBALL_API_KEY")
    if not key:
        print("Missing FOOTBALL_API_KEY - add it to src/.env", file=sys.stderr)
        sys.exit(1)

    url = f"{BASE_URL}/status"
    resp = requests.get(
        url,
        headers={"x-apisports-key": key},
        timeout=15,
    )

    print(f"GET {url}")
    print(f"HTTP {resp.status_code}\n")

    try:
        data = resp.json()
    except ValueError:
        print(resp.text)
        sys.exit(1)

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
