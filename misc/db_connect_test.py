"""Connect test for Supabase Postgres."""

import os
import sys
from pathlib import Path

import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from env_loader import load_repo_dotenv  # noqa: E402


def main() -> None:
    load_repo_dotenv()

    db_url = os.environ.get("SUPABASE_DB_URL")
    if not db_url:
        raise SystemExit("Missing SUPABASE_DB_URL in src/.env")

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute("select 1;")
            row = cur.fetchone()
            print("DB OK:", row[0])
    finally:
        conn.close()


if __name__ == "__main__":
    main()
