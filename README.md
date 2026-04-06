# Gojo — Football analytics

Postgres schema in `sql/postgres_schema.sql`. Application code will live under `src/` (see `src/redis_keys.py`).

**Optional data tooling** (ingestion, API tests) is in [`misc/`](misc/README.md). Add `misc/` to [`.cursorignore`](.cursorignore) locally if you want Cursor to skip that folder.

## Setup

```powershell
cd d:\projects\Gojo
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Create `src/.env` with at least:

- `FOOTBALL_API_KEY`
- `SUPABASE_DB_URL`

**Redis (Upstash, REST)** — copy **REST URL** and **REST token** from the console into `src/.env`.

`Redis.from_env()` (and `get_redis()`) only read **`UPSTASH_REDIS_REST_URL`** and **`UPSTASH_REDIS_REST_TOKEN`**. Dotenv is **not** loaded automatically: call `get_redis()` or run `load_dotenv` before `Redis.from_env()`.

Aliases (after dotenv load, `get_redis()` maps these for you): **`REDIS_URL`** (or `REDIS_REST_URL`) → URL, **`REDIS_API_KEY`** → token.

Use `from gojo.redis_client import get_redis` with `PYTHONPATH` including `src`. See [Upstash Python SDK](https://upstash.com/docs/redis/sdks/py/gettingstarted).

Optional TCP client: `REDIS_URL` (`rediss://…`) or `UPSTASH_REDIS_HOST` / `UPSTASH_REDIS_PORT` / `UPSTASH_REDIS_PASSWORD` with `redis` + `ssl=True` — [connect your client](https://upstash.com/docs/redis/howto/connectclient).

## Remote

```text
https://github.com/sudhersankv/gojo-football-analytics.git
```

```powershell
git init
git remote add origin https://github.com/sudhersankv/gojo-football-analytics.git
git add -A
git commit -m "Initial layout: schema, src, misc tooling"
git branch -M main
git push -u origin main
```
