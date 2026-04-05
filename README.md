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
