# Misc — ingestion & API smoke tests

Run from the **repository root** so imports resolve (`env_loader` loads `src/.env` then `.env`).

```powershell
.\.venv\Scripts\Activate.ps1
python misc\testapi.py
python misc\db_connect_test.py
python misc\test_live_mls.py --match lafc-orlando
python misc\ingest_pl_2025_full.py
```

See each script’s docstring for flags. Requires API and Supabase keys in `src/.env`.
