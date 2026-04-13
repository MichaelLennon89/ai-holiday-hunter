# AI Holiday Hunter — Cloud Product Repo

A deploy-ready repo for running AI Holiday Hunter as a cloud-hosted deal-finding platform with a website UI, background scan jobs, provider-drilling logic, evidence capture, and alerting hooks.

## What this repo does
- serves the website UI with FastAPI
- runs background jobs for live scans, autopilot, and alert generation
- supports Redis-backed cloud jobs when `REDIS_URL` is set
- falls back to local in-process background jobs when Redis is not configured
- stores results, evidence, baskets, tuning traces, and shortlist outputs in `results/`

## What it does not do
- it does not book or submit payment details
- it does not guarantee the cheapest live deal every single run
- it still depends on provider-side pricing, session state, anti-bot behaviour, and stock changes

## Quick start with Docker
```bash
docker compose up --build
```
Open `http://localhost:8000`.

## Environment
Copy `.env.example` to `.env` and set at least:
- `OPENAI_API_KEY` if you want AI-assisted extraction
- `REDIS_URL` for cloud queue mode
- SMTP vars if you want email alerts

## Local development without Docker
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
python -m playwright install chromium
uvicorn main:app --reload
```
Optional worker:
```bash
python workers/worker.py
```

## Deploy
- Railway: use the included `Dockerfile` and `railway.toml`
- Render: use the included `render.yaml`
- Generic container hosts: use `Dockerfile`, `Procfile`, `start_web.sh`, and `start_worker.sh`

## Key paths
- `main.py` — web app and API
- `browser_scan.py` — heavy live scan logic
- `engine.py` — scoring, planning, reports
- `site_extractors.py` — provider extraction
- `workers/worker.py` — Redis queue worker
- `cloud_queue.py` — Redis queue helpers
- `results/` — generated outputs and evidence

## Production notes
- Results are file-backed today. For long-term multi-instance production, move results/history metadata into Postgres or object storage.
- Redis queue mode avoids long UI stalls and lets the worker run scans separately from the web process.
- Use uptime monitoring against `/api/health`.
