# LeadGen

LeadGen is a Flask-based lead generation platform with multiple acquisition tools (Google Maps, LinkedIn, Instagram, Web Crawler), durable Google Maps session orchestration, operational diagnostics, and production-ready container deployment.

## Highlights

- Multi-tool lead collection with unified dashboard and database
- Durable Google Maps session lifecycle (events, tasks, chunk checkpoints)
- Queue-separated worker pools (extract vs contacts) with backpressure and per-user quotas
- Optional PostgreSQL mirror path for orchestration durability
- Ops observability APIs (dashboard, health, alerts, diagnostics, auto-recovery)
- UI continuity system (shared design primitives and consistent action/state affordances)

## Prerequisites

- Python 3.10+
- Google Chrome or Chromium
- Virtual environment support (`venv`)

## Local setup

```bash
cd LeadGen
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Run (macOS/Linux)

```bash
./run_app.sh
```

## Run (Windows)

```bat
run_app.bat
```

App URL: http://localhost:5000

## Docker run

```bash
docker compose up -d --build
```

- App: http://localhost:5000
- App health: http://localhost:5000/health
- Ops health: http://localhost:5000/health/ops

## Core runtime env vars

- `LEADGEN_SECRET_KEY` (required in production)
- `LEADGEN_DB_PATH` (default SQLite path)
- `LEADGEN_OUTPUT_DIR` (CSV output path)
- `LEADGEN_POSTGRES_DSN` (optional PostgreSQL mirror enablement)
- `LEADGEN_EXTRACT_WORKERS`, `LEADGEN_CONTACT_WORKERS`
- `LEADGEN_EXTRACT_MAX_PENDING`, `LEADGEN_CONTACT_MAX_PENDING`
- `LEADGEN_EXTRACT_PER_USER_ACTIVE_LIMIT`, `LEADGEN_CONTACT_PER_USER_ACTIVE_LIMIT`
- `LEADGEN_QUEUE_PENDING_TTL_SECONDS`
- `LEADGEN_AUTO_STALE_SWEEP`, `LEADGEN_AUTO_SWEEP_INTERVAL_SECONDS`, `LEADGEN_AUTO_SWEEP_STALE_SECONDS`
- `LEADGEN_RETENTION_ENABLED`, `LEADGEN_RETENTION_INTERVAL_SECONDS`
- `LEADGEN_RETENTION_EVENTS_DAYS`, `LEADGEN_RETENTION_LOGS_DAYS`, `LEADGEN_RETENTION_TASKS_DAYS`

## Key Google Maps ops endpoints

- `GET /api/gmaps/ops/dashboard`
- `GET /api/gmaps/ops/metrics`
- `GET /api/gmaps/ops/health`
- `GET /api/gmaps/ops/alerts`
- `GET /api/gmaps/sessions/<job_id>/diagnostics`
- `POST /api/gmaps/sessions/<job_id>/recover-auto`
- `GET /api/gmaps/worker-pools`

## Project structure (abridged)

- `app.py` — Flask app, orchestration, APIs
- `scraper.py`, `linkedin_scraper.py`, `instagram_scraper.py`, `web_crawler.py`
- `task_queue/dispatcher.py` — queue pools and backpressure
- `task_queue/postgres_mirror.py` — optional PostgreSQL mirror
- `templates/`, `static/` — UI
- `workers/` — worker runtime helpers
- `docker-compose.yml`, `Dockerfile` — container deployment

## Notes

- Google Maps scraping behavior depends on target-site dynamics and anti-bot controls.
- Use responsibly and in compliance with platform terms, laws, and privacy requirements.
