# LeadGen Deployment Guide

This guide reflects the current LeadGen runtime (Flask + Gunicorn + Chromium, durable Google Maps orchestration, queue-separated workers, optional PostgreSQL mirror).

## Container architecture

- `leadgen` service: Flask app behind Gunicorn
- `postgres` service: optional mirror target for orchestration durability (enabled by DSN)
- Persistent volumes:
  - `/app/data` for SQLite state
  - `/app/output` for CSV output
  - Postgres data volume

## Quick deploy

```bash
docker compose up -d --build
```

Health endpoints:

- `GET /health`
- `GET /health/ops`

## Production recommendations

- Put Nginx/Caddy in front of port `5000`
- Terminate TLS at the reverse proxy
- Set strong `LEADGEN_SECRET_KEY`
- Use external Postgres and set `LEADGEN_POSTGRES_DSN`
- Use external rate-limit backend (`RATELIMIT_STORAGE_URI`) for multi-instance setups

## Required and important env vars

### Security and app

- `LEADGEN_SECRET_KEY` (required in production)
- `FLASK_ENV=production`
- `ALLOWED_ORIGINS`

### Web server tuning

- `GUNICORN_WORKERS` (default `2`)
- `GUNICORN_THREADS` (default `4`)
- `GUNICORN_TIMEOUT` (default `300`)

### Storage

- `LEADGEN_DB_PATH=/app/data/leadgen.db`
- `LEADGEN_OUTPUT_DIR=/app/output`
- `LEADGEN_POSTGRES_DSN` (optional mirror enablement)

### Queue and throughput controls

- `LEADGEN_EXTRACT_WORKERS`
- `LEADGEN_CONTACT_WORKERS`
- `LEADGEN_EXTRACT_MAX_PENDING`
- `LEADGEN_CONTACT_MAX_PENDING`
- `LEADGEN_EXTRACT_PER_USER_ACTIVE_LIMIT`
- `LEADGEN_CONTACT_PER_USER_ACTIVE_LIMIT`
- `LEADGEN_QUEUE_PENDING_TTL_SECONDS`

### Recovery and retention

- `LEADGEN_AUTO_STALE_SWEEP`
- `LEADGEN_AUTO_SWEEP_INTERVAL_SECONDS`
- `LEADGEN_AUTO_SWEEP_STALE_SECONDS`
- `LEADGEN_RETENTION_ENABLED`
- `LEADGEN_RETENTION_INTERVAL_SECONDS`
- `LEADGEN_RETENTION_EVENTS_DAYS`
- `LEADGEN_RETENTION_LOGS_DAYS`
- `LEADGEN_RETENTION_TASKS_DAYS`

### Stripe (optional)

- `STRIPE_SECRET_KEY`
- `STRIPE_WEBHOOK_SECRET`
- `STRIPE_PRICE_ID_PRO`

## Ops endpoints

- `GET /api/gmaps/ops/dashboard`
- `GET /api/gmaps/ops/metrics`
- `GET /api/gmaps/ops/health`
- `GET /api/gmaps/ops/alerts`
- `GET /api/gmaps/sessions/<job_id>/diagnostics`
- `POST /api/gmaps/sessions/<job_id>/recover-auto`
- `GET /api/gmaps/worker-pools`

## Verify deployment

```bash
docker compose ps
docker compose logs -f leadgen
curl http://localhost:5000/health
curl http://localhost:5000/health/ops
```

## Update workflow

```bash
git pull
docker compose up -d --build
```
