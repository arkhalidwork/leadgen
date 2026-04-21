"""
Core shared state and helpers for LeadGen application.

This module centralizes all shared state (job dicts, config, constants)
and common helper functions used across multiple route blueprints.
Prevents circular imports by being the single source of truth.

Blueprint modules import from here instead of from app.py.
"""
from __future__ import annotations

import os
import threading

# ── Shared in-memory job stores ──
scraping_jobs: dict = {}      # Google Maps jobs (legacy thread-based)
linkedin_jobs: dict = {}      # LinkedIn jobs
instagram_jobs: dict = {}     # Instagram jobs
webcrawler_jobs: dict = {}    # Web Crawler jobs

# ── Paths ──
OUTPUT_DIR = os.environ.get("LEADGEN_OUTPUT_DIR", os.path.join(os.path.dirname(os.path.dirname(__file__)), "output"))
DB_PATH = os.environ.get("LEADGEN_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "leadgen.db"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ── Desktop mode flag ──
IS_DESKTOP = os.environ.get("LEADGEN_DESKTOP", "").lower() in ("1", "true", "yes")

# ── Instagram search-type aliases (old → new) ──
IG_TYPE_MAP = {"emails": "profiles", "profiles": "profiles", "businesses": "businesses"}

# ── Stripe config ──
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_ID_PRO = os.environ.get("STRIPE_PRICE_ID_PRO", "")

# ── GMaps config ──
MAX_FINISHED_JOBS = 20
GMAPS_JOB_STATES = {"PENDING", "RUNNING", "PARTIAL", "COMPLETED", "FAILED"}
AUTO_SWEEP_ENABLED = os.environ.get("LEADGEN_AUTO_STALE_SWEEP", "1").lower() in ("1", "true", "yes")
AUTO_SWEEP_INTERVAL_SECONDS = max(15, int(os.environ.get("LEADGEN_AUTO_SWEEP_INTERVAL_SECONDS", "60")))
AUTO_SWEEP_STALE_SECONDS = max(60, int(os.environ.get("LEADGEN_AUTO_SWEEP_STALE_SECONDS", "180")))
TASK_RETRY_MAX_ATTEMPTS_DEFAULT = max(1, int(os.environ.get("LEADGEN_TASK_RETRY_MAX_ATTEMPTS", "3")))
TASK_RETRY_BACKOFF_SECONDS_DEFAULT = max(10, int(os.environ.get("LEADGEN_TASK_RETRY_BACKOFF_SECONDS", "45")))
OPERATOR_OVERRIDE_ALL = os.environ.get("LEADGEN_OPERATOR_OVERRIDE_ALL", "0").lower() in ("1", "true", "yes")
OPERATOR_EMAIL_ALLOWLIST = {
    e.strip().lower()
    for e in os.environ.get("LEADGEN_OPERATOR_EMAILS", "").split(",")
    if e.strip()
}
RETENTION_ENABLED = os.environ.get("LEADGEN_RETENTION_ENABLED", "1").lower() in ("1", "true", "yes")
RETENTION_INTERVAL_SECONDS = max(300, int(os.environ.get("LEADGEN_RETENTION_INTERVAL_SECONDS", "3600")))
RETENTION_EVENTS_DAYS = max(7, int(os.environ.get("LEADGEN_RETENTION_EVENTS_DAYS", "45")))
RETENTION_LOGS_DAYS = max(7, int(os.environ.get("LEADGEN_RETENTION_LOGS_DAYS", "30")))
RETENTION_TASKS_DAYS = max(7, int(os.environ.get("LEADGEN_RETENTION_TASKS_DAYS", "60")))
CONTACT_TASK_CHUNK_SIZE = max(5, int(os.environ.get("LEADGEN_CONTACT_TASK_CHUNK_SIZE", "25")))
OPS_METRICS_DEFAULT_WINDOW_HOURS = max(1, int(os.environ.get("LEADGEN_OPS_METRICS_WINDOW_HOURS", "24")))
OPS_ALERT_QUEUE_WARN_PCT = min(1.0, max(0.1, float(os.environ.get("LEADGEN_OPS_QUEUE_WARN_PCT", "0.80"))))
OPS_ALERT_QUEUE_CRIT_PCT = min(1.0, max(OPS_ALERT_QUEUE_WARN_PCT, float(os.environ.get("LEADGEN_OPS_QUEUE_CRIT_PCT", "0.95"))))
OPS_ALERT_FAILURE_WARN = max(1, int(os.environ.get("LEADGEN_OPS_FAILURE_WARN", "8")))
OPS_ALERT_FAILURE_CRIT = max(OPS_ALERT_FAILURE_WARN, int(os.environ.get("LEADGEN_OPS_FAILURE_CRIT", "20")))

# ── Thread locks for auto-sweep / retention ──
auto_sweep_lock = threading.Lock()
last_auto_sweep_at = 0.0  # mutable — blueprints update this
retention_lock = threading.Lock()
last_retention_at = 0.0
last_retention_summary = {
    "last_run_at": None,
    "events_deleted": 0,
    "logs_deleted": 0,
    "tasks_deleted": 0,
    "error": None,
}
