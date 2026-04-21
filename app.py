"""
LeadGen - Lead Generation Suite
Flask backend with multi-tool scraping API, CSV export,
authentication, license keys, scrape history, and lead analytics.
"""

import os
import atexit
import re
import csv
import io
import json
import uuid
import hmac
import hashlib
import secrets
import sqlite3
import logging
import threading
import functools
import time
from datetime import datetime, timedelta

import bcrypt
import stripe
from flask import Flask, render_template, request, jsonify, Response, session, redirect, url_for, g, abort
from flask_cors import CORS
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect

from scraper import GoogleMapsScraper, clean_leads
from linkedin_scraper import LinkedInScraper, clean_linkedin_leads
from instagram_scraper import InstagramScraper, clean_instagram_leads
from web_crawler import WebCrawlerScraper, clean_web_leads
from task_queue.job_store import (
    save_job_state,
    get_job_state,
    set_job_stop_requested,
    is_job_stop_requested,
    list_job_states,
)
from task_queue.dispatcher import submit_extract_job, submit_contact_job, worker_pool_stats
from task_queue.postgres_mirror import (
    ensure_schema as pg_ensure_schema,
    postgres_enabled as pg_enabled,
    mirror_session_state as pg_mirror_session_state,
    mirror_event as pg_mirror_event,
    mirror_task as pg_mirror_task,
    mirror_task_chunk as pg_mirror_task_chunk,
)
from workers.scraper_worker import run_scraper_job

# Phase 2: Queue system imports
from config import QUEUE_ENABLED, TOOL_CONFIG, MAX_ACTIVE_JOBS_PER_USER
from jobs.store import (
    ensure_jobs_table as _ensure_jobs_table,
    create_job as _create_queue_job,
    get_job as _get_queue_job,
    update_job as _update_queue_job,
    count_active_jobs as _count_active_queue_jobs,
)
from jobs.queue import (
    enqueue_job as _enqueue_redis_job,
    set_stop_signal as _set_redis_stop,
    queue_health as _queue_health,
)
from jobs.sweeper import start_sweeper_thread as _start_sweeper

# Phase 3: Agent routing
from agents.service import (
    assign_execution_mode as _assign_execution_mode,
    get_active_agent_for_user as _get_active_agent,
)
from agents.routes import agents_bp as _agents_bp

# Phase 4: Intelligence Layer
from intelligence.routes import intelligence_bp as _intelligence_bp

# Phase 5: CRM / Outreach / Workflows
from crm.routes       import crm_bp       as _crm_bp
from outreach.routes  import outreach_bp  as _outreach_bp
from workflows.routes import workflows_bp as _workflows_bp

# Phase A: SSE real-time streaming
from api.sse import sse_bp as _sse_bp, publish as _sse_publish

# Phase B: Extracted blueprint modules
from api.linkedin import linkedin_bp as _linkedin_bp
from api.instagram import instagram_bp as _instagram_bp
from api.webcrawler import webcrawler_bp as _webcrawler_bp
# Note: api.pages and api.health are available but not registered here
# because the equivalent routes still exist in this file during migration.

# Instagram search-type aliases (old → new)
_IG_TYPE_MAP = {"emails": "profiles", "profiles": "profiles", "businesses": "businesses"}

# --------------- Logging ---------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger(__name__)

app = Flask(
    __name__,
    template_folder=os.environ.get("LEADGEN_TEMPLATE_DIR", os.path.join(os.path.dirname(__file__), "templates")),
    static_folder=os.environ.get("LEADGEN_STATIC_DIR", os.path.join(os.path.dirname(__file__), "static")),
)

# --- Security configuration ---
_secret = os.environ.get("LEADGEN_SECRET_KEY", "")
if not _secret or _secret == "change-me-to-a-random-64-char-hex-string":
    if os.environ.get("FLASK_ENV") == "production":
        raise RuntimeError("LEADGEN_SECRET_KEY must be set in production!")
    _secret = "leadgen-dev-secret-do-not-use-in-prod"
app.secret_key = _secret

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("FLASK_ENV") == "production",
    PERMANENT_SESSION_LIFETIME=timedelta(days=30),
    WTF_CSRF_TIME_LIMIT=3600,  # 1-hour CSRF token validity
)
app.permanent_session_lifetime = timedelta(days=30)

# --- CSRF protection (auto-injects token in Jinja templates) ---
# Exempt all /api/ routes since they use JSON + SameSite session cookies
csrf = CSRFProtect(app)


@csrf.exempt
def _is_api_route():
    """Marker — not called directly. See exempt_api_blueprint below."""
    pass


# Exempt /api/ routes: CSRFProtect checks happen before before_request,
# so we disable the default check and run it manually for non-API routes.
app.config["WTF_CSRF_CHECK_DEFAULT"] = False


@app.before_request
def _csrf_protect_non_api():
    """Enforce CSRF on form-based routes, skip for /api/ JSON endpoints."""
    if not request.path.startswith("/api/"):
        try:
            csrf.protect()
        except Exception:
            pass  # Let Flask-WTF handle the error response


@app.before_request
def _auto_sweep_stale_tasks_before_request():
    """Best-effort automatic stale-task recovery pass (throttled)."""
    try:
        _run_auto_stale_task_sweeper()
    except Exception:
        pass


@app.before_request
def _auto_retention_cleanup_before_request():
    """Best-effort retention cleanup pass (throttled)."""
    try:
        _run_auto_retention_cleanup()
    except Exception:
        pass

# --- CORS ---
_origins = os.environ.get("ALLOWED_ORIGINS", "*").split(",")
CORS(app, origins=[o.strip() for o in _origins], supports_credentials=True)

# --- Rate limiting ---
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=["200 per minute"],
    storage_uri=os.environ.get("RATELIMIT_STORAGE_URI", "memory://"),
)

# --- Stripe config ---
stripe.api_key = os.environ.get("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "")
STRIPE_PRICE_ID_PRO = os.environ.get("STRIPE_PRICE_ID_PRO", "")

# Desktop mode flag - skip landing page when running via pywebview
IS_DESKTOP = os.environ.get("LEADGEN_DESKTOP", "").lower() in ("1", "true", "yes")

# Store active scraping jobs and their results
scraping_jobs = {}          # Google Maps jobs
linkedin_jobs = {}          # LinkedIn jobs
instagram_jobs = {}         # Instagram jobs
webcrawler_jobs = {}        # Web Crawler jobs
OUTPUT_DIR = os.environ.get("LEADGEN_OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "output"))
DB_PATH = os.environ.get("LEADGEN_DB_PATH", os.path.join(os.path.dirname(__file__), "leadgen.db"))
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Maximum number of completed jobs to keep in memory per store
_MAX_FINISHED_JOBS = 20
GMAPS_JOB_STATES = {"PENDING", "RUNNING", "PARTIAL", "COMPLETED", "FAILED"}
_AUTO_SWEEP_ENABLED = os.environ.get("LEADGEN_AUTO_STALE_SWEEP", "1").lower() in ("1", "true", "yes")
_AUTO_SWEEP_INTERVAL_SECONDS = max(15, int(os.environ.get("LEADGEN_AUTO_SWEEP_INTERVAL_SECONDS", "60")))
_AUTO_SWEEP_STALE_SECONDS = max(60, int(os.environ.get("LEADGEN_AUTO_SWEEP_STALE_SECONDS", "180")))
_TASK_RETRY_MAX_ATTEMPTS_DEFAULT = max(1, int(os.environ.get("LEADGEN_TASK_RETRY_MAX_ATTEMPTS", "3")))
_TASK_RETRY_BACKOFF_SECONDS_DEFAULT = max(10, int(os.environ.get("LEADGEN_TASK_RETRY_BACKOFF_SECONDS", "45")))
_OPERATOR_OVERRIDE_ALL = os.environ.get("LEADGEN_OPERATOR_OVERRIDE_ALL", "0").lower() in ("1", "true", "yes")
_OPERATOR_EMAIL_ALLOWLIST = {
    e.strip().lower()
    for e in os.environ.get("LEADGEN_OPERATOR_EMAILS", "").split(",")
    if e.strip()
}
_RETENTION_ENABLED = os.environ.get("LEADGEN_RETENTION_ENABLED", "1").lower() in ("1", "true", "yes")
_RETENTION_INTERVAL_SECONDS = max(300, int(os.environ.get("LEADGEN_RETENTION_INTERVAL_SECONDS", "3600")))
_RETENTION_EVENTS_DAYS = max(7, int(os.environ.get("LEADGEN_RETENTION_EVENTS_DAYS", "45")))
_RETENTION_LOGS_DAYS = max(7, int(os.environ.get("LEADGEN_RETENTION_LOGS_DAYS", "30")))
_RETENTION_TASKS_DAYS = max(7, int(os.environ.get("LEADGEN_RETENTION_TASKS_DAYS", "60")))
_CONTACT_TASK_CHUNK_SIZE = max(5, int(os.environ.get("LEADGEN_CONTACT_TASK_CHUNK_SIZE", "25")))
_OPS_METRICS_DEFAULT_WINDOW_HOURS = max(1, int(os.environ.get("LEADGEN_OPS_METRICS_WINDOW_HOURS", "24")))
_OPS_ALERT_QUEUE_WARN_PCT = min(1.0, max(0.1, float(os.environ.get("LEADGEN_OPS_QUEUE_WARN_PCT", "0.80"))))
_OPS_ALERT_QUEUE_CRIT_PCT = min(1.0, max(_OPS_ALERT_QUEUE_WARN_PCT, float(os.environ.get("LEADGEN_OPS_QUEUE_CRIT_PCT", "0.95"))))
_OPS_ALERT_FAILURE_WARN = max(1, int(os.environ.get("LEADGEN_OPS_FAILURE_WARN", "8")))
_OPS_ALERT_FAILURE_CRIT = max(_OPS_ALERT_FAILURE_WARN, int(os.environ.get("LEADGEN_OPS_FAILURE_CRIT", "20")))
_auto_sweep_lock = threading.Lock()
_last_auto_sweep_at = 0.0
_retention_lock = threading.Lock()
_last_retention_at = 0.0
_last_retention_summary = {
    "last_run_at": None,
    "events_deleted": 0,
    "logs_deleted": 0,
    "tasks_deleted": 0,
    "error": None,
}


def _cleanup_jobs(store: dict, max_keep: int = _MAX_FINISHED_JOBS):
    """Remove oldest finished jobs when store exceeds max_keep completed entries."""
    finished = [(jid, j) for jid, j in store.items()
                if getattr(j, "status", "") in ("completed", "failed", "stopped")]
    if len(finished) <= max_keep:
        return
    # Sort by start time, oldest first
    finished.sort(key=lambda x: getattr(x[1], "started_at", datetime.min))
    for jid, _ in finished[: len(finished) - max_keep]:
        store.pop(jid, None)


def _state_for_frontend(job_state: dict) -> dict:
    """Map queue job state to legacy frontend shape without losing new lifecycle fields."""
    state = dict(job_state)
    lifecycle = str(state.get("status", "PENDING")).upper()

    if lifecycle == "COMPLETED":
        legacy_status = "completed"
    elif lifecycle == "FAILED":
        legacy_status = "failed"
    elif lifecycle == "PARTIAL":
        legacy_status = "stopped"
    elif lifecycle in ("RUNNING", "PENDING"):
        legacy_status = "running"
    else:
        legacy_status = "running"

    state["lifecycle_status"] = lifecycle
    state["status"] = legacy_status
    state.setdefault("progress", 0)
    state.setdefault("message", "Queued...")
    state.setdefault("results_count", 0)
    state.setdefault("area_stats", {})
    state.setdefault("lead_count", state.get("results_count", 0))
    state.setdefault("phase", "extract")
    state.setdefault("contacts_status", "pending")
    state.setdefault("logs", [])

    # --- Phase 1: Computed real-time metrics ---
    created_at = state.get("created_at")
    now = datetime.utcnow()
    elapsed_seconds = 0
    if created_at:
        try:
            start = datetime.fromisoformat(str(created_at))
            elapsed_seconds = max(0, int((now - start).total_seconds()))
        except (ValueError, TypeError):
            pass
    state["elapsed_seconds"] = elapsed_seconds

    # Speed: leads per minute
    results_count = state.get("results_count", 0) or 0
    if elapsed_seconds > 10 and results_count > 0:
        speed = round((results_count / elapsed_seconds) * 60, 1)
    else:
        speed = 0
    state["speed"] = speed

    # ETA: estimated seconds remaining
    progress = state.get("progress", 0) or 0
    if progress > 5 and progress < 100 and elapsed_seconds > 10:
        eta_seconds = int((elapsed_seconds / progress) * (100 - progress))
    else:
        eta_seconds = None
    state["eta_seconds"] = eta_seconds

    # Phase detail text
    phase = state.get("phase", "extract")
    area_stats = state.get("area_stats", {})
    if phase == "extract":
        total = area_stats.get("geo_cells_total", 0) or 0
        done = area_stats.get("geo_cells_completed", 0) or 0
        if total > 0:
            phase_detail = f"Scanning area {done}/{total}"
        else:
            phase_detail = "Initializing extraction..."
    elif phase == "contacts":
        ws_total = area_stats.get("websites_total", 0) or 0
        ws_done = area_stats.get("websites_scanned", 0) or 0
        phase_detail = f"Enriching contacts ({ws_done}/{ws_total})"
    else:
        phase_detail = state.get("message", "Working...")
    state["phase_detail"] = phase_detail

    return state


def _append_job_log(state: dict, message: str, percent: int | None = None):
    logs = state.get("logs") if isinstance(state.get("logs"), list) else []
    entry = {
        "at": datetime.utcnow().isoformat(),
        "message": str(message),
    }
    if percent is not None:
        entry["progress"] = int(max(0, min(100, percent)))
    logs.append(entry)
    state["logs"] = logs[-300:]


def _structured_log(name: str, *, level: str = "info", **fields):
    payload = {
        "ts": datetime.utcnow().isoformat(),
        "event": str(name or "event"),
        **fields,
    }
    line = json.dumps(payload, default=str, separators=(",", ":"))
    lvl = str(level or "info").lower()
    if lvl == "error":
        log.error(line)
    elif lvl in {"warning", "warn"}:
        log.warning(line)
    else:
        log.info(line)


def _ensure_lead_uid(lead: dict) -> str:
    uid = str(lead.get("lead_uid") or "").strip()
    if uid:
        return uid
    key = "|".join([
        str(lead.get("business_name") or "").strip().lower(),
        str(lead.get("latitude") or "").strip(),
        str(lead.get("longitude") or "").strip(),
        str(lead.get("address") or "").strip().lower(),
    ])
    uid = hashlib.sha1(key.encode("utf-8")).hexdigest()[:16]
    lead["lead_uid"] = uid
    return uid


def _persist_gmaps_state(state: dict):
    """Persist Google Maps session snapshot + leads + latest log into SQLite."""
    if state.get("tool") != "gmaps":
        return

    session_id = str(state.get("job_id") or "").strip()
    user_id = state.get("user_id")
    if not session_id or not user_id:
        return

    keyword = str(state.get("keyword") or "")
    place = str(state.get("place") or "")
    max_leads = state.get("max_leads")
    phase = str(state.get("phase") or "extract")
    extraction_status = str(state.get("extraction_status") or "pending")
    contacts_status = str(state.get("contacts_status") or "pending")
    status = str(state.get("status") or "PENDING")
    progress = int(state.get("progress") or 0)
    message = str(state.get("message") or "")
    created_at = state.get("created_at") or datetime.utcnow().isoformat()
    updated_at = state.get("updated_at") or datetime.utcnow().isoformat()
    finished_at = state.get("finished_at")
    results = state.get("results") if isinstance(state.get("results"), list) else []
    results_count = len(results)
    status_upper = status.upper()

    if status_upper == "COMPLETED" and contacts_status == "completed":
        history_status = "completed"
    elif status_upper == "FAILED":
        history_status = "failed"
    elif status_upper in ("PARTIAL", "STOPPED") or extraction_status in ("partial", "failed") or contacts_status in ("paused", "failed"):
        history_status = "stopped"
    else:
        history_status = "running"

    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")

        db.execute(
            """
            INSERT INTO gmaps_sessions (
                session_id, user_id, keyword, place, max_leads,
                phase, extraction_status, contacts_status, status,
                progress, message, results_count, created_at, updated_at, finished_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id) DO UPDATE SET
                keyword=excluded.keyword,
                place=excluded.place,
                max_leads=excluded.max_leads,
                phase=excluded.phase,
                extraction_status=excluded.extraction_status,
                contacts_status=excluded.contacts_status,
                status=excluded.status,
                progress=excluded.progress,
                message=excluded.message,
                results_count=excluded.results_count,
                updated_at=excluded.updated_at,
                finished_at=excluded.finished_at
            """,
            (
                session_id, user_id, keyword, place, max_leads,
                phase, extraction_status, contacts_status, status,
                progress, message, results_count, created_at, updated_at, finished_at,
            ),
        )

        # Mirror session data into primary lead database tables so session leads
        # are always accessible from the Database view.
        scrape_row = db.execute(
            """
            SELECT id FROM scrape_history
            WHERE user_id = ? AND job_id = ? AND tool = 'gmaps'
            ORDER BY id DESC
            LIMIT 1
            """,
            (user_id, session_id),
        ).fetchone()

        if scrape_row:
            scrape_id = int(scrape_row[0])
        else:
            db.execute(
                """
                INSERT INTO scrape_history (
                    user_id, job_id, tool, keyword, location, search_type,
                    status, lead_count, started_at
                ) VALUES (?, ?, 'gmaps', ?, ?, '', ?, ?, ?)
                """,
                (
                    user_id,
                    session_id,
                    keyword,
                    place,
                    history_status,
                    results_count,
                    created_at,
                ),
            )
            scrape_id = int(db.execute("SELECT last_insert_rowid()").fetchone()[0])

        db.execute(
            """
            UPDATE scrape_history
            SET keyword = ?,
                location = ?,
                status = ?,
                lead_count = ?,
                finished_at = CASE
                    WHEN ? IN ('completed', 'failed', 'stopped') THEN COALESCE(?, datetime('now'))
                    ELSE NULL
                END
            WHERE id = ? AND user_id = ?
            """,
            (
                keyword,
                place,
                history_status,
                results_count,
                history_status,
                finished_at,
                scrape_id,
                user_id,
            ),
        )

        # Keep the primary leads table in-sync with the latest session snapshot.
        db.execute(
            "DELETE FROM leads WHERE user_id = ? AND scrape_id = ? AND tool = 'gmaps'",
            (user_id, scrape_id),
        )

        mirror_rows = []
        for lead in results:
            lead_uid = _ensure_lead_uid(lead)
            email = str(lead.get("email") or "")
            phone = str(lead.get("phone") or "")
            website = str(lead.get("website") or "")
            has_email = bool(email and email != "N/A")
            has_phone = bool(phone and phone != "N/A")
            quality = "strong" if has_email and has_phone else ("medium" if has_email or has_phone else "weak")
            payload = dict(lead)
            payload["lead_uid"] = lead_uid
            mirror_rows.append((
                user_id,
                scrape_id,
                "gmaps",
                keyword,
                place,
                str(lead.get("business_name") or ""),
                email,
                phone,
                website,
                quality,
                json.dumps(payload, default=str),
            ))

        if mirror_rows:
            db.executemany(
                """
                INSERT INTO leads (
                    user_id, scrape_id, tool, keyword, location,
                    title, email, phone, website, quality, data
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                mirror_rows,
            )

        for lead in results:
            lead_uid = _ensure_lead_uid(lead)
            email = str(lead.get("email") or "")
            phone = str(lead.get("phone") or "")
            website = str(lead.get("website") or "")
            is_complete = 1 if (email and email != "N/A") or (phone and phone != "N/A") else 0
            db.execute(
                """
                INSERT INTO gmaps_session_leads (
                    session_id, user_id, lead_uid, business_name, owner_name,
                    phone, website, email, address, rating, reviews,
                    category, latitude, longitude, facebook, instagram,
                    twitter, linkedin, youtube, tiktok, pinterest,
                    stage, is_complete, payload, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
                ON CONFLICT(session_id, lead_uid) DO UPDATE SET
                    business_name=excluded.business_name,
                    owner_name=excluded.owner_name,
                    phone=excluded.phone,
                    website=excluded.website,
                    email=excluded.email,
                    address=excluded.address,
                    rating=excluded.rating,
                    reviews=excluded.reviews,
                    category=excluded.category,
                    latitude=excluded.latitude,
                    longitude=excluded.longitude,
                    facebook=excluded.facebook,
                    instagram=excluded.instagram,
                    twitter=excluded.twitter,
                    linkedin=excluded.linkedin,
                    youtube=excluded.youtube,
                    tiktok=excluded.tiktok,
                    pinterest=excluded.pinterest,
                    stage=excluded.stage,
                    is_complete=excluded.is_complete,
                    payload=excluded.payload,
                    updated_at=datetime('now')
                """,
                (
                    session_id,
                    user_id,
                    lead_uid,
                    str(lead.get("business_name") or ""),
                    str(lead.get("owner_name") or ""),
                    phone,
                    website,
                    email,
                    str(lead.get("address") or ""),
                    str(lead.get("rating") or ""),
                    str(lead.get("reviews") or ""),
                    str(lead.get("category") or ""),
                    str(lead.get("latitude") or ""),
                    str(lead.get("longitude") or ""),
                    str(lead.get("facebook") or ""),
                    str(lead.get("instagram") or ""),
                    str(lead.get("twitter") or ""),
                    str(lead.get("linkedin") or ""),
                    str(lead.get("youtube") or ""),
                    str(lead.get("tiktok") or ""),
                    str(lead.get("pinterest") or ""),
                    phase,
                    is_complete,
                    json.dumps(lead, default=str),
                ),
            )

        logs = state.get("logs") if isinstance(state.get("logs"), list) else []
        if logs:
            latest = logs[-1]
            log_msg = str(latest.get("message") or "")
            log_progress = latest.get("progress")
            log_at = latest.get("at") or datetime.utcnow().isoformat()
            log_hash = hashlib.sha1(f"{session_id}|{log_at}|{log_msg}|{log_progress}".encode("utf-8")).hexdigest()
            db.execute(
                """
                INSERT OR IGNORE INTO gmaps_session_logs (
                    session_id, user_id, phase, progress, message, log_hash, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session_id,
                    user_id,
                    phase,
                    int(log_progress) if isinstance(log_progress, int) else None,
                    log_msg,
                    log_hash,
                    log_at,
                ),
            )

        db.commit()
        db.close()

        if pg_enabled():
            pg_mirror_session_state(state)
    except Exception as exc:
        log.error(f"Failed to persist gmaps session state {session_id}: {exc}")


def _persist_gmaps_event(state: dict, event_type: str, message: str,
                         *, severity: str = "info", payload: dict | None = None):
    """Persist durable event timeline for Google Maps sessions."""
    if state.get("tool") != "gmaps":
        return

    session_id = str(state.get("job_id") or "").strip()
    user_id = state.get("user_id")
    if not session_id or not user_id:
        return

    phase = str(state.get("phase") or "extract")
    status = str(state.get("status") or "PENDING")
    progress = int(state.get("progress") or 0)
    created_at = datetime.utcnow().isoformat()
    safe_message = str(message or "")[:1000]
    payload_json = json.dumps(payload or {}, default=str)

    event_hash_src = "|".join([
        session_id,
        str(user_id),
        str(event_type),
        str(severity),
        phase,
        status,
        str(progress),
        safe_message,
        payload_json,
    ])
    event_hash = hashlib.sha1(event_hash_src.encode("utf-8")).hexdigest()

    _structured_log(
        "gmaps_session_event",
        level=severity,
        session_id=session_id,
        user_id=user_id,
        event_type=event_type,
        severity=severity,
        phase=phase,
        status=status,
        progress=progress,
        message=safe_message,
    )

    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            """
            INSERT OR IGNORE INTO gmaps_session_events (
                session_id, user_id, event_type, severity,
                phase, status, progress, message, payload,
                event_hash, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                session_id,
                user_id,
                event_type,
                severity,
                phase,
                status,
                progress,
                safe_message,
                payload_json,
                event_hash,
                created_at,
            ),
        )
        db.commit()
        db.close()

        if pg_enabled():
            pg_mirror_event(
                session_id=session_id,
                user_id=int(user_id),
                event_type=str(event_type),
                severity=str(severity),
                phase=phase,
                status=status,
                progress=progress,
                message=safe_message,
                payload=payload or {},
                event_hash=event_hash,
                created_at=created_at,
            )
    except Exception as exc:
        log.error(f"Failed to persist gmaps event {session_id}:{event_type}: {exc}")


def _persist_partial_snapshot_checkpoint(state: dict, *, reason: str):
    """Emit durable checkpoint confirming partial data was persisted before transition."""
    if state.get("tool") != "gmaps":
        return

    results = state.get("results") if isinstance(state.get("results"), list) else []
    _persist_gmaps_event(
        state,
        "partial_snapshot_persisted",
        f"Persisted partial snapshot with {len(results)} lead(s) before transition",
        severity="warning",
        payload={
            "reason": (reason or "")[:120],
            "phase": state.get("phase"),
            "status": state.get("status"),
            "results_count": len(results),
        },
    )


def _upsert_gmaps_task(*, session_id: str, user_id: int, task_key: str,
                       phase: str, status: str,
                       payload: dict | None = None,
                       error: str | None = None,
                       retry_reason: str | None = None,
                       max_attempts: int | None = None,
                       retry_backoff_seconds: int | None = None):
    """Persist task lifecycle transitions for resumable orchestration."""
    if not session_id or not user_id or not task_key:
        return

    now = datetime.utcnow().isoformat()
    payload_json = json.dumps(payload or {}, default=str)
    safe_error = (error or "")[:2000]
    safe_status = str(status or "pending").strip().lower()
    safe_retry_reason = (retry_reason or "")[:500]
    safe_max_attempts = int(max_attempts or _TASK_RETRY_MAX_ATTEMPTS_DEFAULT)
    safe_backoff = int(retry_backoff_seconds or _TASK_RETRY_BACKOFF_SECONDS_DEFAULT)
    safe_max_attempts = max(1, safe_max_attempts)
    safe_backoff = max(10, safe_backoff)

    started_at = now if safe_status == "running" else None
    finished_at = now if safe_status in ("completed", "failed", "canceled") else None
    retry_cooldown_until = (
        (datetime.utcnow() + timedelta(seconds=safe_backoff)).isoformat()
        if safe_status == "running"
        else None
    )
    last_retry_at = now if safe_status == "running" else None

    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            """
            INSERT INTO gmaps_session_tasks (
                session_id, user_id, task_key, phase, status,
                attempt_count, last_error, payload,
                max_attempts, retry_backoff_seconds, retry_cooldown_until,
                last_retry_reason, last_retry_at,
                started_at, finished_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id, task_key) DO UPDATE SET
                phase=excluded.phase,
                status=excluded.status,
                attempt_count=CASE
                    WHEN excluded.status='running' THEN gmaps_session_tasks.attempt_count + 1
                    ELSE gmaps_session_tasks.attempt_count
                END,
                last_error=CASE
                    WHEN excluded.last_error!='' THEN excluded.last_error
                    ELSE gmaps_session_tasks.last_error
                END,
                payload=excluded.payload,
                max_attempts=COALESCE(gmaps_session_tasks.max_attempts, excluded.max_attempts),
                retry_backoff_seconds=COALESCE(gmaps_session_tasks.retry_backoff_seconds, excluded.retry_backoff_seconds),
                retry_cooldown_until=CASE
                    WHEN excluded.status='running' THEN excluded.retry_cooldown_until
                    ELSE gmaps_session_tasks.retry_cooldown_until
                END,
                last_retry_reason=CASE
                    WHEN excluded.last_retry_reason!='' THEN excluded.last_retry_reason
                    ELSE gmaps_session_tasks.last_retry_reason
                END,
                last_retry_at=CASE
                    WHEN excluded.status='running' THEN excluded.last_retry_at
                    ELSE gmaps_session_tasks.last_retry_at
                END,
                started_at=CASE
                    WHEN excluded.status='running' THEN excluded.started_at
                    ELSE gmaps_session_tasks.started_at
                END,
                finished_at=CASE
                    WHEN excluded.status IN ('completed', 'failed', 'canceled') THEN excluded.finished_at
                    ELSE gmaps_session_tasks.finished_at
                END,
                updated_at=excluded.updated_at
            """,
            (
                session_id,
                user_id,
                task_key,
                phase,
                safe_status,
                safe_error,
                payload_json,
                safe_max_attempts,
                safe_backoff,
                retry_cooldown_until,
                safe_retry_reason,
                last_retry_at,
                started_at,
                finished_at,
                now,
                now,
            ),
        )
        db.commit()
        db.close()

        if pg_enabled():
            existing = _load_gmaps_task_record(session_id, task_key) or {}
            pg_mirror_task(
                session_id=session_id,
                user_id=int(user_id),
                task_key=task_key,
                phase=str(phase or "extract"),
                status=safe_status,
                attempt_count=int(existing.get("attempt_count") or (1 if safe_status == "running" else 0)),
                last_error=safe_error,
                payload=payload or {},
                max_attempts=safe_max_attempts,
                retry_backoff_seconds=safe_backoff,
                retry_cooldown_until=retry_cooldown_until,
                last_retry_reason=safe_retry_reason,
                last_retry_at=last_retry_at,
                started_at=started_at,
                finished_at=finished_at,
                now=now,
            )
    except Exception as exc:
        log.error(f"Failed to upsert gmaps task {session_id}:{task_key}: {exc}")


def _upsert_gmaps_task_chunk(*, session_id: str, user_id: int, task_key: str,
                             chunk_key: str, status: str,
                             payload: dict | None = None,
                             error: str | None = None):
    if not session_id or not user_id or not task_key or not chunk_key:
        return

    now = datetime.utcnow().isoformat()
    payload_json = json.dumps(payload or {}, default=str)
    safe_error = (error or "")[:2000]
    safe_status = str(status or "pending").strip().lower()
    started_at = now if safe_status == "running" else None
    finished_at = now if safe_status in ("completed", "failed", "canceled") else None

    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            """
            INSERT INTO gmaps_task_chunks (
                session_id, user_id, task_key, chunk_key, status,
                attempt_count, last_error, payload,
                started_at, finished_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(session_id, task_key, chunk_key) DO UPDATE SET
                status=excluded.status,
                attempt_count=CASE
                    WHEN excluded.status='running' THEN gmaps_task_chunks.attempt_count + 1
                    ELSE gmaps_task_chunks.attempt_count
                END,
                last_error=CASE
                    WHEN excluded.last_error!='' THEN excluded.last_error
                    ELSE gmaps_task_chunks.last_error
                END,
                payload=excluded.payload,
                started_at=CASE
                    WHEN excluded.status='running' THEN excluded.started_at
                    ELSE gmaps_task_chunks.started_at
                END,
                finished_at=CASE
                    WHEN excluded.status IN ('completed', 'failed', 'canceled') THEN excluded.finished_at
                    ELSE gmaps_task_chunks.finished_at
                END,
                updated_at=excluded.updated_at
            """,
            (
                session_id,
                user_id,
                task_key,
                chunk_key,
                safe_status,
                safe_error,
                payload_json,
                started_at,
                finished_at,
                now,
                now,
            ),
        )
        db.commit()
        db.close()

        if pg_enabled():
            existing_attempt = 0
            rows = _load_gmaps_task_chunks(session_id, task_key)
            for row in rows:
                if str(row.get("chunk_key") or "") == str(chunk_key):
                    existing_attempt = int(row.get("attempt_count") or 0)
                    break
            pg_mirror_task_chunk(
                session_id=session_id,
                user_id=int(user_id),
                task_key=task_key,
                chunk_key=chunk_key,
                status=safe_status,
                attempt_count=(existing_attempt + 1 if safe_status == "running" else existing_attempt),
                last_error=safe_error,
                payload=payload or {},
                started_at=started_at,
                finished_at=finished_at,
                now=now,
            )
    except Exception as exc:
        log.error(f"Failed to upsert gmaps task chunk {session_id}:{task_key}:{chunk_key}: {exc}")


def _load_gmaps_task_chunks(session_id: str, task_key: str) -> list[dict]:
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT chunk_key, status, attempt_count, last_error, payload,
                   started_at, finished_at, updated_at
            FROM gmaps_task_chunks
            WHERE session_id=? AND task_key=?
            ORDER BY datetime(updated_at) DESC, id DESC
            """,
            (session_id, task_key),
        ).fetchall()
        db.close()

        chunks: list[dict] = []
        for row in rows:
            payload = {}
            try:
                payload = json.loads(row["payload"] or "{}")
            except Exception:
                payload = {}
            chunks.append({
                "chunk_key": row["chunk_key"],
                "status": row["status"],
                "attempt_count": int(row["attempt_count"] or 0),
                "last_error": row["last_error"] or "",
                "payload": payload,
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "updated_at": row["updated_at"],
            })
        return chunks
    except Exception:
        return []


def _task_chunk_summary(session_id: str, task_key: str) -> dict:
    chunks = _load_gmaps_task_chunks(session_id, task_key)
    total = len(chunks)
    completed = sum(1 for c in chunks if str(c.get("status") or "").lower() == "completed")
    failed = sum(1 for c in chunks if str(c.get("status") or "").lower() == "failed")
    running = sum(1 for c in chunks if str(c.get("status") or "").lower() == "running")
    return {
        "task_key": task_key,
        "total_chunks": total,
        "completed_chunks": completed,
        "failed_chunks": failed,
        "running_chunks": running,
        "checkpointed": completed > 0,
    }


def _load_completed_chunk_outputs(session_id: str, task_key: str) -> dict[str, list[dict]]:
    rows = _load_gmaps_task_chunks(session_id, task_key)
    outputs: dict[str, list[dict]] = {}
    for row in rows:
        if str(row.get("status") or "").lower() != "completed":
            continue
        payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
        output_leads = payload.get("output_leads") if isinstance(payload.get("output_leads"), list) else []
        chunk_key = str(row.get("chunk_key") or "").strip()
        if chunk_key:
            outputs[chunk_key] = output_leads
    return outputs


def _clear_task_chunks(session_id: str, task_key: str):
    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            "DELETE FROM gmaps_task_chunks WHERE session_id=? AND task_key=?",
            (session_id, task_key),
        )
        db.commit()
        db.close()
    except Exception as exc:
        log.error(f"Failed to clear task chunks {session_id}:{task_key}: {exc}")


def _build_contacts_chunks(leads: list[dict], chunk_size: int) -> list[dict]:
    safe_chunk_size = max(1, int(chunk_size or _CONTACT_TASK_CHUNK_SIZE))
    normalized: list[dict] = []
    for lead in leads:
        if not isinstance(lead, dict):
            continue
        _ensure_lead_uid(lead)
        normalized.append(dict(lead))

    chunks: list[dict] = []
    for i in range(0, len(normalized), safe_chunk_size):
        part = normalized[i:i + safe_chunk_size]
        ids = [str(p.get("lead_uid") or "") for p in part if str(p.get("lead_uid") or "")]
        seed = "|".join(ids) if ids else f"chunk:{i // safe_chunk_size}"
        chunk_key = hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]
        chunks.append({
            "chunk_index": (i // safe_chunk_size) + 1,
            "chunk_key": chunk_key,
            "leads": part,
        })
    return chunks


def _load_job_state_with_fallback(job_id: str) -> dict | None:
    state = get_job_state(job_id)
    if state:
        return state

    persisted = _load_persisted_session_state(job_id)
    if persisted:
        try:
            save_job_state(job_id, persisted)
        except Exception:
            pass
        return persisted
    return None


def _save_job_state_and_persist(job_id: str, state: dict):
    previous = get_job_state(job_id) or {}
    save_job_state(job_id, state)
    _persist_gmaps_state(state)

    if state.get("tool") != "gmaps":
        return

    prev_status = str(previous.get("status") or "")
    new_status = str(state.get("status") or "")
    if new_status and prev_status != new_status:
        _persist_gmaps_event(
            state,
            "status_changed",
            f"Status changed: {prev_status or 'N/A'} -> {new_status}",
            payload={"from": prev_status, "to": new_status},
        )

    prev_phase = str(previous.get("phase") or "")
    new_phase = str(state.get("phase") or "")
    if new_phase and prev_phase != new_phase:
        _persist_gmaps_event(
            state,
            "phase_changed",
            f"Phase changed: {prev_phase or 'N/A'} -> {new_phase}",
            payload={"from": prev_phase, "to": new_phase},
        )

    prev_bucket = int(previous.get("progress") or 0) // 10
    new_bucket = int(state.get("progress") or 0) // 10
    if new_bucket > prev_bucket:
        _persist_gmaps_event(
            state,
            "progress_checkpoint",
            f"Progress reached {min(100, new_bucket * 10)}%",
            payload={"progress": int(state.get("progress") or 0)},
        )

    prev_error = str(previous.get("error") or "").strip()
    new_error = str(state.get("error") or "").strip()
    if new_error and new_error != prev_error:
        _persist_gmaps_event(
            state,
            "error",
            f"Error captured: {new_error[:300]}",
            severity="error",
            payload={"error": new_error},
        )


def _flush_job_states_on_exit():
    """Best-effort flush of in-memory jobs to SQLite on graceful process exit."""
    try:
        for state in list_job_states():
            if not isinstance(state, dict):
                continue
            job_id = str(state.get("job_id") or "").strip()
            if not job_id:
                continue
            _save_job_state_and_persist(job_id, state)
    except Exception as exc:
        log.error(f"Failed to flush job states on exit: {exc}")


atexit.register(_flush_job_states_on_exit)


def _load_persisted_session_leads(session_id: str) -> list[dict]:
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT payload FROM gmaps_session_leads
            WHERE session_id=?
            ORDER BY updated_at DESC, id DESC
            """,
            (session_id,),
        ).fetchall()
        db.close()
        leads = []
        for row in rows:
            payload = row["payload"] or "{}"
            try:
                leads.append(json.loads(payload))
            except Exception:
                continue
        return leads
    except Exception:
        return []


def _load_persisted_session_state(session_id: str) -> dict | None:
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        row = db.execute(
            """
            SELECT session_id, user_id, keyword, place, max_leads,
                   phase, extraction_status, contacts_status, status,
                   progress, message, results_count, created_at, updated_at, finished_at
            FROM gmaps_sessions
            WHERE session_id=?
            """,
            (session_id,),
        ).fetchone()
        db.close()
        if not row:
            return None
        return {
            "job_id": row["session_id"],
            "tool": "gmaps",
            "user_id": row["user_id"],
            "keyword": row["keyword"],
            "place": row["place"],
            "max_leads": row["max_leads"],
            "phase": row["phase"],
            "extraction_status": row["extraction_status"],
            "contacts_status": row["contacts_status"],
            "status": row["status"],
            "progress": row["progress"],
            "message": row["message"],
            "results_count": row["results_count"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "finished_at": row["finished_at"],
            "results": _load_persisted_session_leads(session_id),
            "logs": [],
        }
    except Exception:
        return None


def _list_persisted_sessions(user_id: int) -> list[dict]:
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT session_id, user_id, keyword, place, max_leads,
                   phase, extraction_status, contacts_status, status,
                   progress, message, results_count, created_at, updated_at, finished_at
            FROM gmaps_sessions
            WHERE user_id=?
            ORDER BY datetime(updated_at) DESC
            LIMIT 200
            """,
            (user_id,),
        ).fetchall()
        db.close()
        sessions = []
        for row in rows:
            sessions.append({
                "job_id": row["session_id"],
                "tool": "gmaps",
                "user_id": row["user_id"],
                "keyword": row["keyword"],
                "place": row["place"],
                "max_leads": row["max_leads"],
                "phase": row["phase"],
                "extraction_status": row["extraction_status"],
                "contacts_status": row["contacts_status"],
                "status": row["status"],
                "progress": row["progress"],
                "message": row["message"],
                "results_count": row["results_count"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "finished_at": row["finished_at"],
                "logs": [],
            })
        return sessions
    except Exception:
        return []


def _completion_by_user(user_id: int) -> dict[str, dict]:
    """Return per-session completion counts for a user."""
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT session_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN is_complete=1 THEN 1 ELSE 0 END) AS complete_count,
                   SUM(CASE WHEN is_complete=0 THEN 1 ELSE 0 END) AS incomplete_count
            FROM gmaps_session_leads
            WHERE user_id=?
            GROUP BY session_id
            """,
            (user_id,),
        ).fetchall()
        db.close()

        result: dict[str, dict] = {}
        for row in rows:
            total = int(row["total"] or 0)
            complete = int(row["complete_count"] or 0)
            incomplete = int(row["incomplete_count"] or 0)
            result[row["session_id"]] = {
                "total": total,
                "complete_count": complete,
                "incomplete_count": incomplete,
                "completion_rate": int((complete / total) * 100) if total > 0 else 0,
            }
        return result
    except Exception:
        return {}


def _load_persisted_session_logs(session_id: str, limit: int = 200) -> list[dict]:
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT phase, progress, message, created_at
            FROM gmaps_session_logs
            WHERE session_id=?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (session_id, int(max(1, limit))),
        ).fetchall()
        db.close()
        logs = []
        for row in reversed(rows):
            logs.append({
                "phase": row["phase"],
                "progress": row["progress"],
                "message": row["message"],
                "at": row["created_at"],
            })
        return logs
    except Exception:
        return []


def _load_persisted_session_events(session_id: str, limit: int = 200) -> list[dict]:
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT event_type, severity, phase, status, progress, message, payload, created_at
            FROM gmaps_session_events
            WHERE session_id=?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (session_id, int(max(1, limit))),
        ).fetchall()
        db.close()

        events: list[dict] = []
        for row in reversed(rows):
            payload = {}
            try:
                payload = json.loads(row["payload"] or "{}")
            except Exception:
                payload = {}
            events.append({
                "event_type": row["event_type"],
                "severity": row["severity"],
                "phase": row["phase"],
                "status": row["status"],
                "progress": row["progress"],
                "message": row["message"],
                "payload": payload,
                "at": row["created_at"],
            })
        return events
    except Exception:
        return []


def _event_matches_audit_scope(event_type: str, scope: str) -> bool:
    et = str(event_type or "").strip().lower()
    s = str(scope or "operator").strip().lower()

    operator_events = {
        "task_operator_denied",
        "task_attempts_reset",
        "task_force_retry_requested",
    }
    recovery_events = {
        "task_retry_requested",
        "task_force_retry_requested",
        "task_retry_blocked",
        "stale_tasks_recovered",
        "auto_stale_sweep_recovered",
        "task_attempts_reset",
    }

    if s == "all":
        return True
    if s == "operator":
        return et in operator_events
    if s == "recovery":
        return et in recovery_events
    return et in operator_events


def _load_scoped_audit_events(session_id: str, scope: str = "operator", limit: int = 200) -> list[dict]:
    events = _load_persisted_session_events(session_id, limit=max(limit * 4, 200))
    filtered = [
        e for e in events
        if _event_matches_audit_scope(e.get("event_type") or "", scope)
    ]
    if limit <= 0:
        return filtered
    return filtered[-int(max(1, limit)):]


def _ops_safe_window_hours(hours: int | None) -> int:
    if hours is None:
        return _OPS_METRICS_DEFAULT_WINDOW_HOURS
    return int(max(1, min(int(hours), 168)))


def _ops_stage_metrics(user_id: int, window_hours: int) -> dict:
    since_iso = (datetime.utcnow() - timedelta(hours=_ops_safe_window_hours(window_hours))).isoformat()
    metrics: dict[str, dict] = {
        "extract": {
            "total": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "retryable": 0,
            "avg_attempts": 0.0,
            "avg_duration_seconds": 0.0,
            "failure_rate_pct": 0,
        },
        "contacts": {
            "total": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
            "retryable": 0,
            "avg_attempts": 0.0,
            "avg_duration_seconds": 0.0,
            "failure_rate_pct": 0,
        },
    }

    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT phase,
                   COUNT(*) AS total,
                   SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running,
                   SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) AS completed,
                   SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
                   SUM(CASE WHEN status='retryable' THEN 1 ELSE 0 END) AS retryable,
                   AVG(COALESCE(attempt_count, 0)) AS avg_attempts
            FROM gmaps_session_tasks
            WHERE user_id=? AND datetime(updated_at) >= datetime(?)
            GROUP BY phase
            """,
            (int(user_id), since_iso),
        ).fetchall()

        duration_rows = db.execute(
            """
            SELECT phase,
                   AVG((julianday(finished_at) - julianday(started_at)) * 86400.0) AS avg_duration_seconds
            FROM gmaps_session_tasks
            WHERE user_id=?
              AND datetime(updated_at) >= datetime(?)
              AND started_at IS NOT NULL
              AND finished_at IS NOT NULL
              AND status IN ('completed', 'failed')
            GROUP BY phase
            """,
            (int(user_id), since_iso),
        ).fetchall()
        db.close()

        for row in rows:
            phase = str(row["phase"] or "").strip().lower()
            if phase not in metrics:
                continue
            total = int(row["total"] or 0)
            failed = int(row["failed"] or 0)
            metrics[phase] = {
                "total": total,
                "running": int(row["running"] or 0),
                "completed": int(row["completed"] or 0),
                "failed": failed,
                "retryable": int(row["retryable"] or 0),
                "avg_attempts": round(float(row["avg_attempts"] or 0.0), 2),
                "avg_duration_seconds": 0.0,
                "failure_rate_pct": int(round((failed / total) * 100)) if total > 0 else 0,
            }

        for row in duration_rows:
            phase = str(row["phase"] or "").strip().lower()
            if phase in metrics:
                metrics[phase]["avg_duration_seconds"] = round(float(row["avg_duration_seconds"] or 0.0), 2)

        return metrics
    except Exception:
        return metrics


def _ops_recent_failures(user_id: int, window_hours: int, limit: int = 15) -> list[dict]:
    since_iso = (datetime.utcnow() - timedelta(hours=_ops_safe_window_hours(window_hours))).isoformat()
    safe_limit = int(max(1, min(limit, 100)))
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT session_id, event_type, severity, phase, status, progress, message, created_at
            FROM gmaps_session_events
            WHERE user_id=?
              AND datetime(created_at) >= datetime(?)
              AND severity IN ('error', 'warning')
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
            """,
            (int(user_id), since_iso, safe_limit),
        ).fetchall()
        db.close()
        return [
            {
                "session_id": row["session_id"],
                "event_type": row["event_type"],
                "severity": row["severity"],
                "phase": row["phase"],
                "status": row["status"],
                "progress": row["progress"],
                "message": row["message"],
                "at": row["created_at"],
            }
            for row in rows
        ]
    except Exception:
        return []


def _ops_stuck_sessions(user_id: int, stale_seconds: int = 600, limit: int = 20) -> list[dict]:
    stale_seconds = int(max(60, stale_seconds))
    safe_limit = int(max(1, min(limit, 200)))
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT session_id, keyword, place, phase, extraction_status, contacts_status,
                   status, progress, updated_at
            FROM gmaps_sessions
            WHERE user_id=?
              AND status IN ('RUNNING', 'PENDING')
              AND ((julianday('now') - julianday(updated_at)) * 86400.0) > ?
            ORDER BY datetime(updated_at) ASC
            LIMIT ?
            """,
            (int(user_id), float(stale_seconds), safe_limit),
        ).fetchall()
        db.close()
        return [
            {
                "job_id": row["session_id"],
                "keyword": row["keyword"],
                "place": row["place"],
                "phase": row["phase"],
                "extraction_status": row["extraction_status"],
                "contacts_status": row["contacts_status"],
                "status": row["status"],
                "progress": int(row["progress"] or 0),
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
    except Exception:
        return []


def _ops_alerts(user_id: int, window_hours: int) -> list[dict]:
    checked_at = datetime.utcnow().isoformat()
    alerts: list[dict] = []

    running_tasks = 0
    stuck_tasks = 0
    failure_count = 0
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        row = db.execute(
            """
            SELECT
              SUM(CASE WHEN status='running' THEN 1 ELSE 0 END) AS running_count,
              SUM(CASE
                    WHEN status='running' AND ((julianday('now') - julianday(updated_at)) * 86400.0) > ?
                    THEN 1 ELSE 0 END) AS stuck_count
            FROM gmaps_session_tasks
            WHERE user_id=?
            """,
            (float(_AUTO_SWEEP_STALE_SECONDS), int(user_id)),
        ).fetchone()
        running_tasks = int((row["running_count"] if row else 0) or 0)
        stuck_tasks = int((row["stuck_count"] if row else 0) or 0)

        since_iso = (datetime.utcnow() - timedelta(hours=_ops_safe_window_hours(window_hours))).isoformat()
        row = db.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM gmaps_session_events
            WHERE user_id=?
              AND severity='error'
              AND datetime(created_at) >= datetime(?)
            """,
            (int(user_id), since_iso),
        ).fetchone()
        failure_count = int((row["cnt"] if row else 0) or 0)
        db.close()
    except Exception:
        pass

    if stuck_tasks > 0:
        alerts.append(
            {
                "code": "stuck_tasks",
                "severity": "critical" if stuck_tasks >= 3 else "warning",
                "message": f"Detected {stuck_tasks} stale running task(s)",
                "metric": "stuck_tasks",
                "value": stuck_tasks,
                "threshold": 1,
                "recommended_action": "Run auto-recovery for impacted sessions.",
                "at": checked_at,
            }
        )

    pools = worker_pool_stats()
    for pool_name, pool in pools.items():
        pending = int(pool.get("pending") or 0)
        max_pending = max(1, int(pool.get("max_pending") or 1))
        ratio = pending / max_pending
        if ratio >= _OPS_ALERT_QUEUE_CRIT_PCT:
            sev = "critical"
        elif ratio >= _OPS_ALERT_QUEUE_WARN_PCT:
            sev = "warning"
        else:
            sev = ""
        if sev:
            alerts.append(
                {
                    "code": f"queue_{pool_name}",
                    "severity": sev,
                    "message": f"{pool_name.title()} queue saturation at {int(round(ratio * 100))}%",
                    "metric": f"{pool_name}.queue_saturation_pct",
                    "value": int(round(ratio * 100)),
                    "threshold": int(round((_OPS_ALERT_QUEUE_CRIT_PCT if sev == 'critical' else _OPS_ALERT_QUEUE_WARN_PCT) * 100)),
                    "recommended_action": "Increase workers or drain queued sessions.",
                    "at": checked_at,
                }
            )

    if failure_count >= _OPS_ALERT_FAILURE_CRIT:
        severity = "critical"
    elif failure_count >= _OPS_ALERT_FAILURE_WARN:
        severity = "warning"
    else:
        severity = ""
    if severity:
        alerts.append(
            {
                "code": "failure_rate",
                "severity": severity,
                "message": f"{failure_count} error-level events in last {_ops_safe_window_hours(window_hours)}h",
                "metric": "error_events",
                "value": failure_count,
                "threshold": _OPS_ALERT_FAILURE_CRIT if severity == "critical" else _OPS_ALERT_FAILURE_WARN,
                "recommended_action": "Inspect diagnostics and retry blocked tasks.",
                "at": checked_at,
            }
        )

    severity_order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda a: (severity_order.get(str(a.get("severity") or "info"), 9), str(a.get("code") or "")))
    return alerts


def _ops_health_snapshot(user_id: int, window_hours: int) -> dict:
    alerts = _ops_alerts(user_id, window_hours)
    has_critical = any(str(a.get("severity") or "").lower() == "critical" for a in alerts)
    has_warning = any(str(a.get("severity") or "").lower() == "warning" for a in alerts)

    if has_critical:
        status = "unhealthy"
    elif has_warning:
        status = "degraded"
    else:
        status = "healthy"

    stuck_sessions = _ops_stuck_sessions(user_id, stale_seconds=_AUTO_SWEEP_STALE_SECONDS, limit=10)
    return {
        "status": status,
        "checked_at": datetime.utcnow().isoformat(),
        "window_hours": _ops_safe_window_hours(window_hours),
        "alerts": alerts,
        "alerts_count": len(alerts),
        "worker_pools": worker_pool_stats(),
        "stuck_sessions": stuck_sessions,
        "stuck_sessions_count": len(stuck_sessions),
    }


def _session_diagnostics(job_id: str, state: dict) -> dict:
    tasks = _load_persisted_session_tasks(job_id, limit=200)
    task_health = _task_health(tasks, stale_seconds=_AUTO_SWEEP_STALE_SECONDS)
    checkpoints = _load_persisted_session_events(job_id, limit=250)
    recent_issues = [
        event for event in checkpoints
        if str(event.get("severity") or "").lower() in {"error", "warning"}
    ][-50:]
    return {
        "job_id": job_id,
        "phase": state.get("phase"),
        "status": state.get("status"),
        "extraction_status": state.get("extraction_status"),
        "contacts_status": state.get("contacts_status"),
        "resume_anchor": _select_resume_anchor(job_id, state),
        "task_health": task_health,
        "tasks": tasks,
        "task_chunk_summary": _task_chunk_summary(job_id, "contacts_main"),
        "recent_issues": recent_issues,
    }


def _load_persisted_session_tasks(session_id: str, limit: int = 200) -> list[dict]:
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT task_key, phase, status, attempt_count, last_error, payload,
                     max_attempts, retry_backoff_seconds, retry_cooldown_until,
                     last_retry_reason, last_retry_at,
                     started_at, finished_at, updated_at
            FROM gmaps_session_tasks
            WHERE session_id=?
            ORDER BY datetime(updated_at) DESC, id DESC
            LIMIT ?
            """,
            (session_id, int(max(1, limit))),
        ).fetchall()
        db.close()

        tasks: list[dict] = []
        for row in rows:
            payload = {}
            try:
                payload = json.loads(row["payload"] or "{}")
            except Exception:
                payload = {}
            tasks.append({
                "task_key": row["task_key"],
                "phase": row["phase"],
                "status": row["status"],
                "attempt_count": int(row["attempt_count"] or 0),
                "last_error": row["last_error"] or "",
                "payload": payload,
                "max_attempts": int(row["max_attempts"] or _TASK_RETRY_MAX_ATTEMPTS_DEFAULT),
                "retry_backoff_seconds": int(row["retry_backoff_seconds"] or _TASK_RETRY_BACKOFF_SECONDS_DEFAULT),
                "retry_cooldown_until": row["retry_cooldown_until"],
                "last_retry_reason": row["last_retry_reason"] or "",
                "last_retry_at": row["last_retry_at"],
                "started_at": row["started_at"],
                "finished_at": row["finished_at"],
                "updated_at": row["updated_at"],
            })
        return tasks
    except Exception:
        return []


def _task_health(tasks: list[dict], stale_seconds: int = 180) -> dict:
    now = datetime.utcnow()
    running = 0
    stuck = 0

    for task in tasks:
        status = str(task.get("status") or "").lower()
        if status != "running":
            continue
        running += 1
        updated_at = _parse_iso_datetime(str(task.get("updated_at") or ""))
        if not updated_at:
            stuck += 1
            continue
        if updated_at.tzinfo is not None:
            updated_at = updated_at.replace(tzinfo=None)
        age = (now - updated_at).total_seconds()
        if age > stale_seconds:
            stuck += 1

    return {
        "running_count": running,
        "stuck_count": stuck,
        "healthy": stuck == 0,
        "stale_threshold_seconds": stale_seconds,
    }


def _mark_stale_tasks_retryable(session_id: str, *, stale_seconds: int = 180) -> dict:
    """Mark stale running tasks as retryable and return recovery summary."""
    tasks = _load_persisted_session_tasks(session_id, limit=2000)
    if not tasks:
        return {"updated": 0, "task_keys": []}

    now = datetime.utcnow()
    stale_task_keys: list[str] = []
    for task in tasks:
        if str(task.get("status") or "").lower() != "running":
            continue
        updated_at = _parse_iso_datetime(str(task.get("updated_at") or ""))
        if not updated_at:
            stale_task_keys.append(str(task.get("task_key") or ""))
            continue
        if updated_at.tzinfo is not None:
            updated_at = updated_at.replace(tzinfo=None)
        age = (now - updated_at).total_seconds()
        if age > stale_seconds:
            stale_task_keys.append(str(task.get("task_key") or ""))

    stale_task_keys = [k for k in stale_task_keys if k]
    if not stale_task_keys:
        return {"updated": 0, "task_keys": []}

    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")
        now_iso = datetime.utcnow().isoformat()
        for task_key in stale_task_keys:
            db.execute(
                """
                UPDATE gmaps_session_tasks
                SET status='retryable',
                    last_error=CASE
                        WHEN COALESCE(last_error, '')='' THEN 'Marked retryable by stale-task recovery'
                        ELSE last_error || ' | Marked retryable by stale-task recovery'
                    END,
                    finished_at=?,
                    updated_at=?
                WHERE session_id=? AND task_key=? AND status='running'
                """,
                (now_iso, now_iso, session_id, task_key),
            )
        db.commit()
        db.close()
    except Exception as exc:
        log.error(f"Failed to recover stale tasks for {session_id}: {exc}")
        return {"updated": 0, "task_keys": []}

    return {"updated": len(stale_task_keys), "task_keys": stale_task_keys}


def _load_gmaps_task_record(session_id: str, task_key: str) -> dict | None:
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        row = db.execute(
            """
                 SELECT task_key, phase, status, attempt_count, max_attempts,
                   retry_backoff_seconds, retry_cooldown_until,
                   last_retry_reason, last_retry_at, updated_at
            FROM gmaps_session_tasks
            WHERE session_id=? AND task_key=?
            LIMIT 1
            """,
            (session_id, task_key),
        ).fetchone()
        db.close()
        if not row:
            return None
        return {
            "task_key": row["task_key"],
            "phase": row["phase"],
            "status": str(row["status"] or "pending").lower(),
            "attempt_count": int(row["attempt_count"] or 0),
            "max_attempts": int(row["max_attempts"] or _TASK_RETRY_MAX_ATTEMPTS_DEFAULT),
            "retry_backoff_seconds": int(row["retry_backoff_seconds"] or _TASK_RETRY_BACKOFF_SECONDS_DEFAULT),
            "retry_cooldown_until": row["retry_cooldown_until"],
            "last_retry_reason": row["last_retry_reason"] or "",
            "last_retry_at": row["last_retry_at"],
            "updated_at": row["updated_at"],
        }
    except Exception:
        return None


def _record_retry_blocked(session_id: str, task_key: str, reason: str):
    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")
        now_iso = datetime.utcnow().isoformat()
        db.execute(
            """
            UPDATE gmaps_session_tasks
            SET last_retry_reason=?, updated_at=?
            WHERE session_id=? AND task_key=?
            """,
            ((reason or "")[:500], now_iso, session_id, task_key),
        )
        db.commit()
        db.close()
    except Exception:
        pass


def _enforce_task_retry_guard(job_id: str, state: dict, task_key: str):
    now = datetime.utcnow()
    task = _load_gmaps_task_record(job_id, task_key)
    if not task:
        return None

    status = str(task.get("status") or "pending").lower()
    if status == "running":
        reason = f"Retry blocked for {task_key}: task is already running"
        _record_retry_blocked(job_id, task_key, reason)
        _persist_gmaps_event(
            state,
            "task_retry_blocked",
            reason,
            severity="warning",
            payload={"task_key": task_key, "reason": "already_running"},
        )
        return jsonify({"error": "Task is already running.", "task_key": task_key}), 409

    attempt_count = int(task.get("attempt_count") or 0)
    max_attempts = max(1, int(task.get("max_attempts") or _TASK_RETRY_MAX_ATTEMPTS_DEFAULT))
    if attempt_count >= max_attempts:
        reason = f"Retry blocked for {task_key}: max attempts reached ({attempt_count}/{max_attempts})"
        _record_retry_blocked(job_id, task_key, reason)
        _persist_gmaps_event(
            state,
            "task_retry_blocked",
            reason,
            severity="warning",
            payload={
                "task_key": task_key,
                "reason": "max_attempts_reached",
                "attempt_count": attempt_count,
                "max_attempts": max_attempts,
            },
        )
        return jsonify({
            "error": "Task retry attempts exhausted.",
            "task_key": task_key,
            "attempt_count": attempt_count,
            "max_attempts": max_attempts,
        }), 409

    cooldown_until_raw = str(task.get("retry_cooldown_until") or "").strip()
    cooldown_until = _parse_iso_datetime(cooldown_until_raw)
    if cooldown_until:
        if cooldown_until.tzinfo is not None:
            cooldown_until = cooldown_until.replace(tzinfo=None)
        if cooldown_until > now:
            wait_seconds = int((cooldown_until - now).total_seconds())
            reason = f"Retry blocked for {task_key}: cooldown active ({wait_seconds}s remaining)"
            _record_retry_blocked(job_id, task_key, reason)
            _persist_gmaps_event(
                state,
                "task_retry_blocked",
                reason,
                severity="warning",
                payload={
                    "task_key": task_key,
                    "reason": "cooldown_active",
                    "retry_after_seconds": wait_seconds,
                },
            )
            return jsonify({
                "error": "Retry cooldown is active.",
                "task_key": task_key,
                "retry_after_seconds": wait_seconds,
                "retry_cooldown_until": cooldown_until_raw,
            }), 429

    return None


def _operator_allowed_for_user(user_row) -> bool:
    if not user_row:
        return False
    if _OPERATOR_OVERRIDE_ALL:
        return True
    email = str(user_row["email"] or "").strip().lower()
    return bool(email and email in _OPERATOR_EMAIL_ALLOWLIST)


def _reset_task_attempts(job_id: str, state: dict, task_key: str, *, reason: str, actor_email: str):
    task = _load_gmaps_task_record(job_id, task_key)
    if not task:
        return jsonify({"error": "Task not found.", "task_key": task_key}), 404

    if str(task.get("status") or "").lower() == "running":
        return jsonify({"error": "Cannot reset attempts while task is running.", "task_key": task_key}), 409

    now_iso = datetime.utcnow().isoformat()
    safe_reason = (reason or "operator_reset_attempts")[:300]

    try:
        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute(
            """
            UPDATE gmaps_session_tasks
            SET attempt_count=0,
                retry_cooldown_until=NULL,
                last_retry_reason=?,
                last_retry_at=?,
                status=CASE
                    WHEN status IN ('failed', 'canceled', 'retryable') THEN 'retryable'
                    ELSE status
                END,
                updated_at=?
            WHERE session_id=? AND task_key=?
            """,
            (f"operator_reset:{safe_reason}", now_iso, now_iso, job_id, task_key),
        )
        db.commit()
        db.close()
    except Exception as exc:
        return jsonify({"error": f"Failed to reset attempts: {exc}"}), 500

    _persist_gmaps_event(
        state,
        "task_attempts_reset",
        f"Operator reset attempts for {task_key}",
        payload={
            "task_key": task_key,
            "reason": safe_reason,
            "actor": actor_email,
        },
    )

    return jsonify({
        "message": "Task attempts reset.",
        "job_id": job_id,
        "task_key": task_key,
        "action": "reset_attempts",
    })


def _run_auto_stale_task_sweeper() -> dict:
    """Automatically recover stale running tasks on a throttled interval."""
    global _last_auto_sweep_at

    if not _AUTO_SWEEP_ENABLED:
        return {"swept_sessions": 0, "recovered_tasks": 0}

    now_ts = time.time()
    if (now_ts - _last_auto_sweep_at) < _AUTO_SWEEP_INTERVAL_SECONDS:
        return {"swept_sessions": 0, "recovered_tasks": 0}

    if not _auto_sweep_lock.acquire(blocking=False):
        return {"swept_sessions": 0, "recovered_tasks": 0}

    swept_sessions = 0
    recovered_tasks = 0
    try:
        now_ts = time.time()
        if (now_ts - _last_auto_sweep_at) < _AUTO_SWEEP_INTERVAL_SECONDS:
            return {"swept_sessions": 0, "recovered_tasks": 0}

        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            """
            SELECT DISTINCT session_id
            FROM gmaps_session_tasks
            WHERE status='running'
            """
        ).fetchall()
        db.close()

        session_ids = [str(r["session_id"] or "").strip() for r in rows if str(r["session_id"] or "").strip()]
        for session_id in session_ids:
            recovered = _mark_stale_tasks_retryable(session_id, stale_seconds=_AUTO_SWEEP_STALE_SECONDS)
            updated = int(recovered.get("updated") or 0)
            if updated <= 0:
                continue

            swept_sessions += 1
            recovered_tasks += updated
            task_keys = recovered.get("task_keys") or []

            state = get_job_state(session_id) or _load_persisted_session_state(session_id)
            if state:
                if "extract_main" in task_keys:
                    state["phase"] = "extract"
                    state["extraction_status"] = "retryable"
                if "contacts_main" in task_keys:
                    state["phase"] = state.get("phase") or "contacts"
                    state["contacts_status"] = "retryable"
                state["status"] = "PARTIAL"
                state["updated_at"] = datetime.utcnow().isoformat()
                state["message"] = f"Auto-recovered {updated} stale task(s) to retryable state."
                _append_job_log(state, state["message"], state.get("progress", 0))
                _save_job_state_and_persist(session_id, state)

                _persist_gmaps_event(
                    state,
                    "auto_stale_sweep_recovered",
                    state["message"],
                    payload={"updated": updated, "task_keys": task_keys},
                )

        _last_auto_sweep_at = time.time()
        return {"swept_sessions": swept_sessions, "recovered_tasks": recovered_tasks}
    except Exception as exc:
        log.error(f"Automatic stale-task sweep failed: {exc}")
        return {"swept_sessions": 0, "recovered_tasks": 0}
    finally:
        _auto_sweep_lock.release()


def _run_auto_retention_cleanup() -> dict:
    """Delete old events/logs/tasks on a throttled schedule."""
    global _last_retention_at, _last_retention_summary

    if not _RETENTION_ENABLED:
        return {"events_deleted": 0, "logs_deleted": 0, "tasks_deleted": 0}

    now_ts = time.time()
    if (now_ts - _last_retention_at) < _RETENTION_INTERVAL_SECONDS:
        return {"events_deleted": 0, "logs_deleted": 0, "tasks_deleted": 0}

    if not _retention_lock.acquire(blocking=False):
        return {"events_deleted": 0, "logs_deleted": 0, "tasks_deleted": 0}

    try:
        now_ts = time.time()
        if (now_ts - _last_retention_at) < _RETENTION_INTERVAL_SECONDS:
            return {"events_deleted": 0, "logs_deleted": 0, "tasks_deleted": 0}

        db = sqlite3.connect(DB_PATH)
        db.execute("PRAGMA journal_mode=WAL")

        cur = db.execute(
            """
            DELETE FROM gmaps_session_events
            WHERE julianday(created_at) < julianday('now') - ?
            """,
            (float(_RETENTION_EVENTS_DAYS),),
        )
        events_deleted = int(cur.rowcount or 0)

        cur = db.execute(
            """
            DELETE FROM gmaps_session_logs
            WHERE julianday(created_at) < julianday('now') - ?
            """,
            (float(_RETENTION_LOGS_DAYS),),
        )
        logs_deleted = int(cur.rowcount or 0)

        cur = db.execute(
            """
            DELETE FROM gmaps_session_tasks
            WHERE status IN ('completed', 'failed', 'canceled', 'retryable')
              AND julianday(updated_at) < julianday('now') - ?
            """,
            (float(_RETENTION_TASKS_DAYS),),
        )
        tasks_deleted = int(cur.rowcount or 0)

        db.commit()
        db.close()

        _last_retention_at = time.time()
        _last_retention_summary = {
            "last_run_at": datetime.utcnow().isoformat(),
            "events_deleted": events_deleted,
            "logs_deleted": logs_deleted,
            "tasks_deleted": tasks_deleted,
            "error": None,
        }
        if events_deleted or logs_deleted or tasks_deleted:
            log.info(
                "Retention cleanup: events=%s logs=%s tasks=%s",
                events_deleted,
                logs_deleted,
                tasks_deleted,
            )

        return {
            "events_deleted": events_deleted,
            "logs_deleted": logs_deleted,
            "tasks_deleted": tasks_deleted,
        }
    except Exception as exc:
        log.error(f"Retention cleanup failed: {exc}")
        _last_retention_summary = {
            "last_run_at": datetime.utcnow().isoformat(),
            "events_deleted": 0,
            "logs_deleted": 0,
            "tasks_deleted": 0,
            "error": str(exc),
        }
        return {"events_deleted": 0, "logs_deleted": 0, "tasks_deleted": 0}
    finally:
        _retention_lock.release()


def _load_archive_rows_for_user(user_id: int, table_name: str, older_than_days: int, limit: int) -> list[dict]:
    table_key = str(table_name or "").strip().lower()
    days = max(1, int(older_than_days))
    safe_limit = int(max(1, min(limit, 20000)))

    if table_key == "events":
        query = """
            SELECT session_id, user_id, created_at, event_type, severity,
                   phase, status, progress, message, payload
            FROM gmaps_session_events
            WHERE user_id=? AND julianday(created_at) < julianday('now') - ?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
        """
    elif table_key == "logs":
        query = """
            SELECT session_id, user_id, created_at, phase, progress, message
            FROM gmaps_session_logs
            WHERE user_id=? AND julianday(created_at) < julianday('now') - ?
            ORDER BY datetime(created_at) DESC, id DESC
            LIMIT ?
        """
    elif table_key == "tasks":
        query = """
            SELECT session_id, user_id, updated_at, task_key, phase, status,
                   attempt_count, max_attempts, retry_backoff_seconds,
                   retry_cooldown_until, last_retry_reason, last_retry_at,
                   last_error, payload
            FROM gmaps_session_tasks
            WHERE user_id=? AND julianday(updated_at) < julianday('now') - ?
            ORDER BY datetime(updated_at) DESC, id DESC
            LIMIT ?
        """
    else:
        return []

    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    rows = db.execute(query, (int(user_id), float(days), safe_limit)).fetchall()
    db.close()
    return [dict(r) for r in rows]


def _select_resume_anchor(session_id: str, state: dict) -> dict:
    """Select the latest safe checkpoint to anchor resume actions."""
    checkpoints = _load_persisted_session_events(session_id, limit=300)
    if not checkpoints:
        return {
            "session_id": session_id,
            "event_type": "none",
            "at": None,
            "suggested_action": "restart_extract",
        }

    safe_types = {
        "extract_started",
        "extract_completed",
        "extract_partial",
        "contacts_started",
        "contacts_paused",
        "contacts_completed",
    }
    latest = None
    for event in reversed(checkpoints):
        if event.get("event_type") in safe_types and event.get("severity") != "error":
            latest = event
            break

    if not latest:
        return {
            "session_id": session_id,
            "event_type": "none",
            "at": None,
            "suggested_action": "restart_extract",
        }

    event_type = str(latest.get("event_type") or "none")
    if event_type in ("extract_started",):
        action = "resume_or_restart_extract"
        task_key = "extract_main"
    elif event_type in ("extract_completed", "extract_partial"):
        action = "start_or_resume_contacts"
        task_key = "contacts_main"
    elif event_type in ("contacts_started", "contacts_paused"):
        action = "resume_contacts"
        task_key = "contacts_main"
    elif event_type == "contacts_completed":
        action = "completed_no_resume_needed"
        task_key = None
    else:
        action = "restart_extract"
        task_key = "extract_main"

    return {
        "session_id": session_id,
        "event_type": event_type,
        "phase": latest.get("phase"),
        "status": latest.get("status"),
        "progress": latest.get("progress"),
        "message": latest.get("message"),
        "at": latest.get("at"),
        "suggested_action": action,
        "suggested_task_key": task_key,
    }


def _retry_extract_task(job_id: str, state: dict, *, force: bool = False, force_reason: str | None = None):
    if not force:
        blocked = _enforce_task_retry_guard(job_id, state, "extract_main")
    else:
        blocked = None
    if blocked:
        return blocked

    payload = state.get("payload") if isinstance(state.get("payload"), dict) else None
    if not payload:
        return jsonify({"error": "No saved payload found for this session."}), 400

    payload = dict(payload)
    payload["crawl_contacts"] = False

    state.update({
        "status": "PENDING",
        "phase": "extract",
        "extraction_status": "pending",
        "contacts_status": "pending",
        "progress": 0,
        "results": [],
        "results_count": 0,
        "lead_count": 0,
        "stop_requested": False,
        "contact_paused": False,
        "contact_stop_requested": False,
        "message": "Retrying extraction task...",
        "updated_at": datetime.utcnow().isoformat(),
        "payload": payload,
    })
    _append_job_log(state, "Extraction task retry requested", 0)
    _save_job_state_and_persist(job_id, state)
    _upsert_gmaps_task(
        session_id=job_id,
        user_id=int(state.get("user_id") or 0),
        task_key="extract_main",
        phase="extract",
        status="running",
        payload={
            "trigger": "force_retry_task" if force else "retry_task",
            "force": bool(force),
            "force_reason": (force_reason or "")[:300],
        },
        retry_reason=(f"force_override:{(force_reason or 'operator_override')[:120]}" if force else "deterministic_retry"),
    )
    _persist_gmaps_event(
        state,
        "task_force_retry_requested" if force else "task_retry_requested",
        (
            f"Forced retry requested for extract_main ({(force_reason or 'operator_override')[:120]})"
            if force
            else "Deterministic retry requested for extract_main"
        ),
        payload={
            "task_key": "extract_main",
            "force": bool(force),
            "force_reason": (force_reason or "")[:300],
        },
    )

    t = threading.Thread(target=_run_scrape_in_thread, args=(job_id, payload), daemon=True)
    t.start()
    return jsonify({
        "message": "Forced extraction retry started." if force else "Extraction retry started.",
        "job_id": job_id,
        "task_key": "extract_main",
        "force": bool(force),
    })


def _retry_contacts_task(job_id: str, state: dict, *, force: bool = False, force_reason: str | None = None):
    if not force:
        blocked = _enforce_task_retry_guard(job_id, state, "contacts_main")
    else:
        blocked = None
    if blocked:
        return blocked

    if str(state.get("contacts_status") or "").lower() == "running":
        return jsonify({"message": "Contact retrieval already running.", "job_id": job_id, "task_key": "contacts_main"})

    leads = state.get("results") if isinstance(state.get("results"), list) else []
    if not leads:
        leads = _load_persisted_session_leads(job_id)
        if leads:
            state["results"] = leads
            state["results_count"] = len(leads)
            state["lead_count"] = len(leads)

    if not leads:
        return jsonify({"error": "No extracted leads found for contacts retry."}), 400

    state.update({
        "contact_paused": False,
        "contact_stop_requested": False,
        "phase": "contacts",
        "extraction_status": "completed",
        "contacts_status": "running",
        "status": "RUNNING",
        "message": "Retrying contact retrieval task...",
        "updated_at": datetime.utcnow().isoformat(),
    })
    _append_job_log(state, "Contact retrieval task retry requested", state.get("progress", 0))
    _save_job_state_and_persist(job_id, state)
    _upsert_gmaps_task(
        session_id=job_id,
        user_id=int(state.get("user_id") or 0),
        task_key="contacts_main",
        phase="contacts",
        status="running",
        payload={
            "trigger": "force_retry_task" if force else "retry_task",
            "force": bool(force),
            "force_reason": (force_reason or "")[:300],
        },
        retry_reason=(f"force_override:{(force_reason or 'operator_override')[:120]}" if force else "deterministic_retry"),
    )
    _persist_gmaps_event(
        state,
        "task_force_retry_requested" if force else "task_retry_requested",
        (
            f"Forced retry requested for contacts_main ({(force_reason or 'operator_override')[:120]})"
            if force
            else "Deterministic retry requested for contacts_main"
        ),
        payload={
            "task_key": "contacts_main",
            "force": bool(force),
            "force_reason": (force_reason or "")[:300],
        },
    )

    t = threading.Thread(target=_run_contact_retrieval_thread, args=(job_id,), daemon=True)
    t.start()
    return jsonify({
        "message": "Forced contacts retry started." if force else "Contacts retry started.",
        "job_id": job_id,
        "task_key": "contacts_main",
        "force": bool(force),
    })


def _load_session_leads_filtered(session_id: str, completion: str = "all", limit: int = 500) -> list[dict]:
    completion = (completion or "all").strip().lower()
    where_extra = ""
    params: list = [session_id]
    if completion == "complete":
        where_extra = " AND is_complete=1"
    elif completion == "incomplete":
        where_extra = " AND is_complete=0"

    params.append(int(max(1, min(limit, 2000))))
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        rows = db.execute(
            f"""
            SELECT payload
            FROM gmaps_session_leads
            WHERE session_id=?{where_extra}
            ORDER BY datetime(updated_at) DESC, id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        db.close()
        leads = []
        for row in rows:
            try:
                leads.append(json.loads(row["payload"] or "{}"))
            except Exception:
                continue
        return leads
    except Exception:
        return []


def _parse_iso_datetime(value: str) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _run_scrape_in_thread(job_id: str, payload: dict):
    """Background thread that runs the Google Maps scraper and updates job state."""
    def progress_callback(message: str, percent: int, snapshot: dict | None = None):
        snapshot = snapshot or {}
        area_stats = snapshot.get("area_stats") if isinstance(snapshot.get("area_stats"), dict) else {}
        results_count = int(
            snapshot.get("results_count") or snapshot.get("lead_count") or 0
        )
        update = {
            "status": "RUNNING",
            "extraction_status": "running",
            "progress": max(0, min(100, percent)),
            "message": message,
            "results_count": results_count,
            "area_stats": area_stats,
            "updated_at": datetime.utcnow().isoformat(),
        }
        if isinstance(snapshot.get("results"), list):
            update["results"] = snapshot["results"]
            update["results_count"] = len(update["results"])
        current = _load_job_state_with_fallback(job_id) or {}
        current.update(update)
        _append_job_log(current, message, percent)
        _save_job_state_and_persist(job_id, current)

        # SSE: push geocell/progress events
        _sse_publish(job_id, "geocell_progress", {
            "percent": max(0, min(100, percent)),
            "message": message,
            "results_count": results_count,
            "area_stats": area_stats,
        })

    def on_lead_found(lead_dict: dict, index: int):
        """SSE hook: push each lead to the browser the instant it's extracted."""
        _sse_publish(job_id, "lead_found", {
            "lead": lead_dict,
            "index": index,
        })

    def should_stop() -> bool:
        return is_job_stop_requested(job_id)

    try:
        state = _load_job_state_with_fallback(job_id) or {}
        state.update({
            "status": "RUNNING",
            "phase": "extract",
            "extraction_status": "running",
            "contacts_status": "pending",
            "message": "Worker started",
            "updated_at": datetime.utcnow().isoformat(),
        })
        _append_job_log(state, "Extraction started", 0)
        _save_job_state_and_persist(job_id, state)
        _upsert_gmaps_task(
            session_id=job_id,
            user_id=int(state.get("user_id") or 0),
            task_key="extract_main",
            phase="extract",
            status="running",
            payload={"keyword": state.get("keyword"), "place": state.get("place")},
        )
        _persist_gmaps_event(
            state,
            "extract_started",
            "Extraction phase started",
            payload={"job_id": job_id},
        )

        # SSE: notify job started
        _sse_publish(job_id, "job_started", {
            "job_id": job_id,
            "keyword": state.get("keyword"),
            "place": state.get("place"),
        })

        result = run_scraper_job(
            payload=payload,
            progress_callback=progress_callback,
            should_stop=should_stop,
            on_lead_found=on_lead_found,
        )

        final_status = "PARTIAL" if result.get("status") == "PARTIAL" else "COMPLETED"

        # Use completed results, but fall back to whatever partial data was saved
        final_leads = result.get("leads", [])
        existing_state = _load_job_state_with_fallback(job_id) or {}
        existing_leads = existing_state.get("results", [])

        # Keep whichever set has more data
        if len(existing_leads) > len(final_leads):
            final_leads = existing_leads

        final_state = existing_state
        final_state.update({
            "status": final_status,
            "progress": 100,
            "phase": "extract",
            "extraction_status": "completed" if final_status == "COMPLETED" else "partial",
            "contacts_status": "pending",
            "message": (
                f"Stopped with {len(final_leads)} leads."
                if final_status == "PARTIAL"
                else f"List extraction complete. Found {len(final_leads)} leads."
            ),
            "results_count": len(final_leads),
            "results": final_leads,
            "area_stats": result.get("area_stats", {}),
            "finished_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        })
        _append_job_log(final_state, final_state["message"], 100)
        _save_job_state_and_persist(job_id, final_state)
        if final_status != "COMPLETED":
            _persist_partial_snapshot_checkpoint(final_state, reason="extract_partial")
        _upsert_gmaps_task(
            session_id=job_id,
            user_id=int(final_state.get("user_id") or 0),
            task_key="extract_main",
            phase="extract",
            status="completed" if final_status == "COMPLETED" else "failed",
            payload={"results_count": len(final_leads), "final_status": final_status},
            error=None if final_status == "COMPLETED" else final_state.get("message"),
        )
        _persist_gmaps_event(
            final_state,
            "extract_completed" if final_status == "COMPLETED" else "extract_partial",
            final_state["message"],
            payload={"results_count": len(final_leads)},
        )

        # SSE: notify extraction complete
        _sse_publish(job_id, "job_completed", {
            "status": final_status,
            "total_leads": len(final_leads),
            "phase": "extract",
            "message": final_state["message"],
        })

    except Exception as exc:
        log.error(f"Scrape job {job_id} failed: {exc}")
        # On failure, preserve whatever partial results we have
        failed_state = _load_job_state_with_fallback(job_id) or {}
        existing_leads = failed_state.get("results", [])
        failed_state.update({
            "status": "PARTIAL" if existing_leads else "FAILED",
            "progress": 100,
            "phase": "extract",
            "extraction_status": "partial" if existing_leads else "failed",
            "contacts_status": "pending",
            "message": (
                f"Error occurred but saved {len(existing_leads)} leads."
                if existing_leads
                else f"Error: {str(exc)}"
            ),
            "error": str(exc),
            "finished_at": datetime.utcnow().isoformat(),
            "updated_at": datetime.utcnow().isoformat(),
        })
        _append_job_log(failed_state, failed_state["message"], 100)
        _save_job_state_and_persist(job_id, failed_state)
        _persist_partial_snapshot_checkpoint(
            failed_state,
            reason=("extract_failed_with_partial" if existing_leads else "extract_failed_no_partial"),
        )
        _upsert_gmaps_task(
            session_id=job_id,
            user_id=int(failed_state.get("user_id") or 0),
            task_key="extract_main",
            phase="extract",
            status="failed",
            payload={"saved_results": len(existing_leads)},
            error=str(exc),
        )
        _persist_gmaps_event(
            failed_state,
            "extract_failed",
            failed_state["message"],
            severity="error",
            payload={"error": str(exc), "saved_results": len(existing_leads)},
        )


def _run_contact_retrieval_thread(job_id: str):
    """Background thread that enriches existing extracted leads with website contacts."""
    scraper = None
    try:
        state = _load_job_state_with_fallback(job_id) or {}
        leads = state.get("results") if isinstance(state.get("results"), list) else []
        if not leads:
            state.update({
                "contacts_status": "failed",
                "message": "No leads available for contact retrieval.",
                "updated_at": datetime.utcnow().isoformat(),
            })
            _append_job_log(state, state["message"], state.get("progress", 0))
            _save_job_state_and_persist(job_id, state)
            _upsert_gmaps_task(
                session_id=job_id,
                user_id=int(state.get("user_id") or 0),
                task_key="contacts_main",
                phase="contacts",
                status="failed",
                payload={"reason": "no_leads"},
                error="No leads available for contact retrieval",
            )
            _persist_gmaps_event(
                state,
                "contacts_failed",
                state["message"],
                severity="error",
                payload={"reason": "no_leads"},
            )
            return

        state.update({
            "status": "RUNNING",
            "phase": "contacts",
            "extraction_status": "completed",
            "contacts_status": "running",
            "contact_stop_requested": False,
            "message": f"Starting contact retrieval for {len(leads)} leads...",
            "updated_at": datetime.utcnow().isoformat(),
        })
        _append_job_log(state, state["message"], 0)
        _save_job_state_and_persist(job_id, state)
        _upsert_gmaps_task(
            session_id=job_id,
            user_id=int(state.get("user_id") or 0),
            task_key="contacts_main",
            phase="contacts",
            status="running",
            payload={"lead_count": len(leads)},
        )
        _persist_gmaps_event(
            state,
            "contacts_started",
            state["message"],
            payload={"lead_count": len(leads)},
        )

        scraper = GoogleMapsScraper(headless=True)

        def _should_stop() -> bool:
            current = _load_job_state_with_fallback(job_id) or {}
            return bool(current.get("contact_stop_requested", False))

        def _should_pause() -> bool:
            current = _load_job_state_with_fallback(job_id) or {}
            return bool(current.get("contact_paused", False))

        chunk_plan = _build_contacts_chunks(leads, _CONTACT_TASK_CHUNK_SIZE)
        total_chunks = max(1, len(chunk_plan))
        completed_chunk_outputs = _load_completed_chunk_outputs(job_id, "contacts_main")

        merged_by_uid: dict[str, dict] = {}
        for outputs in completed_chunk_outputs.values():
            for item in outputs:
                if not isinstance(item, dict):
                    continue
                uid = _ensure_lead_uid(item)
                merged_by_uid[uid] = item

        failed_chunks = 0
        processed_chunks = 0

        for chunk in chunk_plan:
            if _should_stop() or _should_pause():
                break

            chunk_index = int(chunk.get("chunk_index") or 1)
            chunk_key = str(chunk.get("chunk_key") or "")
            chunk_leads = chunk.get("leads") if isinstance(chunk.get("leads"), list) else []
            if not chunk_key:
                continue

            if chunk_key in completed_chunk_outputs:
                processed_chunks += 1
                for item in completed_chunk_outputs[chunk_key]:
                    if not isinstance(item, dict):
                        continue
                    uid = _ensure_lead_uid(item)
                    merged_by_uid[uid] = item

                current = _load_job_state_with_fallback(job_id) or {}
                overall_percent = int((processed_chunks / total_chunks) * 100)
                current.update({
                    "status": "RUNNING",
                    "phase": "contacts",
                    "extraction_status": "completed",
                    "contacts_status": "running",
                    "progress": max(0, min(100, overall_percent)),
                    "message": f"Resumed checkpoint chunk {chunk_index}/{total_chunks}.",
                    "results": list(merged_by_uid.values()),
                    "results_count": len(merged_by_uid),
                    "lead_count": len(merged_by_uid),
                    "updated_at": datetime.utcnow().isoformat(),
                })
                _append_job_log(current, current["message"], overall_percent)
                _save_job_state_and_persist(job_id, current)
                continue

            _upsert_gmaps_task_chunk(
                session_id=job_id,
                user_id=int(state.get("user_id") or 0),
                task_key="contacts_main",
                chunk_key=chunk_key,
                status="running",
                payload={"chunk_index": chunk_index, "total_chunks": total_chunks, "lead_count": len(chunk_leads)},
            )
            _persist_gmaps_event(
                state,
                "contacts_chunk_started",
                f"Contacts chunk {chunk_index}/{total_chunks} started",
                payload={"chunk_key": chunk_key, "chunk_index": chunk_index, "total_chunks": total_chunks},
            )

            def _progress(message: str, percent: int):
                current = _load_job_state_with_fallback(job_id) or {}
                base = chunk_index - 1
                overall = int(((base + (max(0, min(100, percent)) / 100.0)) / total_chunks) * 100)
                current.update({
                    "status": "RUNNING",
                    "phase": "contacts",
                    "extraction_status": "completed",
                    "contacts_status": "running",
                    "progress": max(0, min(100, overall)),
                    "message": f"[Chunk {chunk_index}/{total_chunks}] {message}",
                    "results": list(merged_by_uid.values()),
                    "results_count": len(merged_by_uid),
                    "lead_count": len(merged_by_uid),
                    "updated_at": datetime.utcnow().isoformat(),
                })
                _append_job_log(current, current["message"], overall)
                _save_job_state_and_persist(job_id, current)

            def _on_contact_found(lead_index: int, enriched_lead: dict):
                """SSE hook: push per-lead contact enrichment to the browser."""
                _sse_publish(job_id, "contact_found", {
                    "lead_index": lead_index,
                    "business_name": enriched_lead.get("business_name", ""),
                    "email": enriched_lead.get("email", "N/A"),
                    "phone": enriched_lead.get("phone", "N/A"),
                    "socials": {
                        k: enriched_lead.get(k, "N/A")
                        for k in ("facebook", "instagram", "twitter", "linkedin", "youtube", "tiktok", "pinterest")
                    },
                })

            # SSE: notify crawl phase started for this chunk
            _sse_publish(job_id, "crawl_started", {
                "chunk_index": chunk_index,
                "total_chunks": total_chunks,
                "lead_count": len(chunk_leads),
            })

            try:
                enriched_chunk = scraper.crawl_contacts_for_leads(
                    leads=chunk_leads,
                    progress_callback=_progress,
                    should_stop=_should_stop,
                    should_pause=_should_pause,
                    on_contact_found=_on_contact_found,
                )
                cleaned_chunk = clean_leads(enriched_chunk)
                for item in cleaned_chunk:
                    if not isinstance(item, dict):
                        continue
                    uid = _ensure_lead_uid(item)
                    merged_by_uid[uid] = item

                _upsert_gmaps_task_chunk(
                    session_id=job_id,
                    user_id=int(state.get("user_id") or 0),
                    task_key="contacts_main",
                    chunk_key=chunk_key,
                    status="completed",
                    payload={
                        "chunk_index": chunk_index,
                        "total_chunks": total_chunks,
                        "output_leads": cleaned_chunk,
                    },
                )
                _persist_gmaps_event(
                    state,
                    "contacts_chunk_completed",
                    f"Contacts chunk {chunk_index}/{total_chunks} completed",
                    payload={
                        "chunk_key": chunk_key,
                        "chunk_index": chunk_index,
                        "total_chunks": total_chunks,
                        "output_count": len(cleaned_chunk),
                    },
                )

                processed_chunks += 1
                current = _load_job_state_with_fallback(job_id) or {}
                overall_percent = int((processed_chunks / total_chunks) * 100)
                current.update({
                    "status": "RUNNING",
                    "phase": "contacts",
                    "extraction_status": "completed",
                    "contacts_status": "running",
                    "progress": max(0, min(100, overall_percent)),
                    "message": f"Completed contacts chunk {chunk_index}/{total_chunks}.",
                    "results": list(merged_by_uid.values()),
                    "results_count": len(merged_by_uid),
                    "lead_count": len(merged_by_uid),
                    "updated_at": datetime.utcnow().isoformat(),
                })
                _append_job_log(current, current["message"], overall_percent)
                _save_job_state_and_persist(job_id, current)
            except Exception as chunk_exc:
                failed_chunks += 1
                _upsert_gmaps_task_chunk(
                    session_id=job_id,
                    user_id=int(state.get("user_id") or 0),
                    task_key="contacts_main",
                    chunk_key=chunk_key,
                    status="failed",
                    payload={"chunk_index": chunk_index, "total_chunks": total_chunks},
                    error=str(chunk_exc),
                )
                _persist_gmaps_event(
                    state,
                    "contacts_chunk_failed",
                    f"Contacts chunk {chunk_index}/{total_chunks} failed",
                    severity="error",
                    payload={
                        "chunk_key": chunk_key,
                        "chunk_index": chunk_index,
                        "total_chunks": total_chunks,
                        "error": str(chunk_exc),
                    },
                )
                continue

        cleaned = clean_leads(list(merged_by_uid.values()))

        current = _load_job_state_with_fallback(job_id) or {}
        if current.get("contact_stop_requested") or current.get("contact_paused"):
            current.update({
                "status": "PARTIAL",
                "phase": "contacts",
                "extraction_status": "completed",
                "contacts_status": "paused",
                "results": cleaned,
                "results_count": len(cleaned),
                "lead_count": len(cleaned),
                "message": f"Contact retrieval paused at {len(cleaned)} leads.",
                "updated_at": datetime.utcnow().isoformat(),
            })
        elif failed_chunks > 0:
            current.update({
                "status": "PARTIAL",
                "phase": "contacts",
                "extraction_status": "completed",
                "contacts_status": "failed",
                "results": cleaned,
                "results_count": len(cleaned),
                "lead_count": len(cleaned),
                "message": (
                    f"Contact retrieval finished with {failed_chunks} failed chunk(s). "
                    f"Saved {len(cleaned)} leads."
                ),
                "updated_at": datetime.utcnow().isoformat(),
            })
        else:
            current.update({
                "status": "COMPLETED",
                "phase": "contacts",
                "extraction_status": "completed",
                "contacts_status": "completed",
                "progress": 100,
                "results": cleaned,
                "results_count": len(cleaned),
                "lead_count": len(cleaned),
                "message": f"Contact retrieval complete for {len(cleaned)} leads.",
                "updated_at": datetime.utcnow().isoformat(),
            })
        _append_job_log(current, current["message"], current.get("progress", 100))
        _save_job_state_and_persist(job_id, current)
        if current.get("contacts_status") == "paused":
            _persist_partial_snapshot_checkpoint(current, reason="contacts_paused")
        if current.get("contacts_status") in ("paused", "failed"):
            _persist_gmaps_event(
                current,
                "contacts_checkpoint",
                "Contacts task checkpoint persisted",
                payload={
                    "completed_chunks": int(processed_chunks),
                    "total_chunks": int(total_chunks),
                    "failed_chunks": int(failed_chunks),
                    "results_count": len(cleaned),
                },
            )
        _upsert_gmaps_task(
            session_id=job_id,
            user_id=int(current.get("user_id") or 0),
            task_key="contacts_main",
            phase="contacts",
            status=(
                "completed"
                if current.get("contacts_status") == "completed"
                else ("paused" if current.get("contacts_status") == "paused" else "failed")
            ),
            payload={"results_count": len(cleaned), "contacts_status": current.get("contacts_status")},
            error=(
                None
                if current.get("contacts_status") in ("completed", "paused")
                else current.get("message")
            ),
        )
        _persist_gmaps_event(
            current,
            "contacts_paused" if current.get("contacts_status") == "paused" else "contacts_completed",
            current["message"],
            payload={"results_count": len(cleaned)},
        )

        # SSE: notify contacts phase complete
        _sse_publish(job_id, "job_completed", {
            "status": current.get("status", "COMPLETED"),
            "total_leads": len(cleaned),
            "phase": "contacts",
            "contacts_status": current.get("contacts_status", "completed"),
            "message": current["message"],
        })

    except Exception as exc:
        failed = _load_job_state_with_fallback(job_id) or {}
        partial_leads = scraper.get_partial_leads() if scraper else []
        if partial_leads:
            failed["results"] = partial_leads
            failed["results_count"] = len(partial_leads)
            failed["lead_count"] = len(partial_leads)
        failed.update({
            "status": "PARTIAL",
            "phase": "contacts",
            "extraction_status": "completed",
            "contacts_status": "failed",
            "message": f"Contact retrieval failed: {str(exc)}",
            "error": str(exc),
            "updated_at": datetime.utcnow().isoformat(),
        })
        _append_job_log(failed, failed["message"], failed.get("progress", 0))
        _save_job_state_and_persist(job_id, failed)
        _persist_partial_snapshot_checkpoint(
            failed,
            reason=("contacts_failed_with_partial" if (failed.get("results") or []) else "contacts_failed_no_partial"),
        )
        _upsert_gmaps_task(
            session_id=job_id,
            user_id=int(failed.get("user_id") or 0),
            task_key="contacts_main",
            phase="contacts",
            status="failed",
            payload={"saved_results": len(failed.get("results") or [])},
            error=str(exc),
        )
        _persist_gmaps_event(
            failed,
            "contacts_failed",
            failed["message"],
            severity="error",
            payload={"error": str(exc), "saved_results": len(failed.get("results") or [])},
        )



# ============================================================
# Database
# ============================================================

def get_db():
    """Return a per-request sqlite3 connection."""
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    """Create tables if they don't exist."""
    db = sqlite3.connect(DB_PATH)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            email       TEXT UNIQUE NOT NULL,
            password    TEXT NOT NULL,
            full_name   TEXT DEFAULT '',
            license_key TEXT DEFAULT '',
            is_active   INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now')),
            last_login  TEXT
        );
        CREATE TABLE IF NOT EXISTS license_keys (
            key         TEXT PRIMARY KEY,
            plan        TEXT DEFAULT 'pro',
            max_uses    INTEGER DEFAULT 1,
            used_count  INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now')),
            expires_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS scrape_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            job_id      TEXT NOT NULL,
            tool        TEXT NOT NULL,
            keyword     TEXT DEFAULT '',
            location    TEXT DEFAULT '',
            search_type TEXT DEFAULT '',
            status      TEXT DEFAULT 'running',
            lead_count  INTEGER DEFAULT 0,
            strong      INTEGER DEFAULT 0,
            medium      INTEGER DEFAULT 0,
            weak        INTEGER DEFAULT 0,
            started_at  TEXT DEFAULT (datetime('now')),
            finished_at TEXT,
            csv_path    TEXT DEFAULT '',
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE TABLE IF NOT EXISTS leads (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            scrape_id   INTEGER NOT NULL,
            tool        TEXT NOT NULL,
            keyword     TEXT DEFAULT '',
            location    TEXT DEFAULT '',
            title       TEXT DEFAULT '',
            email       TEXT DEFAULT '',
            phone       TEXT DEFAULT '',
            website     TEXT DEFAULT '',
            quality     TEXT DEFAULT 'weak',
            data        TEXT DEFAULT '{}',
            created_at  TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (scrape_id) REFERENCES scrape_history(id)
        );
        CREATE INDEX IF NOT EXISTS idx_leads_user ON leads(user_id);
        CREATE INDEX IF NOT EXISTS idx_leads_scrape ON leads(scrape_id);
        CREATE INDEX IF NOT EXISTS idx_leads_tool ON leads(user_id, tool);
        CREATE INDEX IF NOT EXISTS idx_leads_keyword ON leads(user_id, keyword);
        CREATE INDEX IF NOT EXISTS idx_leads_location ON leads(user_id, location);

        CREATE TABLE IF NOT EXISTS email_templates (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            lead_id     INTEGER,
            business_name TEXT DEFAULT '',
            email       TEXT DEFAULT '',
            subject     TEXT DEFAULT '',
            body        TEXT DEFAULT '',
            keyword     TEXT DEFAULT '',
            location    TEXT DEFAULT '',
            sender_info TEXT DEFAULT '{}',
            created_at  TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (user_id) REFERENCES users(id),
            FOREIGN KEY (lead_id) REFERENCES leads(id)
        );
        CREATE INDEX IF NOT EXISTS idx_email_tpl_user ON email_templates(user_id);
        CREATE INDEX IF NOT EXISTS idx_email_tpl_lead ON email_templates(lead_id);

        CREATE TABLE IF NOT EXISTS gmaps_sessions (
            session_id         TEXT PRIMARY KEY,
            user_id            INTEGER NOT NULL,
            keyword            TEXT DEFAULT '',
            place              TEXT DEFAULT '',
            max_leads          INTEGER,
            phase              TEXT DEFAULT 'extract',
            extraction_status  TEXT DEFAULT 'pending',
            contacts_status    TEXT DEFAULT 'pending',
            status             TEXT DEFAULT 'PENDING',
            progress           INTEGER DEFAULT 0,
            message            TEXT DEFAULT '',
            results_count      INTEGER DEFAULT 0,
            created_at         TEXT DEFAULT (datetime('now')),
            updated_at         TEXT DEFAULT (datetime('now')),
            finished_at        TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_gmaps_sessions_user_updated
            ON gmaps_sessions(user_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS gmaps_session_leads (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id         TEXT NOT NULL,
            user_id            INTEGER NOT NULL,
            lead_uid           TEXT NOT NULL,
            business_name      TEXT DEFAULT '',
            owner_name         TEXT DEFAULT '',
            phone              TEXT DEFAULT '',
            website            TEXT DEFAULT '',
            email              TEXT DEFAULT '',
            address            TEXT DEFAULT '',
            rating             TEXT DEFAULT '',
            reviews            TEXT DEFAULT '',
            category           TEXT DEFAULT '',
            latitude           TEXT DEFAULT '',
            longitude          TEXT DEFAULT '',
            facebook           TEXT DEFAULT '',
            instagram          TEXT DEFAULT '',
            twitter            TEXT DEFAULT '',
            linkedin           TEXT DEFAULT '',
            youtube            TEXT DEFAULT '',
            tiktok             TEXT DEFAULT '',
            pinterest          TEXT DEFAULT '',
            stage              TEXT DEFAULT 'extract',
            is_complete        INTEGER DEFAULT 0,
            payload            TEXT DEFAULT '{}',
            created_at         TEXT DEFAULT (datetime('now')),
            updated_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(session_id, lead_uid),
            FOREIGN KEY (session_id) REFERENCES gmaps_sessions(session_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_gmaps_leads_session_updated
            ON gmaps_session_leads(session_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_gmaps_leads_user_session
            ON gmaps_session_leads(user_id, session_id);

        CREATE TABLE IF NOT EXISTS gmaps_session_logs (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id         TEXT NOT NULL,
            user_id            INTEGER NOT NULL,
            phase              TEXT DEFAULT 'extract',
            progress           INTEGER,
            message            TEXT DEFAULT '',
            log_hash           TEXT NOT NULL,
            created_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(session_id, log_hash),
            FOREIGN KEY (session_id) REFERENCES gmaps_sessions(session_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_gmaps_logs_session_created
            ON gmaps_session_logs(session_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS gmaps_session_events (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id         TEXT NOT NULL,
            user_id            INTEGER NOT NULL,
            event_type         TEXT NOT NULL,
            severity           TEXT DEFAULT 'info',
            phase              TEXT DEFAULT 'extract',
            status             TEXT DEFAULT 'PENDING',
            progress           INTEGER DEFAULT 0,
            message            TEXT DEFAULT '',
            payload            TEXT DEFAULT '{}',
            event_hash         TEXT NOT NULL,
            created_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(session_id, event_hash),
            FOREIGN KEY (session_id) REFERENCES gmaps_sessions(session_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_gmaps_events_session_created
            ON gmaps_session_events(session_id, created_at DESC);

        CREATE TABLE IF NOT EXISTS gmaps_session_tasks (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id         TEXT NOT NULL,
            user_id            INTEGER NOT NULL,
            task_key           TEXT NOT NULL,
            phase              TEXT DEFAULT 'extract',
            status             TEXT DEFAULT 'pending',
            attempt_count      INTEGER DEFAULT 0,
            last_error         TEXT DEFAULT '',
            payload            TEXT DEFAULT '{}',
            max_attempts       INTEGER DEFAULT 3,
            retry_backoff_seconds INTEGER DEFAULT 45,
            retry_cooldown_until TEXT,
            last_retry_reason  TEXT DEFAULT '',
            last_retry_at      TEXT,
            started_at         TEXT,
            finished_at        TEXT,
            created_at         TEXT DEFAULT (datetime('now')),
            updated_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(session_id, task_key),
            FOREIGN KEY (session_id) REFERENCES gmaps_sessions(session_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_gmaps_tasks_session_updated
            ON gmaps_session_tasks(session_id, updated_at DESC);

        CREATE TABLE IF NOT EXISTS gmaps_task_chunks (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id         TEXT NOT NULL,
            user_id            INTEGER NOT NULL,
            task_key           TEXT NOT NULL,
            chunk_key          TEXT NOT NULL,
            status             TEXT DEFAULT 'pending',
            attempt_count      INTEGER DEFAULT 0,
            last_error         TEXT DEFAULT '',
            payload            TEXT DEFAULT '{}',
            started_at         TEXT,
            finished_at        TEXT,
            created_at         TEXT DEFAULT (datetime('now')),
            updated_at         TEXT DEFAULT (datetime('now')),
            UNIQUE(session_id, task_key, chunk_key),
            FOREIGN KEY (session_id) REFERENCES gmaps_sessions(session_id),
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
        CREATE INDEX IF NOT EXISTS idx_gmaps_task_chunks_session_task_updated
            ON gmaps_task_chunks(session_id, task_key, updated_at DESC);
    """)
    task_migrations = [
        "ALTER TABLE gmaps_session_tasks ADD COLUMN max_attempts INTEGER DEFAULT 3",
        "ALTER TABLE gmaps_session_tasks ADD COLUMN retry_backoff_seconds INTEGER DEFAULT 45",
        "ALTER TABLE gmaps_session_tasks ADD COLUMN retry_cooldown_until TEXT",
        "ALTER TABLE gmaps_session_tasks ADD COLUMN last_retry_reason TEXT DEFAULT ''",
        "ALTER TABLE gmaps_session_tasks ADD COLUMN last_retry_at TEXT",
    ]
    for stmt in task_migrations:
        try:
            db.execute(stmt)
        except sqlite3.OperationalError:
            pass

    chunk_migrations = [
        "ALTER TABLE gmaps_task_chunks ADD COLUMN last_error TEXT DEFAULT ''",
        "ALTER TABLE gmaps_task_chunks ADD COLUMN payload TEXT DEFAULT '{}'",
        "ALTER TABLE gmaps_task_chunks ADD COLUMN started_at TEXT",
        "ALTER TABLE gmaps_task_chunks ADD COLUMN finished_at TEXT",
    ]
    for stmt in chunk_migrations:
        try:
            db.execute(stmt)
        except sqlite3.OperationalError:
            pass

    db.execute(
        """
        UPDATE gmaps_session_tasks
        SET max_attempts=COALESCE(max_attempts, ?),
            retry_backoff_seconds=COALESCE(retry_backoff_seconds, ?)
        """,
        (_TASK_RETRY_MAX_ATTEMPTS_DEFAULT, _TASK_RETRY_BACKOFF_SECONDS_DEFAULT),
    )

    # Seed a demo license key if none exist
    cur = db.execute("SELECT COUNT(*) FROM license_keys")
    if cur.fetchone()[0] == 0:
        demo_key = "LEAD-PRO-2026-DEMO"
        db.execute(
            "INSERT INTO license_keys (key, plan, max_uses) VALUES (?, 'pro', 100)",
            (demo_key,),
        )
    db.commit()
    db.close()

    if pg_enabled():
        pg_ensure_schema()


init_db()

# Phase 2: Create unified jobs table and start sweeper
try:
    _ensure_jobs_table()
    _start_sweeper()
except Exception as _exc:
    log.warning(f"Phase 2 queue init (non-fatal): {_exc}")

# Phase 3: Register agents Blueprint
app.register_blueprint(_agents_bp)

# Phase 4: Intelligence routes
app.register_blueprint(_intelligence_bp)

# Phase 5: CRM / Outreach / Workflows
app.register_blueprint(_crm_bp)
app.register_blueprint(_outreach_bp)
app.register_blueprint(_workflows_bp)

# Phase A: SSE real-time streaming
app.register_blueprint(_sse_bp)

# Phase B: Extracted route modules (LinkedIn, Instagram, Webcrawler)
# Pages and Health blueprints pending full route migration from this file.
app.register_blueprint(_linkedin_bp)
app.register_blueprint(_instagram_bp)
app.register_blueprint(_webcrawler_bp)


# ============================================================
# Auth helpers
# ============================================================

# ---------- Password hashing (bcrypt) ----------

def _hash_password(password: str) -> str:
    """Hash with bcrypt (adaptive cost)."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def _verify_password(password: str, hashed: str) -> bool:
    """Verify a password against a bcrypt hash. Also handles legacy SHA-256 hashes."""
    # Legacy SHA-256 migration path
    if not hashed.startswith("$2"):
        if hashlib.sha256(password.encode()).hexdigest() == hashed:
            return True  # caller should re-hash & update DB
        return False
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))


def _upgrade_password_if_needed(user_id: int, password: str, current_hash: str):
    """Transparently upgrade legacy SHA-256 hash to bcrypt on login."""
    if not current_hash.startswith("$2"):
        new_hash = _hash_password(password)
        try:
            db = get_db()
            db.execute("UPDATE users SET password=? WHERE id=?", (new_hash, user_id))
            db.commit()
            log.info(f"Upgraded password hash for user {user_id}")
        except Exception:
            pass


# ---------- Email validation ----------
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def _is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email))


# ---------- Password strength validation ----------

def _validate_password_strength(password: str) -> str | None:
    """Return an error message if password is too weak, else None."""
    if len(password) < 8:
        return "Password must be at least 8 characters."
    if not re.search(r"[A-Z]", password):
        return "Password must contain at least one uppercase letter."
    if not re.search(r"[a-z]", password):
        return "Password must contain at least one lowercase letter."
    if not re.search(r"[0-9]", password):
        return "Password must contain at least one number."
    return None


# ---------- License key generation ----------

def _generate_license_key() -> str:
    """Generate a cryptographically random license key."""
    segment = lambda n: secrets.token_hex(n).upper()
    return f"LEAD-{segment(2)}-{segment(2)}-{segment(2)}-{segment(2)}"


def current_user():
    """Return the logged-in user row or None."""
    uid = session.get("user_id")
    if not uid:
        return None
    db = get_db()
    return db.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()


def login_required(f):
    """Decorator: redirect to /login if not logged in."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            # API routes return JSON 401; page routes redirect
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required."}), 401
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return wrapper


def subscription_required(f):
    """Decorator: require active subscription (license validated)."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        uid = session.get("user_id")
        if not uid:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required."}), 401
            return redirect(url_for("login_page"))
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
        if not user or not user["is_active"]:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Active subscription required."}), 403
            return redirect(url_for("activate_page"))
        return f(*args, **kwargs)
    return wrapper


# ============================================================
# Lead quality scoring
# ============================================================

def score_lead(lead: dict, tool: str) -> str:
    """
    Score a lead as 'strong', 'medium', or 'weak' based on
    data completeness.
    """
    points = 0
    na = {"N/A", "", None}

    if tool == "gmaps":
        fields = [
            ("email", 3), ("phone", 2), ("website", 2),
            ("business_name", 1), ("address", 1), ("rating", 1),
            ("facebook", 1), ("instagram", 1),
        ]
    elif tool == "linkedin":
        fields = [
            ("profile_url", 2), ("name", 2), ("title", 2),
            ("company", 2), ("location", 1), ("snippet", 1),
        ]
    elif tool == "instagram":
        fields = [
            ("email", 3), ("phone", 2), ("website", 2),
            ("category", 1), ("followers", 1), ("bio", 1),
            ("display_name", 1),
        ]
    elif tool == "webcrawler":
        fields = [
            ("email", 3), ("phone", 2), ("website", 2),
            ("business_name", 1), ("address", 1),
            ("facebook", 1), ("instagram", 1),
        ]
    else:
        fields = [("email", 3), ("phone", 2), ("website", 2)]

    max_points = sum(w for _, w in fields)
    for field, weight in fields:
        val = lead.get(field, "")
        if val and val not in na:
            points += weight

    ratio = points / max_points if max_points else 0
    if ratio >= 0.6:
        return "strong"
    elif ratio >= 0.3:
        return "medium"
    return "weak"


def score_leads(leads: list[dict], tool: str) -> tuple[list[dict], dict]:
    """
    Score all leads and return (scored_leads, summary).
    Each lead gets a '_quality' key.
    """
    counts = {"strong": 0, "medium": 0, "weak": 0}
    for lead in leads:
        q = score_lead(lead, tool)
        lead["_quality"] = q
        counts[q] += 1
    return leads, counts


# ============================================================
# Scrape history helpers
# ============================================================

def record_scrape_start(user_id: int, job_id: str, tool: str,
                         keyword: str, location: str, search_type: str = "") -> int:
    """Insert a scrape_history row when a job starts."""
    db = get_db()
    cur = db.execute(
        "INSERT INTO scrape_history (user_id, job_id, tool, keyword, location, search_type) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, job_id, tool, keyword, location, search_type),
    )
    db.commit()
    return cur.lastrowid


def record_scrape_end(job_id: str, status: str, lead_count: int,
                       strong: int = 0, medium: int = 0, weak: int = 0,
                       csv_path: str = ""):
    """Update a scrape_history row when a job finishes."""
    db = get_db()
    db.execute(
        "UPDATE scrape_history SET status=?, lead_count=?, strong=?, medium=?, weak=?, "
        "finished_at=datetime('now'), csv_path=? WHERE job_id=?",
        (status, lead_count, strong, medium, weak, csv_path, job_id),
    )
    db.commit()


def _record_history_on_complete(job, tool: str):
    """Record history when a job completes (called from background thread)."""
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        _, counts = score_leads(list(job.leads), tool)
        status = getattr(job, "status", "completed")
        csv_path = getattr(job, "csv_path", "") or ""
        db.execute(
            "UPDATE scrape_history SET status=?, lead_count=?, strong=?, medium=?, weak=?, "
            "finished_at=datetime('now'), csv_path=? WHERE job_id=?",
            (status, len(job.leads), counts["strong"], counts["medium"], counts["weak"],
             csv_path, job.id),
        )
        db.commit()

        # Persist individual leads to the leads table
        _persist_leads_to_db(db, job, tool)

        db.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"History record error: {e}")


def _get_lead_title(lead: dict, tool: str) -> str:
    """Extract a display title from a lead dict based on tool type."""
    if tool == "gmaps":
        return lead.get("business_name", "") or ""
    elif tool == "linkedin":
        return lead.get("name", "") or lead.get("company_name", "") or ""
    elif tool == "instagram":
        return lead.get("display_name", "") or lead.get("username", "") or ""
    elif tool == "webcrawler":
        return lead.get("business_name", "") or ""
    return ""


def _persist_leads_to_db(db, job, tool: str):
    """Insert individual lead rows into the leads table (called from bg thread)."""
    try:
        # Look up scrape_history row to get user_id and scrape_id
        row = db.execute(
            "SELECT id, user_id, keyword, location FROM scrape_history WHERE job_id=?",
            (job.id,),
        ).fetchone()
        if not row:
            return
        scrape_id = row["id"]
        user_id = row["user_id"]
        keyword = row["keyword"]
        location = row["location"]

        leads_with_quality = list(job.leads)
        if leads_with_quality and "_quality" not in leads_with_quality[0]:
            score_leads(leads_with_quality, tool)

        insert_data = []
        for lead in leads_with_quality:
            title = _get_lead_title(lead, tool)
            email = lead.get("email", "") or ""
            phone = lead.get("phone", "") or ""
            website = lead.get("website", "") or lead.get("profile_url", "") or ""
            quality = lead.get("_quality", "weak")
            # Store the full lead as JSON (exclude internal _quality key)
            lead_data = {k: v for k, v in lead.items() if not k.startswith("_")}
            insert_data.append((
                user_id, scrape_id, tool, keyword, location,
                title, email, phone, website, quality,
                json.dumps(lead_data, default=str),
            ))

        if insert_data:
            db.executemany(
                "INSERT INTO leads (user_id, scrape_id, tool, keyword, location, "
                "title, email, phone, website, quality, data) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                insert_data,
            )
            db.commit()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"Lead persist error: {e}")


def _insert_history_direct(user_id: int, job_id: str, tool: str,
                            keyword: str, location: str, search_type: str = ""):
    """Insert history row from main thread (has app context)."""
    try:
        db = get_db()
        db.execute(
            "INSERT INTO scrape_history (user_id, job_id, tool, keyword, location, search_type) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, job_id, tool, keyword, location, search_type),
        )
        db.commit()
    except Exception:
        pass


# ============================================================
# Job classes
# ============================================================

class ScrapingJob:
    """Tracks a Google Maps scraping job."""

    def __init__(self, keyword: str, place: str, map_selection: dict | None = None):
        self.id = str(uuid.uuid4())[:8]
        self.keyword = keyword
        self.place = place
        self.map_selection = map_selection or {}
        self.status = "running"
        self.progress = 0
        self.message = "Starting..."
        self.leads = []
        self.error = None
        self.csv_path = None
        self.scraper = None
        self.created_at = datetime.now().isoformat()
        self.started_at = datetime.now()  # for timer

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
        # Elapsed time
        elapsed = (datetime.now() - self.started_at).total_seconds()
        hours, rem = divmod(int(elapsed), 3600)
        minutes, secs = divmod(rem, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"

        # Area stats from scraper
        area_stats = {}
        if self.scraper:
            area_stats = self.scraper.area_stats

        return {
            "id": self.id,
            "keyword": self.keyword,
            "place": self.place,
            "map_selection": self.map_selection,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "lead_count": len(self.leads),
            "error": self.error,
            "created_at": self.created_at,
            "elapsed": elapsed_str,
            "elapsed_seconds": int(elapsed),
            "area_stats": area_stats,
        }

# ============================================================
# Phase 2: Queue job → API response helpers
# ============================================================

def _queue_job_to_status(qjob: dict, tool_type: str) -> dict:
    """Convert a unified job row to the status response format the frontend expects."""
    status = qjob.get("status", "queued")
    created_at = qjob.get("created_at", "")
    started_at = qjob.get("started_at")

    # Compute elapsed
    elapsed_seconds = 0
    if started_at:
        try:
            start = datetime.fromisoformat(str(started_at))
            elapsed_seconds = max(0, int((datetime.utcnow() - start).total_seconds()))
        except (ValueError, TypeError):
            pass
    elif created_at:
        try:
            start = datetime.fromisoformat(str(created_at))
            elapsed_seconds = max(0, int((datetime.utcnow() - start).total_seconds()))
        except (ValueError, TypeError):
            pass

    h, rem = divmod(elapsed_seconds, 3600)
    m, s = divmod(rem, 60)
    elapsed_str = f"{h:02d}:{m:02d}:{s:02d}"

    # Parse result JSON for stats
    result_data = {}
    try:
        result_data = json.loads(qjob.get("result") or "{}")
    except (json.JSONDecodeError, TypeError):
        pass

    return {
        "id": qjob["job_id"],
        "status": status,
        "progress": qjob.get("progress", 0),
        "message": qjob.get("message", ""),
        "lead_count": qjob.get("result_count", 0),
        "error": qjob.get("error", ""),
        "created_at": created_at,
        "elapsed": elapsed_str,
        "elapsed_seconds": elapsed_seconds,
        "scrape_stats": result_data.get("area_stats", {}),
        "attempt": qjob.get("attempt", 1),
        "max_attempts": qjob.get("max_attempts", 3),
        # Phase 3: agent execution info for UI badge
        "execution_mode": qjob.get("execution_mode", "cloud"),
        "agent_id": qjob.get("agent_id", ""),
    }


def _queue_job_results_response(qjob: dict):
    """Convert a unified job row to the results response the frontend expects."""
    status = qjob.get("status", "queued")
    if status not in ("completed", "partial", "failed"):
        return jsonify({"error": "Job not completed yet.", "status": status}), 400

    result_data = {}
    try:
        result_data = json.loads(qjob.get("result") or "{}")
    except (json.JSONDecodeError, TypeError):
        pass

    leads = result_data.get("leads", [])
    return jsonify({
        "leads": leads,
        "total": len(leads),
        "job": _queue_job_to_status(qjob, qjob.get("type", "")),
    })


class LinkedInJob:
    """Tracks a LinkedIn scraping job."""

    def __init__(self, niche: str, place: str, search_type: str = "profiles"):
        self.id = str(uuid.uuid4())[:8]
        self.niche = niche
        self.place = place
        self.search_type = search_type
        self.status = "running"
        self.progress = 0
        self.message = "Starting..."
        self.leads = []
        self.error = None
        self.scraper = None
        self.created_at = datetime.now().isoformat()
        self.started_at = datetime.now()

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
        elapsed = (datetime.now() - self.started_at).total_seconds()
        hours, rem = divmod(int(elapsed), 3600)
        minutes, secs = divmod(rem, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"

        scrape_stats = {}
        if self.scraper:
            scrape_stats = self.scraper.scrape_stats

        return {
            "id": self.id,
            "niche": self.niche,
            "place": self.place,
            "search_type": self.search_type,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "lead_count": len(self.leads),
            "error": self.error,
            "created_at": self.created_at,
            "elapsed": elapsed_str,
            "elapsed_seconds": int(elapsed),
            "scrape_stats": scrape_stats,
        }


class InstagramJob:
    """Tracks an Instagram scraping job."""

    def __init__(self, keywords: str, place: str, search_type: str = "emails"):
        self.id = str(uuid.uuid4())[:8]
        self.keywords = keywords
        self.place = place
        self.search_type = search_type
        self.status = "running"
        self.progress = 0
        self.message = "Starting..."
        self.leads = []
        self.error = None
        self.scraper = None
        self.created_at = datetime.now().isoformat()
        self.started_at = datetime.now()

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
        elapsed = (datetime.now() - self.started_at).total_seconds()
        hours, rem = divmod(int(elapsed), 3600)
        minutes, secs = divmod(rem, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"

        scrape_stats = {}
        if self.scraper:
            scrape_stats = self.scraper.scrape_stats

        return {
            "id": self.id,
            "keywords": self.keywords,
            "place": self.place,
            "search_type": self.search_type,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "lead_count": len(self.leads),
            "error": self.error,
            "created_at": self.created_at,
            "elapsed": elapsed_str,
            "elapsed_seconds": int(elapsed),
            "scrape_stats": scrape_stats,
        }


class WebCrawlerJob:
    """Tracks a Web Crawler scraping job."""

    def __init__(self, keyword: str, place: str):
        self.id = str(uuid.uuid4())[:8]
        self.keyword = keyword
        self.place = place
        self.status = "running"
        self.progress = 0
        self.message = "Starting..."
        self.leads = []
        self.error = None
        self.scraper = None
        self.created_at = datetime.now().isoformat()
        self.started_at = datetime.now()

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
        elapsed = (datetime.now() - self.started_at).total_seconds()
        hours, rem = divmod(int(elapsed), 3600)
        minutes, secs = divmod(rem, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"

        scrape_stats = {}
        if self.scraper:
            scrape_stats = self.scraper.scrape_stats

        return {
            "id": self.id,
            "keyword": self.keyword,
            "place": self.place,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "lead_count": len(self.leads),
            "error": self.error,
            "created_at": self.created_at,
            "elapsed": elapsed_str,
            "elapsed_seconds": int(elapsed),
            "scrape_stats": scrape_stats,
        }


# ============================================================
# Background runners
# ============================================================

def run_scraping_job(job: ScrapingJob):
    """Run Google Maps scraping in a background thread."""
    try:
        scraper = GoogleMapsScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw_leads = scraper.scrape(job.keyword, job.place)
        cleaned = clean_leads(raw_leads)
        job.leads = cleaned

        if cleaned:
            filename = (
                f"leads_{job.keyword}_{job.place}_{job.id}.csv"
                .replace(" ", "_").lower()
            )
            csv_path = os.path.join(OUTPUT_DIR, filename)
            save_gmaps_csv(cleaned, csv_path)
            job.csv_path = csv_path

        if job.status == "stopped":
            # User stopped mid-way — save partial results
            partial = scraper.get_partial_leads()
            if partial:
                cleaned = clean_leads(partial)
                job.leads = cleaned
                if cleaned:
                    filename = (
                        f"leads_{job.keyword}_{job.place}_{job.id}_partial.csv"
                        .replace(" ", "_").lower()
                    )
                    csv_path = os.path.join(OUTPUT_DIR, filename)
                    save_gmaps_csv(cleaned, csv_path)
                    job.csv_path = csv_path
            job.message = f"Stopped. Saved {len(job.leads)} leads."
            _record_history_on_complete(job, "gmaps")
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} leads."
        _record_history_on_complete(job, "gmaps")

    except Exception as e:
        # On error, still save partial results
        if job.scraper:
            partial = job.scraper.get_partial_leads()
            if partial:
                cleaned = clean_leads(partial)
                job.leads = cleaned
        if job.status != "stopped":
            job.status = "failed"
            job.error = str(e)
            job.message = f"Error: {str(e)}. Saved {len(job.leads)} partial leads."
        _record_history_on_complete(job, "gmaps")
    finally:
        # Release scraper resources and prune old jobs
        if job.scraper:
            try: job.scraper.close()
            except Exception: pass
            job.scraper = None
        _cleanup_jobs(scraping_jobs)


def run_linkedin_job(job: LinkedInJob):
    """Run LinkedIn scraping in a background thread."""
    try:
        scraper = LinkedInScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw = scraper.scrape(job.niche, job.place, search_type=job.search_type)
        cleaned = clean_linkedin_leads(raw, job.search_type)
        job.leads = cleaned

        if job.status == "stopped":
            partial = scraper.get_partial_leads()
            if partial:
                cleaned = clean_linkedin_leads(partial, job.search_type)
                job.leads = cleaned
            job.message = f"Stopped. Saved {len(job.leads)} {job.search_type}."
            _record_history_on_complete(job, "linkedin")
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} {job.search_type}."
        _record_history_on_complete(job, "linkedin")

    except Exception as e:
        if job.scraper:
            partial = job.scraper.get_partial_leads()
            if partial:
                cleaned = clean_linkedin_leads(partial, job.search_type)
                job.leads = cleaned
        if job.status != "stopped":
            job.status = "failed"
            job.error = str(e)
            job.message = f"Error: {str(e)}. Saved {len(job.leads)} partial leads."
        _record_history_on_complete(job, "linkedin")
    finally:
        if job.scraper:
            try: job.scraper.close()
            except Exception: pass
            job.scraper = None
        _cleanup_jobs(linkedin_jobs)


def run_instagram_job(job: InstagramJob):
    """Run Instagram scraping in a background thread."""
    try:
        scraper = InstagramScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw = scraper.scrape(
            job.keywords, job.place, search_type=job.search_type,
        )
        cleaned = clean_instagram_leads(raw, job.search_type)
        job.leads = cleaned

        if job.status == "stopped":
            partial = scraper.get_partial_leads()
            if partial:
                cleaned = clean_instagram_leads(partial, job.search_type)
                job.leads = cleaned
            job.message = f"Stopped. Saved {len(job.leads)} Instagram {job.search_type}."
            _record_history_on_complete(job, "instagram")
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} Instagram {job.search_type}."
        _record_history_on_complete(job, "instagram")

    except Exception as e:
        if job.scraper:
            partial = job.scraper.get_partial_leads()
            if partial:
                cleaned = clean_instagram_leads(partial, job.search_type)
                job.leads = cleaned
        if job.status != "stopped":
            job.status = "failed"
            job.error = str(e)
            job.message = f"Error: {str(e)}. Saved {len(job.leads)} partial leads."
        _record_history_on_complete(job, "instagram")
    finally:
        if job.scraper:
            try: job.scraper.close()
            except Exception: pass
            job.scraper = None
        _cleanup_jobs(instagram_jobs)


def run_webcrawler_job(job: WebCrawlerJob):
    """Run Web Crawler scraping in a background thread."""
    try:
        scraper = WebCrawlerScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw = scraper.scrape(job.keyword, job.place)
        cleaned = clean_web_leads(raw)
        job.leads = cleaned

        if job.status == "stopped":
            partial = scraper.get_partial_leads()
            if partial:
                cleaned = clean_web_leads(partial)
                job.leads = cleaned
            job.message = f"Stopped. Saved {len(job.leads)} leads."
            _record_history_on_complete(job, "webcrawler")
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} leads from the web."
        _record_history_on_complete(job, "webcrawler")

    except Exception as e:
        if job.scraper:
            partial = job.scraper.get_partial_leads()
            if partial:
                cleaned = clean_web_leads(partial)
                job.leads = cleaned
        if job.status != "stopped":
            job.status = "failed"
            job.error = str(e)
            job.message = f"Error: {str(e)}. Saved {len(job.leads)} partial leads."
        _record_history_on_complete(job, "webcrawler")
    finally:
        if job.scraper:
            try: job.scraper.close()
            except Exception: pass
            job.scraper = None
        _cleanup_jobs(webcrawler_jobs)


# ============================================================
# Auth & Subscription routes
# ============================================================

@app.route("/login")
def login_page():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    return render_template("login.html")


@app.route("/register")
def register_page():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    return render_template("register.html")


@app.route("/activate")
@login_required
def activate_page():
    user = current_user()
    if user and user["is_active"]:
        return redirect(url_for("dashboard"))
    return render_template("activate.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/api/auth/register", methods=["POST"])
@limiter.limit("5 per minute")
def api_register():
    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()
    full_name = (data.get("full_name") or "").strip()

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400
    if not _is_valid_email(email):
        return jsonify({"error": "Please enter a valid email address."}), 400

    pw_err = _validate_password_strength(password)
    if pw_err:
        return jsonify({"error": pw_err}), 400

    db = get_db()
    existing = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        return jsonify({"error": "An account with this email already exists."}), 409

    hashed = _hash_password(password)
    cur = db.execute(
        "INSERT INTO users (email, password, full_name) VALUES (?, ?, ?)",
        (email, hashed, full_name),
    )
    db.commit()
    user_id = cur.lastrowid
    log.info(f"New user registered: {email} (id={user_id})")

    session.permanent = True
    session["user_id"] = user_id
    session["email"] = email
    return jsonify({"message": "Account created. Please activate your license.", "user_id": user_id}), 201


@app.route("/api/auth/login", methods=["POST"])
@limiter.limit("10 per minute")
def api_login():
    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400

    db = get_db()
    user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    if not user or not _verify_password(password, user["password"]):
        return jsonify({"error": "Invalid email or password."}), 401

    # Transparently upgrade legacy SHA-256 → bcrypt
    _upgrade_password_if_needed(user["id"], password, user["password"])

    db.execute("UPDATE users SET last_login = datetime('now') WHERE id = ?", (user["id"],))
    db.commit()

    # Regenerate session to prevent fixation attacks
    session.clear()
    session.permanent = True
    session["user_id"] = user["id"]
    session["email"] = user["email"]
    return jsonify({
        "message": "Login successful.",
        "is_active": bool(user["is_active"]),
    })


@app.route("/api/auth/activate", methods=["POST"])
@login_required
@limiter.limit("10 per minute")
def api_activate():
    data = request.get_json()
    key = (data.get("license_key") or "").strip().upper()
    if not key:
        return jsonify({"error": "License key is required."}), 400

    db = get_db()
    row = db.execute("SELECT * FROM license_keys WHERE key = ?", (key,)).fetchone()
    if not row:
        return jsonify({"error": "Invalid license key."}), 404
    if row["expires_at"] and row["expires_at"] < datetime.now().isoformat():
        return jsonify({"error": "This license key has expired."}), 410
    if row["used_count"] >= row["max_uses"]:
        return jsonify({"error": "This license key has reached its usage limit."}), 410

    uid = session["user_id"]
    db.execute("UPDATE users SET is_active = 1, license_key = ? WHERE id = ?", (key, uid))
    db.execute("UPDATE license_keys SET used_count = used_count + 1 WHERE key = ?", (key,))
    db.commit()
    log.info(f"License activated for user {uid}: {key}")
    return jsonify({"message": "License activated! Welcome to LeadGen Pro."})


# ============================================================
# Stripe Webhook — auto-generate license key on payment
# ============================================================

@app.route("/api/stripe/webhook", methods=["POST"])
@limiter.exempt
def stripe_webhook():
    """Handle Stripe webhook events for automated license provisioning."""
    payload = request.get_data(as_text=True)
    sig_header = request.headers.get("Stripe-Signature", "")

    if not STRIPE_WEBHOOK_SECRET:
        log.warning("Stripe webhook called but STRIPE_WEBHOOK_SECRET not set")
        return jsonify({"error": "Webhook not configured"}), 500

    try:
        event = stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        return jsonify({"error": "Invalid payload"}), 400
    except stripe.error.SignatureVerificationError:
        return jsonify({"error": "Invalid signature"}), 400

    if event["type"] == "checkout.session.completed":
        session_obj = event["data"]["object"]
        customer_email = (session_obj.get("customer_email") or session_obj.get("customer_details", {}).get("email", "")).lower().strip()

        if customer_email:
            _provision_license_for_email(customer_email, plan="pro")

    elif event["type"] == "invoice.payment_succeeded":
        invoice = event["data"]["object"]
        customer_email = (invoice.get("customer_email") or "").lower().strip()
        if customer_email:
            _provision_license_for_email(customer_email, plan="pro")

    return jsonify({"status": "ok"}), 200


def _provision_license_for_email(email: str, plan: str = "pro"):
    """Auto-create a license key and activate the user's account."""
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    try:
        # Generate a unique license key
        license_key = _generate_license_key()
        expires_at = (datetime.now() + timedelta(days=365)).isoformat()

        db.execute(
            "INSERT INTO license_keys (key, plan, max_uses, expires_at) VALUES (?, ?, 1, ?)",
            (license_key, plan, expires_at),
        )

        # If user already registered, auto-activate
        user = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if user:
            db.execute(
                "UPDATE users SET is_active = 1, license_key = ? WHERE id = ?",
                (license_key, user["id"]),
            )
            db.execute(
                "UPDATE license_keys SET used_count = 1 WHERE key = ?",
                (license_key,),
            )
            log.info(f"Auto-activated license {license_key} for existing user {email}")
        else:
            # Key is ready — user will activate on first login
            log.info(f"License {license_key} created for future user {email}")

        db.commit()
    except Exception as e:
        log.error(f"License provisioning error for {email}: {e}")
    finally:
        db.close()


# ============================================================
# Stripe Checkout — create a checkout session
# ============================================================

@app.route("/api/stripe/create-checkout", methods=["POST"])
def api_create_checkout():
    """Create a Stripe Checkout session for license purchase."""
    if not stripe.api_key or not STRIPE_PRICE_ID_PRO:
        return jsonify({"error": "Payment system not configured."}), 503

    data = request.get_json() or {}
    success_url = data.get("success_url", request.host_url + "activate?payment=success")
    cancel_url = data.get("cancel_url", request.host_url + "register")

    try:
        checkout_session = stripe.checkout.Session.create(
            payment_method_types=["card"],
            line_items=[{"price": STRIPE_PRICE_ID_PRO, "quantity": 1}],
            mode="payment",
            success_url=success_url,
            cancel_url=cancel_url,
            customer_email=session.get("email", "") or None,
        )
        return jsonify({"checkout_url": checkout_session.url})
    except Exception as e:
        log.error(f"Stripe checkout error: {e}")
        return jsonify({"error": "Could not create checkout session."}), 500


@app.route("/api/auth/me")
@login_required
def api_me():
    user = current_user()
    if not user:
        return jsonify({"error": "Not found."}), 404
    return jsonify({
        "id": user["id"],
        "email": user["email"],
        "full_name": user["full_name"],
        "is_active": bool(user["is_active"]),
        "license_key": user["license_key"] or "",
        "created_at": user["created_at"],
    })


# ============================================================
# Dashboard analytics API
# ============================================================

@app.route("/api/dashboard/stats")
@login_required
def api_dashboard_stats():
    uid = session["user_id"]
    db = get_db()

    # Total leads
    total = db.execute(
        "SELECT COALESCE(SUM(lead_count),0) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]

    # Quality breakdown
    strong = db.execute(
        "SELECT COALESCE(SUM(strong),0) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]
    medium = db.execute(
        "SELECT COALESCE(SUM(medium),0) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]
    weak = db.execute(
        "SELECT COALESCE(SUM(weak),0) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]

    # Total scrapes
    scrape_count = db.execute(
        "SELECT COUNT(*) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]

    # By tool
    tool_rows = db.execute(
        "SELECT tool, COUNT(*) as cnt, COALESCE(SUM(lead_count),0) as leads "
        "FROM scrape_history WHERE user_id=? GROUP BY tool",
        (uid,),
    ).fetchall()
    by_tool = {r["tool"]: {"scrapes": r["cnt"], "leads": r["leads"]} for r in tool_rows}

    # Recent 7 days trend
    trend_rows = db.execute(
        "SELECT DATE(started_at) as day, COALESCE(SUM(lead_count),0) as leads "
        "FROM scrape_history WHERE user_id=? AND started_at >= datetime('now','-7 days') "
        "GROUP BY DATE(started_at) ORDER BY day",
        (uid,),
    ).fetchall()
    trend = [{"day": r["day"], "leads": r["leads"]} for r in trend_rows]

    return jsonify({
        "total_leads": total,
        "strong": strong,
        "medium": medium,
        "weak": weak,
        "scrape_count": scrape_count,
        "by_tool": by_tool,
        "trend": trend,
    })


@app.route("/api/dashboard/history")
@login_required
def api_dashboard_history():
    uid = session["user_id"]
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    offset = (page - 1) * per_page

    db = get_db()
    total = db.execute(
        "SELECT COUNT(*) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]

    rows = db.execute(
        "SELECT * FROM scrape_history WHERE user_id=? "
        "ORDER BY started_at DESC LIMIT ? OFFSET ?",
        (uid, per_page, offset),
    ).fetchall()

    history = []
    for r in rows:
        history.append({
            "id": r["id"],
            "job_id": r["job_id"],
            "tool": r["tool"],
            "keyword": r["keyword"],
            "location": r["location"],
            "search_type": r["search_type"],
            "status": r["status"],
            "lead_count": r["lead_count"],
            "strong": r["strong"],
            "medium": r["medium"],
            "weak": r["weak"],
            "started_at": r["started_at"],
            "finished_at": r["finished_at"],
        })

    return jsonify({"history": history, "total": total, "page": page, "per_page": per_page})


@app.route("/api/leads/quality/<job_id>")
@login_required
def api_lead_quality(job_id):
    """Score leads for a specific active job."""
    # Search across all job stores
    job = (
        scraping_jobs.get(job_id)
        or linkedin_jobs.get(job_id)
        or instagram_jobs.get(job_id)
        or webcrawler_jobs.get(job_id)
    )
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if not job.leads:
        return jsonify({"strong": 0, "medium": 0, "weak": 0, "total": 0})

    # Determine tool type
    if job_id in scraping_jobs:
        tool = "gmaps"
    elif job_id in linkedin_jobs:
        tool = "linkedin"
    elif job_id in instagram_jobs:
        tool = "instagram"
    else:
        tool = "webcrawler"

    _, counts = score_leads(list(job.leads), tool)
    return jsonify({**counts, "total": len(job.leads)})


# ============================================================

def save_gmaps_csv(leads: list[dict], filepath: str):
    """Save Google Maps leads to CSV."""
    if not leads:
        return
    fieldnames = [
        "lead_uid",
        "business_name", "owner_name", "phone", "website", "email",
        "address", "rating", "reviews", "category",
        "facebook", "instagram", "twitter", "linkedin",
        "youtube", "tiktok", "pinterest",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(leads)


# ============================================================
# Page routes
# ============================================================

@app.route("/")
def landing_page():
    """Public landing page."""
    if IS_DESKTOP:
        return redirect(url_for("login_page"))
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    return render_template("landing.html", current_year=datetime.now().year)


@app.route("/dashboard")
@login_required
def dashboard():
    """Dashboard page."""
    user = current_user()
    if user and not user["is_active"]:
        return redirect(url_for("activate_page"))
    return render_template("dashboard.html", active_page="dashboard")


@app.route("/tools/google-maps")
@subscription_required
def google_maps_tool():
    """Google Maps scraper page."""
    return render_template("gmaps.html", active_page="gmaps")


@app.route("/tools/linkedin")
@subscription_required
def linkedin_tool():
    """LinkedIn scraper page."""
    return render_template("linkedin.html", active_page="linkedin")


@app.route("/tools/instagram")
@subscription_required
def instagram_tool():
    """Instagram scraper page."""
    return render_template("instagram.html", active_page="instagram")


@app.route("/tools/web-crawler")
@subscription_required
def webcrawler_tool():
    """Web Crawler page."""
    return render_template("webcrawler.html", active_page="webcrawler")


@app.route("/tools/email-outreach")
@subscription_required
def email_outreach_tool():
    """Email Outreach Template Generator page."""
    return render_template("email_outreach.html", active_page="email_outreach")


@app.route("/sessions")
@subscription_required
def sessions_page():
    """Google Maps sessions page."""
    return render_template("sessions.html", active_page="sessions")


@app.route("/intelligence")
@login_required
def page_intelligence():
    """Lead Intelligence page — scored leads, signals, and insights."""
    return render_template("intelligence.html", active_page="intelligence")


# ── Phase 5: New goal-based page routes ───────────────────────────────────

@app.route("/find-leads")
@login_required
def page_find_leads():
    """Find Leads hub — entry point for all scraping tools."""
    return render_template("find_leads.html", active_page="find_leads")


@app.route("/pipeline")
@login_required
def page_pipeline():
    """CRM Pipeline — Kanban board for lead management."""
    return render_template("pipeline.html", active_page="pipeline")


@app.route("/outreach")
@login_required
def page_outreach():
    """Outreach — email campaign manager."""
    return render_template("outreach.html", active_page="outreach")


@app.route("/workflows")
@login_required
def page_workflows():
    """Workflows — visual automation builder."""
    return render_template("workflows.html", active_page="workflows")


# ── Phase 5: Dashboard metrics API ────────────────────────────────────────

@app.route("/api/dashboard/metrics")
@login_required
def api_dashboard_metrics():
    """Return all 6 dashboard widget data in one JSON call."""
    from dashboard.metrics import get_dashboard_metrics
    uid  = int(session["user_id"])
    days = int(request.args.get("days", 30))
    return jsonify(get_dashboard_metrics(uid, days))


# ============================================================
# Google Maps API
# ============================================================

@app.route("/api/scrape", methods=["POST"])
@subscription_required
def start_scrape():
    """Start a new Google Maps scraping job in a background thread."""
    data = request.get_json()
    keyword = data.get("keyword", "").strip()
    place = data.get("place", "").strip()
    max_leads = data.get("max_leads")
    map_selection = data.get("map_selection") if isinstance(data.get("map_selection"), dict) else None

    if not place and map_selection:
        center = map_selection.get("center") if isinstance(map_selection.get("center"), dict) else {}
        lat = center.get("lat")
        lng = center.get("lng")
        if lat is not None and lng is not None:
            place = f"{lat}, {lng}"

    if not keyword or not place:
        return jsonify({"error": "Both keyword and place are required (or select an area on map)."}), 400

    if max_leads is not None:
        try:
            max_leads = int(max_leads)
            if max_leads <= 0:
                max_leads = None
        except (TypeError, ValueError):
            return jsonify({"error": "max_leads must be a number or omitted."}), 400

    job_id = str(uuid.uuid4())[:8]

    payload = {
        "job_id": job_id,
        "tool": "gmaps",
        "keyword": keyword,
        "place": place,
        "map_selection": map_selection,
        "max_leads": max_leads,
        "crawl_contacts": False,
    }

    initial_state = {
        "job_id": job_id,
        "tool": "gmaps",
        "user_id": session["user_id"],
        "status": "PENDING",
        "phase": "extract",
        "extraction_status": "pending",
        "contacts_status": "pending",
        "progress": 0,
        "message": "Starting scraper...",
        "keyword": keyword,
        "place": place,
        "max_leads": max_leads,
        "results_count": 0,
        "results": [],
        "stop_requested": False,
        "contact_paused": False,
        "contact_stop_requested": False,
        "payload": payload,
        "logs": [{
            "at": datetime.utcnow().isoformat(),
            "message": "Session created",
            "progress": 0,
        }],
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat(),
    }
    _save_job_state_and_persist(job_id, initial_state)
    _insert_history_direct(session["user_id"], job_id, "gmaps", keyword, place)

    accepted, reason, pool = submit_extract_job(job_id, session["user_id"], _run_scrape_in_thread, job_id, payload)
    if not accepted:
        initial_state.update({
            "status": "PENDING",
            "message": "Extraction queued capacity reached. Retry shortly.",
            "updated_at": datetime.utcnow().isoformat(),
        })
        _append_job_log(initial_state, f"Queue rejected ({reason})", 0)
        _save_job_state_and_persist(job_id, initial_state)
        _persist_gmaps_event(
            initial_state,
            "extract_queue_rejected",
            "Extraction queue rejected job due to backpressure",
            severity="warning",
            payload={"reason": reason, "pool": pool},
        )
        return jsonify({"error": "Extraction queue is full. Please retry.", "reason": reason, "pool": pool}), 429

    _persist_gmaps_event(
        initial_state,
        "extract_queue_accepted",
        "Extraction job accepted by worker pool",
        payload={"pool": pool},
    )

    return jsonify({"job_id": job_id, "message": "List extraction started."}), 202


@app.route("/api/status/<job_id>")
def job_status(job_id):
    state = get_job_state(job_id)
    if state:
        return jsonify(_state_for_frontend(state))

    persisted = _load_persisted_session_state(job_id)
    if persisted:
        return jsonify(_state_for_frontend(persisted))

    job = scraping_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job.to_dict())


@app.route("/api/results/<job_id>")
def job_results(job_id):
    state = get_job_state(job_id)
    if state:
        lifecycle = str(state.get("status", "PENDING")).upper()
        leads = state.get("results", [])
        if not leads:
            leads = _load_persisted_session_leads(job_id)
        # Return results at ANY stage — partial or complete
        return jsonify({
            "leads": leads,
            "total": len(leads),
            "partial": lifecycle not in ("COMPLETED", "PARTIAL"),
            "status": lifecycle,
            "job": _state_for_frontend(state),
        })

    persisted = _load_persisted_session_state(job_id)
    if persisted:
        lifecycle = str(persisted.get("status", "PENDING")).upper()
        leads = persisted.get("results", [])
        return jsonify({
            "leads": leads,
            "total": len(leads),
            "partial": lifecycle not in ("COMPLETED", "PARTIAL"),
            "status": lifecycle,
            "job": _state_for_frontend(persisted),
        })

    job = scraping_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@app.route("/api/download/<job_id>")
def download_csv(job_id):
    queue_state = get_job_state(job_id)
    if queue_state:
        leads = queue_state.get("results", [])
        if not leads:
            leads = _load_persisted_session_leads(job_id)
        if not leads:
            return jsonify({"error": "No data available for download yet."}), 400

        output = io.StringIO()
        fieldnames = [
            "Lead ID",
            "Business Name", "Owner Name", "Phone", "Website", "Email",
            "Address", "Rating", "Reviews", "Category",
            "Facebook", "Instagram", "Twitter", "LinkedIn",
            "YouTube", "TikTok", "Pinterest",
        ]
        key_map = {
            "Lead ID": "lead_uid",
            "Business Name": "business_name", "Owner Name": "owner_name",
            "Phone": "phone", "Website": "website", "Email": "email",
            "Address": "address", "Rating": "rating", "Reviews": "reviews",
            "Category": "category", "Facebook": "facebook",
            "Instagram": "instagram", "Twitter": "twitter",
            "LinkedIn": "linkedin", "YouTube": "youtube",
            "TikTok": "tiktok", "Pinterest": "pinterest",
        }
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for lead in leads:
            row = {display: lead.get(key, "N/A") for display, key in key_map.items()}
            writer.writerow(row)

        output.seek(0)
        keyword = queue_state.get("keyword", "leads")
        place = queue_state.get("place", "area")
        filename = f"leads_{keyword}_{place}.csv".replace(" ", "_").lower()
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    persisted_state = _load_persisted_session_state(job_id)
    if persisted_state:
        leads = persisted_state.get("results", [])
        if not leads:
            return jsonify({"error": "No data available for download yet."}), 400

        output = io.StringIO()
        fieldnames = [
            "Lead ID",
            "Business Name", "Owner Name", "Phone", "Website", "Email",
            "Address", "Rating", "Reviews", "Category",
            "Facebook", "Instagram", "Twitter", "LinkedIn",
            "YouTube", "TikTok", "Pinterest",
        ]
        key_map = {
            "Lead ID": "lead_uid",
            "Business Name": "business_name", "Owner Name": "owner_name",
            "Phone": "phone", "Website": "website", "Email": "email",
            "Address": "address", "Rating": "rating", "Reviews": "reviews",
            "Category": "category", "Facebook": "facebook",
            "Instagram": "instagram", "Twitter": "twitter",
            "LinkedIn": "linkedin", "YouTube": "youtube",
            "TikTok": "tiktok", "Pinterest": "pinterest",
        }
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for lead in leads:
            row = {display: lead.get(key, "N/A") for display, key in key_map.items()}
            writer.writerow(row)

        output.seek(0)
        keyword = persisted_state.get("keyword", "leads")
        place = persisted_state.get("place", "area")
        filename = f"leads_{keyword}_{place}.csv".replace(" ", "_").lower()
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    job = scraping_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped") or not job.leads:
        return jsonify({"error": "No data available for download."}), 400

    output = io.StringIO()
    fieldnames = [
        "Lead ID",
        "Business Name", "Owner Name", "Phone", "Website", "Email",
        "Address", "Rating", "Reviews", "Category",
        "Facebook", "Instagram", "Twitter", "LinkedIn",
        "YouTube", "TikTok", "Pinterest",
    ]
    key_map = {
        "Lead ID": "lead_uid",
        "Business Name": "business_name", "Owner Name": "owner_name",
        "Phone": "phone", "Website": "website", "Email": "email",
        "Address": "address", "Rating": "rating", "Reviews": "reviews",
        "Category": "category", "Facebook": "facebook",
        "Instagram": "instagram", "Twitter": "twitter",
        "LinkedIn": "linkedin", "YouTube": "youtube",
        "TikTok": "tiktok", "Pinterest": "pinterest",
    }
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for lead in job.leads:
        row = {display: lead.get(key, "N/A") for display, key in key_map.items()}
        writer.writerow(row)

    output.seek(0)
    filename = f"leads_{job.keyword}_{job.place}.csv".replace(" ", "_").lower()
    return Response(
        output.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/stop/<job_id>", methods=["POST"])
def stop_scrape(job_id):
    queue_state = get_job_state(job_id)
    if queue_state:
        phase = str(queue_state.get("phase", "extract"))
        lifecycle = str(queue_state.get("status", "PENDING")).upper()
        if lifecycle in ("COMPLETED", "FAILED", "PARTIAL"):
            return jsonify({"message": f"Job already {lifecycle}."})

        if phase == "contacts":
            queue_state["contact_stop_requested"] = True
            queue_state["contact_paused"] = True
            queue_state["contacts_status"] = "paused"
            queue_state["message"] = "Pause requested for contact retrieval..."
            queue_state["updated_at"] = datetime.utcnow().isoformat()
            _append_job_log(queue_state, queue_state["message"], queue_state.get("progress", 0))
            _save_job_state_and_persist(job_id, queue_state)
            _persist_partial_snapshot_checkpoint(queue_state, reason="contacts_pause_requested_api_stop")
            _persist_gmaps_event(
                queue_state,
                "contacts_paused",
                "Contact retrieval pause requested by user",
                payload={"trigger": "api_stop"},
            )
            _upsert_gmaps_task(
                session_id=job_id,
                user_id=int(queue_state.get("user_id") or 0),
                task_key="contacts_main",
                phase="contacts",
                status="paused",
                payload={"trigger": "api_stop"},
            )
            return jsonify({"message": "Pause signal sent for contact retrieval."})

        set_job_stop_requested(job_id, True)

        pending = get_job_state(job_id) or queue_state
        pending.update({
            "message": "Stop requested. Finishing current step...",
            "updated_at": datetime.utcnow().isoformat(),
        })
        _append_job_log(pending, pending["message"], pending.get("progress", 0))
        _save_job_state_and_persist(job_id, pending)
        _persist_partial_snapshot_checkpoint(pending, reason="extract_stop_requested_api_stop")
        _persist_gmaps_event(
            pending,
            "extract_pause_requested",
            "Extraction stop requested by user",
            payload={"trigger": "api_stop"},
        )
        return jsonify({"message": "Stop signal sent."})

    job = scraping_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.scraper:
        job.scraper.stop()
        # Immediately grab partial leads
        partial = job.scraper.get_partial_leads()
        if partial:
            cleaned = clean_leads(partial)
            job.leads = cleaned
    job.status = "stopped"
    job.message = f"Stopped by user. Saved {len(job.leads)} leads."
    return jsonify({"message": f"Job stopped. {len(job.leads)} leads saved."})


@app.route("/api/gmaps/contacts/start/<job_id>", methods=["POST"])
@subscription_required
def gmaps_start_contact_retrieval(job_id):
    state = _load_job_state_with_fallback(job_id)
    if not state:
        return jsonify({"error": "Session not found."}), 404

    leads = state.get("results") if isinstance(state.get("results"), list) else []
    if not leads:
        return jsonify({"error": "No extracted leads found for this session."}), 400

    if str(state.get("contacts_status", "pending")) == "running":
        return jsonify({"error": "Contact retrieval is already running."}), 409

    state["contact_paused"] = False
    state["contact_stop_requested"] = False
    state["phase"] = "contacts"
    state["extraction_status"] = "completed"
    state["contacts_status"] = "running"
    state["updated_at"] = datetime.utcnow().isoformat()
    _append_job_log(state, "Contact retrieval requested", state.get("progress", 0))
    _save_job_state_and_persist(job_id, state)
    _upsert_gmaps_task(
        session_id=job_id,
        user_id=int(state.get("user_id") or 0),
        task_key="contacts_main",
        phase="contacts",
        status="running",
        payload={"trigger": "contacts_start"},
    )

    accepted, reason, pool = submit_contact_job(job_id, state.get("user_id") or session["user_id"], _run_contact_retrieval_thread, job_id)
    if not accepted:
        state["contacts_status"] = "pending"
        state["status"] = "PENDING"
        state["message"] = "Contact queue full. Retry shortly."
        state["updated_at"] = datetime.utcnow().isoformat()
        _append_job_log(state, f"Contact queue rejected ({reason})", state.get("progress", 0))
        _save_job_state_and_persist(job_id, state)
        _persist_gmaps_event(
            state,
            "contacts_queue_rejected",
            "Contacts queue rejected job due to backpressure",
            severity="warning",
            payload={"reason": reason, "pool": pool},
        )
        return jsonify({"error": "Contact queue is full. Please retry.", "reason": reason, "pool": pool}), 429

    _persist_gmaps_event(
        state,
        "contacts_queue_accepted",
        "Contact retrieval accepted by worker pool",
        payload={"pool": pool},
    )
    return jsonify({"message": "Contact retrieval started.", "job_id": job_id})


@app.route("/api/gmaps/contacts/pause/<job_id>", methods=["POST"])
@subscription_required
def gmaps_pause_contact_retrieval(job_id):
    state = _load_job_state_with_fallback(job_id)
    if not state:
        return jsonify({"error": "Session not found."}), 404

    state["contact_paused"] = True
    state["contacts_status"] = "paused"
    state["updated_at"] = datetime.utcnow().isoformat()
    _append_job_log(state, "Contact retrieval paused", state.get("progress", 0))
    _save_job_state_and_persist(job_id, state)
    _persist_partial_snapshot_checkpoint(state, reason="contacts_pause_requested_endpoint")
    _persist_gmaps_event(
        state,
        "contacts_paused",
        "Contact retrieval paused by user",
        payload={"trigger": "contacts_pause_endpoint"},
    )
    _upsert_gmaps_task(
        session_id=job_id,
        user_id=int(state.get("user_id") or 0),
        task_key="contacts_main",
        phase="contacts",
        status="paused",
        payload={"trigger": "contacts_pause"},
    )
    return jsonify({"message": "Contact retrieval paused."})


@app.route("/api/gmaps/contacts/resume/<job_id>", methods=["POST"])
@subscription_required
def gmaps_resume_contact_retrieval(job_id):
    state = _load_job_state_with_fallback(job_id)
    if not state:
        return jsonify({"error": "Session not found."}), 404

    if str(state.get("contacts_status", "pending")) == "running":
        return jsonify({"message": "Contact retrieval already running."})

    state["contact_paused"] = False
    state["contact_stop_requested"] = False
    state["contacts_status"] = "running"
    state["phase"] = "contacts"
    state["extraction_status"] = "completed"
    state["updated_at"] = datetime.utcnow().isoformat()
    _append_job_log(state, "Contact retrieval resumed", state.get("progress", 0))
    _save_job_state_and_persist(job_id, state)
    _upsert_gmaps_task(
        session_id=job_id,
        user_id=int(state.get("user_id") or 0),
        task_key="contacts_main",
        phase="contacts",
        status="running",
        payload={"trigger": "contacts_resume"},
    )

    accepted, reason, pool = submit_contact_job(job_id, state.get("user_id") or session["user_id"], _run_contact_retrieval_thread, job_id)
    if not accepted:
        state["contacts_status"] = "pending"
        state["status"] = "PENDING"
        state["message"] = "Contact queue full. Retry shortly."
        state["updated_at"] = datetime.utcnow().isoformat()
        _append_job_log(state, f"Contact queue rejected ({reason})", state.get("progress", 0))
        _save_job_state_and_persist(job_id, state)
        _persist_gmaps_event(
            state,
            "contacts_queue_rejected",
            "Contacts queue rejected resumed job due to backpressure",
            severity="warning",
            payload={"reason": reason, "pool": pool},
        )
        return jsonify({"error": "Contact queue is full. Please retry.", "reason": reason, "pool": pool}), 429

    _persist_gmaps_event(
        state,
        "contacts_queue_accepted",
        "Contact retrieval resume accepted by worker pool",
        payload={"pool": pool},
    )
    return jsonify({"message": "Contact retrieval resumed.", "job_id": job_id})


@app.route("/api/gmaps/contacts/restart/<job_id>", methods=["POST"])
@subscription_required
def gmaps_restart_contact_retrieval(job_id):
    state = _load_job_state_with_fallback(job_id)
    if not state:
        return jsonify({"error": "Session not found."}), 404

    leads = state.get("results") if isinstance(state.get("results"), list) else []
    if not leads:
        return jsonify({"error": "No leads found for restart."}), 400

    for lead in leads:
        for key in ("email", "facebook", "instagram", "twitter", "linkedin", "youtube", "tiktok", "pinterest"):
            if key in lead:
                lead[key] = "N/A"

    state["results"] = leads
    state["results_count"] = len(leads)
    state["contact_paused"] = False
    state["contact_stop_requested"] = False
    state["contacts_status"] = "running"
    state["phase"] = "contacts"
    state["extraction_status"] = "completed"
    state["progress"] = 0
    state["updated_at"] = datetime.utcnow().isoformat()
    _append_job_log(state, "Contact retrieval restarted", 0)
    _save_job_state_and_persist(job_id, state)
    _clear_task_chunks(job_id, "contacts_main")
    _upsert_gmaps_task(
        session_id=job_id,
        user_id=int(state.get("user_id") or 0),
        task_key="contacts_main",
        phase="contacts",
        status="running",
        payload={"trigger": "contacts_restart", "reset_results": True},
    )

    accepted, reason, pool = submit_contact_job(job_id, state.get("user_id") or session["user_id"], _run_contact_retrieval_thread, job_id)
    if not accepted:
        state["contacts_status"] = "pending"
        state["status"] = "PENDING"
        state["message"] = "Contact queue full. Retry shortly."
        state["updated_at"] = datetime.utcnow().isoformat()
        _append_job_log(state, f"Contact queue rejected ({reason})", state.get("progress", 0))
        _save_job_state_and_persist(job_id, state)
        _persist_gmaps_event(
            state,
            "contacts_queue_rejected",
            "Contacts queue rejected restarted job due to backpressure",
            severity="warning",
            payload={"reason": reason, "pool": pool},
        )
        return jsonify({"error": "Contact queue is full. Please retry.", "reason": reason, "pool": pool}), 429

    _persist_gmaps_event(
        state,
        "contacts_queue_accepted",
        "Contact retrieval restart accepted by worker pool",
        payload={"pool": pool},
    )
    return jsonify({"message": "Contact retrieval restarted.", "job_id": job_id})


@app.route("/api/gmaps/sessions")
@subscription_required
def gmaps_sessions():
    sessions = []
    by_job_id: dict[str, dict] = {}
    completion_map = _completion_by_user(session["user_id"])

    persisted = _list_persisted_sessions(session["user_id"])
    for row in persisted:
        by_job_id[row["job_id"]] = row

    for state in list_job_states():
        if state.get("tool") != "gmaps":
            continue
        if state.get("user_id") != session["user_id"]:
            continue
        by_job_id[state.get("job_id")] = state

    for value in by_job_id.values():
        item = _state_for_frontend(value)
        cm = completion_map.get(item.get("job_id") or "")
        if cm:
            item["complete_count"] = cm["complete_count"]
            item["incomplete_count"] = cm["incomplete_count"]
            item["completion_rate"] = cm["completion_rate"]
        else:
            total = int(item.get("results_count") or 0)
            item["complete_count"] = 0
            item["incomplete_count"] = total
            item["completion_rate"] = 0
        sessions.append(item)

    sessions.sort(key=lambda s: s.get("updated_at") or s.get("created_at") or "", reverse=True)
    return jsonify({"sessions": sessions})


@app.route("/api/gmaps/sessions/<job_id>/leads")
@subscription_required
def gmaps_session_leads(job_id):
    completion = request.args.get("completion", "all", type=str)
    limit = request.args.get("limit", 500, type=int)

    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404
        state = persisted

    leads = _load_session_leads_filtered(job_id, completion=completion, limit=limit)
    if not leads:
        state_leads = state.get("results") if isinstance(state.get("results"), list) else []
        if state_leads:
            if completion == "complete":
                leads = [
                    lead for lead in state_leads
                    if (
                        (lead.get("email") and lead.get("email") != "N/A")
                        or (lead.get("phone") and lead.get("phone") != "N/A")
                    )
                ]
            elif completion == "incomplete":
                leads = [
                    lead for lead in state_leads
                    if not (
                        (lead.get("email") and lead.get("email") != "N/A")
                        or (lead.get("phone") and lead.get("phone") != "N/A")
                    )
                ]
            else:
                leads = list(state_leads)
            leads = leads[: int(max(1, min(limit, 2000)))]
    logs = _load_persisted_session_logs(job_id, limit=120)

    live_logs = state.get("logs") if isinstance(state.get("logs"), list) else []
    if live_logs:
        merged: list[dict] = []
        seen = set()
        for entry in logs + live_logs[-120:]:
            msg = str(entry.get("message") or "")
            at = str(entry.get("at") or entry.get("created_at") or "")
            key = f"{at}|{msg}|{entry.get('progress')}"
            if key in seen:
                continue
            seen.add(key)
            merged.append({
                "phase": entry.get("phase") or state.get("phase") or "extract",
                "progress": entry.get("progress"),
                "message": msg,
                "at": at,
            })
        logs = merged[-150:]

    cm = _completion_by_user(session["user_id"]).get(job_id, {})
    summary = {
        "total": int(cm.get("total") or state.get("results_count") or 0),
        "complete_count": int(cm.get("complete_count") or 0),
        "incomplete_count": int(cm.get("incomplete_count") or 0),
        "completion_rate": int(cm.get("completion_rate") or 0),
    }
    checkpoints = _load_persisted_session_events(job_id, limit=120)
    resume_anchor = _select_resume_anchor(job_id, state)
    tasks = _load_persisted_session_tasks(job_id, limit=120)
    task_health = _task_health(tasks)
    contacts_chunk_summary = _task_chunk_summary(job_id, "contacts_main")
    operator_controls = {
        "can_manage_tasks": _operator_allowed_for_user(current_user()),
    }

    return jsonify({
        "job_id": job_id,
        "session": _state_for_frontend(state),
        "summary": summary,
        "completion_filter": completion,
        "leads": leads,
        "count": len(leads),
        "logs": logs,
        "checkpoints": checkpoints,
        "resume_anchor": resume_anchor,
        "tasks": tasks,
        "task_health": task_health,
        "task_chunk_summary": contacts_chunk_summary,
        "operator_controls": operator_controls,
    })


@app.route("/api/gmaps/sessions/<job_id>/recover-stale", methods=["POST"])
@subscription_required
def gmaps_recover_stale_tasks(job_id):
    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404
        state = persisted

    recovered = _mark_stale_tasks_retryable(job_id, stale_seconds=180)
    updated = int(recovered.get("updated") or 0)
    task_keys = recovered.get("task_keys") or []

    if updated > 0:
        _persist_gmaps_event(
            state,
            "stale_tasks_recovered",
            f"Recovered {updated} stale task(s) as retryable",
            payload={"updated": updated, "task_keys": task_keys},
        )

    return jsonify({
        "message": f"Recovered {updated} stale task(s).",
        "updated": updated,
        "task_keys": task_keys,
    })


@app.route("/api/gmaps/sessions/<job_id>/recover-auto", methods=["POST"])
@subscription_required
def gmaps_auto_recover_session(job_id):
    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404
        state = persisted

    payload = request.get_json(silent=True) or {}
    apply_retry = bool(payload.get("apply_retry", True))
    force = bool(payload.get("force", False))

    if force and not _operator_allowed_for_user(current_user()):
        return jsonify({"error": "Operator privileges required for forced auto-recovery."}), 403

    recovered = _mark_stale_tasks_retryable(job_id, stale_seconds=_AUTO_SWEEP_STALE_SECONDS)
    anchor = _select_resume_anchor(job_id, state)
    task_key = str(anchor.get("suggested_task_key") or "").strip().lower()

    _persist_gmaps_event(
        state,
        "auto_recovery_requested",
        "Auto-recovery requested for session",
        payload={
            "apply_retry": apply_retry,
            "force": force,
            "recovered_stale_tasks": int(recovered.get("updated") or 0),
            "resume_anchor": anchor.get("event_type"),
            "suggested_task_key": task_key,
        },
    )

    if apply_retry and task_key == "extract_main":
        return _retry_extract_task(job_id, state, force=force, force_reason="auto_recovery")
    if apply_retry and task_key == "contacts_main":
        return _retry_contacts_task(job_id, state, force=force, force_reason="auto_recovery")

    return jsonify(
        {
            "message": "Auto-recovery analysis completed.",
            "job_id": job_id,
            "recovered": recovered,
            "resume_anchor": anchor,
            "retry_started": False,
        }
    )


@app.route("/api/gmaps/sessions/<job_id>/retry-task", methods=["POST"])
@subscription_required
def gmaps_retry_task(job_id):
    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404
        state = persisted

    payload = request.get_json(silent=True) or {}
    task_key = str(payload.get("task_key") or "").strip().lower()
    force = bool(payload.get("force") is True)
    force_reason = str(payload.get("reason") or "").strip()
    if task_key not in {"extract_main", "contacts_main"}:
        return jsonify({"error": "Invalid task_key. Supported: extract_main, contacts_main"}), 400

    if force and not force_reason:
        force_reason = "operator_override"

    if force:
        user = current_user()
        if not _operator_allowed_for_user(user):
            _persist_gmaps_event(
                state,
                "task_operator_denied",
                f"Non-operator attempted force retry for {task_key}",
                severity="warning",
                payload={"task_key": task_key, "action": "force_retry"},
            )
            return jsonify({"error": "Operator privileges required for force retry."}), 403

        task = _load_gmaps_task_record(job_id, task_key)
        if task and str(task.get("status") or "").lower() == "running":
            return jsonify({"error": "Cannot force retry while task is already running.", "task_key": task_key}), 409

    if task_key == "extract_main":
        return _retry_extract_task(job_id, state, force=force, force_reason=force_reason)
    return _retry_contacts_task(job_id, state, force=force, force_reason=force_reason)


@app.route("/api/gmaps/sessions/<job_id>/retry-from-anchor", methods=["POST"])
@subscription_required
def gmaps_retry_from_anchor(job_id):
    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404
        state = persisted

    anchor = _select_resume_anchor(job_id, state)
    action = str(anchor.get("suggested_action") or "").strip().lower()
    task_key = str(anchor.get("suggested_task_key") or "").strip().lower()
    payload = request.get_json(silent=True) or {}
    force = bool(payload.get("force") is True)
    force_reason = str(payload.get("reason") or "").strip() or "anchor_operator_override"

    if force:
        user = current_user()
        if not _operator_allowed_for_user(user):
            _persist_gmaps_event(
                state,
                "task_operator_denied",
                "Non-operator attempted force retry from anchor",
                severity="warning",
                payload={"action": "force_retry_from_anchor"},
            )
            return jsonify({"error": "Operator privileges required for force retry."}), 403

    if action == "completed_no_resume_needed":
        return jsonify({
            "message": "Session already completed. No retry needed.",
            "job_id": job_id,
            "suggested_action": action,
        })

    if task_key == "extract_main":
        return _retry_extract_task(job_id, state, force=force, force_reason=force_reason)
    if task_key == "contacts_main":
        return _retry_contacts_task(job_id, state, force=force, force_reason=force_reason)

    return jsonify({"error": "No deterministic retry action available for current resume anchor."}), 400


@app.route("/api/gmaps/sessions/<job_id>/task-action", methods=["POST"])
@subscription_required
def gmaps_task_action(job_id):
    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404
        state = persisted

    user = current_user()
    if not _operator_allowed_for_user(user):
        _persist_gmaps_event(
            state,
            "task_operator_denied",
            "Non-operator attempted task operator action",
            severity="warning",
            payload={"action": "task_action"},
        )
        return jsonify({"error": "Operator privileges required for task actions."}), 403

    payload = request.get_json(silent=True) or {}
    task_key = str(payload.get("task_key") or "").strip().lower()
    action = str(payload.get("action") or "").strip().lower()
    reason = str(payload.get("reason") or "").strip() or "operator_task_action"
    actor_email = str(user["email"] or "") if user else ""

    if task_key not in {"extract_main", "contacts_main"}:
        return jsonify({"error": "Invalid task_key. Supported: extract_main, contacts_main"}), 400
    if action not in {"retry", "force_retry", "reset_attempts"}:
        return jsonify({"error": "Invalid action. Supported: retry, force_retry, reset_attempts"}), 400

    if action == "reset_attempts":
        return _reset_task_attempts(job_id, state, task_key, reason=reason, actor_email=actor_email)

    if task_key == "extract_main":
        return _retry_extract_task(
            job_id,
            state,
            force=(action == "force_retry"),
            force_reason=reason,
        )
    return _retry_contacts_task(
        job_id,
        state,
        force=(action == "force_retry"),
        force_reason=reason,
    )


@app.route("/api/gmaps/sessions/<job_id>/audit-events")
@subscription_required
def gmaps_session_audit_events(job_id):
    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404

    scope = request.args.get("scope", "operator", type=str)
    limit = request.args.get("limit", 200, type=int)
    safe_limit = int(max(1, min(limit, 1000)))

    events = _load_scoped_audit_events(job_id, scope=scope, limit=safe_limit)
    return jsonify({
        "job_id": job_id,
        "scope": scope,
        "count": len(events),
        "events": events,
    })


@app.route("/api/gmaps/sessions/<job_id>/audit-report.csv")
@subscription_required
def gmaps_session_audit_report_csv(job_id):
    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404

    scope = request.args.get("scope", "recovery", type=str)
    events = _load_scoped_audit_events(job_id, scope=scope, limit=2000)

    output = io.StringIO()
    fieldnames = [
        "at",
        "event_type",
        "severity",
        "phase",
        "status",
        "progress",
        "message",
        "task_key",
        "action",
        "reason",
        "actor",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()

    for event in events:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        writer.writerow({
            "at": event.get("at") or "",
            "event_type": event.get("event_type") or "",
            "severity": event.get("severity") or "",
            "phase": event.get("phase") or "",
            "status": event.get("status") or "",
            "progress": event.get("progress") if event.get("progress") is not None else "",
            "message": event.get("message") or "",
            "task_key": payload.get("task_key") or "",
            "action": payload.get("action") or "",
            "reason": payload.get("reason") or payload.get("force_reason") or "",
            "actor": payload.get("actor") or "",
        })

    output.seek(0)
    filename = f"gmaps_audit_{job_id}_{scope}.csv".replace(" ", "_").lower()
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/gmaps/retention/status")
@subscription_required
def gmaps_retention_status():
    user = current_user()
    if not _operator_allowed_for_user(user):
        return jsonify({"error": "Operator privileges required."}), 403

    now_ts = time.time()
    elapsed = max(0, int(now_ts - float(_last_retention_at or 0.0)))
    next_due = max(0, int(_RETENTION_INTERVAL_SECONDS - elapsed))

    return jsonify({
        "enabled": _RETENTION_ENABLED,
        "interval_seconds": _RETENTION_INTERVAL_SECONDS,
        "events_days": _RETENTION_EVENTS_DAYS,
        "logs_days": _RETENTION_LOGS_DAYS,
        "tasks_days": _RETENTION_TASKS_DAYS,
        "next_due_in_seconds": next_due,
        "last_run": dict(_last_retention_summary),
    })


@app.route("/api/gmaps/retention/archive.csv")
@subscription_required
def gmaps_retention_archive_csv():
    user = current_user()
    if not _operator_allowed_for_user(user):
        return jsonify({"error": "Operator privileges required."}), 403

    table_name = request.args.get("table", "events", type=str).strip().lower()
    days = request.args.get("older_than_days", 30, type=int)
    limit = request.args.get("limit", 5000, type=int)

    if table_name not in {"events", "logs", "tasks"}:
        return jsonify({"error": "Invalid table. Supported: events, logs, tasks"}), 400

    rows = _load_archive_rows_for_user(int(session["user_id"]), table_name, days, limit)

    output = io.StringIO()
    if table_name == "events":
        fieldnames = [
            "session_id", "user_id", "created_at", "event_type", "severity",
            "phase", "status", "progress", "message", "payload",
        ]
    elif table_name == "logs":
        fieldnames = [
            "session_id", "user_id", "created_at", "phase", "progress", "message",
        ]
    else:
        fieldnames = [
            "session_id", "user_id", "updated_at", "task_key", "phase", "status",
            "attempt_count", "max_attempts", "retry_backoff_seconds",
            "retry_cooldown_until", "last_retry_reason", "last_retry_at",
            "last_error", "payload",
        ]

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        safe_row = {k: row.get(k, "") for k in fieldnames}
        writer.writerow(safe_row)

    output.seek(0)
    filename = f"gmaps_{table_name}_archive_gt_{max(1, int(days))}d.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/gmaps/extract/restart/<job_id>", methods=["POST"])
@subscription_required
def gmaps_restart_extraction(job_id):
    state = get_job_state(job_id)
    if not state:
        state = _load_persisted_session_state(job_id)
    if not state:
        return jsonify({"error": "Session not found."}), 404

    payload = state.get("payload") if isinstance(state.get("payload"), dict) else None
    if not payload:
        return jsonify({"error": "No saved payload found for this session."}), 400

    payload = dict(payload)
    payload["crawl_contacts"] = False

    state.update({
        "status": "PENDING",
        "phase": "extract",
        "extraction_status": "pending",
        "contacts_status": "pending",
        "progress": 0,
        "results": [],
        "results_count": 0,
        "lead_count": 0,
        "stop_requested": False,
        "contact_paused": False,
        "contact_stop_requested": False,
        "message": "Restarting extraction...",
        "updated_at": datetime.utcnow().isoformat(),
        "payload": payload,
    })
    _append_job_log(state, "Extraction restarted", 0)
    _save_job_state_and_persist(job_id, state)
    _upsert_gmaps_task(
        session_id=job_id,
        user_id=int(state.get("user_id") or 0),
        task_key="extract_main",
        phase="extract",
        status="running",
        payload={"trigger": "extract_restart"},
    )

    accepted, reason, pool = submit_extract_job(job_id, state.get("user_id") or session["user_id"], _run_scrape_in_thread, job_id, payload)
    if not accepted:
        state["status"] = "PENDING"
        state["message"] = "Extraction queue full. Retry shortly."
        state["updated_at"] = datetime.utcnow().isoformat()
        _append_job_log(state, f"Extraction queue rejected ({reason})", 0)
        _save_job_state_and_persist(job_id, state)
        _persist_gmaps_event(
            state,
            "extract_queue_rejected",
            "Extraction restart rejected by worker pool due to backpressure",
            severity="warning",
            payload={"reason": reason, "pool": pool},
        )
        return jsonify({"error": "Extraction queue is full. Please retry.", "reason": reason, "pool": pool}), 429

    _persist_gmaps_event(
        state,
        "extract_queue_accepted",
        "Extraction restart accepted by worker pool",
        payload={"pool": pool},
    )
    return jsonify({"message": "Extraction restarted.", "job_id": job_id})


@app.route("/api/gmaps/worker-pools")
@subscription_required
def gmaps_worker_pools_status():
    return jsonify({"pools": worker_pool_stats(), "postgres_enabled": pg_enabled()})


@app.route("/api/gmaps/ops/metrics")
@subscription_required
def gmaps_ops_metrics():
    window_hours = _ops_safe_window_hours(request.args.get("hours", _OPS_METRICS_DEFAULT_WINDOW_HOURS, type=int))
    return jsonify(
        {
            "window_hours": window_hours,
            "stage_metrics": _ops_stage_metrics(int(session["user_id"]), window_hours),
            "recent_failures": _ops_recent_failures(int(session["user_id"]), window_hours, limit=20),
        }
    )


@app.route("/api/gmaps/ops/alerts")
@subscription_required
def gmaps_ops_alerts():
    window_hours = _ops_safe_window_hours(request.args.get("hours", _OPS_METRICS_DEFAULT_WINDOW_HOURS, type=int))
    alerts = _ops_alerts(int(session["user_id"]), window_hours)
    return jsonify(
        {
            "window_hours": window_hours,
            "count": len(alerts),
            "alerts": alerts,
        }
    )


@app.route("/api/gmaps/ops/health")
@subscription_required
def gmaps_ops_health():
    window_hours = _ops_safe_window_hours(request.args.get("hours", _OPS_METRICS_DEFAULT_WINDOW_HOURS, type=int))
    return jsonify(_ops_health_snapshot(int(session["user_id"]), window_hours))


@app.route("/api/gmaps/ops/dashboard")
@subscription_required
def gmaps_ops_dashboard():
    window_hours = _ops_safe_window_hours(request.args.get("hours", _OPS_METRICS_DEFAULT_WINDOW_HOURS, type=int))
    sessions = _list_persisted_sessions(int(session["user_id"]))
    running = 0
    completed = 0
    failed = 0
    for row in sessions:
        status = str(row.get("status") or "").upper()
        if status == "RUNNING":
            running += 1
        elif status == "COMPLETED":
            completed += 1
        elif status in {"FAILED", "PARTIAL", "STOPPED"}:
            failed += 1

    health = _ops_health_snapshot(int(session["user_id"]), window_hours)
    return jsonify(
        {
            "window_hours": window_hours,
            "summary": {
                "sessions_total": len(sessions),
                "sessions_running": running,
                "sessions_completed": completed,
                "sessions_failed_or_partial": failed,
            },
            "health": health,
            "stage_metrics": _ops_stage_metrics(int(session["user_id"]), window_hours),
            "recent_failures": _ops_recent_failures(int(session["user_id"]), window_hours, limit=25),
        }
    )


@app.route("/api/gmaps/sessions/<job_id>/diagnostics")
@subscription_required
def gmaps_session_diagnostics(job_id):
    state = get_job_state(job_id)
    if state and state.get("user_id") != session["user_id"]:
        return jsonify({"error": "Session not found."}), 404

    if not state:
        persisted = _load_persisted_session_state(job_id)
        if not persisted or persisted.get("user_id") != session["user_id"]:
            return jsonify({"error": "Session not found."}), 404
        state = persisted

    return jsonify(_session_diagnostics(job_id, state))


@app.route("/api/gmaps/extract/pause/<job_id>", methods=["POST"])
@subscription_required
def gmaps_pause_extraction(job_id):
    state = get_job_state(job_id)
    if not state:
        state = _load_persisted_session_state(job_id)
    if not state:
        return jsonify({"error": "Session not found."}), 404
    set_job_stop_requested(job_id, True)
    state["message"] = "Pause requested for extraction..."
    state["updated_at"] = datetime.utcnow().isoformat()
    _append_job_log(state, state["message"], state.get("progress", 0))
    _save_job_state_and_persist(job_id, state)
    _persist_partial_snapshot_checkpoint(state, reason="extract_pause_requested_endpoint")
    _persist_gmaps_event(
        state,
        "extract_pause_requested",
        "Extraction pause requested by user",
        payload={"trigger": "extract_pause_endpoint"},
    )
    return jsonify({"message": "Pause signal sent for extraction."})


# ============================================================
# Phase 2: Queue Admin API
# ============================================================

@app.route("/api/admin/queue-health")
@subscription_required
def api_queue_health():
    """Return current queue depths and Redis status for monitoring."""
    from jobs.queue import queue_health
    from jobs.store import _get_db

    # Include agent online count
    health = queue_health()
    try:
        db = _get_db()
        row = db.execute(
            "SELECT COUNT(*) FROM agents WHERE status IN ('online','busy')"
        ).fetchone()
        health["agents_online"] = row[0] if row else 0
    except Exception:
        health["agents_online"] = 0
    return jsonify(health)


@app.route("/api/agents/my-agent/status")
def api_my_agent_status():
    """Return current user's active agent status for the UI widget."""
    if "user_id" not in session:
        return jsonify({"status": "offline", "hostname": "", "agent_id": None})

    from agents.service import get_active_agent_for_user
    import json as _json
    agent = get_active_agent_for_user(session["user_id"])
    if not agent:
        return jsonify({"status": "offline", "hostname": "", "agent_id": None})

    return jsonify({
        "status": agent.get("status", "offline"),
        "hostname": agent.get("hostname", ""),
        "agent_id": agent.get("agent_id"),
        "version": agent.get("version", ""),
        "platform": agent.get("platform", ""),
        "capabilities": _json.loads(agent.get("capabilities") or "[]"),
        "last_seen_at": agent.get("last_seen_at"),
    })


@app.route("/api/jobs/<job_id>/status")
@subscription_required
def api_job_status(job_id):
    """Unified job status endpoint — works for all tool types."""
    qjob = _get_queue_job(job_id)
    if not qjob:
        return jsonify({"error": "Job not found."}), 404
    if qjob.get("user_id") != session["user_id"]:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(_queue_job_to_status(qjob, qjob.get("type", "")))


@app.route("/api/jobs/<job_id>/stop", methods=["POST"])
@subscription_required
def api_job_stop(job_id):
    """Unified stop endpoint for any queued job."""
    qjob = _get_queue_job(job_id)
    if not qjob:
        return jsonify({"error": "Job not found."}), 404
    if qjob.get("user_id") != session["user_id"]:
        return jsonify({"error": "Job not found."}), 404
    _set_redis_stop(job_id)
    return jsonify({"message": "Stop signal sent.", "job_id": job_id})


# ============================================================
# Email Outreach API
# ============================================================

@app.route("/api/email-outreach/scan-website", methods=["POST"])
@subscription_required
def api_scan_sender_website():
    """Scan the sender's website to extract services / description."""
    data = request.get_json()
    url = (data.get("url") or "").strip()
    if not url:
        return jsonify({"error": "URL is required."}), 400

    if not url.startswith(("http://", "https://")):
        url = "https://" + url

    try:
        import requests as ext_requests
        from bs4 import BeautifulSoup as BS

        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        resp = ext_requests.get(url, headers=headers, timeout=15, verify=False)
        soup = BS(resp.text, "lxml")

        # Extract title / company name
        company_name = ""
        title_tag = soup.find("title")
        if title_tag:
            company_name = title_tag.get_text(strip=True).split("|")[0].split("—")[0].split("-")[0].strip()

        # Extract meta description
        description = ""
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc:
            description = meta_desc.get("content", "").strip()

        # Extract services from headings, lists, and specific sections
        services = []
        # Look for service-related headings
        for heading in soup.find_all(["h1", "h2", "h3", "h4"]):
            text = heading.get_text(strip=True).lower()
            if any(kw in text for kw in ["service", "solution", "offer", "product", "feature",
                                          "what we do", "our work", "capabilities", "pricing"]):
                # Get sibling content
                parent = heading.find_parent(["section", "div"])
                if parent:
                    for li in parent.find_all("li"):
                        svc = li.get_text(strip=True)
                        if 3 < len(svc) < 100:
                            services.append(svc)
                    if not services:
                        for p in parent.find_all("p"):
                            svc = p.get_text(strip=True)
                            if 5 < len(svc) < 120:
                                services.append(svc)

        # Also try /services and /about pages
        for sub_path in ["/services", "/about"]:
            try:
                sub_resp = ext_requests.get(url.rstrip("/") + sub_path,
                                            headers=headers, timeout=10, verify=False)
                if sub_resp.status_code == 200:
                    sub_soup = BS(sub_resp.text, "lxml")
                    for li in sub_soup.find_all("li"):
                        svc = li.get_text(strip=True)
                        if 3 < len(svc) < 100 and svc not in services:
                            services.append(svc)
                    if not services:
                        for h in sub_soup.find_all(["h2", "h3", "h4"]):
                            svc = h.get_text(strip=True)
                            if 3 < len(svc) < 80 and svc not in services:
                                services.append(svc)
            except Exception:
                pass

        # Deduplicate and limit
        seen = set()
        unique_services = []
        for s in services:
            sl = s.lower().strip()
            if sl not in seen and len(sl) > 3:
                seen.add(sl)
                unique_services.append(s)
        services = unique_services[:15]

        # If no services found, try to extract from all page text
        if not services and description:
            services = [s.strip() for s in description.split(",") if 3 < len(s.strip()) < 60][:8]

        return jsonify({
            "company_name": company_name,
            "description": description,
            "services": services,
            "url": url,
        })
    except Exception as e:
        log.error(f"Website scan error: {e}")
        return jsonify({"error": f"Could not scan website: {str(e)}", "services": [], "description": ""}), 200


@app.route("/api/email-outreach/generate", methods=["POST"])
@subscription_required
def api_generate_email_templates():
    """
    Generate personalised email templates for a batch of leads.
    Uses rule-based template engine — no external AI API required.
    """
    data = request.get_json()
    sender = data.get("sender", {})
    leads = data.get("leads", [])

    if not leads:
        return jsonify({"error": "No leads provided."}), 400

    sender_name = sender.get("name", "there")
    sender_company = sender.get("company", "our company")
    sender_website = sender.get("website", "")
    sender_desc = sender.get("description", "")
    outreach_type = sender.get("outreach_type", "agency")
    website_scan = sender.get("website_scan") or {}
    scanned_services = website_scan.get("services", [])

    # Build a service summary
    if scanned_services:
        svc_text = ", ".join(scanned_services[:5])
    elif sender_desc:
        svc_text = sender_desc[:200]
    else:
        svc_text = "our professional services"

    templates = []
    uid = session["user_id"]
    db = get_db()

    for lead in leads:
        biz = lead.get("title") or lead.get("data", {}).get("business_name") or "your business"
        lead_email = lead.get("email", "")
        lead_location = lead.get("location", "")
        lead_keyword = lead.get("keyword", "")
        lead_website = lead.get("website", "")
        lead_phone = lead.get("phone", "")
        lead_id = lead.get("lead_id")
        lead_data = lead.get("data", {})

        # Build personalised template
        subject, body = _build_email_template(
            sender_name=sender_name,
            sender_company=sender_company,
            sender_website=sender_website,
            sender_desc=sender_desc,
            svc_text=svc_text,
            outreach_type=outreach_type,
            biz_name=biz,
            lead_email=lead_email,
            lead_location=lead_location,
            lead_keyword=lead_keyword,
            lead_website=lead_website,
            lead_data=lead_data,
        )

        tpl = {
            "business_name": biz,
            "email": lead_email,
            "subject": subject,
            "body": body,
            "location": lead_location,
            "keyword": lead_keyword,
        }
        templates.append(tpl)

        # Persist to DB
        try:
            db.execute(
                "INSERT INTO email_templates "
                "(user_id, lead_id, business_name, email, subject, body, keyword, location, sender_info) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (uid, lead_id, biz, lead_email, subject, body,
                 lead_keyword, lead_location, json.dumps(sender, default=str)),
            )
        except Exception as e:
            log.error(f"Template persist error: {e}")

    db.commit()

    return jsonify({"templates": templates, "count": len(templates)})


@app.route("/api/email-outreach/templates")
@login_required
def api_list_email_templates():
    """List saved email templates for the current user."""
    uid = session["user_id"]
    db = get_db()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    offset = (page - 1) * per_page

    total = db.execute(
        "SELECT COUNT(*) FROM email_templates WHERE user_id=?", (uid,)
    ).fetchone()[0]

    rows = db.execute(
        "SELECT * FROM email_templates WHERE user_id=? ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (uid, per_page, offset),
    ).fetchall()

    templates = []
    for r in rows:
        templates.append({
            "id": r["id"],
            "lead_id": r["lead_id"],
            "business_name": r["business_name"],
            "email": r["email"],
            "subject": r["subject"],
            "body": r["body"],
            "keyword": r["keyword"],
            "location": r["location"],
            "created_at": r["created_at"],
        })

    return jsonify({"templates": templates, "total": total, "page": page})


def _build_email_template(*, sender_name, sender_company, sender_website,
                           sender_desc, svc_text, outreach_type,
                           biz_name, lead_email, lead_location, lead_keyword,
                           lead_website, lead_data):
    """
    Build a personalised cold outreach email using rule-based templates.
    Returns (subject, body).
    """
    import random as _rng

    # --- Location / niche personalisation tokens ---
    loc_phrase = f" in {lead_location}" if lead_location else ""
    niche_phrase = lead_keyword or "your industry"
    biz_short = biz_name if biz_name != "your business" else "your company"
    website_line = f"\nYou can learn more about what we do at {sender_website}" if sender_website else ""

    # --- Subject line variants (randomly picked for variety) ---
    subjects_agency = [
        f"Quick idea for {biz_short}",
        f"Helping {niche_phrase} businesses{loc_phrase} grow",
        f"Partnership opportunity for {biz_short}",
        f"{sender_company} × {biz_short} — a quick thought",
        f"Grow {biz_short} with proven {niche_phrase} strategies",
    ]
    subjects_saas = [
        f"A tool built for {niche_phrase} businesses like {biz_short}",
        f"Save hours every week at {biz_short}",
        f"Quick demo for {biz_short}?",
        f"{sender_company} for {niche_phrase} businesses{loc_phrase}",
        f"Automate and scale {biz_short}",
    ]
    subjects_freelance = [
        f"Can I help {biz_short} with {niche_phrase}?",
        f"Freelance {niche_phrase} expert — quick intro",
        f"Let's work together, {biz_short}",
        f"Ideas for {biz_short}{loc_phrase}",
    ]
    subjects_consulting = [
        f"Strategic growth ideas for {biz_short}",
        f"Consulting opportunity — {biz_short}",
        f"Unlock growth for {biz_short}{loc_phrase}",
        f"{niche_phrase} insights for {biz_short}",
    ]

    type_subjects = {
        "agency": subjects_agency,
        "saas": subjects_saas,
        "freelance": subjects_freelance,
        "consulting": subjects_consulting,
    }
    subject = _rng.choice(type_subjects.get(outreach_type, subjects_agency))

    # --- Body templates ---
    # Agency body
    if outreach_type == "agency":
        body = (
            f"Hi {{first_contact}},\n\n"
            f"I came across {biz_short}{loc_phrase} and was impressed by what you've built in the {niche_phrase} space.\n\n"
            f"I'm {sender_name} from {sender_company}. We specialise in {svc_text}, "
            f"and we've helped other {niche_phrase} businesses{loc_phrase} increase their online presence and generate more leads.\n\n"
            f"I had a few ideas specifically for {biz_short} that I think could make a real impact — "
            f"would you be open to a quick 10-minute call this week?"
            f"{website_line}\n\n"
            f"Looking forward to connecting.\n\n"
            f"Best regards,\n{sender_name}\n{sender_company}"
        )
    elif outreach_type == "saas":
        body = (
            f"Hi {{first_contact}},\n\n"
            f"I noticed {biz_short}{loc_phrase} is doing great work in {niche_phrase}. "
            f"I wanted to share a tool we've built at {sender_company} that's helping similar businesses save time and scale faster.\n\n"
            f"In short, {svc_text}.\n\n"
            f"Businesses like yours{loc_phrase} are already using it to streamline operations and boost results. "
            f"I'd love to offer you a free demo or trial so you can see the value first-hand."
            f"{website_line}\n\n"
            f"Would you be interested in a quick walkthrough?\n\n"
            f"Cheers,\n{sender_name}\n{sender_company}"
        )
    elif outreach_type == "freelance":
        body = (
            f"Hi {{first_contact}},\n\n"
            f"I found {biz_short}{loc_phrase} while researching {niche_phrase} businesses, "
            f"and I think there's a great opportunity to enhance what you're already doing really well.\n\n"
            f"I'm {sender_name}, a freelance specialist in {svc_text}. "
            f"I've worked with a number of {niche_phrase} businesses and consistently delivered measurable results.\n\n"
            f"I'd love to share a couple of tailored ideas for {biz_short} — no strings attached. "
            f"Would you be open to a brief chat?"
            f"{website_line}\n\n"
            f"Best,\n{sender_name}"
        )
    else:  # consulting
        body = (
            f"Hi {{first_contact}},\n\n"
            f"I've been studying the {niche_phrase} landscape{loc_phrase} and {biz_short} stood out "
            f"as a business with strong potential for accelerated growth.\n\n"
            f"At {sender_company}, we provide strategic consulting in {svc_text}. "
            f"We've helped businesses similar to yours unlock new revenue streams and optimise operations.\n\n"
            f"I'd welcome the chance to share a few actionable insights tailored to {biz_short}. "
            f"Could we schedule a short call this week?"
            f"{website_line}\n\n"
            f"Warm regards,\n{sender_name}\n{sender_company}"
        )

    # Replace {first_contact} placeholder with a best-guess first name
    contact_name = ""
    if lead_data:
        contact_name = (
            lead_data.get("owner_name") or lead_data.get("name")
            or lead_data.get("display_name") or lead_data.get("contact_name") or ""
        )
    if contact_name:
        first_name = contact_name.strip().split()[0]
    else:
        first_name = "there"
    body = body.replace("{first_contact}", first_name)

    return subject, body


# ============================================================
# Lead Database API
# ============================================================

@app.route("/api/leads")
@login_required
def api_leads():
    """Query leads with filtering + pagination."""
    uid = session["user_id"]
    db = get_db()

    # Filters
    tool = request.args.get("tool", "").strip()
    keyword = request.args.get("keyword", "").strip()
    location = request.args.get("location", "").strip()
    quality = request.args.get("quality", "").strip()
    search = request.args.get("search", "").strip()
    scrape_id = request.args.get("scrape_id", "", type=str).strip()
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 50, type=int)
    per_page = min(per_page, 200)
    offset = (page - 1) * per_page

    where = ["user_id = ?"]
    params = [uid]

    if tool:
        where.append("tool = ?")
        params.append(tool)
    if keyword:
        where.append("keyword LIKE ?")
        params.append(f"%{keyword}%")
    if location:
        where.append("location LIKE ?")
        params.append(f"%{location}%")
    if quality:
        where.append("quality = ?")
        params.append(quality)
    if scrape_id:
        where.append("scrape_id = ?")
        params.append(int(scrape_id))
    if search:
        where.append("(title LIKE ? OR email LIKE ? OR phone LIKE ? OR website LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s, s])

    where_clause = " AND ".join(where)

    total = db.execute(
        f"SELECT COUNT(*) FROM leads WHERE {where_clause}", params
    ).fetchone()[0]

    rows = db.execute(
        f"SELECT * FROM leads WHERE {where_clause} ORDER BY created_at DESC LIMIT ? OFFSET ?",
        params + [per_page, offset],
    ).fetchall()

    leads = []
    for r in rows:
        leads.append({
            "id": r["id"],
            "scrape_id": r["scrape_id"],
            "tool": r["tool"],
            "keyword": r["keyword"],
            "location": r["location"],
            "title": r["title"],
            "email": r["email"],
            "phone": r["phone"],
            "website": r["website"],
            "quality": r["quality"],
            "data": json.loads(r["data"]) if r["data"] else {},
            "created_at": r["created_at"],
        })

    return jsonify({
        "leads": leads,
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page,
    })


@app.route("/api/leads/filters")
@login_required
def api_leads_filters():
    """Return distinct filter values for the current user's leads."""
    uid = session["user_id"]
    db = get_db()

    tools = [r[0] for r in db.execute(
        "SELECT DISTINCT tool FROM leads WHERE user_id=? ORDER BY tool", (uid,)
    ).fetchall()]
    keywords = [r[0] for r in db.execute(
        "SELECT DISTINCT keyword FROM leads WHERE user_id=? AND keyword!='' ORDER BY keyword", (uid,)
    ).fetchall()]
    locations = [r[0] for r in db.execute(
        "SELECT DISTINCT location FROM leads WHERE user_id=? AND location!='' ORDER BY location", (uid,)
    ).fetchall()]

    return jsonify({"tools": tools, "keywords": keywords, "locations": locations})


@app.route("/api/leads/export")
@login_required
def api_leads_export():
    """Export filtered leads as CSV."""
    uid = session["user_id"]
    db = get_db()

    tool = request.args.get("tool", "").strip()
    keyword = request.args.get("keyword", "").strip()
    location = request.args.get("location", "").strip()
    quality = request.args.get("quality", "").strip()
    scrape_id = request.args.get("scrape_id", "").strip()
    search = request.args.get("search", "").strip()

    where = ["user_id = ?"]
    params = [uid]

    if tool:
        where.append("tool = ?")
        params.append(tool)
    if keyword:
        where.append("keyword LIKE ?")
        params.append(f"%{keyword}%")
    if location:
        where.append("location LIKE ?")
        params.append(f"%{location}%")
    if quality:
        where.append("quality = ?")
        params.append(quality)
    if scrape_id:
        where.append("scrape_id = ?")
        params.append(int(scrape_id))
    if search:
        where.append("(title LIKE ? OR email LIKE ? OR phone LIKE ? OR website LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s, s])

    where_clause = " AND ".join(where)

    rows = db.execute(
        f"SELECT * FROM leads WHERE {where_clause} ORDER BY created_at DESC",
        params,
    ).fetchall()

    if not rows:
        return jsonify({"error": "No leads to export."}), 404

    output = io.StringIO()
    fieldnames = ["Title", "Email", "Phone", "Website", "Tool", "Keyword",
                  "Location", "Quality", "Date"]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for r in rows:
        writer.writerow({
            "Title": r["title"],
            "Email": r["email"],
            "Phone": r["phone"],
            "Website": r["website"],
            "Tool": r["tool"],
            "Keyword": r["keyword"],
            "Location": r["location"],
            "Quality": r["quality"],
            "Date": r["created_at"],
        })

    output.seek(0)
    filename = f"leadgen_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        output.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/leads/<int:lead_id>", methods=["DELETE"])
@login_required
def api_lead_delete(lead_id):
    """Delete a single lead."""
    uid = session["user_id"]
    db = get_db()
    result = db.execute("DELETE FROM leads WHERE id=? AND user_id=?", (lead_id, uid))
    db.commit()
    if result.rowcount == 0:
        return jsonify({"error": "Lead not found."}), 404
    return jsonify({"message": "Lead deleted."})


@app.route("/api/leads/bulk-delete", methods=["POST"])
@login_required
def api_leads_bulk_delete():
    """Delete multiple leads by ID."""
    uid = session["user_id"]
    data = request.get_json()
    ids = data.get("ids", [])
    if not ids:
        return jsonify({"error": "No IDs provided."}), 400

    db = get_db()
    placeholders = ",".join("?" * len(ids))
    db.execute(
        f"DELETE FROM leads WHERE user_id=? AND id IN ({placeholders})",
        [uid] + list(ids),
    )
    db.commit()
    return jsonify({"message": f"Deleted {len(ids)} leads."})


@app.route("/api/leads/cleanup", methods=["POST"])
@login_required
def api_leads_cleanup():
    """Remove duplicate and/or outlier leads for the current user."""
    uid = session["user_id"]
    data = request.get_json(silent=True) or {}
    mode = str(data.get("mode", "both")).strip().lower()
    if mode not in ("duplicates", "outliers", "both"):
        return jsonify({"error": "mode must be duplicates, outliers, or both."}), 400

    db = get_db()
    duplicates_removed = 0
    outliers_removed = 0

    if mode in ("duplicates", "both"):
        dup_ids = [
            row["id"] for row in db.execute(
                """
                SELECT l1.id
                FROM leads l1
                JOIN leads l2
                  ON l1.user_id = l2.user_id
                 AND l1.id > l2.id
                 AND lower(trim(COALESCE(l1.title, ''))) = lower(trim(COALESCE(l2.title, '')))
                 AND lower(trim(COALESCE(l1.email, ''))) = lower(trim(COALESCE(l2.email, '')))
                 AND lower(trim(COALESCE(l1.phone, ''))) = lower(trim(COALESCE(l2.phone, '')))
                 AND lower(trim(COALESCE(l1.website, ''))) = lower(trim(COALESCE(l2.website, '')))
                WHERE l1.user_id = ?
                  AND trim(COALESCE(l1.title, '')) != ''
                """,
                (uid,),
            ).fetchall()
        ]
        if dup_ids:
            placeholders = ",".join("?" for _ in dup_ids)
            db.execute(
                f"DELETE FROM leads WHERE user_id=? AND id IN ({placeholders})",
                [uid] + dup_ids,
            )
            duplicates_removed = len(dup_ids)

    if mode in ("outliers", "both"):
        outlier_ids = [
            row["id"] for row in db.execute(
                """
                SELECT id FROM leads
                WHERE user_id=?
                  AND (
                    trim(COALESCE(title,'')) = ''
                    OR (
                      length(trim(COALESCE(title,''))) < 3
                      AND trim(COALESCE(email,'')) = ''
                      AND trim(COALESCE(phone,'')) = ''
                      AND trim(COALESCE(website,'')) = ''
                    )
                  )
                """,
                (uid,),
            ).fetchall()
        ]
        if outlier_ids:
            placeholders = ",".join("?" for _ in outlier_ids)
            db.execute(
                f"DELETE FROM leads WHERE user_id=? AND id IN ({placeholders})",
                [uid] + outlier_ids,
            )
            outliers_removed = len(outlier_ids)

    db.commit()
    return jsonify({
        "message": "Cleanup complete.",
        "duplicates_removed": duplicates_removed,
        "outliers_removed": outliers_removed,
        "total_removed": duplicates_removed + outliers_removed,
    })


@app.route("/api/leads/stats")
@login_required
def api_leads_stats():
    """Quick stats for the leads database page."""
    uid = session["user_id"]
    db = get_db()

    total = db.execute("SELECT COUNT(*) FROM leads WHERE user_id=?", (uid,)).fetchone()[0]
    with_email = db.execute(
        "SELECT COUNT(*) FROM leads WHERE user_id=? AND email!='' AND email IS NOT NULL", (uid,)
    ).fetchone()[0]
    with_phone = db.execute(
        "SELECT COUNT(*) FROM leads WHERE user_id=? AND phone!='' AND phone IS NOT NULL", (uid,)
    ).fetchone()[0]

    quality_rows = db.execute(
        "SELECT quality, COUNT(*) as cnt FROM leads WHERE user_id=? GROUP BY quality", (uid,)
    ).fetchall()
    quality = {r["quality"]: r["cnt"] for r in quality_rows}

    tool_rows = db.execute(
        "SELECT tool, COUNT(*) as cnt FROM leads WHERE user_id=? GROUP BY tool", (uid,)
    ).fetchall()
    by_tool = {r["tool"]: r["cnt"] for r in tool_rows}

    return jsonify({
        "total": total,
        "with_email": with_email,
        "with_phone": with_phone,
        "quality": quality,
        "by_tool": by_tool,
    })


# ============================================================
# Account Settings API
# ============================================================

@app.route("/api/account/profile", methods=["PUT"])
@login_required
def api_account_profile():
    """Update user profile (name, email)."""
    uid = session["user_id"]
    data = request.get_json()
    full_name = (data.get("full_name") or "").strip()
    email = (data.get("email") or "").strip().lower()

    if not email:
        return jsonify({"error": "Email is required."}), 400

    db = get_db()
    # Check if email is taken by another user
    existing = db.execute(
        "SELECT id FROM users WHERE email=? AND id!=?", (email, uid)
    ).fetchone()
    if existing:
        return jsonify({"error": "That email is already in use."}), 409

    db.execute(
        "UPDATE users SET full_name=?, email=? WHERE id=?",
        (full_name, email, uid),
    )
    db.commit()
    session["email"] = email
    return jsonify({"message": "Profile updated."})


@app.route("/api/account/password", methods=["PUT"])
@login_required
def api_account_password():
    """Change password."""
    uid = session["user_id"]
    data = request.get_json()
    current_pw = (data.get("current_password") or "").strip()
    new_pw = (data.get("new_password") or "").strip()

    if not current_pw or not new_pw:
        return jsonify({"error": "Both current and new password are required."}), 400
    if len(new_pw) < 6:
        return jsonify({"error": "New password must be at least 6 characters."}), 400

    db = get_db()
    user = db.execute("SELECT password FROM users WHERE id=?", (uid,)).fetchone()
    if not user or not _verify_password(current_pw, user["password"]):
        return jsonify({"error": "Current password is incorrect."}), 401

    pw_err = _validate_password_strength(new_pw)
    if pw_err:
        return jsonify({"error": pw_err}), 400

    db.execute("UPDATE users SET password=? WHERE id=?", (_hash_password(new_pw), uid))
    db.commit()
    return jsonify({"message": "Password changed successfully."})


@app.route("/api/account/delete", methods=["DELETE"])
@login_required
def api_account_delete():
    """Delete user account and all associated data."""
    uid = session["user_id"]
    db = get_db()
    db.execute("DELETE FROM leads WHERE user_id=?", (uid,))
    db.execute("DELETE FROM scrape_history WHERE user_id=?", (uid,))
    db.execute("DELETE FROM users WHERE id=?", (uid,))
    db.commit()
    session.clear()
    return jsonify({"message": "Account deleted."})


# ============================================================
# Database & Settings page routes
# ============================================================

@app.route("/database")
@login_required
def database_page():
    """Lead database page."""
    user = current_user()
    if user and not user["is_active"]:
        return redirect(url_for("activate_page"))
    scrape_id = request.args.get("scrape_id", "")
    return render_template("database.html", active_page="database", scrape_id=scrape_id)


@app.route("/settings")
@login_required
def settings_page():
    """Account settings page."""
    return render_template("settings.html", active_page="settings")


# ============================================================
# Security headers
# ============================================================

@app.after_request
def set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    if os.environ.get("FLASK_ENV") == "production":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# ============================================================
# Health check (for DigitalOcean App Platform / load balancers)
# ============================================================

@app.route("/health")
@limiter.exempt
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()}), 200


@app.route("/health/ops")
@limiter.exempt
def ops_health_check():
    alerts = []
    try:
        alerts = _ops_alerts(0, _OPS_METRICS_DEFAULT_WINDOW_HOURS)
    except Exception:
        alerts = []

    has_critical = any(str(a.get("severity") or "").lower() == "critical" for a in alerts)
    status = "unhealthy" if has_critical else ("degraded" if alerts else "healthy")
    code = 503 if has_critical else 200
    return jsonify({"status": status, "alerts_count": len(alerts), "timestamp": datetime.now().isoformat()}), code


if __name__ == "__main__":
    # Start Phase 5 background scheduler (campaign sender + workflow engine)
    try:
        from workflows.scheduler import start_scheduler
        start_scheduler()
    except Exception as _sch_exc:
        log.warning(f"Phase 5 scheduler (non-fatal): {_sch_exc}")
    app.run(debug=True, port=5000)
