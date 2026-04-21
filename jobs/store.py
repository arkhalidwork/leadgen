"""
LeadGen — Job Store (DB Operations)

Unified CRUD for the `jobs` table. Works with both SQLite and Postgres.
This is the single source of truth for job state.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local = threading.local()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_db() -> sqlite3.Connection:
    """Thread-local SQLite connection."""
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
        _local.db.execute("PRAGMA busy_timeout=5000")
    return _local.db


# ── Schema ──

JOBS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    type            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'queued',
    priority        INTEGER DEFAULT 0,

    -- Input
    payload         TEXT NOT NULL DEFAULT '{}',

    -- Progress
    progress        INTEGER DEFAULT 0,
    message         TEXT DEFAULT '',
    phase           TEXT DEFAULT '',
    phase_detail    TEXT DEFAULT '',

    -- Output
    result          TEXT DEFAULT '{}',
    result_count    INTEGER DEFAULT 0,
    error           TEXT DEFAULT '',

    -- Retry
    attempt         INTEGER DEFAULT 1,
    max_attempts    INTEGER DEFAULT 3,
    last_error      TEXT DEFAULT '',

    -- Timing
    created_at      TEXT NOT NULL DEFAULT (datetime('now')),
    started_at      TEXT,
    updated_at      TEXT DEFAULT (datetime('now')),
    finished_at     TEXT,

    -- Worker tracking (Phase 2)
    worker_id       TEXT DEFAULT '',
    heartbeat_at    TEXT,

    -- Agent routing (Phase 3)
    execution_mode      TEXT DEFAULT 'auto',
    agent_id            TEXT DEFAULT '',
    execution_lease_at  TEXT,
    checkpoint_seq      INTEGER DEFAULT 0,

    FOREIGN KEY (user_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_jobs_user_status   ON jobs(user_id, status);
CREATE INDEX IF NOT EXISTS idx_jobs_status        ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_type_status   ON jobs(type, status);
CREATE INDEX IF NOT EXISTS idx_jobs_heartbeat     ON jobs(status, heartbeat_at);
CREATE INDEX IF NOT EXISTS idx_jobs_created       ON jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_jobs_agent         ON jobs(agent_id, status);
"""


AGENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS agents (
    agent_id        TEXT PRIMARY KEY,
    user_id         INTEGER NOT NULL,
    hostname        TEXT DEFAULT '',
    platform        TEXT DEFAULT '',
    capabilities    TEXT DEFAULT '[]',
    max_concurrent  INTEGER DEFAULT 1,
    status          TEXT DEFAULT 'offline',
    last_seen_at    TEXT,
    version         TEXT DEFAULT '',
    cpu_pct         REAL DEFAULT 0,
    ram_available_gb REAL DEFAULT 0,
    registered_at   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY (user_id) REFERENCES users(id)
);
CREATE INDEX IF NOT EXISTS idx_agents_user_status ON agents(user_id, status);
CREATE INDEX IF NOT EXISTS idx_agents_last_seen   ON agents(last_seen_at);
"""


CHECKPOINTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS checkpoints (
    job_id      TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    phase       TEXT DEFAULT '',
    data        TEXT DEFAULT '{}',
    leads_count INTEGER DEFAULT 0,
    created_at  TEXT NOT NULL,
    PRIMARY KEY (job_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_ckpt_job_seq ON checkpoints(job_id, seq DESC);
"""


# Phase 3 column migrations (additive — safe on existing DB)
_PHASE3_JOB_MIGRATIONS = [
    "ALTER TABLE jobs ADD COLUMN execution_mode TEXT DEFAULT 'auto'",
    "ALTER TABLE jobs ADD COLUMN agent_id TEXT DEFAULT ''",
    "ALTER TABLE jobs ADD COLUMN execution_lease_at TEXT",
    "ALTER TABLE jobs ADD COLUMN checkpoint_seq INTEGER DEFAULT 0",
]

# Phase 4: Intelligence Layer tables
INTELLIGENCE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS lead_core (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id         INTEGER NOT NULL,
    canonical_name  TEXT NOT NULL,
    category        TEXT DEFAULT '',
    address         TEXT DEFAULT '',
    city            TEXT DEFAULT '',
    country         TEXT DEFAULT '',
    latitude        REAL,
    longitude       REAL,
    geo_hash        TEXT DEFAULT '',
    merge_status    TEXT DEFAULT 'single',
    merged_into_id  INTEGER REFERENCES lead_core(id),
    source_count    INTEGER DEFAULT 1,
    first_seen_at   TEXT DEFAULT (datetime('now')),
    last_seen_at    TEXT DEFAULT (datetime('now')),
    created_at      TEXT DEFAULT (datetime('now')),
    updated_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_lead_core_user      ON lead_core(user_id);
CREATE INDEX IF NOT EXISTS idx_lead_core_category  ON lead_core(user_id, category);
CREATE INDEX IF NOT EXISTS idx_lead_core_geo       ON lead_core(geo_hash);
CREATE INDEX IF NOT EXISTS idx_lead_core_name      ON lead_core(user_id, lower(canonical_name));

CREATE TABLE IF NOT EXISTS lead_sources (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id     INTEGER NOT NULL REFERENCES lead_core(id) ON DELETE CASCADE,
    user_id     INTEGER NOT NULL,
    job_id      TEXT NOT NULL,
    source      TEXT NOT NULL,
    source_url  TEXT DEFAULT '',
    raw_name    TEXT DEFAULT '',
    raw_phone   TEXT DEFAULT '',
    raw_website TEXT DEFAULT '',
    raw_email   TEXT DEFAULT '',
    raw_address TEXT DEFAULT '',
    raw_rating  REAL,
    raw_reviews INTEGER,
    raw_data    TEXT DEFAULT '{}',
    scraped_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(lead_id, job_id, source)
);
CREATE INDEX IF NOT EXISTS idx_lead_sources_lead   ON lead_sources(lead_id);
CREATE INDEX IF NOT EXISTS idx_lead_sources_job    ON lead_sources(job_id);

CREATE TABLE IF NOT EXISTS lead_enrichment (
    lead_id              INTEGER PRIMARY KEY REFERENCES lead_core(id) ON DELETE CASCADE,
    phone                TEXT DEFAULT '',
    phone_alt            TEXT DEFAULT '',
    email                TEXT DEFAULT '',
    email_alt            TEXT DEFAULT '',
    website              TEXT DEFAULT '',
    domain               TEXT DEFAULT '',
    instagram_url        TEXT DEFAULT '',
    linkedin_url         TEXT DEFAULT '',
    facebook_url         TEXT DEFAULT '',
    twitter_url          TEXT DEFAULT '',
    youtube_url          TEXT DEFAULT '',
    google_rating        REAL,
    google_reviews       INTEGER,
    instagram_followers  INTEGER,
    instagram_posts      INTEGER,
    updated_at           TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS lead_signals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id      INTEGER NOT NULL REFERENCES lead_core(id) ON DELETE CASCADE,
    signal_type  TEXT NOT NULL,
    confidence   REAL NOT NULL DEFAULT 1.0,
    value        TEXT DEFAULT '',
    source       TEXT DEFAULT '',
    detected_at  TEXT DEFAULT (datetime('now')),
    expires_at   TEXT,
    UNIQUE(lead_id, signal_type, source)
);
CREATE INDEX IF NOT EXISTS idx_signals_lead    ON lead_signals(lead_id);
CREATE INDEX IF NOT EXISTS idx_signals_type    ON lead_signals(signal_type);

CREATE TABLE IF NOT EXISTS lead_scores (
    lead_id             INTEGER PRIMARY KEY REFERENCES lead_core(id) ON DELETE CASCADE,
    total_score         REAL DEFAULT 0,
    tier                TEXT DEFAULT 'cold',
    completeness_score  REAL DEFAULT 0,
    social_score        REAL DEFAULT 0,
    activity_score      REAL DEFAULT 0,
    sentiment_score     REAL DEFAULT 0,
    freshness_score     REAL DEFAULT 0,
    scored_at           TEXT DEFAULT (datetime('now')),
    score_version       INTEGER DEFAULT 1
);
CREATE INDEX IF NOT EXISTS idx_scores_total ON lead_scores(total_score DESC);
CREATE INDEX IF NOT EXISTS idx_scores_tier  ON lead_scores(tier);

CREATE TABLE IF NOT EXISTS lead_insights (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id          INTEGER NOT NULL REFERENCES lead_core(id) ON DELETE CASCADE,
    summary          TEXT DEFAULT '',
    strengths        TEXT DEFAULT '[]',
    weaknesses       TEXT DEFAULT '[]',
    outreach_angles  TEXT DEFAULT '[]',
    next_action      TEXT DEFAULT '',
    generated_by     TEXT DEFAULT 'rules',
    model_version    TEXT DEFAULT 'v2',
    generated_at     TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_insights_lead ON lead_insights(lead_id);

CREATE TABLE IF NOT EXISTS merge_proposals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    lead_id         INTEGER NOT NULL REFERENCES lead_core(id),
    proposed_name   TEXT DEFAULT '',
    proposed_phone  TEXT DEFAULT '',
    proposed_domain TEXT DEFAULT '',
    confidence      REAL NOT NULL,
    status          TEXT DEFAULT 'pending',
    raw_data        TEXT DEFAULT '{}',
    created_at      TEXT DEFAULT (datetime('now'))
);
"""

# Phase 5: CRM / Outreach / Workflow tables
PHASE5_TABLES_SQL = """
-- ── CRM Pipeline Stages ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_stages (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    name        TEXT NOT NULL,
    color       TEXT DEFAULT '#6366f1',
    icon        TEXT DEFAULT 'bi-circle',
    position    INTEGER NOT NULL DEFAULT 0,
    is_terminal INTEGER DEFAULT 0,
    is_winning  INTEGER DEFAULT 0,
    created_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_stages_user ON pipeline_stages(user_id, position);

-- ── CRM Pipeline Items ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_items (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id          INTEGER NOT NULL,
    lead_id          INTEGER REFERENCES lead_core(id) ON DELETE SET NULL,
    stage_id         INTEGER NOT NULL REFERENCES pipeline_stages(id),
    lead_name        TEXT DEFAULT '',
    lead_email       TEXT DEFAULT '',
    lead_phone       TEXT DEFAULT '',
    lead_score       REAL DEFAULT 0,
    lead_tier        TEXT DEFAULT 'cold',
    deal_value       REAL DEFAULT 0,
    deal_currency    TEXT DEFAULT 'USD',
    priority         INTEGER DEFAULT 0,
    status           TEXT DEFAULT 'active',
    source           TEXT DEFAULT 'manual',
    position_order   REAL DEFAULT 0,
    added_at         TEXT DEFAULT (datetime('now')),
    last_activity_at TEXT DEFAULT (datetime('now')),
    converted_at     TEXT,
    UNIQUE(user_id, lead_id)
);
CREATE INDEX IF NOT EXISTS idx_pipeline_items_user_stage ON pipeline_items(user_id, stage_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_items_lead       ON pipeline_items(lead_id);

-- ── CRM Activity Log ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS activity_log (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id          INTEGER NOT NULL,
    pipeline_item_id INTEGER REFERENCES pipeline_items(id) ON DELETE CASCADE,
    lead_id          INTEGER REFERENCES lead_core(id),
    activity_type    TEXT NOT NULL,
    title            TEXT DEFAULT '',
    body             TEXT DEFAULT '',
    meta             TEXT DEFAULT '{}',
    created_by       INTEGER,
    created_at       TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_activity_item ON activity_log(pipeline_item_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_lead ON activity_log(lead_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_user ON activity_log(user_id, created_at DESC);

-- ── Outreach Campaigns ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS campaigns (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id          INTEGER NOT NULL,
    name             TEXT NOT NULL,
    description      TEXT DEFAULT '',
    status           TEXT DEFAULT 'draft',
    from_email       TEXT DEFAULT '',
    from_name        TEXT DEFAULT '',
    reply_to         TEXT DEFAULT '',
    daily_send_limit INTEGER DEFAULT 50,
    timezone         TEXT DEFAULT 'UTC',
    created_at       TEXT DEFAULT (datetime('now')),
    updated_at       TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_campaigns_user ON campaigns(user_id, status);

-- ── Campaign Sequences (steps) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS campaign_sequences (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id     INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
    step_number     INTEGER NOT NULL,
    delay_days      INTEGER DEFAULT 0,
    subject         TEXT DEFAULT '',
    body_html       TEXT DEFAULT '',
    body_text       TEXT DEFAULT '',
    is_ai           INTEGER DEFAULT 0,
    tone            TEXT DEFAULT 'professional',
    skip_if_replied INTEGER DEFAULT 1,
    skip_if_opened  INTEGER DEFAULT 0,
    UNIQUE(campaign_id, step_number)
);

-- ── Campaign Leads (enrollment) ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS campaign_leads (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_id      INTEGER NOT NULL REFERENCES campaigns(id),
    lead_id          INTEGER NOT NULL REFERENCES lead_core(id),
    pipeline_item_id INTEGER REFERENCES pipeline_items(id),
    status           TEXT DEFAULT 'enrolled',
    current_step     INTEGER DEFAULT 0,
    next_send_at     TEXT,
    last_sent_at     TEXT,
    enrolled_at      TEXT DEFAULT (datetime('now')),
    completed_at     TEXT,
    emails_sent      INTEGER DEFAULT 0,
    opens            INTEGER DEFAULT 0,
    clicks           INTEGER DEFAULT 0,
    replies          INTEGER DEFAULT 0,
    UNIQUE(campaign_id, lead_id)
);
CREATE INDEX IF NOT EXISTS idx_cl_next_send ON campaign_leads(next_send_at, status);
CREATE INDEX IF NOT EXISTS idx_cl_campaign  ON campaign_leads(campaign_id, status);

-- ── Outreach Events (tracking) ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS outreach_events (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    campaign_lead_id INTEGER NOT NULL REFERENCES campaign_leads(id),
    campaign_id      INTEGER NOT NULL,
    lead_id          INTEGER NOT NULL,
    sequence_step    INTEGER NOT NULL,
    event_type       TEXT NOT NULL,
    email_subject    TEXT DEFAULT '',
    message_id       TEXT DEFAULT '',
    meta             TEXT DEFAULT '{}',
    occurred_at      TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_oe_lead     ON outreach_events(lead_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_oe_campaign ON outreach_events(campaign_id, event_type);
CREATE INDEX IF NOT EXISTS idx_oe_msg_id   ON outreach_events(message_id);

-- ── SMTP Config (per user) ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_smtp_config (
    user_id     INTEGER PRIMARY KEY REFERENCES users(id),
    provider    TEXT DEFAULT 'smtp',
    smtp_host   TEXT DEFAULT '',
    smtp_port   INTEGER DEFAULT 587,
    smtp_user   TEXT DEFAULT '',
    smtp_pass   TEXT DEFAULT '',
    api_key     TEXT DEFAULT '',
    is_verified INTEGER DEFAULT 0,
    updated_at  TEXT DEFAULT (datetime('now'))
);

-- ── Workflows ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workflows (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    name        TEXT NOT NULL,
    description TEXT DEFAULT '',
    status      TEXT DEFAULT 'active',
    run_count   INTEGER DEFAULT 0,
    last_run_at TEXT,
    created_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_workflows_user ON workflows(user_id, status);

-- ── Workflow Triggers ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workflow_triggers (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id  INTEGER NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    trigger_type TEXT NOT NULL,
    config       TEXT DEFAULT '{}',
    next_run_at  TEXT
);

-- ── Workflow Actions ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workflow_actions (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id  INTEGER NOT NULL REFERENCES workflows(id) ON DELETE CASCADE,
    step_order   INTEGER NOT NULL,
    action_type  TEXT NOT NULL,
    config       TEXT DEFAULT '{}'
);

-- ── Workflow Runs ──────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workflow_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    workflow_id     INTEGER NOT NULL REFERENCES workflows(id),
    trigger_event   TEXT DEFAULT '{}',
    status          TEXT DEFAULT 'running',
    actions_total   INTEGER DEFAULT 0,
    actions_done    INTEGER DEFAULT 0,
    error           TEXT DEFAULT '',
    started_at      TEXT DEFAULT (datetime('now')),
    finished_at     TEXT,
    idempotency_key TEXT UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_wf_runs_workflow ON workflow_runs(workflow_id, started_at DESC);

-- ── Workflow Run Logs ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workflow_run_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      INTEGER NOT NULL REFERENCES workflow_runs(id),
    step_order  INTEGER,
    action_type TEXT,
    status      TEXT,
    output      TEXT DEFAULT '{}',
    logged_at   TEXT DEFAULT (datetime('now'))
);

-- ── Workflow Event Queue ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS workflow_event_queue (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    event_type  TEXT NOT NULL,
    payload     TEXT DEFAULT '{}',
    processed   INTEGER DEFAULT 0,
    created_at  TEXT DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_wf_events_pending ON workflow_event_queue(processed, created_at ASC);
"""


def ensure_jobs_table() -> None:
    """Create the jobs, agents, checkpoints, and intelligence tables if they don't exist."""
    try:
        db = _get_db()
        db.executescript(JOBS_TABLE_SQL)
        db.executescript(AGENTS_TABLE_SQL)
        db.executescript(CHECKPOINTS_TABLE_SQL)
        db.executescript(INTELLIGENCE_TABLES_SQL)
        db.executescript(PHASE5_TABLES_SQL)
        # Phase 3 migrations (safe to run on existing DB)
        for stmt in _PHASE3_JOB_MIGRATIONS:
            try:
                db.execute(stmt)
            except Exception:
                pass  # Column already exists
        db.commit()
        log.info("Jobs/Agents/Checkpoints/Intelligence/CRM/Outreach/Workflow tables ensured")
    except Exception as exc:
        log.error(f"Failed to create tables: {exc}")


# ── CRUD ──

def create_job(
    job_id: str | None,
    user_id: int,
    job_type: str,
    payload: dict,
    max_attempts: int = 3,
    priority: int = 0,
) -> str:
    """Insert a new job row. Returns the job_id."""
    if not job_id:
        job_id = str(uuid.uuid4())[:8]

    now = _utc_now_iso()
    db = _get_db()
    db.execute(
        """
        INSERT INTO jobs (
            job_id, user_id, type, status, priority,
            payload, progress, message, phase,
            attempt, max_attempts,
            created_at, updated_at
        ) VALUES (?, ?, ?, 'queued', ?, ?, 0, 'Queued...', '',
                  1, ?, ?, ?)
        """,
        (
            job_id,
            int(user_id),
            job_type,
            priority,
            json.dumps(payload, default=str),
            max_attempts,
            now,
            now,
        ),
    )
    db.commit()
    log.info(f"Created job {job_id} (type={job_type}, user={user_id})")
    return job_id


def get_job(job_id: str) -> dict | None:
    """Get a job by ID. Returns dict or None."""
    db = _get_db()
    row = db.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
    if row is None:
        return None
    return dict(row)


def update_job(job_id: str, updates: dict) -> bool:
    """Update specific fields on a job. Returns True if row was found."""
    if not updates:
        return False

    # Always set updated_at
    updates.setdefault("updated_at", _utc_now_iso())

    set_clauses = []
    values = []
    for key, val in updates.items():
        set_clauses.append(f"{key} = ?")
        values.append(val)
    values.append(job_id)

    sql = f"UPDATE jobs SET {', '.join(set_clauses)} WHERE job_id = ?"

    db = _get_db()
    cursor = db.execute(sql, values)
    db.commit()
    return cursor.rowcount > 0


def list_jobs_by_user(
    user_id: int,
    status: str | None = None,
    job_type: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """List jobs for a user, newest first."""
    conditions = ["user_id = ?"]
    params: list = [int(user_id)]

    if status:
        conditions.append("status = ?")
        params.append(status)
    if job_type:
        conditions.append("type = ?")
        params.append(job_type)

    params.append(limit)

    sql = f"""
        SELECT * FROM jobs
        WHERE {' AND '.join(conditions)}
        ORDER BY created_at DESC
        LIMIT ?
    """

    db = _get_db()
    rows = db.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def count_active_jobs(user_id: int) -> int:
    """Count queued + running jobs for a user."""
    db = _get_db()
    row = db.execute(
        "SELECT COUNT(*) FROM jobs WHERE user_id = ? AND status IN ('queued', 'running')",
        (int(user_id),),
    ).fetchone()
    return row[0] if row else 0


def get_stale_jobs(stale_threshold_iso: str) -> list[dict]:
    """Find running jobs whose heartbeat has expired."""
    db = _get_db()
    rows = db.execute(
        """
        SELECT job_id, type, attempt, max_attempts
        FROM jobs
        WHERE status = 'running'
          AND heartbeat_at IS NOT NULL
          AND heartbeat_at < ?
        """,
        (stale_threshold_iso,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_job_result(job_id: str) -> dict | None:
    """Get just the result and metadata for a completed job."""
    db = _get_db()
    row = db.execute(
        """
        SELECT job_id, type, status, result, result_count, error,
               created_at, finished_at
        FROM jobs WHERE job_id = ?
        """,
        (job_id,),
    ).fetchone()
    if row is None:
        return None
    return dict(row)
