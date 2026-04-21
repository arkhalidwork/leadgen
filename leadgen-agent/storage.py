"""
LeadGen Agent — Local SQLite Storage

Persists agent state, job assignments, checkpoints, and logs locally.
This is the primary crash-recovery mechanism — all state is written here
BEFORE being uploaded to the server.
"""
from __future__ import annotations

import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone

import config

log = logging.getLogger(__name__)

_local = threading.local()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_db() -> sqlite3.Connection:
    """Thread-local SQLite connection to the agent's local database."""
    if not hasattr(_local, "db") or _local.db is None:
        config.AGENT_DIR.mkdir(parents=True, exist_ok=True)
        _local.db = sqlite3.connect(str(config.DB_PATH), timeout=10)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
        _local.db.execute("PRAGMA busy_timeout=3000")
    return _local.db


def ensure_schema() -> None:
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS agent_jobs (
            job_id         TEXT PRIMARY KEY,
            type           TEXT NOT NULL,
            status         TEXT DEFAULT 'assigned',
            payload        TEXT DEFAULT '{}',
            attempt        INTEGER DEFAULT 1,
            assigned_at    TEXT,
            started_at     TEXT,
            finished_at    TEXT,
            lease_expires  TEXT,
            error          TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS checkpoints (
            job_id         TEXT NOT NULL,
            seq            INTEGER NOT NULL,
            phase          TEXT DEFAULT '',
            data           TEXT DEFAULT '{}',
            leads_count    INTEGER DEFAULT 0,
            created_at     TEXT NOT NULL,
            uploaded_at    TEXT,
            PRIMARY KEY (job_id, seq)
        );
        CREATE INDEX IF NOT EXISTS idx_chk_job ON checkpoints(job_id, seq DESC);

        CREATE TABLE IF NOT EXISTS agent_logs (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id         TEXT NOT NULL,
            level          TEXT DEFAULT 'info',
            message        TEXT NOT NULL,
            created_at     TEXT NOT NULL,
            uploaded       INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_logs_job ON agent_logs(job_id, uploaded);

        CREATE TABLE IF NOT EXISTS agent_config (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
    """)
    db.commit()
    log.debug("Agent local schema ready")


# ── Job State ──────────────────────────────────────────────────────────────

def upsert_job(job: dict) -> None:
    db = get_db()
    db.execute("""
        INSERT OR REPLACE INTO agent_jobs
            (job_id, type, status, payload, attempt, assigned_at, lease_expires)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        job["job_id"],
        job.get("type", ""),
        job.get("status", "assigned"),
        json.dumps(job.get("payload", {}), default=str),
        int(job.get("attempt", 1)),
        _utcnow(),
        job.get("lease_expires_at"),
    ))
    db.commit()


def update_job_status(job_id: str, status: str, **kwargs) -> None:
    db = get_db()
    now = _utcnow()
    db.execute(
        "UPDATE agent_jobs SET status = ?, " + ", ".join(f"{k} = ?" for k in kwargs) + " WHERE job_id = ?",
        [status] + list(kwargs.values()) + [job_id],
    )
    db.commit()


def get_job(job_id: str) -> dict | None:
    db = get_db()
    row = db.execute("SELECT * FROM agent_jobs WHERE job_id = ?", (job_id,)).fetchone()
    return dict(row) if row else None


def get_assigned_jobs() -> list[dict]:
    """Jobs the agent has claimed but not yet finished — used on restart."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM agent_jobs WHERE status IN ('assigned', 'running')"
    ).fetchall()
    return [dict(r) for r in rows]


# ── Checkpoints ────────────────────────────────────────────────────────────

def save_checkpoint(job_id: str, seq: int, phase: str, data: dict) -> None:
    """Always save checkpoint locally FIRST before uploading to server."""
    db = get_db()
    db.execute("""
        INSERT OR REPLACE INTO checkpoints (job_id, seq, phase, data, leads_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (
        job_id, seq, phase,
        json.dumps(data, default=str),
        len(data.get("leads_so_far", [])),
        _utcnow(),
    ))
    db.commit()
    log.debug(f"Checkpoint saved locally: job={job_id} seq={seq}")


def load_latest_checkpoint(job_id: str) -> dict | None:
    db = get_db()
    row = db.execute(
        "SELECT * FROM checkpoints WHERE job_id = ? ORDER BY seq DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if not row:
        return None
    result = dict(row)
    try:
        result["data"] = json.loads(result.get("data") or "{}")
    except (json.JSONDecodeError, TypeError):
        result["data"] = {}
    return result


def mark_checkpoint_uploaded(job_id: str, seq: int) -> None:
    db = get_db()
    db.execute(
        "UPDATE checkpoints SET uploaded_at = ? WHERE job_id = ? AND seq = ?",
        (_utcnow(), job_id, seq),
    )
    db.commit()


def get_pending_checkpoints(job_id: str) -> list[dict]:
    """Checkpoints not yet uploaded to server (for reconnection flush)."""
    db = get_db()
    rows = db.execute(
        "SELECT * FROM checkpoints WHERE job_id = ? AND uploaded_at IS NULL ORDER BY seq ASC",
        (job_id,),
    ).fetchall()
    return [dict(r) for r in rows]


# ── Logs ────────────────────────────────────────────────────────────────────

def write_log(job_id: str, level: str, message: str) -> None:
    db = get_db()
    db.execute(
        "INSERT INTO agent_logs (job_id, level, message, created_at) VALUES (?, ?, ?, ?)",
        (job_id, level, message[:2000], _utcnow()),
    )
    db.commit()


# ── Config ──────────────────────────────────────────────────────────────────

def get_config(key: str, default: str = "") -> str:
    db = get_db()
    row = db.execute("SELECT value FROM agent_config WHERE key = ?", (key,)).fetchone()
    return row[0] if row else default


def set_config(key: str, value: str) -> None:
    db = get_db()
    db.execute(
        "INSERT OR REPLACE INTO agent_config (key, value) VALUES (?, ?)",
        (key, value),
    )
    db.commit()
