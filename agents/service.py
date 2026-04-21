"""
LeadGen — Agents Service

Agent CRUD, capability matching, job assignment, lease management,
and stale-agent detection logic.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")

# Heartbeat older than this = agent is offline
AGENT_STALE_SECONDS = 45
# Lease duration for a job held by an agent
LEASE_DURATION_SECONDS = 120
# Min agent version required (set to "0.0.0" to allow all)
MIN_REQUIRED_VERSION = os.environ.get("LEADGEN_AGENT_MIN_VERSION", "0.0.0")
LATEST_AGENT_VERSION = os.environ.get("LEADGEN_AGENT_LATEST_VERSION", "1.0.0")


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def _get_db():
    from jobs.store import _get_db as _store_db
    return _store_db()


# ── Version Check ──────────────────────────────────────────────────────────

def _version_tuple(v: str) -> tuple[int, ...]:
    try:
        return tuple(int(x) for x in str(v).split(".")[:3])
    except (ValueError, AttributeError):
        return (0, 0, 0)


def requires_update(agent_version: str) -> bool:
    return _version_tuple(agent_version) < _version_tuple(MIN_REQUIRED_VERSION)


# ── Agent CRUD ─────────────────────────────────────────────────────────────

def upsert_agent(data: dict) -> dict:
    """Create or update an agent record. Returns the final agent row."""
    db = _get_db()
    now = _utcnow()
    agent_id = data["agent_id"]

    existing = db.execute(
        "SELECT agent_id FROM agents WHERE agent_id = ?", (agent_id,)
    ).fetchone()

    caps = json.dumps(data.get("capabilities", []))

    if existing:
        db.execute(
            """
            UPDATE agents SET
                hostname = ?, platform = ?, capabilities = ?,
                max_concurrent = ?, version = ?,
                status = 'online', last_seen_at = ?
            WHERE agent_id = ?
            """,
            (
                data.get("hostname", ""),
                data.get("platform", ""),
                caps,
                int(data.get("max_concurrent", 1)),
                data.get("agent_version", ""),
                now,
                agent_id,
            ),
        )
    else:
        db.execute(
            """
            INSERT INTO agents (
                agent_id, user_id, hostname, platform, capabilities,
                max_concurrent, version, status, last_seen_at, registered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'online', ?, ?)
            """,
            (
                agent_id,
                int(data["user_id"]),
                data.get("hostname", ""),
                data.get("platform", ""),
                caps,
                int(data.get("max_concurrent", 1)),
                data.get("agent_version", ""),
                now,
                now,
            ),
        )
    db.commit()
    return get_agent(agent_id)


def get_agent(agent_id: str) -> dict | None:
    db = _get_db()
    row = db.execute("SELECT * FROM agents WHERE agent_id = ?", (agent_id,)).fetchone()
    return dict(row) if row else None


def get_agent_by_id_and_user(agent_id: str, user_id: int) -> dict | None:
    db = _get_db()
    row = db.execute(
        "SELECT * FROM agents WHERE agent_id = ? AND user_id = ?",
        (agent_id, int(user_id)),
    ).fetchone()
    return dict(row) if row else None


def update_agent_heartbeat(agent_id: str, cpu_pct: float, ram_gb: float, active_jobs: list) -> None:
    db = _get_db()
    status = "busy" if active_jobs else "online"
    db.execute(
        """
        UPDATE agents SET
            status = ?, last_seen_at = ?,
            cpu_pct = ?, ram_available_gb = ?
        WHERE agent_id = ?
        """,
        (status, _utcnow(), cpu_pct, ram_gb, agent_id),
    )
    db.commit()


def set_agent_offline(agent_id: str) -> None:
    db = _get_db()
    db.execute(
        "UPDATE agents SET status = 'offline' WHERE agent_id = ?",
        (agent_id,),
    )
    db.commit()


def get_active_agent_for_user(user_id: int, capability: str = "") -> dict | None:
    """Find an online agent for this user that supports the given capability."""
    db = _get_db()
    threshold = (
        datetime.now(timezone.utc) - timedelta(seconds=AGENT_STALE_SECONDS)
    ).isoformat()

    rows = db.execute(
        """
        SELECT * FROM agents
        WHERE user_id = ?
          AND status IN ('online', 'busy')
          AND last_seen_at >= ?
        ORDER BY last_seen_at DESC
        """,
        (int(user_id), threshold),
    ).fetchall()

    for row in rows:
        agent = dict(row)
        if not capability:
            return agent
        caps = json.loads(agent.get("capabilities") or "[]")
        if capability in caps:
            return agent

    return None


def get_online_agent_count(user_id: int) -> int:
    db = _get_db()
    threshold = (
        datetime.now(timezone.utc) - timedelta(seconds=AGENT_STALE_SECONDS)
    ).isoformat()
    row = db.execute(
        "SELECT COUNT(*) FROM agents WHERE user_id = ? AND last_seen_at >= ?",
        (int(user_id), threshold),
    ).fetchone()
    return row[0] if row else 0


# ── Job Assignment ─────────────────────────────────────────────────────────

def assign_execution_mode(
    user_id: int,
    job_type: str,
    requested_mode: str = "auto",
) -> tuple[str, str]:
    """
    Returns (execution_mode, agent_id).
    execution_mode: 'cloud' | 'local'
    agent_id: '' if cloud
    """
    if requested_mode == "cloud":
        return "cloud", ""

    agent = get_active_agent_for_user(user_id, capability=job_type)

    if agent and requested_mode in ("auto", "local"):
        return "local", agent["agent_id"]

    if requested_mode == "local" and not agent:
        raise ValueError(
            "No active local agent found. "
            "Start the LeadGen Agent on your machine and try again."
        )

    # auto + no agent → cloud
    return "cloud", ""


# ── Lease Management ───────────────────────────────────────────────────────

def claim_job_lease(job_id: str, agent_id: str) -> bool:
    """
    Atomically claim a job lease for an agent.
    Returns True if this agent now holds the lease, False if already taken.
    """
    from jobs.store import _get_db as store_db
    db = store_db()
    cursor = db.execute(
        """
        UPDATE jobs
        SET execution_lease_at = datetime('now', '+2 minutes'),
            agent_id = ?
        WHERE job_id = ?
          AND (execution_lease_at IS NULL OR execution_lease_at < datetime('now'))
        """,
        (agent_id, job_id),
    )
    db.commit()
    return cursor.rowcount > 0


def renew_job_leases(agent_id: str, job_ids: list[str]) -> None:
    """Extend lease for all active jobs of this agent (called on heartbeat)."""
    if not job_ids:
        return
    from jobs.store import _get_db as store_db
    db = store_db()
    for job_id in job_ids:
        db.execute(
            "UPDATE jobs SET execution_lease_at = datetime('now', '+2 minutes') WHERE job_id = ? AND agent_id = ?",
            (job_id, agent_id),
        )
    db.commit()


# ── Checkpoint Store (Server Side) ─────────────────────────────────────────

def save_checkpoint(job_id: str, seq: int, phase: str, data: dict, leads_count: int = 0) -> int:
    """Save a checkpoint from an agent. Returns seq_accepted."""
    db = _get_db()
    now = _utcnow()
    db.execute(
        """
        INSERT OR REPLACE INTO checkpoints (job_id, seq, phase, data, leads_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (job_id, seq, phase, json.dumps(data, default=str), leads_count, now),
    )
    # Update checkpoint_seq on the job
    db.execute(
        "UPDATE jobs SET checkpoint_seq = ?, updated_at = ? WHERE job_id = ?",
        (seq, now, job_id),
    )
    db.commit()
    return seq


def get_latest_checkpoint(job_id: str) -> dict | None:
    """Get the most recent checkpoint for a job."""
    db = _get_db()
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


# ── Stale Agent Sweep ──────────────────────────────────────────────────────

def sweep_stale_agents() -> int:
    """
    Detect offline agents and reassign their orphaned jobs to cloud.
    Returns number of jobs recovered.
    """
    from jobs.store import update_job
    from jobs.queue import enqueue_job

    db = _get_db()
    threshold = (
        datetime.now(timezone.utc) - timedelta(seconds=AGENT_STALE_SECONDS)
    ).isoformat()

    # Mark stale agents offline
    stale_agents = db.execute(
        """
        SELECT agent_id FROM agents
        WHERE status IN ('online', 'busy')
          AND last_seen_at < ?
        """,
        (threshold,),
    ).fetchall()

    recovered = 0
    for row in stale_agents:
        agent_id = row[0]
        db.execute(
            "UPDATE agents SET status = 'offline' WHERE agent_id = ?",
            (agent_id,),
        )
        log.warning(f"Sweeper: agent {agent_id} marked offline (heartbeat lost)")

        # Find jobs owned by this agent that are stuck
        orphaned = db.execute(
            """
            SELECT job_id, type, attempt, max_attempts, checkpoint_seq
            FROM jobs
            WHERE agent_id = ?
              AND status IN ('assigned_to_agent', 'running_local')
            """,
            (agent_id,),
        ).fetchall()

        for job_row in orphaned:
            job_id, job_type, attempt, max_attempts, chk_seq = job_row
            attempt = int(attempt or 1)
            max_attempts = int(max_attempts or 3)
            chk_seq = int(chk_seq or 0)

            if attempt >= max_attempts:
                update_job(job_id, {
                    "status": "failed",
                    "error": f"Agent {agent_id} lost heartbeat — max retries reached",
                    "finished_at": _utcnow(),
                })
                log.error(f"Sweeper: job {job_id} failed (agent lost, max retries)")
            else:
                resume_msg = (
                    f"Agent lost — resuming from checkpoint (attempt {attempt + 1}/{max_attempts})"
                    if chk_seq > 0
                    else f"Agent lost — restarting (attempt {attempt + 1}/{max_attempts})"
                )
                update_job(job_id, {
                    "status": "queued",
                    "execution_mode": "cloud",
                    "agent_id": "",
                    "execution_lease_at": None,
                    "attempt": attempt + 1,
                    "message": resume_msg,
                    "worker_id": "",
                })
                enqueue_job(job_id, job_type, attempt + 1)
                log.warning(f"Sweeper: job {job_id} re-queued for cloud (agent {agent_id} lost)")
            recovered += 1

    db.commit()

    # Also catch any jobs with expired leases (independent of agent status)
    expired_lease_jobs = db.execute(
        """
        SELECT job_id, type, attempt, max_attempts, checkpoint_seq, agent_id
        FROM jobs
        WHERE status IN ('assigned_to_agent', 'running_local')
          AND execution_lease_at IS NOT NULL
          AND execution_lease_at < datetime('now')
        """,
    ).fetchall()

    for row in expired_lease_jobs:
        job_id, job_type, attempt, max_attempts, chk_seq, a_id = row
        attempt = int(attempt or 1)
        max_attempts = int(max_attempts or 3)
        chk_seq = int(chk_seq or 0)

        if attempt >= max_attempts:
            update_job(job_id, {
                "status": "failed",
                "error": f"Job lease expired (agent {a_id}) — max retries reached",
                "finished_at": _utcnow(),
            })
        else:
            update_job(job_id, {
                "status": "queued",
                "execution_mode": "cloud",
                "agent_id": "",
                "execution_lease_at": None,
                "attempt": attempt + 1,
                "message": f"Lease expired — cloud fallback (attempt {attempt + 1}/{max_attempts})",
                "worker_id": "",
            })
            enqueue_job(job_id, job_type, attempt + 1)
            log.warning(f"Sweeper: job {job_id} lease expired — re-queued for cloud")
        recovered += 1

    if recovered:
        db.commit()

    return recovered
