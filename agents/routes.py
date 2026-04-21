"""
LeadGen — Agent Protocol Routes (Flask Blueprint)

Implements the 8 agent-server endpoints plus agent status query.
All endpoints require HMAC-SHA256 request signing.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from datetime import datetime, timezone
from functools import wraps

from flask import Blueprint, g, jsonify, request, session

log = logging.getLogger(__name__)

agents_bp = Blueprint("agents", __name__, url_prefix="/agents")


# ── HMAC Signature Verification ────────────────────────────────────────────

def _get_user_api_key(user_id: int) -> str | None:
    """Load the user's API key for HMAC verification."""
    from jobs.store import _get_db
    db = _get_db()
    row = db.execute(
        "SELECT api_key FROM users WHERE id = ?", (int(user_id),)
    ).fetchone()
    return row[0] if row else None


def require_agent_auth(f):
    """Decorator: verify HMAC signature on all agent endpoints."""
    @wraps(f)
    def decorated(*args, **kwargs):
        agent_id = request.headers.get("X-Agent-ID", "").strip()
        timestamp = request.headers.get("X-Timestamp", "0").strip()
        signature = request.headers.get("X-Signature", "").strip()

        if not agent_id or not timestamp or not signature:
            return jsonify({"error": "Missing authentication headers"}), 401

        # Reject stale requests (replay attack window)
        try:
            ts_int = int(timestamp)
        except ValueError:
            return jsonify({"error": "Invalid timestamp"}), 401

        if abs(time.time() - ts_int) > 60:
            return jsonify({"error": "Request expired (>60s)"}), 401

        # Look up agent
        from agents.service import get_agent
        agent = get_agent(agent_id)
        if not agent:
            # During registration, agent may not exist yet — handled separately
            return jsonify({"error": "Unknown agent"}), 401

        # Get user's API key
        api_key = _get_user_api_key(agent["user_id"])
        if not api_key:
            return jsonify({"error": "Agent user not found"}), 401

        # Verify signature
        body_sha = hashlib.sha256(request.data).hexdigest()
        msg = f"{agent_id}:{timestamp}:{body_sha}"
        expected = hmac.new(
            api_key.encode(), msg.encode(), hashlib.sha256
        ).hexdigest()

        if not hmac.compare_digest(expected, signature):
            return jsonify({"error": "Invalid signature"}), 403

        g.agent = agent
        g.agent_version = request.headers.get("X-Agent-Version", "0.0.0")
        return f(*args, **kwargs)
    return decorated


def _requires_update_response(agent_version: str) -> dict:
    from agents.service import (
        requires_update, MIN_REQUIRED_VERSION,
        LATEST_AGENT_VERSION,
    )
    needs = requires_update(agent_version)
    return {
        "requires_update": needs,
        "min_required_version": MIN_REQUIRED_VERSION,
        "latest_version": LATEST_AGENT_VERSION,
    }


# ── 1. POST /agents/register ───────────────────────────────────────────────

@agents_bp.route("/register", methods=["POST"])
def agent_register():
    """
    Agent announces itself on startup.
    Auth: API key in X-Api-Key header (pre-HMAC — agent doesn't have agent_id yet
    if first registration; server derives and confirms it).
    """
    api_key = request.headers.get("X-Api-Key", "").strip()
    if not api_key:
        return jsonify({"error": "X-Api-Key header required"}), 401

    # Verify API key
    from jobs.store import _get_db
    db = _get_db()
    user_row = db.execute(
        "SELECT id FROM users WHERE api_key = ?", (api_key,)
    ).fetchone()
    if not user_row:
        return jsonify({"error": "Invalid API key"}), 401

    user_id = user_row[0]
    data = request.get_json(force=True) or {}
    agent_id = data.get("agent_id", "").strip()
    if not agent_id:
        return jsonify({"error": "agent_id required"}), 400

    from agents.service import upsert_agent, requires_update, LATEST_AGENT_VERSION, MIN_REQUIRED_VERSION
    data["user_id"] = user_id
    upsert_agent(data)

    agent_version = data.get("agent_version", "0.0.0")
    needs_update = requires_update(agent_version)

    log.info(f"Agent registered: {agent_id} (user={user_id}, v={agent_version})")
    return jsonify({
        "status": "registered",
        "agent_id": agent_id,
        "poll_interval_seconds": 5,
        "requires_update": needs_update,
        "min_required_version": MIN_REQUIRED_VERSION,
        "latest_version": LATEST_AGENT_VERSION,
        "server_time": datetime.now(timezone.utc).isoformat(),
    })


# ── 2. POST /agents/poll ───────────────────────────────────────────────────

@agents_bp.route("/poll", methods=["POST"])
@require_agent_auth
def agent_poll():
    """Agent requests a job. Server returns the next assigned job (or null)."""
    from agents.service import claim_job_lease, get_latest_checkpoint, requires_update
    from jobs.store import get_job, update_job

    agent = g.agent
    agent_id = agent["agent_id"]
    agent_version = g.agent_version
    data = request.get_json(force=True) or {}

    # Update agent stats
    from agents.service import update_agent_heartbeat
    update_agent_heartbeat(
        agent_id,
        cpu_pct=float(data.get("cpu_pct", 0)),
        ram_gb=float(data.get("ram_available_gb", 0)),
        active_jobs=data.get("active_jobs", []),
    )

    upd_resp = _requires_update_response(agent_version)

    if upd_resp["requires_update"]:
        return jsonify({"job": None, **upd_resp})

    # Find next assigned job for this agent
    from jobs.store import _get_db
    db = _get_db()

    capabilities = data.get("capabilities", json.loads(agent.get("capabilities") or "[]"))

    if not capabilities:
        return jsonify({"job": None, **upd_resp})

    placeholders = ",".join("?" * len(capabilities))
    row = db.execute(
        f"""
        SELECT job_id, type, payload, checkpoint_seq FROM jobs
        WHERE agent_id = ?
          AND status = 'assigned_to_agent'
          AND type IN ({placeholders})
        ORDER BY priority DESC, created_at ASC
        LIMIT 1
        """,
        [agent_id] + capabilities,
    ).fetchone()

    if not row:
        return jsonify({"job": None, **upd_resp})

    job_id, job_type, payload_str, chk_seq = row

    # Atomically claim lease
    claimed = claim_job_lease(job_id, agent_id)
    if not claimed:
        # Another agent grabbed it
        return jsonify({"job": None, **upd_resp})

    # Load checkpoint if exists
    checkpoint = None
    if int(chk_seq or 0) > 0:
        checkpoint = get_latest_checkpoint(job_id)

    try:
        payload = json.loads(payload_str or "{}")
    except (json.JSONDecodeError, TypeError):
        payload = {}

    log.info(f"Agent {agent_id} polled job {job_id} (type={job_type})")
    return jsonify({
        "job": {
            "job_id": job_id,
            "type": job_type,
            "payload": payload,
            "checkpoint": checkpoint,
            "lease_expires_at": db.execute(
                "SELECT execution_lease_at FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()[0],
        },
        **upd_resp,
    })


# ── 3. POST /agents/heartbeat ──────────────────────────────────────────────

@agents_bp.route("/heartbeat", methods=["POST"])
@require_agent_auth
def agent_heartbeat():
    """Agent keeps its presence and job leases alive."""
    from agents.service import update_agent_heartbeat, renew_job_leases
    from jobs.queue import is_stop_requested

    agent = g.agent
    agent_id = agent["agent_id"]
    data = request.get_json(force=True) or {}
    active_jobs = data.get("active_jobs", [])

    update_agent_heartbeat(
        agent_id,
        cpu_pct=float(data.get("cpu_pct", 0)),
        ram_gb=float(data.get("ram_available_gb", 0)),
        active_jobs=active_jobs,
    )
    renew_job_leases(agent_id, active_jobs)

    # Tell agent which jobs have stop signals
    stop_jobs = [jid for jid in active_jobs if is_stop_requested(jid)]

    return jsonify({
        "ok": True,
        "stop_jobs": stop_jobs,
        **_requires_update_response(g.agent_version),
    })


# ── 4. POST /agents/job/<id>/start ─────────────────────────────────────────

@agents_bp.route("/job/<job_id>/start", methods=["POST"])
@require_agent_auth
def agent_job_start(job_id):
    """Agent confirms it has started executing the job."""
    from jobs.store import get_job, update_job
    from jobs.queue import is_stop_requested

    agent_id = g.agent["agent_id"]
    data = request.get_json(force=True) or {}

    job = get_job(job_id)
    if not job or job.get("agent_id") != agent_id:
        return jsonify({"error": "Job not found or not assigned to this agent"}), 404

    update_job(job_id, {
        "status": "running_local",
        "started_at": data.get("started_at") or datetime.now(timezone.utc).isoformat(),
        "message": "Running on local agent...",
        "checkpoint_seq": int(data.get("checkpoint_seq", 0)),
    })

    log.info(f"Agent {agent_id} started job {job_id}")
    return jsonify({
        "ok": True,
        "stop_requested": is_stop_requested(job_id),
    })


# ── 5. POST /agents/job/<id>/progress ─────────────────────────────────────

@agents_bp.route("/job/<job_id>/progress", methods=["POST"])
@require_agent_auth
def agent_job_progress(job_id):
    """Throttled progress update from agent."""
    from jobs.store import update_job
    from jobs.queue import is_stop_requested

    agent_id = g.agent["agent_id"]
    data = request.get_json(force=True) or {}

    update_job(job_id, {
        "progress": max(0, min(100, int(data.get("progress", 0)))),
        "message": str(data.get("message", ""))[:500],
        "phase": str(data.get("phase", "")),
        "phase_detail": str(data.get("phase_detail", "")),
        "result_count": int(data.get("result_count", 0)),
        "heartbeat_at": datetime.now(timezone.utc).isoformat(),
    })

    return jsonify({
        "ok": True,
        "stop_requested": is_stop_requested(job_id),
    })


# ── 6. POST /agents/job/<id>/checkpoint ───────────────────────────────────

@agents_bp.route("/job/<job_id>/checkpoint", methods=["POST"])
@require_agent_auth
def agent_job_checkpoint(job_id):
    """Save a checkpoint (resumable state) from the agent."""
    from agents.service import save_checkpoint

    agent_id = g.agent["agent_id"]
    data = request.get_json(force=True) or {}
    seq = int(data.get("seq", 0))
    phase = str(data.get("phase", ""))
    checkpoint_data = data.get("data", {})
    leads_partial = data.get("leads_partial", [])
    leads_count = len(leads_partial) if leads_partial else 0

    seq_accepted = save_checkpoint(job_id, seq, phase, checkpoint_data, leads_count)

    # Optionally store partial results for recovery
    if leads_partial:
        from jobs.store import update_job
        import json as _json
        update_job(job_id, {
            "result": _json.dumps({"leads": leads_partial}, default=str),
            "result_count": leads_count,
        })

    log.debug(f"Agent {agent_id} checkpoint for job {job_id}: seq={seq}, leads={leads_count}")
    return jsonify({"ok": True, "seq_accepted": seq_accepted})


# ── 7. POST /agents/job/<id>/complete ─────────────────────────────────────

@agents_bp.route("/job/<job_id>/complete", methods=["POST"])
@require_agent_auth
def agent_job_complete(job_id):
    """Agent reports job completion with final results."""
    from jobs.store import update_job
    import json as _json

    agent_id = g.agent["agent_id"]
    data = request.get_json(force=True) or {}
    result = data.get("result", {})
    status = str(data.get("status", "completed")).lower()

    final_updates = {
        "status": status,
        "progress": 100,
        "message": str(data.get("message", "Done"))[:500],
        "finished_at": data.get("finished_at") or datetime.now(timezone.utc).isoformat(),
        "result": _json.dumps(result, default=str),
        "result_count": result.get("lead_count", len(result.get("leads", []))),
        "execution_lease_at": None,
    }
    update_job(job_id, final_updates)

    # Clean up Redis stop signal
    from jobs.queue import clear_stop_signal
    clear_stop_signal(job_id)

    # Phase 4: Trigger intelligence pipeline (non-blocking)
    try:
        from jobs.store import _get_db as _jdb
        _jdb_row = _jdb().execute("SELECT user_id FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if _jdb_row:
            from intelligence.pipeline import trigger_pipeline_async
            trigger_pipeline_async(job_id, job.get("type", "unknown") if (job := _jdb().execute("SELECT type FROM jobs WHERE job_id=?", (job_id,)).fetchone()) else "unknown", int(_jdb_row[0]))
    except Exception as _pe:
        log.debug(f"Intelligence pipeline (agent complete) skipped: {_pe}")

    log.info(f"Agent {agent_id} completed job {job_id}: status={status}, leads={final_updates['result_count']}")
    return jsonify({"ok": True})


# ── 8. POST /agents/job/<id>/fail ─────────────────────────────────────────

@agents_bp.route("/job/<job_id>/fail", methods=["POST"])
@require_agent_auth
def agent_job_fail(job_id):
    """Agent reports a failure. Server decides retry strategy."""
    from jobs.store import get_job, update_job
    from jobs.queue import enqueue_job, clear_stop_signal

    agent_id = g.agent["agent_id"]
    data = request.get_json(force=True) or {}
    error = str(data.get("error", "Unknown error"))[:2000]
    attempt = int(data.get("attempt", 1))

    job = get_job(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404

    max_attempts = int(job.get("max_attempts", 3))
    chk_seq = int(job.get("checkpoint_seq", 0))

    if attempt < max_attempts:
        # Retry: prefer cloud if agent failed
        update_job(job_id, {
            "status": "queued",
            "execution_mode": "cloud",
            "agent_id": "",
            "execution_lease_at": None,
            "attempt": attempt + 1,
            "last_error": error,
            "message": f"Agent failed — cloud retry ({attempt + 1}/{max_attempts})",
            "worker_id": "",
        })
        enqueue_job(job_id, job.get("type", "unknown"), attempt + 1)
        log.warning(f"Agent {agent_id} failed job {job_id} (attempt {attempt}) — re-queued for cloud")
        clear_stop_signal(job_id)
        return jsonify({
            "ok": True,
            "retry": True,
            "retry_on": "cloud",
            "backoff_seconds": min(30 * attempt, 120),
        })
    else:
        update_job(job_id, {
            "status": "failed",
            "progress": 100,
            "error": error,
            "last_error": error,
            "message": f"Failed after {max_attempts} attempts: {error[:200]}",
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "execution_lease_at": None,
        })
        log.error(f"Agent {agent_id} job {job_id} permanently failed after {max_attempts} attempts")
        clear_stop_signal(job_id)
        return jsonify({"ok": True, "retry": False})


# ── Agent status query (for UI widget) ────────────────────────────────────

@agents_bp.route("/my-status", methods=["GET"])
def agent_my_status():
    """Return the current user's active agent status (for UI widget)."""
    from flask import session

    # This endpoint is called by the frontend (authenticated user session)
    if "user_id" not in session:
        return jsonify({"status": "offline", "hostname": ""})

    from agents.service import get_active_agent_for_user
    agent = get_active_agent_for_user(session["user_id"])

    if not agent:
        return jsonify({"status": "offline", "hostname": "", "agent_id": None})

    return jsonify({
        "status": agent.get("status", "offline"),
        "hostname": agent.get("hostname", ""),
        "agent_id": agent.get("agent_id"),
        "version": agent.get("version", ""),
        "platform": agent.get("platform", ""),
        "capabilities": json.loads(agent.get("capabilities") or "[]"),
        "last_seen_at": agent.get("last_seen_at"),
    })
