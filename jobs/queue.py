"""
LeadGen — Redis Queue Operations

Manages job enqueue/dequeue, stop signals, heartbeats, and queue health.
All Redis keys use the `leadgen:` namespace.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_redis_client = None


def _get_redis():
    """Lazy-init Redis client. Returns None if Redis is unavailable."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client

    try:
        import redis as redis_lib
    except ImportError:
        log.warning("redis package not installed — queue features disabled")
        return None

    from config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD

    try:
        client = redis_lib.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD or None,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=10,
        )
        client.ping()
        _redis_client = client
        log.info(f"Redis connected: {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")
        return _redis_client
    except Exception as exc:
        log.warning(f"Redis unavailable ({exc}) — queue features disabled")
        return None


def redis_available() -> bool:
    """Check if Redis is reachable."""
    return _get_redis() is not None


# ── Enqueue / Dequeue ──

def enqueue_job(job_id: str, job_type: str, attempt: int = 1) -> bool:
    """Push a job onto the appropriate Redis queue. Returns False if Redis is down."""
    from config import TOOL_QUEUE_MAP, QUEUE_DEFAULT

    r = _get_redis()
    if r is None:
        return False

    queue_name = TOOL_QUEUE_MAP.get(job_type, QUEUE_DEFAULT)
    payload = json.dumps({
        "job_id": job_id,
        "type": job_type,
        "attempt": attempt,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
    })
    try:
        r.lpush(queue_name, payload)
        log.info(f"Enqueued job {job_id} ({job_type}) → {queue_name}")
        return True
    except Exception as exc:
        log.error(f"Failed to enqueue job {job_id}: {exc}")
        return False


def dequeue_job(queue_name: str, timeout: int = 5) -> tuple[dict | None, str]:
    """
    Blocking pop from queue → push to processing queue (atomic via BRPOPLPUSH).
    Returns (parsed_message, raw_payload_string) or (None, "").
    """
    from config import QUEUE_PROCESSING

    r = _get_redis()
    if r is None:
        return None, ""

    try:
        result = r.brpoplpush(queue_name, QUEUE_PROCESSING, timeout=timeout)
        if result:
            return json.loads(result), result
        return None, ""
    except Exception as exc:
        log.error(f"Dequeue failed on {queue_name}: {exc}")
        return None, ""


def ack_job(raw_payload: str) -> None:
    """Remove a completed job from the processing queue."""
    from config import QUEUE_PROCESSING

    r = _get_redis()
    if r is None:
        return

    try:
        r.lrem(QUEUE_PROCESSING, 1, raw_payload)
    except Exception as exc:
        log.error(f"Failed to ack job: {exc}")


# ── Stop Signals ──

def set_stop_signal(job_id: str, ttl: int = 3600) -> None:
    """Signal a running job to stop. Worker checks this periodically."""
    r = _get_redis()
    if r is None:
        return

    try:
        r.setex(f"leadgen:stop:{job_id}", ttl, "1")
    except Exception as exc:
        log.error(f"Failed to set stop signal for {job_id}: {exc}")


def is_stop_requested(job_id: str) -> bool:
    """Check if a stop signal exists for this job."""
    r = _get_redis()
    if r is None:
        return False

    try:
        return bool(r.exists(f"leadgen:stop:{job_id}"))
    except Exception:
        return False


def clear_stop_signal(job_id: str) -> None:
    """Remove stop signal after job has stopped."""
    r = _get_redis()
    if r is None:
        return

    try:
        r.delete(f"leadgen:stop:{job_id}")
    except Exception:
        pass


# ── Heartbeats ──

def set_heartbeat(job_id: str, worker_id: str, ttl: int = 30) -> None:
    """Worker sets this every 10s. If it expires, job is considered stale."""
    r = _get_redis()
    if r is None:
        return

    try:
        r.setex(f"leadgen:heartbeat:{job_id}", ttl, worker_id)
    except Exception:
        pass


def check_heartbeat(job_id: str) -> str | None:
    """Returns worker_id if heartbeat exists, None if expired/missing."""
    r = _get_redis()
    if r is None:
        return None

    try:
        return r.get(f"leadgen:heartbeat:{job_id}")
    except Exception:
        return None


# ── Queue Health ──

def queue_health() -> dict:
    """Return current queue depths for monitoring."""
    from config import QUEUE_DEFAULT, QUEUE_HEAVY, QUEUE_PROCESSING

    r = _get_redis()
    if r is None:
        return {"redis": "unavailable"}

    try:
        return {
            "redis": "connected",
            "default_depth": r.llen(QUEUE_DEFAULT),
            "heavy_depth": r.llen(QUEUE_HEAVY),
            "processing": r.llen(QUEUE_PROCESSING),
        }
    except Exception as exc:
        return {"redis": f"error: {exc}"}
