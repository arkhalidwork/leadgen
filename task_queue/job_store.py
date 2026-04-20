"""
In-memory job state store for scraping jobs.

Thread-safe dictionary-based storage. No Redis dependency.
Job states auto-expire after TTL to prevent memory leaks.
"""
from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from copy import deepcopy

_lock = threading.Lock()
_store: dict[str, dict] = {}

# Jobs older than this are eligible for cleanup (seconds)
JOB_TTL_SECONDS = 86400  # 24 hours


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _cleanup_expired():
    """Remove jobs older than TTL. Called internally, must hold lock."""
    now = time.time()
    expired = [
        jid for jid, state in _store.items()
        if now - state.get("_ts", 0) > JOB_TTL_SECONDS
        and str(state.get("status", "")).upper() in ("COMPLETED", "FAILED", "PARTIAL")
    ]
    for jid in expired:
        _store.pop(jid, None)


def save_job_state(job_id: str, state: dict):
    """Save job state (thread-safe)."""
    with _lock:
        _cleanup_expired()
        data = deepcopy(state)
        data.setdefault("updated_at", _utc_now_iso())
        data["_ts"] = time.time()
        _store[job_id] = data


def get_job_state(job_id: str) -> dict | None:
    """Get job state (thread-safe). Returns a copy."""
    with _lock:
        state = _store.get(job_id)
        if state is None:
            return None
        return deepcopy(state)


def set_job_stop_requested(job_id: str, requested: bool = True):
    """Signal a job to stop."""
    with _lock:
        state = _store.get(job_id)
        if state is None:
            state = {}
            _store[job_id] = state
        state["stop_requested"] = bool(requested)
        state["updated_at"] = _utc_now_iso()
        state["_ts"] = time.time()


def is_job_stop_requested(job_id: str) -> bool:
    """Check if a job has been requested to stop."""
    with _lock:
        state = _store.get(job_id)
        if state is None:
            return False
        return bool(state.get("stop_requested", False))


def list_job_states() -> list[dict]:
    """Return all known job states (thread-safe), newest first."""
    with _lock:
        _cleanup_expired()
        values = [deepcopy(v) for v in _store.values()]

    def _sort_key(state: dict):
        return state.get("updated_at") or state.get("created_at") or ""

    values.sort(key=_sort_key, reverse=True)
    return values
