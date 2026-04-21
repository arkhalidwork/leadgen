"""
LeadGen Agent — HTTPS API Client

Handles all outbound requests to the LeadGen server with:
- HMAC signing on every request
- Exponential backoff retry
- Degraded mode (continues running locally on network failure)
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any

import requests

import auth
import config
import storage

log = logging.getLogger(__name__)

AGENT_ID: str = ""  # Set by main.py after auth init


def _build_headers(body_bytes: bytes) -> dict:
    return auth.build_auth_headers(AGENT_ID, config.API_KEY, body_bytes)


def _post(endpoint: str, data: dict, max_retries: int = 5, use_api_key: bool = False) -> dict | None:
    """POST to server with retry + exponential backoff. Returns None on complete failure."""
    body_bytes = json.dumps(data).encode()
    headers = auth.build_register_headers(config.API_KEY) if use_api_key else _build_headers(body_bytes)
    url = config.SERVER_URL.rstrip("/") + endpoint

    for attempt in range(max_retries):
        try:
            r = requests.post(url, data=body_bytes, headers=headers, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.HTTPError as exc:
            # Don't retry 4xx (auth errors, bad request)
            if exc.response is not None and exc.response.status_code < 500:
                log.error(f"Server error {exc.response.status_code} on {endpoint}: {exc.response.text[:200]}")
                return None
            log.warning(f"HTTP {exc.response.status_code if exc.response else '?'} on {endpoint} (attempt {attempt+1})")
        except (requests.ConnectionError, requests.Timeout) as exc:
            log.warning(f"Network error on {endpoint} (attempt {attempt+1}): {exc}")

        if attempt < max_retries - 1:
            sleep = min(2 ** attempt, 30)
            time.sleep(sleep)

    log.error(f"Failed to reach server after {max_retries} attempts: {endpoint}")
    return None  # Degraded mode — caller handles None


def _get(endpoint: str) -> dict | None:
    url = config.SERVER_URL.rstrip("/") + endpoint
    body_bytes = b""
    headers = _build_headers(body_bytes)
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning(f"GET {endpoint} failed: {exc}")
        return None


# ── Protocol calls ─────────────────────────────────────────────────────────

def register(agent_id: str, hostname: str, platform: str) -> dict | None:
    """Call POST /agents/register. Uses API key auth."""
    return _post("/agents/register", {
        "agent_id": agent_id,
        "agent_version": config.AGENT_VERSION,
        "hostname": hostname,
        "platform": platform,
        "capabilities": config.CAPABILITIES,
        "max_concurrent": config.MAX_CONCURRENT_JOBS,
        "chrome_available": _chrome_available(),
    }, use_api_key=True)


def poll(active_jobs: list[str], cpu_pct: float = 0.0, ram_gb: float = 0.0) -> dict | None:
    """Call POST /agents/poll. Returns job dict or None."""
    resp = _post("/agents/poll", {
        "agent_id": AGENT_ID,
        "capabilities": config.CAPABILITIES,
        "active_jobs": active_jobs,
        "cpu_pct": cpu_pct,
        "ram_available_gb": ram_gb,
    })
    return resp


def heartbeat(active_jobs: list[str], cpu_pct: float = 0.0, ram_gb: float = 0.0) -> dict | None:
    """Call POST /agents/heartbeat. Returns stop_jobs list or None."""
    return _post("/agents/heartbeat", {
        "agent_id": AGENT_ID,
        "active_jobs": active_jobs,
        "cpu_pct": cpu_pct,
        "ram_available_gb": ram_gb,
        "uptime_seconds": int(time.time() - _start_time),
    }, max_retries=3)


def job_start(job_id: str, checkpoint_seq: int = 0) -> dict | None:
    return _post(f"/agents/job/{job_id}/start", {
        "agent_id": AGENT_ID,
        "started_at": _utcnow(),
        "checkpoint_seq": checkpoint_seq,
    })


def job_progress(job_id: str, progress: int, message: str, phase: str = "",
                 phase_detail: str = "", result_count: int = 0, elapsed: int = 0) -> dict | None:
    return _post(f"/agents/job/{job_id}/progress", {
        "agent_id": AGENT_ID,
        "progress": progress,
        "message": message,
        "phase": phase,
        "phase_detail": phase_detail,
        "result_count": result_count,
        "elapsed_seconds": elapsed,
    }, max_retries=3)


def job_checkpoint(job_id: str, seq: int, phase: str, data: dict,
                   leads_partial: list) -> dict | None:
    return _post(f"/agents/job/{job_id}/checkpoint", {
        "agent_id": AGENT_ID,
        "seq": seq,
        "phase": phase,
        "data": data,
        "leads_partial": leads_partial,
        "created_at": _utcnow(),
    }, max_retries=3)


def job_complete(job_id: str, status: str, message: str, result: dict) -> dict | None:
    return _post(f"/agents/job/{job_id}/complete", {
        "agent_id": AGENT_ID,
        "status": status,
        "progress": 100,
        "message": message,
        "result": result,
        "finished_at": _utcnow(),
    })


def job_fail(job_id: str, error: str, attempt: int, last_checkpoint_seq: int = 0) -> dict | None:
    return _post(f"/agents/job/{job_id}/fail", {
        "agent_id": AGENT_ID,
        "error": error,
        "attempt": attempt,
        "last_checkpoint_seq": last_checkpoint_seq,
        "failed_at": _utcnow(),
    })


# ── Helpers ────────────────────────────────────────────────────────────────

_start_time = time.time()


def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _chrome_available() -> bool:
    import shutil
    return (
        shutil.which("google-chrome") is not None
        or shutil.which("chromium") is not None
        or shutil.which("chromium-browser") is not None
    )
