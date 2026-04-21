"""
LeadGen — Stale Job Sweeper

Runs periodically in the API process to detect and recover stale jobs
(workers that crashed without completing).
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import datetime, timezone, timedelta

log = logging.getLogger(__name__)


def _sweep_once() -> int:
    """
    1. Find cloud jobs with expired worker heartbeats → re-enqueue or fail.
    2. Find local-agent jobs with expired leases → reassign to cloud.
    Returns total number of jobs recovered.
    """
    from config import SWEEPER_STALE_THRESHOLD
    from jobs.store import get_stale_jobs, update_job
    from jobs.queue import enqueue_job, check_heartbeat

    recovered = 0

    # ── Phase 2: stale cloud worker jobs ──────────────────────────────────
    threshold = (
        datetime.now(timezone.utc) - timedelta(seconds=SWEEPER_STALE_THRESHOLD)
    ).isoformat()

    stale = get_stale_jobs(threshold)

    for job in stale:
        job_id = job["job_id"]

        # Skip local-agent jobs — handled by agent sweep below
        if job.get("execution_mode") == "local":
            continue

        # Double-check: maybe heartbeat was refreshed after our DB query
        if check_heartbeat(job_id) is not None:
            continue

        attempt = int(job.get("attempt", 1))
        max_attempts = int(job.get("max_attempts", 3))

        if attempt < max_attempts:
            update_job(job_id, {
                "status": "queued",
                "attempt": attempt + 1,
                "last_error": "Worker heartbeat lost (crash recovery)",
                "message": f"Re-queuing (worker lost, attempt {attempt + 1}/{max_attempts})",
                "worker_id": "",
            })
            enqueue_job(job_id, job["type"], attempt + 1)
            log.warning(f"Sweeper: re-enqueued stale cloud job {job_id} (attempt {attempt + 1})")
        else:
            update_job(job_id, {
                "status": "failed",
                "error": "Worker heartbeat lost after max retries",
                "message": f"Failed: worker crashed after {max_attempts} attempts",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            })
            log.error(f"Sweeper: failed stale cloud job {job_id} (max retries reached)")

        recovered += 1

    # ── Phase 3: stale local-agent jobs ───────────────────────────────────
    try:
        from agents.service import sweep_stale_agents
        agent_recovered = sweep_stale_agents()
        recovered += agent_recovered
    except Exception as exc:
        log.error(f"Agent sweep error: {exc}")

    return recovered



def start_sweeper_thread() -> threading.Thread | None:
    """Start the sweeper as a background daemon thread. Returns the thread."""
    from config import SWEEPER_INTERVAL
    from jobs.queue import redis_available

    if not redis_available():
        log.info("Sweeper not started — Redis unavailable")
        return None

    def _loop():
        log.info(f"Stale job sweeper started (interval={SWEEPER_INTERVAL}s)")
        while True:
            try:
                count = _sweep_once()
                if count > 0:
                    log.info(f"Sweeper recovered {count} stale job(s)")
            except Exception as exc:
                log.error(f"Sweeper error: {exc}")
            time.sleep(SWEEPER_INTERVAL)

    t = threading.Thread(target=_loop, daemon=True, name="leadgen-sweeper")
    t.start()
    return t
