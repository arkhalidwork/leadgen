"""
LeadGen Worker — Separate process that consumes jobs from Redis.

Usage:
    python -m workers.worker --queue default
    python -m workers.worker --queue heavy

The worker runs an infinite loop:
1. BRPOPLPUSH a job from the queue
2. Load full job from DB
3. Route to the correct handler by job type
4. Execute with progress callbacks + heartbeats
5. Update final status and ack the job
"""
from __future__ import annotations

import json
import logging
import os
import signal
import socket
import sys
import threading
import time
from datetime import datetime, timezone

log = logging.getLogger("leadgen.worker")

# ── Handler Registry ──
HANDLERS: dict[str, callable] = {}
_shutdown = False

WORKER_ID = f"{socket.gethostname()}-pid-{os.getpid()}"


def register_handler(job_type: str):
    """Decorator to register a handler for a job type."""
    def decorator(fn):
        HANDLERS[job_type] = fn
        log.info(f"Registered handler for job type: {job_type}")
        return fn
    return decorator


def _signal_handler(signum, frame):
    """Graceful shutdown: finish current job, then exit."""
    global _shutdown
    sig_name = signal.Signals(signum).name if hasattr(signal, "Signals") else str(signum)
    log.info(f"Received {sig_name} — finishing current job then shutting down...")
    _shutdown = True


def run_worker(queue_name: str):
    """Main worker loop."""
    global _shutdown

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Import handlers to trigger @register_handler decorators
    try:
        from workers.handlers import (  # noqa: F401
            linkedin_handler,
            instagram_handler,
            webcrawler_handler,
            gmaps_handler,
        )
    except ImportError as exc:
        log.warning(f"Some handlers failed to import: {exc}")

    from jobs.queue import dequeue_job, ack_job, set_heartbeat
    from jobs.store import get_job, update_job, ensure_jobs_table
    from config import WORKER_HEARTBEAT_INTERVAL, WORKER_HEARTBEAT_TTL, WORKER_PROGRESS_THROTTLE

    # Ensure DB table exists
    ensure_jobs_table()

    log.info(f"Worker {WORKER_ID} started — listening on {queue_name}")
    log.info(f"Registered handlers: {list(HANDLERS.keys())}")

    while not _shutdown:
        # ── 1. Dequeue ──
        job_msg, raw_payload = dequeue_job(queue_name, timeout=5)
        if job_msg is None:
            continue  # Timeout — loop again (checks _shutdown)

        job_id = job_msg["job_id"]
        job_type = job_msg["type"]

        handler = HANDLERS.get(job_type)
        if not handler:
            log.error(f"No handler for job type '{job_type}', skipping job {job_id}")
            ack_job(raw_payload)
            continue

        log.info(f"▶ Processing {job_type} job {job_id} (attempt {job_msg.get('attempt', 1)})")

        # ── 2. Mark RUNNING ──
        now = datetime.now(timezone.utc).isoformat()
        update_job(job_id, {
            "status": "running",
            "worker_id": WORKER_ID,
            "started_at": now,
            "heartbeat_at": now,
        })

        # ── 3. Start heartbeat thread ──
        hb_stop = threading.Event()

        def _heartbeat_loop():
            while not hb_stop.wait(WORKER_HEARTBEAT_INTERVAL):
                set_heartbeat(job_id, WORKER_ID, ttl=WORKER_HEARTBEAT_TTL)

        hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name=f"hb-{job_id}")
        hb_thread.start()
        set_heartbeat(job_id, WORKER_ID, ttl=WORKER_HEARTBEAT_TTL)

        try:
            # ── 4. Load full job from DB ──
            job = get_job(job_id)
            if not job:
                log.error(f"Job {job_id} not found in DB, skipping")
                hb_stop.set()
                ack_job(raw_payload)
                continue

            payload = json.loads(job.get("payload") or "{}")

            # ── 5. Create throttled progress callback ──
            _last_progress_write = [0.0]

            def progress_cb(message: str, percent: int, snapshot: dict | None = None):
                now_ts = time.time()
                # Throttle DB writes
                if now_ts - _last_progress_write[0] < WORKER_PROGRESS_THROTTLE:
                    return
                _last_progress_write[0] = now_ts

                set_heartbeat(job_id, WORKER_ID, ttl=WORKER_HEARTBEAT_TTL)
                upd = {
                    "progress": max(0, min(100, percent)),
                    "message": message[:500],
                    "heartbeat_at": datetime.now(timezone.utc).isoformat(),
                }
                if snapshot:
                    if "results_count" in snapshot or "lead_count" in snapshot:
                        upd["result_count"] = int(
                            snapshot.get("results_count")
                            or snapshot.get("lead_count")
                            or 0
                        )
                    if "results" in snapshot and isinstance(snapshot["results"], list):
                        upd["result"] = json.dumps(
                            {"leads": snapshot["results"]}, default=str
                        )
                        upd["result_count"] = len(snapshot["results"])
                    if "area_stats" in snapshot:
                        # Merge area_stats into result JSON
                        try:
                            existing = json.loads(upd.get("result") or "{}")
                        except (json.JSONDecodeError, TypeError):
                            existing = {}
                        existing["area_stats"] = snapshot["area_stats"]
                        upd["result"] = json.dumps(existing, default=str)
                update_job(job_id, upd)

            # Create stop-check callback
            from jobs.queue import is_stop_requested

            def should_stop() -> bool:
                return is_stop_requested(job_id)

            # ── 6. Execute handler ──
            result = handler(job_id, payload, progress_cb, should_stop)

            # ── 7. Mark completed ──
            final_status = str(result.get("status", "completed")).lower()
            final_updates = {
                "status": final_status,
                "progress": 100,
                "message": result.get("message", "Done"),
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }

            # Store final results
            if "leads" in result:
                final_updates["result"] = json.dumps(result, default=str)
                final_updates["result_count"] = result.get("lead_count", len(result.get("leads", [])))
            elif "result" in result:
                final_updates["result"] = json.dumps(result["result"], default=str)

            update_job(job_id, final_updates)

            hb_stop.set()
            ack_job(raw_payload)
            log.info(f"✓ Job {job_id} completed: {final_status} ({final_updates.get('result_count', 0)} results)")

            # Phase 4: Trigger intelligence pipeline async (non-blocking)
            try:
                from intelligence.pipeline import trigger_pipeline_async
                trigger_pipeline_async(job_id, job_type, int(user_id))
            except Exception as _intel_exc:
                log.debug(f"Intelligence pipeline trigger skipped: {_intel_exc}")

        except Exception as exc:
            log.exception(f"✗ Job {job_id} failed: {exc}")
            hb_stop.set()

            # Load current state for retry decision
            job = get_job(job_id)
            attempt = int(job.get("attempt", 1)) if job else 1
            max_attempts = int(job.get("max_attempts", 3)) if job else 3

            if attempt < max_attempts:
                # Re-enqueue for retry
                update_job(job_id, {
                    "status": "queued",
                    "attempt": attempt + 1,
                    "last_error": str(exc)[:2000],
                    "message": f"Retrying (attempt {attempt + 1}/{max_attempts})...",
                    "worker_id": "",
                })
                from jobs.queue import enqueue_job
                enqueue_job(job_id, job.get("type", "unknown") if job else "unknown", attempt + 1)
                log.info(f"↻ Job {job_id} re-enqueued for attempt {attempt + 1}/{max_attempts}")
            else:
                # Terminal failure
                update_job(job_id, {
                    "status": "failed",
                    "progress": 100,
                    "error": str(exc)[:2000],
                    "last_error": str(exc)[:2000],
                    "message": f"Failed after {max_attempts} attempts: {str(exc)[:200]}",
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                })
                log.error(f"✗ Job {job_id} permanently failed after {max_attempts} attempts")

            ack_job(raw_payload)

    log.info(f"Worker {WORKER_ID} shut down gracefully")


# ── CLI Entry Point ──
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="LeadGen Worker")
    parser.add_argument(
        "--queue",
        default="default",
        choices=["default", "heavy"],
        help="Which queue to consume (default: default)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    from config import QUEUE_DEFAULT, QUEUE_HEAVY

    queue = QUEUE_HEAVY if args.queue == "heavy" else QUEUE_DEFAULT
    run_worker(queue)
