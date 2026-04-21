"""
LeadGen Agent — Job Poller

Infinite polling loop that fetches assigned jobs from the server
and dispatches them to the executor.
"""
from __future__ import annotations

import logging
import time

import api_client
import config
import executor
import storage
import uploader

log = logging.getLogger(__name__)


def run_poll_loop() -> None:
    """Main polling loop. Runs forever (call from a daemon thread or main thread)."""
    log.info(f"Poller started (interval={config.POLL_INTERVAL_SECONDS}s)")

    poll_interval = config.POLL_INTERVAL_SECONDS

    while True:
        try:
            active_jobs = executor.get_active_job_ids()

            # Don't poll if at max capacity
            if executor.is_full(config.MAX_CONCURRENT_JOBS):
                time.sleep(poll_interval)
                continue

            resp = api_client.poll(
                active_jobs=active_jobs,
                cpu_pct=_cpu_pct(),
                ram_gb=_ram_gb(),
            )

            if resp is None:
                # Network failure — continue running
                time.sleep(poll_interval)
                continue

            # Update poll interval from server recommendation
            poll_interval = float(resp.get("poll_interval_seconds", config.POLL_INTERVAL_SECONDS))

            # Handle forced update
            if resp.get("requires_update"):
                log.warning("Server requires update. Refusing new jobs until updated.")
                from main import check_for_update
                check_for_update(resp)
                time.sleep(30)
                continue

            job_data = resp.get("job")
            if not job_data:
                time.sleep(poll_interval)
                continue

            job_id = job_data["job_id"]
            log.info(f"Received job {job_id} (type={job_data['type']})")

            # Save locally first
            storage.upsert_job(job_data)

            # Flush any pending checkpoints from previous run (reconnect scenario)
            uploader.flush_pending_checkpoints(job_id)

            # Get checkpoint (either from server response or local SQLite)
            checkpoint = job_data.get("checkpoint")
            if not checkpoint:
                checkpoint = storage.load_latest_checkpoint(job_id)

            # Execute
            executor.execute(job_data, checkpoint=checkpoint)

        except Exception as exc:
            log.error(f"Poller error: {exc}")

        time.sleep(poll_interval)


def _cpu_pct() -> float:
    try:
        import psutil
        return psutil.cpu_percent(interval=0.1)
    except ImportError:
        return 0.0


def _ram_gb() -> float:
    try:
        import psutil
        return round(psutil.virtual_memory().available / (1024 ** 3), 2)
    except ImportError:
        return 0.0
