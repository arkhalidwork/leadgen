"""
LeadGen Agent — Job Executor

Runs the correct scraper for each job type in a separate thread.
Wires up progress callbacks → uploader and stop signal checks.
"""
from __future__ import annotations

import importlib
import json
import logging
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import api_client
import storage
import uploader

log = logging.getLogger(__name__)

# Active jobs: job_id → {"thread": Thread, "stop_event": Event}
_active: dict[str, dict] = {}
_active_lock = threading.Lock()


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_active_job_ids() -> list[str]:
    with _active_lock:
        return list(_active.keys())


def is_full(max_concurrent: int) -> bool:
    with _active_lock:
        return len(_active) >= max_concurrent


def request_stop(job_id: str) -> None:
    """Signal a running job to stop gracefully."""
    with _active_lock:
        entry = _active.get(job_id)
    if entry:
        entry["stop_event"].set()
        log.info(f"Stop requested for job {job_id}")


def execute(job: dict, checkpoint: dict | None = None) -> None:
    """
    Launch a scraper job in a background thread.
    job: dict with job_id, type, payload, attempt
    checkpoint: optional checkpoint dict to resume from
    """
    job_id = job["job_id"]
    job_type = job["type"]

    stop_event = threading.Event()
    with _active_lock:
        _active[job_id] = {"stop_event": stop_event}

    thread = threading.Thread(
        target=_run,
        args=(job, checkpoint, stop_event),
        daemon=True,
        name=f"exec-{job_id}",
    )
    with _active_lock:
        _active[job_id]["thread"] = thread

    thread.start()
    log.info(f"Executor: started job {job_id} (type={job_type})")


def _run(job: dict, checkpoint: dict | None, stop_event: threading.Event) -> None:
    """Actual execution — runs in a thread."""
    job_id = job["job_id"]
    job_type = job["type"]
    attempt = int(job.get("attempt", 1))

    try:
        payload = job.get("payload", {})
        if isinstance(payload, str):
            payload = json.loads(payload)

        # Signal server that we've started
        chk_seq = checkpoint.get("seq", 0) if checkpoint else 0
        api_client.job_start(job_id, checkpoint_seq=chk_seq)
        storage.update_job_status(job_id, "running", started_at=_utcnow())

        _checkpoint_seq = [chk_seq]
        _start_time = time.time()

        def progress_cb(message: str, percent: int, snapshot: dict | None = None):
            elapsed = int(time.time() - _start_time)
            result_count = 0
            if snapshot:
                result_count = int(
                    snapshot.get("results_count")
                    or snapshot.get("lead_count")
                    or len(snapshot.get("results") or snapshot.get("leads_so_far") or [])
                )
            uploader.maybe_upload_progress(
                job_id, percent, message, elapsed=elapsed, result_count=result_count
            )

        def checkpoint_cb(data: dict):
            _checkpoint_seq[0] += 1
            phase = data.get("phase", job_type)
            uploader.maybe_upload_checkpoint(job_id, _checkpoint_seq[0], phase, data)

        def should_stop() -> bool:
            return stop_event.is_set()

        # Load the scraper
        result = _dispatch(job_type, payload, checkpoint, progress_cb, checkpoint_cb, should_stop)

        # Upload final result
        status = str(result.get("status", "completed")).lower()
        message = result.get("message", "Done")
        uploader.upload_final_result(job_id, status, message, result)
        storage.update_job_status(job_id, "complete", finished_at=_utcnow())
        log.info(f"Executor: job {job_id} finished ({status}, {result.get('lead_count', 0)} leads)")

    except Exception as exc:
        log.exception(f"Executor: job {job_id} crashed: {exc}")
        storage.update_job_status(job_id, "failed", error=str(exc)[:500])
        chk_seq = storage.load_latest_checkpoint(job_id)
        last_seq = chk_seq["seq"] if chk_seq else 0
        api_client.job_fail(job_id, str(exc), attempt=attempt, last_checkpoint_seq=last_seq)
        uploader.reset_job_state(job_id)

    finally:
        with _active_lock:
            _active.pop(job_id, None)
        uploader.reset_job_state(job_id)


def _dispatch(job_type: str, payload: dict, checkpoint, progress_cb, checkpoint_cb, should_stop):
    """Route job type to the correct scraper. Scrapers are in ./scrapers/."""
    # Add scrapers/ directory to path
    scrapers_dir = Path(__file__).parent / "scrapers"
    if str(scrapers_dir) not in sys.path:
        sys.path.insert(0, str(scrapers_dir))

    if job_type == "linkedin":
        from linkedin import LinkedInScraper, clean_linkedin_leads
        scraper = LinkedInScraper(headless=True)
        scraper.set_progress_callback(progress_cb)
        raw = scraper.scrape(payload["niche"], payload["place"], payload.get("search_type", "profiles"))
        if should_stop():
            raw = scraper.get_partial_leads() if hasattr(scraper, "get_partial_leads") else raw
        scraper.close()
        cleaned = clean_linkedin_leads(raw, payload.get("search_type", "profiles"))
        status = "partial" if should_stop() else "completed"
        return {"status": status, "leads": cleaned, "lead_count": len(cleaned),
                "message": f"{'Stopped' if should_stop() else 'Done'}. Found {len(cleaned)} leads."}

    elif job_type == "instagram":
        from instagram import InstagramScraper, clean_instagram_leads
        scraper = InstagramScraper(headless=True)
        scraper.set_progress_callback(progress_cb)
        raw = scraper.scrape(payload.get("keywords", ""), payload["place"], payload.get("search_type", "profiles"))
        if should_stop():
            raw = scraper.get_partial_leads() if hasattr(scraper, "get_partial_leads") else raw
        scraper.close()
        cleaned = clean_instagram_leads(raw, payload.get("search_type", "profiles"))
        status = "partial" if should_stop() else "completed"
        return {"status": status, "leads": cleaned, "lead_count": len(cleaned),
                "message": f"Found {len(cleaned)} Instagram leads."}

    elif job_type == "webcrawler":
        from webcrawler import WebCrawlerScraper, clean_web_leads
        scraper = WebCrawlerScraper(headless=True)
        scraper.set_progress_callback(progress_cb)
        raw = scraper.scrape(payload["keyword"], payload["place"])
        scraper.close()
        cleaned = clean_web_leads(raw)
        return {"status": "completed", "leads": cleaned, "lead_count": len(cleaned),
                "message": f"Found {len(cleaned)} web leads."}

    elif job_type == "gmaps":
        from gmaps import GoogleMapsScraper
        scraper = GoogleMapsScraper(headless=True)
        # Pass checkpoint to resume from
        checkpoint_data = checkpoint.get("data") if checkpoint else None
        result = scraper.scrape_with_resume(
            payload, checkpoint_data, progress_cb, checkpoint_cb, should_stop
        )
        scraper.close()
        return result

    else:
        raise ValueError(f"Unknown job type: {job_type}")
