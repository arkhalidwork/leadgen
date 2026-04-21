"""
Google Maps scraping handler for the worker service.

Wraps the existing scraper_worker.run_scraper_job() into the unified handler interface.
This is the most complex handler due to the multi-phase pipeline.
"""
from __future__ import annotations

import logging

from workers.worker import register_handler

log = logging.getLogger(__name__)


@register_handler("gmaps")
def handle_gmaps(
    job_id: str,
    payload: dict,
    progress_cb,
    should_stop,
) -> dict:
    """Execute a Google Maps scraping job via the existing scraper worker."""
    from workers.scraper_worker import run_scraper_job

    # Ensure job_id is in the payload (scraper_worker expects it)
    payload["job_id"] = job_id

    log.info(f"GMaps job {job_id}: keyword={payload.get('keyword')}, place={payload.get('place')}")

    def _bridge_progress(message: str, percent: int, snapshot: dict | None = None):
        """Bridge the existing progress callback format to the unified one."""
        progress_cb(message, percent, snapshot)

    result = run_scraper_job(
        payload=payload,
        progress_callback=_bridge_progress,
        should_stop=should_stop,
    )

    final_status = str(result.get("status", "COMPLETED")).lower()
    leads = result.get("leads", [])

    return {
        "status": final_status,
        "leads": leads,
        "lead_count": len(leads),
        "area_stats": result.get("area_stats", {}),
        "message": (
            f"Stopped with {len(leads)} leads."
            if final_status == "partial"
            else f"Found {len(leads)} leads."
        ),
    }
