"""
LinkedIn scraping handler for the worker service.

Wraps the existing LinkedInScraper into the unified handler interface.
"""
from __future__ import annotations

import logging

from workers.worker import register_handler

log = logging.getLogger(__name__)


@register_handler("linkedin")
def handle_linkedin(
    job_id: str,
    payload: dict,
    progress_cb,
    should_stop,
) -> dict:
    """Execute a LinkedIn scraping job."""
    from linkedin_scraper import LinkedInScraper, clean_linkedin_leads

    niche = str(payload.get("niche", "")).strip()
    place = str(payload.get("place", "")).strip()
    search_type = str(payload.get("search_type", "profiles")).strip()

    log.info(f"LinkedIn job {job_id}: niche={niche}, place={place}, type={search_type}")

    scraper = LinkedInScraper(headless=True)

    def _progress(msg: str, pct: int):
        partial = scraper.get_partial_leads() if hasattr(scraper, "get_partial_leads") else []
        progress_cb(msg, pct, {"results_count": len(partial)})

    scraper.set_progress_callback(_progress)

    try:
        raw = scraper.scrape(niche, place, search_type=search_type)

        # Check if stopped
        if should_stop():
            raw = scraper.get_partial_leads() if hasattr(scraper, "get_partial_leads") else raw

        cleaned = clean_linkedin_leads(raw, search_type)

        status = "partial" if should_stop() else "completed"
        return {
            "status": status,
            "leads": cleaned,
            "lead_count": len(cleaned),
            "search_type": search_type,
            "message": (
                f"Stopped. Saved {len(cleaned)} {search_type}."
                if status == "partial"
                else f"Done! Found {len(cleaned)} {search_type}."
            ),
        }
    finally:
        try:
            scraper.close()
        except Exception:
            pass
