"""
Instagram scraping handler for the worker service.

Wraps the existing InstagramScraper into the unified handler interface.
"""
from __future__ import annotations

import logging

from workers.worker import register_handler

log = logging.getLogger(__name__)


@register_handler("instagram")
def handle_instagram(
    job_id: str,
    payload: dict,
    progress_cb,
    should_stop,
) -> dict:
    """Execute an Instagram scraping job."""
    from instagram_scraper import InstagramScraper, clean_instagram_leads

    keywords = str(payload.get("keywords", "")).strip()
    place = str(payload.get("place", "")).strip()
    search_type = str(payload.get("search_type", "profiles")).strip()

    log.info(f"Instagram job {job_id}: keywords={keywords}, place={place}, type={search_type}")

    scraper = InstagramScraper(headless=True)

    def _progress(msg: str, pct: int):
        partial = scraper.get_partial_leads() if hasattr(scraper, "get_partial_leads") else []
        progress_cb(msg, pct, {"results_count": len(partial)})

    scraper.set_progress_callback(_progress)

    try:
        raw = scraper.scrape(keywords, place, search_type=search_type)

        if should_stop():
            raw = scraper.get_partial_leads() if hasattr(scraper, "get_partial_leads") else raw

        cleaned = clean_instagram_leads(raw, search_type)

        status = "partial" if should_stop() else "completed"
        return {
            "status": status,
            "leads": cleaned,
            "lead_count": len(cleaned),
            "search_type": search_type,
            "message": (
                f"Stopped. Saved {len(cleaned)} Instagram {search_type}."
                if status == "partial"
                else f"Done! Found {len(cleaned)} Instagram {search_type}."
            ),
        }
    finally:
        try:
            scraper.close()
        except Exception:
            pass
