"""
Web Crawler scraping handler for the worker service.

Wraps the existing WebCrawlerScraper into the unified handler interface.
"""
from __future__ import annotations

import logging

from workers.worker import register_handler

log = logging.getLogger(__name__)


@register_handler("webcrawler")
def handle_webcrawler(
    job_id: str,
    payload: dict,
    progress_cb,
    should_stop,
) -> dict:
    """Execute a Web Crawler scraping job."""
    from web_crawler import WebCrawlerScraper, clean_web_leads

    keyword = str(payload.get("keyword", "")).strip()
    place = str(payload.get("place", "")).strip()

    log.info(f"WebCrawler job {job_id}: keyword={keyword}, place={place}")

    scraper = WebCrawlerScraper(headless=True)

    def _progress(msg: str, pct: int):
        partial = scraper.get_partial_leads() if hasattr(scraper, "get_partial_leads") else []
        progress_cb(msg, pct, {"results_count": len(partial)})

    scraper.set_progress_callback(_progress)

    try:
        raw = scraper.scrape(keyword, place)

        if should_stop():
            raw = scraper.get_partial_leads() if hasattr(scraper, "get_partial_leads") else raw

        cleaned = clean_web_leads(raw)

        status = "partial" if should_stop() else "completed"
        return {
            "status": status,
            "leads": cleaned,
            "lead_count": len(cleaned),
            "message": (
                f"Stopped. Saved {len(cleaned)} leads."
                if status == "partial"
                else f"Done! Found {len(cleaned)} leads from the web."
            ),
        }
    finally:
        try:
            scraper.close()
        except Exception:
            pass
