from __future__ import annotations

from typing import Callable

from geo.quadtree import BoundingBox
from scraper import GoogleMapsScraper, clean_leads


JobInput = dict
ProgressCallback = Callable[[str, int, dict | None], None]
ShouldStopCallback = Callable[[], bool]


OnLeadFoundCallback = Callable[[dict, int], None]


def run_scraper_job(
    payload: JobInput,
    progress_callback: ProgressCallback | None = None,
    should_stop: ShouldStopCallback | None = None,
    on_lead_found: OnLeadFoundCallback | None = None,
) -> dict:
    """
    Stateless worker entry point for Google Maps scraping.

    Returns structured JSON:
    {
      "job_id": str,
      "keyword": str,
      "place": str,
      "status": "COMPLETED"|"PARTIAL",
      "leads": list[dict],
      "lead_count": int,
      "area_stats": dict
    }
    """
    job_id = str(payload.get("job_id", ""))
    keyword = str(payload.get("keyword", "")).strip()
    max_leads = payload.get("max_leads")
    crawl_contacts = bool(payload.get("crawl_contacts", False))
    geo_cell = payload.get("geo_cell") if isinstance(payload.get("geo_cell"), dict) else {}
    map_selection = payload.get("map_selection") if isinstance(payload.get("map_selection"), dict) else None

    lat = geo_cell.get("lat")
    lng = geo_cell.get("lng")
    geo_cell_bounds = payload.get("geo_cell_bounds") if isinstance(payload.get("geo_cell_bounds"), dict) else None
    forced_geo_cells: list[BoundingBox] | None = None

    if geo_cell_bounds:
        try:
            forced_geo_cells = [
                BoundingBox(
                    min_lat=float(geo_cell_bounds.get("min_lat")),
                    max_lat=float(geo_cell_bounds.get("max_lat")),
                    min_lng=float(geo_cell_bounds.get("min_lng")),
                    max_lng=float(geo_cell_bounds.get("max_lng")),
                )
            ]
        except (TypeError, ValueError):
            forced_geo_cells = None

    place = str(payload.get("place", "")).strip()
    if not place and lat is not None and lng is not None:
        place = f"{lat}, {lng}"

    if max_leads is not None:
        try:
            max_leads = int(max_leads)
        except (TypeError, ValueError):
            max_leads = None

    scraper = GoogleMapsScraper(headless=True)

    def _progress(message: str, percent: int):
        # Check stop signal on every progress tick
        if should_stop and should_stop():
            scraper.stop()

        snapshot: dict = {}
        area_stats = scraper.area_stats
        partial_leads = scraper.get_partial_leads()

        snapshot["area_stats"] = area_stats
        snapshot["results_count"] = len(partial_leads)
        snapshot["lead_count"] = len(partial_leads)

        # ALWAYS send current partial leads so API always has latest data
        snapshot["results"] = partial_leads

        if progress_callback:
            progress_callback(message, percent, snapshot)

    scraper.set_progress_callback(_progress)

    try:
        raw_leads = scraper.scrape(
            keyword,
            place,
            map_selection=map_selection,
            forced_geo_cells=forced_geo_cells,
            force_primary_keyword_only=bool(forced_geo_cells),
            max_leads=max_leads,
            crawl_contacts=crawl_contacts,
            on_lead_found=on_lead_found,
        )

        # Light cleanup: normalize fields, but do NOT aggressively deduplicate
        cleaned = clean_leads(raw_leads)

        status = "PARTIAL" if should_stop and should_stop() else "COMPLETED"

        return {
            "job_id": job_id,
            "keyword": keyword,
            "place": place,
            "status": status,
            "leads": cleaned,
            "lead_count": len(cleaned),
            "area_stats": scraper.area_stats,
        }
    finally:
        scraper._close_driver()
