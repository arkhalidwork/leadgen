"""
Google Maps Lead Scraper Module — Optimized Edition
====================================================
Scrapes business listings from Google Maps for lead generation
using adaptive geo-partitioning for maximum coverage.

Key improvements over original:
  - Viewport-based search (navigate to coordinates + zoom)
  - Adaptive quadtree partitioning (dense areas → more cells)
  - Multi-keyword expansion (synonyms for broader recall)
  - Spatial deduplication (lat/lng + fuzzy name matching)
  - Smart scrolling with stagnation detection
  - Proper city-wide coverage via Nominatim geocoding
"""

import os
import re
import time
import math
import hashlib
from collections import deque
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, field, asdict
from typing import Callable

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from selenium import webdriver

warnings.filterwarnings("ignore", category=InsecureRequestWarning)
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    StaleElementReferenceException,
    WebDriverException,
)

from geo.quadtree import (
    BoundingBox, bbox_from_place, bbox_from_map_selection,
    bbox_from_coordinates, build_cells_for_area, zoom_for_bbox,
)
from utils.keyword_expander import expand_keywords

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Common social media domain patterns
SOCIAL_PATTERNS = {
    "facebook": re.compile(r'https?://(?:www\.)?facebook\.com/[\w.\-]+', re.I),
    "instagram": re.compile(r'https?://(?:www\.)?instagram\.com/[\w.\-]+', re.I),
    "twitter": re.compile(r'https?://(?:www\.)?(?:twitter|x)\.com/[\w.\-]+', re.I),
    "linkedin": re.compile(r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[\w.\-]+', re.I),
    "youtube": re.compile(r'https?://(?:www\.)?youtube\.com/(?:@|channel/|c/)[\w.\-]+', re.I),
    "tiktok": re.compile(r'https?://(?:www\.)?tiktok\.com/@[\w.\-]+', re.I),
    "pinterest": re.compile(r'https?://(?:www\.)?pinterest\.com/[\w.\-]+', re.I),
}

# Email regex
EMAIL_RE = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.I,
)

PHONE_RE = re.compile(r'(?:\+?\d[\d\s().\-]{6,}\d)')

# Emails to exclude (common false positives)
EMAIL_BLACKLIST = {
    'example.com', 'test.com', 'email.com', 'domain.com',
    'yoursite.com', 'company.com', 'website.com', 'sentry.io',
    'wixpress.com', 'w3.org', 'schema.org', 'googleapis.com',
    'googleusercontent.com', 'gstatic.com',
}

# Regex to extract coordinates from Google Maps URL
COORDS_RE = re.compile(r'@(-?\d+\.?\d*),(-?\d+\.?\d*)')


@dataclass
class BusinessLead:
    """Represents a scraped business lead."""
    business_name: str = ""
    owner_name: str = ""
    phone: str = ""
    website: str = ""
    email: str = ""
    address: str = ""
    rating: str = ""
    reviews: str = ""
    category: str = ""
    latitude: str = ""
    longitude: str = ""
    facebook: str = ""
    instagram: str = ""
    twitter: str = ""
    linkedin: str = ""
    youtube: str = ""
    tiktok: str = ""
    pinterest: str = ""


class GoogleMapsScraper:
    """Scrapes Google Maps search results for business leads with adaptive geo-partitioning."""

    GOOGLE_MAPS_URL = "https://www.google.com/maps"

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self._progress_callback = None
        self._should_stop = False
        self._website_workers = max(1, int(os.environ.get("LEADGEN_WEBSITE_WORKERS", "20")))
        self._website_timeout_seconds = max(2.0, float(os.environ.get("LEADGEN_WEBSITE_TIMEOUT_SECONDS", "8")))
        self._max_pages_per_website = max(1, int(os.environ.get("LEADGEN_WEBSITE_MAX_PAGES", "3")))
        self._max_scroll_attempts = max(30, int(os.environ.get("LEADGEN_SCROLL_ATTEMPTS", "120")))
        # Shared HTTP adapter for connection-pool reuse across parallel workers
        self._http_adapter = requests.adapters.HTTPAdapter(
            pool_connections=self._website_workers,
            pool_maxsize=self._website_workers * 2,
            max_retries=0,
        )

        # --- Live tracking ---
        self._area_stats = {
            "current_area": "",
            "current_area_index": 0,
            "total_areas": 0,
            "completed_areas": 0,
            "leads_found": 0,
            "websites_scanned": 0,
            "websites_total": 0,
            "geo_cells_total": 0,
            "geo_cells_completed": 0,
            "keywords_expanded": [],
            "coverage_score": 0,
        }
        self._partial_leads: list = []  # live partial leads list

    def _new_http_session(self) -> requests.Session:
        session = requests.Session()
        session.verify = False
        session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        # Mount shared adapter — reuses TCP connections + DNS cache across workers
        session.mount("https://", self._http_adapter)
        session.mount("http://", self._http_adapter)
        return session

    def set_progress_callback(self, callback):
        """Set a callback function for progress updates."""
        self._progress_callback = callback

    def stop(self):
        """Signal the scraper to stop."""
        self._should_stop = True

    @property
    def area_stats(self) -> dict:
        """Return current area tracking statistics."""
        return dict(self._area_stats)

    def get_partial_leads(self) -> list:
        """Return the leads collected so far (even mid-scrape)."""
        return list(self._partial_leads)

    def _exact_deduplicate_leads(self, leads: list[dict]) -> list[dict]:
        """Remove duplicates using exact hash only (name + coordinates)."""
        seen_hashes: set[str] = set()
        unique_leads: list[dict] = []

        for lead in leads:
            name = (lead.get("business_name") or "").strip().lower()
            if not name or name == "unknown":
                continue

            latitude = (lead.get("latitude") or "").strip()
            longitude = (lead.get("longitude") or "").strip()
            key = f"{name}|{latitude}|{longitude}"
            lead_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()

            if lead_hash in seen_hashes:
                continue

            seen_hashes.add(lead_hash)
            unique_leads.append(lead)

        return unique_leads

    def _report_progress(self, message: str, percentage: int = -1):
        """Report progress via callback."""
        logger.info(message)
        if self._progress_callback:
            self._progress_callback(message, percentage)

    @staticmethod
    def _is_localhost_read_timeout_error(exc: Exception) -> bool:
        """Detect Selenium command timeouts on local WebDriver HTTP channel."""
        text = str(exc)
        if not text:
            return False
        return (
            "HTTPConnectionPool(host='localhost'" in text
            and "Read timed out" in text
        ) or ("ReadTimeoutError" in text and "localhost" in text)

    def _recover_driver_session(self, reason: str = "") -> bool:
        """Best-effort driver restart after command-channel timeout."""
        try:
            logger.warning(f"Recovering WebDriver session ({reason})")
            self._close_driver()
            self._init_driver()
            return True
        except Exception as recovery_exc:
            logger.error(f"WebDriver recovery failed: {recovery_exc}")
            return False

    def _navigate_with_retries(self, url: str, label: str = "page", retries: int = 2):
        """Navigate with retry/recovery for local WebDriver read timeouts."""
        last_exc = None
        for attempt in range(retries + 1):
            try:
                self.driver.get(url)
                return
            except WebDriverException as exc:
                last_exc = exc
                if not self._is_localhost_read_timeout_error(exc):
                    raise

                logger.warning(
                    f"WebDriver timeout while loading {label} "
                    f"(attempt {attempt + 1}/{retries + 1}): {exc}"
                )
                if attempt < retries:
                    time.sleep(min(2.0, 0.7 + (attempt * 0.5)))
                    if attempt >= 1:
                        self._recover_driver_session(reason=f"timeout on {label}")
                    continue
                break

        if last_exc:
            raise last_exc

    def _init_driver(self):
        """Initialize the Chrome WebDriver."""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=en-US")
        chrome_options.add_argument(
            "--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36"
        )
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

        # Use system-installed Chromium if available (Docker / ARM64)
        chrome_bin = os.environ.get("CHROME_BIN")
        if chrome_bin:
            chrome_options.binary_location = chrome_bin

        chromedriver_path = os.environ.get("CHROMEDRIVER_PATH")
        if chromedriver_path:
            service = Service(executable_path=chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        else:
            self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(2)

    def _search_maps(self, query: str):
        """Navigate to Google Maps and perform a search."""
        self._report_progress(f"Searching Google Maps for: {query}", 5)

        search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}?hl=en"
        self._navigate_with_retries(search_url, label=f"search query '{query}'")
        time.sleep(2)

        # Accept cookies / consent dialog if it appears
        try:
            accept_btn = self.driver.find_element(
                By.XPATH,
                "//button[contains(., 'Accept all') or contains(., 'Accept') or contains(., 'I agree')]"
            )
            accept_btn.click()
            time.sleep(2)
        except NoSuchElementException:
            pass
        except WebDriverException as exc:
            if self._is_localhost_read_timeout_error(exc):
                logger.warning(f"Timeout checking consent dialog for query '{query}': {exc}")
            else:
                raise

        # Wait for results
        try:
            WebDriverWait(self.driver, 15).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, 'div[role="feed"]')
                or d.find_elements(By.CSS_SELECTOR, 'h1')
            )
        except TimeoutException:
            logger.warning("Timed out waiting for search results to load.")
        except WebDriverException as exc:
            if self._is_localhost_read_timeout_error(exc):
                logger.warning(f"Timeout waiting for query results '{query}': {exc}")
                return
            raise

    def _search_maps_viewport(self, query: str, bbox: BoundingBox):
        """
        Search Google Maps within a specific viewport.

        Uses the Maps URL format that includes viewport coordinates
        to force results for a specific geographic area.
        """
        center_lat, center_lng = bbox.center()
        zoom = zoom_for_bbox(bbox)

        # Format: /maps/search/query/@lat,lng,zoom
        search_url = (
            f"https://www.google.com/maps/search/"
            f"{query.replace(' ', '+')}/"
            f"@{center_lat},{center_lng},{zoom}z"
            f"?hl=en"
        )

        self._navigate_with_retries(search_url, label=f"viewport search '{query}'")
        time.sleep(2.5)

        # Accept cookies / consent dialog
        try:
            accept_btn = self.driver.find_element(
                By.XPATH,
                "//button[contains(., 'Accept all') or contains(., 'Accept') or contains(., 'I agree')]"
            )
            accept_btn.click()
            time.sleep(2)
        except NoSuchElementException:
            pass
        except WebDriverException as exc:
            if self._is_localhost_read_timeout_error(exc):
                logger.warning(f"Timeout checking consent dialog for viewport '{query}': {exc}")
            else:
                raise

        # Wait for results
        try:
            WebDriverWait(self.driver, 15).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, 'div[role="feed"]')
                or d.find_elements(By.CSS_SELECTOR, 'h1')
            )
        except TimeoutException:
            logger.warning("Timed out waiting for viewport search results.")
        except WebDriverException as exc:
            if self._is_localhost_read_timeout_error(exc):
                logger.warning(f"Timeout waiting for viewport results '{query}': {exc}")
                return
            raise

    def _scroll_results(self):
        """Scroll the results panel to load all listings with smart stagnation detection."""
        self._report_progress("Scrolling to load all results...", 15)

        try:
            results_panel = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, 'div[role="feed"]')
                )
            )
        except TimeoutException:
            logger.warning("Could not find results feed panel.")
            return

        last_height = 0
        last_count = 0
        stagnation = 0
        max_scroll_attempts = self._max_scroll_attempts
        scroll_attempt = 0

        while scroll_attempt < max_scroll_attempts:
            if self._should_stop:
                self._report_progress("Scraping stopped by user.")
                return

            try:
                # Scroll down
                self.driver.execute_script(
                    "arguments[0].scrollTop = arguments[0].scrollHeight", results_panel
                )
                time.sleep(0.6)

                # Check for "end of list" indicator
                try:
                    end_marker = self.driver.find_element(
                        By.CSS_SELECTOR,
                        'span.HlvSq'
                    )
                    if end_marker:
                        self._report_progress("Reached end of results list.", 30)
                        break
                except NoSuchElementException:
                    pass

                # Check text-based end indicator
                try:
                    page_source_snippet = results_panel.get_attribute("innerHTML")
                    if "You've reached the end of the list" in page_source_snippet:
                        self._report_progress("Reached end of results list.", 30)
                        break
                except Exception:
                    pass

                # Count current listing links
                current_count = len(self.driver.find_elements(
                    By.CSS_SELECTOR, 'a[href*="/maps/place/"]'
                ))

                # Check if scrolling produced new content
                new_height = self.driver.execute_script(
                    "return arguments[0].scrollHeight", results_panel
                )
            except WebDriverException as exc:
                if self._is_localhost_read_timeout_error(exc):
                    logger.warning(f"WebDriver timeout while scrolling results: {exc}")
                    self._report_progress("Temporary browser timeout while scrolling. Continuing...", 30)
                    break
                raise

            if new_height == last_height and current_count == last_count:
                stagnation += 1
                if stagnation >= 3:
                    # Try a slight pan to force more results
                    try:
                        self.driver.execute_script(
                            "arguments[0].scrollTop = arguments[0].scrollHeight - 200",
                            results_panel
                        )
                        time.sleep(0.3)
                        self.driver.execute_script(
                            "arguments[0].scrollTop = arguments[0].scrollHeight",
                            results_panel
                        )
                        time.sleep(0.5)
                    except Exception:
                        pass
                    stagnation += 1

                if stagnation >= 5:
                    self._report_progress("No more results to load.", 30)
                    break
            else:
                stagnation = 0
                last_height = new_height
                last_count = current_count

            scroll_attempt += 1
            progress = min(30, 15 + scroll_attempt)
            self._report_progress(
                f"Loading more results... ({current_count} found)",
                progress,
            )

    def _get_listing_links(self) -> list:
        """Collect all listing links from the results panel."""
        self._report_progress("Collecting listing links...", 35)
        links = []
        try:
            elements = self.driver.find_elements(
                By.CSS_SELECTOR, 'a[href*="/maps/place/"]'
            )
            seen = set()
            for el in elements:
                href = el.get_attribute("href")
                if href and href not in seen:
                    seen.add(href)
                    links.append(href)
        except Exception as e:
            logger.error(f"Error collecting links: {e}")

        self._report_progress(f"Found {len(links)} business listings.", 40)
        return links

    def _extract_coords_from_url(self, url: str) -> tuple[str, str]:
        """Extract latitude/longitude from Google Maps URL."""
        match = COORDS_RE.search(url)
        if match:
            return match.group(1), match.group(2)
        return "", ""

    def _extract_business_detail(self, url: str) -> BusinessLead:
        """Navigate to a business page and extract details."""
        lead = BusinessLead()

        # Extract coordinates from URL
        lat, lng = self._extract_coords_from_url(url)
        lead.latitude = lat
        lead.longitude = lng

        try:
            self._navigate_with_retries(url, label="business detail page")
            time.sleep(0.8)

            # Business Name
            try:
                name_el = WebDriverWait(self.driver, 5).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "h1.DUwDvf")
                    )
                )
                lead.business_name = name_el.text.strip()
            except TimeoutException:
                try:
                    name_from_url = url.split("/maps/place/")[1].split("/")[0]
                    lead.business_name = name_from_url.replace("+", " ")
                except Exception:
                    lead.business_name = "Unknown"

            # Category
            try:
                cat_el = self.driver.find_element(
                    By.CSS_SELECTOR, "button[jsaction*='category']"
                )
                lead.category = cat_el.text.strip()
            except NoSuchElementException:
                try:
                    cat_el = self.driver.find_element(
                        By.CSS_SELECTOR, 'span.DkEaL'
                    )
                    lead.category = cat_el.text.strip()
                except NoSuchElementException:
                    pass

            # Rating and Reviews
            try:
                rating_el = self.driver.find_element(
                    By.CSS_SELECTOR, "div.F7nice span[aria-hidden]"
                )
                lead.rating = rating_el.text.strip()
            except NoSuchElementException:
                pass

            try:
                review_el = self.driver.find_element(
                    By.CSS_SELECTOR, "div.F7nice span span[aria-label]"
                )
                review_text = review_el.get_attribute("aria-label")
                if review_text:
                    nums = re.findall(r"[\d,]+", review_text)
                    lead.reviews = nums[0] if nums else ""
            except NoSuchElementException:
                pass

            # Extract info from the details panel (address, phone, website)
            info_buttons = self.driver.find_elements(
                By.CSS_SELECTOR, 'button[data-item-id]'
            )

            for btn in info_buttons:
                try:
                    data_id = btn.get_attribute("data-item-id") or ""
                    aria_label = btn.get_attribute("aria-label") or ""

                    # Address
                    if data_id.startswith("address") or "Address:" in aria_label:
                        lead.address = aria_label.replace("Address: ", "").strip()

                    # Phone
                    elif data_id.startswith("phone") or "Phone:" in aria_label:
                        phone_text = aria_label.replace("Phone: ", "").strip()
                        lead.phone = phone_text

                    # Website
                    elif data_id.startswith("authority") or "Website:" in aria_label:
                        website_text = aria_label.replace("Website: ", "").strip()
                        lead.website = website_text

                except StaleElementReferenceException:
                    continue

            # Try alternative method for website if not found
            if not lead.website:
                try:
                    website_link = self.driver.find_element(
                        By.CSS_SELECTOR, 'a[data-item-id="authority"]'
                    )
                    lead.website = website_link.get_attribute("href") or ""
                except NoSuchElementException:
                    pass

            # Try to extract coordinates from page URL if not from listing URL
            if not lead.latitude:
                try:
                    current_url = self.driver.current_url
                    lat, lng = self._extract_coords_from_url(current_url)
                    lead.latitude = lat
                    lead.longitude = lng
                except Exception:
                    pass

            # Owner name
            try:
                about_tab = self.driver.find_elements(
                    By.CSS_SELECTOR, 'div.PbZDve span'
                )
                for span in about_tab:
                    text = span.text.lower()
                    if "owner" in text or "proprietor" in text:
                        lead.owner_name = span.text.strip()
                        break
            except Exception:
                pass

        except WebDriverException as e:
            logger.error(f"Error scraping {url}: {e}")

        return lead

    def scrape(
        self,
        keyword: str,
        place: str,
        map_selection: dict | None = None,
        forced_geo_cells: list[BoundingBox] | None = None,
        force_primary_keyword_only: bool = False,
        max_leads: int | None = None,
        crawl_contacts: bool = True,
        on_lead_found: Callable[[dict, int], None] | None = None,
    ) -> list[dict]:
        """
        Main scraping pipeline with 3-phase architecture:

        Phase 1 — COLLECT: Scan all geo-cells with one browser, collect listing URLs
        Phase 2 — EXTRACT: Visit each unique listing to get business details
        Phase 3 — ENRICH: Crawl business websites in parallel for emails & socials

        This approach uses ONE browser for all cells (no duplicate browser spawning)
        and deduplicates listing URLs across cells before extracting details.
        """
        leads = []
        self._should_stop = False
        self._partial_leads = []
        seen_slugs: set[str] = set()
        if max_leads is not None and max_leads <= 0:
            max_leads = None

        try:
            self._report_progress("Initializing browser...", 2)
            self._init_driver()

            # ========================================
            # SETUP: Determine search area + keywords
            # ========================================
            bbox = None

            if map_selection and isinstance(map_selection, dict):
                bounds = map_selection.get("bounds")
                if bounds and isinstance(bounds, dict):
                    bbox = bbox_from_map_selection(bounds)
                    if bbox:
                        self._report_progress(
                            f"Using map selection area", 3
                        )
                if not bbox:
                    center = map_selection.get("center", {})
                    lat = center.get("lat")
                    lng = center.get("lng")
                    if lat is not None and lng is not None:
                        bbox = bbox_from_coordinates(float(lat), float(lng), radius_km=5.0)

            if not bbox:
                coord_match = re.match(
                    r'^(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)$', place.strip()
                )
                if coord_match:
                    lat = float(coord_match.group(1))
                    lng = float(coord_match.group(2))
                    bbox = bbox_from_coordinates(lat, lng, radius_km=5.0)

            if not bbox:
                self._report_progress(f"Geocoding '{place}'...", 3)
                bbox = bbox_from_place(place)

            # Build geo cells
            if forced_geo_cells:
                geo_cells = list(forced_geo_cells)
            elif bbox:
                area = bbox.area_sq_degrees()
                if area > 0.5:
                    target_cells = 16
                elif area > 0.1:
                    target_cells = 12
                elif area > 0.01:
                    target_cells = 8
                else:
                    target_cells = 4
                geo_cells = build_cells_for_area(bbox, target_cell_count=target_cells)
            else:
                geo_cells = []

            # Expand keywords
            keyword_variants = [keyword] if force_primary_keyword_only else expand_keywords(keyword, max_variants=3)
            self._area_stats["keywords_expanded"] = keyword_variants
            self._report_progress(
                f"Keyword variants: {', '.join(keyword_variants)}", 5
            )

            # ========================================
            # PHASE 1: COLLECT listing URLs from all cells
            # ========================================
            all_listing_urls: list[str] = []

            if geo_cells:
                # Build search jobs: primary keyword for all cells, extra variants for first cell only
                jobs: list[tuple[str, BoundingBox]] = []
                for cell in geo_cells:
                    jobs.append((keyword, cell))
                for variant in keyword_variants[1:2]:  # 1 extra variant, first cell only
                    jobs.append((variant, geo_cells[0]))

                total_jobs = len(jobs)
                self._area_stats["total_areas"] = total_jobs
                self._area_stats["geo_cells_total"] = len(geo_cells)

                self._report_progress(
                    f"Phase 1: Scanning {total_jobs} areas for listings...", 6
                )

                for ji, (kw, cell) in enumerate(jobs):
                    if self._should_stop:
                        break

                    center_lat, center_lng = cell.center()
                    self._area_stats["current_area"] = f"{kw} @ ({center_lat:.4f}, {center_lng:.4f})"
                    self._area_stats["current_area_index"] = ji + 1

                    pct = 6 + int((ji / total_jobs) * 30)  # Phase 1 uses 6-36%
                    self._report_progress(
                        f"Scanning area {ji + 1}/{total_jobs}: {kw}",
                        pct,
                    )

                    try:
                        self._search_maps_viewport(kw, cell)
                        self._scroll_results()
                    except WebDriverException as exc:
                        if self._is_localhost_read_timeout_error(exc):
                            logger.warning(
                                f"Skipping area {ji + 1}/{total_jobs} due to timeout: {exc}"
                            )
                            self._report_progress(
                                f"Area {ji + 1}/{total_jobs} timed out; continuing.",
                                min(pct + 1, 36),
                            )
                            continue
                        raise

                    if self._should_stop:
                        break

                    cell_urls = self._get_listing_links()

                    # Deduplicate across cells using URL slug
                    new_count = 0
                    for url in cell_urls:
                        if max_leads and len(all_listing_urls) >= max_leads:
                            break
                        try:
                            slug = url.split("/maps/place/")[1].split("/")[0]
                        except (IndexError, AttributeError):
                            slug = url
                        if slug not in seen_slugs:
                            seen_slugs.add(slug)
                            all_listing_urls.append(url)
                            new_count += 1

                    if max_leads and len(all_listing_urls) >= max_leads:
                        self._report_progress(
                            f"Lead limit reached ({max_leads}) during listing collection.",
                            min(pct + 1, 36),
                        )
                        break

                    self._area_stats["completed_areas"] = ji + 1
                    self._area_stats["geo_cells_completed"] = min(ji + 1, len(geo_cells))

                    logger.info(
                        f"Area {ji+1}/{total_jobs}: found {len(cell_urls)} listings, {new_count} new (total unique: {len(all_listing_urls)})"
                    )

            else:
                # Fallback: text-based search
                queries = [f"{v} in {place}" for v in keyword_variants]
                self._area_stats["total_areas"] = len(queries)

                for qi, query in enumerate(queries):
                    if self._should_stop:
                        break

                    self._area_stats["current_area"] = query
                    self._area_stats["current_area_index"] = qi + 1

                    pct = 6 + int((qi / len(queries)) * 30)
                    self._report_progress(
                        f"Searching: {query}", pct,
                    )

                    try:
                        self._search_maps(query)
                        self._scroll_results()
                    except WebDriverException as exc:
                        if self._is_localhost_read_timeout_error(exc):
                            logger.warning(f"Skipping query '{query}' due to timeout: {exc}")
                            self._report_progress(
                                f"Query '{query}' timed out; continuing.",
                                min(pct + 1, 36),
                            )
                            continue
                        raise

                    if self._should_stop:
                        break

                    query_urls = self._get_listing_links()
                    for url in query_urls:
                        if max_leads and len(all_listing_urls) >= max_leads:
                            break
                        try:
                            slug = url.split("/maps/place/")[1].split("/")[0]
                        except (IndexError, AttributeError):
                            slug = url
                        if slug not in seen_slugs:
                            seen_slugs.add(slug)
                            all_listing_urls.append(url)

                    if max_leads and len(all_listing_urls) >= max_leads:
                        self._report_progress(
                            f"Lead limit reached ({max_leads}) during listing collection.",
                            min(pct + 1, 36),
                        )
                        break

                    self._area_stats["completed_areas"] = qi + 1

            logger.info(f"Phase 1 complete: {len(all_listing_urls)} unique listings found")

            # ========================================
            # PHASE 2: EXTRACT business details from unique URLs
            # ========================================
            all_leads: list[BusinessLead] = []

            if all_listing_urls and not self._should_stop:
                total_urls = len(all_listing_urls)
                self._report_progress(
                    f"Phase 2: Extracting details from {total_urls} businesses...", 37
                )

                for idx, url in enumerate(all_listing_urls):
                    if self._should_stop:
                        break

                    pct = 37 + int((idx / total_urls) * 40)  # Phase 2 uses 37-77%
                    self._report_progress(
                        f"Extracting business {idx + 1}/{total_urls}",
                        min(pct, 77),
                    )

                    lead = self._extract_business_detail(url)
                    if lead.business_name and lead.business_name != "Unknown":
                        all_leads.append(lead)
                        lead_dict = asdict(lead)
                        self._partial_leads = [asdict(l) for l in all_leads]
                        self._area_stats["leads_found"] = len(all_leads)

                        # SSE hook: notify frontend immediately about this lead
                        if on_lead_found:
                            try:
                                on_lead_found(lead_dict, len(all_leads) - 1)
                            except Exception:
                                pass  # never let callback failure stop the scrape

                        if max_leads and len(all_leads) >= max_leads:
                            self._report_progress(
                                f"Lead limit reached ({max_leads}) during business extraction.",
                                min(pct + 1, 77),
                            )
                            break

                logger.info(f"Phase 2 complete: {len(all_leads)} businesses extracted")

            # ========================================
            # PHASE 3: ENRICH via parallel website crawling
            # ========================================
            if crawl_contacts and all_leads and not self._should_stop:
                self._crawl_websites_for_leads(
                    all_leads,
                    label="Enriching",
                    progress=78,
                )
                # Update partial leads after enrichment
                self._partial_leads = [asdict(l) for l in all_leads]

            # ========================================
            # FINAL: Light dedup (by exact name+address) and return
            # ========================================
            leads = [asdict(l) for l in all_leads]
            pre_dedup = len(leads)

            # Exact-hash dedup only (URL slug dedup already handled in Phase 1)
            if leads:
                leads = self._exact_deduplicate_leads(leads)
                deduped = pre_dedup - len(leads)
                if deduped > 0:
                    self._report_progress(
                        f"Removed {deduped} exact duplicates ({pre_dedup} → {len(leads)})", 95
                    )

            total_cells = self._area_stats.get("geo_cells_total", 0) or self._area_stats.get("total_areas", 1)
            completed = self._area_stats.get("geo_cells_completed", 0) or self._area_stats.get("completed_areas", 0)
            if total_cells > 0:
                self._area_stats["coverage_score"] = int((completed / total_cells) * 100)

            self._partial_leads = leads
            self._area_stats["leads_found"] = len(leads)

            if crawl_contacts:
                self._report_progress(
                    f"Done! Found {len(leads)} unique leads.", 100
                )
            else:
                self._report_progress(
                    f"List extraction complete. Found {len(leads)} leads.", 100
                )

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            if 'all_leads' in dir() and all_leads:
                leads = [asdict(l) for l in all_leads]
                leads = self._exact_deduplicate_leads(leads)
                self._partial_leads = leads
            self._report_progress(f"Error: {str(e)}", -1)
            raise

        finally:
            self._close_driver()

        return leads

    def _crawl_websites_for_leads(self, leads: list[BusinessLead], label: str, progress: int):
        website_targets = [lead for lead in leads if lead.website and lead.website != "N/A"]
        if not website_targets:
            logger.info("No websites to crawl — all leads missing website field.")
            return

        self._area_stats["websites_total"] = self._area_stats.get("websites_total", 0) + len(website_targets)
        workers = min(self._website_workers, len(website_targets))

        logger.info(f"Phase 3: Crawling {len(website_targets)} websites with {workers} parallel workers")
        self._report_progress(
            f"{label}: Crawling {len(website_targets)} websites for emails & socials...",
            progress,
        )

        enriched_count = 0
        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {
                executor.submit(self._scrape_website, lead): lead
                for lead in website_targets
                if not self._should_stop
            }

            processed = 0
            for future in as_completed(futures):
                # Check stop FIRST — don't wait on more futures
                if self._should_stop:
                    executor.shutdown(wait=False, cancel_futures=True)
                    logger.info(f"Stop requested — aborting website crawl after {processed}/{len(website_targets)}")
                    break

                processed += 1
                lead = futures[future]
                try:
                    # Short timeout — if one site is truly stuck, skip it
                    future.result(timeout=self._website_timeout_seconds + 2)
                except Exception as e:
                    logger.debug(f"Website crawl skipped for {lead.business_name} ({lead.website}): {e}")

                # Check if enrichment found anything
                if lead.email or lead.facebook or lead.instagram:
                    enriched_count += 1

                self._area_stats["websites_scanned"] = self._area_stats.get("websites_scanned", 0) + 1

                # Report progress frequently (every 5 websites)
                if processed == len(website_targets) or processed % 5 == 0:
                    pct = progress + int((processed / len(website_targets)) * (95 - progress))
                    self._report_progress(
                        f"{label}: Websites {processed}/{len(website_targets)} (contacts found: {enriched_count})",
                        min(pct, 95),
                    )

        logger.info(f"Phase 3 complete: {enriched_count}/{len(website_targets)} websites yielded contact data")

    def crawl_contacts_for_leads(
        self,
        leads: list[dict],
        progress_callback: Callable[[str, int], None] | None = None,
        should_stop: Callable[[], bool] | None = None,
        should_pause: Callable[[], bool] | None = None,
        on_contact_found: Callable[[int, dict], None] | None = None,
    ) -> list[dict]:
        """Enrich existing leads with website contacts using parallel crawling."""
        self._should_stop = False

        if progress_callback:
            self.set_progress_callback(progress_callback)

        lead_objs: list[BusinessLead] = []
        for lead in leads:
            lead_objs.append(BusinessLead(
                business_name=lead.get("business_name", "") or "",
                owner_name=lead.get("owner_name", "") or "",
                phone=lead.get("phone", "") or "",
                website=lead.get("website", "") or "",
                email=lead.get("email", "") or "",
                address=lead.get("address", "") or "",
                rating=lead.get("rating", "") or "",
                reviews=lead.get("reviews", "") or "",
                category=lead.get("category", "") or "",
                latitude=lead.get("latitude", "") or "",
                longitude=lead.get("longitude", "") or "",
                facebook=lead.get("facebook", "") or "",
                instagram=lead.get("instagram", "") or "",
                twitter=lead.get("twitter", "") or "",
                linkedin=lead.get("linkedin", "") or "",
                youtube=lead.get("youtube", "") or "",
                tiktok=lead.get("tiktok", "") or "",
                pinterest=lead.get("pinterest", "") or "",
            ))

            self._partial_leads = [asdict(l) for l in lead_objs]

        website_targets = [lead for lead in lead_objs if lead.website and lead.website != "N/A"]
        total_targets = len(website_targets)
        if total_targets == 0:
            self._report_progress("Contact retrieval skipped: no website URLs available.", 100)
            return [asdict(l) for l in lead_objs]

        self._area_stats["websites_total"] = total_targets
        self._area_stats["websites_scanned"] = 0

        workers = min(self._website_workers, total_targets)
        processed = 0
        enriched_count = 0

        self._report_progress(
            f"Contact retrieval started: scanning {total_targets} websites...",
            2,
        )

        # O(1) lookup: object id → index in lead_objs (used by SSE callback)
        _lead_index_map = {id(lead): i for i, lead in enumerate(lead_objs)}

        with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
            futures = {
                executor.submit(self._scrape_website, lead): lead
                for lead in website_targets
            }

            for future in as_completed(futures):
                if should_stop and should_stop():
                    self._should_stop = True
                    executor.shutdown(wait=False, cancel_futures=True)
                    self._report_progress(
                        f"Contact retrieval stopped at {processed}/{total_targets} websites.",
                        min(99, 2 + int((processed / max(1, total_targets)) * 95)),
                    )
                    break

                while should_pause and should_pause():
                    time.sleep(0.25)

                processed += 1
                lead = futures[future]

                try:
                    future.result(timeout=self._website_timeout_seconds + 2)
                except Exception as e:
                    logger.debug(f"Website crawl skipped for {lead.business_name} ({lead.website}): {e}")

                enriched = lead.email and lead.email != "N/A"
                if enriched:
                    enriched_count += 1

                self._area_stats["websites_scanned"] = processed
                self._partial_leads = [asdict(l) for l in lead_objs]

                # SSE hook: push only the changed contact fields (minimal payload)
                if on_contact_found:
                    try:
                        lead_index = _lead_index_map.get(id(lead), -1)
                        if lead_index >= 0:
                            socials = {
                                k: getattr(lead, k, "") for k in (
                                    "facebook", "instagram", "twitter",
                                    "linkedin", "youtube", "tiktok", "pinterest"
                                )
                                if getattr(lead, k, "")
                            }
                            on_contact_found(lead_index, {
                                "email": lead.email or "",
                                "phone": lead.phone or "",
                                "socials": socials,
                            })
                    except Exception:
                        pass  # never let callback failure stop crawling

                pct = 2 + int((processed / total_targets) * 95)
                self._report_progress(
                    f"Contact retrieval: {processed}/{total_targets} websites "
                    f"(emails found: {enriched_count})",
                    min(99, pct),
                )

        if not self._should_stop:
            self._report_progress(
                f"Contact retrieval complete: scanned {processed}/{total_targets} websites.",
                100,
            )

        self._partial_leads = [asdict(l) for l in lead_objs]

        return [asdict(l) for l in lead_objs]

    def _scrape_website(self, lead: BusinessLead):
        """Visit a business website to extract emails and social profiles."""
        url = lead.website
        if not url or url == "N/A":
            return

        if not url.startswith("http"):
            url = "https://" + url

        # Pages to check: homepage + high-signal contact/about slugs
        candidate_paths = [
            "", "/contact", "/contact-us", "/contactus", "/get-in-touch",
            "/reach-us", "/support", "/help", "/about", "/about-us",
            "/who-we-are", "/our-team", "/team", "/company", "/about/company",
            "/about/team", "/connect",
        ]

        pages_to_check: deque[str] = deque()
        seen_pages: set[str] = set()
        for path in candidate_paths:
            page = url if not path else urljoin(url, path)
            if page not in seen_pages:
                pages_to_check.append(page)
                seen_pages.add(page)

        all_emails = set()
        all_phones = set()
        found_socials = {k: "" for k in SOCIAL_PATTERNS}
        deadline = time.monotonic() + self._website_timeout_seconds
        session = self._new_http_session()
        homepage_reachable = False
        pages_checked = 0

        while pages_to_check and pages_checked < self._max_pages_per_website:
            if self._should_stop:
                break
            page_url = pages_to_check.popleft()

            # Hard deadline check at top to avoid wasted future.result() timeout
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            try:
                # 1.5s connect (fail fast on dead hosts), 3s read max per page
                read_timeout = min(3.0, max(1.0, remaining))
                resp = session.get(
                    page_url,
                    timeout=(1.5, read_timeout),
                    allow_redirects=True,
                )
                # Skip client errors (404) and server errors (5xx) immediately
                if resp.status_code >= 400:
                    continue

                pages_checked += 1
                homepage_reachable = True
                content_type = (resp.headers.get("Content-Type") or "").lower()
                if "text/html" not in content_type and "application/xhtml+xml" not in content_type:
                    continue

                text = resp.text
                soup = BeautifulSoup(text, "lxml")
                text_content = soup.get_text(" ", strip=True)

                # ---- Extract Emails ----
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("mailto:"):
                        email = href.replace("mailto:", "").split("?")[0].strip()
                        if self._is_valid_email(email):
                            all_emails.add(email.lower())

                for match in EMAIL_RE.findall(text):
                    if self._is_valid_email(match):
                        all_emails.add(match.lower())

                for match in EMAIL_RE.findall(text_content):
                    if self._is_valid_email(match):
                        all_emails.add(match.lower())

                # ---- Extract Phones ----
                for phone in self._extract_phones(text_content):
                    all_phones.add(phone)

                # ---- Extract Social Links ----
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    for platform, pattern in SOCIAL_PATTERNS.items():
                        if not found_socials[platform]:
                            m = pattern.match(href)
                            if m:
                                found_socials[platform] = m.group(0)

                for platform, pattern in SOCIAL_PATTERNS.items():
                    if not found_socials[platform]:
                        m = pattern.search(text)
                        if m:
                            found_socials[platform] = m.group(0)

                # ---- Discover internal high-signal links ----
                parsed_base = urlparse(url)
                for a in soup.find_all("a", href=True):
                    href = (a.get("href") or "").strip()
                    if not href:
                        continue

                    abs_url = urljoin(page_url, href)
                    parsed_link = urlparse(abs_url)
                    if parsed_link.netloc and parsed_link.netloc != parsed_base.netloc:
                        continue

                    path_l = (parsed_link.path or "").lower()
                    if any(token in path_l for token in [
                        "contact", "about", "team", "company", "support",
                        "connect", "help", "who-we-are", "our-story",
                    ]):
                        if abs_url not in seen_pages:
                            seen_pages.add(abs_url)
                            pages_to_check.append(abs_url)

                if all_emails and all_phones and all(found_socials.values()):
                    break

            except (requests.ConnectionError, requests.Timeout) as e:
                # Host is unreachable or too slow — skip ALL pages for this domain
                if not homepage_reachable:
                    logger.debug(f"Unreachable: {url} — skipping")
                    break
                continue
            except requests.RequestException:
                continue
            except Exception as e:
                logger.debug(f"Error scraping {page_url}: {e}")
                continue
            finally:
                if time.monotonic() >= deadline:
                    break

        session.close()

        # Assign to lead
        if all_emails:
            lead.email = "; ".join(sorted(all_emails))

        if all_phones:
            normalized_existing = self._extract_phones(lead.phone or "")
            merged_phones = sorted(set(normalized_existing).union(all_phones))
            if merged_phones:
                lead.phone = "; ".join(merged_phones)

        for platform, url_val in found_socials.items():
            if url_val:
                setattr(lead, platform, url_val)

        if all_emails or all_phones or any(found_socials.values()):
            logger.debug(
                f"Enriched {lead.business_name}: emails={len(all_emails)}, phones={len(all_phones)}, "
                f"socials={sum(1 for v in found_socials.values() if v)} (checked {pages_checked} pages)"
            )

    @staticmethod
    def _extract_phones(text: str) -> set[str]:
        phones: set[str] = set()
        if not text:
            return phones

        for raw in PHONE_RE.findall(text):
            digits = re.sub(r"\D", "", raw)
            if 7 <= len(digits) <= 15:
                normalized = re.sub(r"\s+", " ", raw.strip())
                phones.add(normalized)

        return phones

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """Check if an email looks real (not a false positive)."""
        if not email or "@" not in email:
            return False
        domain = email.split("@")[1].lower()
        if domain in EMAIL_BLACKLIST:
            return False
        if domain.endswith((".png", ".jpg", ".gif", ".svg", ".webp", ".js", ".css")):
            return False
        return True

    def _close_driver(self):
        """Close the browser."""
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None


def clean_leads(leads: list[dict]) -> list[dict]:
    """Clean and deduplicate scraped leads."""
    cleaned = []
    seen_names = set()

    for lead in leads:
        name = lead.get("business_name", "").strip()
        if not name or name.lower() == "unknown" or name in seen_names:
            continue

        seen_names.add(name)

        # Clean phone number
        phone = lead.get("phone", "")
        if phone:
            phone = re.sub(r"[^\d+\-() ]", "", phone).strip()

        # Clean website
        website = lead.get("website", "")
        if website and not website.startswith("http"):
            website = "https://" + website

        cleaned.append({
            "lead_uid": hashlib.sha1(
                f"{name.lower()}|{lead.get('latitude', '')}|{lead.get('longitude', '')}|{lead.get('address', '')}".encode("utf-8")
            ).hexdigest()[:16],
            "business_name": name,
            "owner_name": lead.get("owner_name", "N/A") or "N/A",
            "phone": phone or "N/A",
            "website": website or "N/A",
            "email": lead.get("email", "N/A") or "N/A",
            "address": lead.get("address", "N/A") or "N/A",
            "rating": lead.get("rating", "N/A") or "N/A",
            "reviews": lead.get("reviews", "N/A") or "N/A",
            "category": lead.get("category", "N/A") or "N/A",
            "latitude": lead.get("latitude", "") or "",
            "longitude": lead.get("longitude", "") or "",
            "facebook": lead.get("facebook", "N/A") or "N/A",
            "instagram": lead.get("instagram", "N/A") or "N/A",
            "twitter": lead.get("twitter", "N/A") or "N/A",
            "linkedin": lead.get("linkedin", "N/A") or "N/A",
            "youtube": lead.get("youtube", "N/A") or "N/A",
            "tiktok": lead.get("tiktok", "N/A") or "N/A",
            "pinterest": lead.get("pinterest", "N/A") or "N/A",
        })

    return cleaned
