"""
LinkedIn Lead Scraper Module
Finds LinkedIn profiles and companies by niche & location using
Google search (site:linkedin.com).  Direct LinkedIn scraping is
rate-limited/blocked, so we route through Google SERPs with Selenium.
"""

import re
import time
import logging
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    WebDriverException,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class LinkedInProfile:
    """Represents a LinkedIn profile lead."""
    name: str = ""
    title: str = ""
    company: str = ""
    location: str = ""
    profile_url: str = ""
    snippet: str = ""


@dataclass
class LinkedInCompany:
    """Represents a LinkedIn company lead."""
    company_name: str = ""
    industry: str = ""
    location: str = ""
    description: str = ""
    company_url: str = ""
    company_size: str = ""


class LinkedInScraper:
    """Scrapes LinkedIn profiles and companies via Google search."""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self._progress_callback = None
        self._should_stop = False
        self._http_session = requests.Session()
        self._http_session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/144.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        adapter = HTTPAdapter(
            pool_connections=5, pool_maxsize=10,
            max_retries=Retry(total=1, backoff_factor=0.2),
        )
        self._http_session.mount("https://", adapter)
        self._http_session.mount("http://", adapter)

    def set_progress_callback(self, callback):
        self._progress_callback = callback

    def stop(self):
        self._should_stop = True

    def _report_progress(self, message: str, percentage: int = -1):
        logger.info(message)
        if self._progress_callback:
            self._progress_callback(message, percentage)

    # ---- Browser helpers -----------------------------------------------

    def _init_driver(self):
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
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(2)

    def _close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    # ---- Google SERP helpers -------------------------------------------

    def _google_search(self, query: str, num_pages: int = 5) -> list[dict]:
        """
        Run a Google search and collect organic result links + snippets.
        Returns list of dicts with keys: url, title, snippet.
        """
        results = []
        for page in range(num_pages):
            if self._should_stop:
                break

            start = page * 10
            search_url = (
                f"https://www.google.com/search?q={query.replace(' ', '+')}"
                f"&start={start}&hl=en"
            )
            self.driver.get(search_url)
            time.sleep(2.0 + page * 0.5)   # slow down to avoid captcha

            # Accept consent if prompted
            if page == 0:
                try:
                    btn = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(., 'Accept all') or contains(., 'Accept') or contains(., 'I agree')]",
                    )
                    btn.click()
                    time.sleep(1.5)
                except NoSuchElementException:
                    pass

            # Collect results
            try:
                divs = self.driver.find_elements(By.CSS_SELECTOR, "div.g")
                for div in divs:
                    try:
                        a_tag = div.find_element(By.CSS_SELECTOR, "a[href]")
                        href = a_tag.get_attribute("href") or ""
                        title = ""
                        snippet = ""
                        try:
                            title = div.find_element(By.CSS_SELECTOR, "h3").text.strip()
                        except NoSuchElementException:
                            pass
                        # Snippet text
                        try:
                            snippet_el = div.find_element(
                                By.CSS_SELECTOR,
                                "div[data-sncf], span.aCOpRe, div.VwiC3b",
                            )
                            snippet = snippet_el.text.strip()
                        except NoSuchElementException:
                            pass

                        if href:
                            results.append({
                                "url": href,
                                "title": title,
                                "snippet": snippet,
                            })
                    except Exception:
                        continue
            except Exception as e:
                logger.debug(f"Error parsing SERP page {page}: {e}")

            # Check if there's a next page
            try:
                self.driver.find_element(By.ID, "pnnext")
            except NoSuchElementException:
                break  # no more pages

        return results

    # ---- Profile scraping -----------------------------------------------

    def _parse_profile_from_serp(self, result: dict) -> LinkedInProfile | None:
        """Parse a profile from Google SERP result."""
        url = result["url"]
        title = result["title"]
        snippet = result["snippet"]

        # Only accept linkedin.com/in/ URLs
        if "linkedin.com/in/" not in url:
            return None

        profile = LinkedInProfile()
        profile.profile_url = url.split("?")[0]  # clean tracking params
        profile.snippet = snippet

        # Parse name from title (format: "Name - Title - Company | LinkedIn")
        if title:
            title_clean = title.replace(" | LinkedIn", "").replace(" - LinkedIn", "")
            parts = [p.strip() for p in title_clean.split(" - ") if p.strip()]
            if parts:
                profile.name = parts[0]
            if len(parts) > 1:
                profile.title = parts[1]
            if len(parts) > 2:
                profile.company = parts[2]

        # Try extracting location from snippet
        if snippet:
            loc_match = re.search(
                r'(?:located?\s+in|based\s+in|from)\s+([A-Z][^.·\-\n]+)',
                snippet, re.I,
            )
            if loc_match:
                profile.location = loc_match.group(1).strip()[:80]

            # If no title from title tag, try snippet
            if not profile.title and snippet:
                # Often snippet starts with role info
                first_line = snippet.split("·")[0].split("…")[0].strip()
                if first_line and len(first_line) < 120:
                    profile.title = profile.title or first_line

        return profile if profile.name else None

    # ---- Company scraping -----------------------------------------------

    def _parse_company_from_serp(self, result: dict) -> LinkedInCompany | None:
        """Parse a company from Google SERP result."""
        url = result["url"]
        title = result["title"]
        snippet = result["snippet"]

        if "linkedin.com/company/" not in url:
            return None

        company = LinkedInCompany()
        company.company_url = url.split("?")[0]

        # Parse company name from title
        if title:
            title_clean = title.replace(" | LinkedIn", "").replace(" - LinkedIn", "")
            company.company_name = title_clean.strip()

        # Extract info from snippet
        if snippet:
            company.description = snippet[:200]

            # Industry info
            ind_match = re.search(
                r'(?:industry|sector)[:\s]+([^.·\n]+)', snippet, re.I,
            )
            if ind_match:
                company.industry = ind_match.group(1).strip()[:80]

            # Employee count
            size_match = re.search(
                r'(\d[\d,]*\+?\s*(?:employees|workers|people|staff))',
                snippet, re.I,
            )
            if size_match:
                company.company_size = size_match.group(1).strip()

            # Location from snippet
            loc_match = re.search(
                r'(?:headquartered?\s+in|based\s+in|located?\s+in|,\s*)'
                r'([A-Z][A-Za-z\s,]+(?:Area)?)',
                snippet, re.I,
            )
            if loc_match:
                company.location = loc_match.group(1).strip()[:80]

        return company if company.company_name else None

    # ---- Main public API -----------------------------------------------

    def scrape(
        self,
        niche: str,
        place: str,
        search_type: str = "profiles",
        max_pages: int = 5,
    ) -> list[dict]:
        """
        Main scraping method.

        Args:
            niche: Industry/role to search (e.g., "marketing manager")
            place: Location (e.g., "New York")
            search_type: "profiles" or "companies"
            max_pages: Number of Google result pages to scan (10 results each)

        Returns:
            List of dicts (profile or company data)
        """
        self._should_stop = False
        leads = []

        if search_type == "profiles":
            query = f'site:linkedin.com/in/ "{niche}" "{place}"'
        else:
            query = f'site:linkedin.com/company/ "{niche}" "{place}"'

        try:
            self._report_progress("Initializing browser...", 2)
            self._init_driver()

            self._report_progress(f"Searching Google for LinkedIn {search_type}...", 10)
            raw_results = self._google_search(query, num_pages=max_pages)

            if not raw_results:
                self._report_progress("No results found. Try different keywords.", 100)
                return leads

            total = len(raw_results)
            self._report_progress(
                f"Found {total} search results. Parsing...", 60,
            )

            seen_urls = set()
            for idx, result in enumerate(raw_results):
                if self._should_stop:
                    break

                url_clean = result["url"].split("?")[0]
                if url_clean in seen_urls:
                    continue
                seen_urls.add(url_clean)

                if search_type == "profiles":
                    parsed = self._parse_profile_from_serp(result)
                else:
                    parsed = self._parse_company_from_serp(result)

                if parsed:
                    leads.append(asdict(parsed))

                progress = 60 + int((idx / total) * 35)
                if idx % 5 == 0:
                    self._report_progress(
                        f"Parsed {idx + 1}/{total} results...",
                        min(progress, 95),
                    )

            self._report_progress(
                f"Done! Found {len(leads)} {search_type}.", 100,
            )

        except Exception as e:
            logger.error(f"LinkedIn scraping failed: {e}")
            self._report_progress(f"Error: {str(e)}", -1)
            raise
        finally:
            self._close_driver()

        return leads


def clean_linkedin_leads(leads: list[dict], search_type: str = "profiles") -> list[dict]:
    """Clean and deduplicate LinkedIn leads."""
    cleaned = []
    seen = set()

    for lead in leads:
        if search_type == "profiles":
            key = lead.get("profile_url", "")
            name = lead.get("name", "").strip()
            if not name or key in seen:
                continue
            seen.add(key)
            cleaned.append({
                "name": name,
                "title": lead.get("title", "N/A") or "N/A",
                "company": lead.get("company", "N/A") or "N/A",
                "location": lead.get("location", "N/A") or "N/A",
                "profile_url": key or "N/A",
                "snippet": lead.get("snippet", "N/A") or "N/A",
            })
        else:
            key = lead.get("company_url", "")
            name = lead.get("company_name", "").strip()
            if not name or key in seen:
                continue
            seen.add(key)
            cleaned.append({
                "company_name": name,
                "industry": lead.get("industry", "N/A") or "N/A",
                "location": lead.get("location", "N/A") or "N/A",
                "description": lead.get("description", "N/A") or "N/A",
                "company_url": key or "N/A",
                "company_size": lead.get("company_size", "N/A") or "N/A",
            })

    return cleaned
