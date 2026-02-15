"""
LinkedIn Lead Scraper Module
Finds LinkedIn profiles and companies by niche & location using
Google search (site:linkedin.com).  Direct LinkedIn scraping is
rate-limited/blocked, so we route through Google SERPs with Selenium.

Supports multi-role executive search (CEO, Director, Manager, etc.)
with anti-detection measures and multiple fallback parsing strategies.
"""

import re
import time
import random
import logging
from dataclasses import dataclass, asdict
from urllib.parse import quote_plus

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


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class LinkedInProfile:
    """Represents a LinkedIn profile lead."""
    name: str = ""
    title: str = ""
    company: str = ""
    location: str = ""
    profile_url: str = ""
    linkedin_username: str = ""
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


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class LinkedInScraper:
    """Scrapes LinkedIn profiles and companies via Google search."""

    # User-Agent rotation pool
    USER_AGENTS = [
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/144.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/143.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/18.3 Safari/605.1.15"
        ),
    ]

    # Batched executive role groups for OR-combined queries
    ROLE_GROUPS = [
        '"CEO" OR "Chief Executive Officer" OR "Founder" OR "Co-Founder"',
        '"Director" OR "Managing Director" OR "President"',
        '"Manager" OR "General Manager" OR "Senior Manager"',
        '"VP" OR "Vice President" OR "COO" OR "CFO" OR "CTO"',
        '"Head of" OR "Partner" OR "Owner"',
        '"CMO" OR "Chief Marketing Officer" OR "Chief Operating Officer"',
        '"Consultant" OR "Advisor" OR "Specialist" OR "Lead"',
        '"Entrepreneur" OR "Business Development" OR "Sales Director"',
    ]

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self._progress_callback = None
        self._should_stop = False

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
        """Initialize Chrome with anti-detection measures."""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=en-US")
        chrome_options.add_argument("--log-level=3")

        # Anti-detection arguments
        chrome_options.add_argument(
            "--disable-blink-features=AutomationControlled"
        )
        chrome_options.add_experimental_option(
            "excludeSwitches", ["enable-automation", "enable-logging"]
        )
        chrome_options.add_experimental_option("useAutomationExtension", False)

        # Randomised User-Agent
        ua = random.choice(self.USER_AGENTS)
        chrome_options.add_argument(f"--user-agent={ua}")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(2)

        # Remove navigator.webdriver flag so detection scripts see undefined
        try:
            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": """
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        });
                        Object.defineProperty(navigator, 'languages', {
                            get: () => ['en-US', 'en']
                        });
                    """
                },
            )
        except Exception:
            pass  # CDP not always available

    def _close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    # ---- Consent / CAPTCHA handling ------------------------------------

    def _handle_consent(self):
        """Click through Google consent / cookie dialogs."""
        selectors = [
            "//button[contains(., 'Accept all')]",
            "//button[contains(., 'Accept')]",
            "//button[contains(., 'I agree')]",
            "//button[contains(., 'Agree')]",
            "//button[@id='L2AGLb']",
            "//button[@id='W0wltc']",
        ]
        for sel in selectors:
            try:
                btn = self.driver.find_element(By.XPATH, sel)
                if btn.is_displayed():
                    btn.click()
                    time.sleep(2)
                    return True
            except (NoSuchElementException, WebDriverException):
                continue
        return False

    def _check_captcha(self) -> bool:
        """Return True if Google is showing a CAPTCHA page."""
        try:
            src = self.driver.page_source.lower()
            return any(
                kw in src
                for kw in (
                    "captcha", "unusual traffic",
                    "recaptcha", "are you a robot",
                )
            )
        except Exception:
            return False

    # ---- Google SERP search --------------------------------------------

    def _google_search(self, query: str, num_pages: int = 3) -> list[dict]:
        """
        Run a Google search and collect results using multiple fallback
        strategies.  Returns list of dicts: {url, title, snippet}.
        """
        results: list[dict] = []

        for page in range(num_pages):
            if self._should_stop:
                break

            start = page * 10
            search_url = (
                f"https://www.google.com/search"
                f"?q={quote_plus(query)}&start={start}&hl=en&num=10"
            )

            try:
                self.driver.get(search_url)
                time.sleep(3.0 + random.uniform(1.0, 3.0))

                # Consent dialog (first page only)
                if page == 0:
                    self._handle_consent()
                    time.sleep(1)

                # CAPTCHA detection — back off and retry once
                if self._check_captcha():
                    logger.warning("CAPTCHA detected — backing off…")
                    time.sleep(10 + random.uniform(5, 10))
                    self.driver.get(search_url)
                    time.sleep(5)
                    if self._check_captcha():
                        logger.error("Still blocked by CAPTCHA. Stopping.")
                        break

                # ---- Strategy 1: div.g containers ----
                page_results = self._parse_serp_divg()

                # ---- Strategy 2: broader selectors ----
                if not page_results:
                    page_results = self._parse_serp_broad()

                # ---- Strategy 3: regex on page source ----
                if not page_results:
                    page_results = self._parse_serp_regex()

                results.extend(page_results)

                # Check if there is a "Next" page button
                try:
                    self.driver.find_element(By.ID, "pnnext")
                except NoSuchElementException:
                    break  # no more pages

            except WebDriverException as e:
                logger.error(f"Error on SERP page {page}: {e}")
                continue

            # Random delay between pages
            if page < num_pages - 1:
                time.sleep(random.uniform(2.0, 5.0))

        return results

    # ---- SERP parsing strategies ----------------------------------------

    def _parse_serp_divg(self) -> list[dict]:
        """Strategy 1 — standard div.g result containers."""
        results = []
        try:
            divs = self.driver.find_elements(By.CSS_SELECTOR, "div.g")
            for div in divs:
                try:
                    a_tag = div.find_element(By.CSS_SELECTOR, "a[href]")
                    href = a_tag.get_attribute("href") or ""
                    if "linkedin.com" not in href:
                        continue

                    title = ""
                    snippet = ""
                    try:
                        title = div.find_element(
                            By.CSS_SELECTOR, "h3"
                        ).text.strip()
                    except NoSuchElementException:
                        pass
                    try:
                        snippet_el = div.find_element(
                            By.CSS_SELECTOR,
                            "div[data-sncf], span.aCOpRe, div.VwiC3b, "
                            "div[style*='-webkit-line-clamp']",
                        )
                        snippet = snippet_el.text.strip()
                    except NoSuchElementException:
                        # broader fallback
                        try:
                            for span in div.find_elements(
                                By.CSS_SELECTOR, "span"
                            ):
                                t = span.text.strip()
                                if len(t) > 40:
                                    snippet = t
                                    break
                        except Exception:
                            pass

                    results.append(
                        {"url": href, "title": title, "snippet": snippet}
                    )
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Strategy-1 error: {e}")
        return results

    def _parse_serp_broad(self) -> list[dict]:
        """Strategy 2 — look for any <a> pointing to linkedin.com."""
        results = []
        try:
            links = self.driver.find_elements(
                By.CSS_SELECTOR, "a[href*='linkedin.com']"
            )
            for a_tag in links:
                href = a_tag.get_attribute("href") or ""
                if (
                    "linkedin.com/in/" not in href
                    and "linkedin.com/company/" not in href
                ):
                    continue
                # Skip Google-internal redirect links
                if "google.com" in href and "linkedin.com" not in href.split("?")[0]:
                    continue

                title = a_tag.text.strip()[:150] or ""
                snippet = ""
                try:
                    parent = a_tag.find_element(
                        By.XPATH,
                        "./ancestor::div[contains(@class,'g')]",
                    )
                    snippet = parent.text.strip()[:300]
                except Exception:
                    pass

                results.append(
                    {"url": href, "title": title, "snippet": snippet}
                )
        except Exception as e:
            logger.debug(f"Strategy-2 error: {e}")
        return results

    def _parse_serp_regex(self) -> list[dict]:
        """Strategy 3 — regex on page source for LinkedIn URLs."""
        results = []
        try:
            source = self.driver.page_source
            urls = set(
                re.findall(
                    r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[\w\-]+',
                    source,
                )
            )
            for url in urls:
                clean = url.split("?")[0].split("&amp;")[0]
                results.append({"url": clean, "title": "", "snippet": ""})
        except Exception as e:
            logger.debug(f"Strategy-3 error: {e}")
        return results

    # ---- Bing search (secondary engine) --------------------------------

    def _bing_search(self, query: str, num_pages: int = 5) -> list[dict]:
        """
        Run a Bing search as secondary engine.  Bing is far less
        aggressive with CAPTCHAs than Google, so we can pull more pages.
        """
        results: list[dict] = []

        for page in range(num_pages):
            if self._should_stop:
                break

            first = page * 10 + 1
            search_url = (
                f"https://www.bing.com/search"
                f"?q={quote_plus(query)}&first={first}&count=10"
            )

            try:
                self.driver.get(search_url)
                time.sleep(2.0 + random.uniform(1.0, 2.0))

                if page == 0:
                    # Bing cookie consent
                    try:
                        btn = self.driver.find_element(
                            By.ID, "bnp_btn_accept"
                        )
                        if btn.is_displayed():
                            btn.click()
                            time.sleep(1)
                    except (NoSuchElementException, WebDriverException):
                        pass

                # Parse Bing results
                page_results = self._parse_bing_results()
                results.extend(page_results)

                # Check for next page
                try:
                    self.driver.find_element(
                        By.CSS_SELECTOR, "a.sb_pagN"
                    )
                except NoSuchElementException:
                    break

            except WebDriverException as e:
                logger.error(f"Bing error page {page}: {e}")
                continue

            if page < num_pages - 1:
                time.sleep(random.uniform(1.5, 3.0))

        return results

    def _parse_bing_results(self) -> list[dict]:
        """Parse Bing SERP for LinkedIn URLs."""
        results = []
        try:
            # Bing organic results
            items = self.driver.find_elements(
                By.CSS_SELECTOR, "li.b_algo"
            )
            for item in items:
                try:
                    a_tag = item.find_element(By.CSS_SELECTOR, "h2 a")
                    href = a_tag.get_attribute("href") or ""
                    if "linkedin.com" not in href:
                        continue

                    title = a_tag.text.strip()
                    snippet = ""
                    try:
                        snippet_el = item.find_element(
                            By.CSS_SELECTOR, "div.b_caption p"
                        )
                        snippet = snippet_el.text.strip()[:300]
                    except NoSuchElementException:
                        pass

                    results.append(
                        {"url": href, "title": title, "snippet": snippet}
                    )
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Bing parse error: {e}")

        # Fallback: regex
        if not results:
            try:
                source = self.driver.page_source
                urls = set(
                    re.findall(
                        r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[\w\-]+',
                        source,
                    )
                )
                for url in urls:
                    clean = url.split("?")[0].split("&amp;")[0]
                    results.append({"url": clean, "title": "", "snippet": ""})
            except Exception:
                pass

        return results

    # ---- Profile parsing -----------------------------------------------

    def _parse_profile_from_serp(
        self, result: dict
    ) -> LinkedInProfile | None:
        """Parse a profile from a Google SERP result."""
        url = result["url"]
        title = result["title"]
        snippet = result["snippet"]

        if "linkedin.com/in/" not in url:
            return None

        profile = LinkedInProfile()
        profile.profile_url = url.split("?")[0]
        profile.snippet = snippet

        # Extract LinkedIn username from URL
        m = re.search(r'linkedin\.com/in/([\w\-]+)', url)
        if m:
            profile.linkedin_username = m.group(1)

        # Parse name / title / company from Google result title
        # Typical format: "Name - Title - Company | LinkedIn"
        if title:
            title_clean = re.sub(
                r'\s*[\|–—]\s*LinkedIn.*$', '', title
            ).strip()
            parts = [
                p.strip() for p in title_clean.split(" - ") if p.strip()
            ]
            if parts:
                profile.name = parts[0]
            if len(parts) > 1:
                profile.title = parts[1]
            if len(parts) > 2:
                profile.company = parts[2]

        # Snippet-based extraction
        if snippet:
            # Location
            loc_match = re.search(
                r'(?:located?\s+in|based\s+in|from|📍)'
                r'\s+([A-Z][^.·\-\n]+)',
                snippet,
                re.I,
            )
            if loc_match:
                profile.location = loc_match.group(1).strip()[:80]

            # Title fallback
            if not profile.title:
                first_line = snippet.split("·")[0].split("…")[0].strip()
                if first_line and len(first_line) < 120:
                    profile.title = first_line

            # Company fallback — "at <Company>"
            if not profile.company:
                comp_match = re.search(
                    r'(?:\bat\b|@)\s+([A-Z][^.·\-\n,]{2,60})',
                    snippet,
                    re.I,
                )
                if comp_match:
                    profile.company = comp_match.group(1).strip()

        # Last-resort: derive name from username
        if not profile.name and profile.linkedin_username:
            name = profile.linkedin_username.replace("-", " ").title()
            if len(name.split()) >= 2:
                profile.name = name

        return profile if profile.name else None

    # ---- Company parsing -----------------------------------------------

    def _parse_company_from_serp(
        self, result: dict
    ) -> LinkedInCompany | None:
        """Parse a company from a Google SERP result."""
        url = result["url"]
        title = result["title"]
        snippet = result["snippet"]

        if "linkedin.com/company/" not in url:
            return None

        company = LinkedInCompany()
        company.company_url = url.split("?")[0]

        if title:
            title_clean = re.sub(
                r'\s*[\|–—]\s*LinkedIn.*$', '', title
            ).strip()
            company.company_name = title_clean

        if snippet:
            company.description = snippet[:200]

            ind_match = re.search(
                r'(?:industry|sector)[:\s]+([^.·\n]+)', snippet, re.I,
            )
            if ind_match:
                company.industry = ind_match.group(1).strip()[:80]

            size_match = re.search(
                r'(\d[\d,]*\+?\s*(?:employees|workers|people|staff))',
                snippet,
                re.I,
            )
            if size_match:
                company.company_size = size_match.group(1).strip()

            loc_match = re.search(
                r'(?:headquartered?\s+in|based\s+in|located?\s+in|,\s*)'
                r'([A-Z][A-Za-z\s,]+(?:Area)?)',
                snippet,
                re.I,
            )
            if loc_match:
                company.location = loc_match.group(1).strip()[:80]

        return company if company.company_name else None

    # ---- Query builders ------------------------------------------------

    def _build_executive_queries(
        self, niche: str, place: str
    ) -> list[str]:
        """
        Build multiple Google queries — one per executive-role group —
        plus generic industry queries to cast the widest possible net.
        """
        queries = []

        # 1. Role-based queries
        for role_group in self.ROLE_GROUPS:
            q = f'site:linkedin.com/in/ {role_group} "{place}"'
            if niche:
                q += f' "{niche}"'
            queries.append(q)

        # 2. Generic niche + location query (no role filter)
        if niche:
            queries.append(
                f'site:linkedin.com/in/ "{niche}" "{place}"'
            )
            # 3. "works at" / "at" patterns
            queries.append(
                f'site:linkedin.com/in/ "{place}" "at" "{niche}"'
            )
            # 4. Industry keyword variations
            queries.append(
                f'site:linkedin.com/in/ "{niche}" "{place}" "experience"'
            )

        # 5. Location-only broad sweep
        queries.append(
            f'site:linkedin.com/in/ "{place}" "CEO" OR "Manager" OR "Owner" OR "Founder"'
        )

        return queries

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

        For **profiles** mode the scraper automatically searches for
        multiple executive roles (CEO, Director, Manager, VP, …)
        combined with the given niche/industry and place.

        Args:
            niche: Industry or keyword (e.g., "technology", "marketing")
            place: City / location (e.g., "Lahore", "New York")
            search_type: "profiles" or "companies"
            max_pages: Google result pages per query (10 results each)

        Returns:
            List of dicts (profile or company data)
        """
        self._should_stop = False
        leads: list[dict] = []

        try:
            self._report_progress("Initializing browser...", 2)
            self._init_driver()

            # ---- Build queries ----
            if search_type == "profiles":
                queries = self._build_executive_queries(niche, place)
            else:
                queries = [
                    f'site:linkedin.com/company/ "{niche}" "{place}"',
                    f'site:linkedin.com/company/ "{niche}" "{place}" "employees"',
                    f'site:linkedin.com/company/ "{place}" "{niche}" "about"',
                ]
                if niche:
                    queries.append(
                        f'site:linkedin.com/company/ "{niche}" "{place}" "industry"'
                    )

            total_queries = len(queries)
            all_results: list[dict] = []

            # Phase 1: Google search
            for qi, query in enumerate(queries):
                if self._should_stop:
                    break

                pct = 5 + int((qi / total_queries) * 30)
                self._report_progress(
                    f"Google search (query {qi + 1}/{total_queries})…",
                    pct,
                )

                results = self._google_search(query, num_pages=max_pages)
                all_results.extend(results)

                # Pause between different queries to look human
                if qi < total_queries - 1 and not self._should_stop:
                    time.sleep(random.uniform(4, 8))

            # Phase 2: Bing search (secondary engine — less rate limiting)
            google_count = len(all_results)
            self._report_progress(
                f"Google found {google_count} results. Searching Bing…", 40,
            )

            for qi, query in enumerate(queries):
                if self._should_stop:
                    break

                pct = 40 + int((qi / total_queries) * 15)
                self._report_progress(
                    f"Bing search (query {qi + 1}/{total_queries})…",
                    pct,
                )

                bing_results = self._bing_search(query, num_pages=max_pages)
                all_results.extend(bing_results)

                if qi < total_queries - 1 and not self._should_stop:
                    time.sleep(random.uniform(2, 4))

            if not all_results:
                self._report_progress(
                    "No results found. Try different keywords.", 100,
                )
                return leads

            total = len(all_results)
            self._report_progress(
                f"Found {total} search results. Parsing…", 60,
            )

            seen_urls: set[str] = set()
            for idx, result in enumerate(all_results):
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
                        f"Parsed {idx + 1}/{total} results…",
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


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------

def clean_linkedin_leads(
    leads: list[dict], search_type: str = "profiles",
) -> list[dict]:
    """Clean and deduplicate LinkedIn leads."""
    cleaned: list[dict] = []
    seen: set[str] = set()

    for lead in leads:
        if search_type == "profiles":
            key = lead.get("profile_url", "")
            name = lead.get("name", "").strip()
            if not name or key in seen:
                continue
            seen.add(key)

            # Derive username if missing
            username = lead.get("linkedin_username", "")
            if not username and key:
                m = re.search(r'linkedin\.com/in/([\w\-]+)', key)
                if m:
                    username = m.group(1)

            cleaned.append({
                "name": name,
                "title": lead.get("title", "N/A") or "N/A",
                "company": lead.get("company", "N/A") or "N/A",
                "location": lead.get("location", "N/A") or "N/A",
                "profile_url": key or "N/A",
                "linkedin_username": username or "N/A",
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
