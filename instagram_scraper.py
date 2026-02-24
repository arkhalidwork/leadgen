"""
Instagram Lead Scraper Module — High-Power Multi-Engine Edition
===============================================================

Scrapes Instagram profile leads via **three search engines**
(DuckDuckGo, Google, Bing) and optional profile enrichment.
Designed for maximum yield and accuracy.

Two modes
---------
1. **Profile Search** (``search_type="profiles"``)
   Find Instagram users whose bio / profile contains a searched keyword.
   The keyword can be an industry, role, or niche.

2. **Business Search** (``search_type="businesses"``)
   Find Instagram business accounts related to the searched keyword in
   the target location.

Architecture
------------
Phase 1 — DuckDuckGo API search   (fast, no CAPTCHA, primary engine)
Phase 2 — Google SERP via Selenium (high-quality, CAPTCHA-managed)
Phase 3 — Bing SERP via Selenium   (secondary engine, less CAPTCHA)
Phase 4 — Profile enrichment       (visit profiles for bio/email/phone)
Phase 5 — Deduplication & output
"""

import os
import re
import time
import random
import logging
import warnings
from dataclasses import dataclass, asdict
from urllib.parse import quote_plus

import requests as _requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
)

# Suppress noisy SSL warnings
warnings.filterwarnings("ignore", category=_requests.packages.urllib3.exceptions.InsecureRequestWarning)

# DuckDuckGo search library (optional but highly recommended)
try:
    from ddgs import DDGS
    _HAS_DDGS = True
except ImportError:
    _HAS_DDGS = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Industry → synonym map (used to expand queries for broader coverage)
# ---------------------------------------------------------------------------

INDUSTRY_SYNONYMS: dict[str, list[str]] = {
    "real estate": [
        "property", "realtor", "broker", "homes", "realty",
        "properties", "housing", "villas", "apartments",
        "real estate agent", "property dealer", "property management",
    ],
    "marketing": [
        "digital marketing", "social media marketing", "advertising",
        "branding", "SEO", "content marketing", "PR", "media agency",
    ],
    "technology": [
        "tech", "IT", "software", "developer", "startup", "SaaS",
        "AI", "app development", "web development",
    ],
    "restaurant": [
        "food", "dining", "cafe", "cuisine", "eatery", "bistro",
        "catering", "kitchen", "chef",
    ],
    "fitness": [
        "gym", "workout", "training", "health", "wellness",
        "personal trainer", "yoga", "crossfit",
    ],
    "fashion": [
        "clothing", "apparel", "boutique", "designer", "style",
        "wardrobe", "wear", "fashion brand",
    ],
    "beauty": [
        "salon", "cosmetics", "skincare", "makeup", "spa",
        "beauty studio", "aesthetics", "nails",
    ],
    "photography": [
        "photographer", "photo studio", "videography",
        "wedding photographer", "cinematography", "visual artist",
    ],
    "construction": [
        "building", "contractor", "builder", "renovation",
        "architecture", "interior design", "engineering",
    ],
    "travel": [
        "tourism", "tour", "travel agency", "hotels", "vacation",
        "holiday", "adventure", "travel guide",
    ],
    "education": [
        "school", "academy", "training", "courses", "tutoring",
        "coaching", "learning", "institute",
    ],
    "consulting": [
        "consultant", "advisory", "strategy", "management consulting",
        "business consulting", "advisor",
    ],
    "legal": [
        "lawyer", "attorney", "law firm", "legal services",
        "advocate", "solicitor",
    ],
    "medical": [
        "doctor", "healthcare", "clinic", "hospital", "dental",
        "physician", "health center", "wellness clinic",
    ],
    "finance": [
        "financial", "accounting", "investment", "banking",
        "insurance", "wealth management", "fintech",
    ],
    "automotive": [
        "cars", "auto", "dealer", "vehicle", "motors",
        "car dealership", "automobile",
    ],
    "e-commerce": [
        "online store", "shop", "ecommerce", "retail", "marketplace",
        "online shopping", "store",
    ],
    "entertainment": [
        "events", "DJ", "music", "nightlife", "party",
        "entertainment company", "live events",
    ],
    "cleaning": [
        "cleaning services", "maid", "janitorial", "housekeeping",
        "laundry", "dry cleaning",
    ],
    "logistics": [
        "shipping", "freight", "cargo", "delivery", "courier",
        "transport", "supply chain",
    ],
}

BUSINESS_SUFFIXES = [
    "agency", "company", "services", "group", "solutions",
    "official", "team", "hub", "pro", "studio",
]

# Common free-mail domains for email-discovery queries
EMAIL_DOMAINS = [
    "@gmail.com", "@hotmail.com", "@yahoo.com", "@outlook.com",
    "@icloud.com", "@live.com", "@mail.com", "@aol.com",
    "@protonmail.com", "@zoho.com",
]

# Regex to extract emails from text
EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.I,
)

# False-positive email domains to ignore
EMAIL_BLACKLIST = {
    "example.com", "test.com", "email.com", "domain.com",
    "yoursite.com", "company.com", "website.com", "sentry.io",
    "wixpress.com", "w3.org", "schema.org", "googleapis.com",
    "googleusercontent.com", "gstatic.com", "apple.com",
    "facebook.com", "instagram.com", "twitter.com",
}

# Phone regex — international format, at least 8 digits
PHONE_RE = re.compile(
    r"(?:\+\d{1,3}[\s.-]?)?(?:\(?\d{2,4}\)?[\s.-]?)?\d{3,4}[\s.-]?\d{3,4}",
)


# ---------------------------------------------------------------------------
# Unified data class
# ---------------------------------------------------------------------------

@dataclass
class InstagramLead:
    """Represents a scraped Instagram lead (works for both modes)."""
    username: str = ""
    profile_url: str = ""
    display_name: str = ""
    bio: str = ""
    email: str = ""
    phone: str = ""
    website: str = ""
    category: str = ""
    followers: str = ""
    location: str = ""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class InstagramScraper:
    """High-power Instagram scraper using DuckDuckGo + Google + Bing."""

    USER_AGENTS = [
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) "
            "Version/18.3 Safari/605.1.15"
        ),
    ]

    # Max unique usernames before we stop harvesting more
    MAX_UNIQUE_USERNAMES = 500

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self._progress_callback = None
        self._should_stop = False
        self._partial_leads: list[dict] = []
        self._scrape_stats: dict = {
            "queries_completed": 0,
            "total_queries": 0,
            "leads_found": 0,
            "results_parsed": 0,
            "total_results": 0,
            "phase": "idle",
            "ddg_results": 0,
            "google_results": 0,
            "bing_results": 0,
            "enriched": 0,
        }

    # -- Progress / control ------------------------------------------------

    def set_progress_callback(self, callback):
        self._progress_callback = callback

    def stop(self):
        self._should_stop = True

    def get_partial_leads(self) -> list[dict]:
        return list(self._partial_leads)

    @property
    def scrape_stats(self) -> dict:
        return dict(self._scrape_stats)

    def _report(self, message: str, pct: int = -1):
        logger.info(message)
        if self._progress_callback:
            self._progress_callback(message, pct)

    # -- Browser helpers ---------------------------------------------------

    def _init_driver(self):
        opts = Options()
        if self.headless:
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_argument("--lang=en-US")
        opts.add_argument("--log-level=3")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_experimental_option(
            "excludeSwitches", ["enable-automation", "enable-logging"],
        )
        opts.add_experimental_option("useAutomationExtension", False)
        opts.add_argument(f"--user-agent={random.choice(self.USER_AGENTS)}")

        # Use system-installed Chromium if available (Docker / ARM64)
        chrome_bin = os.environ.get("CHROME_BIN")
        if chrome_bin:
            opts.binary_location = chrome_bin

        chromedriver_path = os.environ.get("CHROMEDRIVER_PATH")
        if chromedriver_path:
            from selenium.webdriver.chrome.service import Service
            service = Service(executable_path=chromedriver_path)
            self.driver = webdriver.Chrome(service=service, options=opts)
        else:
            self.driver = webdriver.Chrome(options=opts)
        self.driver.implicitly_wait(2)

        try:
            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": (
                    "Object.defineProperty(navigator,'webdriver',"
                    "{get:()=>undefined});"
                    "Object.defineProperty(navigator,'languages',"
                    "{get:()=>['en-US','en']});"
                )},
            )
        except Exception:
            pass

    def _close_driver(self):
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass
            self.driver = None

    # -- Consent / CAPTCHA -------------------------------------------------

    def _handle_consent(self):
        for sel in [
            "//button[contains(.,'Accept all')]",
            "//button[contains(.,'Accept')]",
            "//button[contains(.,'I agree')]",
            "//button[@id='L2AGLb']",
            "//button[@id='W0wltc']",
        ]:
            try:
                b = self.driver.find_element(By.XPATH, sel)
                if b.is_displayed():
                    b.click()
                    time.sleep(2)
                    return True
            except (NoSuchElementException, WebDriverException):
                continue
        return False

    def _check_captcha(self) -> bool:
        try:
            src = self.driver.page_source.lower()
            return any(k in src for k in (
                "captcha", "unusual traffic", "recaptcha", "are you a robot",
            ))
        except Exception:
            return False

    # =====================================================================
    # Search engines
    # =====================================================================

    # --- DuckDuckGo (primary — fast, no CAPTCHA) -------------------------

    def _duckduckgo_search(self, query: str, max_results: int = 40) -> list[dict]:
        """Search DuckDuckGo via the ddgs library (no Selenium needed)."""
        if not _HAS_DDGS:
            return []
        results: list[dict] = []
        try:
            ddgs = DDGS()
            raw = list(ddgs.text(query, max_results=max_results))
            for r in raw:
                href = r.get("href") or r.get("link") or r.get("url") or ""
                if "instagram.com" not in href:
                    continue
                results.append({
                    "url": href,
                    "title": r.get("title", ""),
                    "snippet": r.get("body") or r.get("snippet") or r.get("description") or "",
                })
        except Exception as e:
            logger.warning(f"DDG search error: {e}")
        return results

    # --- Google (secondary — best quality, CAPTCHA risk) ------------------

    def _google_search(self, query: str, num_pages: int = 4) -> list[dict]:
        results: list[dict] = []

        for page in range(num_pages):
            if self._should_stop:
                break

            start = page * 10
            url = (
                f"https://www.google.com/search"
                f"?q={quote_plus(query)}&start={start}&hl=en&num=10"
            )

            try:
                self.driver.get(url)
                time.sleep(3.0 + random.uniform(1.0, 3.0))

                if page == 0:
                    self._handle_consent()
                    time.sleep(1)

                if self._check_captcha():
                    logger.warning("Google CAPTCHA — backing off…")
                    for attempt in range(3):
                        wait = (15 + attempt * 15) + random.uniform(5, 15)
                        time.sleep(wait)
                        self.driver.get(url)
                        time.sleep(5)
                        if not self._check_captcha():
                            break
                    else:
                        logger.error("Google still blocked — skipping remaining pages.")
                        break

                page_results = (
                    self._parse_google_divg()
                    or self._parse_google_broad()
                    or self._parse_google_regex()
                )
                results.extend(page_results)

                # No next page?
                try:
                    self.driver.find_element(By.ID, "pnnext")
                except NoSuchElementException:
                    break

            except WebDriverException as e:
                logger.error(f"Google page {page} error: {e}")
                continue

            if page < num_pages - 1:
                time.sleep(random.uniform(2.5, 5.0))

        return results

    # --- Bing (tertiary) --------------------------------------------------

    def _bing_search(self, query: str, num_pages: int = 5) -> list[dict]:
        results: list[dict] = []

        for page in range(num_pages):
            if self._should_stop:
                break

            first = page * 10 + 1
            url = (
                f"https://www.bing.com/search"
                f"?q={quote_plus(query)}&first={first}&count=10"
            )

            try:
                self.driver.get(url)
                time.sleep(2.0 + random.uniform(1.0, 2.0))

                if page == 0:
                    try:
                        btn = self.driver.find_element(By.ID, "bnp_btn_accept")
                        if btn.is_displayed():
                            btn.click()
                            time.sleep(1)
                    except (NoSuchElementException, WebDriverException):
                        pass

                page_results = self._parse_bing_results()
                results.extend(page_results)

                try:
                    self.driver.find_element(By.CSS_SELECTOR, "a.sb_pagN")
                except NoSuchElementException:
                    break

            except WebDriverException as e:
                logger.error(f"Bing page {page} error: {e}")
                continue

            if page < num_pages - 1:
                time.sleep(random.uniform(1.5, 3.0))

        return results

    # =====================================================================
    # SERP parsing strategies
    # =====================================================================

    def _parse_google_divg(self) -> list[dict]:
        results = []
        try:
            for div in self.driver.find_elements(By.CSS_SELECTOR, "div.g"):
                try:
                    a = div.find_element(By.CSS_SELECTOR, "a[href]")
                    href = a.get_attribute("href") or ""
                    if "instagram.com" not in href:
                        continue

                    title = ""
                    snippet = ""
                    try:
                        title = div.find_element(By.CSS_SELECTOR, "h3").text.strip()
                    except NoSuchElementException:
                        pass
                    try:
                        el = div.find_element(
                            By.CSS_SELECTOR,
                            "div.VwiC3b, div[data-sncf], "
                            "div[style*='-webkit-line-clamp'], "
                            "span[class*='st'], div.IsZvec, "
                            "div[data-content-feature]",
                        )
                        snippet = el.text.strip()
                    except NoSuchElementException:
                        try:
                            full = div.text.strip()
                            if title and title in full:
                                snippet = full.replace(title, "", 1).strip()
                            elif len(full) > 30:
                                snippet = full[:300]
                        except Exception:
                            pass

                    if not snippet:
                        try:
                            for sp in div.find_elements(By.CSS_SELECTOR, "span"):
                                t = sp.text.strip()
                                if len(t) > 30:
                                    snippet = t
                                    break
                        except Exception:
                            pass

                    results.append({"url": href, "title": title, "snippet": snippet})
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Google divg parse error: {e}")
        return results

    def _parse_google_broad(self) -> list[dict]:
        results = []
        try:
            for a in self.driver.find_elements(
                By.CSS_SELECTOR, "a[href*='instagram.com']",
            ):
                href = a.get_attribute("href") or ""
                if "instagram.com/" not in href:
                    continue
                if "google.com" in href:
                    m = re.search(r'[?&]q=(https?[^&]+)', href)
                    if m:
                        href = m.group(1)
                    elif "instagram.com" not in href.split("?")[0]:
                        continue

                title = a.text.strip()[:150]
                snippet = ""
                try:
                    parent = a.find_element(
                        By.XPATH, "./ancestor::div[contains(@class,'g')]",
                    )
                    snippet = parent.text.strip()[:300]
                except Exception:
                    pass
                results.append({"url": href, "title": title, "snippet": snippet})
        except Exception as e:
            logger.debug(f"Google broad parse error: {e}")
        return results

    def _parse_google_regex(self) -> list[dict]:
        results = []
        try:
            source = self.driver.page_source
            urls = set(re.findall(
                r'https?://(?:www\.)?instagram\.com/[\w.\-]+', source,
            ))
            for url in urls:
                clean = url.split("?")[0].split("&amp;")[0]
                if clean.rstrip("/") in (
                    "https://www.instagram.com",
                    "https://instagram.com",
                ):
                    continue
                results.append({"url": clean, "title": "", "snippet": ""})
        except Exception as e:
            logger.debug(f"Google regex parse error: {e}")
        return results

    def _parse_bing_results(self) -> list[dict]:
        results = []
        try:
            for item in self.driver.find_elements(By.CSS_SELECTOR, "li.b_algo"):
                try:
                    a = item.find_element(By.CSS_SELECTOR, "h2 a")
                    href = a.get_attribute("href") or ""
                    if "instagram.com" not in href:
                        continue
                    title = a.text.strip()
                    snippet = ""
                    try:
                        p = item.find_element(By.CSS_SELECTOR, "div.b_caption p")
                        snippet = p.text.strip()[:300]
                    except NoSuchElementException:
                        pass
                    results.append({"url": href, "title": title, "snippet": snippet})
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Bing parse error: {e}")

        # Regex fallback
        if not results:
            try:
                source = self.driver.page_source
                urls = set(re.findall(
                    r'https?://(?:www\.)?instagram\.com/[\w.\-]+', source,
                ))
                for url in urls:
                    clean = url.split("?")[0].split("&amp;")[0]
                    if clean.rstrip("/") in (
                        "https://www.instagram.com",
                        "https://instagram.com",
                    ):
                        continue
                    results.append({"url": clean, "title": "", "snippet": ""})
            except Exception:
                pass
        return results

    # =====================================================================
    # Helpers
    # =====================================================================

    @staticmethod
    def _extract_username(url: str) -> str:
        m = re.search(r'instagram\.com/([\w][\w.\-]{0,29})', url)
        if m:
            username = m.group(1).rstrip(".")
            if username.lower() in (
                "p", "explore", "reel", "reels", "stories", "tv",
                "accounts", "about", "legal", "developer", "directory",
                "terms", "privacy", "s", "static", "nametag",
                "direct", "lite", "404", "challenge",
            ):
                return ""
            return username
        return ""

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        if not email or "@" not in email:
            return False
        domain = email.split("@")[1].lower()
        if domain in EMAIL_BLACKLIST:
            return False
        if domain.endswith(
            (".png", ".jpg", ".gif", ".svg", ".webp", ".js", ".css"),
        ):
            return False
        return True

    # =====================================================================
    # Lead parsing (from SERP data)
    # =====================================================================

    def _parse_lead(
        self,
        result: dict,
        place: str,
        keywords: str,
    ) -> InstagramLead | None:
        """Parse a SERP result into an InstagramLead."""
        url = result.get("url", "")
        title = result.get("title", "")
        snippet = result.get("snippet", "")

        if "instagram.com" not in url:
            return None

        username = self._extract_username(url)
        if not username:
            return None

        lead = InstagramLead()
        lead.username = username
        lead.profile_url = f"https://www.instagram.com/{username}/"
        lead.location = place

        # Display name from title: "Name (@user) • Instagram …"
        if title:
            name_match = re.match(r'^([^(@•·|]+)', title)
            if name_match:
                name = name_match.group(1).strip()
                for suffix in [
                    "Instagram photos and videos",
                    "Instagram photos",
                    "Instagram",
                    "on Instagram",
                    "is on Instagram",
                ]:
                    if name.lower().endswith(suffix.lower()):
                        name = name[: -len(suffix)].strip(" -·•|—")
                if name:
                    lead.display_name = name

        # Combined text for extraction
        combined = f"{title} {snippet}"

        # Emails
        emails: set[str] = set()
        for m in EMAIL_RE.findall(combined):
            if self._is_valid_email(m):
                emails.add(m.lower())
        if emails:
            lead.email = "; ".join(sorted(emails))

        # Bio snippet
        if snippet:
            lead.bio = snippet[:300]

        return lead

    # =====================================================================
    # Profile enrichment — lightweight requests-based
    # =====================================================================

    def _create_http_session(self) -> _requests.Session:
        s = _requests.Session()
        s.headers.update({
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
        })
        retry = Retry(total=1, backoff_factor=0.5)
        s.mount("https://", HTTPAdapter(max_retries=retry))
        return s

    def _enrich_single_profile(
        self, username: str, session: _requests.Session,
    ) -> dict:
        """Fetch an Instagram profile page and extract available data."""
        url = f"https://www.instagram.com/{username}/"
        data: dict = {}
        try:
            resp = session.get(url, timeout=10, allow_redirects=True)
            if resp.status_code != 200:
                return data

            html = resp.text

            # --- Meta tags (most reliable, served for SEO) ----------------
            soup = BeautifulSoup(html, "lxml")

            og_desc = soup.find("meta", property="og:description")
            if og_desc and og_desc.get("content"):
                content = og_desc["content"]
                # "X Followers, Y Following, Z Posts - See Instagram…"
                follower_m = re.search(
                    r'([\d,.KMkm]+)\s*Followers', content, re.I,
                )
                if follower_m:
                    data["followers"] = follower_m.group(1)
                # Sometimes the bio is appended after the dash
                dash_parts = content.split(" - ", 1)
                if len(dash_parts) > 1:
                    after = dash_parts[1].strip()
                    # Remove "See Instagram photos and videos from …"
                    if not after.lower().startswith("see instagram"):
                        data["bio"] = after[:300]

            og_title = soup.find("meta", property="og:title")
            if og_title and og_title.get("content"):
                nm = re.match(r'^([^(@•·]+)', og_title["content"])
                if nm:
                    data["display_name"] = nm.group(1).strip()

            # --- JSON embedded data (may or may not be present) -----------
            bio_m = re.search(r'"biography"\s*:\s*"((?:[^"\\]|\\.)*)"', html)
            if bio_m:
                try:
                    data["bio"] = bio_m.group(1).encode().decode(
                        "unicode_escape", errors="ignore",
                    )[:300]
                except Exception:
                    data["bio"] = bio_m.group(1)[:300]

            email_m = re.search(r'"business_email"\s*:\s*"((?:[^"\\]|\\.)+)"', html)
            if email_m and self._is_valid_email(email_m.group(1)):
                data["email"] = email_m.group(1)

            phone_m = re.search(r'"business_phone_number"\s*:\s*"((?:[^"\\]|\\.)+)"', html)
            if phone_m:
                data["phone"] = phone_m.group(1)

            cat_m = re.search(r'"category_name"\s*:\s*"((?:[^"\\]|\\.)+)"', html)
            if cat_m:
                data["category"] = cat_m.group(1)

            web_m = re.search(r'"external_url"\s*:\s*"((?:[^"\\]|\\.)+)"', html)
            if web_m:
                found_url = web_m.group(1)
                if "instagram.com" not in found_url:
                    data["website"] = found_url

            name_m = re.search(r'"full_name"\s*:\s*"((?:[^"\\]|\\.)+)"', html)
            if name_m:
                data["display_name"] = name_m.group(1)

            fc = re.search(r'"edge_followed_by"\s*:\s*\{\s*"count"\s*:\s*(\d+)\s*\}', html)
            if fc:
                count = int(fc.group(1))
                if count >= 1000000:
                    data["followers"] = f"{count / 1000000:.1f}M"
                elif count >= 1000:
                    data["followers"] = f"{count / 1000:.1f}K"
                else:
                    data["followers"] = str(count)

            # --- Extra emails from page source ----------------------------
            for em in EMAIL_RE.findall(html):
                if self._is_valid_email(em):
                    data.setdefault("email", em.lower())
                    break

        except Exception as e:
            logger.debug(f"Enrichment failed for @{username}: {e}")

        return data

    @staticmethod
    def _merge_enrichment(lead: dict, enrichment: dict):
        """Merge enrichment data into an existing lead dict."""
        if enrichment.get("bio"):
            lead["bio"] = enrichment["bio"]
        if enrichment.get("email") and (not lead.get("email") or lead["email"] == "N/A"):
            lead["email"] = enrichment["email"]
        if enrichment.get("phone"):
            lead["phone"] = enrichment["phone"]
        if enrichment.get("website"):
            lead["website"] = enrichment["website"]
        if enrichment.get("category"):
            lead["category"] = enrichment["category"]
        if enrichment.get("followers"):
            lead["followers"] = enrichment["followers"]
        if enrichment.get("display_name") and (
            not lead.get("display_name") or lead["display_name"] == "N/A"
        ):
            lead["display_name"] = enrichment["display_name"]

    def _enrich_profiles(
        self,
        leads: list[dict],
        max_profiles: int = 60,
    ) -> list[dict]:
        """Enrich a batch of leads by visiting their Instagram pages."""
        session = self._create_http_session()
        total = min(len(leads), max_profiles)
        enriched_count = 0

        for idx in range(total):
            if self._should_stop:
                break

            username = leads[idx].get("username", "")
            if not username:
                continue

            pct = 80 + int((idx / total) * 15)
            if idx % 8 == 0:
                self._report(
                    f"Enriching profiles ({idx + 1}/{total})… "
                    f"{enriched_count} enriched so far",
                    pct,
                )

            enrichment = self._enrich_single_profile(username, session)
            if enrichment:
                self._merge_enrichment(leads[idx], enrichment)
                enriched_count += 1

            time.sleep(random.uniform(0.8, 2.0))

        self._scrape_stats["enriched"] = enriched_count
        self._report(f"Enriched {enriched_count}/{total} profiles.", 95)
        return leads

    # =====================================================================
    # Keyword expansion
    # =====================================================================

    def _expand_keywords(self, keyword: str) -> list[str]:
        """Return the keyword + related synonyms for broader coverage."""
        kw_lower = keyword.lower().strip()
        expanded = [keyword]

        # Check synonym dictionary
        for key, synonyms in INDUSTRY_SYNONYMS.items():
            if key in kw_lower or kw_lower in key:
                expanded.extend(synonyms)
                break
            # Partial match (e.g. "real estate agent" matches "real estate")
            if any(s.lower() in kw_lower for s in [key]) or any(
                kw_lower in s.lower() for s in synonyms
            ):
                expanded.extend(synonyms)
                break

        # If no synonyms found, add basic variations
        if len(expanded) == 1:
            if kw_lower.endswith("s"):
                expanded.append(kw_lower[:-1])
            else:
                expanded.append(kw_lower + "s")
            for suffix in BUSINESS_SUFFIXES[:4]:
                expanded.append(f"{keyword} {suffix}")

        # Deduplicate, preserve order
        seen: set[str] = set()
        unique: list[str] = []
        for w in expanded:
            wl = w.lower()
            if wl not in seen:
                seen.add(wl)
                unique.append(w)
        return unique[:15]  # cap to prevent query explosion

    # =====================================================================
    # Query builders
    # =====================================================================

    def _build_profile_queries(self, keyword: str, place: str) -> list[str]:
        """
        Build queries for **Profile Search** mode.
        Strategy: find users whose bio / profile mentions the keyword.
        """
        queries: list[str] = []
        related = self._expand_keywords(keyword)

        # 1. Direct keyword + location with site: (core queries)
        for kw in related[:8]:
            queries.append(f'site:instagram.com "{kw}" "{place}"')

        # 2. Email domain queries — find profiles exposing emails
        email_chunks = [
            '"@gmail.com" OR "@yahoo.com" OR "@hotmail.com"',
            '"@outlook.com" OR "@icloud.com" OR "@mail.com"',
        ]
        for chunk in email_chunks:
            queries.append(
                f'site:instagram.com "{keyword}" "{place}" {chunk}'
            )

        # 3. Contact-oriented queries
        contact_terms = [
            '"email" OR "contact" OR "DM for"',
            '"book" OR "call" OR "whatsapp"',
            '"available" OR "hire" OR "inquir"',
        ]
        for ct in contact_terms:
            queries.append(
                f'site:instagram.com "{keyword}" "{place}" {ct}'
            )

        # 4. Without site: restriction (broader results on Google/Bing)
        for kw in related[:4]:
            queries.append(f'instagram.com "{kw}" "{place}"')

        # 5. Combined keyword+location as single phrase
        queries.append(f'site:instagram.com "{keyword} {place}"')
        queries.append(f'site:instagram.com "{place} {keyword}"')

        # 6. @ symbol queries (profiles with emails visible)
        for kw in related[:3]:
            queries.append(f'site:instagram.com "{kw}" "{place}" "@"')

        # 7. Additional keyword only (no location) for top keywords
        queries.append(f'site:instagram.com "{keyword}" "{place}" "follow"')

        # 8. Related keyword combos
        if len(related) > 3:
            queries.append(
                f'site:instagram.com "{related[1]}" OR "{related[2]}" "{place}"'
            )

        return queries

    def _build_business_queries(self, keyword: str, place: str) -> list[str]:
        """
        Build queries for **Business Search** mode.
        Strategy: find business accounts related to the keyword + location.
        """
        queries: list[str] = []
        related = self._expand_keywords(keyword)

        # 1. Business terms
        business_terms = [
            "company", "agency", "services", "official",
            "business", "group", "studio",
        ]
        for bt in business_terms:
            queries.append(
                f'site:instagram.com "{keyword}" "{bt}" "{place}"'
            )

        # 2. Related keywords (direct)
        for kw in related[:8]:
            queries.append(f'site:instagram.com "{kw}" "{place}"')

        # 3. Combined phrases
        for bt in ["company", "agency", "services", "group"]:
            queries.append(f'site:instagram.com "{keyword} {bt}" "{place}"')

        # 4. Email / contact queries
        queries.append(
            f'site:instagram.com "{keyword}" "{place}" '
            f'"@gmail.com" OR "@yahoo.com" OR "@hotmail.com"'
        )
        queries.append(
            f'site:instagram.com "{keyword}" "{place}" '
            f'"email" OR "contact" OR "call"'
        )
        queries.append(
            f'site:instagram.com "{keyword}" "{place}" '
            f'"DM" OR "book" OR "order"'
        )

        # 5. Without site: restriction
        for kw in related[:4]:
            queries.append(f'instagram.com "{kw}" business "{place}"')

        # 6. Corporate terms
        queries.append(
            f'site:instagram.com "{keyword}" "{place}" '
            f'"LLC" OR "Ltd" OR "Inc" OR "Est"'
        )
        queries.append(
            f'site:instagram.com "{keyword}" "{place}" '
            f'"since" OR "established" OR "founded"'
        )

        # 7. Location + keyword as phrase
        queries.append(f'site:instagram.com "{keyword} {place}"')
        queries.append(f'site:instagram.com "{place} {keyword}"')

        # 8. Related keywords + business terms
        for kw in related[1:4]:
            for bt in ["company", "agency", "business"]:
                queries.append(f'site:instagram.com "{kw}" "{bt}" "{place}"')

        # 9. Luxury / premium tier (common for many industries)
        queries.append(
            f'site:instagram.com "{keyword}" "{place}" '
            f'"luxury" OR "premium" OR "best"'
        )

        return queries

    # =====================================================================
    # Main scrape entry point
    # =====================================================================

    def scrape(
        self,
        keywords: str,
        place: str,
        search_type: str = "profiles",
        max_pages: int = 5,
    ) -> list[dict]:
        """
        Run the full multi-engine scraping pipeline.

        Args:
            keywords:    Industry / niche keyword (required).
            place:       Location / city to search in.
            search_type: ``"profiles"`` or ``"businesses"``.
            max_pages:   Max pages per Selenium query (Google/Bing).

        Returns:
            List of lead dicts.
        """
        self._should_stop = False
        self._partial_leads = []
        all_results: list[dict] = []
        seen_usernames: set[str] = set()  # running dedup

        # Build queries
        if search_type == "businesses":
            queries = self._build_business_queries(keywords, place)
        else:
            queries = self._build_profile_queries(keywords, place)

        total_q = len(queries)
        self._scrape_stats["total_queries"] = total_q * 3  # DDG + Google + Bing
        self._scrape_stats["phase"] = "searching"

        def _dedup_count():
            return len(seen_usernames)

        def _register_results(results: list[dict]):
            for r in results:
                u = self._extract_username(r.get("url", ""))
                if u:
                    seen_usernames.add(u)
            all_results.extend(results)

        # =================================================================
        # Phase 1: DuckDuckGo (fast, no CAPTCHA)
        # =================================================================
        self._report("Phase 1/4 — Searching DuckDuckGo…", 2)
        ddg_total = 0

        for qi, query in enumerate(queries):
            if self._should_stop or _dedup_count() >= self.MAX_UNIQUE_USERNAMES:
                break

            pct = 2 + int((qi / total_q) * 13)
            self._report(f"DuckDuckGo ({qi + 1}/{total_q})…", pct)

            results = self._duckduckgo_search(query, max_results=40)
            _register_results(results)
            ddg_total += len(results)
            self._scrape_stats["queries_completed"] = qi + 1
            self._scrape_stats["ddg_results"] = ddg_total

            # Small delay to be polite even to DDG API
            if qi < total_q - 1:
                time.sleep(random.uniform(0.5, 1.5))

        self._report(
            f"DuckDuckGo found {ddg_total} results "
            f"({_dedup_count()} unique profiles). Starting browser…",
            16,
        )

        # =================================================================
        # Phase 2: Google (best quality, CAPTCHA risk)
        # =================================================================
        try:
            self._init_driver()
        except Exception as e:
            logger.error(f"Browser init failed: {e}")
            # Continue with DDG results only
            self._report(f"Browser init failed — using {ddg_total} DDG results.", 60)
            self._scrape_stats["phase"] = "parsing"
            return self._finalize_leads(all_results, seen_usernames, place, keywords, search_type)

        # Limit Google queries to reduce CAPTCHA risk
        google_queries = queries[:min(15, total_q)]
        google_total = 0

        self._report("Phase 2/4 — Searching Google…", 18)

        for qi, query in enumerate(google_queries):
            if self._should_stop or _dedup_count() >= self.MAX_UNIQUE_USERNAMES:
                break

            pct = 18 + int((qi / len(google_queries)) * 22)
            self._report(f"Google ({qi + 1}/{len(google_queries)})…", pct)

            results = self._google_search(query, num_pages=min(max_pages, 4))
            _register_results(results)
            google_total += len(results)
            self._scrape_stats["queries_completed"] = total_q + qi + 1
            self._scrape_stats["google_results"] = google_total

            if qi < len(google_queries) - 1 and not self._should_stop:
                time.sleep(random.uniform(3, 6))

        self._report(
            f"Google found {google_total} results "
            f"({_dedup_count()} unique profiles). Searching Bing…",
            42,
        )

        # =================================================================
        # Phase 3: Bing
        # =================================================================
        bing_total = 0

        for qi, query in enumerate(queries):
            if self._should_stop or _dedup_count() >= self.MAX_UNIQUE_USERNAMES:
                break

            pct = 42 + int((qi / total_q) * 18)
            self._report(f"Bing ({qi + 1}/{total_q})…", pct)

            results = self._bing_search(query, num_pages=min(max_pages, 5))
            _register_results(results)
            bing_total += len(results)
            self._scrape_stats["queries_completed"] = total_q * 2 + qi + 1
            self._scrape_stats["bing_results"] = bing_total

            if qi < total_q - 1 and not self._should_stop:
                time.sleep(random.uniform(1.5, 3.0))

        self._close_driver()

        total_raw = len(all_results)
        unique_count = _dedup_count()
        self._report(
            f"Search complete: {total_raw} raw results, "
            f"{unique_count} unique profiles. Processing…",
            62,
        )

        return self._finalize_leads(
            all_results, seen_usernames, place, keywords, search_type,
        )

    # =====================================================================
    # Finalization pipeline
    # =====================================================================

    def _finalize_leads(
        self,
        all_results: list[dict],
        seen_usernames: set[str],
        place: str,
        keywords: str,
        search_type: str,
    ) -> list[dict]:
        """Parse, enrich, filter, and return the final leads list."""

        self._scrape_stats["total_results"] = len(all_results)
        self._scrape_stats["phase"] = "parsing"

        # --- Parse leads from SERP data -----------------------------------
        leads: list[dict] = []
        processed_usernames: set[str] = set()

        for idx, result in enumerate(all_results):
            if self._should_stop:
                break

            username = self._extract_username(result.get("url", ""))
            if not username or username in processed_usernames:
                continue
            processed_usernames.add(username)

            lead = self._parse_lead(result, place, keywords)
            if lead:
                lead_dict = asdict(lead)
                leads.append(lead_dict)
                self._partial_leads.append(lead_dict)
                self._scrape_stats["leads_found"] = len(leads)

            self._scrape_stats["results_parsed"] = idx + 1
            if idx % 20 == 0:
                pct = 62 + int((idx / max(len(all_results), 1)) * 16)
                self._report(
                    f"Parsed {idx + 1}/{len(all_results)} results… "
                    f"({len(leads)} leads)",
                    min(pct, 78),
                )

        self._report(f"Parsed {len(leads)} leads. Starting enrichment…", 80)

        # --- Profile enrichment -------------------------------------------
        if leads and not self._should_stop:
            try:
                leads = self._enrich_profiles(leads, max_profiles=60)
            except Exception as e:
                logger.error(f"Enrichment phase failed: {e}")
                self._report(f"Enrichment error (non-fatal): {e}", 95)

        self._scrape_stats["phase"] = "done"
        self._report(
            f"Done! Found {len(leads)} Instagram leads "
            f"(DDG: {self._scrape_stats.get('ddg_results', 0)}, "
            f"Google: {self._scrape_stats.get('google_results', 0)}, "
            f"Bing: {self._scrape_stats.get('bing_results', 0)}, "
            f"Enriched: {self._scrape_stats.get('enriched', 0)}).",
            100,
        )

        return leads


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------

def clean_instagram_leads(
    leads: list[dict],
    search_type: str = "profiles",
) -> list[dict]:
    """Clean and deduplicate Instagram leads."""
    cleaned: list[dict] = []
    seen: set[str] = set()

    for lead in leads:
        username = lead.get("username", "").strip()
        if not username or username in seen:
            continue
        seen.add(username)

        cleaned.append({
            "username": username,
            "profile_url": lead.get("profile_url", "N/A") or "N/A",
            "display_name": lead.get("display_name", "N/A") or "N/A",
            "bio": lead.get("bio", "N/A") or "N/A",
            "email": lead.get("email", "N/A") or "N/A",
            "phone": lead.get("phone", "N/A") or "N/A",
            "website": lead.get("website", "N/A") or "N/A",
            "category": lead.get("category", "N/A") or "N/A",
            "followers": lead.get("followers", "N/A") or "N/A",
            "location": lead.get("location", "N/A") or "N/A",
        })

    return cleaned
