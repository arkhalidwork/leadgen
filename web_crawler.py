"""
Multi-Source Web Crawler — Lead Generation Engine
Crawls multiple search engines (Google, Bing) and business directory sites
to build a massive database of business leads with emails, phones,
websites, and social profiles.

Sources:
  1. Google Search (general web — not restricted to any site)
  2. Bing Search
  3. Yellow Pages / Business Directory patterns
  4. Website crawling for email & social extraction

This is the most productive scraper in the suite — it casts the widest net
by searching the open web instead of being limited to a single platform.
"""

import re
import time
import random
import logging
import warnings
from dataclasses import dataclass, asdict, field
from urllib.parse import quote_plus, urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
    TimeoutException,
)
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

warnings.filterwarnings("ignore", category=InsecureRequestWarning)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---- Regex patterns -------------------------------------------------------

EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.I
)

PHONE_RE = re.compile(
    r"(?:\+?\d{1,4}[\s\-.]?)?"          # country code
    r"(?:\(?\d{1,5}\)?[\s\-.]?)?"        # area code
    r"\d{2,4}[\s\-.]?\d{2,4}[\s\-.]?\d{0,4}",
)

EMAIL_BLACKLIST = {
    "example.com", "test.com", "email.com", "domain.com",
    "yoursite.com", "company.com", "website.com", "sentry.io",
    "wixpress.com", "w3.org", "schema.org", "googleapis.com",
    "googleusercontent.com", "gstatic.com", "facebook.com",
    "twitter.com", "instagram.com", "linkedin.com", "google.com",
    "bing.com", "microsoft.com", "apple.com", "amazon.com",
}

SOCIAL_PATTERNS = {
    "facebook": re.compile(
        r'https?://(?:www\.)?facebook\.com/[\w.\-]+', re.I
    ),
    "instagram": re.compile(
        r'https?://(?:www\.)?instagram\.com/[\w.\-]+', re.I
    ),
    "twitter": re.compile(
        r'https?://(?:www\.)?(?:twitter|x)\.com/[\w.\-]+', re.I
    ),
    "linkedin": re.compile(
        r'https?://(?:www\.)?linkedin\.com/(?:in|company)/[\w.\-]+', re.I
    ),
    "youtube": re.compile(
        r'https?://(?:www\.)?youtube\.com/(?:@|channel/|c/)[\w.\-]+', re.I
    ),
}

# Domains to skip when crawling (search engines, social, CDN, etc.)
SKIP_DOMAINS = {
    "google.com", "bing.com", "yahoo.com", "facebook.com",
    "instagram.com", "twitter.com", "x.com", "linkedin.com",
    "youtube.com", "tiktok.com", "pinterest.com", "reddit.com",
    "wikipedia.org", "amazon.com", "ebay.com", "apple.com",
    "microsoft.com", "github.com", "stackoverflow.com",
    "cloudflare.com", "gstatic.com", "googleapis.com",
    "googleusercontent.com", "yelp.com",
}


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------

@dataclass
class WebLead:
    """A business lead found via web crawling."""
    business_name: str = ""
    phone: str = ""
    email: str = ""
    website: str = ""
    address: str = ""
    description: str = ""
    source: str = ""  # which search/site found this
    facebook: str = ""
    instagram: str = ""
    twitter: str = ""
    linkedin: str = ""
    youtube: str = ""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class WebCrawlerScraper:
    """Multi-source web crawler for maximum lead generation."""

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
    ]

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self._progress_callback = None
        self._should_stop = False
        # Reusable HTTP session
        self._http_session = requests.Session()
        self._http_session.verify = False
        self._http_session.headers.update({
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "en-US,en;q=0.9",
        })
        adapter = HTTPAdapter(
            pool_connections=10, pool_maxsize=20,
            max_retries=Retry(total=2, backoff_factor=0.2),
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

    # ---- Browser -------------------------------------------------------

    def _init_driver(self):
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=en-US")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_argument(
            "--disable-blink-features=AutomationControlled"
        )
        chrome_options.add_experimental_option(
            "excludeSwitches", ["enable-automation", "enable-logging"]
        )
        chrome_options.add_experimental_option("useAutomationExtension", False)
        ua = random.choice(self.USER_AGENTS)
        chrome_options.add_argument(f"--user-agent={ua}")

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(2)

        try:
            self.driver.execute_cdp_cmd(
                "Page.addScriptToEvaluateOnNewDocument",
                {
                    "source": """
                        Object.defineProperty(navigator, 'webdriver', {
                            get: () => undefined
                        });
                    """
                },
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

    # ---- Consent / CAPTCHA ---------------------------------------------

    def _handle_consent(self):
        selectors = [
            "//button[contains(., 'Accept all')]",
            "//button[contains(., 'Accept')]",
            "//button[contains(., 'I agree')]",
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
        try:
            src = self.driver.page_source.lower()
            return any(
                kw in src
                for kw in ("captcha", "unusual traffic", "recaptcha")
            )
        except Exception:
            return False

    # ---- Build search queries ------------------------------------------

    def _build_queries(self, keyword: str, place: str) -> list[tuple[str, str]]:
        """
        Build a diverse set of (query, engine) tuples for maximum coverage.
        Returns list of (query_string, "google"|"bing").
        """
        queries = []

        # --- Google queries ---
        google_queries = [
            f'"{keyword}" "{place}" email phone',
            f'"{keyword}" "{place}" contact',
            f'"{keyword}" in {place} "phone" OR "email" OR "contact"',
            f'"{keyword}" "{place}" "@gmail.com" OR "@yahoo.com" OR "@hotmail.com"',
            f'"{keyword}" company "{place}" directory',
            f'"{keyword}" business "{place}" list',
            f'"{keyword}" "{place}" site:yellowpages.com OR site:yelp.com',
            f'"{keyword}" "{place}" "phone:" OR "tel:" OR "email:"',
            f'inurl:directory "{keyword}" "{place}"',
            f'"{keyword}" services "{place}" contact us',
        ]
        for q in google_queries:
            queries.append((q, "google"))

        # --- Bing queries (different phrasing for variety) ---
        bing_queries = [
            f'"{keyword}" "{place}" email phone contact',
            f'"{keyword}" business "{place}" directory listing',
            f'"{keyword}" "{place}" "@gmail.com" OR "@yahoo.com"',
            f'"{keyword}" companies "{place}" contact details',
            f'"{keyword}" "{place}" telephone address website',
            f'"{keyword}" professional "{place}" email',
            f'"{keyword}" shop store "{place}" phone',
            f'"{keyword}" agency firm "{place}" contact',
        ]
        for q in bing_queries:
            queries.append((q, "bing"))

        return queries

    # ---- Google Search -------------------------------------------------

    def _google_search(self, query: str, num_pages: int = 5) -> list[dict]:
        """Search Google for business websites (not limited to any site:)."""
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

                if page == 0:
                    self._handle_consent()
                    time.sleep(1)

                if self._check_captcha():
                    logger.warning("Google CAPTCHA — backing off…")
                    time.sleep(15 + random.uniform(5, 15))
                    self.driver.get(search_url)
                    time.sleep(5)
                    if self._check_captcha():
                        logger.error("Still blocked. Skipping query.")
                        break

                # Parse results
                page_results = self._parse_google_results()
                results.extend(page_results)

                try:
                    self.driver.find_element(By.ID, "pnnext")
                except NoSuchElementException:
                    break

            except WebDriverException as e:
                logger.error(f"Google error: {e}")
                continue

            if page < num_pages - 1:
                time.sleep(random.uniform(2.0, 5.0))

        return results

    def _parse_google_results(self) -> list[dict]:
        """Extract URLs + snippets from Google SERP."""
        results = []
        try:
            divs = self.driver.find_elements(By.CSS_SELECTOR, "div.g")
            for div in divs:
                try:
                    a_tag = div.find_element(By.CSS_SELECTOR, "a[href]")
                    href = a_tag.get_attribute("href") or ""
                    if not href.startswith("http"):
                        continue

                    # Skip search engine / social domains
                    domain = urlparse(href).netloc.lower()
                    root_domain = ".".join(domain.split(".")[-2:])
                    if root_domain in SKIP_DOMAINS:
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
                            "div.VwiC3b, div[data-sncf], "
                            "div[style*='-webkit-line-clamp']",
                        )
                        snippet = snippet_el.text.strip()
                    except NoSuchElementException:
                        try:
                            snippet = div.text.strip()[:300]
                        except Exception:
                            pass

                    results.append({
                        "url": href, "title": title, "snippet": snippet
                    })
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Google parse error: {e}")
        return results

    # ---- Bing Search ---------------------------------------------------

    def _bing_search(self, query: str, num_pages: int = 5) -> list[dict]:
        """Search Bing for business websites."""
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
                logger.error(f"Bing error: {e}")
                continue

            if page < num_pages - 1:
                time.sleep(random.uniform(1.5, 3.0))

        return results

    def _parse_bing_results(self) -> list[dict]:
        """Extract URLs + snippets from Bing SERP."""
        results = []
        try:
            items = self.driver.find_elements(By.CSS_SELECTOR, "li.b_algo")
            for item in items:
                try:
                    a_tag = item.find_element(By.CSS_SELECTOR, "h2 a")
                    href = a_tag.get_attribute("href") or ""
                    if not href.startswith("http"):
                        continue

                    domain = urlparse(href).netloc.lower()
                    root_domain = ".".join(domain.split(".")[-2:])
                    if root_domain in SKIP_DOMAINS:
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

                    results.append({
                        "url": href, "title": title, "snippet": snippet
                    })
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"Bing parse error: {e}")
        return results

    # ---- Website deep scraping -----------------------------------------

    def _scrape_website(self, url: str) -> WebLead | None:
        """
        Visit a business website and extract all contact info:
        emails, phones, social links, address, description.
        """
        lead = WebLead()
        lead.website = url
        lead.source = urlparse(url).netloc

        if not url.startswith("http"):
            url = "https://" + url

        pages_to_check = [url]
        for path in ["/contact", "/contact-us", "/about", "/about-us"]:
            pages_to_check.append(urljoin(url, path))

        all_emails: set[str] = set()
        all_phones: set[str] = set()
        found_socials: dict[str, str] = {k: "" for k in SOCIAL_PATTERNS}

        for page_url in pages_to_check:
            if self._should_stop:
                break
            try:
                resp = self._http_session.get(
                    page_url, timeout=8, allow_redirects=True,
                )
                if resp.status_code != 200:
                    continue

                text = resp.text
                soup = BeautifulSoup(text, "lxml")

                # Business name from <title> tag
                if not lead.business_name:
                    title_tag = soup.find("title")
                    if title_tag and title_tag.string:
                        name = title_tag.string.strip()
                        # Clean common suffixes
                        for sep in [" | ", " - ", " — ", " – "]:
                            if sep in name:
                                name = name.split(sep)[0].strip()
                        if name and len(name) < 100:
                            lead.business_name = name

                # Description from meta
                if not lead.description:
                    meta_desc = soup.find(
                        "meta", attrs={"name": "description"}
                    )
                    if meta_desc and meta_desc.get("content"):
                        lead.description = meta_desc["content"].strip()[:200]

                # Emails from mailto: links
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("mailto:"):
                        email = href.replace("mailto:", "").split("?")[0].strip()
                        if self._is_valid_email(email):
                            all_emails.add(email.lower())

                # Emails from page text
                for match in EMAIL_RE.findall(text):
                    if self._is_valid_email(match):
                        all_emails.add(match.lower())

                # Phones from tel: links
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("tel:"):
                        phone = href.replace("tel:", "").strip()
                        phone = re.sub(r"[^\d+\-() ]", "", phone)
                        if len(phone) >= 7:
                            all_phones.add(phone)

                # Phones from page text (only from likely sections)
                for el in soup.find_all(
                    ["p", "span", "div", "a", "li"],
                    string=re.compile(
                        r"(?:phone|tel|call|mobile|whatsapp|contact)",
                        re.I,
                    ),
                ):
                    parent_text = el.get_text()
                    for m in PHONE_RE.findall(parent_text):
                        cleaned = re.sub(r"[^\d+\-() ]", "", m).strip()
                        if len(cleaned) >= 7 and len(cleaned) <= 20:
                            all_phones.add(cleaned)

                # Social links
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    for platform, pattern in SOCIAL_PATTERNS.items():
                        if not found_socials[platform]:
                            mt = pattern.match(href)
                            if mt:
                                found_socials[platform] = mt.group(0)

                # Social links from raw source (JS-embedded)
                for platform, pattern in SOCIAL_PATTERNS.items():
                    if not found_socials[platform]:
                        mt = pattern.search(text)
                        if mt:
                            found_socials[platform] = mt.group(0)

                # Address patterns
                if not lead.address:
                    # Look for address in structured data
                    for script in soup.find_all("script", type="application/ld+json"):
                        try:
                            import json
                            data = json.loads(script.string or "")
                            if isinstance(data, dict):
                                addr = data.get("address", {})
                                if isinstance(addr, dict):
                                    parts = [
                                        addr.get("streetAddress", ""),
                                        addr.get("addressLocality", ""),
                                        addr.get("addressRegion", ""),
                                        addr.get("postalCode", ""),
                                        addr.get("addressCountry", ""),
                                    ]
                                    full = ", ".join(p for p in parts if p)
                                    if full:
                                        lead.address = full[:200]
                        except Exception:
                            pass

                # Early exit if we have everything
                if all_emails and all_phones and all(found_socials.values()):
                    break

            except requests.RequestException:
                continue
            except Exception as e:
                logger.debug(f"Error scraping {page_url}: {e}")
                continue

        # Assign to lead
        if all_emails:
            lead.email = "; ".join(sorted(all_emails))
        if all_phones:
            lead.phone = "; ".join(sorted(all_phones)[:3])  # max 3 phones
        for platform, url_val in found_socials.items():
            setattr(lead, platform, url_val)

        # Only return if we found something useful
        has_contact = lead.email or lead.phone
        has_name = lead.business_name and lead.business_name != "Unknown"
        if has_contact or has_name:
            return lead
        return None

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        if not email or "@" not in email:
            return False
        domain = email.split("@")[1].lower()
        if domain in EMAIL_BLACKLIST:
            return False
        if domain.endswith(
            (".png", ".jpg", ".gif", ".svg", ".webp", ".js", ".css")
        ):
            return False
        return True

    # ---- Quick snippet-based lead extraction ---------------------------

    def _extract_lead_from_snippet(
        self, result: dict, keyword: str, place: str,
    ) -> WebLead | None:
        """
        Try to extract basic lead info directly from search result
        snippets without visiting the website (fast path).
        """
        title = result.get("title", "")
        snippet = result.get("snippet", "")
        url = result.get("url", "")
        combined = f"{title} {snippet}"

        # Extract emails from snippet
        emails = set()
        for m in EMAIL_RE.findall(combined):
            if self._is_valid_email(m):
                emails.add(m.lower())

        # Extract phones from snippet
        phones = set()
        for m in PHONE_RE.findall(combined):
            cleaned = re.sub(r"[^\d+\-() ]", "", m).strip()
            if 7 <= len(cleaned) <= 20:
                phones.add(cleaned)

        if not emails and not phones:
            return None

        lead = WebLead()
        lead.website = url
        lead.source = urlparse(url).netloc if url else "search"
        lead.email = "; ".join(sorted(emails))
        lead.phone = "; ".join(sorted(phones)[:3])

        # Name from title
        if title:
            name = title
            for sep in [" | ", " - ", " — ", " – "]:
                if sep in name:
                    name = name.split(sep)[0].strip()
            lead.business_name = name[:100]

        if snippet:
            lead.description = snippet[:200]

        return lead

    # ---- Main scrape method -------------------------------------------

    def scrape(
        self,
        keyword: str,
        place: str,
        max_pages: int = 5,
    ) -> list[dict]:
        """
        Main scraping entry point.

        Searches Google + Bing with multiple query patterns, then deep-scrapes
        the found websites in parallel for emails, phones, and socials.

        Args:
            keyword: Business type (e.g., "real estate", "plumber")
            place: Location (e.g., "Dubai", "New York")
            max_pages: Result pages per query per engine

        Returns:
            List of WebLead dicts
        """
        self._should_stop = False
        leads: list[dict] = []

        try:
            self._report_progress("Initializing browser...", 2)
            self._init_driver()

            queries = self._build_queries(keyword, place)
            total_queries = len(queries)
            all_search_results: list[dict] = []
            snippet_leads: list[WebLead] = []

            # Phase 1: Search engines
            for qi, (query, engine) in enumerate(queries):
                if self._should_stop:
                    break

                pct = 3 + int((qi / total_queries) * 40)
                self._report_progress(
                    f"{engine.title()} search ({qi + 1}/{total_queries})…",
                    pct,
                )

                if engine == "google":
                    results = self._google_search(query, num_pages=max_pages)
                else:
                    results = self._bing_search(query, num_pages=max_pages)

                all_search_results.extend(results)

                # Quick snippet extraction
                for r in results:
                    snippet_lead = self._extract_lead_from_snippet(
                        r, keyword, place
                    )
                    if snippet_lead:
                        snippet_leads.append(snippet_lead)

                # Delay between queries
                if qi < total_queries - 1 and not self._should_stop:
                    delay = random.uniform(3, 6) if engine == "google" else random.uniform(1.5, 3)
                    time.sleep(delay)

            # Close browser — we use HTTP session for website scraping
            self._close_driver()

            # Deduplicate search results by domain
            seen_domains: set[str] = set()
            unique_urls: list[str] = []
            for r in all_search_results:
                url = r["url"]
                domain = urlparse(url).netloc.lower()
                if domain not in seen_domains:
                    seen_domains.add(domain)
                    unique_urls.append(url)

            total_urls = len(unique_urls)
            self._report_progress(
                f"Found {total_urls} unique websites + {len(snippet_leads)} "
                f"snippet leads. Deep-scraping websites…", 45,
            )

            # Phase 2: Deep-scrape websites in parallel
            deep_leads: list[WebLead] = []
            if unique_urls and not self._should_stop:
                with ThreadPoolExecutor(max_workers=10) as executor:
                    futures = {
                        executor.submit(self._scrape_website, url): url
                        for url in unique_urls
                    }
                    done_count = 0
                    for future in as_completed(futures):
                        done_count += 1
                        if self._should_stop:
                            executor.shutdown(wait=False, cancel_futures=True)
                            break
                        try:
                            lead = future.result()
                            if lead:
                                deep_leads.append(lead)
                        except Exception as e:
                            logger.debug(f"Website scrape error: {e}")

                        if done_count % 5 == 0 or done_count == total_urls:
                            pct = 45 + int((done_count / total_urls) * 50)
                            self._report_progress(
                                f"Scraped {done_count}/{total_urls} websites "
                                f"({len(deep_leads)} leads found)…",
                                min(pct, 95),
                            )

            # Phase 3: Merge snippet leads + deep leads, deduplicate
            all_leads: list[WebLead] = []
            seen_keys: set[str] = set()

            # Deep leads first (more complete)
            for lead in deep_leads:
                key = lead.website or lead.business_name
                if key and key not in seen_keys:
                    seen_keys.add(key)
                    all_leads.append(lead)

            # Then snippet leads
            for lead in snippet_leads:
                key = lead.website or lead.business_name
                if key and key not in seen_keys:
                    seen_keys.add(key)
                    all_leads.append(lead)

            leads = [asdict(l) for l in all_leads]

            self._report_progress(
                f"Done! Found {len(leads)} leads from {total_queries} queries "
                f"across Google & Bing.", 100,
            )

        except Exception as e:
            logger.error(f"Web crawler failed: {e}")
            self._report_progress(f"Error: {str(e)}", -1)
            raise
        finally:
            self._close_driver()

        return leads


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------

def clean_web_leads(leads: list[dict]) -> list[dict]:
    """Clean and deduplicate web crawler leads."""
    cleaned: list[dict] = []
    seen: set[str] = set()

    for lead in leads:
        # Deduplicate by website domain or business name
        website = lead.get("website", "").strip()
        name = lead.get("business_name", "").strip()
        domain = urlparse(website).netloc.lower() if website else ""

        key = domain or name.lower()
        if not key or key in seen:
            continue
        seen.add(key)

        # Clean phone
        phone = lead.get("phone", "")
        if phone:
            phone = re.sub(r"[^\d+\-();, ]", "", phone).strip()

        cleaned.append({
            "business_name": name or "N/A",
            "phone": phone or "N/A",
            "email": lead.get("email", "N/A") or "N/A",
            "website": website or "N/A",
            "address": lead.get("address", "N/A") or "N/A",
            "description": lead.get("description", "N/A") or "N/A",
            "source": lead.get("source", "N/A") or "N/A",
            "facebook": lead.get("facebook", "N/A") or "N/A",
            "instagram": lead.get("instagram", "N/A") or "N/A",
            "twitter": lead.get("twitter", "N/A") or "N/A",
            "linkedin": lead.get("linkedin", "N/A") or "N/A",
            "youtube": lead.get("youtube", "N/A") or "N/A",
        })

    return cleaned
