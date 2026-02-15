"""
Instagram Lead Scraper Module
Scrapes Instagram profiles & emails via Google SERP
(site:instagram.com) using Selenium.

Two modes:
  1. "emails"   – Find Instagram profiles that expose email addresses
                   in a given location.
                   Query pattern:
                     site:instagram.com "<place>" "@gmail.com" "@hotmail.com"
  2. "profiles" – Find executive / role-based profiles (CEO, Director,
                   Manager…) and extract company & bio info from SERP
                   snippets.
                   Query pattern:
                     site:instagram.com "CEO" "Chief Executive Officer" <place>
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
from selenium.common.exceptions import (
    NoSuchElementException,
    WebDriverException,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Common free-mail domains to search for exposed emails
EMAIL_DOMAINS = [
    "@gmail.com",
    "@hotmail.com",
    "@yahoo.com",
    "@outlook.com",
    "@icloud.com",
    "@live.com",
    "@mail.com",
]

# Email regex
EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.I,
)

# Blacklisted email domains (false positives)
EMAIL_BLACKLIST = {
    "example.com", "test.com", "email.com", "domain.com",
    "yoursite.com", "company.com", "website.com", "sentry.io",
    "wixpress.com", "w3.org", "schema.org", "googleapis.com",
    "googleusercontent.com", "gstatic.com",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class InstagramEmailLead:
    """A lead found via email-mode scraping."""
    username: str = ""
    profile_url: str = ""
    display_name: str = ""
    email: str = ""
    bio_snippet: str = ""
    location: str = ""


@dataclass
class InstagramProfileLead:
    """A lead found via profile / executive-mode scraping."""
    username: str = ""
    profile_url: str = ""
    display_name: str = ""
    title: str = ""
    company: str = ""
    company_url: str = ""
    bio_snippet: str = ""
    location: str = ""


# ---------------------------------------------------------------------------
# Scraper
# ---------------------------------------------------------------------------

class InstagramScraper:
    """Scrapes Instagram profiles & emails via Google search."""

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

    # Role groups for executive / profile search
    ROLE_GROUPS = [
        '"CEO" OR "Chief Executive Officer" OR "Founder" OR "Co-Founder"',
        '"Director" OR "Managing Director" OR "President"',
        '"Manager" OR "General Manager" OR "Senior Manager"',
        '"VP" OR "Vice President" OR "COO" OR "CFO" OR "CTO"',
        '"Head of" OR "Partner" OR "Owner"',
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
        """Initialize Chrome with anti-detection."""
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
                        Object.defineProperty(navigator, 'languages', {
                            get: () => ['en-US', 'en']
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
        Run a Google search and return results: [{url, title, snippet}].
        Uses 3 fallback strategies (same as LinkedIn scraper).
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

                if page == 0:
                    self._handle_consent()
                    time.sleep(1)

                if self._check_captcha():
                    logger.warning("CAPTCHA detected — backing off…")
                    for attempt in range(3):
                        wait = (15 + attempt * 15) + random.uniform(5, 15)
                        logger.info(f"CAPTCHA retry {attempt + 1}/3, waiting {wait:.0f}s")
                        time.sleep(wait)
                        self.driver.get(search_url)
                        time.sleep(5)
                        if not self._check_captcha():
                            break
                    else:
                        logger.error("Still blocked by CAPTCHA after retries.")
                        break

                # Strategy 1: div.g containers
                page_results = self._parse_serp_divg()

                # Strategy 2: broad <a> selector
                if not page_results:
                    page_results = self._parse_serp_broad()

                # Strategy 3: regex fallback
                if not page_results:
                    page_results = self._parse_serp_regex()

                results.extend(page_results)

                try:
                    self.driver.find_element(By.ID, "pnnext")
                except NoSuchElementException:
                    break

            except WebDriverException as e:
                logger.error(f"Error on SERP page {page}: {e}")
                continue

            if page < num_pages - 1:
                time.sleep(random.uniform(2.0, 5.0))

        return results

    # ---- SERP Parsing Strategies ----------------------------------------

    def _parse_serp_divg(self) -> list[dict]:
        results = []
        try:
            divs = self.driver.find_elements(By.CSS_SELECTOR, "div.g")
            for div in divs:
                try:
                    a_tag = div.find_element(By.CSS_SELECTOR, "a[href]")
                    href = a_tag.get_attribute("href") or ""
                    if "instagram.com" not in href:
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
                            "div[style*='-webkit-line-clamp'], "
                            "span[class*='st'], div.IsZvec, "
                            "div[data-content-feature]",
                        )
                        snippet = snippet_el.text.strip()
                    except NoSuchElementException:
                        # Fallback: grab entire div text minus title
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
                            for span in div.find_elements(
                                By.CSS_SELECTOR, "span"
                            ):
                                t = span.text.strip()
                                if len(t) > 30:
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
        results = []
        try:
            links = self.driver.find_elements(
                By.CSS_SELECTOR, "a[href*='instagram.com']"
            )
            for a_tag in links:
                href = a_tag.get_attribute("href") or ""
                if "instagram.com/" not in href:
                    continue
                # Skip internal Google links (but keep Google redirect URLs)
                if "google.com" in href:
                    if "instagram.com" not in href:
                        continue
                    # Extract actual destination from Google redirect
                    redir_m = re.search(r'[?&]q=(https?[^&]+)', href)
                    if redir_m:
                        href = redir_m.group(1)
                    elif "instagram.com" not in href.split("?")[0]:
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
        results = []
        try:
            source = self.driver.page_source
            urls = set(
                re.findall(
                    r'https?://(?:www\.)?instagram\.com/[\w.\-]+',
                    source,
                )
            )
            for url in urls:
                clean = url.split("?")[0].split("&amp;")[0]
                # Skip generic IG pages
                if clean.rstrip("/") in (
                    "https://www.instagram.com",
                    "https://instagram.com",
                ):
                    continue
                results.append({"url": clean, "title": "", "snippet": ""})
        except Exception as e:
            logger.debug(f"Strategy-3 error: {e}")
        return results

    # ---- Helpers: extract username from IG URL -------------------------

    @staticmethod
    def _extract_username(url: str) -> str:
        """Return the IG username from a URL, or empty string."""
        m = re.search(
            r'instagram\.com/([\w][\w.\-]{0,29})',
            url,
        )
        if m:
            username = m.group(1).rstrip(".")
            # Skip non-profile pages
            if username.lower() in (
                "p", "explore", "reel", "reels", "stories",
                "tv", "accounts", "about", "legal", "developer",
                "directory", "terms", "privacy", "s", "static",
                "accounts", "nametag", "direct", "lite",
            ):
                return ""
            return username
        return ""

    # ---- Helpers: validate email ----------------------------------------

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

    # ---- Email-mode parsing --------------------------------------------

    def _parse_email_lead(self, result: dict, place: str) -> InstagramEmailLead | None:
        """Parse an email-bearing lead from a SERP result."""
        url = result["url"]
        title = result["title"]
        snippet = result["snippet"]

        if "instagram.com" not in url:
            return None

        username = self._extract_username(url)
        if not username:
            return None

        lead = InstagramEmailLead()
        lead.username = username
        lead.profile_url = f"https://www.instagram.com/{username}/"
        lead.location = place

        # Display name from title: "Name (@user) • Instagram ..."
        if title:
            name_match = re.match(r'^([^(@]+)', title)
            if name_match:
                lead.display_name = name_match.group(1).strip()

        # Combine title + snippet for email extraction
        combined_text = f"{title} {snippet}"

        # Extract emails
        emails = set()
        for match in EMAIL_RE.findall(combined_text):
            if self._is_valid_email(match):
                emails.add(match.lower())

        if emails:
            lead.email = "; ".join(sorted(emails))
        else:
            lead.email = ""  # No email in snippet — keep lead anyway

        # Bio snippet
        if snippet:
            lead.bio_snippet = snippet[:200]

        return lead

    # ---- Profile-mode parsing -------------------------------------------

    def _parse_profile_lead(self, result: dict, place: str) -> InstagramProfileLead | None:
        """Parse a role/executive profile from a SERP result."""
        url = result["url"]
        title = result["title"]
        snippet = result["snippet"]

        if "instagram.com" not in url:
            return None

        username = self._extract_username(url)
        if not username:
            return None

        lead = InstagramProfileLead()
        lead.username = username
        lead.profile_url = f"https://www.instagram.com/{username}/"
        lead.location = place

        # Display name from title
        if title:
            name_match = re.match(r'^([^(@]+)', title)
            if name_match:
                lead.display_name = name_match.group(1).strip()

        combined = f"{title} {snippet}"

        # Try to extract title/role
        role_patterns = [
            r'(CEO|Chief Executive Officer|Founder|Co-Founder)',
            r'(Director|Managing Director|President)',
            r'(Manager|General Manager|Senior Manager)',
            r'(VP|Vice President|COO|CFO|CTO)',
            r'(Head of\s+\w+)',
            r'(Partner|Owner)',
        ]
        for pat in role_patterns:
            m = re.search(pat, combined, re.I)
            if m:
                lead.title = m.group(1).strip()
                break

        # Company name: look for patterns like "at <Company>", "@ <Company>",
        # or lines starting with uppercase after role
        comp_patterns = [
            r'(?:\bat\b|@)\s+([A-Z][^.·\-\n,@]{2,60})',
            r'(?:company|org|organisation|organization)[:\s]+([^.·\n,]{2,60})',
        ]
        for pat in comp_patterns:
            m = re.search(pat, combined, re.I)
            if m:
                lead.company = m.group(1).strip()
                break

        # Try to find a URL in snippet (website / company URL in bio)
        url_match = re.search(
            r'(https?://[^\s"\'<>,]+)',
            snippet or "",
        )
        if url_match:
            found_url = url_match.group(1).rstrip(".")
            # Only keep if not an instagram URL
            if "instagram.com" not in found_url:
                lead.company_url = found_url

        # Bio snippet
        if snippet:
            lead.bio_snippet = snippet[:200]

        return lead

    # ---- Query builders -------------------------------------------------

    def _build_email_queries(self, place: str, keywords: str) -> list[str]:
        """
        Build email-mode queries:
          site:instagram.com "<place>" ("@gmail.com" OR "@hotmail.com" OR ...)
        We split email domains into groups of 3 and join with OR.
        """
        queries = []
        # Chunk email domains into groups of 3 joined with OR
        for i in range(0, len(EMAIL_DOMAINS), 3):
            chunk = EMAIL_DOMAINS[i:i + 3]
            domain_part = " OR ".join(f'"{d}"' for d in chunk)
            q = f'site:instagram.com "{place}" ({domain_part})'
            if keywords:
                q += f' "{keywords}"'
            queries.append(q)
        return queries

    def _build_profile_queries(self, keywords: str, place: str) -> list[str]:
        """
        Build profile-mode queries using role groups:
          site:instagram.com ("CEO" OR "Founder") "<place>"
        Parentheses ensure the site: restriction applies to all OR branches.
        """
        queries = []
        for role_group in self.ROLE_GROUPS:
            q = f'site:instagram.com ({role_group}) "{place}"'
            if keywords:
                q += f' "{keywords}"'
            queries.append(q)
        return queries

    # ---- Main public API -----------------------------------------------

    def scrape(
        self,
        keywords: str,
        place: str,
        search_type: str = "emails",
        max_pages: int = 3,
    ) -> list[dict]:
        """
        Main scraping entry point.

        Args:
            keywords: Optional niche / industry keyword
            place: Location / city to search in
            search_type: "emails" or "profiles"
            max_pages: Google result pages per query

        Returns:
            List of dicts (email leads or profile leads)
        """
        self._should_stop = False
        leads: list[dict] = []

        try:
            self._report_progress("Initializing browser...", 2)
            self._init_driver()

            # Build query set
            if search_type == "emails":
                queries = self._build_email_queries(place, keywords)
            else:
                queries = self._build_profile_queries(keywords, place)

            total_queries = len(queries)
            all_results: list[dict] = []

            for qi, query in enumerate(queries):
                if self._should_stop:
                    break

                pct = 5 + int((qi / total_queries) * 45)
                self._report_progress(
                    f"Searching Google (query {qi + 1}/{total_queries})…",
                    pct,
                )

                results = self._google_search(query, num_pages=max_pages)
                all_results.extend(results)

                if qi < total_queries - 1 and not self._should_stop:
                    time.sleep(random.uniform(4, 8))

            if not all_results:
                self._report_progress(
                    "No results found. Try different keywords.", 100,
                )
                return leads

            total = len(all_results)
            self._report_progress(
                f"Found {total} search results. Parsing…", 60,
            )

            seen_usernames: set[str] = set()

            for idx, result in enumerate(all_results):
                if self._should_stop:
                    break

                # Deduplicate by username
                username = self._extract_username(result["url"])
                if not username or username in seen_usernames:
                    continue
                seen_usernames.add(username)

                if search_type == "emails":
                    parsed = self._parse_email_lead(result, place)
                else:
                    parsed = self._parse_profile_lead(result, place)

                if parsed:
                    leads.append(asdict(parsed))

                progress = 60 + int((idx / total) * 35)
                if idx % 5 == 0:
                    self._report_progress(
                        f"Parsed {idx + 1}/{total} results…",
                        min(progress, 95),
                    )

            self._report_progress(
                f"Done! Found {len(leads)} Instagram {search_type}.", 100,
            )

        except Exception as e:
            logger.error(f"Instagram scraping failed: {e}")
            self._report_progress(f"Error: {str(e)}", -1)
            raise
        finally:
            self._close_driver()

        return leads


# ---------------------------------------------------------------------------
# Post-processing
# ---------------------------------------------------------------------------

def clean_instagram_leads(
    leads: list[dict], search_type: str = "emails",
) -> list[dict]:
    """Clean and deduplicate Instagram leads."""
    cleaned: list[dict] = []
    seen: set[str] = set()

    for lead in leads:
        username = lead.get("username", "").strip()
        if not username or username in seen:
            continue
        seen.add(username)

        if search_type == "emails":
            cleaned.append({
                "username": username,
                "profile_url": lead.get("profile_url", "N/A") or "N/A",
                "display_name": lead.get("display_name", "N/A") or "N/A",
                "email": lead.get("email", "N/A") or "N/A",
                "bio_snippet": lead.get("bio_snippet", "N/A") or "N/A",
                "location": lead.get("location", "N/A") or "N/A",
            })
        else:
            cleaned.append({
                "username": username,
                "profile_url": lead.get("profile_url", "N/A") or "N/A",
                "display_name": lead.get("display_name", "N/A") or "N/A",
                "title": lead.get("title", "N/A") or "N/A",
                "company": lead.get("company", "N/A") or "N/A",
                "company_url": lead.get("company_url", "N/A") or "N/A",
                "bio_snippet": lead.get("bio_snippet", "N/A") or "N/A",
                "location": lead.get("location", "N/A") or "N/A",
            })

    return cleaned
