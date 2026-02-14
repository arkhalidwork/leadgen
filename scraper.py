"""
Google Maps Lead Scraper Module
Scrapes business listings from Google Maps for lead generation.
"""

import time
import re
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
from dataclasses import dataclass, field, asdict
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from bs4 import BeautifulSoup
from selenium import webdriver

# Suppress SSL warnings for verify=False requests
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

# Emails to exclude (common false positives)
EMAIL_BLACKLIST = {
    'example.com', 'test.com', 'email.com', 'domain.com',
    'yoursite.com', 'company.com', 'website.com', 'sentry.io',
    'wixpress.com', 'w3.org', 'schema.org', 'googleapis.com',
    'googleusercontent.com', 'gstatic.com',
}


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
    facebook: str = ""
    instagram: str = ""
    twitter: str = ""
    linkedin: str = ""
    youtube: str = ""
    tiktok: str = ""
    pinterest: str = ""


class GoogleMapsScraper:
    """Scrapes Google Maps search results for business leads."""

    GOOGLE_MAPS_URL = "https://www.google.com/maps"

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.driver = None
        self._progress_callback = None
        self._should_stop = False
        # Reusable HTTP session with connection pooling
        self._http_session = requests.Session()
        self._http_session.verify = False
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
            pool_connections=10, pool_maxsize=20,
            max_retries=Retry(total=1, backoff_factor=0.1),
        )
        self._http_session.mount("https://", adapter)
        self._http_session.mount("http://", adapter)

    def set_progress_callback(self, callback):
        """Set a callback function for progress updates."""
        self._progress_callback = callback

    def stop(self):
        """Signal the scraper to stop."""
        self._should_stop = True

    def _report_progress(self, message: str, percentage: int = -1):
        """Report progress via callback."""
        logger.info(message)
        if self._progress_callback:
            self._progress_callback(message, percentage)

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
        # Suppress logging
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])

        # Use Selenium's built-in driver manager (auto-downloads matching ChromeDriver)
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.implicitly_wait(2)

    def _search_maps(self, query: str):
        """Navigate to Google Maps and perform a search."""
        self._report_progress(f"Searching Google Maps for: {query}", 5)

        # Use the direct search URL — much more reliable than navigating and typing
        search_url = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        self.driver.get(search_url)
        time.sleep(3)

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

        # Wait for results to appear (feed panel or individual place page)
        try:
            WebDriverWait(self.driver, 15).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, 'div[role="feed"]')
                or d.find_elements(By.CSS_SELECTOR, 'h1')
            )
        except TimeoutException:
            logger.warning("Timed out waiting for search results to load.")

    def _scroll_results(self):
        """Scroll the results panel to load all listings."""
        self._report_progress("Scrolling to load all results...", 15)

        # Find the scrollable results panel
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
        scroll_attempts = 0
        max_scroll_attempts = 50  # Safety limit

        while scroll_attempts < max_scroll_attempts:
            if self._should_stop:
                self._report_progress("Scraping stopped by user.")
                return

            # Scroll down
            self.driver.execute_script(
                "arguments[0].scrollTop = arguments[0].scrollHeight", results_panel
            )
            time.sleep(1.2)

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

            # Also check for the text-based end indicator
            try:
                page_source_snippet = results_panel.get_attribute("innerHTML")
                if "You've reached the end of the list" in page_source_snippet:
                    self._report_progress("Reached end of results list.", 30)
                    break
            except Exception:
                pass

            # Check if scrolling produced new content
            new_height = self.driver.execute_script(
                "return arguments[0].scrollHeight", results_panel
            )
            if new_height == last_height:
                scroll_attempts += 1
                if scroll_attempts >= 3:
                    self._report_progress("No more results to load.", 30)
                    break
            else:
                scroll_attempts = 0
                last_height = new_height

            progress = min(30, 15 + scroll_attempts)
            self._report_progress(
                f"Loading more results... (scroll cycle {scroll_attempts})",
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

    def _extract_business_detail(self, url: str) -> BusinessLead:
        """Navigate to a business page and extract details."""
        lead = BusinessLead()

        try:
            self.driver.get(url)
            time.sleep(1.5)

            # Business Name
            try:
                name_el = WebDriverWait(self.driver, 8).until(
                    EC.presence_of_element_located(
                        (By.CSS_SELECTOR, "h1.DUwDvf")
                    )
                )
                lead.business_name = name_el.text.strip()
            except TimeoutException:
                # Fallback: try to get name from the URL
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

            # Owner name - usually not directly shown on Google Maps,
            # but sometimes appears in specific business types
            try:
                # Some businesses list the owner/proprietor
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

    def scrape(self, keyword: str, place: str) -> list[dict]:
        """
        Main scraping method.

        Args:
            keyword: Business type to search (e.g., "restaurants")
            place: Location to search in (e.g., "New York")

        Returns:
            List of BusinessLead dicts
        """
        query = f"{keyword} in {place}"
        leads = []
        self._should_stop = False

        try:
            self._report_progress("Initializing browser...", 2)
            self._init_driver()

            # Search on Google Maps
            self._search_maps(query)

            # Scroll to load all results
            self._scroll_results()

            if self._should_stop:
                return leads

            # Collect all listing URLs
            listing_urls = self._get_listing_links()

            if not listing_urls:
                self._report_progress("No listings found for this search.", 100)
                return leads

            total = len(listing_urls)
            self._report_progress(f"Scraping details for {total} businesses...", 45)

            # Phase 1: Extract business details from Google Maps
            for idx, url in enumerate(listing_urls):
                if self._should_stop:
                    self._report_progress("Scraping stopped by user.")
                    break

                progress = 45 + int((idx / total) * 40)
                self._report_progress(
                    f"Scraping business {idx + 1}/{total}...", progress
                )

                lead = self._extract_business_detail(url)
                if lead.business_name:
                    leads.append(lead)

                time.sleep(0.5)

            # Phase 2: Scrape websites for emails & socials in parallel
            leads_with_websites = [l for l in leads if l.website]
            if leads_with_websites and not self._should_stop:
                ws_total = len(leads_with_websites)
                self._report_progress(
                    f"Scanning {ws_total} websites for emails & socials...", 88
                )

                with ThreadPoolExecutor(max_workers=8) as executor:
                    futures = {
                        executor.submit(self._scrape_website, lead): lead
                        for lead in leads_with_websites
                    }
                    done_count = 0
                    for future in as_completed(futures):
                        done_count += 1
                        if self._should_stop:
                            executor.shutdown(wait=False, cancel_futures=True)
                            break
                        try:
                            future.result()
                        except Exception as e:
                            logger.debug(f"Website scrape error: {e}")
                        if done_count % 3 == 0 or done_count == ws_total:
                            pct = 88 + int((done_count / ws_total) * 10)
                            self._report_progress(
                                f"Scanned {done_count}/{ws_total} websites...",
                                min(pct, 98),
                            )

            # Convert dataclass leads to dicts
            leads = [asdict(l) for l in leads]

            self._report_progress(
                f"Scraping complete! Found {len(leads)} leads.", 100
            )

        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            self._report_progress(f"Error: {str(e)}", -1)
            raise

        finally:
            self._close_driver()

        return leads

    def _scrape_website(self, lead: BusinessLead):
        """Visit a business website to extract emails and social profiles."""
        url = lead.website
        if not url or url == "N/A":
            return

        if not url.startswith("http"):
            url = "https://" + url

        # Pages to check: homepage first, then contact/about only if needed
        pages_to_check = [url]
        for path in ["/contact", "/contact-us", "/about", "/about-us"]:
            pages_to_check.append(urljoin(url, path))

        all_emails = set()
        found_socials = {k: "" for k in SOCIAL_PATTERNS}

        for page_url in pages_to_check:
            try:
                resp = self._http_session.get(
                    page_url, timeout=5, allow_redirects=True,
                )
                if resp.status_code != 200:
                    continue

                text = resp.text
                soup = BeautifulSoup(text, "lxml")

                # ---- Extract Emails ----
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    if href.startswith("mailto:"):
                        email = href.replace("mailto:", "").split("?")[0].strip()
                        if self._is_valid_email(email):
                            all_emails.add(email.lower())

                # From page text via regex
                for match in EMAIL_RE.findall(text):
                    if self._is_valid_email(match):
                        all_emails.add(match.lower())

                # ---- Extract Social Links ----
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    for platform, pattern in SOCIAL_PATTERNS.items():
                        if not found_socials[platform]:
                            m = pattern.match(href)
                            if m:
                                found_socials[platform] = m.group(0)

                # Also check raw page source if links are embedded in JS
                for platform, pattern in SOCIAL_PATTERNS.items():
                    if not found_socials[platform]:
                        m = pattern.search(text)
                        if m:
                            found_socials[platform] = m.group(0)

                # Early exit: stop checking more pages if we have email + all socials found
                if all_emails and all(found_socials.values()):
                    break

            except requests.RequestException:
                continue
            except Exception as e:
                logger.debug(f"Error scraping {page_url}: {e}")
                continue

        # Assign to lead
        if all_emails:
            lead.email = "; ".join(sorted(all_emails))
        for platform, url_val in found_socials.items():
            setattr(lead, platform, url_val)

    @staticmethod
    def _is_valid_email(email: str) -> bool:
        """Check if an email looks real (not a false positive)."""
        if not email or "@" not in email:
            return False
        domain = email.split("@")[1].lower()
        if domain in EMAIL_BLACKLIST:
            return False
        # Filter out image/file extensions
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
            "business_name": name,
            "owner_name": lead.get("owner_name", "N/A") or "N/A",
            "phone": phone or "N/A",
            "website": website or "N/A",
            "email": lead.get("email", "N/A") or "N/A",
            "address": lead.get("address", "N/A") or "N/A",
            "rating": lead.get("rating", "N/A") or "N/A",
            "reviews": lead.get("reviews", "N/A") or "N/A",
            "category": lead.get("category", "N/A") or "N/A",
            "facebook": lead.get("facebook", "N/A") or "N/A",
            "instagram": lead.get("instagram", "N/A") or "N/A",
            "twitter": lead.get("twitter", "N/A") or "N/A",
            "linkedin": lead.get("linkedin", "N/A") or "N/A",
            "youtube": lead.get("youtube", "N/A") or "N/A",
            "tiktok": lead.get("tiktok", "N/A") or "N/A",
            "pinterest": lead.get("pinterest", "N/A") or "N/A",
        })

    return cleaned
