"""
LeadGen — Intelligence: Field Normalizer

Standardizes raw scraped fields so entity resolution can match across sources.
"""
from __future__ import annotations

import re
import unicodedata


# ── Phone normalization ────────────────────────────────────────────────────

_PHONE_DIGITS_RE = re.compile(r"[^\d+]")
_BUSINESS_SUFFIX_RE = re.compile(
    r"\b(llc|ltd|inc|co|corp|limited|pvt|pty|plc|gmbh|sa|bv|srl|lp|llp|pllc)\.?\b",
    re.IGNORECASE,
)
_NOISE_WORDS_RE = re.compile(
    r"\b(the|a|an|and|of|&)\b", re.IGNORECASE
)


def normalize_phone(phone: str) -> str:
    """
    Strip non-digits. Returns digits-only string (for matching), e.g. '12125550001'.
    Handles leading +, country codes. Returns '' if too short to be valid.
    """
    if not phone:
        return ""
    digits = _PHONE_DIGITS_RE.sub("", phone)
    # Strip leading country code for Pakistan (+92) or US (+1) to get local
    # But keep full number for uniqueness — return 10+ digit strings
    if len(digits) < 7:
        return ""
    return digits


def normalize_phone_display(phone: str) -> str:
    """Returns a cleaned phone for display, not for matching."""
    raw = normalize_phone(phone)
    return phone.strip() if phone else ""


# ── Domain extraction ──────────────────────────────────────────────────────

_SCHEME_RE = re.compile(r"^https?://", re.IGNORECASE)
_WWW_RE    = re.compile(r"^www\.", re.IGNORECASE)


def extract_domain(url: str) -> str:
    """
    'https://www.DentalDepot.com/about' → 'dentaldepot.com'
    Returns '' if not a valid URL.
    """
    if not url:
        return ""
    url = url.strip()
    # Remove scheme
    url = _SCHEME_RE.sub("", url)
    # Take only the host part (before first /)
    host = url.split("/")[0].split("?")[0].split("#")[0]
    # Remove www.
    host = _WWW_RE.sub("", host)
    return host.lower().rstrip(".")


# ── Business name normalization ────────────────────────────────────────────

def normalize_name(name: str) -> str:
    """
    'Dental Depot, LLC' → 'dental depot'
    Used for fuzzy matching — NOT for display (use canonical_name for that).
    """
    if not name:
        return ""
    # Unicode normalize
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    # Lowercase
    name = name.lower()
    # Remove business suffixes
    name = _BUSINESS_SUFFIX_RE.sub("", name)
    # Remove noise words
    name = _NOISE_WORDS_RE.sub("", name)
    # Remove punctuation except spaces
    name = re.sub(r"[^a-z0-9\s]", " ", name)
    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()
    return name


def canonical_name_from_sources(names: list[str]) -> str:
    """
    Choose the best canonical name from a list of raw names.
    Prefer: longest non-empty name (more likely to be the full name).
    """
    cleaned = [n.strip() for n in names if n and n.strip()]
    if not cleaned:
        return ""
    return max(cleaned, key=len)


# ── Geohash (simple prefix-based) ─────────────────────────────────────────

def geohash_prefix(lat: float | None, lng: float | None, precision: int = 4) -> str:
    """
    Encode lat/lng to a geohash prefix for proximity blocking.
    Uses a simple grid: each cell is ~5km at precision=4.
    """
    if lat is None or lng is None:
        return ""
    try:
        # Encode to ~1km resolution grid string for blocking
        lat_int = int((float(lat) + 90) * 10 ** (precision - 2))
        lng_int = int((float(lng) + 180) * 10 ** (precision - 2))
        return f"{lat_int:06d}{lng_int:07d}"
    except (ValueError, TypeError):
        return ""


def haversine_km(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Compute distance between two lat/lng points in km."""
    import math
    R = 6371  # Earth radius km
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1))
         * math.cos(math.radians(lat2))
         * math.sin(dlng / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# ── General record normalize ────────────────────────────────────────────────

def normalize_record(raw: dict) -> dict:
    """
    Normalize a raw lead dict (from scraper output) into a consistent structure
    for entity resolution and enrichment insertion.
    """
    name    = (raw.get("business_name") or raw.get("name") or "").strip()
    phone   = normalize_phone(raw.get("phone") or "")
    website = (raw.get("website") or "").strip().rstrip("/")
    domain  = extract_domain(website)
    email   = (raw.get("email") or "").strip().lower()

    try:
        lat = float(raw.get("latitude") or raw.get("lat") or 0) or None
        lng = float(raw.get("longitude") or raw.get("lng") or 0) or None
    except (ValueError, TypeError):
        lat = lng = None

    try:
        rating = float(raw.get("rating") or 0) or None
    except (ValueError, TypeError):
        rating = None

    try:
        reviews = int(str(raw.get("reviews") or "0").replace(",", "")) or None
    except (ValueError, TypeError):
        reviews = None

    return {
        "name":          name,
        "name_norm":     normalize_name(name),
        "phone":         phone,
        "phone_raw":     raw.get("phone", ""),
        "website":       website,
        "domain":        domain,
        "email":         email,
        "address":       (raw.get("address") or "").strip(),
        "category":      (raw.get("category") or "").strip(),
        "city":          (raw.get("city") or raw.get("place") or "").strip(),
        "latitude":      lat,
        "longitude":     lng,
        "geo_hash":      geohash_prefix(lat, lng),
        "rating":        rating,
        "reviews":       reviews,
        "instagram_url": (raw.get("instagram") or raw.get("instagram_url") or "").strip(),
        "linkedin_url":  (raw.get("linkedin") or raw.get("linkedin_url") or "").strip(),
        "facebook_url":  (raw.get("facebook") or raw.get("facebook_url") or "").strip(),
        "raw":           raw,
    }
