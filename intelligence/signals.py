"""
LeadGen — Intelligence: Signal Extractor

Detects behavioral intent signals from raw scraped data per source.
Each signal has a type, confidence (0–1), value (text evidence), and source.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone, timedelta
from typing import Callable

log = logging.getLogger(__name__)

# ── Helpers ────────────────────────────────────────────────────────────────

def _days_since(date_str: str | None) -> int:
    """Parse a date string and return days since today. Returns 9999 on failure."""
    if not date_str:
        return 9999
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%dT%H:%M:%S%z", "%d/%m/%Y", "%m/%d/%Y"):
        try:
            d = datetime.strptime(str(date_str)[:19], fmt[:len(str(date_str)[:10])])
            return max(0, (datetime.utcnow() - d).days)
        except ValueError:
            continue
    return 9999


def _engagement_rate(raw: dict) -> float:
    """Instagram engagement rate = (likes + comments) / followers."""
    followers = int(raw.get("instagram_followers") or raw.get("followers") or 0)
    likes     = int(raw.get("avg_likes") or raw.get("likes") or 0)
    comments  = int(raw.get("avg_comments") or raw.get("comments") or 0)
    if followers < 10:
        return 0.0
    return (likes + comments) / followers


def _has_hiring_keyword(text: str) -> bool:
    keywords = [
        "hiring", "we're growing", "join our team", "now recruiting",
        "open position", "job opening", "career opportunity", "apply now",
        "looking for", "vacancy", "vacancies",
    ]
    t = text.lower()
    return any(kw in t for kw in keywords)


def _has_growth_keyword(text: str) -> bool:
    keywords = [
        "new location", "expanding", "just opened", "grand opening",
        "new branch", "now serving", "coming soon", "second location",
        "franchise", "scale", "growth",
    ]
    t = text.lower()
    return any(kw in t for kw in keywords)


def _poor_website_reason(raw: dict) -> str:
    reasons = []
    if raw.get("ssl") is False:
        reasons.append("No SSL/HTTPS")
    if raw.get("mobile_friendly") is False:
        reasons.append("Not mobile-friendly")
    if int(raw.get("broken_links_count") or 0) > 5:
        reasons.append(f"{raw['broken_links_count']} broken links")
    if int(raw.get("page_speed_score") or 100) < 40:
        reasons.append(f"PageSpeed score: {raw['page_speed_score']}")
    return "; ".join(reasons) or "Poor quality indicators"


# ── Signal Extractor Registry ──────────────────────────────────────────────

def extract_signals(lead_id: int, source: str, raw: dict) -> list[dict]:
    """
    Run all applicable extractors for a given source.
    Returns list of signal dicts ready for DB insertion.
    """
    results = []

    # ── Hiring signal ──────────────────────────────────────────────────────
    if source in ("linkedin", "webcrawler", "gmaps"):
        text = " ".join([
            str(raw.get("description", "")),
            str(raw.get("about", "")),
            str(raw.get("bio", "")),
            str(raw.get("title", "")),
        ])
        if _has_hiring_keyword(text):
            results.append({
                "lead_id":     lead_id,
                "signal_type": "hiring",
                "confidence":  0.85,
                "value":       _first_match(text, ["hiring", "recruiting", "join our team", "open position"])[:200],
                "source":      source,
            })

    # ── Active social ──────────────────────────────────────────────────────
    if source == "instagram":
        last_post = raw.get("last_post_date") or raw.get("latest_post_date")
        days = _days_since(last_post)
        if days < 30:
            conf = 1.0 if days < 7 else 0.80
            results.append({
                "lead_id":     lead_id,
                "signal_type": "active_social",
                "confidence":  conf,
                "value":       f"Last post {days} days ago" + (f" (on {last_post})" if last_post else ""),
                "source":      source,
            })

    # ── Recent reviews ─────────────────────────────────────────────────────
    if source == "gmaps":
        recent = int(raw.get("reviews_last_30_days") or 0)
        if recent > 0:
            conf = min(1.0, recent / 10)
            results.append({
                "lead_id":     lead_id,
                "signal_type": "recent_reviews",
                "confidence":  round(conf, 3),
                "value":       f"{recent} new reviews in last 30 days",
                "source":      source,
            })

    # ── Growth indicators ──────────────────────────────────────────────────
    if source in ("gmaps", "linkedin", "webcrawler", "instagram"):
        text = " ".join(str(v) for v in raw.values() if isinstance(v, str))
        if _has_growth_keyword(text):
            results.append({
                "lead_id":     lead_id,
                "signal_type": "growth_indicators",
                "confidence":  0.72,
                "value":       _first_match(text, ["new location", "expanding", "grand opening", "coming soon"])[:200],
                "source":      source,
            })

    # ── Poor website ───────────────────────────────────────────────────────
    if source == "webcrawler":
        is_poor = (
            raw.get("ssl") is False
            or raw.get("mobile_friendly") is False
            or int(raw.get("broken_links_count") or 0) > 5
            or int(raw.get("page_speed_score") or 100) < 40
        )
        if is_poor:
            results.append({
                "lead_id":     lead_id,
                "signal_type": "poor_website",
                "confidence":  0.90,
                "value":       _poor_website_reason(raw),
                "source":      source,
            })

    # ── No website ─────────────────────────────────────────────────────────
    # This is checked at pipeline level (across all sources), not per-source.

    # ── Email available ────────────────────────────────────────────────────
    email = str(raw.get("email") or "").strip()
    if email and "@" in email and source != "":
        results.append({
            "lead_id":     lead_id,
            "signal_type": "email_available",
            "confidence":  1.0,
            "value":       email,
            "source":      source,
        })

    # ── High engagement ────────────────────────────────────────────────────
    if source == "instagram":
        er = _engagement_rate(raw)
        if er > 0.035:
            results.append({
                "lead_id":     lead_id,
                "signal_type": "high_engagement",
                "confidence":  min(1.0, round(er / 0.10, 3)),
                "value":       f"{er*100:.1f}% engagement rate",
                "source":      source,
            })

    return results


def _first_match(text: str, keywords: list[str]) -> str:
    """Find the first sentence containing any keyword."""
    tl = text.lower()
    for kw in keywords:
        idx = tl.find(kw)
        if idx >= 0:
            start = max(0, idx - 40)
            end   = min(len(text), idx + 80)
            return "..." + text[start:end] + "..."
    return ""


def add_no_website_signal(lead_id: int) -> dict:
    """Called by pipeline when enrichment has no website across all sources."""
    return {
        "lead_id":     lead_id,
        "signal_type": "no_website",
        "confidence":  1.0,
        "value":       "No website found across all scraped sources",
        "source":      "pipeline",
    }
