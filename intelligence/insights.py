"""
LeadGen — Intelligence: Insight Generator

Rule-based engine that produces:
  - Business summary (sentence)
  - Strengths list
  - Weaknesses list
  - Outreach angles list
  - Next action (top recommendation)
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local   = threading.local()


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
    return _local.db


def _has_signal(signals: list[dict], signal_type: str, min_confidence: float = 0.0) -> bool:
    return any(
        s["signal_type"] == signal_type and float(s.get("confidence", 1.0)) >= min_confidence
        for s in signals
    )


def _signal_value(signals: list[dict], signal_type: str) -> str:
    for s in signals:
        if s["signal_type"] == signal_type:
            return str(s.get("value", ""))
    return ""


# ── Insight Rules ──────────────────────────────────────────────────────────

_RULES = [
    {
        "id": "no_website_active_social",
        "priority": 10,
        "condition": lambda sigs, e: (
            not e.get("website")
            and _has_signal(sigs, "active_social", 0.7)
        ),
        "weakness": "No website — missing online presence despite active social",
        "angle":    "Pitch website creation: they have an audience but nowhere to send them",
        "action":   "Cold email: offer website package (showcase their social proof)",
    },
    {
        "id": "poor_website_high_engagement",
        "priority": 9,
        "condition": lambda sigs, e: (
            _has_signal(sigs, "poor_website", 0.7)
            and _has_signal(sigs, "high_engagement", 0.5)
        ),
        "weakness": "Website needs improvement despite strong social engagement",
        "angle":    "Pitch website redesign — high engagement proves their audience exists",
        "action":   "Show competitor analysis + current engagement stats as proof of market",
    },
    {
        "id": "no_website_has_phone",
        "priority": 8,
        "condition": lambda sigs, e: (
            not e.get("website")
            and bool(e.get("phone"))
        ),
        "weakness": "No website found — relying on phone-only inquiries",
        "angle":    "Offer website + click-to-call landing page to convert their digital traffic",
        "action":   "Call them directly — reference digital gap vs competitors",
    },
    {
        "id": "hiring_or_growth",
        "priority": 8,
        "condition": lambda sigs, e: (
            _has_signal(sigs, "hiring") or _has_signal(sigs, "growth_indicators")
        ),
        "strength": "Business is actively growing",
        "angle":    "They have budget — pitch premium services or bulk/volume deals",
        "action":   "Reach out with growth-focused pitch; emphasize scalability and ROI",
    },
    {
        "id": "high_rating_low_social",
        "priority": 7,
        "condition": lambda sigs, e: (
            float(e.get("google_rating") or 0) >= 4.5
            and not _has_signal(sigs, "active_social")
        ),
        "strength": "Excellent Google reputation",
        "angle":    "Pitch social media management — credibility exists but no social voice",
        "action":   "Show their competitor's social vs theirs; offer social content creation",
    },
    {
        "id": "poor_website_no_growth",
        "priority": 6,
        "condition": lambda sigs, e: (
            _has_signal(sigs, "poor_website", 0.7)
            and not _has_signal(sigs, "growth_indicators")
        ),
        "weakness": "Poor website quality — may be losing customers to competitors",
        "angle":    "Pitch website audit + redesign with conversion focus",
        "action":   "Email with free audit offer; include 2-3 specific issues found",
    },
    {
        "id": "email_warm_lead",
        "priority": 5,
        "condition": lambda sigs, e: (
            bool(e.get("email"))
            and not _has_signal(sigs, "poor_website")
            and not _has_signal(sigs, "no_website")
        ),
        "strength": "Direct email contact available",
        "angle":    "Standard warm outreach — direct contact with real business interest",
        "action":   "Send personalized cold email; reference specific business detail or review",
    },
    {
        "id": "no_contact_info",
        "priority": 2,
        "condition": lambda sigs, e: (
            not e.get("email") and not e.get("phone")
        ),
        "weakness": "No contact information found — hard to reach",
        "angle":    "Manual research needed; LinkedIn or in-person visit may work",
        "action":   "Research business manually; find decision-maker via LinkedIn",
    },
    {
        "id": "recent_reviews_opportunity",
        "priority": 4,
        "condition": lambda sigs, e: (
            _has_signal(sigs, "recent_reviews", 0.6)
            and float(e.get("google_rating") or 0) < 4.0
        ),
        "weakness": "Getting reviews but low rating — reputation management issue",
        "angle":    "Pitch reputation management or customer feedback tools",
        "action":   "Reference low rating trend; offer review management solution",
    },
]


def generate_insights(lead_id: int, core: dict, enrichment: dict,
                      signals: list[dict], score: dict) -> dict:
    """
    Generate insight record for a lead entity.
    Returns dict ready for lead_insights table.
    """
    strengths: list[str]   = []
    weaknesses: list[str]  = []
    angles: list[str]      = []
    actions: list[str]     = []

    matched_rules = []
    for rule in sorted(_RULES, key=lambda r: -r["priority"]):
        try:
            if rule["condition"](signals, enrichment):
                matched_rules.append(rule["id"])
                if "strength" in rule:
                    strengths.append(rule["strength"])
                if "weakness" in rule:
                    weaknesses.append(rule["weakness"])
                if "angle" in rule:
                    angles.append(rule["angle"])
                if "action" in rule:
                    actions.append(rule["action"])
        except Exception as exc:
            log.debug(f"Rule {rule['id']} eval error: {exc}")

    # Data-driven strengths (always appended if data exists)
    rating  = float(enrichment.get("google_rating") or 0)
    reviews = int(enrichment.get("google_reviews") or 0)
    followers = int(enrichment.get("instagram_followers") or 0)

    if rating >= 4.0:
        strengths.append(f"Strong Google rating: {rating:.1f}★ ({reviews:,} reviews)")
    if rating > 0 and rating < 3.5:
        weaknesses.append(f"Low Google rating: {rating:.1f}★ — reputation risk")
    if followers >= 1000:
        strengths.append(f"Active Instagram: {followers:,} followers")
    if enrichment.get("email"):
        strengths.append("Direct email contact available")
    if enrichment.get("phone"):
        strengths.append("Phone number verified")
    if enrichment.get("website"):
        strengths.append("Web presence established")

    # Deduplicate preserving order
    def _dedup(lst): return list(dict.fromkeys(lst))

    summary = _build_summary(core, enrichment, signals, score)

    return {
        "lead_id":        lead_id,
        "summary":        summary,
        "strengths":      json.dumps(_dedup(strengths)[:5]),
        "weaknesses":     json.dumps(_dedup(weaknesses)[:5]),
        "outreach_angles": json.dumps(_dedup(angles)[:3]),
        "next_action":    actions[0] if actions else "Research and qualify lead before outreach",
        "generated_by":   "rules",
        "model_version":  "v2",
    }


def _build_summary(core: dict, e: dict, signals: list[dict], score: dict) -> str:
    name     = core.get("canonical_name", "This business")
    category = core.get("category", "business")
    city     = core.get("city", "")
    loc      = f" in {city}" if city else ""

    parts = [f"{name} is a {category}{loc}."]

    rating = float(e.get("google_rating") or 0)
    reviews = int(e.get("google_reviews") or 0)
    if rating >= 4.0:
        parts.append(f"Rated {rating:.1f}★ on Google with {reviews:,} reviews.")
    elif rating > 0:
        parts.append(f"Google rating: {rating:.1f}★ ({reviews} reviews).")

    if _has_signal(signals, "active_social"):
        parts.append("Actively posting on social media.")
    elif _has_signal(signals, "high_engagement"):
        parts.append("Strong social engagement.")

    if _has_signal(signals, "hiring"):
        parts.append("Currently hiring — growth phase detected.")
    elif _has_signal(signals, "growth_indicators"):
        parts.append("Growth indicators found.")

    if not e.get("website"):
        parts.append("No website detected — significant digital gap.")
    elif _has_signal(signals, "poor_website"):
        parts.append("Website quality issues identified.")

    tier = score.get("tier", "cold")
    total = score.get("total_score", 0)
    parts.append(f"Lead score: {total:.0f}/100 ({tier.upper()} tier).")

    return " ".join(parts)


def upsert_insights(insight_dict: dict) -> None:
    """Save or overwrite insights for a lead."""
    db = _db()
    lead_id = insight_dict["lead_id"]
    db.execute("DELETE FROM lead_insights WHERE lead_id = ?", (lead_id,))
    db.execute("""
        INSERT INTO lead_insights (
            lead_id, summary, strengths, weaknesses,
            outreach_angles, next_action,
            generated_by, model_version, generated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
    """, (
        lead_id,
        insight_dict["summary"],
        insight_dict["strengths"],
        insight_dict["weaknesses"],
        insight_dict["outreach_angles"],
        insight_dict["next_action"],
        insight_dict.get("generated_by", "rules"),
        insight_dict.get("model_version", "v2"),
    ))
    db.commit()
