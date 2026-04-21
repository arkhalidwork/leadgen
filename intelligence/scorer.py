"""
LeadGen — Intelligence: Lead Scorer

Computes a 0–100 composite score from 5 sub-scores:
  completeness (30) + social (20) + activity (20) + sentiment (15) + freshness (15)
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from datetime import datetime

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local = threading.local()


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
    return _local.db


# ── Sub-score formulas ─────────────────────────────────────────────────────

def score_completeness(enrichment: dict) -> float:
    """Max 30 pts. Rewards having real contact data."""
    score = 0.0
    score += 10 if bool(enrichment.get("email"))         else 0
    score += 8  if bool(enrichment.get("phone"))         else 0
    score += 7  if bool(enrichment.get("website"))       else 0
    score += 5  if _has_any_social(enrichment)           else 0
    return score


def score_social(enrichment: dict) -> float:
    """Max 20 pts. Rewards follower count + multi-platform presence."""
    score = 0.0
    followers = int(enrichment.get("instagram_followers") or 0)
    posts     = int(enrichment.get("instagram_posts") or 0)

    if followers > 0:
        score += min(8.0, 8.0 * (followers / 10_000))
    if posts > 0:
        score += min(5.0, 5.0 * (min(posts, 100) / 100))
    if bool(enrichment.get("linkedin_url")):
        score += 4.0
    if bool(enrichment.get("facebook_url")) or bool(enrichment.get("twitter_url")):
        score += 3.0
    return min(score, 20.0)


def score_activity(signals: list[dict]) -> float:
    """Max 20 pts. Rewards behavioral signals weighted by confidence."""
    WEIGHTS = {
        "active_social":     6.0,
        "recent_reviews":    5.0,
        "high_engagement":   5.0,
        "hiring":            2.0,
        "growth_indicators": 2.0,
    }
    score = 0.0
    seen  = set()
    for sig in signals:
        stype = sig.get("signal_type", "")
        if stype in WEIGHTS and stype not in seen:
            conf = float(sig.get("confidence") or 1.0)
            score += WEIGHTS[stype] * min(1.0, conf)
            seen.add(stype)
    return min(score, 20.0)


def score_sentiment(enrichment: dict) -> float:
    """Max 15 pts. Google rating as quality/trust proxy."""
    rating  = float(enrichment.get("google_rating") or 0)
    reviews = int(enrichment.get("google_reviews") or 0)
    if rating == 0:
        return 0.0
    # Rating component: 3.0→0 pts, 5.0→10 pts (linear)
    rating_score  = max(0.0, (rating - 3.0) / 2.0) * 10.0
    # Volume component: 0→ 0 pts, 100+ → 5 pts
    review_score  = min(5.0, 5.0 * (reviews / 100))
    return round(rating_score + review_score, 2)


def score_freshness(last_seen_at: str | None) -> float:
    """Max 15 pts. Penalizes stale data exponentially."""
    if not last_seen_at:
        return 0.0
    try:
        seen = datetime.fromisoformat(str(last_seen_at).replace("Z", "+00:00"))
        days = max(0, (datetime.utcnow() - seen.replace(tzinfo=None)).days)
    except (ValueError, TypeError):
        return 5.0   # Unknown age → moderate score

    if days < 7:    return 15.0
    if days < 30:   return 12.0
    if days < 90:   return 7.0
    if days < 180:  return 3.0
    return 0.0


def classify_tier(score: float) -> str:
    if score >= 70: return "hot"
    if score >= 45: return "warm"
    if score >= 20: return "cold"
    return "dead"


def tier_color(tier: str) -> str:
    return {
        "hot":  "#ef4444",
        "warm": "#f59e0b",
        "cold": "#6366f1",
        "dead": "#4b5563",
    }.get(tier, "#6b7280")


# ── Main scoring function ──────────────────────────────────────────────────

def compute_score(lead_id: int, enrichment: dict, signals: list[dict],
                  last_seen_at: str | None) -> dict:
    """
    Compute all sub-scores and total. Returns dict ready for lead_scores table.
    """
    c = round(score_completeness(enrichment), 2)
    s = round(score_social(enrichment), 2)
    a = round(score_activity(signals), 2)
    q = round(score_sentiment(enrichment), 2)
    f = round(score_freshness(last_seen_at), 2)
    total = round(c + s + a + q + f, 2)
    tier  = classify_tier(total)

    return {
        "lead_id":            lead_id,
        "total_score":        total,
        "tier":               tier,
        "completeness_score": c,
        "social_score":       s,
        "activity_score":     a,
        "sentiment_score":    q,
        "freshness_score":    f,
        "score_version":      1,
    }


def upsert_score(score_dict: dict) -> None:
    """Save or overwrite the score record for a lead."""
    db = _db()
    db.execute("""
        INSERT INTO lead_scores (
            lead_id, total_score, tier,
            completeness_score, social_score, activity_score,
            sentiment_score, freshness_score, score_version, scored_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(lead_id) DO UPDATE SET
            total_score        = excluded.total_score,
            tier               = excluded.tier,
            completeness_score = excluded.completeness_score,
            social_score       = excluded.social_score,
            activity_score     = excluded.activity_score,
            sentiment_score    = excluded.sentiment_score,
            freshness_score    = excluded.freshness_score,
            score_version      = excluded.score_version,
            scored_at          = datetime('now')
    """, (
        score_dict["lead_id"], score_dict["total_score"], score_dict["tier"],
        score_dict["completeness_score"], score_dict["social_score"],
        score_dict["activity_score"], score_dict["sentiment_score"],
        score_dict["freshness_score"], score_dict["score_version"],
    ))
    db.commit()


# ── Helpers ────────────────────────────────────────────────────────────────

def _has_any_social(e: dict) -> bool:
    return bool(
        e.get("instagram_url") or e.get("linkedin_url")
        or e.get("facebook_url") or e.get("twitter_url")
    )
