"""
LeadGen — Intelligence: Entity Resolver

Matches incoming lead records to existing canonical entities (lead_core),
merges duplicates, and creates new entities when no match is found.

Tiers:
  ≥ 0.92 → auto-merge
  0.70 – 0.92 → create proposal + new entity
  < 0.70 → create new entity
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading

from intelligence.normalizer import (
    normalize_name, normalize_phone, extract_domain,
    haversine_km, geohash_prefix,
)

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local = threading.local()

AUTO_MERGE_THRESHOLD    = 0.88
MANUAL_REVIEW_THRESHOLD = 0.60

# Matching weights
PHONE_WEIGHT    = 0.50
DOMAIN_WEIGHT   = 0.38
NAME_WEIGHT     = 0.20
GEO_WEIGHT      = 0.15
CATEGORY_BONUS  = 0.05


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
        _local.db.execute("PRAGMA busy_timeout=5000")
    return _local.db


# ── Jaro-Winkler similarity (no external deps) ─────────────────────────────

def _jaro(s1: str, s2: str) -> float:
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0
    match_dist = max(len(s1), len(s2)) // 2 - 1
    match_dist = max(0, match_dist)
    s1_matches = [False] * len(s1)
    s2_matches = [False] * len(s2)
    matches = 0
    transpositions = 0
    for i, c1 in enumerate(s1):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, len(s2))
        for j in range(start, end):
            if s2_matches[j] or c1 != s2[j]:
                continue
            s1_matches[i] = s2_matches[j] = True
            matches += 1
            break
    if matches == 0:
        return 0.0
    k = 0
    for i, matched in enumerate(s1_matches):
        if not matched:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1
    return (matches / len(s1) + matches / len(s2) +
            (matches - transpositions / 2) / matches) / 3


def jaro_winkler(s1: str, s2: str, p: float = 0.1) -> float:
    j = _jaro(s1, s2)
    prefix = 0
    for c1, c2 in zip(s1[:4], s2[:4]):
        if c1 == c2:
            prefix += 1
        else:
            break
    return j + prefix * p * (1 - j)


# ── Match confidence ────────────────────────────────────────────────────────

def compute_match_confidence(record_a: dict, record_b: dict) -> float:
    score = 0.0

    # Phone exact match (strongest signal)
    phone_a = record_a.get("phone", "")
    phone_b = record_b.get("phone", "")
    if phone_a and phone_b and phone_a == phone_b and len(phone_a) >= 7:
        score += PHONE_WEIGHT

    # Domain exact match
    domain_a = record_a.get("domain", "")
    domain_b = record_b.get("domain", "")
    if domain_a and domain_b and domain_a == domain_b:
        score += DOMAIN_WEIGHT

    # Fuzzy name similarity
    name_a = record_a.get("name_norm") or normalize_name(record_a.get("name", ""))
    name_b = record_b.get("name_norm") or normalize_name(record_b.get("name", ""))
    if name_a and name_b:
        sim = jaro_winkler(name_a, name_b)
        score += NAME_WEIGHT * sim

    # Geo proximity bonus
    try:
        lat_a, lng_a = record_a.get("latitude"), record_a.get("longitude")
        lat_b, lng_b = record_b.get("latitude"), record_b.get("longitude")
        if all(v is not None for v in [lat_a, lng_a, lat_b, lng_b]):
            dist = haversine_km(float(lat_a), float(lng_a), float(lat_b), float(lng_b))
            if dist < 0.1:   score += GEO_WEIGHT
            elif dist < 0.5: score += GEO_WEIGHT * 0.5
    except (ValueError, TypeError):
        pass

    # Category match bonus
    cat_a = (record_a.get("category") or "").lower()
    cat_b = (record_b.get("category") or "").lower()
    if cat_a and cat_b and cat_a == cat_b:
        score += CATEGORY_BONUS

    return min(score, 1.0)


# ── Core resolve function ───────────────────────────────────────────────────

def resolve_entity(record: dict, user_id: int) -> int:
    """
    Match record against existing lead_core entities for this user.
    Returns lead_core.id (either matched/merged, or newly created).
    """
    db = _db()

    # Build blocking query — only compare candidates with similar phone or domain
    phone = record.get("phone", "")
    domain = record.get("domain", "")
    name   = record.get("name", "")

    # Simple SQLite-compatible blocking: phone exact OR domain exact OR name prefix
    name_prefix = name[:4].lower() if name else ""

    rows = db.execute("""
        SELECT lc.id, lc.canonical_name, lc.category, lc.latitude, lc.longitude,
               le.phone, le.domain
        FROM lead_core lc
        LEFT JOIN lead_enrichment le ON le.lead_id = lc.id
        WHERE lc.user_id = ?
          AND lc.merge_status != 'merged'
          AND (
              (le.phone != '' AND le.phone = ?)
              OR (le.domain != '' AND le.domain = ?)
              OR lower(substr(lc.canonical_name, 1, 4)) = lower(?)
          )
        LIMIT 15
    """, (int(user_id), phone, domain, name_prefix)).fetchall()

    best_match_id, best_score = None, 0.0
    for row in rows:
        candidate = {
            "name":      row["canonical_name"],
            "name_norm": normalize_name(row["canonical_name"]),
            "category":  row["category"] or "",
            "phone":     normalize_phone(row["phone"] or ""),
            "domain":    row["domain"] or "",
            "latitude":  row["latitude"],
            "longitude": row["longitude"],
        }
        conf = compute_match_confidence(record, candidate)
        if conf > best_score:
            best_score, best_match_id = conf, row["id"]

    if best_score >= AUTO_MERGE_THRESHOLD and best_match_id:
        log.info(f"Entity resolution: auto-merge into lead_id={best_match_id} (conf={best_score:.3f})")
        merge_source_into(best_match_id, record)
        return best_match_id

    if best_score >= MANUAL_REVIEW_THRESHOLD and best_match_id:
        log.info(f"Entity resolution: merge proposal for lead_id={best_match_id} (conf={best_score:.3f})")
        _create_merge_proposal(best_match_id, record, best_score)
        # Still create a new entity — proposal is for human review
        return _create_new_entity(record, user_id)

    return _create_new_entity(record, user_id)


def _create_new_entity(record: dict, user_id: int) -> int:
    db = _db()
    cursor = db.execute("""
        INSERT INTO lead_core (
            user_id, canonical_name, category, address, city,
            latitude, longitude, geo_hash, merge_status, source_count
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'single', 1)
    """, (
        int(user_id),
        record.get("name", "Unknown Business"),
        record.get("category", ""),
        record.get("address", ""),
        record.get("city", ""),
        record.get("latitude"),
        record.get("longitude"),
        record.get("geo_hash", ""),
    ))
    lead_id = cursor.lastrowid
    db.commit()

    # Create enrichment skeleton
    db.execute("""
        INSERT OR IGNORE INTO lead_enrichment (
            lead_id, phone, email, website, domain,
            instagram_url, linkedin_url, facebook_url,
            google_rating, google_reviews
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        lead_id,
        record.get("phone", ""),
        record.get("email", ""),
        record.get("website", ""),
        record.get("domain", ""),
        record.get("instagram_url", ""),
        record.get("linkedin_url", ""),
        record.get("facebook_url", ""),
        record.get("rating"),
        record.get("reviews"),
    ))
    db.commit()
    log.info(f"Entity resolution: created new lead_id={lead_id} for '{record.get('name')}'")
    return lead_id


def merge_source_into(lead_id: int, record: dict) -> None:
    """
    Field-level conflict resolution when merging a new record into an existing entity.
    Rule: never overwrite with empty; prefer HTTPS; prefer higher rating.
    """
    db = _db()
    existing = db.execute(
        "SELECT * FROM lead_enrichment WHERE lead_id = ?", (lead_id,)
    ).fetchone()

    if not existing:
        db.execute("""
            INSERT OR IGNORE INTO lead_enrichment (lead_id, phone, email, website, domain,
                instagram_url, linkedin_url, facebook_url, google_rating, google_reviews)
            VALUES (?, '', '', '', '', '', '', '', NULL, NULL)
        """, (lead_id,))
        db.commit()
        existing = db.execute("SELECT * FROM lead_enrichment WHERE lead_id = ?", (lead_id,)).fetchone()

    updates: dict = {}

    # Phone: prefer existing, fill if empty
    if not existing["phone"] and record.get("phone"):
        updates["phone"] = record["phone"]

    # Email: fill primary, then alt
    if not existing["email"] and record.get("email"):
        updates["email"] = record["email"]
    elif record.get("email") and record["email"] != existing["email"] and record.get("email"):
        updates["email_alt"] = record["email"]

    # Website: prefer HTTPS
    new_web = record.get("website", "")
    old_web = existing["website"] or ""
    if new_web and (not old_web or (new_web.startswith("https://") and not old_web.startswith("https://"))):
        updates["website"] = new_web
        updates["domain"]  = record.get("domain", "") or extract_domain(new_web)

    # Social: fill if empty
    for field in ("instagram_url", "linkedin_url", "facebook_url"):
        if not existing[field] and record.get(field):
            updates[field] = record[field]

    # Rating: keep best
    new_rating = record.get("rating")
    old_rating = existing["google_rating"]
    if new_rating and (not old_rating or float(new_rating) > float(old_rating)):
        updates["google_rating"]  = new_rating
        updates["google_reviews"] = record.get("reviews") or existing["google_reviews"]

    # Instagram followers: keep max
    new_followers = record.get("instagram_followers")
    if new_followers and (not existing.get("instagram_followers") or new_followers > existing["instagram_followers"]):
        updates["instagram_followers"] = new_followers

    if updates:
        set_clause = ", ".join(f"{k} = ?" for k in updates)
        db.execute(
            f"UPDATE lead_enrichment SET {set_clause}, updated_at = datetime('now') WHERE lead_id = ?",
            list(updates.values()) + [lead_id],
        )

    # Always update lead_core metadata
    db.execute("""
        UPDATE lead_core
        SET last_seen_at = datetime('now'),
            source_count = source_count + 1,
            updated_at = datetime('now')
        WHERE id = ?
    """, (lead_id,))
    db.commit()


def _create_merge_proposal(lead_id: int, record: dict, confidence: float) -> None:
    """Store a proposed merge for manual review (uses lead_core.merge_status)."""
    db = _db()
    try:
        db.execute("""
            INSERT OR IGNORE INTO merge_proposals
                (lead_id, proposed_name, proposed_phone, proposed_domain, confidence, raw_data)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            lead_id,
            record.get("name", ""),
            record.get("phone", ""),
            record.get("domain", ""),
            round(confidence, 4),
            json.dumps(record.get("raw", {}), default=str)[:4000],
        ))
        db.commit()
    except Exception:
        pass  # Table may not exist yet — proposals are optional
