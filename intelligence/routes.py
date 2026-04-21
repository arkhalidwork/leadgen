"""
LeadGen — Intelligence: Flask API Routes

Endpoints:
  GET  /api/intelligence/leads              — filtered lead list with scores + signals
  GET  /api/intelligence/leads/<id>         — full lead detail + insights
  GET  /api/intelligence/leads/<id>/similar — entity match candidates
  POST /api/intelligence/leads/<id>/rescore — re-run scoring + insights
  GET  /api/intelligence/stats              — distribution breakdown
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from flask import Blueprint, jsonify, request, session

log = logging.getLogger(__name__)

intelligence_bp = Blueprint("intelligence", __name__, url_prefix="/api/intelligence")

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local   = threading.local()


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
    return _local.db


def _require_user():
    uid = session.get("user_id")
    if not uid:
        return None, (jsonify({"error": "Authentication required"}), 401)
    return int(uid), None


def _parse_json_field(val, default):
    if not val:
        return default
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return default


# ── GET /api/intelligence/leads ────────────────────────────────────────────

@intelligence_bp.route("/leads", methods=["GET"])
def list_leads():
    user_id, err = _require_user()
    if err:
        return err

    tier     = request.args.get("tier", "").lower()
    category = request.args.get("category", "")
    source   = request.args.get("source", "")
    signal   = request.args.get("signal", "")
    sort     = request.args.get("sort", "score")       # score | name | recency
    limit    = min(int(request.args.get("limit", 50)), 200)
    offset   = int(request.args.get("offset", 0))
    q        = request.args.get("q", "").strip()       # name search

    db = _db()

    # Build WHERE clauses
    where_parts = ["lc.user_id = ?", "lc.merge_status != 'merged'"]
    params: list = [user_id]

    if tier:
        where_parts.append("ls.tier = ?")
        params.append(tier)
    if category:
        where_parts.append("lower(lc.category) LIKE ?")
        params.append(f"%{category.lower()}%")
    if q:
        where_parts.append("lower(lc.canonical_name) LIKE ?")
        params.append(f"%{q.lower()}%")

    order_clause = {
        "score":   "COALESCE(ls.total_score, 0) DESC",
        "name":    "lc.canonical_name ASC",
        "recency": "lc.last_seen_at DESC",
    }.get(sort, "COALESCE(ls.total_score, 0) DESC")

    where_sql = " AND ".join(where_parts)

    # Signal filter (sub-query)
    signal_join  = ""
    signal_where = ""
    if signal:
        signal_join  = "INNER JOIN lead_signals sig_f ON sig_f.lead_id = lc.id AND sig_f.signal_type = ?"
        params.insert(-offset if offset else len(params), signal)
        # Actually append signal param before limit/offset
        signal_params = [signal]
    else:
        signal_params = []

    # Main count
    count_sql = f"""
        SELECT COUNT(DISTINCT lc.id)
        FROM lead_core lc
        LEFT JOIN lead_scores ls ON ls.lead_id = lc.id
        {"INNER JOIN lead_signals sig_f ON sig_f.lead_id = lc.id AND sig_f.signal_type = ?" if signal else ""}
        WHERE {where_sql}
    """
    count_params = signal_params + params[:]
    total = db.execute(count_sql, count_params).fetchone()[0]

    # Main query
    query_sql = f"""
        SELECT
            lc.id, lc.canonical_name, lc.category, lc.city, lc.source_count,
            lc.last_seen_at, lc.first_seen_at,
            COALESCE(ls.total_score, 0) as total_score,
            COALESCE(ls.tier, 'cold') as tier,
            le.phone, le.email, le.website, le.instagram_url, le.linkedin_url,
            le.google_rating, le.google_reviews, le.instagram_followers,
            li.next_action, li.summary
        FROM lead_core lc
        LEFT JOIN lead_scores ls ON ls.lead_id = lc.id
        LEFT JOIN lead_enrichment le ON le.lead_id = lc.id
        LEFT JOIN lead_insights li ON li.lead_id = lc.id
        {"INNER JOIN lead_signals sig_f ON sig_f.lead_id = lc.id AND sig_f.signal_type = ?" if signal else ""}
        WHERE {where_sql}
        GROUP BY lc.id
        ORDER BY {order_clause}
        LIMIT ? OFFSET ?
    """
    query_params = signal_params + params + [limit, offset]
    rows = db.execute(query_sql, query_params).fetchall()

    # Build signals map (batch fetch for performance)
    lead_ids = [r["id"] for r in rows]
    signals_map: dict[int, list[str]] = {}
    if lead_ids:
        ph = ",".join("?" * len(lead_ids))
        sig_rows = db.execute(
            f"SELECT lead_id, signal_type FROM lead_signals WHERE lead_id IN ({ph}) ORDER BY confidence DESC",
            lead_ids,
        ).fetchall()
        for sr in sig_rows:
            signals_map.setdefault(sr["lead_id"], []).append(sr["signal_type"])

    from intelligence.scorer import tier_color
    leads_out = []
    for r in rows:
        tier_val = r["tier"] or "cold"
        leads_out.append({
            "lead_id":        r["id"],
            "canonical_name": r["canonical_name"],
            "category":       r["category"] or "",
            "city":           r["city"] or "",
            "source_count":   r["source_count"] or 1,
            "last_seen_at":   r["last_seen_at"],
            "score":          round(float(r["total_score"]), 1),
            "tier":           tier_val,
            "tier_color":     tier_color(tier_val),
            "signals":        signals_map.get(r["id"], [])[:6],
            "has_email":      bool(r["email"]),
            "has_phone":      bool(r["phone"]),
            "has_website":    bool(r["website"]),
            "has_instagram":  bool(r["instagram_url"]),
            "google_rating":  r["google_rating"],
            "instagram_followers": r["instagram_followers"],
            "next_action":    r["next_action"] or "",
            "summary":        (r["summary"] or "")[:200],
        })

    # Score distribution
    dist_rows = db.execute("""
        SELECT COALESCE(ls.tier, 'cold') tier, COUNT(*) cnt
        FROM lead_core lc
        LEFT JOIN lead_scores ls ON ls.lead_id = lc.id
        WHERE lc.user_id = ? AND lc.merge_status != 'merged'
        GROUP BY tier
    """, (user_id,)).fetchall()
    dist = {r["tier"]: r["cnt"] for r in dist_rows}

    return jsonify({
        "total":              total,
        "limit":              limit,
        "offset":             offset,
        "leads":              leads_out,
        "score_distribution": dist,
        "filters_applied":    {k: v for k, v in {
            "tier": tier, "category": category, "signal": signal, "q": q
        }.items() if v},
    })


# ── GET /api/intelligence/leads/<id> ───────────────────────────────────────

@intelligence_bp.route("/leads/<int:lead_id>", methods=["GET"])
def lead_detail(lead_id: int):
    user_id, err = _require_user()
    if err:
        return err

    db = _db()

    core = db.execute(
        "SELECT * FROM lead_core WHERE id = ? AND user_id = ?",
        (lead_id, user_id),
    ).fetchone()
    if not core:
        return jsonify({"error": "Lead not found"}), 404

    enrichment = db.execute(
        "SELECT * FROM lead_enrichment WHERE lead_id = ?", (lead_id,)
    ).fetchone()

    score = db.execute(
        "SELECT * FROM lead_scores WHERE lead_id = ?", (lead_id,)
    ).fetchone()

    insights = db.execute(
        "SELECT * FROM lead_insights WHERE lead_id = ? ORDER BY generated_at DESC LIMIT 1",
        (lead_id,),
    ).fetchone()

    signals = db.execute(
        "SELECT * FROM lead_signals WHERE lead_id = ? ORDER BY confidence DESC",
        (lead_id,),
    ).fetchall()

    sources = db.execute(
        "SELECT source, job_id, scraped_at, raw_name, raw_phone, raw_website FROM lead_sources WHERE lead_id = ? ORDER BY scraped_at DESC",
        (lead_id,),
    ).fetchall()

    e = dict(enrichment) if enrichment else {}
    s = dict(score) if score else {}
    i = dict(insights) if insights else {}

    from intelligence.scorer import tier_color

    return jsonify({
        "lead_id":        core["id"],
        "canonical_name": core["canonical_name"],
        "category":       core["category"] or "",
        "city":           core["city"] or "",
        "address":        core["address"] or "",
        "latitude":       core["latitude"],
        "longitude":      core["longitude"],
        "source_count":   core["source_count"] or 1,
        "merge_status":   core["merge_status"] or "single",
        "first_seen_at":  core["first_seen_at"],
        "last_seen_at":   core["last_seen_at"],

        "score":      round(float(s.get("total_score", 0)), 1),
        "tier":       s.get("tier", "cold"),
        "tier_color": tier_color(s.get("tier", "cold")),
        "sub_scores": {
            "completeness": s.get("completeness_score", 0),
            "social":       s.get("social_score", 0),
            "activity":     s.get("activity_score", 0),
            "sentiment":    s.get("sentiment_score", 0),
            "freshness":    s.get("freshness_score", 0),
        },
        "scored_at": s.get("scored_at"),

        "enrichment": {
            "phone":             e.get("phone", ""),
            "phone_alt":         e.get("phone_alt", ""),
            "email":             e.get("email", ""),
            "email_alt":         e.get("email_alt", ""),
            "website":           e.get("website", ""),
            "domain":            e.get("domain", ""),
            "instagram_url":     e.get("instagram_url", ""),
            "linkedin_url":      e.get("linkedin_url", ""),
            "facebook_url":      e.get("facebook_url", ""),
            "twitter_url":       e.get("twitter_url", ""),
            "google_rating":     e.get("google_rating"),
            "google_reviews":    e.get("google_reviews"),
            "instagram_followers": e.get("instagram_followers"),
            "instagram_posts":   e.get("instagram_posts"),
        },

        "signals": [
            {
                "type":        r["signal_type"],
                "confidence":  round(float(r["confidence"]), 3),
                "value":       r["value"] or "",
                "source":      r["source"] or "",
                "detected_at": r["detected_at"],
            }
            for r in signals
        ],

        "insights": {
            "summary":         i.get("summary", ""),
            "strengths":       _parse_json_field(i.get("strengths"), []),
            "weaknesses":      _parse_json_field(i.get("weaknesses"), []),
            "outreach_angles": _parse_json_field(i.get("outreach_angles"), []),
            "next_action":     i.get("next_action", ""),
            "generated_by":    i.get("generated_by", "rules"),
            "generated_at":    i.get("generated_at"),
        },

        "sources": [
            {
                "source":     r["source"],
                "job_id":     r["job_id"],
                "scraped_at": r["scraped_at"],
                "raw_name":   r["raw_name"],
            }
            for r in sources
        ],
    })


# ── GET /api/intelligence/leads/<id>/similar ───────────────────────────────

@intelligence_bp.route("/leads/<int:lead_id>/similar", methods=["GET"])
def lead_similar(lead_id: int):
    user_id, err = _require_user()
    if err:
        return err

    db = _db()
    core = db.execute(
        "SELECT * FROM lead_core WHERE id = ? AND user_id = ?",
        (lead_id, user_id),
    ).fetchone()
    if not core:
        return jsonify({"error": "Lead not found"}), 404

    from intelligence.normalizer import normalize_name
    from intelligence.resolver import compute_match_confidence

    name_prefix = core["canonical_name"][:4].lower()
    candidates = db.execute("""
        SELECT lc.id, lc.canonical_name, lc.category, lc.latitude, lc.longitude,
               le.phone, le.domain
        FROM lead_core lc
        LEFT JOIN lead_enrichment le ON le.lead_id = lc.id
        WHERE lc.user_id = ? AND lc.id != ? AND lc.merge_status != 'merged'
          AND lower(substr(lc.canonical_name, 1, 4)) = ?
        LIMIT 10
    """, (user_id, lead_id, name_prefix)).fetchall()

    enrichment = db.execute(
        "SELECT * FROM lead_enrichment WHERE lead_id = ?", (lead_id,)
    ).fetchone()
    e = dict(enrichment) if enrichment else {}

    record_a = {
        "name": core["canonical_name"],
        "name_norm": normalize_name(core["canonical_name"]),
        "phone": e.get("phone", ""),
        "domain": e.get("domain", ""),
        "category": core["category"] or "",
        "latitude": core["latitude"],
        "longitude": core["longitude"],
    }

    similar = []
    for c in candidates:
        conf = compute_match_confidence(record_a, {
            "name": c["canonical_name"],
            "name_norm": normalize_name(c["canonical_name"]),
            "phone": c["phone"] or "",
            "domain": c["domain"] or "",
            "category": c["category"] or "",
            "latitude": c["latitude"],
            "longitude": c["longitude"],
        })
        if conf >= 0.40:
            similar.append({
                "lead_id":       c["id"],
                "name":          c["canonical_name"],
                "confidence":    round(conf, 3),
                "merge_proposed": conf >= 0.70,
            })

    similar.sort(key=lambda x: -x["confidence"])
    return jsonify({"similar_leads": similar[:5]})


# ── POST /api/intelligence/leads/<id>/rescore ──────────────────────────────

@intelligence_bp.route("/leads/<int:lead_id>/rescore", methods=["POST"])
def rescore_lead(lead_id: int):
    user_id, err = _require_user()
    if err:
        return err

    db = _db()
    core = db.execute(
        "SELECT id FROM lead_core WHERE id = ? AND user_id = ?",
        (lead_id, user_id),
    ).fetchone()
    if not core:
        return jsonify({"error": "Lead not found"}), 404

    try:
        from intelligence.pipeline import _rescore_lead
        _rescore_lead(lead_id)
        score = db.execute(
            "SELECT total_score, tier FROM lead_scores WHERE lead_id = ?",
            (lead_id,),
        ).fetchone()
        return jsonify({
            "ok":        True,
            "new_score": round(float(score["total_score"]), 1) if score else 0,
            "tier":      score["tier"] if score else "cold",
        })
    except Exception as exc:
        log.error(f"Rescore {lead_id}: {exc}")
        return jsonify({"error": str(exc)}), 500


# ── GET /api/intelligence/stats ────────────────────────────────────────────

@intelligence_bp.route("/stats", methods=["GET"])
def intelligence_stats():
    user_id, err = _require_user()
    if err:
        return err

    db = _db()

    tier_dist = db.execute("""
        SELECT COALESCE(ls.tier, 'unscored') tier, COUNT(*) cnt
        FROM lead_core lc
        LEFT JOIN lead_scores ls ON ls.lead_id = lc.id
        WHERE lc.user_id = ? AND lc.merge_status != 'merged'
        GROUP BY tier ORDER BY cnt DESC
    """, (user_id,)).fetchall()

    top_signals = db.execute("""
        SELECT sig.signal_type, COUNT(DISTINCT sig.lead_id) cnt
        FROM lead_signals sig
        INNER JOIN lead_core lc ON lc.id = sig.lead_id
        WHERE lc.user_id = ?
        GROUP BY sig.signal_type ORDER BY cnt DESC LIMIT 8
    """, (user_id,)).fetchall()

    total_leads = db.execute(
        "SELECT COUNT(*) FROM lead_core WHERE user_id = ? AND merge_status != 'merged'",
        (user_id,),
    ).fetchone()[0]

    avg_score = db.execute("""
        SELECT AVG(ls.total_score)
        FROM lead_scores ls
        INNER JOIN lead_core lc ON lc.id = ls.lead_id
        WHERE lc.user_id = ?
    """, (user_id,)).fetchone()[0] or 0

    return jsonify({
        "total_leads":    total_leads,
        "avg_score":      round(float(avg_score), 1),
        "tier_distribution": {r["tier"]: r["cnt"] for r in tier_dist},
        "top_signals":       [{"signal": r["signal_type"], "count": r["cnt"]}
                              for r in top_signals],
    })
