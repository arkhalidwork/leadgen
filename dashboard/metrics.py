"""
LeadGen — Dashboard Metrics

Aggregated queries for the business metrics dashboard.
All queries are pre-filtered per user and time window.
"""
from __future__ import annotations

import logging
import os
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local   = threading.local()


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
    return _local.db


def get_dashboard_metrics(user_id: int, days: int = 30) -> dict:
    """
    Returns all 6 dashboard widget data sources in one call.
    """
    db    = _db()
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    return {
        "leads_by_day":      _leads_by_day(db, user_id, since),
        "email_stats":       _email_stats(db, user_id, since),
        "pipeline_funnel":   _pipeline_funnel(db, user_id),
        "source_performance":_source_perf(db, user_id),
        "roi":               _roi(db, user_id),
        "active_workflows":  _active_workflows(db, user_id, since),
        "summary":           _summary_numbers(db, user_id, since),
    }


def _leads_by_day(db, user_id: int, since: str) -> list[dict]:
    rows = db.execute("""
        SELECT date(first_seen_at) AS day, COUNT(*) AS count
        FROM lead_core
        WHERE user_id = ? AND first_seen_at >= ?
        GROUP BY day ORDER BY day ASC
    """, (user_id, since)).fetchall()
    return [{"day": r["day"], "count": r["count"]} for r in rows]


def _email_stats(db, user_id: int, since: str) -> dict:
    row = db.execute("""
        SELECT
            COALESCE(SUM(CASE WHEN oe.event_type='sent'    THEN 1 ELSE 0 END), 0) AS sent,
            COALESCE(SUM(CASE WHEN oe.event_type='opened'  THEN 1 ELSE 0 END), 0) AS opens,
            COALESCE(SUM(CASE WHEN oe.event_type='replied' THEN 1 ELSE 0 END), 0) AS replies,
            COALESCE(SUM(CASE WHEN oe.event_type='bounced' THEN 1 ELSE 0 END), 0) AS bounces
        FROM outreach_events oe
        JOIN campaign_leads cl ON cl.id = oe.campaign_lead_id
        JOIN campaigns c ON c.id = oe.campaign_id
        WHERE c.user_id = ? AND oe.occurred_at >= ?
    """, (user_id, since)).fetchone()
    d = dict(row) if row else {"sent": 0, "opens": 0, "replies": 0, "bounces": 0}
    # Compute rates
    sent = d.get("sent") or 0
    d["open_rate"]  = round((d.get("opens",0) / sent * 100), 1) if sent else 0
    d["reply_rate"] = round((d.get("replies",0) / sent * 100), 1) if sent else 0
    return d


def _pipeline_funnel(db, user_id: int) -> list[dict]:
    rows = db.execute("""
        SELECT ps.id, ps.name, ps.color, ps.position, ps.is_terminal, ps.is_winning,
               COUNT(pi.id) AS item_count,
               COALESCE(SUM(pi.deal_value), 0) AS deal_value
        FROM pipeline_stages ps
        LEFT JOIN pipeline_items pi ON pi.stage_id = ps.id
            AND pi.status = 'active' AND pi.user_id = ?
        WHERE ps.user_id = ?
        GROUP BY ps.id ORDER BY ps.position
    """, (user_id, user_id)).fetchall()
    return [dict(r) for r in rows]


def _source_perf(db, user_id: int) -> list[dict]:
    rows = db.execute("""
        SELECT ls.source,
               COUNT(DISTINCT ls.lead_id) AS total_leads,
               COALESCE(SUM(CASE WHEN sc.tier='hot'  THEN 1 ELSE 0 END), 0) AS hot,
               COALESCE(SUM(CASE WHEN sc.tier='warm' THEN 1 ELSE 0 END), 0) AS warm,
               COALESCE(SUM(CASE WHEN sc.tier='cold' THEN 1 ELSE 0 END), 0) AS cold,
               COALESCE(AVG(sc.total_score), 0) AS avg_score
        FROM lead_sources ls
        JOIN lead_core lc ON lc.id = ls.lead_id AND lc.user_id = ?
        LEFT JOIN lead_scores sc ON sc.lead_id = ls.lead_id
        GROUP BY ls.source ORDER BY total_leads DESC
    """, (user_id,)).fetchall()
    return [dict(r) for r in rows]


def _roi(db, user_id: int) -> dict:
    # Converted stage items
    row = db.execute("""
        SELECT COUNT(pi.id) AS conversions,
               COALESCE(SUM(pi.deal_value), 0) AS revenue
        FROM pipeline_items pi
        JOIN pipeline_stages ps ON ps.id = pi.stage_id
        WHERE pi.user_id = ? AND ps.is_winning = 1 AND pi.status = 'active'
    """, (user_id,)).fetchone()
    if not row:
        return {"conversions": 0, "revenue": 0.0}
    return {"conversions": row["conversions"], "revenue": float(row["revenue"])}


def _active_workflows(db, user_id: int, since: str) -> list[dict]:
    rows = db.execute("""
        SELECT w.id, w.name, w.run_count, w.last_run_at, w.status,
               COUNT(wr.id) AS recent_runs,
               COALESCE(SUM(CASE WHEN wr.status='failed' THEN 1 ELSE 0 END), 0) AS recent_failures
        FROM workflows w
        LEFT JOIN workflow_runs wr ON wr.workflow_id = w.id
            AND wr.started_at >= ?
        WHERE w.user_id = ? AND w.status = 'active'
        GROUP BY w.id ORDER BY w.last_run_at DESC NULLS LAST
        LIMIT 5
    """, (since, user_id)).fetchall()
    return [dict(r) for r in rows]


def _summary_numbers(db, user_id: int, since: str) -> dict:
    """Top-level numbers for the summary cards."""
    total_leads = db.execute(
        "SELECT COUNT(*) FROM lead_core WHERE user_id=? AND merge_status!='merged'",
        (user_id,)
    ).fetchone()[0]

    hot_leads = db.execute("""
        SELECT COUNT(*) FROM lead_core lc
        JOIN lead_scores ls ON ls.lead_id = lc.id
        WHERE lc.user_id=? AND ls.tier='hot' AND lc.merge_status!='merged'
    """, (user_id,)).fetchone()[0]

    pipeline_total = db.execute(
        "SELECT COUNT(*) FROM pipeline_items WHERE user_id=? AND status='active'",
        (user_id,)
    ).fetchone()[0]

    campaigns_active = db.execute(
        "SELECT COUNT(*) FROM campaigns WHERE user_id=? AND status='active'",
        (user_id,)
    ).fetchone()[0]

    new_leads_period = db.execute(
        "SELECT COUNT(*) FROM lead_core WHERE user_id=? AND first_seen_at >= ?",
        (user_id, since)
    ).fetchone()[0]

    return {
        "total_leads":      total_leads,
        "hot_leads":        hot_leads,
        "pipeline_items":   pipeline_total,
        "active_campaigns": campaigns_active,
        "new_leads_period": new_leads_period,
    }
