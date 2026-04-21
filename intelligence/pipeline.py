"""
LeadGen — Intelligence: Pipeline Orchestrator

Stages 2–5: Normalize → Deduplicate → Enrich/Signal → Score → Insight → Store
Triggered as a daemon thread after a job reaches 'completed' status.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local   = threading.local()


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
        _local.db.execute("PRAGMA busy_timeout=5000")
    return _local.db


# ── Entry Point ────────────────────────────────────────────────────────────

def run_pipeline(job_id: str, job_type: str, user_id: int) -> int:
    """
    Run the full intelligence pipeline for all leads from a completed job.
    Returns number of lead entities processed.
    """
    log.info(f"Intelligence pipeline starting — job={job_id} type={job_type} user={user_id}")

    try:
        # Load job result JSON
        raw_leads = _load_job_leads(job_id)
    except Exception as exc:
        log.error(f"Pipeline: failed to load leads for job {job_id}: {exc}")
        return 0

    if not raw_leads:
        log.info(f"Pipeline: no leads in job {job_id}, skipping")
        return 0

    processed = 0
    errors    = 0

    for raw in raw_leads:
        try:
            lead_id = _process_single(raw, job_id, job_type, user_id)
            _insert_source_record(lead_id, job_id, job_type, user_id, raw)
            _rescore_lead(lead_id)
            processed += 1
        except Exception as exc:
            errors += 1
            log.warning(f"Pipeline: error on lead '{raw.get('business_name', '?')}': {exc}")

    log.info(f"Intelligence pipeline done — job={job_id}: {processed} processed, {errors} errors")
    _invalidate_cache(user_id)
    return processed


def _process_single(raw: dict, job_id: str, job_type: str, user_id: int) -> int:
    """Normalize → Resolve → Signal → returns lead_id."""
    from intelligence.normalizer import normalize_record
    from intelligence.resolver  import resolve_entity
    from intelligence.signals   import extract_signals, add_no_website_signal

    # Stage 2: Normalize
    record = normalize_record(raw)

    # Stage 3: Entity resolution (deduplicate)
    lead_id = resolve_entity(record, user_id)

    # Stage 4a: Extract + upsert signals
    signals = extract_signals(lead_id, job_type, raw)

    # Cross-source signal: no website at all?
    enrichment = _get_enrichment(lead_id)
    if not enrichment or not enrichment.get("website"):
        signals.append(add_no_website_signal(lead_id))

    _upsert_signals(lead_id, signals)

    return lead_id


def _rescore_lead(lead_id: int) -> None:
    """Stage 4b+5: Recompute score + insights and save."""
    from intelligence.scorer  import compute_score, upsert_score
    from intelligence.insights import generate_insights, upsert_insights

    db = _db()
    core_row = db.execute(
        "SELECT * FROM lead_core WHERE id = ?", (lead_id,)
    ).fetchone()
    if not core_row:
        return

    core        = dict(core_row)
    enrichment  = _get_enrichment(lead_id) or {}
    signals     = _get_signals(lead_id)
    last_seen   = core.get("last_seen_at")

    # Score
    score_dict = compute_score(lead_id, enrichment, signals, last_seen)
    upsert_score(score_dict)

    # Insights
    insight_dict = generate_insights(lead_id, core, enrichment, signals, score_dict)
    upsert_insights(insight_dict)

    # Phase 5: Emit event for workflow engine
    try:
        from workflows.engine import emit_event
        emit_event("lead_scored", {
            "user_id":  core.get("user_id"),
            "lead_id":  lead_id,
            "tier":     score_dict.get("tier", "cold"),
            "score":    score_dict.get("total_score", 0),
            "category": core.get("category", ""),
        })
    except Exception:
        pass


# ── Source record ──────────────────────────────────────────────────────────

def _insert_source_record(lead_id: int, job_id: str, job_type: str,
                           user_id: int, raw: dict) -> None:
    db = _db()
    try:
        db.execute("""
            INSERT OR IGNORE INTO lead_sources (
                lead_id, user_id, job_id, source,
                raw_name, raw_phone, raw_website, raw_email,
                raw_address, raw_rating, raw_reviews, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            lead_id, int(user_id), job_id, job_type,
            str(raw.get("business_name") or raw.get("name") or "")[:500],
            str(raw.get("phone") or "")[:50],
            str(raw.get("website") or "")[:500],
            str(raw.get("email") or "")[:300],
            str(raw.get("address") or "")[:500],
            float(raw.get("rating") or 0) or None,
            _safe_int(raw.get("reviews")),
            json.dumps({k: v for k, v in raw.items()
                        if k not in ("raw",)}, default=str)[:8000],
        ))
        db.commit()
    except Exception as exc:
        log.debug(f"Source record insert: {exc}")


# ── DB helpers ─────────────────────────────────────────────────────────────

def _load_job_leads(job_id: str) -> list[dict]:
    db = _db()
    row = db.execute(
        "SELECT result FROM jobs WHERE job_id = ?", (job_id,)
    ).fetchone()
    if not row or not row[0]:
        return []
    try:
        data = json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return []

    # Support both {"leads": [...]} and [...] formats
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        return (data.get("leads") or data.get("results")
                or data.get("businesses") or [])
    return []


def _get_enrichment(lead_id: int) -> dict | None:
    db = _db()
    row = db.execute(
        "SELECT * FROM lead_enrichment WHERE lead_id = ?", (lead_id,)
    ).fetchone()
    return dict(row) if row else None


def _get_signals(lead_id: int) -> list[dict]:
    db = _db()
    rows = db.execute(
        "SELECT * FROM lead_signals WHERE lead_id = ? ORDER BY confidence DESC",
        (lead_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _upsert_signals(lead_id: int, signals: list[dict]) -> None:
    if not signals:
        return
    db = _db()
    for sig in signals:
        try:
            db.execute("""
                INSERT INTO lead_signals
                    (lead_id, signal_type, confidence, value, source, detected_at)
                VALUES (?, ?, ?, ?, ?, datetime('now'))
                ON CONFLICT(lead_id, signal_type, source) DO UPDATE SET
                    confidence  = MAX(excluded.confidence, lead_signals.confidence),
                    value       = excluded.value,
                    detected_at = datetime('now')
            """, (
                lead_id,
                sig["signal_type"],
                float(sig.get("confidence", 1.0)),
                str(sig.get("value", ""))[:500],
                sig.get("source", ""),
            ))
        except Exception as exc:
            log.debug(f"Signal upsert: {exc}")
    db.commit()


def _invalidate_cache(user_id: int) -> None:
    """Invalidate Redis cache for user's intelligence leads list."""
    try:
        import redis as _redis
        r = _redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379"))
        keys = r.keys(f"intel:leads:{user_id}:*")
        if keys:
            r.delete(*keys)
    except Exception:
        pass  # Redis optional — cache miss is acceptable


def _safe_int(val) -> int | None:
    try:
        return int(str(val).replace(",", "")) if val else None
    except (ValueError, TypeError):
        return None


# ── Public trigger helper ───────────────────────────────────────────────────

def trigger_pipeline_async(job_id: str, job_type: str, user_id: int) -> None:
    """
    Launch pipeline in a daemon thread (non-blocking).
    Called from worker/agent completion hooks.
    """
    t = threading.Thread(
        target=_safe_run,
        args=(job_id, job_type, user_id),
        daemon=True,
        name=f"intel-{job_id[:8]}",
    )
    t.start()


def _safe_run(job_id: str, job_type: str, user_id: int) -> None:
    try:
        run_pipeline(job_id, job_type, user_id)
    except Exception as exc:
        log.error(f"Intelligence pipeline crashed: {exc}", exc_info=True)
