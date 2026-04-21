"""
LeadGen — CRM: Pipeline Service

Core logic: seed stages, add leads, move stages, log activity, emit events.
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
_local = threading.local()

DEFAULT_STAGES = [
    {"name": "New Leads",  "color": "#6366f1", "icon": "bi-star",          "position": 0, "is_terminal": 0, "is_winning": 0},
    {"name": "Contacted",  "color": "#3B82F6", "icon": "bi-send",          "position": 1, "is_terminal": 0, "is_winning": 0},
    {"name": "Replied",    "color": "#10B981", "icon": "bi-chat-dots",     "position": 2, "is_terminal": 0, "is_winning": 0},
    {"name": "Qualified",  "color": "#F59E0B", "icon": "bi-check-circle",  "position": 3, "is_terminal": 0, "is_winning": 0},
    {"name": "Converted",  "color": "#22C55E", "icon": "bi-trophy",        "position": 4, "is_terminal": 1, "is_winning": 1},
    {"name": "Lost",       "color": "#6B7280", "icon": "bi-x-circle",      "position": 5, "is_terminal": 1, "is_winning": 0},
]


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
        _local.db.execute("PRAGMA busy_timeout=5000")
        _local.db.execute("PRAGMA foreign_keys=ON")
    return _local.db


# ── Stage helpers ──────────────────────────────────────────────────────────

def seed_default_stages(user_id: int) -> list[dict]:
    """Create the 6 default pipeline stages for a new user (idempotent)."""
    db = _db()
    existing = db.execute(
        "SELECT COUNT(*) FROM pipeline_stages WHERE user_id = ?", (user_id,)
    ).fetchone()[0]
    if existing:
        return get_stages(user_id)

    for s in DEFAULT_STAGES:
        db.execute("""
            INSERT INTO pipeline_stages (user_id, name, color, icon, position, is_terminal, is_winning)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (user_id, s["name"], s["color"], s["icon"],
              s["position"], s["is_terminal"], s["is_winning"]))
    db.commit()
    log.info(f"Seeded default pipeline stages for user {user_id}")
    return get_stages(user_id)


def get_stages(user_id: int) -> list[dict]:
    db = _db()
    rows = db.execute(
        "SELECT * FROM pipeline_stages WHERE user_id = ? ORDER BY position", (user_id,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_stage_by_name(user_id: int, name: str) -> dict | None:
    db = _db()
    row = db.execute(
        "SELECT * FROM pipeline_stages WHERE user_id = ? AND name = ?",
        (user_id, name)
    ).fetchone()
    return dict(row) if row else None


# ── Add leads ──────────────────────────────────────────────────────────────

def bulk_add_leads(lead_ids: list[int], stage_id: int, user_id: int,
                   source: str = "manual") -> dict:
    """Add multiple leads to a pipeline stage. Skips duplicates."""
    db = _db()
    added, skipped = 0, 0

    for lead_id in lead_ids:
        # Pull enrichment + score snapshot
        enr = db.execute(
            "SELECT email, phone FROM lead_enrichment WHERE lead_id = ?", (lead_id,)
        ).fetchone()
        sc = db.execute(
            "SELECT total_score, tier FROM lead_scores WHERE lead_id = ?", (lead_id,)
        ).fetchone()
        core = db.execute(
            "SELECT canonical_name FROM lead_core WHERE id = ? AND user_id = ?",
            (lead_id, user_id)
        ).fetchone()
        if not core:
            skipped += 1
            continue

        try:
            # Get next position for this stage
            max_pos = db.execute(
                "SELECT COALESCE(MAX(position_order), 0) FROM pipeline_items WHERE stage_id = ?",
                (stage_id,)
            ).fetchone()[0]

            db.execute("""
                INSERT OR IGNORE INTO pipeline_items
                    (user_id, lead_id, stage_id, lead_name, lead_email,
                     lead_phone, lead_score, lead_tier, source, position_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, lead_id, stage_id,
                core["canonical_name"],
                enr["email"] if enr else "",
                enr["phone"] if enr else "",
                sc["total_score"] if sc else 0,
                sc["tier"] if sc else "cold",
                source,
                float(max_pos) + 1.0,
            ))
            if db.execute("SELECT changes()").fetchone()[0]:
                _log_activity(
                    user_id=user_id, lead_id=lead_id, pipeline_item_id=None,
                    atype="system", title="Added to pipeline",
                    meta={"stage_id": stage_id, "source": source},
                )
                added += 1
            else:
                skipped += 1
        except Exception as exc:
            log.debug(f"bulk_add_leads skip lead {lead_id}: {exc}")
            skipped += 1

    db.commit()
    _emit("leads_added_to_pipeline", {
        "user_id": user_id, "stage_id": stage_id, "added": added,
    })
    return {"added": added, "skipped": skipped, "stage_id": stage_id}


def add_single_lead(lead_id: int, stage_id: int, user_id: int,
                    source: str = "manual") -> dict | None:
    result = bulk_add_leads([lead_id], stage_id, user_id, source)
    if result["added"]:
        db = _db()
        row = db.execute(
            "SELECT * FROM pipeline_items WHERE user_id=? AND lead_id=?",
            (user_id, lead_id)
        ).fetchone()
        return dict(row) if row else None
    return None


# ── Move item ──────────────────────────────────────────────────────────────

def move_item(item_id: int, new_stage_id: int, user_id: int) -> bool:
    """Move a pipeline item to a new stage — logs activity + emits event."""
    db = _db()
    item = db.execute(
        "SELECT * FROM pipeline_items WHERE id = ? AND user_id = ?",
        (item_id, user_id)
    ).fetchone()
    if not item:
        return False

    old_stage_id = item["stage_id"]
    if old_stage_id == new_stage_id:
        return True

    # Check if new stage is a winning terminal
    new_stage = db.execute(
        "SELECT is_terminal, is_winning FROM pipeline_stages WHERE id = ?",
        (new_stage_id,)
    ).fetchone()

    updates = {
        "stage_id": new_stage_id,
        "last_activity_at": datetime.now(timezone.utc).isoformat(),
    }
    if new_stage and new_stage["is_winning"]:
        updates["converted_at"] = datetime.now(timezone.utc).isoformat()

    set_sql = ", ".join(f"{k} = ?" for k in updates)
    db.execute(
        f"UPDATE pipeline_items SET {set_sql} WHERE id = ? AND user_id = ?",
        list(updates.values()) + [item_id, user_id]
    )

    _log_activity(
        user_id=user_id,
        lead_id=item["lead_id"],
        pipeline_item_id=item_id,
        atype="stage_move",
        title="Moved to new stage",
        meta={"from_stage_id": old_stage_id, "to_stage_id": new_stage_id},
    )
    db.commit()

    _emit("pipeline_moved", {
        "user_id": user_id,
        "item_id": item_id,
        "lead_id": item["lead_id"],
        "from_stage_id": old_stage_id,
        "to_stage_id": new_stage_id,
    })
    return True


# ── Update item ────────────────────────────────────────────────────────────

def update_item(item_id: int, user_id: int, updates: dict) -> bool:
    db = _db()
    allowed = {"deal_value", "priority", "status", "lead_email", "lead_phone"}
    safe = {k: v for k, v in updates.items() if k in allowed}
    if not safe:
        return False
    safe["last_activity_at"] = datetime.now(timezone.utc).isoformat()
    set_sql = ", ".join(f"{k} = ?" for k in safe)
    db.execute(
        f"UPDATE pipeline_items SET {set_sql} WHERE id = ? AND user_id = ?",
        list(safe.values()) + [item_id, user_id]
    )
    db.commit()
    return True


# ── Notes / Activity ───────────────────────────────────────────────────────

def add_note(item_id: int, user_id: int, body: str) -> dict:
    db = _db()
    item = db.execute(
        "SELECT lead_id FROM pipeline_items WHERE id = ? AND user_id = ?",
        (item_id, user_id)
    ).fetchone()
    if not item:
        raise ValueError("Item not found")

    row_id = _log_activity(
        user_id=user_id,
        lead_id=item["lead_id"],
        pipeline_item_id=item_id,
        atype="note",
        title="Note added",
        body=body,
    )
    db.execute(
        "UPDATE pipeline_items SET last_activity_at = datetime('now') WHERE id = ?",
        (item_id,)
    )
    db.commit()
    return {"id": row_id, "body": body, "created_at": datetime.now(timezone.utc).isoformat()}


def get_activity(item_id: int, user_id: int, limit: int = 50) -> list[dict]:
    db = _db()
    rows = db.execute("""
        SELECT * FROM activity_log
        WHERE pipeline_item_id = ? AND user_id = ?
        ORDER BY created_at DESC LIMIT ?
    """, (item_id, user_id, limit)).fetchall()
    return [dict(r) for r in rows]


# ── Board query ────────────────────────────────────────────────────────────

def get_board(user_id: int) -> dict:
    """Return all stages + items for Kanban board render."""
    db = _db()

    # Ensure user has stages
    stages = get_stages(user_id)
    if not stages:
        stages = seed_default_stages(user_id)

    stage_map = {s["id"]: {**s, "items": []} for s in stages}

    items = db.execute("""
        SELECT pi.*, ls.total_score AS current_score, ls.tier AS current_tier
        FROM pipeline_items pi
        LEFT JOIN lead_scores ls ON ls.lead_id = pi.lead_id
        WHERE pi.user_id = ? AND pi.status = 'active'
        ORDER BY pi.position_order ASC
    """, (user_id,)).fetchall()

    for item in items:
        sid = item["stage_id"]
        if sid in stage_map:
            stage_map[sid]["items"].append(dict(item))

    return {
        "stages": [stage_map[s["id"]] for s in stages],
        "total_items": len(items),
    }


def archive_item(item_id: int, user_id: int) -> bool:
    db = _db()
    db.execute(
        "UPDATE pipeline_items SET status='archived' WHERE id=? AND user_id=?",
        (item_id, user_id)
    )
    db.commit()
    return db.execute("SELECT changes()").fetchone()[0] > 0


# ── Auto-advance pipeline from outreach ───────────────────────────────────

def auto_advance_to_stage_name(lead_id: int, user_id: int, target_name: str) -> bool:
    """Move lead to named stage IF it's at an earlier position (no regression)."""
    db = _db()
    item = db.execute(
        "SELECT id, stage_id FROM pipeline_items WHERE lead_id=? AND user_id=? AND status='active'",
        (lead_id, user_id)
    ).fetchone()
    if not item:
        return False

    cur_stage = db.execute(
        "SELECT position FROM pipeline_stages WHERE id=?", (item["stage_id"],)
    ).fetchone()
    tgt_stage = db.execute(
        "SELECT id, position FROM pipeline_stages WHERE user_id=? AND name=?",
        (user_id, target_name)
    ).fetchone()

    if not cur_stage or not tgt_stage:
        return False
    if tgt_stage["position"] <= cur_stage["position"]:
        return False  # Never move backwards

    return move_item(item["id"], tgt_stage["id"], user_id)


# ── Internal helpers ───────────────────────────────────────────────────────

def _log_activity(*, user_id: int, lead_id: int | None,
                  pipeline_item_id: int | None, atype: str,
                  title: str, body: str = "", meta: dict | None = None) -> int:
    db = _db()
    cursor = db.execute("""
        INSERT INTO activity_log
            (user_id, pipeline_item_id, lead_id, activity_type, title, body, meta)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, pipeline_item_id, lead_id, atype, title, body,
          json.dumps(meta or {}, default=str)))
    return cursor.lastrowid


def _emit(event_type: str, payload: dict) -> None:
    try:
        db = _db()
        db.execute("""
            INSERT INTO workflow_event_queue (user_id, event_type, payload)
            VALUES (?, ?, ?)
        """, (payload.get("user_id"), event_type, json.dumps(payload, default=str)))
        db.commit()
    except Exception as exc:
        log.debug(f"Event emit {event_type}: {exc}")
