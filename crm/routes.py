"""
LeadGen — CRM Pipeline Routes (Blueprint)

GET  /api/pipeline/board           → Kanban board (all stages + items)
GET  /api/pipeline/stages          → stage list
POST /api/pipeline/stages          → create stage
GET  /api/pipeline/items           → filtered item list
POST /api/pipeline/items           → add lead(s) to pipeline
PATCH /api/pipeline/items/<id>     → move stage / update deal / add note
DELETE /api/pipeline/items/<id>    → archive item
GET  /api/pipeline/items/<id>/activity → timeline
POST /api/pipeline/items/<id>/activity → add note
"""
from __future__ import annotations

from flask import Blueprint, jsonify, request, session

crm_bp = Blueprint("crm", __name__, url_prefix="/api/pipeline")


def _uid():
    uid = session.get("user_id")
    if not uid:
        return None, (jsonify({"error": "Auth required"}), 401)
    return int(uid), None


# ── Board ──────────────────────────────────────────────────────────────────

@crm_bp.route("/board", methods=["GET"])
def get_board():
    uid, err = _uid()
    if err: return err
    from crm.service import get_board
    return jsonify(get_board(uid))


# ── Stages ─────────────────────────────────────────────────────────────────

@crm_bp.route("/stages", methods=["GET"])
def list_stages():
    uid, err = _uid()
    if err: return err
    from crm.service import get_stages, seed_default_stages
    stages = get_stages(uid)
    if not stages:
        stages = seed_default_stages(uid)
    return jsonify({"stages": stages})


@crm_bp.route("/stages", methods=["POST"])
def create_stage():
    uid, err = _uid()
    if err: return err
    data = request.get_json(force=True) or {}
    name = str(data.get("name", "")).strip()
    if not name:
        return jsonify({"error": "name required"}), 400

    from crm.service import _db
    db = _db()
    max_pos = db.execute(
        "SELECT COALESCE(MAX(position), -1) FROM pipeline_stages WHERE user_id=?", (uid,)
    ).fetchone()[0]
    db.execute("""
        INSERT INTO pipeline_stages (user_id, name, color, icon, position)
        VALUES (?, ?, ?, ?, ?)
    """, (uid, name, data.get("color", "#6366f1"),
          data.get("icon", "bi-circle"), max_pos + 1))
    db.commit()
    row = db.execute(
        "SELECT * FROM pipeline_stages WHERE user_id=? ORDER BY id DESC LIMIT 1", (uid,)
    ).fetchone()
    return jsonify(dict(row)), 201


# ── Items ──────────────────────────────────────────────────────────────────

@crm_bp.route("/items", methods=["POST"])
def add_items():
    uid, err = _uid()
    if err: return err
    data     = request.get_json(force=True) or {}
    lead_ids = data.get("lead_ids", [])
    stage_id = data.get("stage_id")
    source   = data.get("source", "manual")

    if not lead_ids or not stage_id:
        return jsonify({"error": "lead_ids and stage_id required"}), 400

    from crm.service import bulk_add_leads, seed_default_stages, get_stages
    # Ensure user has stages
    if not get_stages(uid):
        seed_default_stages(uid)

    result = bulk_add_leads([int(i) for i in lead_ids], int(stage_id), uid, source)
    return jsonify(result), 201


@crm_bp.route("/items/<int:item_id>", methods=["PATCH"])
def update_item(item_id: int):
    uid, err = _uid()
    if err: return err
    data = request.get_json(force=True) or {}

    # Stage move
    if "stage_id" in data:
        from crm.service import move_item
        ok = move_item(item_id, int(data["stage_id"]), uid)
        return jsonify({"ok": ok})

    # Field updates
    from crm.service import update_item as _upd
    ok = _upd(item_id, uid, data)
    return jsonify({"ok": ok})


@crm_bp.route("/items/<int:item_id>", methods=["DELETE"])
def archive_item(item_id: int):
    uid, err = _uid()
    if err: return err
    from crm.service import archive_item as _arch
    ok = _arch(item_id, uid)
    return jsonify({"ok": ok})


@crm_bp.route("/items/<int:item_id>/activity", methods=["GET"])
def get_activity(item_id: int):
    uid, err = _uid()
    if err: return err
    limit = int(request.args.get("limit", 50))
    from crm.service import get_activity
    return jsonify({"activity": get_activity(item_id, uid, limit)})


@crm_bp.route("/items/<int:item_id>/activity", methods=["POST"])
def add_note(item_id: int):
    uid, err = _uid()
    if err: return err
    data = request.get_json(force=True) or {}
    body = str(data.get("body", "")).strip()
    if not body:
        return jsonify({"error": "body required"}), 400
    from crm.service import add_note
    try:
        note = add_note(item_id, uid, body)
        return jsonify(note), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404


# ── Bulk add shortcut from Intelligence page ───────────────────────────────

@crm_bp.route("/bulk-add", methods=["POST"])
def bulk_add():
    uid, err = _uid()
    if err: return err
    data     = request.get_json(force=True) or {}
    lead_ids = data.get("lead_ids", [])
    tier     = data.get("tier", "")        # optional: add all leads of tier
    stage_id = data.get("stage_id")

    if not stage_id:
        return jsonify({"error": "stage_id required"}), 400

    # If tier filter provided, load lead_ids automatically
    if tier and not lead_ids:
        from crm.service import _db
        db = _db()
        rows = db.execute("""
            SELECT lc.id FROM lead_core lc
            JOIN lead_scores ls ON ls.lead_id = lc.id
            WHERE lc.user_id = ? AND ls.tier = ?
              AND lc.merge_status != 'merged'
        """, (uid, tier)).fetchall()
        lead_ids = [r[0] for r in rows]

    if not lead_ids:
        return jsonify({"added": 0, "skipped": 0, "stage_id": stage_id})

    from crm.service import bulk_add_leads, seed_default_stages, get_stages
    if not get_stages(uid):
        seed_default_stages(uid)

    result = bulk_add_leads([int(i) for i in lead_ids], int(stage_id), uid, "intelligence")
    return jsonify(result), 201
