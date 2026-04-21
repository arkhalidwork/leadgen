"""
LeadGen — Workflows Routes (Blueprint)

GET  /api/workflows           → list workflows
POST /api/workflows           → create workflow
GET  /api/workflows/<id>      → detail + run history
PATCH /api/workflows/<id>     → update (name/description/status)
DELETE /api/workflows/<id>    → archive
POST /api/workflows/<id>/trigger    → add trigger
DELETE /api/workflows/<id>/triggers/<tid>
POST /api/workflows/<id>/action     → add action
PATCH /api/workflows/<id>/actions/<aid>
DELETE /api/workflows/<id>/actions/<aid>
POST /api/workflows/<id>/run        → manual fire
GET  /api/workflows/<id>/runs       → run history
"""
from __future__ import annotations

import json

from flask import Blueprint, jsonify, request, session

workflows_bp = Blueprint("workflows", __name__, url_prefix="/api/workflows")


def _uid():
    uid = session.get("user_id")
    if not uid:
        return None, (jsonify({"error": "Auth required"}), 401)
    return int(uid), None


def _db():
    from workflows.engine import _db as _edb
    return _edb()


# ── Workflows CRUD ─────────────────────────────────────────────────────────

@workflows_bp.route("", methods=["GET"])
def list_workflows():
    uid, err = _uid()
    if err: return err
    db = _db()
    rows = db.execute("""
        SELECT w.*,
               COUNT(DISTINCT wt.id) AS trigger_count,
               COUNT(DISTINCT wa.id) AS action_count
        FROM workflows w
        LEFT JOIN workflow_triggers wt ON wt.workflow_id = w.id
        LEFT JOIN workflow_actions  wa ON wa.workflow_id = w.id
        WHERE w.user_id = ? AND w.status != 'archived'
        GROUP BY w.id ORDER BY w.created_at DESC
    """, (uid,)).fetchall()
    return jsonify({"workflows": [dict(r) for r in rows]})


@workflows_bp.route("", methods=["POST"])
def create_workflow():
    uid, err = _uid()
    if err: return err
    data = request.get_json(force=True) or {}
    name = str(data.get("name", "")).strip()
    if not name:
        return jsonify({"error": "name required"}), 400

    db = _db()
    wid = db.execute("""
        INSERT INTO workflows (user_id, name, description, status)
        VALUES (?, ?, ?, 'active')
    """, (uid, name, data.get("description", ""))).lastrowid
    db.commit()

    # Optionally accept trigger + actions in same call
    if "trigger" in data:
        _insert_trigger(db, wid, data["trigger"])
    for act in data.get("actions", []):
        _insert_action(db, wid, act)
    db.commit()

    return jsonify({"id": wid, "name": name}), 201


@workflows_bp.route("/<int:wid>", methods=["GET"])
def get_workflow(wid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    row = db.execute("SELECT * FROM workflows WHERE id=? AND user_id=?", (wid, uid)).fetchone()
    if not row: return jsonify({"error": "Not found"}), 404

    triggers = db.execute("SELECT * FROM workflow_triggers WHERE workflow_id=?", (wid,)).fetchall()
    actions  = db.execute(
        "SELECT * FROM workflow_actions WHERE workflow_id=? ORDER BY step_order", (wid,)
    ).fetchall()
    runs = db.execute("""
        SELECT id, status, actions_total, actions_done, error, started_at, finished_at
        FROM workflow_runs WHERE workflow_id=? ORDER BY started_at DESC LIMIT 10
    """, (wid,)).fetchall()

    return jsonify({
        **dict(row),
        "triggers": [dict(t) for t in triggers],
        "actions":  [dict(a) for a in actions],
        "recent_runs": [dict(r) for r in runs],
    })


@workflows_bp.route("/<int:wid>", methods=["PATCH"])
def update_workflow(wid: int):
    uid, err = _uid()
    if err: return err
    data    = request.get_json(force=True) or {}
    allowed = {"name", "description", "status"}
    safe    = {k: v for k, v in data.items() if k in allowed}
    if not safe: return jsonify({"ok": False}), 400

    db = _db()
    set_sql = ", ".join(f"{k}=?" for k in safe)
    db.execute(f"UPDATE workflows SET {set_sql} WHERE id=? AND user_id=?",
               list(safe.values()) + [wid, uid])
    db.commit()
    return jsonify({"ok": True})


@workflows_bp.route("/<int:wid>", methods=["DELETE"])
def delete_workflow(wid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    db.execute("UPDATE workflows SET status='archived' WHERE id=? AND user_id=?", (wid, uid))
    db.commit()
    return jsonify({"ok": True})


# ── Triggers ───────────────────────────────────────────────────────────────

@workflows_bp.route("/<int:wid>/trigger", methods=["POST"])
def add_trigger(wid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    if not db.execute("SELECT 1 FROM workflows WHERE id=? AND user_id=?", (wid, uid)).fetchone():
        return jsonify({"error": "Not found"}), 404
    data = request.get_json(force=True) or {}
    _insert_trigger(db, wid, data)
    db.commit()
    return jsonify({"ok": True}), 201


@workflows_bp.route("/<int:wid>/triggers/<int:tid>", methods=["DELETE"])
def delete_trigger(wid: int, tid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    db.execute("DELETE FROM workflow_triggers WHERE id=? AND workflow_id=?", (tid, wid))
    db.commit()
    return jsonify({"ok": True})


# ── Actions ────────────────────────────────────────────────────────────────

@workflows_bp.route("/<int:wid>/action", methods=["POST"])
def add_action(wid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    if not db.execute("SELECT 1 FROM workflows WHERE id=? AND user_id=?", (wid, uid)).fetchone():
        return jsonify({"error": "Not found"}), 404
    data = request.get_json(force=True) or {}
    _insert_action(db, wid, data)
    db.commit()
    return jsonify({"ok": True}), 201


@workflows_bp.route("/<int:wid>/actions/<int:aid>", methods=["PATCH"])
def update_action(wid: int, aid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    data    = request.get_json(force=True) or {}
    allowed = {"action_type", "config", "step_order"}
    safe    = {k: v for k, v in data.items() if k in allowed}
    if "config" in safe and isinstance(safe["config"], dict):
        safe["config"] = json.dumps(safe["config"])
    if not safe: return jsonify({"ok": False}), 400
    set_sql = ", ".join(f"{k}=?" for k in safe)
    db.execute(f"UPDATE workflow_actions SET {set_sql} WHERE id=? AND workflow_id=?",
               list(safe.values()) + [aid, wid])
    db.commit()
    return jsonify({"ok": True})


@workflows_bp.route("/<int:wid>/actions/<int:aid>", methods=["DELETE"])
def delete_action(wid: int, aid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    db.execute("DELETE FROM workflow_actions WHERE id=? AND workflow_id=?", (aid, wid))
    db.commit()
    return jsonify({"ok": True})


# ── Manual Run ─────────────────────────────────────────────────────────────

@workflows_bp.route("/<int:wid>/run", methods=["POST"])
def manual_run(wid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    row = db.execute("SELECT * FROM workflows WHERE id=? AND user_id=?", (wid, uid)).fetchone()
    if not row: return jsonify({"error": "Not found"}), 404

    import threading
    from workflows.engine import WorkflowEngine
    engine = WorkflowEngine()
    t = threading.Thread(
        target=engine._execute_workflow,
        args=(wid, uid, {"manual": True}, None),
        daemon=True,
    )
    t.start()
    return jsonify({"ok": True, "message": "Workflow triggered"})


@workflows_bp.route("/<int:wid>/runs", methods=["GET"])
def get_runs(wid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    if not db.execute("SELECT 1 FROM workflows WHERE id=? AND user_id=?", (wid, uid)).fetchone():
        return jsonify({"error": "Not found"}), 404
    limit = min(int(request.args.get("limit", 20)), 100)
    rows = db.execute("""
        SELECT wr.*, GROUP_CONCAT(wrl.status) as step_statuses
        FROM workflow_runs wr
        LEFT JOIN workflow_run_logs wrl ON wrl.run_id = wr.id
        WHERE wr.workflow_id=?
        GROUP BY wr.id
        ORDER BY wr.started_at DESC LIMIT ?
    """, (wid, limit)).fetchall()
    return jsonify({"runs": [dict(r) for r in rows]})


# ── Helpers ────────────────────────────────────────────────────────────────

def _insert_trigger(db, workflow_id: int, data: dict) -> None:
    from datetime import datetime, timezone, timedelta
    trigger_type = data.get("trigger_type", "event")
    config       = data.get("config", {})
    if isinstance(config, dict):
        config = json.dumps(config)

    next_run = None
    if trigger_type == "schedule":
        next_run = (datetime.now(timezone.utc) + timedelta(minutes=1)).isoformat()

    db.execute("""
        INSERT INTO workflow_triggers (workflow_id, trigger_type, config, next_run_at)
        VALUES (?, ?, ?, ?)
    """, (workflow_id, trigger_type, config, next_run))


def _insert_action(db, workflow_id: int, data: dict) -> None:
    max_order = db.execute(
        "SELECT COALESCE(MAX(step_order),0) FROM workflow_actions WHERE workflow_id=?",
        (workflow_id,)
    ).fetchone()[0]
    config = data.get("config", {})
    if isinstance(config, dict):
        config = json.dumps(config)
    db.execute("""
        INSERT INTO workflow_actions (workflow_id, step_order, action_type, config)
        VALUES (?, ?, ?, ?)
    """, (workflow_id, max_order + 1, data.get("action_type", ""), config))
