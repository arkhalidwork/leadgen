"""
LeadGen — Outreach Routes (Blueprint)

/api/campaigns/*    Campaign CRUD
/api/campaigns/<id>/sequences    Sequence step management
/api/campaigns/<id>/leads        Enrollment management
/api/campaigns/<id>/stats        Open/reply metrics
/api/outreach/smtp               SMTP config
/t/<token>.gif                   Open-tracking pixel (no auth)
"""
from __future__ import annotations

from flask import Blueprint, jsonify, make_response, request, session

outreach_bp = Blueprint("outreach", __name__)


def _uid():
    uid = session.get("user_id")
    if not uid:
        return None, (jsonify({"error": "Auth required"}), 401)
    return int(uid), None


def _db():
    from outreach.sender import _db as _sdb
    return _sdb()


# ── Campaigns CRUD ─────────────────────────────────────────────────────────

@outreach_bp.route("/api/campaigns", methods=["GET"])
def list_campaigns():
    uid, err = _uid()
    if err: return err
    db = _db()
    rows = db.execute(
        "SELECT * FROM campaigns WHERE user_id=? ORDER BY created_at DESC", (uid,)
    ).fetchall()
    return jsonify({"campaigns": [dict(r) for r in rows]})


@outreach_bp.route("/api/campaigns", methods=["POST"])
def create_campaign():
    uid, err = _uid()
    if err: return err
    data = request.get_json(force=True) or {}
    name = str(data.get("name", "")).strip()
    if not name:
        return jsonify({"error": "name required"}), 400

    db = _db()
    cid = db.execute("""
        INSERT INTO campaigns
            (user_id, name, description, from_email, from_name, reply_to,
             daily_send_limit, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'draft')
    """, (
        uid, name,
        data.get("description", ""),
        data.get("from_email", ""),
        data.get("from_name", ""),
        data.get("reply_to", ""),
        int(data.get("daily_send_limit", 50)),
    )).lastrowid
    db.commit()
    row = db.execute("SELECT * FROM campaigns WHERE id=?", (cid,)).fetchone()
    return jsonify(dict(row)), 201


@outreach_bp.route("/api/campaigns/<int:cid>", methods=["GET"])
def get_campaign(cid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    row = db.execute("SELECT * FROM campaigns WHERE id=? AND user_id=?", (cid, uid)).fetchone()
    if not row: return jsonify({"error": "Not found"}), 404

    seqs  = db.execute("SELECT * FROM campaign_sequences WHERE campaign_id=? ORDER BY step_number", (cid,)).fetchall()
    stats = db.execute("""
        SELECT
            COUNT(*)                              as total_enrolled,
            SUM(emails_sent)                      as total_sent,
            SUM(opens)                            as total_opens,
            SUM(replies)                          as total_replies,
            SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active,
            SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END) as completed
        FROM campaign_leads WHERE campaign_id=?
    """, (cid,)).fetchone()

    return jsonify({
        **dict(row),
        "sequences": [dict(s) for s in seqs],
        "stats": dict(stats) if stats else {},
    })


@outreach_bp.route("/api/campaigns/<int:cid>", methods=["PATCH"])
def update_campaign(cid: int):
    uid, err = _uid()
    if err: return err
    data = request.get_json(force=True) or {}
    allowed = {"name", "description", "status", "from_email", "from_name",
               "reply_to", "daily_send_limit"}
    safe = {k: v for k, v in data.items() if k in allowed}
    if not safe:
        return jsonify({"ok": False, "error": "No valid fields"}), 400

    db = _db()
    set_sql = ", ".join(f"{k}=?" for k in safe) + ", updated_at=datetime('now')"
    db.execute(
        f"UPDATE campaigns SET {set_sql} WHERE id=? AND user_id=?",
        list(safe.values()) + [cid, uid]
    )
    db.commit()
    return jsonify({"ok": True})


@outreach_bp.route("/api/campaigns/<int:cid>", methods=["DELETE"])
def delete_campaign(cid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    db.execute("UPDATE campaigns SET status='archived' WHERE id=? AND user_id=?", (cid, uid))
    db.commit()
    return jsonify({"ok": True})


# ── Sequences ──────────────────────────────────────────────────────────────

@outreach_bp.route("/api/campaigns/<int:cid>/sequences", methods=["GET"])
def list_sequences(cid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    if not db.execute("SELECT 1 FROM campaigns WHERE id=? AND user_id=?", (cid, uid)).fetchone():
        return jsonify({"error": "Not found"}), 404
    rows = db.execute(
        "SELECT * FROM campaign_sequences WHERE campaign_id=? ORDER BY step_number", (cid,)
    ).fetchall()
    return jsonify({"sequences": [dict(r) for r in rows]})


@outreach_bp.route("/api/campaigns/<int:cid>/sequences", methods=["POST"])
def add_sequence_step(cid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    camp = db.execute("SELECT id FROM campaigns WHERE id=? AND user_id=?", (cid, uid)).fetchone()
    if not camp: return jsonify({"error": "Not found"}), 404

    data = request.get_json(force=True) or {}
    max_step = db.execute(
        "SELECT COALESCE(MAX(step_number), 0) FROM campaign_sequences WHERE campaign_id=?", (cid,)
    ).fetchone()[0]
    sid = db.execute("""
        INSERT INTO campaign_sequences
            (campaign_id, step_number, delay_days, subject, body_html, body_text,
             is_ai, tone, skip_if_replied)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        cid, max_step + 1,
        int(data.get("delay_days", 0)),
        data.get("subject", ""),
        data.get("body_html", ""),
        data.get("body_text", ""),
        int(data.get("is_ai", 0)),
        data.get("tone", "professional"),
        int(data.get("skip_if_replied", 1)),
    )).lastrowid
    db.commit()
    row = db.execute("SELECT * FROM campaign_sequences WHERE id=?", (sid,)).fetchone()
    return jsonify(dict(row)), 201


@outreach_bp.route("/api/campaigns/<int:cid>/sequences/<int:sid>", methods=["PATCH"])
def update_sequence_step(cid: int, sid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    if not db.execute("SELECT 1 FROM campaigns WHERE id=? AND user_id=?", (cid, uid)).fetchone():
        return jsonify({"error": "Not found"}), 404

    data    = request.get_json(force=True) or {}
    allowed = {"delay_days", "subject", "body_html", "body_text",
               "is_ai", "tone", "skip_if_replied", "skip_if_opened"}
    safe    = {k: v for k, v in data.items() if k in allowed}
    if not safe:
        return jsonify({"ok": False}), 400

    set_sql = ", ".join(f"{k}=?" for k in safe)
    db.execute(
        f"UPDATE campaign_sequences SET {set_sql} WHERE id=? AND campaign_id=?",
        list(safe.values()) + [sid, cid]
    )
    db.commit()
    return jsonify({"ok": True})


@outreach_bp.route("/api/campaigns/<int:cid>/sequences/<int:sid>", methods=["DELETE"])
def delete_sequence_step(cid: int, sid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    if not db.execute("SELECT 1 FROM campaigns WHERE id=? AND user_id=?", (cid, uid)).fetchone():
        return jsonify({"error": "Not found"}), 404
    db.execute("DELETE FROM campaign_sequences WHERE id=? AND campaign_id=?", (sid, cid))
    db.commit()
    return jsonify({"ok": True})


# ── Enrollment ─────────────────────────────────────────────────────────────

@outreach_bp.route("/api/campaigns/<int:cid>/leads", methods=["POST"])
def enroll_leads(cid: int):
    uid, err = _uid()
    if err: return err
    data     = request.get_json(force=True) or {}
    lead_ids = data.get("lead_ids", [])
    if not lead_ids:
        return jsonify({"error": "lead_ids required"}), 400

    from outreach.sender import bulk_enroll
    try:
        result = bulk_enroll(cid, [int(i) for i in lead_ids], uid)
        return jsonify(result), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 404


@outreach_bp.route("/api/campaigns/<int:cid>/leads", methods=["GET"])
def list_campaign_leads(cid: int):
    uid, err = _uid()
    if err: return err
    db = _db()
    if not db.execute("SELECT 1 FROM campaigns WHERE id=? AND user_id=?", (cid, uid)).fetchone():
        return jsonify({"error": "Not found"}), 404
    rows = db.execute("""
        SELECT cl.*, lc.canonical_name, lc.category, lc.city,
               le.email
        FROM campaign_leads cl
        JOIN lead_core lc ON lc.id = cl.lead_id
        LEFT JOIN lead_enrichment le ON le.lead_id = cl.lead_id
        WHERE cl.campaign_id = ?
        ORDER BY cl.enrolled_at DESC
    """, (cid,)).fetchall()
    return jsonify({"leads": [dict(r) for r in rows]})


# ── SMTP Config ─────────────────────────────────────────────────────────────

@outreach_bp.route("/api/outreach/smtp", methods=["GET"])
def get_smtp():
    uid, err = _uid()
    if err: return err
    from outreach.smtp import get_smtp_config
    conf = get_smtp_config(uid) or {}
    # Redact password
    if "smtp_pass" in conf and conf["smtp_pass"]:
        conf["smtp_pass"] = "••••••••"
    if "api_key" in conf and conf["api_key"]:
        conf["api_key"] = "••••••••"
    return jsonify(conf)


@outreach_bp.route("/api/outreach/smtp", methods=["POST"])
def save_smtp():
    uid, err = _uid()
    if err: return err
    data = request.get_json(force=True) or {}
    from outreach.smtp import save_smtp_config
    save_smtp_config(uid, data)
    return jsonify({"ok": True})


@outreach_bp.route("/api/outreach/smtp/verify", methods=["POST"])
def verify_smtp():
    uid, err = _uid()
    if err: return err
    from outreach.smtp import verify_smtp as _verify
    ok, msg = _verify(uid)
    return jsonify({"ok": ok, "message": msg})


# ── Open-tracking pixel (no auth) ──────────────────────────────────────────

@outreach_bp.route("/t/<token>.gif", methods=["GET"])
def tracking_pixel(token: str):
    from outreach.smtp import decode_tracking_token, TRANSPARENT_GIF
    from outreach.sender import record_open_event
    data = decode_tracking_token(token)
    if data:
        try:
            record_open_event(data["campaign_lead_id"], data["step"])
        except Exception:
            pass
    resp = make_response(TRANSPARENT_GIF)
    resp.headers["Content-Type"]  = "image/gif"
    resp.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    return resp


# ── Manual send trigger ────────────────────────────────────────────────────

@outreach_bp.route("/api/outreach/process-queue", methods=["POST"])
def trigger_queue():
    uid, err = _uid()
    if err: return err
    from outreach.sender import process_campaign_queue
    import threading
    t = threading.Thread(target=process_campaign_queue, daemon=True)
    t.start()
    return jsonify({"ok": True, "message": "Queue processing started"})
