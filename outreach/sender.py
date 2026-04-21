"""
LeadGen — Outreach: Campaign Sender

Called every 5 minutes by APScheduler.
Processes due campaign_leads steps, respects daily limits, advances state.
"""
from __future__ import annotations

import json
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
        _local.db.execute("PRAGMA busy_timeout=5000")
    return _local.db


# ── Main entry ─────────────────────────────────────────────────────────────

def process_campaign_queue() -> int:
    """
    Find all due campaign_lead steps and send them.
    Returns number of emails sent this cycle.
    """
    db   = _db()
    now  = datetime.now(timezone.utc).isoformat()
    sent = 0
    daily_counts: dict[int, int] = {}  # user_id → sent today

    try:
        due_rows = db.execute("""
            SELECT cl.id, cl.campaign_id, cl.lead_id, cl.current_step,
                   cl.pipeline_item_id, cl.emails_sent,
                   c.from_email, c.from_name, c.reply_to,
                   c.daily_send_limit, c.user_id, c.status AS campaign_status
            FROM campaign_leads cl
            JOIN campaigns c ON c.id = cl.campaign_id
            WHERE cl.status = 'active'
              AND cl.next_send_at IS NOT NULL
              AND cl.next_send_at <= ?
              AND c.status = 'active'
            ORDER BY cl.next_send_at ASC
            LIMIT 500
        """, (now,)).fetchall()

    except Exception as exc:
        log.error(f"Campaign queue fetch error: {exc}")
        return 0

    for row in due_rows:
        user_id = row["user_id"]
        limit   = row["daily_send_limit"] or 50

        # Enforce daily limit per user
        if user_id not in daily_counts:
            daily_counts[user_id] = _count_sent_today(user_id)
        if daily_counts[user_id] >= limit:
            continue

        ok = _send_next_step(dict(row))
        if ok:
            daily_counts[user_id] = daily_counts.get(user_id, 0) + 1
            sent += 1

    log.info(f"Campaign queue processed: {sent} emails sent")
    return sent


# ── Send single step ───────────────────────────────────────────────────────

def _send_next_step(enrollment: dict) -> bool:
    from outreach.personalizer import token_replace, ai_personalize
    from outreach.smtp import send_email, get_smtp_config, make_tracking_token

    db = _db()
    step_num  = enrollment["current_step"] + 1
    campaign_id = enrollment["campaign_id"]
    lead_id     = enrollment["lead_id"]
    enroll_id   = enrollment["id"]
    user_id     = enrollment["user_id"]

    # Load sequence step
    seq = db.execute("""
        SELECT * FROM campaign_sequences
        WHERE campaign_id = ? AND step_number = ?
    """, (campaign_id, step_num)).fetchone()

    if not seq:
        # No more steps — mark completed
        _complete_enrollment(enroll_id)
        return False

    # Skip conditions
    if seq["skip_if_replied"] and _has_replied(enroll_id):
        _complete_enrollment(enroll_id)
        return False
    if seq["skip_if_opened"] and _has_opened(enroll_id):
        _complete_enrollment(enroll_id)
        return False

    # Load lead context
    context = _load_lead_context(lead_id)
    to_email = context.get("email") or ""
    if not to_email:
        log.debug(f"Enrollment {enroll_id}: no email address, skipping")
        db.execute("UPDATE campaign_leads SET status='paused' WHERE id=?", (enroll_id,))
        db.commit()
        return False

    # Personalize
    try:
        if seq["is_ai"]:
            subject, body_html = ai_personalize(dict(seq), context)
        else:
            subject, body_html = token_replace(seq["subject"], seq["body_html"], context)
        body_text = _html_to_text(body_html)
    except Exception as exc:
        log.error(f"Personalization error enrollment {enroll_id}: {exc}")
        return False

    # Get SMTP config
    smtp_conf = get_smtp_config(user_id)
    if not smtp_conf or not smtp_conf.get("smtp_host") and not smtp_conf.get("api_key"):
        log.warning(f"User {user_id} has no SMTP config — campaign paused")
        db.execute("UPDATE campaigns SET status='paused' WHERE id=? AND user_id=?",
                   (campaign_id, user_id))
        db.commit()
        return False

    # Send
    tracking_token = make_tracking_token(enroll_id, step_num)
    from_email = enrollment["from_email"] or smtp_conf.get("smtp_user", "")
    from_name  = enrollment["from_name"] or ""

    try:
        message_id = send_email(
            smtp_conf,
            to=to_email,
            subject=subject,
            html=body_html,
            text=body_text,
            from_name=from_name,
            from_email=from_email,
            tracking_token=tracking_token,
        )
    except Exception as exc:
        log.error(f"SMTP send failed enrollment {enroll_id}: {exc}")
        # Pause enrollment on repeated failure
        db.execute(
            "UPDATE campaign_leads SET status='paused' WHERE id=?", (enroll_id,)
        )
        db.commit()
        return False

    # Record sent event
    db.execute("""
        INSERT INTO outreach_events
            (campaign_lead_id, campaign_id, lead_id, sequence_step, event_type,
             email_subject, message_id, occurred_at)
        VALUES (?, ?, ?, ?, 'sent', ?, ?, datetime('now'))
    """, (enroll_id, campaign_id, lead_id, step_num, subject, message_id))

    # Advance enrollment
    next_seq = db.execute("""
        SELECT delay_days FROM campaign_sequences
        WHERE campaign_id = ? AND step_number = ?
    """, (campaign_id, step_num + 1)).fetchone()

    if next_seq:
        next_send = (datetime.now(timezone.utc) + timedelta(
            days=max(1, int(next_seq["delay_days"]))
        )).isoformat()
        db.execute("""
            UPDATE campaign_leads
            SET current_step = ?, next_send_at = ?,
                last_sent_at = datetime('now'),
                emails_sent = emails_sent + 1,
                status = 'active'
            WHERE id = ?
        """, (step_num, next_send, enroll_id))
    else:
        _complete_enrollment(enroll_id, db=db)

    db.commit()

    # Auto-advance pipeline to "Contacted"
    try:
        from crm.service import auto_advance_to_stage_name
        auto_advance_to_stage_name(lead_id, user_id, "Contacted")
    except Exception:
        pass

    # Emit event for workflow
    _emit("email_sent", {
        "user_id": user_id, "lead_id": lead_id,
        "campaign_id": campaign_id, "step": step_num,
    })

    log.debug(f"Sent step {step_num} of campaign {campaign_id} to lead {lead_id}")
    return True


# ── Enrollment helpers ────────────────────────────────────────────────────

def enroll_lead(campaign_id: int, lead_id: int, user_id: int,
                pipeline_item_id: int | None = None) -> bool:
    """
    Enroll a lead in a campaign. Sets next_send_at to now.
    Returns False if already enrolled.
    """
    db = _db()
    camp = db.execute(
        "SELECT id FROM campaigns WHERE id=? AND user_id=?",
        (campaign_id, user_id)
    ).fetchone()
    if not camp:
        raise ValueError("Campaign not found")

    try:
        db.execute("""
            INSERT INTO campaign_leads
                (campaign_id, lead_id, pipeline_item_id, status, current_step, next_send_at)
            VALUES (?, ?, ?, 'active', 0, datetime('now'))
        """, (campaign_id, lead_id, pipeline_item_id))
        db.commit()

        _emit("campaign_enrolled", {
            "user_id": user_id, "lead_id": lead_id, "campaign_id": campaign_id,
        })
        return True
    except Exception:
        return False  # Already enrolled (UNIQUE constraint)


def bulk_enroll(campaign_id: int, lead_ids: list[int],
                user_id: int) -> dict:
    enrolled, skipped = 0, 0
    for lid in lead_ids:
        if enroll_lead(campaign_id, lid, user_id):
            enrolled += 1
        else:
            skipped += 1
    return {"enrolled": enrolled, "skipped": skipped}


def _complete_enrollment(enroll_id: int, db=None) -> None:
    if db is None:
        db = _db()
    db.execute("""
        UPDATE campaign_leads
        SET status='completed', completed_at=datetime('now'), next_send_at=NULL
        WHERE id=?
    """, (enroll_id,))


def _has_replied(enroll_id: int) -> bool:
    db = _db()
    row = db.execute(
        "SELECT 1 FROM outreach_events WHERE campaign_lead_id=? AND event_type='replied' LIMIT 1",
        (enroll_id,)
    ).fetchone()
    return row is not None


def _has_opened(enroll_id: int) -> bool:
    db = _db()
    row = db.execute(
        "SELECT 1 FROM outreach_events WHERE campaign_lead_id=? AND event_type='opened' LIMIT 1",
        (enroll_id,)
    ).fetchone()
    return row is not None


def _count_sent_today(user_id: int) -> int:
    db = _db()
    row = db.execute("""
        SELECT COUNT(*) FROM outreach_events oe
        JOIN campaign_leads cl ON cl.id = oe.campaign_lead_id
        JOIN campaigns c ON c.id = oe.campaign_id
        WHERE c.user_id = ? AND oe.event_type = 'sent'
          AND date(oe.occurred_at) = date('now')
    """, (user_id,)).fetchone()
    return row[0] if row else 0


# ── Lead context ───────────────────────────────────────────────────────────

def _load_lead_context(lead_id: int) -> dict:
    db = _db()
    core = db.execute("SELECT * FROM lead_core WHERE id=?", (lead_id,)).fetchone()
    enr  = db.execute("SELECT * FROM lead_enrichment WHERE lead_id=?", (lead_id,)).fetchone()
    ins  = db.execute("""
        SELECT * FROM lead_insights WHERE lead_id=?
        ORDER BY generated_at DESC LIMIT 1
    """, (lead_id,)).fetchone()
    sigs = db.execute("""
        SELECT * FROM lead_signals WHERE lead_id=?
        ORDER BY confidence DESC LIMIT 5
    """, (lead_id,)).fetchall()

    ctx = {}
    if core: ctx.update(dict(core))
    if enr:  ctx.update(dict(enr))
    if ins:
        i = dict(ins)
        ctx["next_action"]     = i.get("next_action", "")
        ctx["weaknesses"]      = i.get("weaknesses", "[]")
        ctx["strengths"]       = i.get("strengths", "[]")
        ctx["outreach_angles"] = i.get("outreach_angles", "[]")
    ctx["signals"] = [dict(s) for s in sigs]
    return ctx


def _html_to_text(html: str) -> str:
    """Very basic HTML → plain text strip."""
    import re
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def record_open_event(campaign_lead_id: int, step: int) -> None:
    db = _db()
    row = db.execute("SELECT * FROM campaign_leads WHERE id=?", (campaign_lead_id,)).fetchone()
    if not row:
        return
    db.execute("""
        INSERT OR IGNORE INTO outreach_events
            (campaign_lead_id, campaign_id, lead_id, sequence_step, event_type)
        VALUES (?, ?, ?, ?, 'opened')
    """, (campaign_lead_id, row["campaign_id"], row["lead_id"], step))
    db.execute(
        "UPDATE campaign_leads SET opens=opens+1 WHERE id=?", (campaign_lead_id,)
    )
    db.commit()


def _emit(event_type: str, payload: dict) -> None:
    try:
        db = _db()
        db.execute(
            "INSERT INTO workflow_event_queue (user_id, event_type, payload) VALUES (?,?,?)",
            (payload.get("user_id"), event_type, json.dumps(payload, default=str))
        )
        db.commit()
    except Exception:
        pass
