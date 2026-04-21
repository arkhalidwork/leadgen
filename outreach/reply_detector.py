"""
LeadGen — Outreach: IMAP Reply Detector

Polls user's inbox for replies to tracked campaign emails.
Matches In-Reply-To or References headers against outreach_events.message_id.
"""
from __future__ import annotations

import email as email_lib
import imaplib
import json
import logging
import os
import sqlite3
import threading

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local   = threading.local()


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
    return _local.db


def poll_inbox_for_replies(user_id: int, max_messages: int = 50) -> int:
    """
    Poll IMAP inbox. Return count of replies matched + recorded.
    """
    from outreach.smtp import get_smtp_config
    conf = get_smtp_config(user_id)
    if not conf or not conf.get("smtp_host") or not conf.get("smtp_user"):
        return 0

    try:
        return _poll_imap(user_id, conf, max_messages)
    except Exception as exc:
        log.debug(f"IMAP poll user {user_id}: {exc}")
        return 0


def _poll_imap(user_id: int, conf: dict, max_messages: int) -> int:
    db      = _db()
    replies = 0

    with imaplib.IMAP4_SSL(conf["smtp_host"]) as imap:
        imap.login(conf["smtp_user"], conf["smtp_pass"])
        imap.select("INBOX")
        _, data = imap.search(None, "UNSEEN")
        msg_ids = data[0].split() if data[0] else []

        for mid in msg_ids[:max_messages]:
            try:
                _, raw = imap.fetch(mid, "(RFC822)")
                if not raw or not raw[0]:
                    continue
                msg = email_lib.message_from_bytes(raw[0][1])

                # Match via In-Reply-To or References
                candidates = []
                irt = msg.get("In-Reply-To", "").strip()
                ref = msg.get("References", "")
                if irt:
                    candidates.append(irt)
                for r in ref.split():
                    r = r.strip()
                    if r:
                        candidates.append(r)

                for msg_id_ref in candidates:
                    event = db.execute(
                        "SELECT * FROM outreach_events WHERE message_id = ?",
                        (msg_id_ref,)
                    ).fetchone()
                    if not event:
                        continue

                    enroll_id   = event["campaign_lead_id"]
                    campaign_id = event["campaign_id"]
                    lead_id     = event["lead_id"]
                    step        = event["sequence_step"]

                    # Check not already recorded
                    dup = db.execute("""
                        SELECT 1 FROM outreach_events
                        WHERE campaign_lead_id=? AND event_type='replied' LIMIT 1
                    """, (enroll_id,)).fetchone()
                    if dup:
                        break

                    # Record reply event
                    db.execute("""
                        INSERT INTO outreach_events
                            (campaign_lead_id, campaign_id, lead_id, sequence_step,
                             event_type, email_subject, occurred_at)
                        VALUES (?, ?, ?, ?, 'replied', ?, datetime('now'))
                    """, (enroll_id, campaign_id, lead_id, step,
                          msg.get("Subject", "Re:...")))

                    # Mark enrollment as replied+completed
                    db.execute("""
                        UPDATE campaign_leads
                        SET status='replied', replies=replies+1,
                            completed_at=datetime('now'), next_send_at=NULL
                        WHERE id=?
                    """, (enroll_id,))
                    db.commit()

                    # Auto-advance pipeline to Replied
                    try:
                        from crm.service import auto_advance_to_stage_name
                        auto_advance_to_stage_name(lead_id, user_id, "Replied")
                    except Exception:
                        pass

                    # Emit event for workflow
                    try:
                        db.execute("""
                            INSERT INTO workflow_event_queue (user_id, event_type, payload)
                            VALUES (?, 'reply_received', ?)
                        """, (user_id, json.dumps({
                            "user_id": user_id, "lead_id": lead_id,
                            "campaign_id": campaign_id, "step": step,
                        }, default=str)))
                        db.commit()
                    except Exception:
                        pass

                    replies += 1
                    break

            except Exception as exc:
                log.debug(f"IMAP message parse error: {exc}")

    return replies
