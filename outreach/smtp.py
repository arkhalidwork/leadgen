"""
LeadGen — Outreach: SMTP / Sending Utilities

Handles SMTP credential storage, email dispatch, tracking token generation.
"""
from __future__ import annotations

import base64
import email as email_lib
import hashlib
import hmac
import imaplib
import json
import logging
import os
import smtplib
import sqlite3
import threading
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

log = logging.getLogger(__name__)

_DB_PATH    = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_HMAC_KEY   = os.environ.get("LEADGEN_SECRET_KEY", "dev-secret-change-in-prod").encode()
_local      = threading.local()

TRANSPARENT_GIF = base64.b64decode(
    "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7"
)


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
    return _local.db


# ── SMTP Config ────────────────────────────────────────────────────────────

def get_smtp_config(user_id: int) -> dict | None:
    db = _db()
    row = db.execute(
        "SELECT * FROM user_smtp_config WHERE user_id = ?", (user_id,)
    ).fetchone()
    return dict(row) if row else None


def save_smtp_config(user_id: int, config: dict) -> None:
    db = _db()
    db.execute("""
        INSERT INTO user_smtp_config
            (user_id, provider, smtp_host, smtp_port, smtp_user, smtp_pass, api_key, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(user_id) DO UPDATE SET
            provider  = excluded.provider,
            smtp_host = excluded.smtp_host,
            smtp_port = excluded.smtp_port,
            smtp_user = excluded.smtp_user,
            smtp_pass = excluded.smtp_pass,
            api_key   = excluded.api_key,
            updated_at = datetime('now')
    """, (
        user_id,
        config.get("provider", "smtp"),
        config.get("smtp_host", ""),
        int(config.get("smtp_port", 587)),
        config.get("smtp_user", ""),
        config.get("smtp_pass", ""),  # TODO: AES encrypt before storing
        config.get("api_key", ""),
    ))
    db.commit()


def verify_smtp(user_id: int) -> tuple[bool, str]:
    """Attempt SMTP login. Returns (success, message)."""
    conf = get_smtp_config(user_id)
    if not conf or not conf.get("smtp_host"):
        return False, "No SMTP config found"
    try:
        with smtplib.SMTP(conf["smtp_host"], int(conf["smtp_port"]), timeout=10) as s:
            s.starttls()
            s.login(conf["smtp_user"], conf["smtp_pass"])
        db = _db()
        db.execute(
            "UPDATE user_smtp_config SET is_verified=1, updated_at=datetime('now') WHERE user_id=?",
            (user_id,)
        )
        db.commit()
        return True, "SMTP verified successfully"
    except smtplib.SMTPAuthenticationError:
        return False, "SMTP authentication failed — check username/password"
    except Exception as exc:
        return False, f"SMTP error: {exc}"


# ── Send Email ─────────────────────────────────────────────────────────────

def send_email(smtp_conf: dict, *, to: str, subject: str,
               html: str, text: str = "", from_name: str = "",
               from_email: str = "", tracking_token: str = "") -> str:
    """
    Send a single email via SMTP. Returns Message-ID string.
    Raises on failure.
    """
    provider = smtp_conf.get("provider", "smtp")

    if provider == "sendgrid":
        return _send_sendgrid(smtp_conf, to=to, subject=subject, html=html,
                              text=text, from_name=from_name, from_email=from_email)
    elif provider == "mailgun":
        return _send_mailgun(smtp_conf, to=to, subject=subject, html=html,
                             text=text, from_name=from_email, domain=smtp_conf.get("smtp_host",""))

    # Default: raw SMTP
    return _send_smtp(smtp_conf, to=to, subject=subject, html=html, text=text,
                      from_name=from_name, from_email=from_email,
                      tracking_token=tracking_token)


def _send_smtp(conf: dict, *, to: str, subject: str, html: str, text: str,
               from_name: str, from_email: str, tracking_token: str) -> str:
    msg = MIMEMultipart("alternative")
    message_id = f"<{uuid.uuid4()}@leadgen>"
    msg["Message-ID"] = message_id
    msg["Subject"]    = subject
    msg["From"]       = f"{from_name} <{from_email}>" if from_name else from_email
    msg["To"]         = to
    msg["List-Unsubscribe"] = f"<mailto:{from_email}?subject=unsubscribe>"

    if text:
        msg.attach(MIMEText(text, "plain"))

    # Inject tracking pixel
    if tracking_token:
        base_url = os.environ.get("APP_BASE_URL", "http://localhost:5000")
        pixel = f'<img src="{base_url}/t/{tracking_token}.gif" width="1" height="1" />'
        html  = html + pixel

    msg.attach(MIMEText(html, "html"))

    host = conf["smtp_host"]
    port = int(conf.get("smtp_port", 587))
    with smtplib.SMTP(host, port, timeout=20) as s:
        s.starttls()
        s.login(conf["smtp_user"], conf["smtp_pass"])
        s.sendmail(from_email, [to], msg.as_string())

    return message_id


def _send_sendgrid(conf: dict, *, to: str, subject: str, html: str, text: str,
                   from_name: str, from_email: str) -> str:
    import urllib.request, urllib.parse
    payload = json.dumps({
        "personalizations": [{"to": [{"email": to}]}],
        "from": {"email": from_email, "name": from_name},
        "subject": subject,
        "content": [
            {"type": "text/plain", "value": text or ""},
            {"type": "text/html",  "value": html},
        ],
    }).encode()
    req = urllib.request.Request(
        "https://api.sendgrid.com/v3/mail/send",
        data=payload,
        headers={
            "Authorization": f"Bearer {conf['api_key']}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        if resp.status not in (200, 202):
            raise RuntimeError(f"SendGrid error {resp.status}")
    return f"<sg-{uuid.uuid4()}@sendgrid>"


def _send_mailgun(conf: dict, *, to: str, subject: str, html: str, text: str,
                  from_name: str, domain: str) -> str:
    import urllib.request, urllib.parse
    data = urllib.parse.urlencode({
        "from": from_name,
        "to": to,
        "subject": subject,
        "text": text or "",
        "html": html,
    }).encode()
    api_key  = conf.get("api_key", "")
    creds    = base64.b64encode(f"api:{api_key}".encode()).decode()
    req = urllib.request.Request(
        f"https://api.mailgun.net/v3/{domain}/messages",
        data=data,
        headers={"Authorization": f"Basic {creds}"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=20) as resp:
        body = json.loads(resp.read())
    return body.get("id", f"<mg-{uuid.uuid4()}>")


# ── Tracking Token ─────────────────────────────────────────────────────────

def make_tracking_token(campaign_lead_id: int, step: int) -> str:
    payload = f"{campaign_lead_id}:{step}"
    sig = hmac.new(_HMAC_KEY, payload.encode(), hashlib.sha256).hexdigest()[:16]
    raw = base64.urlsafe_b64encode(f"{payload}:{sig}".encode()).decode().rstrip("=")
    return raw


def decode_tracking_token(token: str) -> dict | None:
    try:
        padded = token + "=" * (-len(token) % 4)
        raw    = base64.urlsafe_b64decode(padded).decode()
        parts  = raw.split(":")
        if len(parts) != 3:
            return None
        claim_id, step, sig = parts
        expected_sig = hmac.new(
            _HMAC_KEY, f"{claim_id}:{step}".encode(), hashlib.sha256
        ).hexdigest()[:16]
        if not hmac.compare_digest(expected_sig, sig):
            return None
        return {"campaign_lead_id": int(claim_id), "step": int(step)}
    except Exception:
        return None
