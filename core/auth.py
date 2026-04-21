"""
Authentication helpers and decorators for LeadGen.

Provides login_required, subscription_required, password hashing,
license key generation, and email/password validation.
"""
from __future__ import annotations

import re
import secrets
import hashlib
import functools
import logging

import bcrypt
from flask import session, request, redirect, url_for, jsonify

from core.db import get_db

log = logging.getLogger(__name__)

# ── Email validation ──
_EMAIL_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def is_valid_email(email: str) -> bool:
    return bool(_EMAIL_RE.match(email))


# ── Password hashing (bcrypt) ──

def hash_password(password: str) -> str:
    """Hash with bcrypt (adaptive cost)."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against a bcrypt hash. Also handles legacy SHA-256 hashes."""
    if not hashed.startswith("$2"):
        if hashlib.sha256(password.encode()).hexdigest() == hashed:
            return True
        return False
    return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))


def upgrade_password_if_needed(user_id: int, password: str, current_hash: str):
    """Transparently upgrade legacy SHA-256 hash to bcrypt on login."""
    if not current_hash.startswith("$2"):
        new_hash = hash_password(password)
        try:
            db = get_db()
            db.execute("UPDATE users SET password=? WHERE id=?", (new_hash, user_id))
            db.commit()
            log.info(f"Upgraded password hash for user {user_id}")
        except Exception:
            pass


# ── Password strength validation ──

def validate_password_strength(password: str) -> str | None:
    """Return an error message if password is too weak, else None."""
    if len(password) < 8:
        return "Password must be at least 8 characters."
    if not re.search(r"[A-Z]", password):
        return "Password must contain at least one uppercase letter."
    if not re.search(r"[a-z]", password):
        return "Password must contain at least one lowercase letter."
    if not re.search(r"[0-9]", password):
        return "Password must contain at least one number."
    return None


# ── License key generation ──

def generate_license_key() -> str:
    """Generate a cryptographically random license key."""
    segment = lambda n: secrets.token_hex(n).upper()
    return f"LEAD-{segment(2)}-{segment(2)}-{segment(2)}-{segment(2)}"


# ── User helpers ──

def current_user():
    """Return the logged-in user row or None."""
    uid = session.get("user_id")
    if not uid:
        return None
    db = get_db()
    return db.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()


# ── Decorators ──

def login_required(f):
    """Decorator: redirect to /login if not logged in."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required."}), 401
            return redirect(url_for("login_page"))
        return f(*args, **kwargs)
    return wrapper


def subscription_required(f):
    """Decorator: require active subscription (license validated)."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        uid = session.get("user_id")
        if not uid:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Authentication required."}), 401
            return redirect(url_for("login_page"))
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()
        if not user or not user["is_active"]:
            if request.path.startswith("/api/"):
                return jsonify({"error": "Active subscription required."}), 403
            return redirect(url_for("activate_page"))
        return f(*args, **kwargs)
    return wrapper
