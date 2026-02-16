"""
LeadGen - Lead Generation Suite
Flask backend with multi-tool scraping API, CSV export,
authentication, license keys, scrape history, and lead analytics.
"""

import os
import csv
import io
import json
import uuid
import hashlib
import sqlite3
import threading
import functools
from datetime import datetime, timedelta
from flask import Flask, render_template, request, jsonify, Response, session, redirect, url_for, g
from flask_cors import CORS
from scraper import GoogleMapsScraper, clean_leads
from linkedin_scraper import LinkedInScraper, clean_linkedin_leads
from instagram_scraper import InstagramScraper, clean_instagram_leads
from web_crawler import WebCrawlerScraper, clean_web_leads

# Instagram search-type aliases (old → new)
_IG_TYPE_MAP = {"emails": "profiles", "profiles": "profiles", "businesses": "businesses"}

app = Flask(
    __name__,
    template_folder=os.environ.get("LEADGEN_TEMPLATE_DIR", os.path.join(os.path.dirname(__file__), "templates")),
    static_folder=os.environ.get("LEADGEN_STATIC_DIR", os.path.join(os.path.dirname(__file__), "static")),
)
app.secret_key = os.environ.get("LEADGEN_SECRET_KEY", "leadgen-secret-change-me-in-prod")
app.permanent_session_lifetime = timedelta(days=30)
CORS(app)

# Store active scraping jobs and their results
scraping_jobs = {}          # Google Maps jobs
linkedin_jobs = {}          # LinkedIn jobs
instagram_jobs = {}         # Instagram jobs
webcrawler_jobs = {}        # Web Crawler jobs
OUTPUT_DIR = os.environ.get("LEADGEN_OUTPUT_DIR", os.path.join(os.path.dirname(__file__), "output"))
DB_PATH = os.environ.get("LEADGEN_DB_PATH", os.path.join(os.path.dirname(__file__), "leadgen.db"))
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ============================================================
# Database
# ============================================================

def get_db():
    """Return a per-request sqlite3 connection."""
    if "db" not in g:
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    """Create tables if they don't exist."""
    db = sqlite3.connect(DB_PATH)
    db.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            email       TEXT UNIQUE NOT NULL,
            password    TEXT NOT NULL,
            full_name   TEXT DEFAULT '',
            license_key TEXT DEFAULT '',
            is_active   INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now')),
            last_login  TEXT
        );
        CREATE TABLE IF NOT EXISTS license_keys (
            key         TEXT PRIMARY KEY,
            plan        TEXT DEFAULT 'pro',
            max_uses    INTEGER DEFAULT 1,
            used_count  INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now')),
            expires_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS scrape_history (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id     INTEGER NOT NULL,
            job_id      TEXT NOT NULL,
            tool        TEXT NOT NULL,
            keyword     TEXT DEFAULT '',
            location    TEXT DEFAULT '',
            search_type TEXT DEFAULT '',
            status      TEXT DEFAULT 'running',
            lead_count  INTEGER DEFAULT 0,
            strong      INTEGER DEFAULT 0,
            medium      INTEGER DEFAULT 0,
            weak        INTEGER DEFAULT 0,
            started_at  TEXT DEFAULT (datetime('now')),
            finished_at TEXT,
            csv_path    TEXT DEFAULT '',
            FOREIGN KEY (user_id) REFERENCES users(id)
        );
    """)
    # Seed a demo license key if none exist
    cur = db.execute("SELECT COUNT(*) FROM license_keys")
    if cur.fetchone()[0] == 0:
        demo_key = "LEAD-PRO-2026-DEMO"
        db.execute(
            "INSERT INTO license_keys (key, plan, max_uses) VALUES (?, 'pro', 100)",
            (demo_key,),
        )
    db.commit()
    db.close()


init_db()


# ============================================================
# Auth helpers
# ============================================================

def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def current_user():
    """Return the logged-in user row or None."""
    uid = session.get("user_id")
    if not uid:
        return None
    db = get_db()
    return db.execute("SELECT * FROM users WHERE id = ?", (uid,)).fetchone()


def login_required(f):
    """Decorator: redirect to /login if not logged in."""
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("user_id"):
            # API routes return JSON 401; page routes redirect
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


# ============================================================
# Lead quality scoring
# ============================================================

def score_lead(lead: dict, tool: str) -> str:
    """
    Score a lead as 'strong', 'medium', or 'weak' based on
    data completeness.
    """
    points = 0
    na = {"N/A", "", None}

    if tool == "gmaps":
        fields = [
            ("email", 3), ("phone", 2), ("website", 2),
            ("business_name", 1), ("address", 1), ("rating", 1),
            ("facebook", 1), ("instagram", 1),
        ]
    elif tool == "linkedin":
        fields = [
            ("profile_url", 2), ("name", 2), ("title", 2),
            ("company", 2), ("location", 1), ("snippet", 1),
        ]
    elif tool == "instagram":
        fields = [
            ("email", 3), ("phone", 2), ("website", 2),
            ("category", 1), ("followers", 1), ("bio", 1),
            ("display_name", 1),
        ]
    elif tool == "webcrawler":
        fields = [
            ("email", 3), ("phone", 2), ("website", 2),
            ("business_name", 1), ("address", 1),
            ("facebook", 1), ("instagram", 1),
        ]
    else:
        fields = [("email", 3), ("phone", 2), ("website", 2)]

    max_points = sum(w for _, w in fields)
    for field, weight in fields:
        val = lead.get(field, "")
        if val and val not in na:
            points += weight

    ratio = points / max_points if max_points else 0
    if ratio >= 0.6:
        return "strong"
    elif ratio >= 0.3:
        return "medium"
    return "weak"


def score_leads(leads: list[dict], tool: str) -> tuple[list[dict], dict]:
    """
    Score all leads and return (scored_leads, summary).
    Each lead gets a '_quality' key.
    """
    counts = {"strong": 0, "medium": 0, "weak": 0}
    for lead in leads:
        q = score_lead(lead, tool)
        lead["_quality"] = q
        counts[q] += 1
    return leads, counts


# ============================================================
# Scrape history helpers
# ============================================================

def record_scrape_start(user_id: int, job_id: str, tool: str,
                         keyword: str, location: str, search_type: str = "") -> int:
    """Insert a scrape_history row when a job starts."""
    db = get_db()
    cur = db.execute(
        "INSERT INTO scrape_history (user_id, job_id, tool, keyword, location, search_type) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (user_id, job_id, tool, keyword, location, search_type),
    )
    db.commit()
    return cur.lastrowid


def record_scrape_end(job_id: str, status: str, lead_count: int,
                       strong: int = 0, medium: int = 0, weak: int = 0,
                       csv_path: str = ""):
    """Update a scrape_history row when a job finishes."""
    db = get_db()
    db.execute(
        "UPDATE scrape_history SET status=?, lead_count=?, strong=?, medium=?, weak=?, "
        "finished_at=datetime('now'), csv_path=? WHERE job_id=?",
        (status, lead_count, strong, medium, weak, csv_path, job_id),
    )
    db.commit()


def _record_history_on_complete(job, tool: str):
    """Record history when a job completes (called from background thread)."""
    try:
        db = sqlite3.connect(DB_PATH)
        db.row_factory = sqlite3.Row
        _, counts = score_leads(list(job.leads), tool)
        status = getattr(job, "status", "completed")
        csv_path = getattr(job, "csv_path", "") or ""
        db.execute(
            "UPDATE scrape_history SET status=?, lead_count=?, strong=?, medium=?, weak=?, "
            "finished_at=datetime('now'), csv_path=? WHERE job_id=?",
            (status, len(job.leads), counts["strong"], counts["medium"], counts["weak"],
             csv_path, job.id),
        )
        db.commit()
        db.close()
    except Exception as e:
        import logging
        logging.getLogger(__name__).error(f"History record error: {e}")


def _insert_history_direct(user_id: int, job_id: str, tool: str,
                            keyword: str, location: str, search_type: str = ""):
    """Insert history row from main thread (has app context)."""
    try:
        db = get_db()
        db.execute(
            "INSERT INTO scrape_history (user_id, job_id, tool, keyword, location, search_type) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, job_id, tool, keyword, location, search_type),
        )
        db.commit()
    except Exception:
        pass


# ============================================================
# Job classes
# ============================================================

class ScrapingJob:
    """Tracks a Google Maps scraping job."""

    def __init__(self, keyword: str, place: str):
        self.id = str(uuid.uuid4())[:8]
        self.keyword = keyword
        self.place = place
        self.status = "running"
        self.progress = 0
        self.message = "Starting..."
        self.leads = []
        self.error = None
        self.csv_path = None
        self.scraper = None
        self.created_at = datetime.now().isoformat()
        self.started_at = datetime.now()  # for timer

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
        # Elapsed time
        elapsed = (datetime.now() - self.started_at).total_seconds()
        hours, rem = divmod(int(elapsed), 3600)
        minutes, secs = divmod(rem, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"

        # Area stats from scraper
        area_stats = {}
        if self.scraper:
            area_stats = self.scraper.area_stats

        return {
            "id": self.id,
            "keyword": self.keyword,
            "place": self.place,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "lead_count": len(self.leads),
            "error": self.error,
            "created_at": self.created_at,
            "elapsed": elapsed_str,
            "elapsed_seconds": int(elapsed),
            "area_stats": area_stats,
        }


class LinkedInJob:
    """Tracks a LinkedIn scraping job."""

    def __init__(self, niche: str, place: str, search_type: str = "profiles"):
        self.id = str(uuid.uuid4())[:8]
        self.niche = niche
        self.place = place
        self.search_type = search_type
        self.status = "running"
        self.progress = 0
        self.message = "Starting..."
        self.leads = []
        self.error = None
        self.scraper = None
        self.created_at = datetime.now().isoformat()
        self.started_at = datetime.now()

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
        elapsed = (datetime.now() - self.started_at).total_seconds()
        hours, rem = divmod(int(elapsed), 3600)
        minutes, secs = divmod(rem, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"

        scrape_stats = {}
        if self.scraper:
            scrape_stats = self.scraper.scrape_stats

        return {
            "id": self.id,
            "niche": self.niche,
            "place": self.place,
            "search_type": self.search_type,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "lead_count": len(self.leads),
            "error": self.error,
            "created_at": self.created_at,
            "elapsed": elapsed_str,
            "elapsed_seconds": int(elapsed),
            "scrape_stats": scrape_stats,
        }


class InstagramJob:
    """Tracks an Instagram scraping job."""

    def __init__(self, keywords: str, place: str, search_type: str = "emails"):
        self.id = str(uuid.uuid4())[:8]
        self.keywords = keywords
        self.place = place
        self.search_type = search_type
        self.status = "running"
        self.progress = 0
        self.message = "Starting..."
        self.leads = []
        self.error = None
        self.scraper = None
        self.created_at = datetime.now().isoformat()
        self.started_at = datetime.now()

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
        elapsed = (datetime.now() - self.started_at).total_seconds()
        hours, rem = divmod(int(elapsed), 3600)
        minutes, secs = divmod(rem, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"

        scrape_stats = {}
        if self.scraper:
            scrape_stats = self.scraper.scrape_stats

        return {
            "id": self.id,
            "keywords": self.keywords,
            "place": self.place,
            "search_type": self.search_type,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "lead_count": len(self.leads),
            "error": self.error,
            "created_at": self.created_at,
            "elapsed": elapsed_str,
            "elapsed_seconds": int(elapsed),
            "scrape_stats": scrape_stats,
        }


class WebCrawlerJob:
    """Tracks a Web Crawler scraping job."""

    def __init__(self, keyword: str, place: str):
        self.id = str(uuid.uuid4())[:8]
        self.keyword = keyword
        self.place = place
        self.status = "running"
        self.progress = 0
        self.message = "Starting..."
        self.leads = []
        self.error = None
        self.scraper = None
        self.created_at = datetime.now().isoformat()
        self.started_at = datetime.now()

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
        elapsed = (datetime.now() - self.started_at).total_seconds()
        hours, rem = divmod(int(elapsed), 3600)
        minutes, secs = divmod(rem, 60)
        elapsed_str = f"{hours:02d}:{minutes:02d}:{secs:02d}"

        scrape_stats = {}
        if self.scraper:
            scrape_stats = self.scraper.scrape_stats

        return {
            "id": self.id,
            "keyword": self.keyword,
            "place": self.place,
            "status": self.status,
            "progress": self.progress,
            "message": self.message,
            "lead_count": len(self.leads),
            "error": self.error,
            "created_at": self.created_at,
            "elapsed": elapsed_str,
            "elapsed_seconds": int(elapsed),
            "scrape_stats": scrape_stats,
        }


# ============================================================
# Background runners
# ============================================================

def run_scraping_job(job: ScrapingJob):
    """Run Google Maps scraping in a background thread."""
    try:
        scraper = GoogleMapsScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw_leads = scraper.scrape(job.keyword, job.place)
        cleaned = clean_leads(raw_leads)
        job.leads = cleaned

        if cleaned:
            filename = (
                f"leads_{job.keyword}_{job.place}_{job.id}.csv"
                .replace(" ", "_").lower()
            )
            csv_path = os.path.join(OUTPUT_DIR, filename)
            save_gmaps_csv(cleaned, csv_path)
            job.csv_path = csv_path

        if job.status == "stopped":
            # User stopped mid-way — save partial results
            partial = scraper.get_partial_leads()
            if partial:
                cleaned = clean_leads(partial)
                job.leads = cleaned
                if cleaned:
                    filename = (
                        f"leads_{job.keyword}_{job.place}_{job.id}_partial.csv"
                        .replace(" ", "_").lower()
                    )
                    csv_path = os.path.join(OUTPUT_DIR, filename)
                    save_gmaps_csv(cleaned, csv_path)
                    job.csv_path = csv_path
            job.message = f"Stopped. Saved {len(job.leads)} leads."
            _record_history_on_complete(job, "gmaps")
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} leads."
        _record_history_on_complete(job, "gmaps")
        _record_history_on_complete(job, "gmaps")

    except Exception as e:
        # On error, still save partial results
        if job.scraper:
            partial = job.scraper.get_partial_leads()
            if partial:
                cleaned = clean_leads(partial)
                job.leads = cleaned
        if job.status != "stopped":
            job.status = "failed"
            job.error = str(e)
            job.message = f"Error: {str(e)}. Saved {len(job.leads)} partial leads."
        _record_history_on_complete(job, "gmaps")


def run_linkedin_job(job: LinkedInJob):
    """Run LinkedIn scraping in a background thread."""
    try:
        scraper = LinkedInScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw = scraper.scrape(job.niche, job.place, search_type=job.search_type)
        cleaned = clean_linkedin_leads(raw, job.search_type)
        job.leads = cleaned

        if job.status == "stopped":
            partial = scraper.get_partial_leads()
            if partial:
                cleaned = clean_linkedin_leads(partial, job.search_type)
                job.leads = cleaned
            job.message = f"Stopped. Saved {len(job.leads)} {job.search_type}."
            _record_history_on_complete(job, "linkedin")
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} {job.search_type}."
        _record_history_on_complete(job, "linkedin")

    except Exception as e:
        if job.scraper:
            partial = job.scraper.get_partial_leads()
            if partial:
                cleaned = clean_linkedin_leads(partial, job.search_type)
                job.leads = cleaned
        if job.status != "stopped":
            job.status = "failed"
            job.error = str(e)
            job.message = f"Error: {str(e)}. Saved {len(job.leads)} partial leads."
        _record_history_on_complete(job, "linkedin")


def run_instagram_job(job: InstagramJob):
    """Run Instagram scraping in a background thread."""
    try:
        scraper = InstagramScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw = scraper.scrape(
            job.keywords, job.place, search_type=job.search_type,
        )
        cleaned = clean_instagram_leads(raw, job.search_type)
        job.leads = cleaned

        if job.status == "stopped":
            partial = scraper.get_partial_leads()
            if partial:
                cleaned = clean_instagram_leads(partial, job.search_type)
                job.leads = cleaned
            job.message = f"Stopped. Saved {len(job.leads)} Instagram {job.search_type}."
            _record_history_on_complete(job, "instagram")
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} Instagram {job.search_type}."
        _record_history_on_complete(job, "instagram")

    except Exception as e:
        if job.scraper:
            partial = job.scraper.get_partial_leads()
            if partial:
                cleaned = clean_instagram_leads(partial, job.search_type)
                job.leads = cleaned
        if job.status != "stopped":
            job.status = "failed"
            job.error = str(e)
            job.message = f"Error: {str(e)}. Saved {len(job.leads)} partial leads."
        _record_history_on_complete(job, "instagram")


def run_webcrawler_job(job: WebCrawlerJob):
    """Run Web Crawler scraping in a background thread."""
    try:
        scraper = WebCrawlerScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw = scraper.scrape(job.keyword, job.place)
        cleaned = clean_web_leads(raw)
        job.leads = cleaned

        if job.status == "stopped":
            partial = scraper.get_partial_leads()
            if partial:
                cleaned = clean_web_leads(partial)
                job.leads = cleaned
            job.message = f"Stopped. Saved {len(job.leads)} leads."
            _record_history_on_complete(job, "webcrawler")
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} leads from the web."
        _record_history_on_complete(job, "webcrawler")

    except Exception as e:
        if job.scraper:
            partial = job.scraper.get_partial_leads()
            if partial:
                cleaned = clean_web_leads(partial)
                job.leads = cleaned
        if job.status != "stopped":
            job.status = "failed"
            job.error = str(e)
            job.message = f"Error: {str(e)}. Saved {len(job.leads)} partial leads."
        _record_history_on_complete(job, "webcrawler")


# ============================================================
# Auth & Subscription routes
# ============================================================

@app.route("/login")
def login_page():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    return render_template("login.html")


@app.route("/register")
def register_page():
    if session.get("user_id"):
        return redirect(url_for("dashboard"))
    return render_template("register.html")


@app.route("/activate")
@login_required
def activate_page():
    user = current_user()
    if user and user["is_active"]:
        return redirect(url_for("dashboard"))
    return render_template("activate.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login_page"))


@app.route("/api/auth/register", methods=["POST"])
def api_register():
    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()
    full_name = (data.get("full_name") or "").strip()

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400
    if len(password) < 6:
        return jsonify({"error": "Password must be at least 6 characters."}), 400

    db = get_db()
    existing = db.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        return jsonify({"error": "An account with this email already exists."}), 409

    hashed = _hash_password(password)
    cur = db.execute(
        "INSERT INTO users (email, password, full_name) VALUES (?, ?, ?)",
        (email, hashed, full_name),
    )
    db.commit()
    user_id = cur.lastrowid

    session.permanent = True
    session["user_id"] = user_id
    session["email"] = email
    return jsonify({"message": "Account created. Please activate your license.", "user_id": user_id}), 201


@app.route("/api/auth/login", methods=["POST"])
def api_login():
    data = request.get_json()
    email = (data.get("email") or "").strip().lower()
    password = (data.get("password") or "").strip()

    if not email or not password:
        return jsonify({"error": "Email and password are required."}), 400

    db = get_db()
    user = db.execute("SELECT * FROM users WHERE email = ?", (email,)).fetchone()
    if not user or user["password"] != _hash_password(password):
        return jsonify({"error": "Invalid email or password."}), 401

    db.execute("UPDATE users SET last_login = datetime('now') WHERE id = ?", (user["id"],))
    db.commit()

    session.permanent = True
    session["user_id"] = user["id"]
    session["email"] = user["email"]
    return jsonify({
        "message": "Login successful.",
        "is_active": bool(user["is_active"]),
    })


@app.route("/api/auth/activate", methods=["POST"])
@login_required
def api_activate():
    data = request.get_json()
    key = (data.get("license_key") or "").strip().upper()
    if not key:
        return jsonify({"error": "License key is required."}), 400

    db = get_db()
    row = db.execute("SELECT * FROM license_keys WHERE key = ?", (key,)).fetchone()
    if not row:
        return jsonify({"error": "Invalid license key."}), 404
    if row["expires_at"] and row["expires_at"] < datetime.now().isoformat():
        return jsonify({"error": "This license key has expired."}), 410
    if row["used_count"] >= row["max_uses"]:
        return jsonify({"error": "This license key has reached its usage limit."}), 410

    uid = session["user_id"]
    db.execute("UPDATE users SET is_active = 1, license_key = ? WHERE id = ?", (key, uid))
    db.execute("UPDATE license_keys SET used_count = used_count + 1 WHERE key = ?", (key,))
    db.commit()
    return jsonify({"message": "License activated! Welcome to LeadGen Pro."})


@app.route("/api/auth/me")
@login_required
def api_me():
    user = current_user()
    if not user:
        return jsonify({"error": "Not found."}), 404
    return jsonify({
        "id": user["id"],
        "email": user["email"],
        "full_name": user["full_name"],
        "is_active": bool(user["is_active"]),
        "license_key": user["license_key"] or "",
        "created_at": user["created_at"],
    })


# ============================================================
# Dashboard analytics API
# ============================================================

@app.route("/api/dashboard/stats")
@login_required
def api_dashboard_stats():
    uid = session["user_id"]
    db = get_db()

    # Total leads
    total = db.execute(
        "SELECT COALESCE(SUM(lead_count),0) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]

    # Quality breakdown
    strong = db.execute(
        "SELECT COALESCE(SUM(strong),0) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]
    medium = db.execute(
        "SELECT COALESCE(SUM(medium),0) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]
    weak = db.execute(
        "SELECT COALESCE(SUM(weak),0) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]

    # Total scrapes
    scrape_count = db.execute(
        "SELECT COUNT(*) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]

    # By tool
    tool_rows = db.execute(
        "SELECT tool, COUNT(*) as cnt, COALESCE(SUM(lead_count),0) as leads "
        "FROM scrape_history WHERE user_id=? GROUP BY tool",
        (uid,),
    ).fetchall()
    by_tool = {r["tool"]: {"scrapes": r["cnt"], "leads": r["leads"]} for r in tool_rows}

    # Recent 7 days trend
    trend_rows = db.execute(
        "SELECT DATE(started_at) as day, COALESCE(SUM(lead_count),0) as leads "
        "FROM scrape_history WHERE user_id=? AND started_at >= datetime('now','-7 days') "
        "GROUP BY DATE(started_at) ORDER BY day",
        (uid,),
    ).fetchall()
    trend = [{"day": r["day"], "leads": r["leads"]} for r in trend_rows]

    return jsonify({
        "total_leads": total,
        "strong": strong,
        "medium": medium,
        "weak": weak,
        "scrape_count": scrape_count,
        "by_tool": by_tool,
        "trend": trend,
    })


@app.route("/api/dashboard/history")
@login_required
def api_dashboard_history():
    uid = session["user_id"]
    page = request.args.get("page", 1, type=int)
    per_page = request.args.get("per_page", 20, type=int)
    offset = (page - 1) * per_page

    db = get_db()
    total = db.execute(
        "SELECT COUNT(*) FROM scrape_history WHERE user_id=?", (uid,)
    ).fetchone()[0]

    rows = db.execute(
        "SELECT * FROM scrape_history WHERE user_id=? "
        "ORDER BY started_at DESC LIMIT ? OFFSET ?",
        (uid, per_page, offset),
    ).fetchall()

    history = []
    for r in rows:
        history.append({
            "id": r["id"],
            "job_id": r["job_id"],
            "tool": r["tool"],
            "keyword": r["keyword"],
            "location": r["location"],
            "search_type": r["search_type"],
            "status": r["status"],
            "lead_count": r["lead_count"],
            "strong": r["strong"],
            "medium": r["medium"],
            "weak": r["weak"],
            "started_at": r["started_at"],
            "finished_at": r["finished_at"],
        })

    return jsonify({"history": history, "total": total, "page": page, "per_page": per_page})


@app.route("/api/leads/quality/<job_id>")
@login_required
def api_lead_quality(job_id):
    """Score leads for a specific active job."""
    # Search across all job stores
    job = (
        scraping_jobs.get(job_id)
        or linkedin_jobs.get(job_id)
        or instagram_jobs.get(job_id)
        or webcrawler_jobs.get(job_id)
    )
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if not job.leads:
        return jsonify({"strong": 0, "medium": 0, "weak": 0, "total": 0})

    # Determine tool type
    if job_id in scraping_jobs:
        tool = "gmaps"
    elif job_id in linkedin_jobs:
        tool = "linkedin"
    elif job_id in instagram_jobs:
        tool = "instagram"
    else:
        tool = "webcrawler"

    _, counts = score_leads(list(job.leads), tool)
    return jsonify({**counts, "total": len(job.leads)})


# ============================================================

def save_gmaps_csv(leads: list[dict], filepath: str):
    """Save Google Maps leads to CSV."""
    if not leads:
        return
    fieldnames = [
        "business_name", "owner_name", "phone", "website", "email",
        "address", "rating", "reviews", "category",
        "facebook", "instagram", "twitter", "linkedin",
        "youtube", "tiktok", "pinterest",
    ]
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(leads)


# ============================================================
# Page routes
# ============================================================

@app.route("/")
@login_required
def dashboard():
    """Dashboard landing page."""
    user = current_user()
    if user and not user["is_active"]:
        return redirect(url_for("activate_page"))
    return render_template("dashboard.html", active_page="dashboard")


@app.route("/tools/google-maps")
@subscription_required
def google_maps_tool():
    """Google Maps scraper page."""
    return render_template("gmaps.html", active_page="gmaps")


@app.route("/tools/linkedin")
@subscription_required
def linkedin_tool():
    """LinkedIn scraper page."""
    return render_template("linkedin.html", active_page="linkedin")


@app.route("/tools/instagram")
@subscription_required
def instagram_tool():
    """Instagram scraper page."""
    return render_template("instagram.html", active_page="instagram")


@app.route("/tools/web-crawler")
@subscription_required
def webcrawler_tool():
    """Web Crawler page."""
    return render_template("webcrawler.html", active_page="webcrawler")


# ============================================================
# Google Maps API
# ============================================================

@app.route("/api/scrape", methods=["POST"])
@subscription_required
def start_scrape():
    """Start a new Google Maps scraping job."""
    data = request.get_json()
    keyword = data.get("keyword", "").strip()
    place = data.get("place", "").strip()

    if not keyword or not place:
        return jsonify({"error": "Both keyword and place are required."}), 400

    job = ScrapingJob(keyword, place)
    scraping_jobs[job.id] = job
    _insert_history_direct(session["user_id"], job.id, "gmaps", keyword, place)

    thread = threading.Thread(target=run_scraping_job, args=(job,), daemon=True)
    thread.start()

    return jsonify({"job_id": job.id, "message": "Scraping started."}), 202


@app.route("/api/status/<job_id>")
def job_status(job_id):
    job = scraping_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job.to_dict())


@app.route("/api/results/<job_id>")
def job_results(job_id):
    job = scraping_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped"):
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@app.route("/api/download/<job_id>")
def download_csv(job_id):
    job = scraping_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped") or not job.leads:
        return jsonify({"error": "No data available for download."}), 400

    output = io.StringIO()
    fieldnames = [
        "Business Name", "Owner Name", "Phone", "Website", "Email",
        "Address", "Rating", "Reviews", "Category",
        "Facebook", "Instagram", "Twitter", "LinkedIn",
        "YouTube", "TikTok", "Pinterest",
    ]
    key_map = {
        "Business Name": "business_name", "Owner Name": "owner_name",
        "Phone": "phone", "Website": "website", "Email": "email",
        "Address": "address", "Rating": "rating", "Reviews": "reviews",
        "Category": "category", "Facebook": "facebook",
        "Instagram": "instagram", "Twitter": "twitter",
        "LinkedIn": "linkedin", "YouTube": "youtube",
        "TikTok": "tiktok", "Pinterest": "pinterest",
    }
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for lead in job.leads:
        row = {display: lead.get(key, "N/A") for display, key in key_map.items()}
        writer.writerow(row)

    output.seek(0)
    filename = f"leads_{job.keyword}_{job.place}.csv".replace(" ", "_").lower()
    return Response(
        output.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/stop/<job_id>", methods=["POST"])
def stop_scrape(job_id):
    job = scraping_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.scraper:
        job.scraper.stop()
        # Immediately grab partial leads
        partial = job.scraper.get_partial_leads()
        if partial:
            cleaned = clean_leads(partial)
            job.leads = cleaned
    job.status = "stopped"
    job.message = f"Stopped by user. Saved {len(job.leads)} leads."
    return jsonify({"message": f"Job stopped. {len(job.leads)} leads saved."})


# ============================================================
# LinkedIn API
# ============================================================

@app.route("/api/linkedin/scrape", methods=["POST"])
@subscription_required
def linkedin_start_scrape():
    """Start a new LinkedIn scraping job."""
    data = request.get_json()
    niche = data.get("niche", "").strip()
    place = data.get("place", "").strip()
    search_type = data.get("search_type", "profiles").strip()

    if not niche or not place:
        return jsonify({"error": "Both niche and place are required."}), 400
    if search_type not in ("profiles", "companies"):
        return jsonify({"error": "search_type must be 'profiles' or 'companies'."}), 400

    job = LinkedInJob(niche, place, search_type)
    linkedin_jobs[job.id] = job
    _insert_history_direct(session["user_id"], job.id, "linkedin", niche, place, search_type)

    thread = threading.Thread(target=run_linkedin_job, args=(job,), daemon=True)
    thread.start()

    return jsonify({"job_id": job.id, "message": "LinkedIn scraping started."}), 202


@app.route("/api/linkedin/status/<job_id>")
def linkedin_status(job_id):
    job = linkedin_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job.to_dict())


@app.route("/api/linkedin/results/<job_id>")
def linkedin_results(job_id):
    job = linkedin_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped"):
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@app.route("/api/linkedin/download/<job_id>")
def linkedin_download(job_id):
    job = linkedin_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped") or not job.leads:
        return jsonify({"error": "No data available for download."}), 400

    output = io.StringIO()

    if job.search_type == "profiles":
        fieldnames = ["Name", "Title", "Company", "Location", "Profile URL", "LinkedIn Username", "Snippet"]
        key_map = {
            "Name": "name", "Title": "title", "Company": "company",
            "Location": "location", "Profile URL": "profile_url",
            "LinkedIn Username": "linkedin_username", "Snippet": "snippet",
        }
    else:
        fieldnames = ["Company Name", "Industry", "Size", "Location", "Company URL", "Description"]
        key_map = {
            "Company Name": "company_name", "Industry": "industry",
            "Size": "company_size", "Location": "location",
            "Company URL": "company_url", "Description": "description",
        }

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for lead in job.leads:
        row = {display: lead.get(key, "N/A") for display, key in key_map.items()}
        writer.writerow(row)

    output.seek(0)
    filename = f"linkedin_{job.search_type}_{job.niche}_{job.place}.csv".replace(" ", "_").lower()
    return Response(
        output.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/linkedin/stop/<job_id>", methods=["POST"])
def linkedin_stop(job_id):
    job = linkedin_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.scraper:
        job.scraper.stop()
        partial = job.scraper.get_partial_leads()
        if partial:
            cleaned = clean_linkedin_leads(partial, job.search_type)
            job.leads = cleaned
    job.status = "stopped"
    job.message = f"Stopped by user. Saved {len(job.leads)} {job.search_type}."
    return jsonify({"message": f"Job stopped. {len(job.leads)} leads saved."})


# ============================================================
# Instagram API
# ============================================================

@app.route("/api/instagram/scrape", methods=["POST"])
@subscription_required
def instagram_start_scrape():
    """Start a new Instagram scraping job."""
    data = request.get_json()
    keywords = data.get("keywords", "").strip()
    place = data.get("place", "").strip()
    search_type = data.get("search_type", "emails").strip()

    if not place:
        return jsonify({"error": "Location is required."}), 400
    search_type = _IG_TYPE_MAP.get(search_type, search_type)
    if search_type not in ("profiles", "businesses"):
        return jsonify({"error": "search_type must be 'profiles' or 'businesses'."}), 400

    job = InstagramJob(keywords, place, search_type)
    instagram_jobs[job.id] = job
    _insert_history_direct(session["user_id"], job.id, "instagram", keywords, place, search_type)

    thread = threading.Thread(target=run_instagram_job, args=(job,), daemon=True)
    thread.start()

    return jsonify({"job_id": job.id, "message": "Instagram scraping started."}), 202


@app.route("/api/instagram/status/<job_id>")
def instagram_status(job_id):
    job = instagram_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job.to_dict())


@app.route("/api/instagram/results/<job_id>")
def instagram_results(job_id):
    job = instagram_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped"):
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@app.route("/api/instagram/download/<job_id>")
def instagram_download(job_id):
    job = instagram_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped") or not job.leads:
        return jsonify({"error": "No data available for download."}), 400

    output = io.StringIO()

    fieldnames = [
        "Username", "Display Name", "Bio", "Email", "Phone",
        "Website", "Category", "Followers", "Location", "Profile URL",
    ]
    key_map = {
        "Username": "username", "Display Name": "display_name",
        "Bio": "bio", "Email": "email", "Phone": "phone",
        "Website": "website", "Category": "category",
        "Followers": "followers", "Location": "location",
        "Profile URL": "profile_url",
    }

    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for lead in job.leads:
        row = {display: lead.get(key, "N/A") for display, key in key_map.items()}
        writer.writerow(row)

    output.seek(0)
    filename = f"instagram_{job.search_type}_{job.place}.csv".replace(" ", "_").lower()
    return Response(
        output.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/instagram/stop/<job_id>", methods=["POST"])
def instagram_stop(job_id):
    job = instagram_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.scraper:
        job.scraper.stop()
        partial = job.scraper.get_partial_leads()
        if partial:
            cleaned = clean_instagram_leads(partial, job.search_type)
            job.leads = cleaned
    job.status = "stopped"
    job.message = f"Stopped by user. Saved {len(job.leads)} {job.search_type}."
    return jsonify({"message": f"Job stopped. {len(job.leads)} leads saved."})


# ============================================================
# Web Crawler API
# ============================================================

@app.route("/api/webcrawler/scrape", methods=["POST"])
@subscription_required
def webcrawler_start_scrape():
    """Start a new Web Crawler scraping job."""
    data = request.get_json()
    keyword = data.get("keyword", "").strip()
    place = data.get("place", "").strip()

    if not keyword or not place:
        return jsonify({"error": "Both keyword and place are required."}), 400

    job = WebCrawlerJob(keyword, place)
    webcrawler_jobs[job.id] = job
    _insert_history_direct(session["user_id"], job.id, "webcrawler", keyword, place)

    thread = threading.Thread(target=run_webcrawler_job, args=(job,), daemon=True)
    thread.start()

    return jsonify({"job_id": job.id, "message": "Web crawling started."}), 202


@app.route("/api/webcrawler/status/<job_id>")
def webcrawler_status(job_id):
    job = webcrawler_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job.to_dict())


@app.route("/api/webcrawler/results/<job_id>")
def webcrawler_results(job_id):
    job = webcrawler_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped"):
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@app.route("/api/webcrawler/download/<job_id>")
def webcrawler_download(job_id):
    job = webcrawler_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped") or not job.leads:
        return jsonify({"error": "No data available for download."}), 400

    output = io.StringIO()
    fieldnames = [
        "Business Name", "Phone", "Email", "Website", "Address",
        "Description", "Source", "Facebook", "Instagram",
        "Twitter", "LinkedIn", "YouTube",
    ]
    key_map = {
        "Business Name": "business_name", "Phone": "phone",
        "Email": "email", "Website": "website",
        "Address": "address", "Description": "description",
        "Source": "source", "Facebook": "facebook",
        "Instagram": "instagram", "Twitter": "twitter",
        "LinkedIn": "linkedin", "YouTube": "youtube",
    }
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for lead in job.leads:
        row = {display: lead.get(key, "N/A") for display, key in key_map.items()}
        writer.writerow(row)

    output.seek(0)
    filename = f"webcrawler_{job.keyword}_{job.place}.csv".replace(" ", "_").lower()
    return Response(
        output.getvalue(), mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/webcrawler/stop/<job_id>", methods=["POST"])
def webcrawler_stop(job_id):
    job = webcrawler_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.scraper:
        job.scraper.stop()
        partial = job.scraper.get_partial_leads()
        if partial:
            cleaned = clean_web_leads(partial)
            job.leads = cleaned
    job.status = "stopped"
    job.message = f"Stopped by user. Saved {len(job.leads)} leads."
    return jsonify({"message": f"Job stopped. {len(job.leads)} leads saved."})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
