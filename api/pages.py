"""
Page routes — Blueprint module.

All render_template() routes: landing, dashboard, tool pages,
goal-based pages, database, settings.
"""
from __future__ import annotations

from datetime import datetime
import logging

from flask import Blueprint, render_template, redirect, url_for, session, request, jsonify

from core import IS_DESKTOP
from core.auth import login_required, subscription_required, current_user

log = logging.getLogger(__name__)

pages_bp = Blueprint("pages", __name__)


@pages_bp.route("/")
def landing_page():
    """Public landing page."""
    if IS_DESKTOP:
        return redirect(url_for("pages.login_page"))
    if session.get("user_id"):
        return redirect(url_for("pages.dashboard"))
    return render_template("landing.html", current_year=datetime.now().year)


@pages_bp.route("/dashboard")
@login_required
def dashboard():
    """Dashboard page."""
    user = current_user()
    if user and not user["is_active"]:
        return redirect(url_for("pages.activate_page"))
    return render_template("dashboard.html", active_page="dashboard")


@pages_bp.route("/tools/google-maps")
@subscription_required
def google_maps_tool():
    """Google Maps scraper page."""
    return render_template("gmaps.html", active_page="gmaps")


@pages_bp.route("/tools/linkedin")
@subscription_required
def linkedin_tool():
    """LinkedIn scraper page."""
    return render_template("linkedin.html", active_page="linkedin")


@pages_bp.route("/tools/instagram")
@subscription_required
def instagram_tool():
    """Instagram scraper page."""
    return render_template("instagram.html", active_page="instagram")


@pages_bp.route("/tools/web-crawler")
@subscription_required
def webcrawler_tool():
    """Web Crawler page."""
    return render_template("webcrawler.html", active_page="webcrawler")


@pages_bp.route("/tools/email-outreach")
@subscription_required
def email_outreach_tool():
    """Email Outreach Template Generator page."""
    return render_template("email_outreach.html", active_page="email_outreach")


@pages_bp.route("/sessions")
@subscription_required
def sessions_page():
    """Google Maps sessions page."""
    return render_template("sessions.html", active_page="sessions")


@pages_bp.route("/intelligence")
@login_required
def page_intelligence():
    """Lead Intelligence page — scored leads, signals, and insights."""
    return render_template("intelligence.html", active_page="intelligence")


# ── Phase 5: Goal-based page routes ──

@pages_bp.route("/find-leads")
@login_required
def page_find_leads():
    """Find Leads hub — entry point for all scraping tools."""
    return render_template("find_leads.html", active_page="find_leads")


@pages_bp.route("/pipeline")
@login_required
def page_pipeline():
    """CRM Pipeline — Kanban board for lead management."""
    return render_template("pipeline.html", active_page="pipeline")


@pages_bp.route("/outreach")
@login_required
def page_outreach():
    """Outreach — email campaign manager."""
    return render_template("outreach.html", active_page="outreach")


@pages_bp.route("/workflows")
@login_required
def page_workflows():
    """Workflows — visual automation builder."""
    return render_template("workflows.html", active_page="workflows")


@pages_bp.route("/database")
@login_required
def database_page():
    """Lead database page."""
    user = current_user()
    if user and not user["is_active"]:
        return redirect(url_for("pages.activate_page"))
    scrape_id = request.args.get("scrape_id", "")
    return render_template("database.html", active_page="database", scrape_id=scrape_id)


@pages_bp.route("/settings")
@login_required
def settings_page():
    """Account settings page."""
    return render_template("settings.html", active_page="settings")


# ── Auth pages ──

@pages_bp.route("/login")
def login_page():
    if session.get("user_id"):
        return redirect(url_for("pages.dashboard"))
    return render_template("login.html")


@pages_bp.route("/register")
def register_page():
    if session.get("user_id"):
        return redirect(url_for("pages.dashboard"))
    return render_template("register.html")


@pages_bp.route("/activate")
@login_required
def activate_page():
    user = current_user()
    if user and user["is_active"]:
        return redirect(url_for("pages.dashboard"))
    return render_template("activate.html")


@pages_bp.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("pages.login_page"))


# ── Dashboard metrics API ──

@pages_bp.route("/api/dashboard/metrics")
@login_required
def api_dashboard_metrics():
    """Return all 6 dashboard widget data in one JSON call."""
    from dashboard.metrics import get_dashboard_metrics
    uid = int(session["user_id"])
    days = int(request.args.get("days", 30))
    return jsonify(get_dashboard_metrics(uid, days))
