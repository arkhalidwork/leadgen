"""
Health check endpoints — Blueprint module.

Routes: /health, /health/ops
Used by DigitalOcean App Platform / load balancers.
"""
from __future__ import annotations

from datetime import datetime
import logging

from flask import Blueprint, jsonify

from core import OPS_METRICS_DEFAULT_WINDOW_HOURS

log = logging.getLogger(__name__)

health_bp = Blueprint("health", __name__)


@health_bp.route("/health")
def health_check():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()}), 200


@health_bp.route("/health/ops")
def ops_health_check():
    alerts = []
    try:
        import app as main_app
        alerts = main_app._ops_alerts(0, OPS_METRICS_DEFAULT_WINDOW_HOURS)
    except Exception:
        alerts = []

    has_critical = any(str(a.get("severity") or "").lower() == "critical" for a in alerts)
    status = "unhealthy" if has_critical else ("degraded" if alerts else "healthy")
    code = 503 if has_critical else 200
    return jsonify({"status": status, "alerts_count": len(alerts), "timestamp": datetime.now().isoformat()}), code
