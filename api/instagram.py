"""
Instagram Scraping API — Blueprint module.

Routes: /api/instagram/scrape, /api/instagram/status, /api/instagram/results,
        /api/instagram/download, /api/instagram/stop
"""
from __future__ import annotations

import io
import csv
import uuid
import threading
import logging

from flask import Blueprint, request, session, jsonify, Response

from core import instagram_jobs, IG_TYPE_MAP
from core.auth import subscription_required

log = logging.getLogger(__name__)

instagram_bp = Blueprint("instagram", __name__)


def _get_app_helpers():
    """Lazy import to avoid circular dependency with app.py during transition."""
    import app as main_app
    return {
        "QUEUE_ENABLED": main_app.QUEUE_ENABLED,
        "TOOL_CONFIG": main_app.TOOL_CONFIG,
        "_assign_execution_mode": main_app._assign_execution_mode,
        "_create_queue_job": main_app._create_queue_job,
        "_update_queue_job": main_app._update_queue_job,
        "_enqueue_redis_job": main_app._enqueue_redis_job,
        "_insert_history_direct": main_app._insert_history_direct,
        "_get_queue_job": main_app._get_queue_job,
        "_queue_job_to_status": main_app._queue_job_to_status,
        "_queue_job_results_response": main_app._queue_job_results_response,
        "_set_redis_stop": main_app._set_redis_stop,
        "InstagramJob": main_app.InstagramJob,
        "run_instagram_job": main_app.run_instagram_job,
        "clean_instagram_leads": main_app.clean_instagram_leads,
    }


@instagram_bp.route("/api/instagram/scrape", methods=["POST"])
@subscription_required
def instagram_start_scrape():
    """Start a new Instagram scraping job."""
    h = _get_app_helpers()
    data = request.get_json()
    keywords = data.get("keywords", "").strip()
    place = data.get("place", "").strip()
    search_type = data.get("search_type", "emails").strip()

    if not place:
        return jsonify({"error": "Location is required."}), 400
    search_type = IG_TYPE_MAP.get(search_type, search_type)
    if search_type not in ("profiles", "businesses"):
        return jsonify({"error": "search_type must be 'profiles' or 'businesses'."}), 400

    # Phase 2+3: Queue-based path
    if h["QUEUE_ENABLED"].get("instagram"):
        exec_mode, agent_id = "cloud", ""
        try:
            exec_mode, agent_id = h["_assign_execution_mode"](
                session["user_id"], "instagram", "auto"
            )
        except ValueError:
            pass

        job_id = str(uuid.uuid4())[:8]
        h["_create_queue_job"](job_id, session["user_id"], "instagram", payload={
            "keywords": keywords, "place": place, "search_type": search_type,
        }, max_attempts=h["TOOL_CONFIG"]["instagram"]["max_attempts"])

        if exec_mode == "local" and agent_id:
            h["_update_queue_job"](job_id, {
                "status": "assigned_to_agent",
                "execution_mode": "local",
                "agent_id": agent_id,
            })
        else:
            h["_update_queue_job"](job_id, {"execution_mode": "cloud"})
            h["_enqueue_redis_job"](job_id, "instagram")

        h["_insert_history_direct"](session["user_id"], job_id, "instagram", keywords, place, search_type)
        return jsonify({
            "job_id": job_id,
            "execution_mode": exec_mode,
            "message": "Instagram scraping started.",
        }), 202

    # Legacy: Thread-based path
    job = h["InstagramJob"](keywords, place, search_type)
    instagram_jobs[job.id] = job
    h["_insert_history_direct"](session["user_id"], job.id, "instagram", keywords, place, search_type)

    thread = threading.Thread(target=h["run_instagram_job"], args=(job,), daemon=True)
    thread.start()

    return jsonify({"job_id": job.id, "message": "Instagram scraping started."}), 202


@instagram_bp.route("/api/instagram/status/<job_id>")
def instagram_status(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "instagram":
        return jsonify(h["_queue_job_to_status"](qjob, "instagram"))

    job = instagram_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job.to_dict())


@instagram_bp.route("/api/instagram/results/<job_id>")
def instagram_results(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "instagram":
        return h["_queue_job_results_response"](qjob)

    job = instagram_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped"):
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@instagram_bp.route("/api/instagram/download/<job_id>")
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


@instagram_bp.route("/api/instagram/stop/<job_id>", methods=["POST"])
def instagram_stop(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "instagram":
        h["_set_redis_stop"](job_id)
        return jsonify({"message": "Stop signal sent."})

    job = instagram_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.scraper:
        job.scraper.stop()
        partial = job.scraper.get_partial_leads()
        if partial:
            cleaned = h["clean_instagram_leads"](partial, job.search_type)
            job.leads = cleaned
    job.status = "stopped"
    job.message = f"Stopped by user. Saved {len(job.leads)} {job.search_type}."
    return jsonify({"message": f"Job stopped. {len(job.leads)} leads saved."})
