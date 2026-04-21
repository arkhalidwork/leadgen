"""
LinkedIn Scraping API — Blueprint module.

Routes: /api/linkedin/scrape, /api/linkedin/status, /api/linkedin/results,
        /api/linkedin/download, /api/linkedin/stop
"""
from __future__ import annotations

import io
import csv
import uuid
import threading
import logging

from flask import Blueprint, request, session, jsonify, Response

from core import linkedin_jobs, IG_TYPE_MAP
from core.auth import subscription_required

log = logging.getLogger(__name__)

linkedin_bp = Blueprint("linkedin", __name__)


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
        "LinkedInJob": main_app.LinkedInJob,
        "run_linkedin_job": main_app.run_linkedin_job,
        "clean_linkedin_leads": main_app.clean_linkedin_leads,
    }


@linkedin_bp.route("/api/linkedin/scrape", methods=["POST"])
@subscription_required
def linkedin_start_scrape():
    """Start a new LinkedIn scraping job."""
    h = _get_app_helpers()
    data = request.get_json()
    niche = data.get("niche", "").strip()
    place = data.get("place", "").strip()
    search_type = data.get("search_type", "profiles").strip()

    if not niche or not place:
        return jsonify({"error": "Both niche and place are required."}), 400
    if search_type not in ("profiles", "companies"):
        return jsonify({"error": "search_type must be 'profiles' or 'companies'."}), 400

    # Phase 2+3: Queue-based path
    if h["QUEUE_ENABLED"].get("linkedin"):
        exec_mode, agent_id = "cloud", ""
        try:
            exec_mode, agent_id = h["_assign_execution_mode"](
                session["user_id"], "linkedin",
                request.get_json(force=True, silent=True) or {}
                    .get("execution_mode", "auto")
            )
        except ValueError:
            pass

        job_id = str(uuid.uuid4())[:8]
        h["_create_queue_job"](job_id, session["user_id"], "linkedin", payload={
            "niche": niche, "place": place, "search_type": search_type,
        }, max_attempts=h["TOOL_CONFIG"]["linkedin"]["max_attempts"])

        if exec_mode == "local" and agent_id:
            h["_update_queue_job"](job_id, {
                "status": "assigned_to_agent",
                "execution_mode": "local",
                "agent_id": agent_id,
            })
        else:
            h["_update_queue_job"](job_id, {"execution_mode": "cloud"})
            h["_enqueue_redis_job"](job_id, "linkedin")

        h["_insert_history_direct"](session["user_id"], job_id, "linkedin", niche, place, search_type)
        return jsonify({
            "job_id": job_id,
            "execution_mode": exec_mode,
            "message": "LinkedIn scraping started.",
        }), 202

    # Legacy: Thread-based path
    job = h["LinkedInJob"](niche, place, search_type)
    linkedin_jobs[job.id] = job
    h["_insert_history_direct"](session["user_id"], job.id, "linkedin", niche, place, search_type)

    thread = threading.Thread(target=h["run_linkedin_job"], args=(job,), daemon=True)
    thread.start()

    return jsonify({"job_id": job.id, "message": "LinkedIn scraping started."}), 202


@linkedin_bp.route("/api/linkedin/status/<job_id>")
def linkedin_status(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "linkedin":
        return jsonify(h["_queue_job_to_status"](qjob, "linkedin"))

    job = linkedin_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job.to_dict())


@linkedin_bp.route("/api/linkedin/results/<job_id>")
def linkedin_results(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "linkedin":
        return h["_queue_job_results_response"](qjob)

    job = linkedin_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped"):
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@linkedin_bp.route("/api/linkedin/download/<job_id>")
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


@linkedin_bp.route("/api/linkedin/stop/<job_id>", methods=["POST"])
def linkedin_stop(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "linkedin":
        h["_set_redis_stop"](job_id)
        return jsonify({"message": "Stop signal sent."})

    job = linkedin_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.scraper:
        job.scraper.stop()
        partial = job.scraper.get_partial_leads()
        if partial:
            cleaned = h["clean_linkedin_leads"](partial, job.search_type)
            job.leads = cleaned
    job.status = "stopped"
    job.message = f"Stopped by user. Saved {len(job.leads)} {job.search_type}."
    return jsonify({"message": f"Job stopped. {len(job.leads)} leads saved."})
