"""
Web Crawler Scraping API — Blueprint module.

Routes: /api/webcrawler/scrape, /api/webcrawler/status, /api/webcrawler/results,
        /api/webcrawler/download, /api/webcrawler/stop
"""
from __future__ import annotations

import io
import csv
import uuid
import threading
import logging

from flask import Blueprint, request, session, jsonify, Response

from core import webcrawler_jobs
from core.auth import subscription_required

log = logging.getLogger(__name__)

webcrawler_bp = Blueprint("webcrawler", __name__)


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
        "WebCrawlerJob": main_app.WebCrawlerJob,
        "run_webcrawler_job": main_app.run_webcrawler_job,
        "clean_web_leads": main_app.clean_web_leads,
    }


@webcrawler_bp.route("/api/webcrawler/scrape", methods=["POST"])
@subscription_required
def webcrawler_start_scrape():
    """Start a new Web Crawler scraping job."""
    h = _get_app_helpers()
    data = request.get_json()
    keyword = data.get("keyword", "").strip()
    place = data.get("place", "").strip()

    if not keyword or not place:
        return jsonify({"error": "Both keyword and place are required."}), 400

    # Phase 2+3: Queue-based path
    if h["QUEUE_ENABLED"].get("webcrawler"):
        exec_mode, agent_id = "cloud", ""
        try:
            exec_mode, agent_id = h["_assign_execution_mode"](
                session["user_id"], "webcrawler", "auto"
            )
        except ValueError:
            pass

        job_id = str(uuid.uuid4())[:8]
        h["_create_queue_job"](job_id, session["user_id"], "webcrawler", payload={
            "keyword": keyword, "place": place,
        }, max_attempts=h["TOOL_CONFIG"]["webcrawler"]["max_attempts"])

        if exec_mode == "local" and agent_id:
            h["_update_queue_job"](job_id, {
                "status": "assigned_to_agent",
                "execution_mode": "local",
                "agent_id": agent_id,
            })
        else:
            h["_update_queue_job"](job_id, {"execution_mode": "cloud"})
            h["_enqueue_redis_job"](job_id, "webcrawler")

        h["_insert_history_direct"](session["user_id"], job_id, "webcrawler", keyword, place)
        return jsonify({
            "job_id": job_id,
            "execution_mode": exec_mode,
            "message": "Web crawling started.",
        }), 202

    # Legacy: Thread-based path
    job = h["WebCrawlerJob"](keyword, place)
    webcrawler_jobs[job.id] = job
    h["_insert_history_direct"](session["user_id"], job.id, "webcrawler", keyword, place)

    thread = threading.Thread(target=h["run_webcrawler_job"], args=(job,), daemon=True)
    thread.start()

    return jsonify({"job_id": job.id, "message": "Web crawling started."}), 202


@webcrawler_bp.route("/api/webcrawler/status/<job_id>")
def webcrawler_status(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "webcrawler":
        return jsonify(h["_queue_job_to_status"](qjob, "webcrawler"))

    job = webcrawler_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    return jsonify(job.to_dict())


@webcrawler_bp.route("/api/webcrawler/results/<job_id>")
def webcrawler_results(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "webcrawler":
        return h["_queue_job_results_response"](qjob)

    job = webcrawler_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status not in ("completed", "stopped"):
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@webcrawler_bp.route("/api/webcrawler/download/<job_id>")
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


@webcrawler_bp.route("/api/webcrawler/stop/<job_id>", methods=["POST"])
def webcrawler_stop(job_id):
    h = _get_app_helpers()
    qjob = h["_get_queue_job"](job_id)
    if qjob and qjob.get("type") == "webcrawler":
        h["_set_redis_stop"](job_id)
        return jsonify({"message": "Stop signal sent."})

    job = webcrawler_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.scraper:
        job.scraper.stop()
        partial = job.scraper.get_partial_leads()
        if partial:
            cleaned = h["clean_web_leads"](partial)
            job.leads = cleaned
    job.status = "stopped"
    job.message = f"Stopped by user. Saved {len(job.leads)} leads."
    return jsonify({"message": f"Job stopped. {len(job.leads)} leads saved."})
