"""
LeadGen - Lead Generation Suite
Flask backend with multi-tool scraping API and CSV export.
"""

import os
import csv
import io
import uuid
import threading
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response
from flask_cors import CORS
from scraper import GoogleMapsScraper, clean_leads
from linkedin_scraper import LinkedInScraper, clean_linkedin_leads
from instagram_scraper import InstagramScraper, clean_instagram_leads
from web_crawler import WebCrawlerScraper, clean_web_leads

app = Flask(__name__)
CORS(app)

# Store active scraping jobs and their results
scraping_jobs = {}          # Google Maps jobs
linkedin_jobs = {}          # LinkedIn jobs
instagram_jobs = {}         # Instagram jobs
webcrawler_jobs = {}        # Web Crawler jobs
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output")
os.makedirs(OUTPUT_DIR, exist_ok=True)


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

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
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

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
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

    def update_progress(self, message: str, percentage: int):
        self.message = message
        if percentage >= 0:
            self.progress = percentage

    def to_dict(self):
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
            return

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} leads."

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


def run_linkedin_job(job: LinkedInJob):
    """Run LinkedIn scraping in a background thread."""
    try:
        scraper = LinkedInScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw = scraper.scrape(job.niche, job.place, search_type=job.search_type)
        cleaned = clean_linkedin_leads(raw, job.search_type)
        job.leads = cleaned

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} {job.search_type}."

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.message = f"Error: {str(e)}"


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

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} Instagram {job.search_type}."

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.message = f"Error: {str(e)}"


def run_webcrawler_job(job: WebCrawlerJob):
    """Run Web Crawler scraping in a background thread."""
    try:
        scraper = WebCrawlerScraper(headless=True)
        job.scraper = scraper
        scraper.set_progress_callback(job.update_progress)

        raw = scraper.scrape(job.keyword, job.place)
        cleaned = clean_web_leads(raw)
        job.leads = cleaned

        job.status = "completed"
        job.progress = 100
        job.message = f"Done! Found {len(cleaned)} leads from the web."

    except Exception as e:
        job.status = "failed"
        job.error = str(e)
        job.message = f"Error: {str(e)}"


# ============================================================
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
def dashboard():
    """Dashboard landing page."""
    return render_template("dashboard.html", active_page="dashboard")


@app.route("/tools/google-maps")
def google_maps_tool():
    """Google Maps scraper page."""
    return render_template("gmaps.html", active_page="gmaps")


@app.route("/tools/linkedin")
def linkedin_tool():
    """LinkedIn scraper page."""
    return render_template("linkedin.html", active_page="linkedin")


@app.route("/tools/instagram")
def instagram_tool():
    """Instagram scraper page."""
    return render_template("instagram.html", active_page="instagram")


@app.route("/tools/web-crawler")
def webcrawler_tool():
    """Web Crawler page."""
    return render_template("webcrawler.html", active_page="webcrawler")


# ============================================================
# Google Maps API
# ============================================================

@app.route("/api/scrape", methods=["POST"])
def start_scrape():
    """Start a new Google Maps scraping job."""
    data = request.get_json()
    keyword = data.get("keyword", "").strip()
    place = data.get("place", "").strip()

    if not keyword or not place:
        return jsonify({"error": "Both keyword and place are required."}), 400

    job = ScrapingJob(keyword, place)
    scraping_jobs[job.id] = job

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
    if job.status != "completed":
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@app.route("/api/linkedin/download/<job_id>")
def linkedin_download(job_id):
    job = linkedin_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status != "completed" or not job.leads:
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
    job.status = "stopped"
    job.message = "Scraping stopped by user."
    return jsonify({"message": "Job stop requested."})


# ============================================================
# Instagram API
# ============================================================

@app.route("/api/instagram/scrape", methods=["POST"])
def instagram_start_scrape():
    """Start a new Instagram scraping job."""
    data = request.get_json()
    keywords = data.get("keywords", "").strip()
    place = data.get("place", "").strip()
    search_type = data.get("search_type", "emails").strip()

    if not place:
        return jsonify({"error": "Location is required."}), 400
    if search_type not in ("emails", "profiles"):
        return jsonify({"error": "search_type must be 'emails' or 'profiles'."}), 400

    job = InstagramJob(keywords, place, search_type)
    instagram_jobs[job.id] = job

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
    if job.status != "completed":
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@app.route("/api/instagram/download/<job_id>")
def instagram_download(job_id):
    job = instagram_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status != "completed" or not job.leads:
        return jsonify({"error": "No data available for download."}), 400

    output = io.StringIO()

    if job.search_type == "emails":
        fieldnames = ["Username", "Display Name", "Email", "Location", "Profile URL", "Bio"]
        key_map = {
            "Username": "username", "Display Name": "display_name",
            "Email": "email", "Location": "location",
            "Profile URL": "profile_url", "Bio": "bio_snippet",
        }
    else:
        fieldnames = [
            "Username", "Display Name", "Title", "Company",
            "Company URL", "Location", "Profile URL", "Bio",
        ]
        key_map = {
            "Username": "username", "Display Name": "display_name",
            "Title": "title", "Company": "company",
            "Company URL": "company_url", "Location": "location",
            "Profile URL": "profile_url", "Bio": "bio_snippet",
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
    job.status = "stopped"
    job.message = "Scraping stopped by user."
    return jsonify({"message": "Job stop requested."})


# ============================================================
# Web Crawler API
# ============================================================

@app.route("/api/webcrawler/scrape", methods=["POST"])
def webcrawler_start_scrape():
    """Start a new Web Crawler scraping job."""
    data = request.get_json()
    keyword = data.get("keyword", "").strip()
    place = data.get("place", "").strip()

    if not keyword or not place:
        return jsonify({"error": "Both keyword and place are required."}), 400

    job = WebCrawlerJob(keyword, place)
    webcrawler_jobs[job.id] = job

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
    if job.status != "completed":
        return jsonify({"error": "Job not completed yet.", "status": job.status}), 400
    return jsonify({"leads": job.leads, "total": len(job.leads), "job": job.to_dict()})


@app.route("/api/webcrawler/download/<job_id>")
def webcrawler_download(job_id):
    job = webcrawler_jobs.get(job_id)
    if not job:
        return jsonify({"error": "Job not found."}), 404
    if job.status != "completed" or not job.leads:
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
    job.status = "stopped"
    job.message = "Scraping stopped by user."
    return jsonify({"message": "Job stop requested."})


if __name__ == "__main__":
    app.run(debug=True, port=5000)
