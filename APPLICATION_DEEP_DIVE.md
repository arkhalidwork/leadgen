# LeadGen Complete Application Deep Dive

This document explains the full product from UI to backend internals, including each tool, major button/action behavior, and the exact code paths that implement crawling, orchestration, persistence, and exports.

## 1) Product Architecture

LeadGen is a Flask application that combines:

- Multi-tool scraping engines (Google Maps, LinkedIn, Instagram, Web Crawler)
- Persistent session/task/event tracking for Google Maps phase-based workflows
- Unified lead storage and analytics in SQLite (with optional PostgreSQL mirroring)
- Account/auth/license management + optional Stripe checkout/webhook provisioning
- Frontend pages built with Jinja templates and tool-specific JavaScript controllers

Primary backend entrypoint: `app.py`.

Core architecture layers:

1. **UI layer**
   - Jinja templates in `templates/`
   - Browser logic in `static/js/`

2. **API layer**
   - Flask routes for auth/account/dashboard/tools/database/session ops

3. **Execution layer**
   - Google Maps: queue-backed worker pools (`task_queue/dispatcher.py`) + durable state/events/tasks
   - Other tools: background threads with in-memory job objects

4. **Persistence layer**
   - SQLite tables initialized in `init_db()`
   - Optional Postgres mirror in `task_queue/postgres_mirror.py`

5. **Scraper engines**
   - Google Maps Selenium engine: `scraper.py`
   - LinkedIn via Google/Bing SERP scraping: `linkedin_scraper.py`
   - Instagram multi-engine scraper: `instagram_scraper.py`
   - HTTP-first website crawler: `web_crawler.py`

## 2) Navigation and Main Pages

Global navigation is defined in `templates/base.html`.

Sidebar sections:

- Main: Dashboard
- Tools: Google Maps, LinkedIn, Instagram, Web Crawler, Email Outreach
- Data: My Database, Sessions
- Account: Settings

`base.html` also:

- Fetches user identity via `/api/auth/me`
- Shows active/inactive badge
- Provides mobile sidebar open/close behavior

## 3) Authentication, Activation, Billing

### 3.1 Register/Login/Activate flow

Pages:

- `templates/register.html`
- `templates/login.html`
- `templates/activate.html`

APIs:

- `POST /api/auth/register`
- `POST /api/auth/login`
- `POST /api/auth/activate`
- `GET /api/auth/me`
- `GET /logout`

Behavior summary:

- Register validates email + password strength, creates user, starts session.
- Login validates credentials (with legacy hash upgrade path), regenerates session.
- Activate validates license key usage/expiry and flips `users.is_active=1`.
- Most tool routes require `@subscription_required` (authenticated + active).

### 3.2 Stripe purchase integration

- `POST /api/stripe/create-checkout` creates checkout session.
- `POST /api/stripe/webhook` handles payment success, auto-provisions license with `_provision_license_for_email()`.

### 3.3 Auth/Landing button and action map

Landing page (`templates/landing.html`):

- **Sign In** button → `/login`
- **Get Started** button(s) → `/register`
- **Learn More** button → anchor scroll to `#features`

Login page (`templates/login.html`):

- **Sign In** submits `POST /api/auth/login`
- On success:
  - active users → `/`
  - inactive users → `/activate`
- **Create one** link → `/register`

Register page (`templates/register.html`):

- **Create Account** submits `POST /api/auth/register`
- On success redirects to `/activate`
- **Sign In** link → `/login`

Activate page (`templates/activate.html`):

- **Activate** submits `POST /api/auth/activate`
- On success redirects to `/`
- **Sign out** link → `/logout`

## 4) Dashboard (Analytics + History)

Files:

- UI: `templates/dashboard.html`
- JS: `static/js/dashboard.js`
- APIs: `/api/dashboard/stats`, `/api/dashboard/history`

User-visible functionality:

- Stat cards: total leads, strong/medium/weak quality counts
- Doughnut chart: quality distribution
- Line chart: last 7 days trend
- Tool breakdown table (scrapes + leads by tool)
- Recent scrape history table (click row opens filtered database view)
- Quick Launch buttons for all tools

## 5) Google Maps Tool (Most Advanced Workflow)

Files:

- UI: `templates/gmaps.html`
- JS controller: `static/js/app.js`
- APIs in `app.py`: `/api/scrape`, `/api/status/<job_id>`, `/api/results/<job_id>`, `/api/download/<job_id>`, `/api/stop/<job_id>`, contact/session/ops endpoints
- Worker entry: `workers/scraper_worker.py` (`run_scraper_job`)
- Scraper engine: `scraper.py` (`GoogleMapsScraper`)
- Queue + state infra: `task_queue/job_store.py`, `task_queue/dispatcher.py`

### 5.1 Every major control/button and action mapping

From `templates/gmaps.html` + `static/js/app.js`:

1. **Go** (`btnScrape`)
   - Sends `POST /api/scrape` with keyword/place/map_selection/max_leads.
   - Backend creates session state, persists it, inserts scrape history, and submits extract job to extract pool.

2. **Lead Limit slider** (`leadLimitRange`)
   - Controls `max_leads` sent to `/api/scrape`.

3. **Drag Select Area** (`btnUseMapSelection`)
   - Enables Leaflet rectangle draw mode.
   - Drawn bounds become `map_selection` payload.

4. **Clear map selection** (`btnClearMapSelection`)
   - Clears rectangle and selected bounds.

5. **Stop** (`btnStop`)
   - Calls `POST /api/stop/<job_id>`.
   - If phase is extract: sets stop_requested.
   - If phase is contacts: marks paused/contact_stop_requested and checkpoints partial snapshot.

6. **Start Contact Retrieval** (`btnStartContacts`)
   - Calls `POST /api/gmaps/contacts/start/<job_id>`.
   - Moves session to contacts phase and submits contact job to contact pool.

7. **Download CSV** (`btnDownload`)
   - Fetches `GET /api/download/<job_id>` and downloads generated CSV.

8. **Filter input** (`filterInput`)
   - Client-side filter over in-memory result list.

9. **Column header sorting**
   - Client-side sort on selected columns.

10. **Realtime logs**

- Rendered from status payload `logs` and refreshed during polling.

### 5.2 Step-by-step extraction pipeline (how crawling is achieved in code)

Google Maps extraction pipeline lives mainly in `scraper.py` and is executed via `workers/scraper_worker.py` + `_run_scrape_in_thread()` in `app.py`.

#### Step A: Session bootstrap

- `/api/scrape` creates `job_id`, initial state, history row, and persists state via `_save_job_state_and_persist`.
- `submit_extract_job()` places work into extract pool with backpressure checks.

#### Step B: Worker starts

- `_run_scrape_in_thread(job_id, payload)`:
  - marks task running (`extract_main`)
  - emits durable events (`extract_started`)
  - invokes `run_scraper_job(payload, progress_callback, should_stop)`.

#### Step C: Spatial strategy and search coverage

Inside `GoogleMapsScraper.scrape(...)`:

1. **Keyword expansion**
   - Uses `expand_keywords()` from `utils/keyword_expander.py`.

2. **Geospatial partitioning**
   - Uses `geo/quadtree.py` to build bounding boxes/cells.
   - Supports map-drawn bounds and coordinate/place resolution.

3. **Viewport search per cell**
   - `_search_maps_viewport(query, bbox)` navigates to map URL anchored by cell center + computed zoom.

4. **Smart result loading**
   - `_scroll_results()` loads map feed with stagnation/end detection.

5. **Listing link extraction + URL-slug dedup**
   - Collects unique listing URLs across all cells/queries.

#### Step D: Detail extraction

- Iterates unique listing URLs and calls `_extract_business_detail(url)`.
- Builds `BusinessLead` entries with business fields (name/phone/site/address/rating/reviews/category/coords/social placeholders).

#### Step E: Optional contact enrichment

- Contacts phase (or explicit contact crawl path) uses `_crawl_websites_for_leads()` / `crawl_contacts_for_leads()`.
- It scans websites in parallel `ThreadPoolExecutor` workers using `_scrape_website()`.
- Checks homepage + contact/about-like pages and extracts:
  - emails (`mailto:` + regex)
  - phones (`tel:` + regex)
  - social links (pattern matching)

#### Step F: Finalization and persistence

- `clean_leads()` normalizes rows and computes `lead_uid`.
- `_run_scrape_in_thread` writes final/partial state with logs/events/tasks.
- `_persist_gmaps_state` syncs session snapshots + leads into:
  - `gmaps_sessions`, `gmaps_session_leads`, `gmaps_session_logs`
  - mirror records in `scrape_history` and `leads` tables
- On stop/failure/partial paths, partial checkpoint events are persisted.

### 5.3 Why the Google Maps workflow is resilient

- Queue backpressure by pool size and per-user active limits.
- Durable checkpoints/events/tasks in DB.
- Resume-anchor logic (`_select_resume_anchor`) to suggest deterministic retry path.
- Task retry guards (attempt limits + cooldown) and operator force overrides.
- Automatic stale-task sweeper and auto-recovery hooks.
- Retention cleanup for events/logs/tasks.

## 6) Sessions Page (Operations + Recovery Console)

Files:

- UI: `templates/sessions.html`
- JS: `static/js/sessions.js`
- APIs: `/api/gmaps/sessions*`, `/api/gmaps/ops/*`, retention/diagnostics/retry/action endpoints

### 6.1 Key controls and exact actions

Top controls:

- **Refresh** (`btnRefreshSessions`): reloads sessions table.
- **Refresh Ops** (`btnRefreshOps`): reloads ops dashboard health + alerts.

Row action icons per session:

- **Start contacts**: `/api/gmaps/contacts/start/<id>`
- **Pause**: contacts phase → `/contacts/pause`; extract phase → `/extract/pause`
- **Resume**: contacts phase → `/contacts/resume`; extract phase → `/extract/restart`
- **Restart**: contacts phase → `/contacts/restart`; extract phase → `/extract/restart`

Session detail controls:

- **Completion filter**: changes `completion` query for `/sessions/<id>/leads`.
- **Apply Suggested Recovery**: `/sessions/<id>/retry-from-anchor`.
- **Force Suggested Retry**: `/sessions/<id>/retry-task` with `force=true`.
- **Recover Stale Tasks**: `/sessions/<id>/recover-stale`.
- **Auto Recover Session**: `/sessions/<id>/recover-auto`.
- **Task timeline actions** (operator only): `/sessions/<id>/task-action` with `retry`, `force_retry`, `reset_attempts`.
- **Audit scope + export**:
  - feed: `/sessions/<id>/audit-events`
  - CSV: `/sessions/<id>/audit-report.csv`
- **Retention admin** (operator only):
  - status: `/api/gmaps/retention/status`
  - archive export: `/api/gmaps/retention/archive.csv`

### 6.2 Auto-refresh model

`sessions.js` runs a periodic refresh loop (~3s) for:

- ops dashboard
- session list
- selected session details/diagnostics

## 7) LinkedIn Tool

Files:

- UI: `templates/linkedin.html`
- JS: `static/js/linkedin.js`
- Backend APIs: `/api/linkedin/*`
- Engine: `linkedin_scraper.py`

### 7.1 Controls and actions

- **Go** submits `/api/linkedin/scrape` with niche/place/search_type.
- **Stop** calls `/api/linkedin/stop/<job_id>`.
- **Download CSV** calls `/api/linkedin/download/<job_id>`.
- **Filter** is client-side table search.

### 7.2 Crawling methodology

LinkedIn is not scraped directly from LinkedIn pages in bulk. Instead:

1. Build query sets (profiles/companies).
2. Search via Google SERPs (multiple parser fallbacks).
3. Search via Bing SERPs.
4. Parse result cards into profile/company models.
5. Classify profile seniority from title.
6. Deduplicate and normalize with `clean_linkedin_leads`.

## 8) Instagram Tool

Files:

- UI: `templates/instagram.html`
- JS: `static/js/instagram.js`
- Backend APIs: `/api/instagram/*`
- Engine: `instagram_scraper.py`

### 8.1 Controls and actions

- **Mode selector** (Profile Search / Business Search).
- **Go** → `/api/instagram/scrape`.
- **Stop** → `/api/instagram/stop/<job_id>`.
- **Download CSV** → `/api/instagram/download/<job_id>`.
- **Filter** is client-side.

### 8.2 Crawling methodology

Instagram pipeline is multi-engine:

1. DuckDuckGo text search (primary, fast).
2. Google SERP search via Selenium.
3. Bing SERP search via Selenium.
4. Username extraction + dedup cap.
5. Parse leads from SERP snippets.
6. Optional profile enrichment by fetching profile pages (bio/email/phone/website/category/followers/verified/post count).
7. Normalize in `clean_instagram_leads`.

## 9) Web Crawler Tool

Files:

- UI: `templates/webcrawler.html`
- JS: `static/js/webcrawler.js`
- Backend APIs: `/api/webcrawler/*`
- Engine: `web_crawler.py`

### 9.1 Controls and actions

- **Go** → `/api/webcrawler/scrape`
- **Stop** → `/api/webcrawler/stop/<job_id>`
- **Download CSV** → `/api/webcrawler/download/<job_id>`
- **Filter** is client-side

### 9.2 Crawling methodology

This tool is HTTP-first (no browser required):

1. Build diversified query list across Google/Bing/DDG.
2. Fetch SERPs via requests and parse result URLs/snippets.
3. Extract quick snippet leads (emails/phones from snippets).
4. Deduplicate domains.
5. Parallel deep scrape websites (`ThreadPoolExecutor`), scanning homepage + contact/about pages.
6. Extract emails, phones, socials, structured-data/address hints.
7. Merge deep + snippet leads and normalize with `clean_web_leads`.

## 10) Email Outreach Tool (4-Step Wizard)

Files:

- UI: `templates/email_outreach.html`
- JS: `static/js/email_outreach.js`
- APIs: `/api/email-outreach/scan-website`, `/api/email-outreach/generate`, `/api/email-outreach/templates`

### 10.1 Step-by-step UX and actions

Step 1 — Your Business:

- **Scan Website & Continue** (`btnScanAndNext`)
  - Saves sender info to localStorage.
  - Optionally calls `/api/email-outreach/scan-website`.
  - Shows detected company/services/summary.

Step 2 — Select Leads:

- **Recent Scrapes** source:
  - loads `/api/dashboard/history`, then `/api/leads?scrape_id=...`
- **From Database** source:
  - filter lists from `/api/leads/filters`
  - loads leads from `/api/leads?...`
- **Upload CSV** source:
  - parses client-side CSV and maps common columns to lead fields
- **Continue to Generate** (`btnToGenerate`) starts generation.

Step 3 — Generate:

- Batches selected leads (size 10).
- Calls `/api/email-outreach/generate` per batch.
- Tracks progress bar + message.

Step 4 — Templates:

- Card list of generated templates.
- **View** opens modal.
- **Copy to Clipboard** copies subject+body.
- **Export All** downloads templates CSV client-side.

### 10.2 Template generation method

Backend uses rule-based personalization in `_build_email_template(...)`:

- outreach-type-specific subject pools and body variants
- sender context + scanned services/description
- lead personalization tokens (business/location/keyword/contact)
- first-contact best guess from lead metadata

## 11) My Database Page

Files:

- UI: `templates/database.html`
- JS: `static/js/database.js`
- APIs: `/api/leads`, `/api/leads/filters`, `/api/leads/stats`, `/api/leads/export`, `/api/leads/<id>`, `/api/leads/bulk-delete`, `/api/leads/cleanup`

### 11.1 Controls and actions

- **Search + Filter** (`dbApplyFilters`) reloads paginated leads query.
- **Magic Cleanup** (`dbCleanupBtn`) prompts mode (`duplicates`, `outliers`, `both`) and posts `/api/leads/cleanup`.
- **Export** (`dbExportBtn`) navigates to `/api/leads/export?...`.
- **Select all / row selection** drives bulk bar.
- **Delete Selected** posts `/api/leads/bulk-delete`.
- **Per-row delete** calls `DELETE /api/leads/<id>`.
- **Title click** opens lead detail modal.
- **Prev/Next** paginates results.

## 12) Settings Page

Files:

- UI + inline JS: `templates/settings.html`
- APIs: `/api/auth/me`, `/api/account/profile`, `/api/account/password`, `/api/account/delete`

Controls:

- **Save Changes** updates profile (name/email).
- **Change Password** validates confirmation and updates password.
- **Delete Account** requires double confirm, then deletes account + related data and redirects to login.

## 13) Data Model and Persistence Strategy

Database schema is initialized in `init_db()` in `app.py`.

Major tables:

- `users`, `license_keys`
- `scrape_history` (session-level historical analytics)
- `leads` (normalized lead rows for cross-tool browsing)
- `email_templates`
- Google Maps durable orchestration tables:
  - `gmaps_sessions`
  - `gmaps_session_leads`
  - `gmaps_session_logs`
  - `gmaps_session_events`
  - `gmaps_session_tasks`
  - `gmaps_task_chunks`

Design pattern:

- Google Maps uses **durable state machine + task timeline + event sourcing style logs**.
- Other tools use in-memory job objects during run, then persist into `scrape_history` + `leads` on completion.

## 14) Queueing, Backpressure, and Execution Control

Google Maps execution control is in `task_queue/dispatcher.py`:

- Separate pools for extraction and contacts.
- Configurable worker count, max pending queue, per-user active limits.
- Queue rejection reasons returned to API (`queue_full`, `user_active_quota_reached`, etc.).
- Pending TTL pruning to avoid stale queue buildup.

Job state store in `task_queue/job_store.py`:

- thread-safe in-memory state map
- stop_requested signaling
- TTL cleanup for completed/failed/partial jobs

## 15) Observability and Reliability Features

Implemented mostly in `app.py`:

- Structured event logging via `_structured_log`
- Session events persisted via `_persist_gmaps_event`
- Status/phase/progress checkpoint events
- Task health evaluation (`_task_health`)
- Resume anchor calculation (`_select_resume_anchor`)
- Retry guard rails (`_enforce_task_retry_guard`)
- Manual and automatic stale task recovery
- Retention cleanup with configurable windows
- Ops endpoints:
  - `/api/gmaps/ops/metrics`
  - `/api/gmaps/ops/alerts`
  - `/api/gmaps/ops/health`
  - `/api/gmaps/ops/dashboard`
  - `/health` and `/health/ops`

## 16) Security and Access Control

Implemented in `app.py`:

- Session cookie hardening (`HttpOnly`, `SameSite`, secure in production)
- CSRF handling for non-API form routes
- Rate limiting via Flask-Limiter
- Password hashing via bcrypt (+ legacy migration)
- Role-like operator controls via allowlist for force-retry/admin actions
- Security headers via `@app.after_request`

## 17) End-to-End Workflow Examples

### 17.1 Google Maps full run

1. User submits query in Google Maps page.
2. Frontend calls `/api/scrape`.
3. Backend creates durable session + history and enqueues extract task.
4. Worker runs map partition/search/detail extraction.
5. Polling endpoint `/api/status/<id>` streams progress/logs/stats.
6. `/api/results/<id>` returns partial/final leads.
7. User optionally starts contacts phase.
8. Contacts chunk pipeline enriches websites in parallel.
9. Session ends completed/partial/failed with persisted events/tasks/logs.
10. Leads are available in Sessions view, Database view, and CSV download.

### 17.2 Email outreach generation

1. User enters sender details and optional website.
2. Site scan enriches sender context.
3. User selects lead source and checks desired rows.
4. Generator batches requests to `/api/email-outreach/generate`.
5. Backend builds personalized templates and persists to `email_templates`.
6. User reviews/copies/exports templates.

## 18) Technology Stack Summary

- **Backend:** Flask, SQLite, optional PostgreSQL mirror, threading, ThreadPoolExecutor
- **Scraping:** Selenium (Google Maps, LinkedIn/Instagram SERP phases), requests + BeautifulSoup, DDGS
- **Frontend:** Jinja, Bootstrap 5, Bootstrap Icons, vanilla JavaScript, Chart.js, Leaflet + Leaflet.draw
- **Security/infra:** bcrypt, Flask-WTF CSRF, Flask-Limiter, Stripe

## 19) Runtime, Build, and Deployment Methodology

Primary runtime modes in this repository:

1. **Local Python runtime**
   - Flask app + worker threads in one process space.
   - SQLite as primary persistence.

2. **Containerized runtime**
   - `Dockerfile` builds image with Gunicorn + Chromium/Chromedriver support.
   - `docker-compose.yml` orchestrates app + Postgres service.

3. **Optional PostgreSQL mirror mode**
   - Controlled via `LEADGEN_POSTGRES_DSN`.
   - Session/task/event mirrors written via `task_queue/postgres_mirror.py`.

Methodology details:

- **Concurrency model:** queue-backed thread pools for Google Maps phases; direct background threads for other tools.
- **Backpressure model:** max pending queues + per-user active limits enforced before job acceptance.
- **State durability model:** in-memory state + SQLite durable snapshots/events/tasks; optional Postgres mirror for operational analytics redundancy.
- **Health model:** `/health` for app liveness and `/health/ops` for operational diagnostics.
- **Scripted operations:** shell/batch scripts are included for local start and deployment flows.

## 20) Operational Notes

- Google Maps is the only tool with full durable resumable orchestration tables and session operations console.
- LinkedIn/Instagram/Web Crawler jobs are in-memory while active, then persisted as completed history/leads.
- Environment variables tune workers, queue limits, retries, retention, and ops thresholds.

## 21) Quick Index (Where to read what)

- App routes + orchestration: `app.py`
- Google Maps scraper engine: `scraper.py`
- LinkedIn scraper engine: `linkedin_scraper.py`
- Instagram scraper engine: `instagram_scraper.py`
- Web crawler engine: `web_crawler.py`
- Queue pools: `task_queue/dispatcher.py`
- In-memory state store: `task_queue/job_store.py`
- Optional Postgres mirror: `task_queue/postgres_mirror.py`
- Worker entry for gmaps extraction: `workers/scraper_worker.py`
- Geospatial partitioning: `geo/quadtree.py`
- Keyword expansion: `utils/keyword_expander.py`
- Main tool UIs: `templates/*.html`
- Main tool browser controllers: `static/js/*.js`
