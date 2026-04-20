# LeadGen Industrial-Scale Action Plan

Last updated: 2026-04-18
Owner: Copilot + Product Owner

## Objective

Transform LeadGen from a monolithic, best-effort scraping app into an industrial-scale, failure-tolerant system that can process very large jobs with durable state, recoverability, and consistent UI behavior.

## Phases

### Phase 1 — Reliability Foundation (Completed)

1. Durable session event timeline (status/phase/progress/error checkpoints)
2. Explicit checkpoint boundaries by workflow phase
3. Stronger timeout/error classification + non-fatal continuation where safe
4. Guaranteed partial data persistence before failure/stop transitions

Success criteria:

- Session timeline survives process restarts
- Post-mortem can explain why/where a session failed
- No single transient timeout halts the entire run

Phase 1 closure validation (2026-04-18):

- Worker-driven transitions and API-triggered pause/stop transitions now both emit durable checkpoint evidence.
- Partial snapshot persistence is explicitly recorded before all pause/failure transition paths.

### Phase 2 — Durable Job Orchestration (Completed)

1. Replace memory-centric orchestration with DB-backed state machine
2. Add job/task tables for chunked processing
3. Introduce idempotent task execution contracts
4. Resume from task checkpoints, not full session restart

Success criteria:

- Restart-safe execution without data loss
- Fine-grained retries (task-level)

Phase 2 closure validation (2026-04-18):

- Runtime uses DB-backed fallback state loading in worker paths (not memory-only state reads).
- Task chunk table added for chunked execution contracts and resumable checkpoints.
- Contact enrichment now runs in deterministic chunks with idempotent chunk upserts.
- Completed chunk outputs are checkpointed and skipped on resume (checkpoint-level restart).
- Contact restart endpoint clears chunk checkpoints explicitly to force clean re-execution.

### Phase 3 — Scale Architecture (Completed)

1. Queue-based worker pool separation (extract vs enrich)
2. Batch/chunk processing for 10k+ targets
3. Concurrency controls + backpressure
4. Move from SQLite to production-grade DB (target: PostgreSQL)

Success criteria:

- Horizontal worker scaling
- Stable high-volume throughput

Phase 3 closure validation (2026-04-19):

- Queue-separated worker pools are active for extraction vs contacts with bounded queues.
- Chunked execution contracts are in place for high-volume contact enrichment and checkpoint resume.
- Concurrency controls include backpressure (`429`), per-user active quotas, and queue aging TTL.
- Production DB target path implemented via PostgreSQL mirror persistence with schema bootstrap and dockerized Postgres service.

### Phase 4 — Observability & Ops (Completed)

1. Structured logging + metrics per stage
2. Failure dashboards and session diagnostics
3. Automated health checks and alerts
4. Recovery tools for failed/stuck sessions

Success criteria:

- Root cause visible without code debugging
- Operational confidence for long-running jobs

Phase 4 closure validation (2026-04-19):

- Structured event logging now emits machine-readable operational records for every durable session event.
- Dedicated Ops APIs provide dashboard, stage metrics, health, and alerts views without requiring direct DB inspection.
- Session diagnostics and auto-recovery APIs support operator troubleshooting and deterministic remediation for stuck/failed states.
- Sessions UI now surfaces Ops status/alerts, diagnostics event trails, and one-click auto-recovery controls.

### Phase 5 — UI Continuity System (Completed)

1. Standardized component patterns (cards, buttons, tables, spacing)
2. Remove inline styling drift
3. Session states reflected consistently across pages
4. Unified action icon language and affordances

Success criteria:

- Consistent UI behavior and appearance across all tools

Phase 5 closure validation (2026-04-19):

- Shared continuity primitives now cover cards, action controls, progress/results/error sections, icon accents, timers, and common sizing utilities.
- Tool-facing pages now use standardized class-based patterns and have removed inline style drift in high-traffic flows.
- Session and step-driven views use consistent hidden-state and status affordances (`is-hidden` + shared visual semantics).
- Action/icon language has been aligned across scraper and operations surfaces for predictable UX behavior.

## Execution Log

### Patch 1 (Completed)

- Added this action plan file
- Added durable `gmaps_session_events` persistence
- Added automatic event emission for:
  - status transitions
  - phase transitions
  - 10% progress checkpoints
  - error capture

## Next Patch Target

Patch 2: Introduce explicit workflow checkpoint records (`extract_started`, `extract_completed`, `contacts_started`, `contacts_completed`, `contacts_failed`) and use them as resume anchors.

### Patch 2 (Completed)

- Added explicit checkpoint events to the runtime lifecycle:
  - `extract_started`
  - `extract_completed` / `extract_partial`
  - `extract_failed`
  - `contacts_started`
  - `contacts_completed` / `contacts_paused`
  - `contacts_failed`
- Checkpoint events are now persisted to durable `gmaps_session_events` for post-mortem and resume-anchor analysis.

## Next Patch Target

Patch 3: Add read APIs/utilities for checkpoint timeline inspection and a lightweight resume-anchor selector (latest safe checkpoint per session).

### Patch 3 (Completed)

- Added durable checkpoint timeline loader from `gmaps_session_events`
- Added resume-anchor selector (`_select_resume_anchor`) to infer safest next action
- Extended session detail API to return:
  - `checkpoints`
  - `resume_anchor`

## Next Patch Target

Patch 4: Introduce database-backed task table (`gmaps_session_tasks`) with idempotent upsert and basic task lifecycle fields (`pending/running/completed/failed`) to reduce memory-only orchestration risk.

### Patch 4 (Completed)

- Added durable `gmaps_session_tasks` table
- Added task lifecycle upsert helper with:
  - status transitions
  - attempt count increments on `running`
  - payload snapshot and last error persistence
  - started/finished timestamps
- Wired task lifecycle into runtime:
  - `extract_main` (`running` -> `completed/failed`)
  - `contacts_main` (`running` -> `completed/paused/failed`)

## Next Patch Target

Patch 5: Expose task timeline in sessions APIs + UI detail panel and add basic stuck-task detection (`running` tasks with stale `updated_at`).

### Patch 5 (Completed)

- Added backend task timeline loader and task health summary (including stuck-task count)
- Extended session detail API to include:
  - `tasks`
  - `task_health`
- Extended Sessions detail UI to display:
  - resume anchor
  - task health
  - task timeline table

## Next Patch Target

Patch 6: Add automated recovery hooks for stale running tasks (mark stale -> retryable) and expose one-click recovery action in sessions UI.

### Patch 6 (Completed)

- Added backend stale-task recovery helper (`running` + stale -> `retryable`)
- Added API endpoint: `POST /api/gmaps/sessions/<job_id>/recover-stale`
- Added event emission on recovery (`stale_tasks_recovered`)
- Added Sessions detail UI one-click action: "Recover Stale Tasks"

## Next Patch Target

Patch 7: Add automatic background stale-task sweeper and task-level retry orchestration hooks for extract/contact stages.

### Patch 7 (Completed)

- Added automatic stale-task sweeper (`_run_auto_stale_task_sweeper`) with:
  - throttled execution
  - stale running task detection
  - automatic `running -> retryable` transition
  - session state/log/event updates on recovery
- Added request-cycle trigger for automatic sweeps (`@app.before_request`)
- Added task-level retry lifecycle hooks on control actions:
  - contacts start/pause/resume/restart
  - extraction restart

## Next Patch Target

Patch 8: Add explicit retry endpoint(s) by task key and wire resume-anchor suggested actions to these endpoints for deterministic recovery flows.

### Patch 8 (Completed)

- Added deterministic retry APIs:
  - `POST /api/gmaps/sessions/<job_id>/retry-task` (explicit `task_key` retries)
  - `POST /api/gmaps/sessions/<job_id>/retry-from-anchor` (resume-anchor driven retries)
- Added backend deterministic retry handlers for:
  - `extract_main` (rebuild extraction run with persisted payload)
  - `contacts_main` (resume/retry contacts using persisted leads)
- Extended resume anchor payload with `suggested_task_key` for deterministic routing.
- Wired Sessions UI one-click action: "Apply Suggested Recovery" to `retry-from-anchor` endpoint.

## Next Patch Target

Patch 9: Add retry policy metadata (max attempts/backoff/last retry reason) per task and enforce deterministic retry guards to prevent rapid retry loops.

### Patch 9 (Completed)

- Added retry policy metadata to durable task lifecycle records:
  - `max_attempts`
  - `retry_backoff_seconds`
  - `retry_cooldown_until`
  - `last_retry_reason`
  - `last_retry_at`
- Added backward-compatible SQLite migrations for existing `gmaps_session_tasks` tables.
- Added deterministic retry guardrails before task retry launch:
  - block when task is already `running`
  - block when `attempt_count >= max_attempts`
  - block when cooldown window has not expired
- Added blocked-retry event emission (`task_retry_blocked`) with reason payload for post-mortem visibility.

## Next Patch Target

Patch 10: Add operator override controls for exhausted/cooldown-blocked tasks (force retry with audit trail) and surface retry-policy metadata in Sessions task timeline UI.

### Patch 10 (Completed)

- Added operator force-retry override support through deterministic retry APIs:
  - `POST /api/gmaps/sessions/<job_id>/retry-task` now accepts:
    - `force: true`
    - `reason: "..."`
  - force override bypasses cooldown/max-attempt guardrails, while still blocking retries for tasks currently `running`.
- Added force-retry audit trail persistence:
  - task payload captures force context (`force`, `force_reason`, trigger)
  - task retry reason stores force override marker
  - durable timeline event `task_force_retry_requested`
- Extended Sessions task timeline UI with retry-policy metadata:
  - attempt policy (`attempt_count/max_attempts` + backoff seconds)
  - cooldown timestamp
  - last retry reason
- Added one-click operator control in Sessions detail panel:
  - "Force Suggested Retry" (with confirmation guard)
  - routes to deterministic force retry by resume-anchor suggested task key.

## Next Patch Target

Patch 11: Add per-task operator controls directly in task timeline rows (Retry / Force Retry / Reset attempts) with strict role checks and full audit events.

### Patch 11 (Completed)

- Added strict operator role checks for sensitive task operations using operator allowlist controls:
  - env-based operator gate (`LEADGEN_OPERATOR_EMAILS`)
  - optional global operator override switch (`LEADGEN_OPERATOR_OVERRIDE_ALL`)
- Added dedicated per-task operator API endpoint:
  - `POST /api/gmaps/sessions/<job_id>/task-action`
  - supported actions: `retry`, `force_retry`, `reset_attempts`
- Added full audit trail coverage for operator actions and denials:
  - `task_operator_denied`
  - `task_force_retry_requested` (existing force flow now operator-gated)
  - `task_attempts_reset`
- Added task-attempt reset operation:
  - clears `attempt_count`
  - clears cooldown metadata
  - marks failed/canceled/retryable tasks back to `retryable`
- Extended session detail payload with operator capability flag (`operator_controls.can_manage_tasks`).
- Updated Sessions task timeline UI with row-level controls:
  - Retry
  - Force
  - Reset
  - controls are hidden/disabled for non-operator users.

## Next Patch Target

Patch 12: Add immutable operator audit feed in Sessions detail (filterable by operator events) and exportable recovery-action report for compliance/ops review.

### Patch 12 (Completed)

- Added immutable session audit feed APIs sourced from durable `gmaps_session_events`:
  - `GET /api/gmaps/sessions/<job_id>/audit-events`
  - supports scopes: `operator`, `recovery`, `all`
- Added exportable compliance/ops report endpoint:
  - `GET /api/gmaps/sessions/<job_id>/audit-report.csv`
  - exports scoped recovery/operator audit data as CSV
  - includes event metadata plus extracted action context (`task_key`, `action`, `reason`, `actor`)
- Added scoped audit filtering helpers in backend to keep feed/report behavior consistent.
- Added Sessions detail "Operator Audit Feed" UI:
  - scope selector (Operator/Recovery/All)
  - audit table rendering immutable timeline events
  - one-click "Export Audit CSV" bound to selected scope
- Integrated audit feed refresh into session detail loading flow and scope-change updates.

## Next Patch Target

Patch 13: Add retention and archival policy for event/log/task tables (rolling window + optional archive export) to keep long-running production datasets performant.

### Patch 13 (Completed)

- Added rolling retention policy with throttled automatic cleanup for durable operational tables:
  - `gmaps_session_events`
  - `gmaps_session_logs`
  - `gmaps_session_tasks` (terminal/retryable states only)
- Added retention scheduler controls via environment configuration:
  - `LEADGEN_RETENTION_ENABLED`
  - `LEADGEN_RETENTION_INTERVAL_SECONDS`
  - `LEADGEN_RETENTION_EVENTS_DAYS`
  - `LEADGEN_RETENTION_LOGS_DAYS`
  - `LEADGEN_RETENTION_TASKS_DAYS`
- Added operator-only retention observability endpoint:
  - `GET /api/gmaps/retention/status`
- Added operator-only archival export endpoint for pre/post cleanup snapshots:
  - `GET /api/gmaps/retention/archive.csv`
  - supports tables: `events`, `logs`, `tasks`
  - supports `older_than_days` and `limit` filters

### Patch 14 (Completed)

- Closed remaining Phase 1 reliability gap by adding explicit durable checkpoint emission when partial snapshots are persisted before partial/failed transitions.
- Added event type:
  - `partial_snapshot_persisted`
- Wired checkpoint emission into extraction/contact lifecycle transitions where partial/failure state is committed:
  - extraction partial completion
  - extraction failure (with/without partial leads)
  - contacts paused
  - contacts failure (with/without partial leads)
- Result: all Phase 1 requirements now have explicit durable evidence in the timeline:
  - status/phase/progress/error timeline
  - workflow checkpoint anchors
  - timeout/non-fatal continuation hardening
  - partial persistence confirmation before failure/stop transitions

## Next Patch Target

Patch 15: Add admin runbook surface in UI for retention/archive operations (status panel + archive export controls + last-run telemetry) with operator-only visibility.

### Patch 15 (Completed)

- Added retention last-run telemetry in backend retention status response:
  - `last_run.last_run_at`
  - `last_run.events_deleted`
  - `last_run.logs_deleted`
  - `last_run.tasks_deleted`
  - `last_run.error`
  - `next_due_in_seconds`
- Added Sessions UI retention/admin runbook panel (operator-only visibility):
  - retention status summary surface
  - archive export controls (`table`, `older_than_days`, `limit`)
  - status refresh action
- Wired UI controls to retention APIs:
  - `GET /api/gmaps/retention/status`
  - `GET /api/gmaps/retention/archive.csv`
- Visibility guard:
  - panel hidden for non-operator users based on existing operator capability flag.

## Next Patch Target

Patch 16: Complete Phase 2 durable orchestration end-to-end in runtime.

### Patch 16 (Completed)

- Added DB-backed task chunk table: `gmaps_task_chunks` with idempotent unique key (`session_id`, `task_key`, `chunk_key`).
- Added chunk lifecycle helpers:
  - chunk upsert with attempt counting and status transitions
  - chunk checkpoint loading and completed-output restore
  - deterministic chunk plan builder (`lead_uid`-based chunk keys)
- Refactored contacts runtime from monolithic pass to checkpointed chunk orchestration:
  - chunk-level start/completed/failed events
  - skip already completed chunks on resume
  - merge checkpointed outputs into final result set
  - partial/failure outcomes preserve checkpointed outputs
- Added task-chunk summary to sessions detail payload for resume visibility.
- Strengthened DB-backed state-machine behavior in worker runtime by using state loader with persisted fallback.

## Next Patch Target

Patch 17: Phase 3 kickoff — extract/contact worker separation with queue-backed dispatch and concurrency caps.

### Patch 17 (Completed)

- Added queue-backed worker pool dispatcher with explicit pool separation:
  - extract pool
  - contacts pool
- Added configurable concurrency and backpressure controls:
  - `LEADGEN_EXTRACT_WORKERS`
  - `LEADGEN_CONTACT_WORKERS`
  - `LEADGEN_EXTRACT_MAX_PENDING`
  - `LEADGEN_CONTACT_MAX_PENDING`
- Replaced direct thread spawning in Google Maps routes with pool submission:
  - extraction start/restart
  - contacts start/resume/restart
- Added deterministic backpressure handling in APIs:
  - queue reject paths return `429`
  - durable queue accepted/rejected events persisted for post-mortem
- Added worker pool status endpoint:
  - `GET /api/gmaps/worker-pools`

### Patch 18 (Completed)

- Added fair scheduling controls to worker pools:
  - per-user active limits (`LEADGEN_EXTRACT_PER_USER_ACTIVE_LIMIT`, `LEADGEN_CONTACT_PER_USER_ACTIVE_LIMIT`)
  - user-aware queue admission with deterministic rejection reasons
- Added queue aging policy:
  - pending TTL cleanup (`LEADGEN_QUEUE_PENDING_TTL_SECONDS`)
  - stale queued jobs are evicted before admission checks
- Extended worker pool telemetry:
  - pending/active counts per user
  - per-pool quota/TTL configuration visibility

### Patch 19 (Completed)

- Added PostgreSQL production-path persistence mirror:
  - new module: `task_queue/postgres_mirror.py`
  - schema bootstrap for core orchestration tables
  - dual-write hooks from session, event, task, and chunk persistence paths
- Added PostgreSQL runtime wiring:
  - env DSN: `LEADGEN_POSTGRES_DSN`
  - pool status includes `postgres_enabled`
- Added infrastructure support:
  - `psycopg2-binary` dependency
  - Docker Compose Postgres service + DSN passthrough

## Next Patch Target

Patch 20: Phase 4 kickoff — introduce structured metrics/events dashboard API for failure and throughput diagnostics.

### Patch 20 (Completed)

- Added structured operational event logs for Google Maps durable events:
  - machine-readable JSON log lines emitted alongside persisted session events
- Added Phase 4 observability APIs:
  - `GET /api/gmaps/ops/dashboard`
  - `GET /api/gmaps/ops/metrics`
  - `GET /api/gmaps/ops/health`
  - `GET /api/gmaps/ops/alerts`
- Added per-session diagnostics and recovery APIs:
  - `GET /api/gmaps/sessions/<job_id>/diagnostics`
  - `POST /api/gmaps/sessions/<job_id>/recover-auto`
- Added health automation endpoint for operators/infra checks:
  - `GET /health/ops` (returns degraded/unhealthy with `503` on critical conditions)
- Updated Sessions UI with Phase 4 operational surfaces:
  - Ops overview card with health badge + active alerts table
  - session diagnostics panel (recent warning/error event trail)
  - one-click auto-recovery action in session detail controls

## Next Patch Target

Patch 21: Phase 5 kickoff — begin UI continuity system (standardized component patterns and cross-tool visual consistency).

### Patch 21 (Completed)

- Added shared UI continuity primitives in global stylesheet for reuse across tools:
  - hidden/visibility utility
  - summary toggle cursor style
  - compact session progress style
  - standardized small control width utilities
  - dashboard accent/icon/quick-launch utility classes
- Removed Sessions inline style drift and switched to class-based styling:
  - controls/select widths
  - details summary cursor affordances
  - card/panel hidden state classes
- Updated Sessions frontend rendering to align with shared continuity classes:
  - class-based visibility for detail/retention panels
  - unified clickable row + compact progress styling
- Removed dashboard page-local style block and migrated it to shared global classes.
- Applied shared continuity classes across Dashboard stats and quick-launch elements for consistent component patterns.

## Next Patch Target

Patch 22: Continue Phase 5 by standardizing progress/results/error sections across scraper tool pages (`gmaps`, `linkedin`, `instagram`, `webcrawler`) and replacing remaining inline style drift with shared classes.

### Patch 22 (Completed)

- Added shared scraper continuity primitives in global stylesheet for cross-tool reuse:
  - compact section title and results title classes
  - elapsed timer badge style
  - shared realtime progress logs panel style
  - brand spinner/progress bar accent classes for each scraper tool
  - standardized result badge variants and filter-width utility
  - unified error icon style
- Standardized progress/results/error sections in scraper tool templates:
  - `templates/gmaps.html`
  - `templates/linkedin.html`
  - `templates/instagram.html`
  - `templates/webcrawler.html`
- Replaced inline display/font/width/color style drift in those sections with shared classes.
- Unified hidden-state initialization using reusable class-based visibility (`is-hidden`) across tool sections.
- Kept runtime behavior unchanged while improving UI consistency and maintainability.

## Next Patch Target

Patch 23: Continue Phase 5 by standardizing remaining inline style drift in scraper headers/forms/sort affordances and aligning icon-accent language across all tool pages.

### Patch 23 (Completed)

- Added shared scraper icon-accent utility classes in global stylesheet:
  - `tool-icon-gmaps`
  - `tool-icon-linkedin`
  - `tool-icon-instagram`
  - `tool-icon-webcrawler`
- Removed remaining inline icon-color drift from scraper pages and switched to shared classes:
  - `templates/gmaps.html` (header icon + map selection icon)
  - `templates/linkedin.html` (header icon)
  - `templates/instagram.html` (header icon)
  - `templates/webcrawler.html` (header icon)
- Completed icon-accent language alignment across scraper tool pages using reusable class primitives.

## Next Patch Target

Patch 24: Continue Phase 5 by standardizing remaining inline style drift in non-scraper high-traffic pages (`database`, `settings`, `email_outreach`) using shared continuity classes.

### Patch 24 (Completed)

- Added final shared continuity utilities in global stylesheet for non-scraper high-traffic views:
  - monospace/text-size helpers
  - shared modal surface and textarea wrapping helpers
  - common width/scroll/spinner helpers
  - accent icon variants and shared input-addon styling
- Standardized and removed inline style drift in:
  - `templates/database.html`
  - `templates/settings.html`
  - `templates/email_outreach.html`
- Unified icon/action affordances on those pages using shared accent and error classes.
- Aligned hidden-state initialization on multi-step outreach flows with continuity class semantics.

## Next Patch Target

Phase 5 is complete. No pending Phase 5 tasks remain.
