(function () {
  "use strict";

  const body = document.getElementById("sessionsBody");
  const refreshBtn = document.getElementById("btnRefreshSessions");
  const btnRefreshOps = document.getElementById("btnRefreshOps");
  const opsStatusBadge = document.getElementById("opsStatusBadge");
  const opsSummaryText = document.getElementById("opsSummaryText");
  const opsAlertsBody = document.getElementById("opsAlertsBody");
  const detailCard = document.getElementById("sessionDetailCard");
  const detailSessionId = document.getElementById("detailSessionId");
  const detailSummary = document.getElementById("detailSummary");
  const detailFilter = document.getElementById("detailCompletionFilter");
  const detailLeadsBody = document.getElementById("detailLeadsBody");
  const detailLogs = document.getElementById("detailLogs");
  const detailTasksBody = document.getElementById("detailTasksBody");
  const detailTaskHealth = document.getElementById("detailTaskHealth");
  const detailResumeAnchor = document.getElementById("detailResumeAnchor");
  const detailAuditBody = document.getElementById("detailAuditBody");
  const detailAuditScope = document.getElementById("detailAuditScope");
  const btnExportAuditReport = document.getElementById("btnExportAuditReport");
  const detailRetentionWrap = document.getElementById("detailRetentionWrap");
  const detailRetentionStatus = document.getElementById(
    "detailRetentionStatus",
  );
  const retentionArchiveTable = document.getElementById(
    "retentionArchiveTable",
  );
  const retentionArchiveDays = document.getElementById("retentionArchiveDays");
  const retentionArchiveLimit = document.getElementById(
    "retentionArchiveLimit",
  );
  const btnRetentionRefresh = document.getElementById("btnRetentionRefresh");
  const btnRetentionExportArchive = document.getElementById(
    "btnRetentionExportArchive",
  );
  const btnRecoverStaleTasks = document.getElementById("btnRecoverStaleTasks");
  const btnAutoRecoverSession = document.getElementById(
    "btnAutoRecoverSession",
  );
  const btnRetryFromAnchor = document.getElementById("btnRetryFromAnchor");
  const btnForceRetryFromAnchor = document.getElementById(
    "btnForceRetryFromAnchor",
  );
  const detailDiagnosticsSummary = document.getElementById(
    "detailDiagnosticsSummary",
  );
  const detailDiagnosticsIssuesBody = document.getElementById(
    "detailDiagnosticsIssuesBody",
  );
  let selectedSessionId = "";
  let currentResumeAnchor = null;
  let canManageTaskActions = false;
  const pendingActionBySession = new Set();
  let isRefreshing = false;

  function renderOpsAlerts(alerts) {
    if (!opsAlertsBody) return;
    if (!Array.isArray(alerts) || !alerts.length) {
      opsAlertsBody.innerHTML =
        '<tr><td colspan="4" class="text-center text-muted py-3">No active alerts.</td></tr>';
      return;
    }

    opsAlertsBody.innerHTML = alerts
      .slice(0, 25)
      .map((a) => {
        const sev = String(a.severity || "info").toLowerCase();
        const sevClass =
          sev === "critical"
            ? "text-danger"
            : sev === "warning"
              ? "text-warning"
              : "text-muted";
        return `<tr>
          <td class="fw-semibold ${sevClass}">${esc(a.severity || "info")}</td>
          <td>${esc(a.code || "—")}</td>
          <td class="small text-muted">${esc(a.message || "")}</td>
          <td class="small text-muted">${esc(a.recommended_action || "")}</td>
        </tr>`;
      })
      .join("");
  }

  function renderOpsSummary(data) {
    if (opsSummaryText) {
      const summary = data?.summary || {};
      const health = data?.health || {};
      opsSummaryText.textContent = `Sessions: ${Number(summary.sessions_total || 0)} total · ${Number(summary.sessions_running || 0)} running · ${Number(summary.sessions_failed_or_partial || 0)} failed/partial · Alerts: ${Number(health.alerts_count || 0)}`;
    }
    if (opsStatusBadge) {
      const status = String(data?.health?.status || "healthy").toLowerCase();
      if (status === "unhealthy") {
        opsStatusBadge.className = "badge bg-danger";
      } else if (status === "degraded") {
        opsStatusBadge.className = "badge bg-warning text-dark";
      } else {
        opsStatusBadge.className = "badge bg-success";
      }
      opsStatusBadge.textContent = status;
    }
    renderOpsAlerts(data?.health?.alerts || []);
  }

  async function loadOpsDashboard() {
    try {
      const res = await fetch("/api/gmaps/ops/dashboard?hours=24");
      const data = await res.json().catch(() => ({}));
      if (!res.ok)
        throw new Error(data.error || "Failed to load ops dashboard");
      renderOpsSummary(data);
    } catch (e) {
      if (opsSummaryText) {
        opsSummaryText.textContent = "Failed to load operational dashboard.";
      }
      if (opsStatusBadge) {
        opsStatusBadge.className = "badge bg-secondary";
        opsStatusBadge.textContent = "unknown";
      }
      renderOpsAlerts([]);
    }
  }

  function renderSessionDiagnostics(data) {
    if (detailDiagnosticsSummary) {
      const th = data?.task_health || {};
      const anchor = data?.resume_anchor || {};
      detailDiagnosticsSummary.textContent = `Status ${String(data?.status || "—")} · Phase ${String(data?.phase || "—")} · Running ${Number(th.running_count || 0)} · Stuck ${Number(th.stuck_count || 0)} · Resume ${String(anchor.event_type || "none")}`;
    }
    if (!detailDiagnosticsIssuesBody) return;
    const issues = Array.isArray(data?.recent_issues) ? data.recent_issues : [];
    if (!issues.length) {
      detailDiagnosticsIssuesBody.innerHTML =
        '<tr><td colspan="4" class="text-center text-muted py-3">No warning/error events in the current window.</td></tr>';
      return;
    }

    detailDiagnosticsIssuesBody.innerHTML = issues
      .slice(-50)
      .reverse()
      .map((event) => {
        const at = event.at ? new Date(event.at).toLocaleString() : "—";
        const sev = String(event.severity || "info").toLowerCase();
        const sevClass =
          sev === "error"
            ? "text-danger"
            : sev === "warning"
              ? "text-warning"
              : "text-muted";
        return `<tr>
          <td class="small text-muted">${esc(at)}</td>
          <td class="fw-semibold ${sevClass}">${esc(event.severity || "info")}</td>
          <td>${esc(event.event_type || "—")}</td>
          <td class="small text-muted">${esc(event.message || "")}</td>
        </tr>`;
      })
      .join("");
  }

  async function loadSessionDiagnostics() {
    if (!selectedSessionId) return;
    try {
      const res = await fetch(
        `/api/gmaps/sessions/${selectedSessionId}/diagnostics`,
      );
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || "Diagnostics unavailable");
      renderSessionDiagnostics(data);
    } catch (e) {
      renderSessionDiagnostics({});
    }
  }

  async function callAction(url) {
    const res = await fetch(url, { method: "POST" });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || "Request failed");
    return data;
  }

  async function postJson(url, body) {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body || {}),
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data.error || "Request failed");
    return data;
  }

  function esc(s) {
    const el = document.createElement("span");
    el.textContent = s || "";
    return el.innerHTML;
  }

  function badge(status) {
    const map = {
      running: "bg-primary",
      completed: "bg-success",
      failed: "bg-danger",
      stopped: "bg-warning text-dark",
    };
    return `<span class="badge ${map[status] || "bg-secondary"}">${esc(status)}</span>`;
  }

  function renderLogs(logs) {
    if (!detailLogs) return;
    if (!Array.isArray(logs) || !logs.length) {
      detailLogs.innerHTML = '<div class="text-muted">No logs yet.</div>';
      return;
    }
    detailLogs.innerHTML = logs
      .slice(-150)
      .map((l) => {
        const ts = l.at ? new Date(l.at).toLocaleTimeString() : "--:--:--";
        const pct = Number.isFinite(l.progress) ? `[${l.progress}%] ` : "";
        return `<div>[${ts}] ${pct}${esc(l.message || "")}</div>`;
      })
      .join("");
    detailLogs.scrollTop = detailLogs.scrollHeight;
  }

  function renderTaskMeta(resumeAnchor, taskHealth) {
    if (detailResumeAnchor) {
      if (
        !resumeAnchor ||
        !resumeAnchor.event_type ||
        resumeAnchor.event_type === "none"
      ) {
        currentResumeAnchor = null;
        detailResumeAnchor.innerHTML =
          '<span class="text-muted">Resume Anchor: not available</span>';
        if (btnRetryFromAnchor) btnRetryFromAnchor.disabled = true;
        if (btnForceRetryFromAnchor) btnForceRetryFromAnchor.disabled = true;
      } else {
        currentResumeAnchor = resumeAnchor;
        detailResumeAnchor.innerHTML = `<span class="text-info">Resume Anchor</span>: ${esc(resumeAnchor.event_type)} → <span class="text-muted">${esc(resumeAnchor.suggested_action || "unknown")}</span>${resumeAnchor.suggested_task_key ? ` <span class="text-secondary">(${esc(resumeAnchor.suggested_task_key)})</span>` : ""}`;
        const noRetryNeeded =
          String(resumeAnchor.suggested_action || "").toLowerCase() ===
          "completed_no_resume_needed";
        if (btnRetryFromAnchor) btnRetryFromAnchor.disabled = noRetryNeeded;
        if (btnForceRetryFromAnchor)
          btnForceRetryFromAnchor.disabled =
            noRetryNeeded ||
            !resumeAnchor.suggested_task_key ||
            !canManageTaskActions;
      }
    }

    if (detailTaskHealth) {
      const healthy = taskHealth && taskHealth.healthy !== false;
      const runningCount = Number(taskHealth?.running_count || 0);
      const stuckCount = Number(taskHealth?.stuck_count || 0);
      const cls = healthy ? "text-success" : "text-warning";
      detailTaskHealth.innerHTML = `<span class="${cls}">Task Health</span>: ${healthy ? "Healthy" : "Attention"} — ${runningCount} running / ${stuckCount} stuck`;
    }
  }

  function renderTasks(tasks) {
    if (!detailTasksBody) return;
    if (!Array.isArray(tasks) || !tasks.length) {
      detailTasksBody.innerHTML =
        '<tr><td colspan="9" class="text-center text-muted py-3">No task timeline found.</td></tr>';
      return;
    }

    detailTasksBody.innerHTML = tasks
      .map((task) => {
        const updated = task.updated_at
          ? new Date(task.updated_at).toLocaleString()
          : "—";
        const maxAttempts = Number(task.max_attempts || 0);
        const backoff = Number(task.retry_backoff_seconds || 0);
        const policy =
          maxAttempts > 0
            ? `${Number(task.attempt_count || 0)}/${maxAttempts} · ${backoff}s`
            : "—";
        const cooldown = task.retry_cooldown_until
          ? new Date(task.retry_cooldown_until).toLocaleString()
          : "—";
        const retryReason = task.last_retry_reason || "—";
        const controls = canManageTaskActions
          ? `<div class="d-flex gap-1 flex-wrap">
              <button class="btn btn-xs btn-outline-info task-action-btn" data-action="retry" data-task-key="${esc(task.task_key || "")}" title="Retry task">Retry</button>
              <button class="btn btn-xs btn-outline-danger task-action-btn" data-action="force_retry" data-task-key="${esc(task.task_key || "")}" title="Force retry">Force</button>
              <button class="btn btn-xs btn-outline-secondary task-action-btn" data-action="reset_attempts" data-task-key="${esc(task.task_key || "")}" title="Reset attempts">Reset</button>
            </div>`
          : '<span class="text-muted small">Operator only</span>';
        return `<tr>
          <td>${esc(task.task_key || "—")}</td>
          <td>${esc(task.phase || "—")}</td>
          <td>${esc(task.status || "—")}</td>
          <td>${Number(task.attempt_count || 0)}</td>
          <td class="small text-muted">${esc(policy)}</td>
          <td class="small text-muted">${esc(cooldown)}</td>
          <td class="small text-muted">${esc(retryReason)}</td>
          <td>${controls}</td>
          <td class="small text-muted">${esc(updated)}</td>
        </tr>`;
      })
      .join("");

    detailTasksBody.querySelectorAll(".task-action-btn").forEach((btn) => {
      btn.addEventListener("click", async () => {
        if (!selectedSessionId || !canManageTaskActions) return;
        const taskKey = String(btn.dataset.taskKey || "").trim();
        const action = String(btn.dataset.action || "").trim();
        if (!taskKey || !action) return;

        if (action === "force_retry") {
          const ok = window.confirm(
            "Force retry bypasses cooldown/max-attempt guards. Continue?",
          );
          if (!ok) return;
        }
        if (action === "reset_attempts") {
          const ok = window.confirm(
            "Reset attempts will clear retry counters and cooldown. Continue?",
          );
          if (!ok) return;
        }

        btn.disabled = true;
        try {
          await postJson(
            `/api/gmaps/sessions/${selectedSessionId}/task-action`,
            {
              task_key: taskKey,
              action,
              reason: "operator_task_timeline_action",
            },
          );
          await loadSessionDetails();
          await loadSessions(false);
        } catch (e) {
          alert(e.message || "Task action failed");
          await loadSessionDetails();
        }
      });
    });
  }

  function renderAuditEvents(events) {
    if (!detailAuditBody) return;
    if (!Array.isArray(events) || !events.length) {
      detailAuditBody.innerHTML =
        '<tr><td colspan="4" class="text-center text-muted py-3">No matching audit events.</td></tr>';
      return;
    }

    detailAuditBody.innerHTML = events
      .map((event) => {
        const at = event.at ? new Date(event.at).toLocaleString() : "—";
        return `<tr>
          <td class="small text-muted">${esc(at)}</td>
          <td>${esc(event.event_type || "—")}</td>
          <td>${esc(event.severity || "info")}</td>
          <td class="small text-muted">${esc(event.message || "")}</td>
        </tr>`;
      })
      .join("");
  }

  async function loadAuditEvents() {
    if (!selectedSessionId) return;
    const scope = detailAuditScope ? detailAuditScope.value : "operator";
    try {
      const res = await fetch(
        `/api/gmaps/sessions/${selectedSessionId}/audit-events?scope=${encodeURIComponent(scope)}&limit=200`,
      );
      const data = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(data.error || "Failed to load audit feed");
      renderAuditEvents(data.events || []);
    } catch (e) {
      renderAuditEvents([]);
    }
  }

  function renderRetentionStatus(data) {
    if (!detailRetentionStatus) return;
    if (!data || typeof data !== "object") {
      detailRetentionStatus.textContent = "Retention status unavailable.";
      return;
    }

    const last = data.last_run || {};
    const lastAt = last.last_run_at
      ? new Date(last.last_run_at).toLocaleString()
      : "never";
    const deleted = `${Number(last.events_deleted || 0)} events / ${Number(last.logs_deleted || 0)} logs / ${Number(last.tasks_deleted || 0)} tasks`;
    const nextDue = Number(data.next_due_in_seconds || 0);
    const base = `Enabled: ${data.enabled ? "yes" : "no"} · Window: events ${Number(data.events_days || 0)}d, logs ${Number(data.logs_days || 0)}d, tasks ${Number(data.tasks_days || 0)}d · Next run in ~${nextDue}s · Last run: ${lastAt} · Last deleted: ${deleted}`;
    detailRetentionStatus.textContent = last.error
      ? `${base} · Last error: ${String(last.error)}`
      : base;
  }

  async function loadRetentionStatus() {
    if (!canManageTaskActions) {
      if (detailRetentionWrap) detailRetentionWrap.classList.add("is-hidden");
      return;
    }
    if (detailRetentionWrap) detailRetentionWrap.classList.remove("is-hidden");

    try {
      const res = await fetch(`/api/gmaps/retention/status`);
      const data = await res.json().catch(() => ({}));
      if (!res.ok)
        throw new Error(data.error || "Failed to load retention status");
      renderRetentionStatus(data);
    } catch (e) {
      renderRetentionStatus(null);
    }
  }

  function renderRows(sessions) {
    if (!sessions.length) {
      body.innerHTML =
        '<tr><td colspan="8" class="text-center text-muted py-4">No active sessions.</td></tr>';
      return;
    }

    body.innerHTML = sessions
      .map((s) => {
        const stage =
          s.phase === "contacts" ? "Contact Retrieval" : "List Extraction";
        const contactsStatus = s.contacts_status || "pending";
        const extractionStatus = s.extraction_status || "pending";
        const updated = s.updated_at
          ? new Date(s.updated_at).toLocaleString()
          : "—";
        const completion = Number(s.completion_rate || 0);
        const completeCount = Number(s.complete_count || 0);
        const incompleteCount = Number(s.incomplete_count || 0);
        const sid = String(s.job_id || s.id || "");

        const lifecycle = String(s.status || "running").toLowerCase();
        const contactsDone = contactsStatus === "completed";
        const fullyDone = lifecycle === "completed" && contactsDone;
        const isRunning = lifecycle === "running";
        const isPaused = contactsStatus === "paused" || lifecycle === "stopped";
        const lock = pendingActionBySession.has(sid);

        const showControls = !fullyDone;
        const disableStart =
          lock ||
          isRunning ||
          contactsStatus === "running" ||
          extractionStatus !== "completed";
        const disablePause =
          lock || (!isRunning && contactsStatus !== "running");
        const disableResume =
          lock || (!isPaused && contactsStatus !== "failed");
        const disableRestart = lock || isRunning;

        const controlsHtml = showControls
          ? `<div class="d-flex gap-1 flex-wrap session-actions" role="group" aria-label="Session actions">
              <button class="btn btn-sm btn-outline-info action-start session-icon-btn" title="Start contact retrieval" aria-label="Start contacts" ${disableStart ? "disabled" : ""} data-phase="${esc(s.phase || "extract")}" data-id="${esc(sid)}"><i class="bi bi-play-fill"></i></button>
              <button class="btn btn-sm btn-outline-warning action-pause session-icon-btn" title="Pause" aria-label="Pause" ${disablePause ? "disabled" : ""} data-phase="${esc(s.phase || "extract")}" data-id="${esc(sid)}"><i class="bi bi-pause-fill"></i></button>
              <button class="btn btn-sm btn-outline-success action-resume session-icon-btn" title="Resume" aria-label="Resume" ${disableResume ? "disabled" : ""} data-phase="${esc(s.phase || "extract")}" data-id="${esc(sid)}"><i class="bi bi-skip-forward-fill"></i></button>
              <button class="btn btn-sm btn-outline-secondary action-restart session-icon-btn" title="Restart" aria-label="Restart" ${disableRestart ? "disabled" : ""} data-phase="${esc(s.phase || "extract")}" data-id="${esc(sid)}"><i class="bi bi-arrow-repeat"></i></button>
            </div>`
          : `<span class="text-muted small">—</span>`;

        return `<tr class="session-row session-row-clickable" data-id="${esc(sid)}">
          <td><strong>${esc(sid)}</strong></td>
          <td>${esc((s.keyword || "") + " in " + (s.place || ""))}</td>
          <td>${esc(stage)} <span class="text-muted small">(${esc(contactsStatus)})</span></td>
          <td>${badge(s.status || "running")}</td>
          <td>${Number(s.results_count || s.lead_count || 0)}</td>
          <td>
            <div class="small fw-semibold">${completion}%</div>
            <div class="progress session-progress-compact"><div class="progress-bar bg-success" style="width:${completion}%"></div></div>
            <div class="small text-muted">${completeCount} complete / ${incompleteCount} incomplete</div>
          </td>
          <td class="small text-muted">${esc(updated)}</td>
          <td>${controlsHtml}</td>
        </tr>`;
      })
      .join("");

    body.querySelectorAll(".session-row").forEach((row) => {
      row.addEventListener("click", async (e) => {
        const target = e.target;
        if (target && (target.closest("button") || target.closest("a"))) {
          return;
        }
        selectedSessionId = row.dataset.id;
        await loadSessionDetails();
      });
    });

    body.querySelectorAll(".action-start").forEach((b) => {
      b.addEventListener("click", async () => {
        if (pendingActionBySession.has(b.dataset.id)) return;
        pendingActionBySession.add(b.dataset.id);
        try {
          await callAction(`/api/gmaps/contacts/start/${b.dataset.id}`);
          await loadSessions();
          if (selectedSessionId === b.dataset.id) await loadSessionDetails();
        } catch (e) {
          alert(e.message);
        } finally {
          pendingActionBySession.delete(b.dataset.id);
        }
      });
    });
    body.querySelectorAll(".action-pause").forEach((b) => {
      b.addEventListener("click", async () => {
        if (pendingActionBySession.has(b.dataset.id)) return;
        pendingActionBySession.add(b.dataset.id);
        try {
          const endpoint =
            b.dataset.phase === "contacts"
              ? `/api/gmaps/contacts/pause/${b.dataset.id}`
              : `/api/gmaps/extract/pause/${b.dataset.id}`;
          await callAction(endpoint);
          await loadSessions();
          if (selectedSessionId === b.dataset.id) await loadSessionDetails();
        } catch (e) {
          alert(e.message);
        } finally {
          pendingActionBySession.delete(b.dataset.id);
        }
      });
    });
    body.querySelectorAll(".action-resume").forEach((b) => {
      b.addEventListener("click", async () => {
        if (pendingActionBySession.has(b.dataset.id)) return;
        pendingActionBySession.add(b.dataset.id);
        try {
          const endpoint =
            b.dataset.phase === "contacts"
              ? `/api/gmaps/contacts/resume/${b.dataset.id}`
              : `/api/gmaps/extract/restart/${b.dataset.id}`;
          await callAction(endpoint);
          await loadSessions();
          if (selectedSessionId === b.dataset.id) await loadSessionDetails();
        } catch (e) {
          alert(e.message);
        } finally {
          pendingActionBySession.delete(b.dataset.id);
        }
      });
    });
    body.querySelectorAll(".action-restart").forEach((b) => {
      b.addEventListener("click", async () => {
        if (pendingActionBySession.has(b.dataset.id)) return;
        pendingActionBySession.add(b.dataset.id);
        try {
          const endpoint =
            b.dataset.phase === "contacts"
              ? `/api/gmaps/contacts/restart/${b.dataset.id}`
              : `/api/gmaps/extract/restart/${b.dataset.id}`;
          await callAction(endpoint);
          await loadSessions();
          if (selectedSessionId === b.dataset.id) await loadSessionDetails();
        } catch (e) {
          alert(e.message);
        } finally {
          pendingActionBySession.delete(b.dataset.id);
        }
      });
    });
  }

  function renderSessionLeads(leads) {
    if (!detailLeadsBody) return;
    if (!leads || !leads.length) {
      detailLeadsBody.innerHTML =
        '<tr><td colspan="4" class="text-center text-muted py-3">No leads for selected filter.</td></tr>';
      return;
    }

    detailLeadsBody.innerHTML = leads
      .map((lead) => {
        const website =
          lead.website && lead.website !== "N/A" ? lead.website : "—";
        const email = lead.email && lead.email !== "N/A" ? lead.email : "—";
        const phone = lead.phone && lead.phone !== "N/A" ? lead.phone : "—";
        return `<tr>
          <td>${esc(lead.business_name || "—")}</td>
          <td>${esc(email)}</td>
          <td>${esc(phone)}</td>
          <td>${esc(website)}</td>
        </tr>`;
      })
      .join("");
  }

  async function loadSessionDetails() {
    if (!selectedSessionId) return;
    if (detailCard) detailCard.classList.remove("is-hidden");
    if (detailSessionId) detailSessionId.textContent = `#${selectedSessionId}`;
    if (detailSummary) detailSummary.textContent = "Loading session details...";

    try {
      const filter = detailFilter ? detailFilter.value : "all";
      const res = await fetch(
        `/api/gmaps/sessions/${selectedSessionId}/leads?completion=${encodeURIComponent(filter)}&limit=300`,
      );
      const data = await res.json();
      if (!res.ok)
        throw new Error(data.error || "Failed to load session details");

      const summary = data.summary || {};
      canManageTaskActions = !!data.operator_controls?.can_manage_tasks;
      if (detailSummary) {
        detailSummary.textContent = `Completion ${summary.completion_rate || 0}% — ${summary.complete_count || 0} complete / ${summary.incomplete_count || 0} incomplete / ${summary.total || 0} total`;
      }
      renderSessionLeads(data.leads || []);
      renderLogs(data.logs || []);
      renderTasks(data.tasks || []);
      renderTaskMeta(data.resume_anchor || {}, data.task_health || {});
      await loadAuditEvents();
      await loadSessionDiagnostics();
      await loadRetentionStatus();
    } catch (e) {
      canManageTaskActions = false;
      if (detailSummary)
        detailSummary.textContent =
          e.message || "Failed to load session details.";
      renderSessionLeads([]);
      renderLogs([]);
      renderTasks([]);
      renderTaskMeta({}, {});
      renderAuditEvents([]);
      renderSessionDiagnostics({});
      renderRetentionStatus(null);
      if (detailRetentionWrap) detailRetentionWrap.classList.add("is-hidden");
    }
  }

  async function loadSessions(showLoading = true) {
    if (isRefreshing) return;
    isRefreshing = true;
    if (showLoading) {
      body.innerHTML =
        '<tr><td colspan="8" class="text-center text-muted py-4">Loading sessions...</td></tr>';
    }
    try {
      const res = await fetch("/api/gmaps/sessions");
      const data = await res.json();
      renderRows(data.sessions || []);
    } catch (e) {
      if (showLoading) {
        body.innerHTML =
          '<tr><td colspan="8" class="text-center text-danger py-4">Failed to load sessions.</td></tr>';
      }
    } finally {
      isRefreshing = false;
    }
  }

  if (refreshBtn) refreshBtn.addEventListener("click", loadSessions);
  if (btnRefreshOps) {
    btnRefreshOps.addEventListener("click", async () => {
      await loadOpsDashboard();
    });
  }
  if (btnRecoverStaleTasks) {
    btnRecoverStaleTasks.addEventListener("click", async () => {
      if (!selectedSessionId) return;
      btnRecoverStaleTasks.disabled = true;
      try {
        const res = await fetch(
          `/api/gmaps/sessions/${selectedSessionId}/recover-stale`,
          { method: "POST" },
        );
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "Recovery failed");
        await loadSessionDetails();
        await loadSessions(false);
      } catch (e) {
        alert(e.message || "Recovery failed");
      } finally {
        btnRecoverStaleTasks.disabled = false;
      }
    });
  }
  if (btnRetryFromAnchor) {
    btnRetryFromAnchor.addEventListener("click", async () => {
      if (!selectedSessionId) return;
      btnRetryFromAnchor.disabled = true;
      try {
        const res = await fetch(
          `/api/gmaps/sessions/${selectedSessionId}/retry-from-anchor`,
          { method: "POST" },
        );
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "Suggested recovery failed");
        await loadSessionDetails();
        await loadSessions(false);
      } catch (e) {
        alert(e.message || "Suggested recovery failed");
        await loadSessionDetails();
      }
    });
  }
  if (btnForceRetryFromAnchor) {
    btnForceRetryFromAnchor.addEventListener("click", async () => {
      if (!selectedSessionId || !currentResumeAnchor?.suggested_task_key)
        return;
      const confirmed = window.confirm(
        "Force retry bypasses cooldown/max-attempt guards. Continue?",
      );
      if (!confirmed) return;

      btnForceRetryFromAnchor.disabled = true;
      try {
        const res = await fetch(
          `/api/gmaps/sessions/${selectedSessionId}/retry-task`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              task_key: currentResumeAnchor.suggested_task_key,
              force: true,
              reason: "operator_override_from_sessions_ui",
            }),
          },
        );
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "Force retry failed");
        await loadSessionDetails();
        await loadSessions(false);
      } catch (e) {
        alert(e.message || "Force retry failed");
        await loadSessionDetails();
      }
    });
  }
  if (btnAutoRecoverSession) {
    btnAutoRecoverSession.addEventListener("click", async () => {
      if (!selectedSessionId) return;
      btnAutoRecoverSession.disabled = true;
      try {
        const res = await fetch(
          `/api/gmaps/sessions/${selectedSessionId}/recover-auto`,
          {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ apply_retry: true, force: false }),
          },
        );
        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || "Auto recovery failed");
        await loadSessionDetails();
        await loadSessions(false);
        await loadOpsDashboard();
      } catch (e) {
        alert(e.message || "Auto recovery failed");
        await loadSessionDetails();
      } finally {
        btnAutoRecoverSession.disabled = false;
      }
    });
  }
  if (detailFilter) {
    detailFilter.addEventListener("change", async () => {
      await loadSessionDetails();
    });
  }
  if (detailAuditScope) {
    detailAuditScope.addEventListener("change", async () => {
      await loadAuditEvents();
    });
  }
  if (btnExportAuditReport) {
    btnExportAuditReport.addEventListener("click", async () => {
      if (!selectedSessionId) return;
      const scope = detailAuditScope ? detailAuditScope.value : "recovery";
      window.location.href = `/api/gmaps/sessions/${selectedSessionId}/audit-report.csv?scope=${encodeURIComponent(scope)}`;
    });
  }
  if (btnRetentionRefresh) {
    btnRetentionRefresh.addEventListener("click", async () => {
      await loadRetentionStatus();
    });
  }
  if (btnRetentionExportArchive) {
    btnRetentionExportArchive.addEventListener("click", async () => {
      if (!canManageTaskActions) return;
      const table = retentionArchiveTable
        ? retentionArchiveTable.value
        : "events";
      const days = Math.max(1, Number(retentionArchiveDays?.value || 30));
      const limit = Math.max(
        1,
        Math.min(20000, Number(retentionArchiveLimit?.value || 5000)),
      );
      window.location.href = `/api/gmaps/retention/archive.csv?table=${encodeURIComponent(table)}&older_than_days=${encodeURIComponent(String(days))}&limit=${encodeURIComponent(String(limit))}`;
    });
  }
  loadOpsDashboard();
  loadSessions(true);
  setInterval(async () => {
    await loadOpsDashboard();
    await loadSessions(false);
    if (selectedSessionId) await loadSessionDetails();
  }, 3000);
})();
