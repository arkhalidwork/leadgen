/**
 * LeadGen — Frontend Application Logic (Phase A: SSE Real-Time Streaming)
 *
 * Replaces 1-second polling with Server-Sent Events (SSE) for zero-latency
 * updates. Leads appear in the table one by one as they're discovered.
 * Contact enrichment shows per-row crawling state.
 *
 * Architecture:
 *   Browser ──EventSource──► /api/stream/<job_id>
 *   Events: job_started, geocell_progress, lead_found, crawl_started,
 *           contact_found, job_completed, stream_timeout
 *
 * Session persistence: localStorage + auto-restore on page load.
 */

document.addEventListener("DOMContentLoaded", () => {
  // ── DOM Elements ──
  const form = document.getElementById("scrapeForm");
  const keywordInput = document.getElementById("keyword");
  const placeInput = document.getElementById("place");
  const btnScrape = document.getElementById("btnScrape");
  const btnStop = document.getElementById("btnStop");
  const btnDownload = document.getElementById("btnDownload");
  const btnStartContacts = document.getElementById("btnStartContacts");
  const mapContainer = document.getElementById("mapSelector");
  const mapSection = document.getElementById("mapSectionWrapper");
  const btnUseMapSelection = document.getElementById("btnUseMapSelection");
  const btnClearMapSelection = document.getElementById("btnClearMapSelection");
  const mapSelectionStatus = document.getElementById("mapSelectionStatus");
  const previewQuery = document.getElementById("previewQuery");
  const progressSection = document.getElementById("progressSection");
  const progressBar = document.getElementById("progressBar");
  const progressMessage = document.getElementById("progressMessage");
  const progressPercent = document.getElementById("progressPercent");
  const progressTitle = document.getElementById("progressTitle");
  const coverageBadge = document.getElementById("coverageBadge");
  const keywordsExpanded = document.getElementById("keywordsExpanded");
  const resultsSection = document.getElementById("resultsSection");
  const resultsBody = document.getElementById("resultsBody");
  const resultCount = document.getElementById("resultCount");
  const filterInput = document.getElementById("filterInput");
  const errorSection = document.getElementById("errorSection");
  const errorMessage = document.getElementById("errorMessage");
  const leadLimitRange = document.getElementById("leadLimitRange");
  const leadLimitValue = document.getElementById("leadLimitValue");
  const progressLogs = document.getElementById("progressLogs");
  const resultsSkeleton = document.getElementById("resultsSkeleton");
  const stateDot = document.getElementById("stateDot");
  const phaseIndicator = document.getElementById("phaseIndicator");
  const phaseDetail = document.getElementById("phaseDetail");
  const statWebsitesWrap = document.getElementById("statWebsitesWrap");

  // Timer & live stats elements
  const elapsedTimer = document.getElementById("elapsedTimer");
  const liveStats = document.getElementById("liveStats");
  const statAreas = document.getElementById("statAreas");
  const statLeads = document.getElementById("statLeads");
  const statSpeed = document.getElementById("statSpeed");
  const statETA = document.getElementById("statETA");
  const statWebsites = document.getElementById("statWebsites");

  let currentJobId = null;
  let eventSource = null;
  let allLeads = [];
  let selectedMapArea = null;
  let map = null;
  let drawnItems = null;
  let rectangleDrawer = null;
  let sortColumn = null;
  let sortDirection = "asc";
  let latestPhase = "extract";
  let latestContactsStatus = "pending";
  let lastRenderedLogCount = 0;

  // Fallback polling (used only if SSE fails repeatedly)
  let pollTimeout = null;
  let pollStartTime = null;
  let sseFailCount = 0;
  const SSE_FAIL_THRESHOLD = 5; // fall back to polling after this many SSE failures

  // ── Timer ──
  let timerInterval = null;
  let timerStartTime = null;

  function startTimer() {
    timerStartTime = Date.now();
    timerInterval = setInterval(() => {
      const elapsed = Math.floor((Date.now() - timerStartTime) / 1000);
      const h = String(Math.floor(elapsed / 3600)).padStart(2, "0");
      const m = String(Math.floor((elapsed % 3600) / 60)).padStart(2, "0");
      const s = String(elapsed % 60).padStart(2, "0");
      elapsedTimer.innerHTML = `<i class="bi bi-clock me-1"></i>${h}:${m}:${s}`;
    }, 1000);
  }

  function stopTimer() {
    if (timerInterval) {
      clearInterval(timerInterval);
      timerInterval = null;
    }
  }

  // ── State Dot Management ──
  function updateStateDot(status) {
    if (!stateDot) return;
    stateDot.className = "state-dot";
    switch (status) {
      case "running":
        stateDot.classList.add("state-dot--running");
        break;
      case "completed":
      case "stopped":
        stateDot.classList.add("state-dot--success");
        break;
      case "failed":
        stateDot.classList.add("state-dot--error");
        break;
      default:
        stateDot.classList.add("state-dot--idle");
    }
  }

  // ── Live Stats Update ──
  function updateLiveStats(data) {
    if (!data) return;
    if (liveStats) liveStats.style.display = "";

    const as = data.area_stats || {};
    const totalCells =
      data.total_cells || as.geo_cells_total || as.total_areas || 0;
    const completedCells =
      data.completed_cells || as.geo_cells_completed || as.completed_areas || 0;
    const leadCount =
      data.results_count || data.lead_count || as.leads_found || allLeads.length || 0;

    if (totalCells > 0) {
      statAreas.textContent = `${completedCells} / ${totalCells}`;
    } else {
      statAreas.textContent = "0 / 0";
    }
    statLeads.textContent = leadCount;

    // Speed (leads/min)
    const speed = data.speed || 0;
    if (statSpeed) {
      statSpeed.textContent = speed > 0 ? speed.toFixed(1) : "—";
    }

    // ETA
    const etaSeconds = data.eta_seconds;
    if (statETA) {
      if (etaSeconds && etaSeconds > 0) {
        const m = Math.floor(etaSeconds / 60);
        const s = etaSeconds % 60;
        statETA.textContent = m > 0 ? `~${m}m ${s}s` : `~${s}s`;
      } else {
        statETA.textContent = "—";
      }
    }

    // Phase detail
    if (phaseIndicator && phaseDetail && data.phase_detail) {
      phaseIndicator.style.display = "";
      phaseDetail.textContent = data.phase_detail;
    }

    // Websites (during contacts phase)
    if ((as.websites_total || 0) > 0 && statWebsitesWrap) {
      statWebsitesWrap.style.display = "";
      statWebsitesWrap.classList.remove("is-hidden");
      if (statWebsites) {
        statWebsites.textContent = `${as.websites_scanned || 0} / ${as.websites_total || 0}`;
      }
    }

    // Coverage quality indicator
    if (coverageBadge && as.coverage_score > 0) {
      const score = as.coverage_score;
      let label, cls;
      if (score >= 90) {
        label = "High";
        cls = "bg-success";
      } else if (score >= 60) {
        label = "Medium";
        cls = "bg-warning text-dark";
      } else {
        label = "Low";
        cls = "bg-danger";
      }
      coverageBadge.innerHTML = `<span class="badge ${cls}">Coverage: ${score}% (${label})</span>`;
      coverageBadge.style.display = "";
    }

    // Keywords expanded
    if (
      keywordsExpanded &&
      as.keywords_expanded &&
      as.keywords_expanded.length > 1
    ) {
      keywordsExpanded.innerHTML = `<small class="text-muted"><i class="bi bi-tags me-1"></i>Keywords: ${as.keywords_expanded.join(", ")}</small>`;
      keywordsExpanded.style.display = "";
    }
  }

  // ── Timer Sync ──
  function syncTimerWithServer(elapsedSeconds) {
    if (elapsedSeconds && elapsedTimer) {
      const h = String(Math.floor(elapsedSeconds / 3600)).padStart(2, "0");
      const m = String(Math.floor((elapsedSeconds % 3600) / 60)).padStart(
        2,
        "0",
      );
      const s = String(elapsedSeconds % 60).padStart(2, "0");
      elapsedTimer.innerHTML = `<i class="bi bi-clock me-1"></i>${h}:${m}:${s}`;
    }
  }

  // ── Search Preview ──
  function updatePreview() {
    const kw = keywordInput.value.trim() || "keyword";
    const pl = placeInput.value.trim() || "place";
    previewQuery.textContent = `${kw} in ${pl}`;
  }

  function formatCoordinate(v) {
    return Number(v).toFixed(6);
  }

  // ── Map Selection ──
  function updateMapSelectionStatus() {
    if (!mapSelectionStatus) return;
    if (!selectedMapArea) {
      mapSelectionStatus.textContent = "No map area selected.";
      return;
    }

    const center = selectedMapArea.center;
    mapSelectionStatus.textContent = `Selected area center: ${formatCoordinate(center.lat)}, ${formatCoordinate(center.lng)}`;
  }

  function clearMapSelection() {
    selectedMapArea = null;
    if (drawnItems) {
      drawnItems.clearLayers();
    }
    updateMapSelectionStatus();
  }

  function setMapSelectionFromBounds(bounds) {
    const northEast = bounds.getNorthEast();
    const southWest = bounds.getSouthWest();
    const center = bounds.getCenter();

    selectedMapArea = {
      center: { lat: center.lat, lng: center.lng },
      bounds: {
        north: northEast.lat,
        east: northEast.lng,
        south: southWest.lat,
        west: southWest.lng,
      },
    };

    placeInput.value = `${formatCoordinate(center.lat)}, ${formatCoordinate(center.lng)}`;
    updatePreview();
    updateMapSelectionStatus();
  }

  function initMapSelector() {
    if (!mapContainer || !window.L) return;

    map = L.map("mapSelector", {
      zoomControl: true,
    }).setView([25.2048, 55.2708], 10);

    L.tileLayer("https://mt1.google.com/vt/lyrs=m&hl=en&x={x}&y={y}&z={z}", {
      maxZoom: 19,
      attribution: "&copy; Google",
    }).addTo(map);

    drawnItems = new L.FeatureGroup();
    map.addLayer(drawnItems);

    const drawControl = new L.Control.Draw({
      draw: {
        polygon: false,
        polyline: false,
        circle: false,
        marker: false,
        circlemarker: false,
        rectangle: {
          shapeOptions: {
            color: "#4f8cff",
            weight: 2,
          },
        },
      },
      edit: {
        featureGroup: drawnItems,
        edit: false,
        remove: false,
      },
    });
    map.addControl(drawControl);

    rectangleDrawer = new L.Draw.Rectangle(map, {
      shapeOptions: {
        color: "#4f8cff",
        weight: 2,
      },
    });

    map.on(L.Draw.Event.CREATED, (event) => {
      drawnItems.clearLayers();
      const layer = event.layer;
      drawnItems.addLayer(layer);
      if (layer.getBounds) {
        setMapSelectionFromBounds(layer.getBounds());
      }
    });

    if (btnUseMapSelection) {
      btnUseMapSelection.addEventListener("click", () => {
        if (rectangleDrawer) {
          rectangleDrawer.enable();
        }
      });
    }

    if (btnClearMapSelection) {
      btnClearMapSelection.addEventListener("click", () => {
        clearMapSelection();
      });
    }

    updateMapSelectionStatus();
  }

  keywordInput.addEventListener("input", updatePreview);
  placeInput.addEventListener("input", updatePreview);

  function updateLeadLimitLabel() {
    if (!leadLimitRange || !leadLimitValue) return;
    const v = parseInt(leadLimitRange.value || "0", 10);
    leadLimitValue.textContent = v > 0 ? String(v) : "Max";
  }
  if (leadLimitRange) {
    leadLimitRange.addEventListener("input", updateLeadLimitLabel);
    updateLeadLimitLabel();
  }

  // ── Log Rendering ──
  function appendLog(message, percent, timestamp) {
    if (!progressLogs) return;
    const entry = document.createElement("div");
    entry.className = "log-entry";
    const ts = timestamp
      ? new Date(timestamp).toLocaleTimeString()
      : new Date().toLocaleTimeString();
    const pct = Number.isFinite(percent)
      ? `${Math.max(0, Math.min(100, percent))}%`
      : "";
    entry.innerHTML = `
      <span class="log-ts">${ts}</span>
      ${pct ? `<span class="log-pct">${pct}</span>` : ""}
      <span class="log-msg">${escapeHtml(message || "")}</span>
    `;
    progressLogs.appendChild(entry);
    progressLogs.scrollTop = progressLogs.scrollHeight;
  }

  function appendLogs(logs) {
    if (!progressLogs || !Array.isArray(logs)) return;
    const recent = logs.slice(-120);
    const newEntries = recent.slice(lastRenderedLogCount);

    newEntries.forEach((l) => {
      appendLog(l.message, l.progress, l.at);
    });

    lastRenderedLogCount = recent.length;
  }

  initMapSelector();

  // ── Ripple Effect ──
  function addRippleEffect(button) {
    if (!button) return;
    button.addEventListener("click", function (e) {
      const ripple = document.createElement("span");
      ripple.className = "btn-ripple-effect";
      const rect = this.getBoundingClientRect();
      const size = Math.max(rect.width, rect.height);
      ripple.style.width = ripple.style.height = size + "px";
      ripple.style.left = e.clientX - rect.left - size / 2 + "px";
      ripple.style.top = e.clientY - rect.top - size / 2 + "px";
      this.appendChild(ripple);
      setTimeout(() => ripple.remove(), 600);
    });
  }

  // Apply ripple to action buttons
  document
    .querySelectorAll(".action-btn, .btn-primary, .btn-success")
    .forEach(addRippleEffect);

  // ============================================================
  // SSE STREAMING (replaces polling)
  // ============================================================

  /**
   * Connect to the SSE stream for the given job.
   * Receives real-time events and updates the UI incrementally.
   */
  function startStream(jobId) {
    stopStream(); // close any existing stream
    sseFailCount = 0;

    eventSource = new EventSource(`/api/stream/${jobId}`);

    // ── job_started ──
    eventSource.addEventListener("job_started", (e) => {
      const d = JSON.parse(e.data);
      appendLog(`Job started: ${d.keyword} in ${d.place}`, 0);
      updateStateDot("running");
    });

    // ── geocell_progress ──
    eventSource.addEventListener("geocell_progress", (e) => {
      const d = JSON.parse(e.data);
      updateProgress(d.percent, d.message);
      updateLiveStats(d);
      appendLog(d.message, d.percent);

      // Update live speed calculation from lead count
      if (timerStartTime && d.results_count > 0) {
        const elapsedSec = (Date.now() - timerStartTime) / 1000;
        const speed = elapsedSec > 10 ? ((d.results_count / elapsedSec) * 60) : 0;
        if (statSpeed) statSpeed.textContent = speed > 0 ? speed.toFixed(1) : "—";

        // ETA
        if (statETA && d.percent > 5 && d.percent < 100) {
          const etaSec = Math.round((elapsedSec / d.percent) * (100 - d.percent));
          const m = Math.floor(etaSec / 60);
          const s = etaSec % 60;
          statETA.textContent = m > 0 ? `~${m}m ${s}s` : `~${s}s`;
        }
      }
    });

    // ── lead_found ── (core: per-lead streaming into table)
    eventSource.addEventListener("lead_found", (e) => {
      const d = JSON.parse(e.data);
      const lead = d.lead;
      const index = d.index;

      // Add to allLeads array
      allLeads[index] = lead;

      // Update count badge
      resultCount.textContent = allLeads.length;
      if (statLeads) statLeads.textContent = allLeads.length;

      // Show results section + hide skeleton
      hideResultsSkeleton();
      showResults();

      // Append this single row (NOT re-render entire table)
      appendLeadRow(lead, index);
    });

    // ── crawl_started ── (contacts phase begins for a chunk)
    eventSource.addEventListener("crawl_started", (e) => {
      const d = JSON.parse(e.data);
      latestPhase = "contacts";
      progressTitle.textContent = "Contact retrieval in progress...";
      if (btnStartContacts) btnStartContacts.style.display = "none";
      appendLog(
        `Crawling websites: chunk ${d.chunk_index}/${d.total_chunks} (${d.lead_count} leads)`,
        null,
      );

      // Mark all visible rows as "pending crawl"
      if (statWebsitesWrap) {
        statWebsitesWrap.style.display = "";
        statWebsitesWrap.classList.remove("is-hidden");
      }
    });

    // ── contact_found ── (per-lead email/socials enrichment)
    eventSource.addEventListener("contact_found", (e) => {
      const d = JSON.parse(e.data);
      const idx = d.lead_index;

      // Update the lead in allLeads
      if (allLeads[idx]) {
        if (d.email && d.email !== "N/A") allLeads[idx].email = d.email;
        if (d.phone && d.phone !== "N/A") allLeads[idx].phone = d.phone;
        if (d.socials) {
          for (const [k, v] of Object.entries(d.socials)) {
            if (v && v !== "N/A") allLeads[idx][k] = v;
          }
        }
      }

      // Update the specific row in the DOM (not full re-render)
      updateRowContacts(idx, d);
    });

    // ── job_completed ── (terminal event)
    eventSource.addEventListener("job_completed", (e) => {
      const d = JSON.parse(e.data);
      stopStream();
      stopTimer();
      clearPersistedSession();

      const phase = d.phase || "extract";
      const status = d.status || "COMPLETED";

      progressTitle.textContent =
        phase === "contacts"
          ? "Contact retrieval complete!"
          : "List extraction complete!";
      progressBar.classList.remove("progress-bar-animated");
      progressBar.classList.add("progress-bar-complete");
      updateProgress(100, d.message || "Done!");
      updateStateDot("completed");
      setFormEnabled(true);
      showMapSection();
      hideResultsSkeleton();

      // Show contact retrieval button if extraction just finished
      if (phase === "extract" && btnStartContacts) {
        btnStartContacts.style.display = "";
      } else if (btnStartContacts) {
        btnStartContacts.style.display = "none";
      }
      if (phaseIndicator) phaseIndicator.style.display = "none";
      updateExecutionMode({ execution_mode: "cloud" });

      appendLog(d.message || "Job completed", 100);

      // Load final results via REST to ensure we have everything
      loadResults();
    });

    // ── job_failed ──
    eventSource.addEventListener("job_failed", (e) => {
      const d = JSON.parse(e.data);
      stopStream();
      stopTimer();
      clearPersistedSession();
      updateStateDot("failed");
      hideResultsSkeleton();

      if (allLeads.length > 0) {
        progressTitle.textContent = `Error — but ${allLeads.length} leads saved`;
        progressBar.classList.remove("progress-bar-animated");
        progressBar.classList.add("progress-bar-complete");
      } else {
        hideProgress();
        showError(d.error || d.message || "Scraping failed.");
      }
      setFormEnabled(true);
      showMapSection();
    });

    // ── stream_timeout / stream_closed ──
    eventSource.addEventListener("stream_timeout", () => {
      stopStream();
    });
    eventSource.addEventListener("stream_closed", () => {
      stopStream();
    });

    // ── SSE error handler with fallback ──
    eventSource.onerror = () => {
      sseFailCount++;
      if (sseFailCount >= SSE_FAIL_THRESHOLD) {
        console.warn("SSE failed repeatedly, falling back to polling");
        stopStream();
        startPolling();
      }
      // EventSource auto-reconnects on transient errors
    };
  }

  function stopStream() {
    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
  }

  // ============================================================
  // PER-ROW TABLE MANAGEMENT (SSE-driven incremental updates)
  // ============================================================

  /**
   * Append a single lead row to the table with slide-in animation.
   * Called once per `lead_found` SSE event — NOT a full re-render.
   */
  function appendLeadRow(lead, index) {
    const row = document.createElement("tr");
    row.className = "lead-row lead-row-new";
    row.dataset.leadIndex = index;
    row.dataset.state = "populated";
    row.innerHTML = buildRowHTML(lead, index);
    resultsBody.appendChild(row);

    // Trigger slide-in animation
    requestAnimationFrame(() => {
      row.classList.add("lead-row-visible");
    });
  }

  /**
   * Update an existing row's contact fields (email, phone, socials)
   * with a green flash animation. Called per `contact_found` event.
   */
  function updateRowContacts(index, data) {
    const row = resultsBody.querySelector(`tr[data-lead-index="${index}"]`);
    if (!row) return;

    row.dataset.state = "enriched";

    // Update email cell
    const emailCell = row.querySelector(".cell-email");
    if (emailCell && data.email && data.email !== "N/A") {
      const emailLink = `<a href="mailto:${escapeHtml(data.email.split(";")[0].trim())}">${escapeHtml(truncate(data.email, 28))}</a>`;
      emailCell.innerHTML = emailLink;
      emailCell.classList.add("contact-found-flash");
      setTimeout(() => emailCell.classList.remove("contact-found-flash"), 1500);
    }

    // Update phone cell
    const phoneCell = row.querySelector(".cell-phone");
    if (phoneCell && data.phone && data.phone !== "N/A") {
      phoneCell.textContent = data.phone;
    }

    // Update socials cell
    const socialsCell = row.querySelector(".cell-socials");
    if (socialsCell && data.socials) {
      const socialsHtml = buildSocialsHTML(data.socials);
      if (socialsHtml !== "N/A") {
        socialsCell.innerHTML = socialsHtml;
        socialsCell.classList.add("contact-found-flash");
        setTimeout(() => socialsCell.classList.remove("contact-found-flash"), 1500);
      }
    }
  }

  /**
   * Build the HTML content for a single table row.
   */
  function buildRowHTML(lead, idx) {
    const website =
      lead.website && lead.website !== "N/A"
        ? `<a href="${escapeUrl(lead.website)}" target="_blank" rel="noopener">${escapeHtml(truncate(lead.website, 25))}</a>`
        : "N/A";

    const email =
      lead.email && lead.email !== "N/A"
        ? `<a href="mailto:${escapeHtml(lead.email.split(";")[0].trim())}">${escapeHtml(truncate(lead.email, 28))}</a>`
        : '<span class="text-muted">—</span>';

    const socialsHtml = buildSocialsHTML(lead);

    return `
      <td>${idx + 1}</td>
      <td class="fw-semibold">${escapeHtml(lead.business_name)}</td>
      <td>${escapeHtml(lead.owner_name)}</td>
      <td class="cell-phone">${escapeHtml(lead.phone)}</td>
      <td class="cell-truncate-sm cell-email">${email}</td>
      <td class="cell-truncate-sm">${website}</td>
      <td class="cell-socials" style="white-space:nowrap">${socialsHtml}</td>
      <td class="cell-truncate">${escapeHtml(lead.address)}</td>
      <td>${escapeHtml(lead.rating)}</td>
      <td>${escapeHtml(lead.reviews)}</td>
      <td><span class="badge bg-secondary">${escapeHtml(lead.category)}</span></td>
    `;
  }

  function buildSocialsHTML(lead) {
    const socials = [];
    const socialPlatforms = [
      { key: "facebook", icon: "bi-facebook", color: "#1877F2" },
      { key: "instagram", icon: "bi-instagram", color: "#E4405F" },
      { key: "twitter", icon: "bi-twitter-x", color: "#fff" },
      { key: "linkedin", icon: "bi-linkedin", color: "#0A66C2" },
      { key: "youtube", icon: "bi-youtube", color: "#FF0000" },
      { key: "tiktok", icon: "bi-tiktok", color: "#fff" },
      { key: "pinterest", icon: "bi-pinterest", color: "#E60023" },
    ];
    socialPlatforms.forEach((p) => {
      const val = lead[p.key] || (lead.socials && lead.socials[p.key]);
      if (val && val !== "N/A") {
        socials.push(
          `<a href="${escapeUrl(val)}" target="_blank" rel="noopener" title="${p.key}" style="color:${p.color};margin-right:4px;"><i class="bi ${p.icon}"></i></a>`,
        );
      }
    });
    return socials.length > 0 ? socials.join("") : "N/A";
  }

  /**
   * Full re-render of all leads. Used for sorting, filtering, and final load.
   */
  function renderLeads(leads) {
    resultsBody.innerHTML = "";

    if (leads.length === 0) {
      resultsBody.innerHTML = `
        <tr>
          <td colspan="11" class="text-center text-muted py-4">
            No results found.
          </td>
        </tr>`;
      return;
    }

    leads.forEach((lead, idx) => {
      const row = document.createElement("tr");
      row.className = "lead-row lead-row-visible";
      row.dataset.leadIndex = idx;
      row.dataset.state = "populated";
      row.innerHTML = buildRowHTML(lead, idx);
      resultsBody.appendChild(row);
    });
  }

  // ============================================================
  // SESSION PERSISTENCE (localStorage)
  // ============================================================

  function persistSession(jobId, keyword, place) {
    localStorage.setItem(
      "leadgen_active_job",
      JSON.stringify({
        job_id: jobId,
        keyword: keyword,
        place: place,
        started_at: Date.now(),
      }),
    );
  }

  function clearPersistedSession() {
    localStorage.removeItem("leadgen_active_job");
  }

  /**
   * On page load: check if there's an active session to restore.
   */
  async function restoreSession() {
    const saved = JSON.parse(
      localStorage.getItem("leadgen_active_job") || "null",
    );
    if (!saved || !saved.job_id) return;

    // Don't restore sessions older than 2 hours
    if (Date.now() - saved.started_at > 2 * 60 * 60 * 1000) {
      clearPersistedSession();
      return;
    }

    try {
      const res = await fetch(`/api/status/${saved.job_id}`);
      if (!res.ok) {
        clearPersistedSession();
        return;
      }
      const data = await res.json();

      currentJobId = saved.job_id;

      // Fill form fields
      if (saved.keyword) keywordInput.value = saved.keyword;
      if (saved.place) placeInput.value = saved.place;
      updatePreview();

      if (data.status === "running") {
        // Job is still running — restore UI and reconnect SSE
        showProgress();
        setFormEnabled(false);
        startTimer();
        updateStateDot("running");
        if (mapSection) mapSection.style.display = "none";

        latestPhase = data.phase || "extract";
        latestContactsStatus = data.contacts_status || "pending";

        updateProgress(data.progress || 0, data.message || "Reconnecting...");
        updateLiveStats(data);

        // Restore existing leads
        if (Array.isArray(data.results) && data.results.length > 0) {
          allLeads = data.results;
          resultCount.textContent = allLeads.length;
          renderLeads(allLeads);
          showResults();
        }

        // Restore logs
        if (Array.isArray(data.logs)) {
          appendLogs(data.logs);
        }

        progressTitle.textContent =
          latestPhase === "contacts"
            ? "Contact retrieval in progress..."
            : "List extraction in progress...";

        // Reconnect SSE stream
        startStream(saved.job_id);

      } else if (
        data.status === "completed" ||
        data.status === "stopped"
      ) {
        // Job finished while we were away — show final results
        showProgress();
        progressTitle.textContent =
          data.status === "completed"
            ? "Extraction complete!"
            : `Stopped — ${data.lead_count || 0} leads saved`;
        progressBar.style.width = "100%";
        progressBar.classList.remove("progress-bar-animated");
        progressBar.classList.add("progress-bar-complete");
        updateProgress(100, data.message || "Done");
        updateStateDot(data.status === "completed" ? "completed" : "stopped");
        setFormEnabled(true);

        // Load and show results
        await loadResults();

        // Show contacts button if applicable
        if (
          data.contacts_status === "pending" &&
          data.lead_count > 0 &&
          btnStartContacts
        ) {
          btnStartContacts.style.display = "";
        }

        clearPersistedSession();
      } else {
        clearPersistedSession();
      }
    } catch (err) {
      console.error("Session restore failed:", err);
      clearPersistedSession();
    }
  }

  // ============================================================
  // FORM SUBMISSION
  // ============================================================

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const keyword = keywordInput.value.trim();
    const place = placeInput.value.trim();

    if (!keyword || !place) return;

    // Reset UI
    hideError();
    hideResults();
    showProgress();
    setFormEnabled(false);
    startTimer();
    lastRenderedLogCount = 0;
    allLeads = [];
    latestPhase = "extract";
    latestContactsStatus = "pending";
    updateStateDot("running");

    // Show skeleton loader
    showResultsSkeleton();

    // Hide map section during scraping
    if (mapSection) {
      mapSection.style.display = "none";
    }

    try {
      const res = await fetch("/api/scrape", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          keyword,
          place,
          map_selection: selectedMapArea,
          max_leads:
            leadLimitRange && parseInt(leadLimitRange.value || "0", 10) > 0
              ? parseInt(leadLimitRange.value || "0", 10)
              : null,
        }),
      });

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error || "Failed to start scraping.");
      }

      currentJobId = data.job_id;

      // Persist session for page navigation recovery
      persistSession(data.job_id, keyword, place);

      // Connect SSE stream (replaces polling)
      startStream(data.job_id);
    } catch (err) {
      showError(err.message);
      hideProgress();
      hideResultsSkeleton();
      setFormEnabled(true);
      stopTimer();
      updateStateDot("failed");
    }
  });

  // ── Stop Button ──
  btnStop.addEventListener("click", async () => {
    if (!currentJobId) return;
    btnStop.disabled = true;
    btnStop.innerHTML =
      '<span class="spinner-border spinner-border-sm me-1"></span>Stopping...';
    try {
      const res = await fetch(`/api/stop/${currentJobId}`, { method: "POST" });
      await res.json();
      progressTitle.textContent =
        latestPhase === "contacts"
          ? "Pause requested for contact retrieval..."
          : "Stopped — saving results...";
      progressBar.classList.remove("progress-bar-animated");
      progressBar.classList.add("progress-bar-complete");
      appendLog("Stop requested by user", null);
    } catch (err) {
      console.error("Stop failed:", err);
    } finally {
      btnStop.disabled = false;
      btnStop.innerHTML = '<i class="bi bi-stop-circle me-1"></i>Stop';
    }
  });

  // ── Download Button ──
  btnDownload.addEventListener("click", async () => {
    if (!currentJobId) return;
    try {
      btnDownload.disabled = true;
      btnDownload.innerHTML =
        '<span class="spinner-border spinner-border-sm me-1"></span>Preparing...';

      const res = await fetch(`/api/download/${currentJobId}`);
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || "Download failed.");
      }

      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;

      // Try to extract filename from Content-Disposition header
      const cd = res.headers.get("Content-Disposition");
      if (cd) {
        const match = cd.match(/filename=([^;]+)/);
        a.download = match ? match[1].trim() : `leads_${currentJobId}.csv`;
      } else {
        a.download = `leads_${currentJobId}.csv`;
      }

      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Download failed:", err);
      showError("Download failed: " + err.message);
    } finally {
      btnDownload.disabled = false;
      btnDownload.innerHTML = '<i class="bi bi-download me-1"></i>Download CSV';
    }
  });

  if (btnStartContacts) {
    btnStartContacts.addEventListener("click", async () => {
      if (!currentJobId) return;
      btnStartContacts.disabled = true;
      btnStartContacts.innerHTML =
        '<span class="spinner-border spinner-border-sm me-1"></span>Starting...';
      try {
        const res = await fetch(`/api/gmaps/contacts/start/${currentJobId}`, {
          method: "POST",
        });
        const data = await res.json();
        if (!res.ok)
          throw new Error(data.error || "Failed to start contact retrieval.");
        showProgress();
        progressTitle.textContent = "Contact retrieval in progress...";
        updateStateDot("running");
        latestPhase = "contacts";

        // Re-persist session
        persistSession(currentJobId, keywordInput.value, placeInput.value);

        // Connect SSE for contacts phase
        startStream(currentJobId);
      } catch (err) {
        showError(err.message);
      } finally {
        btnStartContacts.disabled = false;
        btnStartContacts.innerHTML =
          '<i class="bi bi-play-circle me-1"></i>Start Contact Retrieval';
      }
    });
  }

  // ── Filter Results ──
  filterInput.addEventListener("input", () => {
    const term = filterInput.value.toLowerCase();
    renderLeads(
      allLeads.filter((lead) =>
        Object.values(lead).some((v) => String(v).toLowerCase().includes(term)),
      ),
    );
  });

  // ============================================================
  // FALLBACK POLLING (only if SSE fails)
  // ============================================================

  function startPolling() {
    stopPolling();
    pollStartTime = Date.now();
    schedulePoll();
  }

  function stopPolling() {
    if (pollTimeout) {
      clearTimeout(pollTimeout);
      pollTimeout = null;
    }
  }

  function schedulePoll() {
    const elapsed = Date.now() - (pollStartTime || Date.now());
    // 1s for first 3 minutes, then back off to 2s
    const interval = elapsed > 180000 ? 2000 : 1000;

    pollTimeout = setTimeout(async () => {
      // Don't poll when tab is hidden
      if (document.visibilityState === "visible") {
        await pollStatus();
      }
      // Re-schedule as long as we have an active job
      if (currentJobId && pollTimeout !== null) {
        schedulePoll();
      }
    }, interval);
  }

  // Pause/resume polling on tab visibility
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible" && currentJobId && !pollTimeout && !eventSource) {
      schedulePoll();
    }
  });

  async function pollStatus() {
    if (!currentJobId) return;

    try {
      const res = await fetch(`/api/status/${currentJobId}`);
      const data = await res.json();

      latestPhase = data.phase || "extract";
      latestContactsStatus = data.contacts_status || "pending";

      updateProgress(data.progress, data.message);
      updateLiveStats(data);
      appendLogs(data.logs || []);
      updateStateDot(data.status);

      // Phase 3: Show/hide Local Agent execution badge
      updateExecutionMode(data);

      // Sync timer with server
      if (data.elapsed_seconds) {
        syncTimerWithServer(data.elapsed_seconds);
      }

      if (data.status === "completed") {
        stopPolling();
        stopTimer();
        clearPersistedSession();
        progressTitle.textContent =
          latestPhase === "contacts" && latestContactsStatus === "completed"
            ? "Contact retrieval complete!"
            : "List extraction complete!";
        progressBar.classList.remove("progress-bar-animated");
        progressBar.classList.add("progress-bar-complete");
        updateStateDot("completed");
        setFormEnabled(true);
        showMapSection();
        hideResultsSkeleton();
        await loadResults();
        if (latestContactsStatus === "pending" && btnStartContacts) {
          btnStartContacts.style.display = "";
        } else if (btnStartContacts) {
          btnStartContacts.style.display = "none";
        }
        if (phaseIndicator) phaseIndicator.style.display = "none";
        updateExecutionMode({ execution_mode: "cloud" });
      } else if (data.status === "failed") {
        stopPolling();
        stopTimer();
        clearPersistedSession();
        updateStateDot("failed");
        hideResultsSkeleton();
        if (data.lead_count > 0) {
          progressTitle.textContent = `Error — but ${data.lead_count} leads saved`;
          progressBar.classList.remove("progress-bar-animated");
          progressBar.classList.add("progress-bar-complete");
          await loadResults();
        } else {
          hideProgress();
          showError(data.error || "Scraping failed.");
        }
        setFormEnabled(true);
        showMapSection();
        if (phaseIndicator) phaseIndicator.style.display = "none";
        updateExecutionMode({ execution_mode: "cloud" });
      } else if (data.status === "stopped") {
        stopPolling();
        stopTimer();
        clearPersistedSession();
        progressTitle.textContent = `Stopped — ${data.lead_count} leads saved`;
        progressBar.classList.remove("progress-bar-animated");
        progressBar.classList.add("progress-bar-complete");
        updateStateDot("stopped");
        setFormEnabled(true);
        showMapSection();
        hideResultsSkeleton();
        if (data.lead_count > 0) {
          await loadResults();
          if (btnStartContacts) btnStartContacts.style.display = "";
        }
        if (phaseIndicator) phaseIndicator.style.display = "none";
        updateExecutionMode({ execution_mode: "cloud" });
      } else {
        if (latestPhase === "contacts") {
          progressTitle.textContent = "Contact retrieval in progress...";
          if (btnStartContacts) btnStartContacts.style.display = "none";
        } else {
          progressTitle.textContent = "List extraction in progress...";
        }
        if (Array.isArray(data.results) && data.results.length > 0) {
          allLeads = data.results;
          resultCount.textContent = data.results.length;
          renderLeads(allLeads);
          showResults();
          hideResultsSkeleton();
        }
      }
    } catch (err) {
      console.error("Poll failed:", err);
    }
  }

  // Phase 3: Show "Running on Local Agent" badge when execution_mode === 'local'
  function updateExecutionMode(data) {
    const badge = document.getElementById("executionModeBadge");
    if (!badge) return;
    const isLocal = data.execution_mode === "local" || data.execution_mode === "running_local";
    badge.style.display = isLocal ? "inline-flex" : "none";
  }


  async function loadResults() {
    try {
      const res = await fetch(`/api/results/${currentJobId}`);
      const data = await res.json();

      if (res.ok && data.leads) {
        allLeads = data.leads;
        resultCount.textContent = data.total;
        renderLeads(allLeads);
        showResults();
      }
    } catch (err) {
      console.error("Load results failed:", err);
    }
  }

  // ── Column Sorting ──
  function sortLeads(column) {
    if (sortColumn === column) {
      sortDirection = sortDirection === "asc" ? "desc" : "asc";
    } else {
      sortColumn = column;
      sortDirection = "asc";
    }

    const sorted = [...allLeads].sort((a, b) => {
      let va = a[column] || "";
      let vb = b[column] || "";

      // Numeric sort for rating/reviews
      if (column === "rating" || column === "reviews") {
        va = parseFloat(String(va).replace(/,/g, "")) || 0;
        vb = parseFloat(String(vb).replace(/,/g, "")) || 0;
      } else {
        va = String(va).toLowerCase();
        vb = String(vb).toLowerCase();
      }

      if (va < vb) return sortDirection === "asc" ? -1 : 1;
      if (va > vb) return sortDirection === "asc" ? 1 : -1;
      return 0;
    });

    renderLeads(sorted);
    updateSortIndicators();
  }

  function updateSortIndicators() {
    document.querySelectorAll("th[data-sort]").forEach((th) => {
      const icon = th.querySelector(".sort-icon");
      if (!icon) return;
      if (th.dataset.sort === sortColumn) {
        icon.className = `bi sort-icon ${sortDirection === "asc" ? "bi-caret-up-fill" : "bi-caret-down-fill"}`;
        icon.style.opacity = "1";
      } else {
        icon.className = "bi bi-caret-up sort-icon";
        icon.style.opacity = "0.3";
      }
    });
  }

  // Attach sort listeners after DOM ready
  document.querySelectorAll("th[data-sort]").forEach((th) => {
    th.style.cursor = "pointer";
    th.addEventListener("click", () => sortLeads(th.dataset.sort));
  });

  // ── UI Helpers ──
  function showProgress() {
    progressSection.style.display = "";
    progressBar.style.width = "0%";
    progressBar.classList.add("progress-bar-animated");
    progressBar.classList.remove("progress-bar-complete");
    progressMessage.textContent = "Starting...";
    if (progressPercent) progressPercent.textContent = "0%";
    progressTitle.textContent = "Scraping in progress...";
    updateStateDot("running");
    if (liveStats) liveStats.style.display = "none";
    if (coverageBadge) coverageBadge.style.display = "none";
    if (keywordsExpanded) keywordsExpanded.style.display = "none";
    if (elapsedTimer)
      elapsedTimer.innerHTML = '<i class="bi bi-clock me-1"></i>00:00:00';
    if (progressLogs) progressLogs.innerHTML = "";
    if (phaseIndicator) phaseIndicator.style.display = "none";
    if (statWebsitesWrap) {
      statWebsitesWrap.style.display = "none";
      statWebsitesWrap.classList.add("is-hidden");
    }
    lastRenderedLogCount = 0;
  }

  function hideProgress() {
    progressSection.style.display = "none";
  }

  function showMapSection() {
    if (mapSection) {
      mapSection.style.display = "";
      // Invalidate map size after show (Leaflet needs this)
      if (map) setTimeout(() => map.invalidateSize(), 200);
    }
  }

  function showResults() {
    resultsSection.style.display = "";
  }

  function hideResults() {
    resultsSection.style.display = "none";
    resultsBody.innerHTML = "";
    allLeads = [];
    filterInput.value = "";
  }

  function showResultsSkeleton() {
    if (resultsSkeleton) resultsSkeleton.style.display = "";
    if (resultsSkeleton) resultsSkeleton.classList.remove("is-hidden");
  }

  function hideResultsSkeleton() {
    if (resultsSkeleton) resultsSkeleton.style.display = "none";
    if (resultsSkeleton) resultsSkeleton.classList.add("is-hidden");
  }

  function showError(msg) {
    errorSection.style.display = "";
    errorMessage.textContent = msg;
  }

  function hideError() {
    errorSection.style.display = "none";
  }

  function setFormEnabled(enabled) {
    keywordInput.disabled = !enabled;
    placeInput.disabled = !enabled;
    btnScrape.disabled = !enabled;
    if (btnUseMapSelection) btnUseMapSelection.disabled = !enabled;
    if (btnClearMapSelection) btnClearMapSelection.disabled = !enabled;
    if (enabled) {
      btnScrape.innerHTML = '<i class="bi bi-rocket-takeoff me-1"></i>Go';
    } else {
      btnScrape.innerHTML =
        '<span class="spinner-border spinner-border-sm me-1"></span>Working...';
    }
  }

  function updateProgress(pct, msg) {
    progressBar.style.width = `${pct}%`;
    progressMessage.textContent = msg;
    if (progressPercent) {
      progressPercent.textContent = `${Math.round(pct)}%`;
    }
  }

  // ── Utilities ──
  function escapeHtml(str) {
    if (!str) return "";
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function escapeUrl(url) {
    if (!url) return "#";
    if (!url.startsWith("http")) url = "https://" + url;
    return encodeURI(url);
  }

  function truncate(str, len) {
    if (!str) return "";
    return str.length > len ? str.substring(0, len) + "..." : str;
  }

  // ── Init: Restore session on page load ──
  restoreSession();
});
