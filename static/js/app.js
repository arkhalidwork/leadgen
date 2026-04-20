/**
 * LeadGen — Frontend Application Logic
 * Handles form submission, polling, results display, and CSV download.
 */

document.addEventListener("DOMContentLoaded", () => {
  // DOM Elements
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
  const progressTitle = document.getElementById("progressTitle");
  const progressSpinner = document.getElementById("progressSpinner");
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

  // Timer & live stats elements
  const elapsedTimer = document.getElementById("elapsedTimer");
  const liveStats = document.getElementById("liveStats");
  const statAreas = document.getElementById("statAreas");
  const statLeads = document.getElementById("statLeads");
  const statWebsites = document.getElementById("statWebsites");

  let currentJobId = null;
  let pollInterval = null;
  let allLeads = [];
  let selectedMapArea = null;
  let map = null;
  let drawnItems = null;
  let rectangleDrawer = null;
  let sortColumn = null;
  let sortDirection = "asc";
  let liveLeadsRendered = false;
  let latestPhase = "extract";
  let latestContactsStatus = "pending";

  // Timer
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

  // Update live stats from server response
  function updateLiveStats(data) {
    if (!data) return;
    liveStats.style.display = "flex";
    const as = data.area_stats || {};
    const totalCells =
      data.total_cells || as.geo_cells_total || as.total_areas || 0;
    const completedCells =
      data.completed_cells || as.geo_cells_completed || as.completed_areas || 0;
    const leadCount =
      data.results_count || data.lead_count || as.leads_found || 0;

    if (totalCells > 0) {
      statAreas.textContent = `${completedCells} / ${totalCells}`;
    } else {
      statAreas.textContent = "0 / 0";
    }
    statLeads.textContent = leadCount;

    if ((as.websites_total || 0) > 0) {
      statWebsites.textContent = `${as.websites_scanned || 0} / ${as.websites_total || 0}`;
    } else if (totalCells > 0 && completedCells < totalCells) {
      statWebsites.textContent = "—";
    } else {
      statWebsites.textContent = "0";
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

  // Sync timer with server elapsed time
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

  // Live preview of search query
  function updatePreview() {
    const kw = keywordInput.value.trim() || "keyword";
    const pl = placeInput.value.trim() || "place";
    previewQuery.textContent = `${kw} in ${pl}`;
  }

  function formatCoordinate(v) {
    return Number(v).toFixed(6);
  }

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

  function appendLogs(logs) {
    if (!progressLogs || !Array.isArray(logs)) return;
    const recent = logs.slice(-120);
    progressLogs.innerHTML = recent
      .map((l) => {
        const ts = l.at ? new Date(l.at).toLocaleTimeString() : "--:--:--";
        const pct = Number.isFinite(l.progress)
          ? `[${Math.max(0, Math.min(100, l.progress))}%] `
          : "";
        return `<div>[${ts}] ${pct}${escapeHtml(l.message || "")}</div>`;
      })
      .join("");
    progressLogs.scrollTop = progressLogs.scrollHeight;
  }

  initMapSelector();

  // Form submit
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
    liveLeadsRendered = false;

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
      startPolling();
    } catch (err) {
      showError(err.message);
      hideProgress();
      setFormEnabled(true);
      stopTimer();
    }
  });

  // Stop button
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
      progressSpinner.style.display = "none";
      progressBar.classList.remove("progress-bar-animated");
      await loadLiveResults();
    } catch (err) {
      console.error("Stop failed:", err);
    } finally {
      btnStop.disabled = false;
      btnStop.innerHTML = '<i class="bi bi-stop-circle me-1"></i>Stop';
    }
  });

  // Download button — fetch blob for reliable cross-browser download
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
        startPolling();
      } catch (err) {
        showError(err.message);
      } finally {
        btnStartContacts.disabled = false;
        btnStartContacts.innerHTML =
          '<i class="bi bi-play-circle me-1"></i>Start Contact Retrieval';
      }
    });
  }

  // Filter results
  filterInput.addEventListener("input", () => {
    const term = filterInput.value.toLowerCase();
    renderLeads(
      allLeads.filter((lead) =>
        Object.values(lead).some((v) => String(v).toLowerCase().includes(term)),
      ),
    );
  });

  // Polling
  function startPolling() {
    stopPolling();
    pollInterval = setInterval(pollStatus, 1500);
  }

  function stopPolling() {
    if (pollInterval) {
      clearInterval(pollInterval);
      pollInterval = null;
    }
  }

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

      // Sync timer with server
      if (data.elapsed_seconds) {
        syncTimerWithServer(data.elapsed_seconds);
      }

      if (data.status === "completed") {
        stopPolling();
        stopTimer();
        progressTitle.textContent =
          latestPhase === "contacts" && latestContactsStatus === "completed"
            ? "Contact retrieval complete!"
            : "List extraction complete!";
        progressSpinner.style.display = "none";
        progressBar.classList.remove("progress-bar-animated");
        setFormEnabled(true);
        showMapSection();
        await loadResults();
        if (latestContactsStatus === "pending" && btnStartContacts) {
          btnStartContacts.style.display = "";
        } else if (btnStartContacts) {
          btnStartContacts.style.display = "none";
        }
      } else if (data.status === "failed") {
        stopPolling();
        stopTimer();
        // Even on failure, try to load partial results
        if (data.lead_count > 0) {
          progressTitle.textContent = `Error — but ${data.lead_count} leads saved`;
          progressSpinner.style.display = "none";
          await loadResults();
        } else {
          hideProgress();
          showError(data.error || "Scraping failed.");
        }
        setFormEnabled(true);
        showMapSection();
      } else if (data.status === "stopped") {
        stopPolling();
        stopTimer();
        progressTitle.textContent = `Stopped — ${data.lead_count} leads saved`;
        progressSpinner.style.display = "none";
        progressBar.classList.remove("progress-bar-animated");
        setFormEnabled(true);
        showMapSection();
        if (data.lead_count > 0) {
          await loadResults();
          if (btnStartContacts) btnStartContacts.style.display = "";
        }
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
        }
        // Still running — render live partial results
        if (data.lead_count > 0 && !liveLeadsRendered) {
          liveLeadsRendered = true;
          await loadLiveResults();
        } else if (data.lead_count > (allLeads.length || 0) + 5) {
          // Refresh every time we have 5+ new leads
          await loadLiveResults();
        }
      }
    } catch (err) {
      console.error("Poll failed:", err);
    }
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

  async function loadLiveResults() {
    try {
      const res = await fetch(`/api/status/${currentJobId}`);
      const data = await res.json();
      if (Array.isArray(data.results) && data.results.length > 0) {
        allLeads = data.results;
        resultCount.textContent = data.results.length;
        renderLeads(allLeads);
        showResults();
        return;
      }
      if (
        (data.area_stats && data.area_stats.leads_found > 0) ||
        data.lead_count > 0
      ) {
        // Try loading partial results even during running state
        try {
          const rr = await fetch(`/api/results/${currentJobId}`);
          const rd = await rr.json();
          if (rd.leads && rd.leads.length > 0) {
            allLeads = rd.leads;
            resultCount.textContent = rd.total;
            renderLeads(allLeads);
            showResults();
          }
        } catch (_) {
          // Results may not be available yet during RUNNING — that's OK
        }
      }
    } catch (err) {
      console.error("Live results failed:", err);
    }
  }

  // Column sorting
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

  // Render
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
      const website =
        lead.website && lead.website !== "N/A"
          ? `<a href="${escapeUrl(lead.website)}" target="_blank" rel="noopener">${escapeHtml(truncate(lead.website, 25))}</a>`
          : "N/A";

      const email =
        lead.email && lead.email !== "N/A"
          ? `<a href="mailto:${escapeHtml(lead.email.split(";")[0].trim())}">${escapeHtml(truncate(lead.email, 28))}</a>`
          : "N/A";

      // Build social icons
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
        if (lead[p.key] && lead[p.key] !== "N/A") {
          socials.push(
            `<a href="${escapeUrl(lead[p.key])}" target="_blank" rel="noopener" title="${p.key}" style="color:${p.color};margin-right:4px;"><i class="bi ${p.icon}"></i></a>`,
          );
        }
      });
      const socialsHtml = socials.length > 0 ? socials.join("") : "N/A";

      const row = document.createElement("tr");
      row.className = "lead-row";
      row.innerHTML = `
                <td>${idx + 1}</td>
                <td class="fw-semibold">${escapeHtml(lead.business_name)}</td>
                <td>${escapeHtml(lead.owner_name)}</td>
                <td>${escapeHtml(lead.phone)}</td>
                <td class="cell-truncate-sm">${email}</td>
                <td class="cell-truncate-sm">${website}</td>
                <td style="white-space:nowrap">${socialsHtml}</td>
                <td class="cell-truncate">${escapeHtml(lead.address)}</td>
                <td>${escapeHtml(lead.rating)}</td>
                <td>${escapeHtml(lead.reviews)}</td>
                <td><span class="badge bg-secondary">${escapeHtml(lead.category)}</span></td>
            `;
      resultsBody.appendChild(row);
    });
  }

  // UI helpers
  function showProgress() {
    progressSection.style.display = "";
    progressBar.style.width = "0%";
    progressBar.classList.add("progress-bar-animated");
    progressMessage.textContent = "Starting...";
    progressTitle.textContent = "Scraping in progress...";
    progressSpinner.style.display = "";
    if (liveStats) liveStats.style.display = "none";
    if (coverageBadge) coverageBadge.style.display = "none";
    if (keywordsExpanded) keywordsExpanded.style.display = "none";
    if (elapsedTimer)
      elapsedTimer.innerHTML = '<i class="bi bi-clock me-1"></i>00:00:00';
    if (progressLogs) progressLogs.innerHTML = "";
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
  }

  // Utilities
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
});
