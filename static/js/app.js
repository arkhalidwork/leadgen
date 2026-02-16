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
  const previewQuery = document.getElementById("previewQuery");
  const progressSection = document.getElementById("progressSection");
  const progressBar = document.getElementById("progressBar");
  const progressMessage = document.getElementById("progressMessage");
  const progressTitle = document.getElementById("progressTitle");
  const progressSpinner = document.getElementById("progressSpinner");
  const resultsSection = document.getElementById("resultsSection");
  const resultsBody = document.getElementById("resultsBody");
  const resultCount = document.getElementById("resultCount");
  const filterInput = document.getElementById("filterInput");
  const errorSection = document.getElementById("errorSection");
  const errorMessage = document.getElementById("errorMessage");

  // Timer & live stats elements
  const elapsedTimer = document.getElementById("elapsedTimer");
  const liveStats = document.getElementById("liveStats");
  const statAreas = document.getElementById("statAreas");
  const statLeads = document.getElementById("statLeads");
  const statWebsites = document.getElementById("statWebsites");

  let currentJobId = null;
  let pollInterval = null;
  let allLeads = [];

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
    if (!data.area_stats) return;
    liveStats.style.display = "flex";
    const as = data.area_stats;

    if (as.total_areas > 0) {
      statAreas.textContent = `${as.completed_areas} / ${as.total_areas}`;
    }
    statLeads.textContent = as.leads_found || data.lead_count || 0;

    if (as.websites_total > 0) {
      statWebsites.textContent = `${as.websites_scanned} / ${as.websites_total}`;
    } else if (as.completed_areas < as.total_areas) {
      statWebsites.textContent = "—";
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

  keywordInput.addEventListener("input", updatePreview);
  placeInput.addEventListener("input", updatePreview);

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

    try {
      const res = await fetch("/api/scrape", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ keyword, place }),
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
      const data = await res.json();
      stopPolling();
      stopTimer();
      progressTitle.textContent = "Stopped — saving results...";
      progressSpinner.style.display = "none";
      progressBar.classList.remove("progress-bar-animated");
      setFormEnabled(true);
      // Load whatever results were saved
      await loadResults();
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

      updateProgress(data.progress, data.message);
      updateLiveStats(data);

      // Sync timer with server
      if (data.elapsed_seconds) {
        syncTimerWithServer(data.elapsed_seconds);
      }

      if (data.status === "completed") {
        stopPolling();
        stopTimer();
        progressTitle.textContent = "Complete!";
        progressSpinner.style.display = "none";
        progressBar.classList.remove("progress-bar-animated");
        setFormEnabled(true);
        await loadResults();
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
      } else if (data.status === "stopped") {
        stopPolling();
        stopTimer();
        progressTitle.textContent = `Stopped — ${data.lead_count} leads saved`;
        progressSpinner.style.display = "none";
        progressBar.classList.remove("progress-bar-animated");
        setFormEnabled(true);
        if (data.lead_count > 0) {
          await loadResults();
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
    if (elapsedTimer)
      elapsedTimer.innerHTML = '<i class="bi bi-clock me-1"></i>00:00:00';
  }

  function hideProgress() {
    progressSection.style.display = "none";
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
