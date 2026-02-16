/**
 * LeadGen — LinkedIn Tool Frontend Logic
 */

document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("linkedinForm");
  const nicheInput = document.getElementById("niche");
  const placeInput = document.getElementById("place");
  const searchTypeSelect = document.getElementById("searchType");
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
  const profilesTableCard = document.getElementById("profilesTableCard");
  const companiesTableCard = document.getElementById("companiesTableCard");
  const profilesBody = document.getElementById("profilesBody");
  const companiesBody = document.getElementById("companiesBody");
  const resultCount = document.getElementById("resultCount");
  const filterInput = document.getElementById("filterInput");
  const errorSection = document.getElementById("errorSection");
  const errorMessage = document.getElementById("errorMessage");

  let currentJobId = null;
  let pollInterval = null;
  let allLeads = [];
  let currentSearchType = "profiles";
  let timerInterval = null;
  let timerStart = null;

  // Timer helpers
  function startTimer() {
    timerStart = Date.now();
    const el = document.getElementById("elapsedTimer");
    timerInterval = setInterval(() => {
      const diff = Math.floor((Date.now() - timerStart) / 1000);
      const h = String(Math.floor(diff / 3600)).padStart(2, "0");
      const m = String(Math.floor((diff % 3600) / 60)).padStart(2, "0");
      const s = String(diff % 60).padStart(2, "0");
      if (el) el.innerHTML = `<i class="bi bi-clock me-1"></i>${h}:${m}:${s}`;
    }, 1000);
  }

  function stopTimer() {
    if (timerInterval) {
      clearInterval(timerInterval);
      timerInterval = null;
    }
  }

  function syncTimerWithServer(elapsedSeconds) {
    const el = document.getElementById("elapsedTimer");
    if (elapsedSeconds && el) {
      const h = String(Math.floor(elapsedSeconds / 3600)).padStart(2, "0");
      const m = String(Math.floor((elapsedSeconds % 3600) / 60)).padStart(
        2,
        "0",
      );
      const s = String(elapsedSeconds % 60).padStart(2, "0");
      el.innerHTML = `<i class="bi bi-clock me-1"></i>${h}:${m}:${s}`;
    }
  }

  function updateLiveStats(data) {
    const liveStats = document.getElementById("liveStats");
    if (!data.scrape_stats) return;
    if (liveStats) liveStats.style.display = "flex";
    const stats = data.scrape_stats;
    const statQueries = document.getElementById("statQueries");
    const statLeads = document.getElementById("statLeads");
    const statParsed = document.getElementById("statParsed");
    if (statQueries)
      statQueries.textContent = `${stats.queries_completed || 0} / ${stats.total_queries || 0}`;
    if (statLeads) statLeads.textContent = stats.leads_found || 0;
    if (statParsed)
      statParsed.textContent = stats.total_results
        ? `${stats.results_parsed || 0} / ${stats.total_results}`
        : "—";
  }

  // Live preview
  function updatePreview() {
    const n = nicheInput.value.trim() || "niche";
    const p = placeInput.value.trim() || "place";
    const t = searchTypeSelect.value;
    previewQuery.textContent = `LinkedIn ${t} for "${n}" in "${p}"`;
  }

  nicheInput.addEventListener("input", updatePreview);
  placeInput.addEventListener("input", updatePreview);
  searchTypeSelect.addEventListener("change", updatePreview);

  // Form submit
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const niche = nicheInput.value.trim();
    const place = placeInput.value.trim();
    const searchType = searchTypeSelect.value;

    if (!niche || !place) return;

    currentSearchType = searchType;
    hideError();
    hideResults();
    showProgress();
    setFormEnabled(false);

    try {
      const res = await fetch("/api/linkedin/scrape", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ niche, place, search_type: searchType }),
      });

      const data = await res.json();
      if (!res.ok) throw new Error(data.error || "Failed to start scraping.");

      currentJobId = data.job_id;
      startPolling();
      startTimer();
    } catch (err) {
      showError(err.message);
      hideProgress();
      setFormEnabled(true);
    }
  });

  // Stop
  btnStop.addEventListener("click", async () => {
    if (!currentJobId) return;
    try {
      const res = await fetch(`/api/linkedin/stop/${currentJobId}`, {
        method: "POST",
      });
      const data = await res.json();
      stopPolling();
      stopTimer();
      progressTitle.textContent = "Stopped";
      progressSpinner.style.display = "none";
      setFormEnabled(true);
      // Load partial results if available
      if (data.message && data.message.includes("leads saved")) {
        await loadResults();
      }
    } catch (err) {
      console.error("Stop failed:", err);
    }
  });

  // Download — fetch blob for reliable cross-browser download
  btnDownload.addEventListener("click", async () => {
    if (!currentJobId) return;
    try {
      btnDownload.disabled = true;
      btnDownload.innerHTML =
        '<span class="spinner-border spinner-border-sm me-1"></span>Preparing...';

      const res = await fetch(`/api/linkedin/download/${currentJobId}`);
      if (!res.ok) {
        const errData = await res.json().catch(() => ({}));
        throw new Error(errData.error || "Download failed.");
      }

      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;

      const cd = res.headers.get("Content-Disposition");
      if (cd) {
        const match = cd.match(/filename=([^;]+)/);
        a.download = match ? match[1].trim() : `linkedin_${currentJobId}.csv`;
      } else {
        a.download = `linkedin_${currentJobId}.csv`;
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

  // Filter
  filterInput.addEventListener("input", () => {
    const term = filterInput.value.toLowerCase();
    renderLeads(
      allLeads.filter((l) =>
        Object.values(l).some((v) => String(v).toLowerCase().includes(term)),
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
      const res = await fetch(`/api/linkedin/status/${currentJobId}`);
      const data = await res.json();

      updateProgress(data.progress, data.message);
      updateLiveStats(data);
      if (data.elapsed_seconds) syncTimerWithServer(data.elapsed_seconds);

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
        hideProgress();
        showError(data.error || "Scraping failed.");
        setFormEnabled(true);
        if (data.lead_count > 0) await loadResults();
      } else if (data.status === "stopped") {
        stopPolling();
        stopTimer();
        progressTitle.textContent = "Stopped";
        progressSpinner.style.display = "none";
        setFormEnabled(true);
        if (data.lead_count > 0) await loadResults();
      }
    } catch (err) {
      console.error("Poll failed:", err);
    }
  }

  async function loadResults() {
    try {
      const res = await fetch(`/api/linkedin/results/${currentJobId}`);
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
    if (currentSearchType === "profiles") {
      profilesTableCard.style.display = "";
      companiesTableCard.style.display = "none";
      renderProfiles(leads);
    } else {
      profilesTableCard.style.display = "none";
      companiesTableCard.style.display = "";
      renderCompanies(leads);
    }
  }

  function renderProfiles(leads) {
    profilesBody.innerHTML = "";
    if (leads.length === 0) {
      profilesBody.innerHTML = `<tr><td colspan="8" class="text-center text-muted py-4">No results found.</td></tr>`;
      return;
    }
    leads.forEach((l, idx) => {
      const profileLink =
        l.profile_url && l.profile_url !== "N/A"
          ? `<a href="${escapeUrl(l.profile_url)}" target="_blank" rel="noopener"><i class="bi bi-box-arrow-up-right me-1"></i>View</a>`
          : "N/A";
      const username =
        l.linkedin_username && l.linkedin_username !== "N/A"
          ? `<span class="badge bg-info text-dark">${escapeHtml(l.linkedin_username)}</span>`
          : "N/A";
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${idx + 1}</td>
        <td class="fw-semibold">${escapeHtml(l.name)}</td>
        <td>${escapeHtml(l.title)}</td>
        <td>${escapeHtml(l.company)}</td>
        <td>${escapeHtml(l.location)}</td>
        <td>${username}</td>
        <td>${profileLink}</td>
        <td class="cell-truncate">${escapeHtml(l.snippet)}</td>
      `;
      profilesBody.appendChild(row);
    });
  }

  function renderCompanies(leads) {
    companiesBody.innerHTML = "";
    if (leads.length === 0) {
      companiesBody.innerHTML = `<tr><td colspan="7" class="text-center text-muted py-4">No results found.</td></tr>`;
      return;
    }
    leads.forEach((l, idx) => {
      const companyLink =
        l.company_url && l.company_url !== "N/A"
          ? `<a href="${escapeUrl(l.company_url)}" target="_blank" rel="noopener"><i class="bi bi-box-arrow-up-right me-1"></i>View</a>`
          : "N/A";
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${idx + 1}</td>
        <td class="fw-semibold">${escapeHtml(l.company_name)}</td>
        <td>${escapeHtml(l.industry)}</td>
        <td>${escapeHtml(l.company_size)}</td>
        <td>${escapeHtml(l.location)}</td>
        <td>${companyLink}</td>
        <td class="cell-truncate">${escapeHtml(l.description)}</td>
      `;
      companiesBody.appendChild(row);
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
    const el = document.getElementById("elapsedTimer");
    if (el) el.innerHTML = '<i class="bi bi-clock me-1"></i>00:00:00';
  }

  function hideProgress() {
    progressSection.style.display = "none";
    const liveStats = document.getElementById("liveStats");
    if (liveStats) liveStats.style.display = "none";
  }

  function showResults() {
    resultsSection.style.display = "";
  }

  function hideResults() {
    resultsSection.style.display = "none";
    profilesBody.innerHTML = "";
    companiesBody.innerHTML = "";
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
    nicheInput.disabled = !enabled;
    placeInput.disabled = !enabled;
    searchTypeSelect.disabled = !enabled;
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
});
