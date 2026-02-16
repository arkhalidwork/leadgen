/**
 * LeadGen — Instagram Tool Frontend Logic
 * Supports: Profile Search & Business Search modes
 */

document.addEventListener("DOMContentLoaded", () => {
  const form = document.getElementById("instagramForm");
  const keywordsInput = document.getElementById("keywords");
  const placeInput = document.getElementById("place");
  const searchTypeSelect = document.getElementById("searchType");
  const keywordsLabel = document.getElementById("keywordsLabel");
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
  const leadsTableCard = document.getElementById("leadsTableCard");
  const leadsBody = document.getElementById("leadsBody");
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

  // ---- Timer helpers ---------------------------------------------------

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

  // ---- Live stats ------------------------------------------------------

  function updateLiveStats(data) {
    const liveStats = document.getElementById("liveStats");
    if (!data.scrape_stats) return;
    if (liveStats) liveStats.style.display = "";
    const stats = data.scrape_stats;

    const statQueries = document.getElementById("statQueries");
    const statLeads = document.getElementById("statLeads");
    const statParsed = document.getElementById("statParsed");
    const statEngines = document.getElementById("statEngines");

    if (statQueries)
      statQueries.textContent = `${stats.queries_completed || 0} / ${stats.total_queries || 0}`;
    if (statLeads) statLeads.textContent = stats.leads_found || 0;
    if (statParsed)
      statParsed.textContent = stats.total_results
        ? `${stats.results_parsed || 0} / ${stats.total_results}`
        : "—";
    if (statEngines) {
      const d = stats.ddg_results || 0;
      const g = stats.google_results || 0;
      const b = stats.bing_results || 0;
      statEngines.textContent = `${d} / ${g} / ${b}`;
    }
  }

  // ---- Mode preview ----------------------------------------------------

  function updatePreview() {
    const kw = keywordsInput.value.trim();
    const p = placeInput.value.trim() || "place";
    const t = searchTypeSelect.value;

    if (t === "profiles") {
      keywordsLabel.textContent = "Industry / Keyword";
      keywordsInput.placeholder = "e.g. real estate, marketing, CEO";
      const extra = kw ? ` "${kw}"` : "";
      previewQuery.textContent = `Instagram profiles with${extra} in "${p}"`;
    } else {
      keywordsLabel.textContent = "Business Niche";
      keywordsInput.placeholder = "e.g. real estate, restaurant, gym";
      const extra = kw ? ` "${kw}"` : "";
      previewQuery.textContent = `Instagram businesses for${extra} in "${p}"`;
    }
  }

  keywordsInput.addEventListener("input", updatePreview);
  placeInput.addEventListener("input", updatePreview);
  searchTypeSelect.addEventListener("change", updatePreview);
  updatePreview(); // initial render

  // ---- Form submit -----------------------------------------------------

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const keywords = keywordsInput.value.trim();
    const place = placeInput.value.trim();
    const searchType = searchTypeSelect.value;

    if (!keywords || !place) return;

    currentSearchType = searchType;
    hideError();
    hideResults();
    showProgress();
    setFormEnabled(false);

    try {
      const res = await fetch("/api/instagram/scrape", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ keywords, place, search_type: searchType }),
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

  // ---- Stop ------------------------------------------------------------

  btnStop.addEventListener("click", async () => {
    if (!currentJobId) return;
    try {
      const res = await fetch(`/api/instagram/stop/${currentJobId}`, {
        method: "POST",
      });
      const data = await res.json();
      stopPolling();
      stopTimer();
      progressTitle.textContent = "Stopped";
      progressSpinner.style.display = "none";
      setFormEnabled(true);
      if (data.message && data.message.includes("leads saved")) {
        await loadResults();
      }
    } catch (err) {
      console.error("Stop failed:", err);
    }
  });

  // ---- Download --------------------------------------------------------

  btnDownload.addEventListener("click", async () => {
    if (!currentJobId) return;
    try {
      btnDownload.disabled = true;
      btnDownload.innerHTML =
        '<span class="spinner-border spinner-border-sm me-1"></span>Preparing...';

      const res = await fetch(`/api/instagram/download/${currentJobId}`);
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
        a.download = match ? match[1].trim() : `instagram_${currentJobId}.csv`;
      } else {
        a.download = `instagram_${currentJobId}.csv`;
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

  // ---- Filter ----------------------------------------------------------

  filterInput.addEventListener("input", () => {
    const term = filterInput.value.toLowerCase();
    renderLeads(
      allLeads.filter((l) =>
        Object.values(l).some((v) => String(v).toLowerCase().includes(term)),
      ),
    );
  });

  // ---- Polling ---------------------------------------------------------

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
      const res = await fetch(`/api/instagram/status/${currentJobId}`);
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
      const res = await fetch(`/api/instagram/results/${currentJobId}`);
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

  // ---- Render (unified table) ------------------------------------------

  function renderLeads(leads) {
    leadsBody.innerHTML = "";
    if (leads.length === 0) {
      leadsBody.innerHTML = `<tr><td colspan="11" class="text-center text-muted py-4">No results found.</td></tr>`;
      return;
    }
    leads.forEach((l, idx) => {
      const profileLink =
        l.profile_url && l.profile_url !== "N/A"
          ? `<a href="${escapeUrl(l.profile_url)}" target="_blank" rel="noopener"><i class="bi bi-box-arrow-up-right me-1"></i>View</a>`
          : "N/A";

      const emailHtml =
        l.email && l.email !== "N/A"
          ? `<a href="mailto:${escapeHtml(l.email.split(";")[0].trim())}">${escapeHtml(truncate(l.email, 28))}</a>`
          : '<span class="text-muted">—</span>';

      const phoneHtml =
        l.phone && l.phone !== "N/A"
          ? escapeHtml(l.phone)
          : '<span class="text-muted">—</span>';

      const websiteHtml =
        l.website && l.website !== "N/A"
          ? `<a href="${escapeUrl(l.website)}" target="_blank" rel="noopener">${escapeHtml(truncate(l.website, 25))}</a>`
          : '<span class="text-muted">—</span>';

      const categoryHtml =
        l.category && l.category !== "N/A"
          ? `<span class="badge bg-secondary">${escapeHtml(l.category)}</span>`
          : '<span class="text-muted">—</span>';

      const followersHtml =
        l.followers && l.followers !== "N/A"
          ? `<span class="badge bg-info text-dark">${escapeHtml(l.followers)}</span>`
          : '<span class="text-muted">—</span>';

      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${idx + 1}</td>
        <td class="fw-semibold"><span class="badge bg-danger">@${escapeHtml(l.username)}</span></td>
        <td>${escapeHtml(l.display_name)}</td>
        <td>${emailHtml}</td>
        <td>${phoneHtml}</td>
        <td>${websiteHtml}</td>
        <td>${categoryHtml}</td>
        <td>${followersHtml}</td>
        <td>${escapeHtml(l.location)}</td>
        <td>${profileLink}</td>
        <td class="cell-truncate">${escapeHtml(truncate(l.bio, 80))}</td>
      `;
      leadsBody.appendChild(row);
    });
  }

  // ---- UI helpers ------------------------------------------------------

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
    leadsBody.innerHTML = "";
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
    keywordsInput.disabled = !enabled;
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

  function truncate(str, len) {
    if (!str) return "";
    return str.length > len ? str.substring(0, len) + "…" : str;
  }
});
