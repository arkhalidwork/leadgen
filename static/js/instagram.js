/**
 * LeadGen — Instagram Tool Frontend Logic
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
  const emailsTableCard = document.getElementById("emailsTableCard");
  const profilesTableCard = document.getElementById("profilesTableCard");
  const emailsBody = document.getElementById("emailsBody");
  const profilesBody = document.getElementById("profilesBody");
  const resultCount = document.getElementById("resultCount");
  const filterInput = document.getElementById("filterInput");
  const errorSection = document.getElementById("errorSection");
  const errorMessage = document.getElementById("errorMessage");

  let currentJobId = null;
  let pollInterval = null;
  let allLeads = [];
  let currentSearchType = "emails";

  // Update label & preview when mode changes
  function updatePreview() {
    const kw = keywordsInput.value.trim();
    const p = placeInput.value.trim() || "place";
    const t = searchTypeSelect.value;

    if (t === "emails") {
      keywordsLabel.textContent = "Keywords (optional)";
      keywordsInput.placeholder = "e.g. real estate, marketing";
      const extra = kw ? ` for "${kw}"` : "";
      previewQuery.textContent = `Instagram emails in "${p}"${extra}`;
    } else {
      keywordsLabel.textContent = "Niche / Industry";
      keywordsInput.placeholder = "e.g. technology, marketing";
      const extra = kw ? ` "${kw}"` : "";
      previewQuery.textContent = `Instagram CEO/Director/Manager in "${p}"${extra}`;
    }
  }

  keywordsInput.addEventListener("input", updatePreview);
  placeInput.addEventListener("input", updatePreview);
  searchTypeSelect.addEventListener("change", updatePreview);

  // Form submit
  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const keywords = keywordsInput.value.trim();
    const place = placeInput.value.trim();
    const searchType = searchTypeSelect.value;

    if (!place) return;

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
      await fetch(`/api/instagram/stop/${currentJobId}`, { method: "POST" });
      stopPolling();
      progressTitle.textContent = "Stopped";
      progressSpinner.style.display = "none";
      setFormEnabled(true);
    } catch (err) {
      console.error("Stop failed:", err);
    }
  });

  // Download — fetch blob
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
      const res = await fetch(`/api/instagram/status/${currentJobId}`);
      const data = await res.json();

      updateProgress(data.progress, data.message);

      if (data.status === "completed") {
        stopPolling();
        progressTitle.textContent = "Complete!";
        progressSpinner.style.display = "none";
        progressBar.classList.remove("progress-bar-animated");
        setFormEnabled(true);
        await loadResults();
      } else if (data.status === "failed") {
        stopPolling();
        hideProgress();
        showError(data.error || "Scraping failed.");
        setFormEnabled(true);
      } else if (data.status === "stopped") {
        stopPolling();
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

  // Render
  function renderLeads(leads) {
    if (currentSearchType === "emails") {
      emailsTableCard.style.display = "";
      profilesTableCard.style.display = "none";
      renderEmails(leads);
    } else {
      emailsTableCard.style.display = "none";
      profilesTableCard.style.display = "";
      renderProfiles(leads);
    }
  }

  function renderEmails(leads) {
    emailsBody.innerHTML = "";
    if (leads.length === 0) {
      emailsBody.innerHTML = `<tr><td colspan="7" class="text-center text-muted py-4">No results found.</td></tr>`;
      return;
    }
    leads.forEach((l, idx) => {
      const profileLink =
        l.profile_url && l.profile_url !== "N/A"
          ? `<a href="${escapeUrl(l.profile_url)}" target="_blank" rel="noopener"><i class="bi bi-box-arrow-up-right me-1"></i>View</a>`
          : "N/A";
      const emailHtml =
        l.email && l.email !== "N/A"
          ? `<a href="mailto:${escapeHtml(l.email.split(";")[0].trim())}">${escapeHtml(l.email)}</a>`
          : "N/A";
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${idx + 1}</td>
        <td class="fw-semibold"><span class="badge bg-danger">@${escapeHtml(l.username)}</span></td>
        <td>${escapeHtml(l.display_name)}</td>
        <td>${emailHtml}</td>
        <td>${escapeHtml(l.location)}</td>
        <td>${profileLink}</td>
        <td class="cell-truncate">${escapeHtml(l.bio_snippet)}</td>
      `;
      emailsBody.appendChild(row);
    });
  }

  function renderProfiles(leads) {
    profilesBody.innerHTML = "";
    if (leads.length === 0) {
      profilesBody.innerHTML = `<tr><td colspan="9" class="text-center text-muted py-4">No results found.</td></tr>`;
      return;
    }
    leads.forEach((l, idx) => {
      const profileLink =
        l.profile_url && l.profile_url !== "N/A"
          ? `<a href="${escapeUrl(l.profile_url)}" target="_blank" rel="noopener"><i class="bi bi-box-arrow-up-right me-1"></i>View</a>`
          : "N/A";
      const companyUrlHtml =
        l.company_url && l.company_url !== "N/A"
          ? `<a href="${escapeUrl(l.company_url)}" target="_blank" rel="noopener">${escapeHtml(truncate(l.company_url, 30))}</a>`
          : "N/A";
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${idx + 1}</td>
        <td class="fw-semibold"><span class="badge bg-danger">@${escapeHtml(l.username)}</span></td>
        <td>${escapeHtml(l.display_name)}</td>
        <td>${escapeHtml(l.title)}</td>
        <td>${escapeHtml(l.company)}</td>
        <td>${companyUrlHtml}</td>
        <td>${escapeHtml(l.location)}</td>
        <td>${profileLink}</td>
        <td class="cell-truncate">${escapeHtml(l.bio_snippet)}</td>
      `;
      profilesBody.appendChild(row);
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
  }

  function hideProgress() {
    progressSection.style.display = "none";
  }

  function showResults() {
    resultsSection.style.display = "";
  }

  function hideResults() {
    resultsSection.style.display = "none";
    emailsBody.innerHTML = "";
    profilesBody.innerHTML = "";
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
    return str.length > len ? str.substring(0, len) + "..." : str;
  }
});
