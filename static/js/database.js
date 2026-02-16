/**
 * Lead Database — LeadGen
 * Full lead browser with filtering, pagination, detail modal,
 * bulk actions, and CSV export.
 */

(function () {
  "use strict";

  const TOOL_LABELS = {
    gmaps: "Google Maps",
    linkedin: "LinkedIn",
    instagram: "Instagram",
    webcrawler: "Web Crawler",
  };

  /* ──── state ──── */
  let currentPage = 1;
  let totalPages = 1;
  let selectedIds = new Set();
  let initialScrapeId = "";

  /* ──── DOM refs ──── */
  const $ = (s) => document.querySelector(s);
  const body = $("#leadsTableBody");
  const searchInput = $("#dbSearch");
  const filterTool = $("#dbFilterTool");
  const filterKeyword = $("#dbFilterKeyword");
  const filterLocation = $("#dbFilterLocation");
  const filterQuality = $("#dbFilterQuality");
  const applyBtn = $("#dbApplyFilters");
  const exportBtn = $("#dbExportBtn");
  const prevBtn = $("#prevPage");
  const nextBtn = $("#nextPage");
  const pageIndicator = $("#pageIndicator");
  const paginationInfo = $("#paginationInfo");
  const selectAll = $("#selectAll");
  const bulkBar = $("#bulkBar");
  const bulkCount = $("#bulkCount");
  const bulkDeleteBtn = $("#bulkDeleteBtn");
  const bulkClearBtn = $("#bulkClearBtn");
  const scrapeBanner = $("#scrapeFilterBanner");
  const scrapeFilterId = $("#scrapeFilterId");
  const scrapeFilterMeta = $("#scrapeFilterMeta");
  const clearScrapeFilter = $("#clearScrapeFilter");

  /* ──── init ──── */
  function init() {
    // Check for scrape_id in URL or template variable
    const urlParams = new URLSearchParams(window.location.search);
    initialScrapeId = urlParams.get("scrape_id") || "";

    if (initialScrapeId) {
      showScrapeBanner(initialScrapeId);
    }

    loadFilters();
    loadStats();
    loadLeads();

    applyBtn.addEventListener("click", () => {
      currentPage = 1;
      loadLeads();
    });
    exportBtn.addEventListener("click", exportCSV);
    prevBtn.addEventListener("click", () => {
      if (currentPage > 1) {
        currentPage--;
        loadLeads();
      }
    });
    nextBtn.addEventListener("click", () => {
      if (currentPage < totalPages) {
        currentPage++;
        loadLeads();
      }
    });
    selectAll.addEventListener("change", toggleSelectAll);
    bulkDeleteBtn.addEventListener("click", bulkDelete);
    bulkClearBtn.addEventListener("click", clearSelection);
    if (clearScrapeFilter) {
      clearScrapeFilter.addEventListener("click", () => {
        initialScrapeId = "";
        scrapeBanner.classList.add("d-none");
        history.replaceState(null, "", "/database");
        currentPage = 1;
        loadLeads();
      });
    }

    // Search on Enter
    searchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        currentPage = 1;
        loadLeads();
      }
    });
  }

  /* ──── build query string ──── */
  function buildQuery(extra) {
    const params = new URLSearchParams();
    params.set("page", currentPage);
    params.set("per_page", 50);

    if (initialScrapeId) params.set("scrape_id", initialScrapeId);
    const tool = filterTool.value;
    const keyword = filterKeyword.value;
    const location = filterLocation.value;
    const quality = filterQuality.value;
    const search = searchInput.value.trim();

    if (tool) params.set("tool", tool);
    if (keyword) params.set("keyword", keyword);
    if (location) params.set("location", location);
    if (quality) params.set("quality", quality);
    if (search) params.set("search", search);
    if (extra) Object.entries(extra).forEach(([k, v]) => params.set(k, v));

    return params.toString();
  }

  /* ──── load filter options ──── */
  async function loadFilters() {
    try {
      const res = await fetch("/api/leads/filters");
      const data = await res.json();

      populateSelect(filterTool, data.tools || [], TOOL_LABELS);
      populateSelect(filterKeyword, data.keywords || []);
      populateSelect(filterLocation, data.locations || []);
    } catch (e) {
      console.error("Filter load error:", e);
    }
  }

  function populateSelect(el, values, labelMap) {
    const firstOption = el.querySelector("option");
    el.innerHTML = "";
    el.appendChild(firstOption);
    values.forEach((v) => {
      const opt = document.createElement("option");
      opt.value = v;
      opt.textContent = labelMap ? labelMap[v] || v : v;
      el.appendChild(opt);
    });
  }

  /* ──── load stats ──── */
  async function loadStats() {
    try {
      const res = await fetch("/api/leads/stats");
      const s = await res.json();
      setText("dbStatTotal", s.total || 0);
      setText("dbStatEmail", s.with_email || 0);
      setText("dbStatPhone", s.with_phone || 0);
      setText("dbStatStrong", (s.quality || {}).strong || 0);
    } catch (e) {
      console.error("Stats load error:", e);
    }
  }

  function setText(id, v) {
    const el = document.getElementById(id);
    if (el) el.textContent = v;
  }

  /* ──── scrape filter banner ──── */
  function showScrapeBanner(scrapeId) {
    if (scrapeBanner) {
      scrapeBanner.classList.remove("d-none");
      if (scrapeFilterId) scrapeFilterId.textContent = scrapeId;
    }
  }

  /* ──── load leads ──── */
  async function loadLeads() {
    body.innerHTML =
      '<tr><td colspan="10" class="text-center text-muted py-5"><div class="spinner-border spinner-border-sm me-2"></div>Loading...</td></tr>';
    selectedIds.clear();
    updateBulkBar();
    if (selectAll) selectAll.checked = false;

    try {
      const res = await fetch("/api/leads?" + buildQuery());
      const data = await res.json();

      totalPages = data.pages || 1;
      currentPage = data.page || 1;
      updatePagination(data.total, data.page, data.per_page, data.pages);
      renderLeads(data.leads || []);
    } catch (e) {
      console.error("Lead load error:", e);
      body.innerHTML =
        '<tr><td colspan="10" class="text-center text-danger py-5"><i class="bi bi-exclamation-triangle me-2"></i>Failed to load leads.</td></tr>';
    }
  }

  /* ──── render table ──── */
  function renderLeads(leads) {
    if (!leads.length) {
      body.innerHTML =
        '<tr><td colspan="10" class="text-center text-muted py-5"><i class="bi bi-inbox me-2 fs-5"></i><br><span class="mt-2 d-inline-block">No leads found. Start scraping to build your database!</span></td></tr>';
      return;
    }

    let html = "";
    leads.forEach((lead) => {
      const toolClass = lead.tool || "";
      const toolLabel = TOOL_LABELS[lead.tool] || lead.tool;
      const qualClass = lead.quality || "weak";
      const date = lead.created_at
        ? new Date(lead.created_at).toLocaleDateString("en", {
            month: "short",
            day: "numeric",
          })
        : "\u2014";

      html += `<tr data-id="${lead.id}">
        <td class="check-col">
          <input type="checkbox" class="form-check-input lead-check" value="${lead.id}">
        </td>
        <td class="cell-title">
          <a href="#" class="text-decoration-none lead-detail-link" data-lead='${escapeAttr(JSON.stringify(lead))}'>
            ${esc(lead.title || "\u2014")}
          </a>
        </td>
        <td class="cell-email">${esc(lead.email || "\u2014")}</td>
        <td>${esc(lead.phone || "\u2014")}</td>
        <td><span class="tool-badge ${toolClass}">${toolLabel}</span></td>
        <td class="cell-title">${esc(lead.keyword || "\u2014")}</td>
        <td class="cell-title">${esc(lead.location || "\u2014")}</td>
        <td><span class="quality-badge ${qualClass}">${qualClass}</span></td>
        <td class="text-muted small">${date}</td>
        <td class="text-center">
          <button class="btn btn-sm btn-link text-danger p-0 delete-lead-btn" data-id="${lead.id}" title="Delete">
            <i class="bi bi-trash3"></i>
          </button>
        </td>
      </tr>`;
    });
    body.innerHTML = html;

    // Attach events
    body.querySelectorAll(".lead-check").forEach((cb) => {
      cb.addEventListener("change", () => {
        if (cb.checked) selectedIds.add(parseInt(cb.value));
        else selectedIds.delete(parseInt(cb.value));
        updateBulkBar();
      });
    });

    body.querySelectorAll(".lead-detail-link").forEach((link) => {
      link.addEventListener("click", (e) => {
        e.preventDefault();
        try {
          const lead = JSON.parse(link.dataset.lead);
          showLeadDetail(lead);
        } catch (err) {
          console.error(err);
        }
      });
    });

    body.querySelectorAll(".delete-lead-btn").forEach((btn) => {
      btn.addEventListener("click", () => deleteLead(parseInt(btn.dataset.id)));
    });
  }

  /* ──── lead detail modal ──── */
  function showLeadDetail(lead) {
    const modalBody = $("#leadDetailBody");
    if (!modalBody) return;

    const allData = lead.data || {};
    // Merge top-level fields with JSON data
    const fields = {
      Title: lead.title,
      Email: lead.email,
      Phone: lead.phone,
      Website: lead.website,
      Tool: TOOL_LABELS[lead.tool] || lead.tool,
      Keyword: lead.keyword,
      Location: lead.location,
      Quality: lead.quality,
      Date: lead.created_at
        ? new Date(lead.created_at).toLocaleString()
        : "\u2014",
    };

    // Add all extra data fields
    Object.entries(allData).forEach(([k, v]) => {
      const key = k.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
      if (!Object.values(fields).includes(v) && v && v !== "N/A") {
        fields[key] = v;
      }
    });

    let html = "";
    Object.entries(fields).forEach(([label, value]) => {
      if (!value || value === "N/A") return;
      let displayVal = esc(String(value));
      // Make URLs clickable
      if (
        typeof value === "string" &&
        (value.startsWith("http") || value.includes("@"))
      ) {
        if (value.startsWith("http")) {
          displayVal = `<a href="${esc(value)}" target="_blank" class="text-decoration-none" style="color:var(--accent)">${esc(value)}</a>`;
        } else if (value.includes("@")) {
          displayVal = `<a href="mailto:${esc(value)}" class="text-decoration-none" style="color:var(--accent)">${esc(value)}</a>`;
        }
      }
      html += `<div class="detail-row">
        <div class="detail-label">${esc(label)}</div>
        <div class="detail-value">${displayVal}</div>
      </div>`;
    });

    modalBody.innerHTML =
      html || '<p class="text-muted">No details available.</p>';

    const modal = new bootstrap.Modal(
      document.getElementById("leadDetailModal"),
    );
    modal.show();
  }

  /* ──── pagination ──── */
  function updatePagination(total, page, perPage, pages) {
    const start = total === 0 ? 0 : (page - 1) * perPage + 1;
    const end = Math.min(page * perPage, total);
    paginationInfo.textContent = `Showing ${start}–${end} of ${total} leads`;
    pageIndicator.textContent = `${page} / ${pages}`;
    prevBtn.disabled = page <= 1;
    nextBtn.disabled = page >= pages;
  }

  /* ──── selection / bulk ──── */
  function toggleSelectAll() {
    const checked = selectAll.checked;
    body.querySelectorAll(".lead-check").forEach((cb) => {
      cb.checked = checked;
      const id = parseInt(cb.value);
      if (checked) selectedIds.add(id);
      else selectedIds.delete(id);
    });
    updateBulkBar();
  }

  function clearSelection() {
    selectedIds.clear();
    selectAll.checked = false;
    body.querySelectorAll(".lead-check").forEach((cb) => {
      cb.checked = false;
    });
    updateBulkBar();
  }

  function updateBulkBar() {
    const count = selectedIds.size;
    if (count > 0) {
      bulkBar.classList.remove("d-none");
      bulkCount.textContent = count;
    } else {
      bulkBar.classList.add("d-none");
    }
  }

  async function bulkDelete() {
    if (!selectedIds.size) return;
    if (!confirm(`Delete ${selectedIds.size} selected leads?`)) return;

    try {
      const res = await fetch("/api/leads/bulk-delete", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ ids: Array.from(selectedIds) }),
      });
      if (res.ok) {
        selectedIds.clear();
        updateBulkBar();
        loadLeads();
        loadStats();
      }
    } catch (e) {
      console.error("Bulk delete error:", e);
    }
  }

  async function deleteLead(id) {
    if (!confirm("Delete this lead?")) return;
    try {
      const res = await fetch(`/api/leads/${id}`, { method: "DELETE" });
      if (res.ok) {
        loadLeads();
        loadStats();
      }
    } catch (e) {
      console.error("Delete error:", e);
    }
  }

  /* ──── export ──── */
  function exportCSV() {
    const query = buildQuery();
    window.location.href = "/api/leads/export?" + query;
  }

  /* ──── helpers ──── */
  function esc(s) {
    const el = document.createElement("span");
    el.textContent = s;
    return el.innerHTML;
  }

  function escapeAttr(s) {
    return s
      .replace(/&/g, "&amp;")
      .replace(/'/g, "&#39;")
      .replace(/"/g, "&quot;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  /* ──── boot ──── */
  init();
})();
