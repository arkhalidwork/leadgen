/**
 * LeadGen — Email Outreach Template Generator
 * Multi-step wizard: sender details → select leads → generate → view templates
 */

(function () {
  "use strict";

  /* ──── State ──── */
  let currentStep = 1;
  let senderInfo = {};
  let websiteScanData = null;
  let selectedLeads = [];
  let generatedTemplates = [];
  let activeSource = "recent"; // recent | database | upload
  let recentLeads = [];
  let dbLeads = [];
  let csvLeads = [];

  /* ──── Step Navigation ──── */
  window.goToStep = function (step) {
    for (let i = 1; i <= 4; i++) {
      const el = document.getElementById(`step${i}`);
      if (el) el.style.display = i === step ? "block" : "none";
    }
    // Update step indicators
    document.querySelectorAll(".step-item").forEach((s) => {
      const sNum = parseInt(s.dataset.step);
      s.classList.toggle("active", sNum === step);
      s.classList.toggle("completed", sNum < step);
    });
    currentStep = step;
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  window.switchLeadSource = function (source) {
    activeSource = source;
    document
      .querySelectorAll("#leadSourceTabs .nav-link")
      .forEach((btn) =>
        btn.classList.toggle("active", btn.dataset.source === source),
      );
    document.getElementById("sourceRecent").style.display =
      source === "recent" ? "block" : "none";
    document.getElementById("sourceDatabase").style.display =
      source === "database" ? "block" : "none";
    document.getElementById("sourceUpload").style.display =
      source === "upload" ? "block" : "none";
  };

  /* ──── Init ──── */
  document.addEventListener("DOMContentLoaded", () => {
    loadSavedSenderInfo();
    loadRecentScrapes();
    loadDbFilters();
    bindEvents();
  });

  function bindEvents() {
    // Step 1 → scan & next
    const scanBtn = document.getElementById("btnScanAndNext");
    if (scanBtn) scanBtn.addEventListener("click", handleScanAndNext);

    // Step 2 → generate
    const genBtn = document.getElementById("btnToGenerate");
    if (genBtn) genBtn.addEventListener("click", handleStartGenerate);

    // Recent scrape select
    const recentSelect = document.getElementById("recentScrapeSelect");
    if (recentSelect)
      recentSelect.addEventListener("change", handleRecentScrapeSelect);

    // Database load
    const dbLoadBtn = document.getElementById("btnLoadDbLeads");
    if (dbLoadBtn) dbLoadBtn.addEventListener("click", handleLoadDbLeads);

    // CSV upload
    const csvInput = document.getElementById("csvFileInput");
    if (csvInput) csvInput.addEventListener("change", handleCsvUpload);

    const clearCsvBtn = document.getElementById("btnClearCsv");
    if (clearCsvBtn)
      clearCsvBtn.addEventListener("click", () => {
        csvLeads = [];
        document.getElementById("csvPreview").style.display = "none";
        document.getElementById("csvFileInput").value = "";
      });

    // Upload zone drag & drop
    const zone = document.getElementById("uploadZone");
    if (zone) {
      zone.addEventListener("dragover", (e) => {
        e.preventDefault();
        zone.classList.add("drag-over");
      });
      zone.addEventListener("dragleave", () =>
        zone.classList.remove("drag-over"),
      );
      zone.addEventListener("drop", (e) => {
        e.preventDefault();
        zone.classList.remove("drag-over");
        const file = e.dataTransfer.files[0];
        if (file && file.name.endsWith(".csv")) {
          parseCsvFile(file);
        }
      });
      zone.addEventListener("click", (e) => {
        if (e.target.tagName !== "BUTTON" && e.target.tagName !== "INPUT") {
          document.getElementById("csvFileInput").click();
        }
      });
    }

    // Select-all checkboxes
    bindSelectAll("selectAllRecentLeads", () => recentLeads);
    bindSelectAll("selectAllDbLeads", () => dbLeads);
    bindSelectAll("selectAllCsvLeads", () => csvLeads);

    // Export templates
    const exportBtn = document.getElementById("btnExportTemplates");
    if (exportBtn) exportBtn.addEventListener("click", exportTemplates);

    // Copy template in modal
    const copyBtn = document.getElementById("btnCopyTemplate");
    if (copyBtn) copyBtn.addEventListener("click", copyTemplate);
  }

  function bindSelectAll(checkboxId, getLeadsFn) {
    const cb = document.getElementById(checkboxId);
    if (cb) {
      cb.addEventListener("change", () => {
        const leads = getLeadsFn();
        leads.forEach((l) => (l._selected = cb.checked));
        refreshLeadTable(checkboxId);
      });
    }
  }

  function refreshLeadTable(selectAllId) {
    if (selectAllId === "selectAllRecentLeads")
      renderLeadsTable(recentLeads, "recentLeadsBody");
    else if (selectAllId === "selectAllDbLeads")
      renderLeadsTable(dbLeads, "dbLeadsBody");
    else if (selectAllId === "selectAllCsvLeads")
      renderLeadsTable(csvLeads, "csvLeadsBody");
  }

  /* ──── Load saved sender info from localStorage ──── */
  function loadSavedSenderInfo() {
    try {
      const saved = localStorage.getItem("leadgen_sender_info");
      if (saved) {
        const data = JSON.parse(saved);
        if (data.name) document.getElementById("senderName").value = data.name;
        if (data.company)
          document.getElementById("senderCompany").value = data.company;
        if (data.website)
          document.getElementById("senderWebsite").value = data.website;
        if (data.description)
          document.getElementById("senderDescription").value = data.description;
        if (data.outreach_type)
          document.getElementById("outreachType").value = data.outreach_type;
      }
    } catch (e) {}
  }

  function saveSenderInfo() {
    const info = {
      name: document.getElementById("senderName").value.trim(),
      company: document.getElementById("senderCompany").value.trim(),
      website: document.getElementById("senderWebsite").value.trim(),
      description: document.getElementById("senderDescription").value.trim(),
      outreach_type: document.getElementById("outreachType").value,
    };
    localStorage.setItem("leadgen_sender_info", JSON.stringify(info));
    return info;
  }

  /* ──── Step 1: Scan website & continue ──── */
  async function handleScanAndNext() {
    senderInfo = saveSenderInfo();

    if (!senderInfo.name || !senderInfo.company) {
      showError("Please enter your name and company name.");
      return;
    }

    // Show spinner
    document.getElementById("scanBtnText").style.display = "none";
    document.getElementById("scanBtnSpinner").style.display = "inline";

    try {
      if (senderInfo.website) {
        const resp = await fetch("/api/email-outreach/scan-website", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: senderInfo.website }),
        });
        const data = await resp.json();
        if (resp.ok && data.services) {
          websiteScanData = data;
          document.getElementById("websiteScanResult").style.display = "block";
          const details = [];
          if (data.company_name)
            details.push(`<strong>Company:</strong> ${esc(data.company_name)}`);
          if (data.services && data.services.length)
            details.push(
              `<strong>Services:</strong> ${data.services.map(esc).join(", ")}`,
            );
          if (data.description)
            details.push(`<strong>Summary:</strong> ${esc(data.description)}`);
          document.getElementById("websiteScanDetails").innerHTML =
            details.join("<br>");
        }
      }

      // Even without website, proceed to step 2
      goToStep(2);
    } catch (e) {
      showError("Failed to scan website. Continuing without scan data.");
      goToStep(2);
    } finally {
      document.getElementById("scanBtnText").style.display = "inline";
      document.getElementById("scanBtnSpinner").style.display = "none";
    }
  }

  /* ──── Load recent scrape sessions ──── */
  async function loadRecentScrapes() {
    try {
      const resp = await fetch("/api/dashboard/history?per_page=50");
      const data = await resp.json();
      const select = document.getElementById("recentScrapeSelect");
      select.innerHTML =
        '<option value="">— Select a scrape session —</option>';

      if (data.history && data.history.length) {
        data.history
          .filter((h) => h.status === "completed" && h.lead_count > 0)
          .forEach((h) => {
            const opt = document.createElement("option");
            opt.value = h.id;
            const date = new Date(h.started_at).toLocaleDateString();
            opt.textContent = `${h.keyword} in ${h.location} — ${h.lead_count} leads (${h.tool}, ${date})`;
            select.appendChild(opt);
          });
      }
    } catch (e) {
      console.error("Failed to load recent scrapes:", e);
    }
  }

  async function handleRecentScrapeSelect() {
    const scrapeId = document.getElementById("recentScrapeSelect").value;
    if (!scrapeId) {
      document.getElementById("recentScrapePreview").style.display = "none";
      recentLeads = [];
      return;
    }

    try {
      const resp = await fetch(`/api/leads?scrape_id=${scrapeId}&per_page=200`);
      const data = await resp.json();
      recentLeads = (data.leads || []).map((l) => ({ ...l, _selected: true }));

      document.getElementById("recentScrapePreview").style.display = "block";
      document.getElementById("recentLeadCount").textContent =
        `${recentLeads.length} leads`;
      document.getElementById("recentTool").textContent =
        recentLeads[0]?.tool || "-";
      document.getElementById("recentKeyword").textContent =
        recentLeads[0]?.keyword || "-";

      renderLeadsTable(recentLeads, "recentLeadsBody");
    } catch (e) {
      console.error("Failed to load leads:", e);
    }
  }

  /* ──── Database filter & load ──── */
  async function loadDbFilters() {
    try {
      const resp = await fetch("/api/leads/filters");
      const data = await resp.json();

      populateSelect("dbFilterKeyword", data.keywords || [], "All Keywords");
      populateSelect("dbFilterLocation", data.locations || [], "All Locations");
      populateSelect("dbFilterTool", data.tools || [], "All Tools");
    } catch (e) {}
  }

  function populateSelect(id, options, defaultLabel) {
    const sel = document.getElementById(id);
    sel.innerHTML = `<option value="">${defaultLabel}</option>`;
    options.forEach((o) => {
      const opt = document.createElement("option");
      opt.value = o;
      opt.textContent = o;
      sel.appendChild(opt);
    });
  }

  async function handleLoadDbLeads() {
    const keyword = document.getElementById("dbFilterKeyword").value;
    const location = document.getElementById("dbFilterLocation").value;
    const tool = document.getElementById("dbFilterTool").value;

    const params = new URLSearchParams({ per_page: "200" });
    if (keyword) params.set("keyword", keyword);
    if (location) params.set("location", location);
    if (tool) params.set("tool", tool);

    try {
      const resp = await fetch(`/api/leads?${params}`);
      const data = await resp.json();
      dbLeads = (data.leads || []).map((l) => ({ ...l, _selected: true }));

      document.getElementById("dbLeadsPreview").style.display = "block";
      document.getElementById("dbLeadCount").textContent =
        `${dbLeads.length} leads`;
      renderLeadsTable(dbLeads, "dbLeadsBody");
    } catch (e) {
      showError("Failed to load leads from database.");
    }
  }

  /* ──── CSV Upload ──── */
  function handleCsvUpload(e) {
    const file = e.target.files[0];
    if (file) parseCsvFile(file);
  }

  function parseCsvFile(file) {
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const text = e.target.result;
        const lines = text.split("\n").filter((l) => l.trim());
        if (lines.length < 2) {
          showError("CSV file appears to be empty.");
          return;
        }

        const headers = lines[0].split(",").map((h) => h.trim().toLowerCase());
        csvLeads = [];

        for (let i = 1; i < lines.length; i++) {
          const vals = parseCSVLine(lines[i]);
          if (vals.length < headers.length) continue;

          const row = {};
          headers.forEach((h, idx) => {
            row[h] = vals[idx]?.trim() || "";
          });

          csvLeads.push({
            title:
              row.business_name ||
              row.business ||
              row.name ||
              row.company ||
              row.title ||
              "",
            email: row.email || "",
            phone: row.phone || "",
            website: row.website || row.url || "",
            location:
              row.location || row.city || row.region || row.address || "",
            keyword:
              row.keyword || row.niche || row.category || row.industry || "",
            _selected: true,
            data: row,
          });
        }

        document.getElementById("csvPreview").style.display = "block";
        document.getElementById("csvLeadCount").textContent =
          `${csvLeads.length} leads`;
        renderLeadsTable(csvLeads, "csvLeadsBody");
      } catch (err) {
        showError("Failed to parse CSV file: " + err.message);
      }
    };
    reader.readAsText(file);
  }

  function parseCSVLine(line) {
    const result = [];
    let current = "";
    let inQuotes = false;
    for (let i = 0; i < line.length; i++) {
      const c = line[i];
      if (c === '"') {
        inQuotes = !inQuotes;
      } else if (c === "," && !inQuotes) {
        result.push(current);
        current = "";
      } else {
        current += c;
      }
    }
    result.push(current);
    return result;
  }

  /* ──── Render leads table ──── */
  function renderLeadsTable(leads, tbodyId) {
    const tbody = document.getElementById(tbodyId);
    if (!tbody) return;
    tbody.innerHTML = "";

    leads.forEach((lead, idx) => {
      const tr = document.createElement("tr");
      tr.innerHTML = `
        <td><input type="checkbox" class="lead-cb" data-idx="${idx}" ${lead._selected ? "checked" : ""} /></td>
        <td>${esc(lead.title || "-")}</td>
        <td>${esc(lead.email || "-")}</td>
        <td>${esc(lead.location || "-")}</td>
      `;
      tr.querySelector(".lead-cb").addEventListener("change", (e) => {
        lead._selected = e.target.checked;
      });
      tbody.appendChild(tr);
    });
  }

  /* ──── Step 2 → Step 3: Gather selected leads & generate ──── */
  function getSelectedLeads() {
    if (activeSource === "recent")
      return recentLeads.filter((l) => l._selected);
    if (activeSource === "database") return dbLeads.filter((l) => l._selected);
    if (activeSource === "upload") return csvLeads.filter((l) => l._selected);
    return [];
  }

  async function handleStartGenerate() {
    selectedLeads = getSelectedLeads();
    if (!selectedLeads.length) {
      showError("Please select at least one lead to generate templates for.");
      return;
    }

    // Filter leads with email only - warn if some lack emails
    const withEmail = selectedLeads.filter((l) => l.email && l.email !== "N/A");
    const noEmail = selectedLeads.length - withEmail.length;

    goToStep(3);
    await generateTemplates(selectedLeads);
  }

  async function generateTemplates(leads) {
    generatedTemplates = [];
    const progressBar = document.getElementById("genProgressBar");
    const progressMsg = document.getElementById("genProgressMsg");
    const batchSize = 10;
    const batches = [];

    for (let i = 0; i < leads.length; i += batchSize) {
      batches.push(leads.slice(i, i + batchSize));
    }

    let processed = 0;

    for (const batch of batches) {
      try {
        progressMsg.textContent = `Processing ${processed + 1} – ${Math.min(processed + batch.length, leads.length)} of ${leads.length} leads...`;

        const resp = await fetch("/api/email-outreach/generate", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            sender: {
              ...senderInfo,
              website_scan: websiteScanData,
            },
            leads: batch.map((l) => ({
              title: l.title || "",
              email: l.email || "",
              phone: l.phone || "",
              website: l.website || "",
              location: l.location || "",
              keyword: l.keyword || "",
              data: l.data || {},
              lead_id: l.id || null,
            })),
          }),
        });

        const data = await resp.json();
        if (data.templates) {
          generatedTemplates.push(...data.templates);
        }
      } catch (e) {
        console.error("Batch generation failed:", e);
      }

      processed += batch.length;
      const pct = Math.round((processed / leads.length) * 100);
      progressBar.style.width = `${pct}%`;
    }

    progressMsg.textContent = `Generated ${generatedTemplates.length} templates!`;
    setTimeout(() => {
      goToStep(4);
      renderTemplates();
    }, 800);
  }

  /* ──── Step 4: Render generated templates ──── */
  function renderTemplates() {
    const container = document.getElementById("templatesList");
    const countBadge = document.getElementById("templateCount");
    countBadge.textContent = `${generatedTemplates.length} templates`;
    container.innerHTML = "";

    generatedTemplates.forEach((tpl, idx) => {
      const card = document.createElement("div");
      card.className = "template-card";
      card.innerHTML = `
        <div class="template-card-header">
          <div class="d-flex align-items-center gap-2">
            <i class="bi bi-envelope-fill" style="color: var(--accent)"></i>
            <strong>${esc(tpl.business_name || "Business")}</strong>
          </div>
          ${tpl.email ? `<span class="badge bg-info">${esc(tpl.email)}</span>` : ""}
        </div>
        <div class="template-card-subject">
          <small class="text-muted">Subject:</small> ${esc(tpl.subject)}
        </div>
        <div class="template-card-preview">${esc(tpl.body).substring(0, 150)}...</div>
        <div class="template-card-footer">
          <span class="text-muted small">${esc(tpl.location || "")} ${tpl.keyword ? "• " + esc(tpl.keyword) : ""}</span>
          <button class="btn btn-sm btn-outline-primary" onclick="viewTemplate(${idx})">
            <i class="bi bi-eye me-1"></i>View
          </button>
        </div>
      `;
      container.appendChild(card);
    });
  }

  window.viewTemplate = function (idx) {
    const tpl = generatedTemplates[idx];
    if (!tpl) return;

    document.getElementById("modalTemplateBusiness").textContent =
      tpl.business_name || "Email Template";
    document.getElementById("modalSubject").value = tpl.subject || "";
    document.getElementById("modalBody").value = tpl.body || "";
    document.getElementById("modalLeadEmail").textContent =
      tpl.email || "No email";
    document.getElementById("modalLeadLocation").textContent =
      tpl.location || "-";
    document.getElementById("modalLeadKeyword").textContent =
      tpl.keyword || "-";

    const modal = new bootstrap.Modal(document.getElementById("templateModal"));
    modal.show();
  };

  function copyTemplate() {
    const subject = document.getElementById("modalSubject").value;
    const body = document.getElementById("modalBody").value;
    const text = `Subject: ${subject}\n\n${body}`;
    navigator.clipboard.writeText(text).then(() => {
      const btn = document.getElementById("btnCopyTemplate");
      btn.innerHTML = '<i class="bi bi-check-lg me-1"></i>Copied!';
      setTimeout(() => {
        btn.innerHTML = '<i class="bi bi-clipboard me-1"></i>Copy to Clipboard';
      }, 2000);
    });
  }

  function exportTemplates() {
    if (!generatedTemplates.length) return;

    const headers = [
      "business_name",
      "email",
      "subject",
      "body",
      "location",
      "keyword",
    ];
    let csv = headers.join(",") + "\n";

    generatedTemplates.forEach((tpl) => {
      const row = headers.map((h) => {
        const val = (tpl[h] || "").toString().replace(/"/g, '""');
        return `"${val}"`;
      });
      csv += row.join(",") + "\n";
    });

    const blob = new Blob([csv], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `email_templates_${new Date().toISOString().split("T")[0]}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  /* ──── Helpers ──── */
  function esc(str) {
    if (!str) return "";
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function showError(msg) {
    const el = document.getElementById("outreachError");
    const msgEl = document.getElementById("outreachErrorMsg");
    if (el && msgEl) {
      msgEl.textContent = msg;
      el.style.display = "block";
      setTimeout(() => (el.style.display = "none"), 6000);
    }
  }
})();
