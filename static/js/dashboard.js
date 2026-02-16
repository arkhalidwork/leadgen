/**
 * Dashboard — LeadGen
 * Fetches stats + history from API and renders charts.
 */

(function () {
  "use strict";

  /* ──────── helpers ──────── */
  const $ = (s) => document.querySelector(s);
  const setText = (id, v) => {
    const el = document.getElementById(id);
    if (el) el.textContent = v;
  };

  const TOOL_LABELS = {
    gmaps: "Google Maps",
    linkedin: "LinkedIn",
    instagram: "Instagram",
    webcrawler: "Web Crawler",
  };

  const TOOL_ICONS = {
    gmaps: "bi-geo-alt-fill",
    linkedin: "bi-linkedin",
    instagram: "bi-instagram",
    webcrawler: "bi-globe2",
  };

  const STATUS_BADGE = {
    running: "bg-primary-subtle text-primary-emphasis",
    completed: "bg-success-subtle text-success-emphasis",
    failed: "bg-danger-subtle text-danger-emphasis",
    stopped: "bg-warning-subtle text-warning-emphasis",
  };

  /* ──────── chart instances ──────── */
  let qualityChartInstance = null;
  let trendChartInstance = null;

  /* ──────── fetch & render ──────── */
  async function loadDashboard() {
    try {
      const [statsRes, historyRes] = await Promise.all([
        fetch("/api/dashboard/stats"),
        fetch("/api/dashboard/history?per_page=15"),
      ]);
      const stats = await statsRes.json();
      const history = await historyRes.json();

      renderStats(stats);
      renderQualityChart(stats);
      renderTrendChart(stats.trend || []);
      renderToolStats(stats.by_tool || {});
      renderHistory(history);
    } catch (err) {
      console.error("Dashboard load error:", err);
    }
  }

  function renderStats(s) {
    setText("statTotalLeads", s.total_leads ?? 0);
    setText("statStrong", s.strong ?? 0);
    setText("statMedium", s.medium ?? 0);
    setText("statWeak", s.weak ?? 0);
  }

  /* ──────── quality doughnut ──────── */
  function renderQualityChart(s) {
    const strong = s.strong || 0;
    const medium = s.medium || 0;
    const weak = s.weak || 0;
    const total = strong + medium + weak || 1;

    setText("qualityStrongPct", Math.round((strong / total) * 100) + "%");
    setText("qualityMediumPct", Math.round((medium / total) * 100) + "%");
    setText("qualityWeakPct", Math.round((weak / total) * 100) + "%");

    const ctx = document.getElementById("qualityChart");
    if (!ctx) return;

    if (qualityChartInstance) qualityChartInstance.destroy();

    qualityChartInstance = new Chart(ctx, {
      type: "doughnut",
      data: {
        labels: ["Strong", "Medium", "Weak"],
        datasets: [
          {
            data: [strong, medium, weak],
            backgroundColor: ["#28a745", "#ffc107", "#dc3545"],
            borderWidth: 0,
            hoverOffset: 6,
          },
        ],
      },
      options: {
        cutout: "70%",
        responsive: false,
        plugins: {
          legend: { display: false },
          tooltip: {
            callbacks: {
              label: (ctx) => `${ctx.label}: ${ctx.parsed} leads`,
            },
          },
        },
      },
    });
  }

  /* ──────── 7-day trend ──────── */
  function renderTrendChart(trend) {
    const ctx = document.getElementById("trendChart");
    if (!ctx) return;

    // Fill in missing days
    const days = [];
    const counts = [];
    const trendMap = {};
    trend.forEach((t) => (trendMap[t.day] = t.leads));

    for (let i = 6; i >= 0; i--) {
      const d = new Date();
      d.setDate(d.getDate() - i);
      const key = d.toISOString().slice(0, 10);
      days.push(d.toLocaleDateString("en", { month: "short", day: "numeric" }));
      counts.push(trendMap[key] || 0);
    }

    if (trendChartInstance) trendChartInstance.destroy();

    trendChartInstance = new Chart(ctx, {
      type: "line",
      data: {
        labels: days,
        datasets: [
          {
            label: "Leads",
            data: counts,
            fill: true,
            backgroundColor: "rgba(79, 140, 255, 0.1)",
            borderColor: "#4f8cff",
            borderWidth: 2,
            pointRadius: 4,
            pointBackgroundColor: "#4f8cff",
            tension: 0.35,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            grid: { color: "rgba(255,255,255,0.04)" },
            ticks: { color: "#8e92b0" },
          },
          y: {
            beginAtZero: true,
            grid: { color: "rgba(255,255,255,0.04)" },
            ticks: { color: "#8e92b0", precision: 0 },
          },
        },
        plugins: {
          legend: { display: false },
        },
      },
    });
  }

  /* ──────── tool stats table ──────── */
  function renderToolStats(byTool) {
    const body = document.getElementById("toolStatsBody");
    if (!body) return;

    const tools = ["gmaps", "linkedin", "instagram", "webcrawler"];
    let html = "";
    tools.forEach((t) => {
      const d = byTool[t] || { scrapes: 0, leads: 0 };
      html += `<tr>
        <td><i class="bi ${TOOL_ICONS[t]} me-2"></i>${TOOL_LABELS[t]}</td>
        <td class="text-end">${d.scrapes}</td>
        <td class="text-end fw-semibold">${d.leads}</td>
      </tr>`;
    });

    if (!html)
      html =
        '<tr><td colspan="3" class="text-center text-muted">No data yet</td></tr>';
    body.innerHTML = html;
  }

  /* ──────── history table ──────── */
  function renderHistory(data) {
    const body = document.getElementById("historyBody");
    const countEl = document.getElementById("historyCount");
    if (!body) return;

    const rows = data.history || [];
    if (countEl) countEl.textContent = `${data.total || 0} total`;

    if (!rows.length) {
      body.innerHTML =
        '<tr><td colspan="7" class="text-center text-muted py-4"><i class="bi bi-inbox me-2"></i>No scrapes yet. Launch a tool to get started!</td></tr>';
      return;
    }

    let html = "";
    rows.forEach((r) => {
      const toolLabel = TOOL_LABELS[r.tool] || r.tool;
      const icon = TOOL_ICONS[r.tool] || "bi-cpu";
      const badgeClass =
        STATUS_BADGE[r.status] || "bg-secondary-subtle text-secondary-emphasis";
      const total = (r.strong || 0) + (r.medium || 0) + (r.weak || 0) || 1;
      const sPct = Math.round(((r.strong || 0) / total) * 100);
      const mPct = Math.round(((r.medium || 0) / total) * 100);
      const wPct = 100 - sPct - mPct;
      const date = r.started_at
        ? new Date(r.started_at).toLocaleDateString("en", {
            month: "short",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
          })
        : "—";

      html += `<tr>
        <td><i class="bi ${icon} me-1"></i>${toolLabel}</td>
        <td class="cell-truncate-sm">${r.keyword || "—"}</td>
        <td class="cell-truncate-sm">${r.location || "—"}</td>
        <td class="text-end fw-semibold">${r.lead_count || 0}</td>
        <td style="min-width:120px;">
          <div class="d-flex quality-bar overflow-hidden">
            <div style="width:${sPct}%; background:#28a745;" title="Strong: ${r.strong || 0}"></div>
            <div style="width:${mPct}%; background:#ffc107;" title="Medium: ${r.medium || 0}"></div>
            <div style="width:${wPct}%; background:#dc3545;" title="Weak: ${r.weak || 0}"></div>
          </div>
        </td>
        <td><span class="badge ${badgeClass}">${r.status}</span></td>
        <td class="text-muted small">${date}</td>
      </tr>`;
    });
    body.innerHTML = html;
  }

  /* ──────── init ──────── */
  loadDashboard();
})();
