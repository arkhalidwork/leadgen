/**
 * Dashboard — LeadGen
 * Fetches stats + history from API and renders charts.
 * Memory-safe: properly destroys Chart.js instances & resets canvases.
 */

(function () {
  "use strict";

  /* ──────── guards ──────── */
  let _loaded = false;

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

  /** Safely destroy a chart and reset its canvas */
  function destroyChart(instance, canvasId) {
    if (instance) {
      instance.destroy();
    }
    const canvas = document.getElementById(canvasId);
    if (canvas) {
      const ctx = canvas.getContext("2d");
      if (ctx) ctx.clearRect(0, 0, canvas.width, canvas.height);
    }
    return null;
  }

  /* ──────── fetch & render ──────── */
  async function loadDashboard() {
    if (_loaded) return;
    _loaded = true;

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
      _loaded = false;
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

    qualityChartInstance = destroyChart(qualityChartInstance, "qualityChart");

    qualityChartInstance = new Chart(ctx, {
      type: "doughnut",
      data: {
        labels: ["Strong", "Medium", "Weak"],
        datasets: [
          {
            data: [strong, medium, weak],
            backgroundColor: ["#22c55e", "#eab308", "#ef4444"],
            borderWidth: 0,
            hoverOffset: 6,
          },
        ],
      },
      options: {
        cutout: "72%",
        responsive: false,
        animation: { duration: 800, easing: "easeOutQuart" },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "rgba(23, 26, 45, 0.95)",
            borderColor: "rgba(79, 140, 255, 0.2)",
            borderWidth: 1,
            titleColor: "#e4e6f0",
            bodyColor: "#8e92b0",
            cornerRadius: 8,
            padding: 10,
            callbacks: {
              label: (c) => ` ${c.label}: ${c.parsed} leads`,
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

    trendChartInstance = destroyChart(trendChartInstance, "trendChart");

    trendChartInstance = new Chart(ctx, {
      type: "line",
      data: {
        labels: days,
        datasets: [
          {
            label: "Leads",
            data: counts,
            fill: true,
            backgroundColor: "rgba(79, 140, 255, 0.08)",
            borderColor: "#4f8cff",
            borderWidth: 2.5,
            pointRadius: 5,
            pointBackgroundColor: "#4f8cff",
            pointBorderColor: "#171a2d",
            pointBorderWidth: 2,
            pointHoverRadius: 7,
            tension: 0.4,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        scales: {
          x: {
            grid: { color: "rgba(255,255,255,0.03)", drawBorder: false },
            ticks: { color: "#8e92b0", font: { size: 11 } },
          },
          y: {
            beginAtZero: true,
            grid: { color: "rgba(255,255,255,0.03)", drawBorder: false },
            ticks: { color: "#8e92b0", precision: 0, font: { size: 11 } },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            backgroundColor: "rgba(23, 26, 45, 0.95)",
            borderColor: "rgba(79, 140, 255, 0.2)",
            borderWidth: 1,
            titleColor: "#e4e6f0",
            bodyColor: "#8e92b0",
            cornerRadius: 8,
            padding: 10,
          },
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
        '<tr><td colspan="3" class="text-center text-muted py-3">No data yet</td></tr>';
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

    const limited = rows.slice(0, 15);
    let html = "";
    limited.forEach((r) => {
      const toolLabel = TOOL_LABELS[r.tool] || r.tool;
      const icon = TOOL_ICONS[r.tool] || "bi-cpu";
      const badgeClass =
        STATUS_BADGE[r.status] || "bg-secondary-subtle text-secondary-emphasis";
      const total = (r.strong || 0) + (r.medium || 0) + (r.weak || 0) || 1;
      const sPct = Math.round(((r.strong || 0) / total) * 100);
      const mPct = Math.round(((r.medium || 0) / total) * 100);
      const wPct = Math.max(0, 100 - sPct - mPct);
      const date = r.started_at
        ? new Date(r.started_at).toLocaleDateString("en", {
            month: "short",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
          })
        : "\u2014";

      html += `<tr class="cursor-pointer" onclick="window.location.href='/database?scrape_id=${r.id}'" title="View leads from this scrape">
        <td><i class="bi ${icon} me-1 opacity-75"></i> ${toolLabel}</td>
        <td class="cell-truncate-sm">${r.keyword || "\u2014"}</td>
        <td class="cell-truncate-sm">${r.location || "\u2014"}</td>
        <td class="text-end fw-semibold">${r.lead_count || 0}</td>
        <td style="min-width:110px;">
          <div class="d-flex quality-bar overflow-hidden">
            <div style="width:${sPct}%; background:#22c55e;" title="Strong: ${r.strong || 0}"></div>
            <div style="width:${mPct}%; background:#eab308;" title="Medium: ${r.medium || 0}"></div>
            <div style="width:${wPct}%; background:#ef4444;" title="Weak: ${r.weak || 0}"></div>
          </div>
        </td>
        <td><span class="badge ${badgeClass}">${r.status}</span></td>
        <td class="text-muted small">${date}</td>
      </tr>`;
    });
    body.innerHTML = html;
  }

  /* ──────── cleanup on page unload ──────── */
  window.addEventListener("beforeunload", () => {
    qualityChartInstance = destroyChart(qualityChartInstance, "qualityChart");
    trendChartInstance = destroyChart(trendChartInstance, "trendChart");
  });

  /* ──────── init ──────── */
  loadDashboard();
})();
