/**
 * LeadGen — Lead Intelligence UI
 * Score rings, signal icons, insight panel, filters, pagination.
 */

const SIGNAL_META = {
  active_social:    { icon: "bi-camera-video-fill", color: "#E4405F",  label: "Active Social" },
  hiring:           { icon: "bi-person-plus-fill",  color: "#0A66C2",  label: "Hiring" },
  recent_reviews:   { icon: "bi-star-fill",         color: "#FBBF24",  label: "Recent Reviews" },
  growth_indicators:{ icon: "bi-graph-up-arrow",    color: "#10B981",  label: "Growing" },
  poor_website:     { icon: "bi-exclamation-triangle-fill", color: "#F59E0B", label: "Poor Website" },
  no_website:       { icon: "bi-x-circle-fill",     color: "#EF4444",  label: "No Website" },
  email_available:  { icon: "bi-envelope-check-fill",color: "#818CF8", label: "Email Found" },
  high_engagement:  { icon: "bi-fire",              color: "#F97316",  label: "High Engagement" },
};

const TIER_COLORS = {
  hot:  "#ef4444",
  warm: "#f59e0b",
  cold: "#6366f1",
  dead: "#4b5563",
};

// State
let allLeads      = [];
let currentPage   = 0;
let currentTier   = "";
let currentSort   = "score";
let currentQ      = "";
let activeLead    = null;
let searchDebounce = null;
const PAGE_SIZE   = 30;

// DOM
const listEl      = document.getElementById("intelLeadList");
const panelEl     = document.getElementById("intelInsightPanel");
const insightEmpty= document.getElementById("insightEmpty");
const insightContent = document.getElementById("insightContent");

// ── Initialise ─────────────────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", async () => {
  await loadStats();
  await loadLeads(true);
  wireFilters();
});

// ── Stats Bar ──────────────────────────────────────────────────────────────
async function loadStats() {
  try {
    const r = await fetch("/api/intelligence/stats");
    if (!r.ok) return;
    const d = await r.json();
    setText("statTotal",   d.total_leads  ?? "—");
    setText("statAvgScore", d.avg_score ?? "—");
    const dist = d.tier_distribution || {};
    setText("statHot",  dist.hot  ?? 0);
    setText("statWarm", dist.warm ?? 0);
    setText("statCold", dist.cold ?? 0);
  } catch (e) { /* offline */ }
}

// ── Load Leads ─────────────────────────────────────────────────────────────
async function loadLeads(reset = false) {
  if (reset) {
    currentPage = 0;
    allLeads = [];
    listEl.innerHTML = "";
    showSkeletons(6);
  }

  const offset = currentPage * PAGE_SIZE;
  const params = new URLSearchParams({
    limit:  PAGE_SIZE,
    offset: offset,
    sort:   currentSort,
  });
  if (currentTier) params.set("tier", currentTier);
  if (currentQ)    params.set("q",    currentQ);

  try {
    const r = await fetch("/api/intelligence/leads?" + params);
    if (!r.ok) throw new Error(r.status);
    const d = await r.json();

    if (reset) listEl.innerHTML = "";

    if (!d.leads || d.leads.length === 0 && reset) {
      listEl.innerHTML = `
        <div class="intel-loading">
          <i class="bi bi-inbox" style="font-size:2.5rem;opacity:0.3"></i>
          <div class="mt-2">No leads scored yet.<br>
          <span style="font-size:0.78rem;opacity:0.6">Run a scraping job — results will appear here automatically.</span></div>
        </div>`;
      hideLoadMore();
      return;
    }

    d.leads.forEach(lead => {
      allLeads.push(lead);
      listEl.appendChild(buildLeadItem(lead));
    });

    // Load more button
    const total = d.total || 0;
    if ((currentPage + 1) * PAGE_SIZE < total) {
      showLoadMore();
    } else {
      hideLoadMore();
    }
    currentPage++;

  } catch (err) {
    listEl.innerHTML = `<div class="intel-loading text-danger">Failed to load leads. <button class="btn btn-sm btn-link" onclick="loadLeads(true)">Retry</button></div>`;
  }
}

// ── Build Lead Row ─────────────────────────────────────────────────────────
function buildLeadItem(lead) {
  const el = document.createElement("div");
  el.className = "intel-lead-item";
  el.dataset.leadId = lead.lead_id;

  const score     = Math.round(lead.score || 0);
  const tier      = lead.tier || "cold";
  const tierColor = TIER_COLORS[tier] || "#6b7280";
  const circ      = 2 * Math.PI * 15.9;  // circumference for r=15.9
  const dashVal   = ((score / 100) * circ).toFixed(1);

  const signalHtml = (lead.signals || []).slice(0, 5).map(s => {
    const m = SIGNAL_META[s] || { icon: "bi-circle", color: "#9ca3af", label: s };
    return `<span class="signal-pill" style="border-color:${m.color}20;color:${m.color}" title="${m.label}">
      <i class="bi ${m.icon}" style="font-size:0.6rem"></i>${m.label}
    </span>`;
  }).join("");

  const contactIcons = [
    lead.has_email   ? `<i class="bi bi-envelope-fill" style="color:#818CF8" title="Email"></i>` : "",
    lead.has_phone   ? `<i class="bi bi-telephone-fill" style="color:#34D399" title="Phone"></i>` : "",
    lead.has_website ? `<i class="bi bi-globe2" style="color:#38BDF8" title="Website"></i>` : "",
    lead.has_instagram ? `<i class="bi bi-instagram" style="color:#E4405F" title="Instagram"></i>` : "",
  ].filter(Boolean).join(" ");

  el.innerHTML = `
    <div class="score-ring-wrap">
      <svg class="score-ring-svg" viewBox="0 0 36 36">
        <circle class="score-ring-bg"   cx="18" cy="18" r="15.9"/>
        <circle class="score-ring-fill" cx="18" cy="18" r="15.9"
          stroke="${tierColor}"
          stroke-dasharray="${dashVal} ${circ.toFixed(1)}"
          stroke-dashoffset="0"/>
      </svg>
      <div class="score-ring-val">${score}</div>
    </div>
    <div style="flex:1;min-width:0">
      <div style="display:flex;align-items:center;gap:4px;flex-wrap:wrap">
        <span style="font-weight:700;font-size:0.88rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:180px" title="${escHtml(lead.canonical_name)}">${escHtml(lead.canonical_name)}</span>
        <span class="tier-badge tier-${tier}">${tier.toUpperCase()}</span>
      </div>
      <div style="font-size:0.73rem;color:var(--text-secondary,#9ca3af);margin-top:2px">
        ${escHtml(lead.category || "")}${lead.city ? " · " + escHtml(lead.city) : ""}
        <span style="margin-left:8px;opacity:0.7">${contactIcons}</span>
      </div>
      <div class="signal-pills">${signalHtml}</div>
    </div>`;

  el.addEventListener("click", () => selectLead(lead.lead_id, el));
  return el;
}

// ── Select Lead → Insight Panel ─────────────────────────────────────────────
async function selectLead(leadId, el) {
  // Deactivate previous
  document.querySelectorAll(".intel-lead-item.active").forEach(e => e.classList.remove("active"));
  el.classList.add("active");
  activeLead = leadId;

  insightEmpty.style.display = "none";
  insightContent.style.display = "";
  insightContent.innerHTML = `<div class="intel-loading"><span class="spinner-border spinner-border-sm me-2"></span>Loading insights...</div>`;

  try {
    const r = await fetch(`/api/intelligence/leads/${leadId}`);
    if (!r.ok) throw new Error("Not found");
    const d = await r.json();
    renderInsightPanel(d);
  } catch (e) {
    insightContent.innerHTML = `<div class="text-danger p-3">Failed to load insights.</div>`;
  }
}

// ── Render Insight Panel ───────────────────────────────────────────────────
function renderInsightPanel(d) {
  const score     = Math.round(d.score || 0);
  const tier      = d.tier || "cold";
  const tierColor = TIER_COLORS[tier] || "#6b7280";
  const sub       = d.sub_scores || {};
  const ins       = d.insights   || {};
  const enr       = d.enrichment || {};

  const subScoreBars = [
    { label: "Completeness", val: sub.completeness || 0, max: 30, color: "#818CF8" },
    { label: "Social",       val: sub.social       || 0, max: 20, color: "#E4405F" },
    { label: "Activity",     val: sub.activity     || 0, max: 20, color: "#10B981" },
    { label: "Sentiment",    val: sub.sentiment    || 0, max: 15, color: "#FBBF24" },
    { label: "Freshness",    val: sub.freshness    || 0, max: 15, color: "#38BDF8" },
  ].map(s => `
    <div class="subscore-row">
      <span class="subscore-label">${s.label}</span>
      <div class="subscore-track">
        <div class="subscore-fill" style="width:${(s.val/s.max*100).toFixed(1)}%;background:${s.color}"></div>
      </div>
      <span class="subscore-val">${s.val.toFixed(0)}/${s.max}</span>
    </div>`).join("");

  // Signals
  const sigHtml = (d.signals || []).map(s => {
    const m = SIGNAL_META[s.type] || { icon: "bi-circle", color: "#9ca3af", label: s.type };
    return `<div class="insight-item">
      <i class="bi ${m.icon} mt-1" style="color:${m.color}"></i>
      <span>${m.label} <span style="font-size:0.7rem;opacity:0.6">(${Math.round(s.confidence*100)}% conf)</span>
      ${s.value ? `<br><span style="font-size:0.73rem;opacity:0.65">${escHtml(s.value)}</span>` : ""}
      </span>
    </div>`;
  }).join("") || `<div class="insight-item" style="opacity:0.5"><i class="bi bi-dash-circle"></i> No signals detected</div>`;

  // Strengths / Weaknesses
  const strengths  = parseArr(ins.strengths);
  const weaknesses = parseArr(ins.weaknesses);
  const angles     = parseArr(ins.outreach_angles);

  const strHtml = strengths.map(s => `
    <div class="insight-item"><i class="bi bi-check-circle-fill" style="color:#10B981"></i><span>${escHtml(s)}</span></div>`).join("") ||
    `<div class="insight-item" style="opacity:0.5"><i class="bi bi-dash-circle"></i> None identified</div>`;

  const wkiHtml = weaknesses.map(w => `
    <div class="insight-item"><i class="bi bi-exclamation-circle-fill" style="color:#F59E0B"></i><span>${escHtml(w)}</span></div>`).join("") ||
    `<div class="insight-item" style="opacity:0.5"><i class="bi bi-dash-circle"></i> None identified</div>`;

  const angHtml = angles.map(a => `
    <div class="insight-item"><i class="bi bi-lightning-charge-fill" style="color:#818CF8"></i><span>${escHtml(a)}</span></div>`).join("") ||
    `<div class="insight-item" style="opacity:0.5">No specific angle identified.</div>`;

  // Contact chips
  const contacts = [
    enr.email   ? `<a href="mailto:${escHtml(enr.email)}" class="contact-chip"><i class="bi bi-envelope"></i>${escHtml(enr.email)}</a>` : "",
    enr.phone   ? `<a href="tel:${escHtml(enr.phone)}"  class="contact-chip"><i class="bi bi-telephone"></i>${escHtml(enr.phone)}</a>` : "",
    enr.website ? `<a href="${escHtml(enr.website)}" target="_blank" rel="noopener" class="contact-chip"><i class="bi bi-globe2"></i>${escHtml(enr.domain || enr.website)}</a>` : "",
    enr.instagram_url ? `<a href="${escHtml(enr.instagram_url)}" target="_blank" rel="noopener" class="contact-chip" style="color:#E4405F"><i class="bi bi-instagram"></i>Instagram</a>` : "",
    enr.linkedin_url  ? `<a href="${escHtml(enr.linkedin_url)}"  target="_blank" rel="noopener" class="contact-chip" style="color:#0A66C2"><i class="bi bi-linkedin"></i>LinkedIn</a>`  : "",
  ].filter(Boolean).join("");

  // Rating
  const ratingHtml = enr.google_rating
    ? `<span style="color:#FBBF24">★${enr.google_rating.toFixed(1)}</span> <span style="opacity:0.6;font-size:0.75rem">(${enr.google_reviews || 0} reviews)</span>`
    : "";

  insightContent.innerHTML = `
    <!-- Header -->
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
      <div class="score-ring-wrap">
        <svg class="score-ring-svg" viewBox="0 0 36 36">
          <circle class="score-ring-bg" cx="18" cy="18" r="15.9"/>
          <circle class="score-ring-fill" cx="18" cy="18" r="15.9"
            stroke="${tierColor}"
            stroke-dasharray="${(score/100*100.5).toFixed(1)} 100.5"/>
        </svg>
        <div class="score-ring-val">${score}</div>
      </div>
      <div>
        <div style="font-weight:800;font-size:1rem;line-height:1.2">${escHtml(d.canonical_name)}</div>
        <div style="font-size:0.75rem;opacity:0.6">${escHtml(d.category || "")}${d.city ? " · " + escHtml(d.city) : ""}
          <span class="tier-badge tier-${tier}" style="margin-left:6px">${tier.toUpperCase()}</span>
        </div>
        ${ratingHtml ? `<div style="font-size:0.8rem;margin-top:2px">${ratingHtml}</div>` : ""}
      </div>
      <button class="btn btn-sm btn-outline-secondary ms-auto" style="font-size:0.7rem;padding:3px 10px" onclick="rescoreLead(${d.lead_id})">
        <i class="bi bi-arrow-clockwise me-1"></i>Rescore
      </button>
    </div>

    <!-- Summary -->
    ${ins.summary ? `
    <div class="insight-section">
      <div class="insight-section-title"><i class="bi bi-card-text me-1"></i>Summary</div>
      <p style="font-size:0.8rem;color:var(--text-secondary,#9ca3af);margin:0;line-height:1.55">${escHtml(ins.summary)}</p>
    </div>` : ""}

    <!-- Sub-score breakdown -->
    <div class="insight-section">
      <div class="insight-section-title"><i class="bi bi-bar-chart-fill me-1"></i>Score Breakdown</div>
      ${subScoreBars}
    </div>

    <!-- Contact -->
    ${contacts ? `
    <div class="insight-section">
      <div class="insight-section-title"><i class="bi bi-person-lines-fill me-1"></i>Contact</div>
      <div>${contacts}</div>
    </div>` : ""}

    <!-- Signals -->
    <div class="insight-section">
      <div class="insight-section-title"><i class="bi bi-broadcast me-1"></i>Detected Signals</div>
      ${sigHtml}
    </div>

    <!-- Strengths -->
    <div class="insight-section">
      <div class="insight-section-title"><i class="bi bi-check-circle me-1" style="color:#10B981"></i>Strengths</div>
      ${strHtml}
    </div>

    <!-- Weaknesses -->
    <div class="insight-section">
      <div class="insight-section-title"><i class="bi bi-exclamation-circle me-1" style="color:#F59E0B"></i>Weaknesses</div>
      ${wkiHtml}
    </div>

    <!-- Outreach angles -->
    <div class="insight-section">
      <div class="insight-section-title"><i class="bi bi-lightning-charge me-1" style="color:#818CF8"></i>Outreach Angles</div>
      ${angHtml}
    </div>

    <!-- Next action -->
    ${ins.next_action ? `
    <div class="next-action-card">
      <div class="action-label"><i class="bi bi-arrow-right-circle me-1"></i>Recommended Next Action</div>
      ${escHtml(ins.next_action)}
    </div>` : ""}

    <!-- Source history -->
    <div class="insight-section" style="margin-top:16px">
      <div class="insight-section-title"><i class="bi bi-database me-1"></i>Data Sources (${d.source_count || 1})</div>
      ${(d.sources || []).map(s => `
        <div class="insight-item" style="font-size:0.74rem;opacity:0.7">
          <i class="bi ${sourceIcon(s.source)}"></i>
          <span>${escHtml(s.source)} · ${escHtml(s.raw_name || "")} · ${formatDate(s.scraped_at)}</span>
        </div>`).join("") || ""}
    </div>`;
}

// ── Rescore ────────────────────────────────────────────────────────────────
async function rescoreLead(leadId) {
  try {
    const r = await fetch(`/api/intelligence/leads/${leadId}/rescore`, { method: "POST" });
    const d = await r.json();
    if (d.ok) {
      showToast(`Rescored: ${d.new_score}/100 (${d.tier.toUpperCase()})`);
      // Refresh the panel
      const el = document.querySelector(`.intel-lead-item[data-lead-id="${leadId}"]`);
      if (el) await selectLead(leadId, el);
      await loadStats();
    }
  } catch (e) { showToast("Rescore failed", "danger"); }
}

// ── Wire Filters ───────────────────────────────────────────────────────────
function wireFilters() {
  document.querySelectorAll(".intel-filter-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".intel-filter-btn").forEach(b => b.classList.remove("active"));
      btn.classList.add("active");
      currentTier = btn.dataset.tier || "";
      loadLeads(true);
    });
  });

  document.getElementById("sortSelect").addEventListener("change", e => {
    currentSort = e.target.value;
    loadLeads(true);
  });

  document.getElementById("intelSearch").addEventListener("input", e => {
    clearTimeout(searchDebounce);
    searchDebounce = setTimeout(() => {
      currentQ = e.target.value.trim();
      loadLeads(true);
    }, 350);
  });

  document.getElementById("btnLoadMore")?.addEventListener("click", () => loadLeads(false));

  document.getElementById("btnRescoreAll")?.addEventListener("click", async () => {
    if (!confirm("Re-run scoring on all leads? This may take a moment.")) return;
    showToast("Rescoring in background...", "info");
    // TODO: Batch rescore endpoint (Phase 4b)
  });
}

// ── Helpers ────────────────────────────────────────────────────────────────
function showSkeletons(n) {
  listEl.innerHTML = Array(n).fill(`
    <div class="intel-skeleton">
      <div class="skel-circle"></div>
      <div class="skel-lines">
        <div class="skel-line"></div>
        <div class="skel-line short"></div>
        <div class="skel-line xshort"></div>
      </div>
    </div>`).join("");
}

function showLoadMore()  { document.getElementById("loadMoreWrap").style.display = ""; }
function hideLoadMore()  { document.getElementById("loadMoreWrap").style.display = "none"; }

function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}

function escHtml(str) {
  if (!str) return "";
  const d = document.createElement("div");
  d.textContent = String(str);
  return d.innerHTML;
}

function parseArr(val) {
  if (Array.isArray(val)) return val;
  if (!val) return [];
  try { return JSON.parse(val); } catch { return []; }
}

function formatDate(iso) {
  if (!iso) return "";
  try { return new Date(iso).toLocaleDateString(); } catch { return iso; }
}

function sourceIcon(src) {
  return { gmaps: "bi-geo-alt-fill", linkedin: "bi-linkedin",
           instagram: "bi-instagram", webcrawler: "bi-globe2" }[src] || "bi-database";
}

function showToast(msg, type = "success") {
  const t = document.createElement("div");
  t.className = `alert alert-${type === "success" ? "success" : type === "info" ? "info" : "danger"} position-fixed`;
  t.style.cssText = "bottom:20px;right:20px;z-index:9999;min-width:220px;font-size:0.82rem;padding:10px 16px;border-radius:10px;animation:fadeIn .2s ease";
  t.innerHTML = `<i class="bi bi-check-circle me-2"></i>${escHtml(msg)}`;
  document.body.appendChild(t);
  setTimeout(() => t.remove(), 3000);
}
