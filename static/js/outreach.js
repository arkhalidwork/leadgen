/**
 * LeadGen — Outreach.js
 * Campaign list, builder, step editor, enrollment, SMTP config.
 */
const OutreachApp = (() => {
  let _currentCampaignId = null;
  let _currentStepId     = null;
  let _stages            = [];

  // ── Init ───────────────────────────────────────────────────────────────
  async function init() {
    await loadCampaigns();
    await loadSmtpConfig();
  }

  // ── Campaign List ──────────────────────────────────────────────────────
  async function loadCampaigns() {
    const res = await fetch('/api/campaigns');
    const d   = await res.json();
    const campaigns = d.campaigns || [];
    const listEl  = document.getElementById('campaignList');
    const emptyEl = document.getElementById('campaignEmpty');
    if (!campaigns.length) {
      listEl.style.display  = 'none';
      emptyEl.style.display = 'block';
      return;
    }
    listEl.style.display  = '';
    emptyEl.style.display = 'none';
    listEl.innerHTML = campaigns.map(c => `
      <div class="campaign-card" onclick="OutreachApp.openCampaign(${c.id})">
        <div class="d-flex align-items-center justify-content-between mb-2">
          <div class="campaign-card-name">${esc(c.name)}</div>
          <span class="badge badge-${c.status}" style="font-size:.68rem">${c.status}</span>
        </div>
        <div style="font-size:.78rem;color:#aaa">
          ${c.from_email ? `<i class="bi bi-envelope me-1"></i>${esc(c.from_email)}` : '<span class="text-warning"><i class="bi bi-exclamation-circle me-1"></i>No sender set</span>'}
        </div>
      </div>
    `).join('');
  }

  // ── Open Campaign Builder ──────────────────────────────────────────────
  window.OutreachApp = { init, openCampaign };

  async function openCampaign(id) {
    _currentCampaignId = id;
    const res = await fetch(`/api/campaigns/${id}`);
    const c   = await res.json();
    document.getElementById('campaignListView').style.display    = 'none';
    document.getElementById('campaignBuilder').style.display     = '';
    document.getElementById('builderCampaignName').textContent   = c.name;
    document.getElementById('builderStatusBadge').textContent    = c.status;
    document.getElementById('builderStatusBadge').className      = `badge ms-2 badge-${c.status}`;
    document.getElementById('btnActivate').style.display         = c.status === 'active' ? 'none' : '';
    renderStats(c.stats || {});
    renderSequenceTimeline(c.sequences || []);
    loadEnrolledLeads(id);
  }

  window.backToList = function() {
    _currentCampaignId = null;
    _currentStepId     = null;
    document.getElementById('campaignListView').style.display = '';
    document.getElementById('campaignBuilder').style.display  = 'none';
    document.getElementById('stepEditor').style.display       = 'none';
    loadCampaigns();
  };

  function renderStats(stats) {
    const el = document.getElementById('campaignStats');
    const s  = [
      { label: 'Enrolled', val: stats.total_enrolled || 0 },
      { label: 'Sent',     val: stats.total_sent     || 0 },
      { label: 'Opens',    val: stats.total_opens    || 0 },
      { label: 'Replies',  val: stats.total_replies  || 0 },
    ];
    el.innerHTML = s.map(x => `
      <div class="campaign-stat-box">
        <div class="val">${x.val}</div>
        <div class="lbl">${x.label}</div>
      </div>
    `).join('');
  }

  // ── Sequence Timeline ──────────────────────────────────────────────────
  function renderSequenceTimeline(seqs) {
    const el = document.getElementById('sequenceTimeline');
    let html = '';
    seqs.forEach((s, i) => {
      if (i > 0) html += `<div class="seq-connector">→</div>`;
      html += `
        <div class="seq-step">
          <div class="seq-node ${_currentStepId === s.id ? 'active-step' : ''}"
               onclick="OutreachApp.editStep(${JSON.stringify(s).replace(/"/g,'&quot;')})">
            <div class="seq-node-day">Day ${accumulateDelay(seqs, i)}</div>
            <div class="seq-node-title">${esc(s.subject) || 'Step ' + s.step_number}</div>
            <div style="font-size:.65rem;color:#888;margin-top:.25rem">${s.is_ai ? '✨ AI' : '📝 Template'} · ${cap(s.tone)}</div>
          </div>
        </div>
      `;
    });
    el.innerHTML = html;
  }

  function accumulateDelay(seqs, idx) {
    return seqs.slice(0, idx + 1).reduce((sum, s) => sum + (s.delay_days || 0), 0);
  }

  window.addStep = async function() {
    if (!_currentCampaignId) return;
    await fetch(`/api/campaigns/${_currentCampaignId}/sequences`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ delay_days: 3, subject: '', body_html: '', tone: 'professional', is_ai: 0 }),
    });
    const res = await fetch(`/api/campaigns/${_currentCampaignId}`);
    const c   = await res.json();
    renderSequenceTimeline(c.sequences || []);
  };

  // ── Step Editor ────────────────────────────────────────────────────────
  OutreachApp.editStep = function(step) {
    _currentStepId = step.id;
    document.getElementById('stepEditor').style.display        = '';
    document.getElementById('editorStepTitle').textContent     = `Step ${step.step_number}`;
    document.getElementById('editorDelay').value               = step.delay_days || 0;
    document.getElementById('editorTone').value                = step.tone || 'professional';
    document.getElementById('editorIsAI').checked              = !!step.is_ai;
    document.getElementById('editorSubject').value             = step.subject || '';
    document.getElementById('editorBody').value                = step.body_html || '';
  };

  window.closeStepEditor = function() {
    document.getElementById('stepEditor').style.display = 'none';
    _currentStepId = null;
  };

  window.saveStep = async function() {
    if (!_currentStepId) return;
    const data = {
      delay_days:      parseInt(document.getElementById('editorDelay').value) || 0,
      tone:            document.getElementById('editorTone').value,
      is_ai:           document.getElementById('editorIsAI').checked ? 1 : 0,
      subject:         document.getElementById('editorSubject').value,
      body_html:       document.getElementById('editorBody').value,
      body_text:       document.getElementById('editorBody').value.replace(/<[^>]+>/g, ' ').trim(),
    };
    await fetch(`/api/campaigns/${_currentCampaignId}/sequences/${_currentStepId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    toast('Step saved ✓');
    const res = await fetch(`/api/campaigns/${_currentCampaignId}`);
    const c   = await res.json();
    renderSequenceTimeline(c.sequences || []);
  };

  window.deleteStep = async function() {
    if (!_currentStepId || !confirm('Delete this step?')) return;
    await fetch(`/api/campaigns/${_currentCampaignId}/sequences/${_currentStepId}`, { method: 'DELETE' });
    closeStepEditor();
    const res = await fetch(`/api/campaigns/${_currentCampaignId}`);
    const c   = await res.json();
    renderSequenceTimeline(c.sequences || []);
  };

  window.insertToken = function(token) {
    const ta = document.getElementById('editorBody');
    const pos = ta.selectionStart;
    ta.value = ta.value.slice(0, pos) + token + ta.value.slice(pos);
    ta.focus();
    ta.selectionStart = ta.selectionEnd = pos + token.length;
  };

  // ── Create Campaign ────────────────────────────────────────────────────
  window.showCreateCampaign = function() {
    new bootstrap.Modal(document.getElementById('createCampaignModal')).show();
  };

  window.createCampaign = async function() {
    const name      = document.getElementById('newCampName').value.trim();
    const fromEmail = document.getElementById('newCampFromEmail').value.trim();
    const fromName  = document.getElementById('newCampFromName').value.trim();
    const limit     = parseInt(document.getElementById('newCampLimit').value) || 50;
    if (!name) { toast('Name required', 'warning'); return; }
    const res = await fetch('/api/campaigns', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, from_email: fromEmail, from_name: fromName, daily_send_limit: limit }),
    });
    const c = await res.json();
    bootstrap.Modal.getInstance(document.getElementById('createCampaignModal'))?.hide();
    openCampaign(c.id);
  };

  // ── Activate ──────────────────────────────────────────────────────────
  window.activateCampaign = async function() {
    await fetch(`/api/campaigns/${_currentCampaignId}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: 'active' }),
    });
    document.getElementById('builderStatusBadge').textContent = 'active';
    document.getElementById('builderStatusBadge').className   = 'badge ms-2 badge-active';
    document.getElementById('btnActivate').style.display      = 'none';
    toast('Campaign activated ✓');
  };

  // ── Enroll ────────────────────────────────────────────────────────────
  window.showEnrollModal = function() {
    new bootstrap.Modal(document.getElementById('enrollModal')).show();
  };

  window.enrollByTier = async function(tier) {
    // Get leads of this tier from intelligence
    const res = await fetch(`/api/intelligence/leads?tier=${tier}&limit=500`);
    const d   = await res.json();
    const ids = (d.leads || []).map(l => l.id);
    if (!ids.length) { toast(`No ${tier} leads found`, 'warning'); return; }
    const r2 = await fetch(`/api/campaigns/${_currentCampaignId}/leads`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lead_ids: ids }),
    });
    const result = await r2.json();
    toast(`Enrolled ${result.enrolled}, skipped ${result.skipped}`);
    bootstrap.Modal.getInstance(document.getElementById('enrollModal'))?.hide();
    loadEnrolledLeads(_currentCampaignId);
  };

  window.enrollCustom = async function() {
    const raw = document.getElementById('enrollLeadIds').value;
    const ids = raw.split(',').map(s => parseInt(s.trim())).filter(Boolean);
    if (!ids.length) { toast('Enter at least one ID', 'warning'); return; }
    const res = await fetch(`/api/campaigns/${_currentCampaignId}/leads`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ lead_ids: ids }),
    });
    const d = await res.json();
    toast(`Enrolled ${d.enrolled}, skipped ${d.skipped}`);
    bootstrap.Modal.getInstance(document.getElementById('enrollModal'))?.hide();
    loadEnrolledLeads(_currentCampaignId);
  };

  async function loadEnrolledLeads(cid) {
    const res = await fetch(`/api/campaigns/${cid}/leads`);
    const d   = await res.json();
    const el  = document.getElementById('enrolledLeadsList');
    const leads = d.leads || [];
    el.innerHTML = leads.length
      ? leads.map(l => `
          <div class="enrolled-row">
            <span class="fw-semibold">${esc(l.canonical_name || l.lead_name || '—')}</span>
            <span class="text-muted">${esc(l.email||'')}</span>
            <span class="badge bg-secondary ms-auto">${l.status}</span>
            <span style="font-size:.68rem;color:#888">Step ${l.current_step}/${l.emails_sent} sent</span>
          </div>
        `).join('')
      : '<div class="text-muted text-center p-2 small">No leads enrolled yet.</div>';
  }

  // ── SMTP ──────────────────────────────────────────────────────────────
  window.openSmtpModal = function() {
    new bootstrap.Modal(document.getElementById('smtpModal')).show();
  };

  async function loadSmtpConfig() {
    const res  = await fetch('/api/outreach/smtp');
    const conf = await res.json();
    if (conf.smtp_host)  document.getElementById('smtpHost').value = conf.smtp_host;
    if (conf.smtp_port)  document.getElementById('smtpPort').value = conf.smtp_port;
    if (conf.smtp_user)  document.getElementById('smtpUser').value = conf.smtp_user;
    if (conf.provider)   document.getElementById('smtpProvider').value = conf.provider;
  }

  window.saveSmtp = async function() {
    const data = {
      provider:  document.getElementById('smtpProvider').value,
      smtp_host: document.getElementById('smtpHost').value,
      smtp_port: parseInt(document.getElementById('smtpPort').value) || 587,
      smtp_user: document.getElementById('smtpUser').value,
      smtp_pass: document.getElementById('smtpPass').value,
    };
    await fetch('/api/outreach/smtp', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data),
    });
    toast('SMTP config saved ✓');
    bootstrap.Modal.getInstance(document.getElementById('smtpModal'))?.hide();
  };

  window.verifySmtp = async function() {
    const resultEl = document.getElementById('smtpVerifyResult');
    resultEl.innerHTML = '<span class="text-muted small">Testing…</span>';
    const res = await fetch('/api/outreach/smtp/verify', { method: 'POST' });
    const d   = await res.json();
    resultEl.innerHTML = d.ok
      ? `<span class="text-success small"><i class="bi bi-check-circle me-1"></i>${d.message}</span>`
      : `<span class="text-danger small"><i class="bi bi-x-circle me-1"></i>${d.message}</span>`;
  };

  // ── Helpers ───────────────────────────────────────────────────────────
  function toast(msg, type = 'success') {
    const el = document.createElement('div');
    el.className = `toast align-items-center text-white border-0 bg-${type === 'success' ? 'success' : type === 'warning' ? 'warning' : 'danger'}`;
    el.style.cssText = 'position:fixed;bottom:1.5rem;right:1.5rem;z-index:9999';
    el.innerHTML = `<div class="d-flex"><div class="toast-body">${msg}</div><button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button></div>`;
    document.body.appendChild(el);
    new bootstrap.Toast(el, { delay: 3000 }).show();
    setTimeout(() => el.remove(), 4000);
  }

  function esc(s) { return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') }
  function cap(s) { return s ? s[0].toUpperCase() + s.slice(1) : '' }

  return { init, openCampaign, editStep };
})();
