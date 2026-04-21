/**
 * LeadGen — Pipeline.js
 * Kanban board with Sortable.js drag-drop, activity drawer, bulk-add.
 */
const PipelineApp = (() => {
  let _boardData   = null;
  let _activeItemId = null;
  let _activeStageColor = '#6366f1';
  const TIER_COLORS = { hot:'#f97316', warm:'#facc15', cold:'#60a5fa', dead:'#6b7280' };

  // ── Init ─────────────────────────────────────────────────────────────────
  async function init() {
    await loadBoard();
    await populateStageSelects();
  }

  // ── Load board ────────────────────────────────────────────────────────────
  async function loadBoard() {
    const board = document.getElementById('kanbanBoard');
    try {
      const res  = await fetch('/api/pipeline/board');
      _boardData = await res.json();
    } catch {
      board.innerHTML = '<div class="text-danger p-4">Failed to load pipeline. Check network.</div>';
      return;
    }

    // Summary pills
    renderSummary(_boardData.stages);

    // Kanban columns
    board.innerHTML = '';
    _boardData.stages.forEach(stage => board.appendChild(buildColumn(stage)));

    // Activate Sortable on each column body
    document.querySelectorAll('.kanban-col-body').forEach(colEl => {
      Sortable.create(colEl, {
        group: 'pipeline',
        animation: 150,
        ghostClass: 'drag-ghost',
        onEnd: onDragEnd,
      });
    });
  }

  function renderSummary(stages) {
    const el = document.getElementById('pipelineSummary');
    el.innerHTML = stages.map(s => `
      <span class="summary-pill">
        <span style="width:8px;height:8px;border-radius:50%;background:${s.color};display:inline-block"></span>
        ${s.name} <strong>${s.items.length}</strong>
      </span>
    `).join('');
  }

  function buildColumn(stage) {
    const col = document.createElement('div');
    col.className = 'kanban-column';
    col.dataset.stageId = stage.id;
    col.innerHTML = `
      <div class="kanban-col-header">
        <div class="kanban-col-title">
          <span style="width:10px;height:10px;border-radius:50%;background:${stage.color};display:inline-block"></span>
          ${stage.name}
        </div>
        <span class="kanban-count">${stage.items.length}</span>
      </div>
      <div class="kanban-col-body" data-stage-id="${stage.id}">
        ${stage.items.length
          ? stage.items.map(buildCardHTML).join('')
          : '<div class="kanban-empty-col">Drop leads here</div>'}
      </div>
    `;
    return col;
  }

  function buildCardHTML(item) {
    const tier  = item.current_tier || item.lead_tier || 'cold';
    const score = item.current_score ?? item.lead_score ?? 0;
    return `
      <div class="kanban-card" data-item-id="${item.id}" data-lead-id="${item.lead_id || ''}">
        <div class="kanban-card-name" title="${esc(item.lead_name)}">${esc(item.lead_name) || 'Unknown'}</div>
        <div class="kanban-card-meta">
          <span class="tier-dot tier-${tier}"></span>
          <span>${cap(tier)}</span>
          <span>${Math.round(score)}/100</span>
          ${item.lead_email ? `<span class="text-truncate" style="max-width:100px" title="${esc(item.lead_email)}"><i class="bi bi-envelope"></i></span>` : ''}
        </div>
        ${item.deal_value > 0 ? `<div style="font-size:.7rem;color:#10b981;margin-top:.3rem">$${item.deal_value.toLocaleString()}</div>` : ''}
        <div class="kanban-card-actions">
          <button class="btn btn-xs btn-outline-secondary" onclick="openActivity(${item.id}, '${esc(item.lead_name)}'); event.stopPropagation()">
            <i class="bi bi-chat-left-text"></i>
          </button>
          <button class="btn btn-xs btn-outline-danger" onclick="archiveItem(${item.id}); event.stopPropagation()">
            <i class="bi bi-archive"></i>
          </button>
        </div>
      </div>
    `;
  }

  // ── Drag end → PATCH stage ─────────────────────────────────────────────
  async function onDragEnd(evt) {
    const itemEl  = evt.item;
    const newColEl = evt.to;
    const itemId  = parseInt(itemEl.dataset.itemId);
    const stageId = parseInt(newColEl.dataset.stageId);
    if (!itemId || !stageId) return;
    try {
      await fetch(`/api/pipeline/items/${itemId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stage_id: stageId }),
      });
      // Update count badges
      document.querySelectorAll('.kanban-col-body').forEach(col => {
        const cnt = col.querySelectorAll('.kanban-card').length;
        const badge = col.closest('.kanban-column').querySelector('.kanban-count');
        if (badge) badge.textContent = cnt;
      });
    } catch { toast('Move failed', 'danger') }
  }

  // ── Activity Drawer ────────────────────────────────────────────────────
  window.openActivity = async function(itemId, name) {
    _activeItemId = itemId;
    document.getElementById('drawerTitle').textContent = name || 'Activity';
    document.getElementById('activityDrawer').classList.add('open');
    const res = await fetch(`/api/pipeline/items/${itemId}/activity`);
    const d   = await res.json();
    const list = document.getElementById('activityList');
    list.innerHTML = (d.activity || []).map(a => `
      <div class="activity-item">
        <div class="act-type">${cap(a.activity_type.replace('_',' '))}</div>
        <div>${esc(a.title)}</div>
        ${a.body ? `<div style="color:#ccc;margin-top:.2rem">${esc(a.body)}</div>` : ''}
        <div style="color:#666;font-size:.68rem;margin-top:.2rem">${fmtTime(a.created_at)}</div>
      </div>
    `).join('') || '<div class="text-muted text-center p-3">No activity yet.</div>';
  };

  window.closeDrawer = () => document.getElementById('activityDrawer').classList.remove('open');

  window.submitNote = async function() {
    const body = document.getElementById('noteInput').value.trim();
    if (!body || !_activeItemId) return;
    await fetch(`/api/pipeline/items/${_activeItemId}/activity`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ body }),
    });
    document.getElementById('noteInput').value = '';
    openActivity(_activeItemId);
    toast('Note saved');
  };

  // ── Archive ────────────────────────────────────────────────────────────
  window.archiveItem = async function(itemId) {
    if (!confirm('Archive this lead from pipeline?')) return;
    await fetch(`/api/pipeline/items/${itemId}`, { method: 'DELETE' });
    const card = document.querySelector(`[data-item-id="${itemId}"]`);
    if (card) card.remove();
    toast('Lead archived');
  };

  // ── Add from Intelligence ──────────────────────────────────────────────
  window.showAddFromIntelModal = function() {
    new bootstrap.Modal(document.getElementById('addFromIntelModal')).show();
  };

  window.addFromIntelligence = async function() {
    const tier    = document.getElementById('intelTierFilter').value;
    const stageId = document.getElementById('intelStageSelect').value;
    if (!stageId) { toast('Select a stage', 'warning'); return; }
    const msg = document.getElementById('intelAddMsg');
    msg.textContent = 'Adding leads…';
    const res  = await fetch('/api/pipeline/bulk-add', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tier, stage_id: parseInt(stageId) }),
    });
    const d = await res.json();
    msg.textContent = `✓ Added ${d.added}, skipped ${d.skipped} (already in pipeline)`;
    if (d.added > 0) setTimeout(() => { bootstrap.Modal.getInstance(document.getElementById('addFromIntelModal'))?.hide(); loadBoard(); }, 1200);
  };

  async function populateStageSelects() {
    const res = await fetch('/api/pipeline/stages');
    const d   = await res.json();
    const stages = d.stages || [];
    const selects = ['intelStageSelect'];
    selects.forEach(id => {
      const el = document.getElementById(id);
      if (!el) return;
      el.innerHTML = stages.map(s => `<option value="${s.id}">${s.name}</option>`).join('');
    });
  }

  // ── Add Stage ────────────────────────────────────────────────────────
  window.showAddStageModal = function() {
    _activeStageColor = '#6366f1';
    new bootstrap.Modal(document.getElementById('addStageModal')).show();
  };

  window.selectColor = function(el) {
    document.querySelectorAll('.color-swatch').forEach(s => s.classList.remove('selected'));
    el.classList.add('selected');
    _activeStageColor = el.dataset.color;
  };

  window.createStage = async function() {
    const name = document.getElementById('newStageName').value.trim();
    if (!name) return;
    await fetch('/api/pipeline/stages', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name, color: _activeStageColor }),
    });
    bootstrap.Modal.getInstance(document.getElementById('addStageModal'))?.hide();
    loadBoard();
    toast(`Stage "${name}" created`);
  };

  // ── Helpers ────────────────────────────────────────────────────────────
  function toast(msg, type = 'success') {
    const el = document.getElementById('pipelineToast');
    el.className = `toast align-items-center text-white border-0 position-fixed bottom-0 end-0 m-3 bg-${type === 'success' ? 'success' : type === 'danger' ? 'danger' : 'warning'}`;
    document.getElementById('toastBody').textContent = msg;
    new bootstrap.Toast(el, { delay: 3000 }).show();
  }

  function esc(s) { return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;') }
  function cap(s) { return s ? s[0].toUpperCase() + s.slice(1) : '' }
  function fmtTime(iso) {
    if (!iso) return '';
    const d = new Date(iso); return isNaN(d) ? iso : d.toLocaleString();
  }

  return { init };
})();
