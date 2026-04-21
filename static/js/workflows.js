/**
 * LeadGen — Workflows.js
 * Workflow list, builder modal, template presets, manual run + history.
 */
const WorkflowApp = (() => {
  let _editingId      = null;
  let _actionCount    = 0;
  let _stages         = [];
  let _campaigns      = [];

  const TEMPLATES = {
    hot_to_pipeline: {
      name: 'Hot Lead → Pipeline',
      trigger: { trigger_type: 'event', config: { event_type: 'lead_scored', conditions: { tier: 'hot' } } },
      actions: [{ action_type: 'add_to_pipeline', config: {} }],
    },
    daily_scrape: {
      name: 'Daily Scrape → Enrich',
      trigger: { trigger_type: 'schedule', config: { cron: '0 9 * * *' } },
      actions: [{ action_type: 'scrape_job', config: { type: 'gmaps' } }],
    },
    reply_to_stage: {
      name: 'Reply → Move Stage',
      trigger: { trigger_type: 'event', config: { event_type: 'reply_received', conditions: {} } },
      actions: [{ action_type: 'move_pipeline', config: {} }],
    },
    hot_to_campaign: {
      name: 'Hot Lead → Campaign',
      trigger: { trigger_type: 'event', config: { event_type: 'lead_scored', conditions: { tier: 'hot' } } },
      actions: [
        { action_type: 'add_to_pipeline', config: {} },
        { action_type: 'enroll_campaign', config: {} },
      ],
    },
  };

  // ── Init ──────────────────────────────────────────────────────────────
  async function init() {
    await Promise.all([loadWorkflows(), loadStages(), loadCampaigns()]);
  }

  // ── Load list ─────────────────────────────────────────────────────────
  async function loadWorkflows() {
    const el = document.getElementById('workflowList');
    const res = await fetch('/api/workflows');
    const d   = await res.json();
    const wfs = d.workflows || [];
    if (!wfs.length) {
      el.innerHTML = `
        <div class="text-center py-5 text-muted">
          <i class="bi bi-diagram-3" style="font-size:2.5rem;color:#555"></i>
          <p class="mt-2">No workflows yet. Use a template or create one.</p>
        </div>`;
      return;
    }
    el.innerHTML = wfs.map(w => `
      <div class="workflow-card">
        <div class="wf-icon"><i class="bi bi-diagram-3-fill"></i></div>
        <div class="wf-info">
          <div class="wf-name">${esc(w.name)}</div>
          <div class="wf-meta">
            <span><i class="bi bi-lightning-fill me-1"></i>${w.trigger_count || 0} trigger${w.trigger_count !== 1 ? 's' : ''}</span>
            <span><i class="bi bi-play-fill me-1"></i>${w.action_count || 0} actions</span>
            <span><i class="bi bi-repeat me-1"></i>${w.run_count || 0} runs</span>
            ${w.last_run_at ? `<span>Last run: ${fmtTime(w.last_run_at)}</span>` : ''}
          </div>
        </div>
        <div class="wf-actions-row">
          <span class="badge badge-${w.status}" style="font-size:.68rem">${w.status}</span>
          <button class="btn btn-xs btn-outline-secondary" onclick="WorkflowApp.editWorkflow(${w.id})" title="Edit">
            <i class="bi bi-pencil"></i>
          </button>
          <button class="btn btn-xs btn-outline-success" onclick="WorkflowApp.manualRun(${w.id})" title="Run now">
            <i class="bi bi-play-fill"></i>
          </button>
          <button class="btn btn-xs btn-outline-info" onclick="WorkflowApp.showRuns(${w.id}, '${esc(w.name)}')" title="History">
            <i class="bi bi-clock-history"></i>
          </button>
          <button class="btn btn-xs btn-outline-${w.status === 'active' ? 'warning' : 'success'}"
                  onclick="WorkflowApp.toggleStatus(${w.id}, '${w.status}')" title="Pause/Resume">
            <i class="bi bi-${w.status === 'active' ? 'pause-fill' : 'play-fill'}"></i>
          </button>
          <button class="btn btn-xs btn-outline-danger" onclick="WorkflowApp.deleteWorkflow(${w.id})" title="Delete">
            <i class="bi bi-trash"></i>
          </button>
        </div>
      </div>
    `).join('');
  }

  // ── Templates ─────────────────────────────────────────────────────────
  WorkflowApp = { init, showCreateModal, applyTemplate, editWorkflow, manualRun, showRuns, toggleStatus, deleteWorkflow };

  WorkflowApp.applyTemplate = function(key) {
    const tpl = TEMPLATES[key];
    if (!tpl) return;
    resetBuilder();
    document.getElementById('wfName').value = tpl.name;

    // Set trigger
    applyTriggerConfig(tpl.trigger);

    // Add actions
    tpl.actions.forEach(a => addActionNode(a));

    document.getElementById('wfModalTitle').textContent = tpl.name;
    new bootstrap.Modal(document.getElementById('wfBuilderModal')).show();
  };

  // ── Show create modal ─────────────────────────────────────────────────
  function showCreateModal() {
    resetBuilder();
    document.getElementById('wfModalTitle').textContent = 'New Workflow';
    document.getElementById('wfName').value = '';
    new bootstrap.Modal(document.getElementById('wfBuilderModal')).show();
  }

  // ── Edit ──────────────────────────────────────────────────────────────
  WorkflowApp.editWorkflow = async function(id) {
    _editingId = id;
    const res  = await fetch(`/api/workflows/${id}`);
    const w    = await res.json();
    resetBuilder();
    document.getElementById('wfModalTitle').textContent = `Edit: ${w.name}`;
    document.getElementById('wfName').value = w.name;

    const trig = (w.triggers || [])[0];
    if (trig) applyTriggerConfig(trig);

    (w.actions || []).forEach(a => addActionNode({
      action_type: a.action_type,
      config: JSON.parse(a.config || '{}'),
    }));

    new bootstrap.Modal(document.getElementById('wfBuilderModal')).show();
  };

  // ── Save ──────────────────────────────────────────────────────────────
  WorkflowApp.saveWorkflow = async function() {
    const name = document.getElementById('wfName').value.trim();
    if (!name) { toast('Workflow name required', 'warning'); return; }

    const trigger = buildTriggerPayload();
    const actions = buildActionsPayload();

    if (_editingId) {
      // Update name/status only (new triggers/actions = future enhancement)
      await fetch(`/api/workflows/${_editingId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });
      toast('Workflow updated');
    } else {
      await fetch('/api/workflows', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, trigger, actions }),
      });
      toast('Workflow created ✓');
    }
    bootstrap.Modal.getInstance(document.getElementById('wfBuilderModal'))?.hide();
    resetBuilder();
    loadWorkflows();
  };

  // ── Manual Run ────────────────────────────────────────────────────────
  WorkflowApp.manualRun = async function(id) {
    await fetch(`/api/workflows/${id}/run`, { method: 'POST' });
    toast('Workflow triggered ▶');
    setTimeout(loadWorkflows, 2000);
  };

  // ── Run History ───────────────────────────────────────────────────────
  WorkflowApp.showRuns = async function(id, name) {
    document.getElementById('wfRunsTitle').textContent = `${name} — Runs`;
    const res  = await fetch(`/api/workflows/${id}/runs`);
    const d    = await res.json();
    const runs = d.runs || [];
    document.getElementById('wfRunsTable').innerHTML = runs.map(r => `
      <tr>
        <td><span class="badge bg-${r.status === 'completed' ? 'success' : r.status === 'failed' ? 'danger' : 'secondary'}">${r.status}</span></td>
        <td>${fmtTime(r.started_at)}</td>
        <td>${r.actions_done}/${r.actions_total}</td>
        <td style="color:#ef4444;max-width:200px;overflow:hidden;text-overflow:ellipsis">${esc(r.error || '')}</td>
      </tr>
    `).join('') || '<tr><td colspan="4" class="text-center text-muted">No runs yet.</td></tr>';
    new bootstrap.Modal(document.getElementById('wfRunsModal')).show();
  };

  // ── Toggle Status ─────────────────────────────────────────────────────
  WorkflowApp.toggleStatus = async function(id, currentStatus) {
    const newStatus = currentStatus === 'active' ? 'paused' : 'active';
    await fetch(`/api/workflows/${id}`, {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ status: newStatus }),
    });
    loadWorkflows();
  };

  // ── Delete ────────────────────────────────────────────────────────────
  WorkflowApp.deleteWorkflow = async function(id) {
    if (!confirm('Delete this workflow?')) return;
    await fetch(`/api/workflows/${id}`, { method: 'DELETE' });
    loadWorkflows();
  };

  // ── Add Action (from modal button) ───────────────────────────────────
  WorkflowApp.addAction = addAction;

  function addAction() {
    addActionNode({ action_type: 'add_to_pipeline', config: {} });
  }

  function addActionNode(action) {
    _actionCount++;
    const chainEl = document.getElementById('wfActionsChain');
    const node    = document.createElement('div');
    node.className = 'wf-action-node wf-builder-node';
    node.dataset.actionIdx = _actionCount;

    const stageOptions   = _stages.map(s => `<option value="${s.id}">${esc(s.name)}</option>`).join('');
    const campaignOptions = _campaigns.map(c => `<option value="${c.id}">${esc(c.name)}</option>`).join('');

    node.innerHTML = `
      <div class="wf-chain-arrow" style="text-align:left;font-size:.8rem;color:#666;margin-bottom:.4rem">▼</div>
      <div class="wfn-label">ACTION ${_actionCount}</div>
      <select class="form-select form-select-sm bg-dark text-white border-secondary mt-2 action-type-sel"
              onchange="WorkflowApp.onActionTypeChange(this)">
        <option value="add_to_pipeline" ${action.action_type==='add_to_pipeline'?'selected':''}>Add to Pipeline</option>
        <option value="enroll_campaign" ${action.action_type==='enroll_campaign'?'selected':''}>Enroll in Campaign</option>
        <option value="move_pipeline"   ${action.action_type==='move_pipeline'?'selected':''}>Move Pipeline Stage</option>
        <option value="scrape_job"      ${action.action_type==='scrape_job'?'selected':''}>Start Scrape Job</option>
        <option value="send_notification" ${action.action_type==='send_notification'?'selected':''}>Send Notification</option>
      </select>
      <div class="action-config mt-2">
        ${buildActionConfig(action)}
      </div>
      <button class="btn btn-xs btn-outline-danger mt-2" onclick="this.closest('.wf-action-node').remove()">Remove</button>
    `;
    chainEl.appendChild(node);
  }

  function buildActionConfig(action) {
    const stageOptions    = _stages.map(s => `<option value="${s.id}">${esc(s.name)}</option>`).join('');
    const campaignOptions = _campaigns.map(c => `<option value="${c.id}">${esc(c.name)}</option>`).join('');
    const c = action.config || {};
    switch (action.action_type) {
      case 'add_to_pipeline':
      case 'move_pipeline':
        return `<select class="form-select form-select-sm bg-dark text-white border-secondary action-stage-sel">${stageOptions}</select>`;
      case 'enroll_campaign':
        return `<select class="form-select form-select-sm bg-dark text-white border-secondary action-campaign-sel">${campaignOptions}</select>`;
      case 'scrape_job':
        return `
          <input class="form-control form-control-sm bg-dark text-white border-secondary action-kw-field mb-1"
                 placeholder="Keyword (e.g. dentist)" value="${esc(c.keyword||'')}">
          <input class="form-control form-control-sm bg-dark text-white border-secondary action-place-field"
                 placeholder="Place (e.g. New York)" value="${esc(c.place||'')}">
        `;
      case 'send_notification':
        return `<input class="form-control form-control-sm bg-dark text-white border-secondary action-msg-field"
                        placeholder="Notification message" value="${esc(c.message||'')}">`;
      default: return '';
    }
  }

  WorkflowApp.onActionTypeChange = function(sel) {
    const node     = sel.closest('.wf-action-node');
    const configEl = node.querySelector('.action-config');
    const actType  = sel.value;
    configEl.innerHTML = buildActionConfig({ action_type: actType, config: {} });
  };

  // ── Trigger helpers ───────────────────────────────────────────────────
  WorkflowApp.onTriggerTypeChange = function() {
    const type = document.getElementById('triggerType').value;
    document.getElementById('triggerEventConfig').style.display    = type === 'event'    ? '' : 'none';
    document.getElementById('triggerScheduleConfig').style.display = type === 'schedule' ? '' : 'none';
  };

  function applyTriggerConfig(trig) {
    const conf = typeof trig.config === 'string' ? JSON.parse(trig.config || '{}') : (trig.config || {});
    document.getElementById('triggerType').value = trig.trigger_type;
    WorkflowApp.onTriggerTypeChange();
    if (trig.trigger_type === 'event' && conf.event_type) {
      document.getElementById('triggerEvent').value = conf.event_type;
      const conds = conf.conditions || {};
      const firstKey = Object.keys(conds)[0];
      if (firstKey) {
        document.getElementById('condKey').value = firstKey;
        const val = conds[firstKey];
        if (typeof val === 'object') {
          const op = Object.keys(val)[0];
          document.getElementById('condOp').value  = op;
          document.getElementById('condVal').value = val[op];
        } else {
          document.getElementById('condOp').value  = 'eq';
          document.getElementById('condVal').value = val;
        }
      }
    } else if (trig.trigger_type === 'schedule' && conf.cron) {
      document.getElementById('triggerCron').value = conf.cron;
    }
  }

  function buildTriggerPayload() {
    const type = document.getElementById('triggerType').value;
    let config = {};
    if (type === 'event') {
      const evType = document.getElementById('triggerEvent').value;
      const key    = document.getElementById('condKey').value;
      const op     = document.getElementById('condOp').value;
      const val    = document.getElementById('condVal').value.trim();
      const cond   = op === 'eq' ? val : { [op]: isNaN(val) ? val : parseFloat(val) };
      config = { event_type: evType, conditions: val ? { [key]: cond } : {} };
    } else {
      config = { cron: document.getElementById('triggerCron').value.trim() };
    }
    return { trigger_type: type, config };
  }

  function buildActionsPayload() {
    const nodes = document.querySelectorAll('#wfActionsChain .wf-action-node');
    return Array.from(nodes).map((node, i) => {
      const actType = node.querySelector('.action-type-sel')?.value || '';
      let config = {};
      const stageSel    = node.querySelector('.action-stage-sel');
      const campaignSel = node.querySelector('.action-campaign-sel');
      const kwField     = node.querySelector('.action-kw-field');
      const placeField  = node.querySelector('.action-place-field');
      const msgField    = node.querySelector('.action-msg-field');

      if (stageSel)    config.stage_id    = parseInt(stageSel.value);
      if (campaignSel) config.campaign_id = parseInt(campaignSel.value);
      if (kwField)     config.keyword     = kwField.value;
      if (placeField)  config.place       = placeField.value;
      if (msgField)    config.message     = msgField.value;
      if (actType === 'scrape_job') config.type = 'gmaps';

      return { action_type: actType, config, step_order: i + 1 };
    });
  }

  function resetBuilder() {
    _editingId   = null;
    _actionCount = 0;
    document.getElementById('wfActionsChain').innerHTML = '';
    document.getElementById('triggerType').value  = 'event';
    document.getElementById('condKey').value  = 'tier';
    document.getElementById('condOp').value   = 'eq';
    document.getElementById('condVal').value  = '';
    WorkflowApp.onTriggerTypeChange();
  }

  // ── Load metadata ─────────────────────────────────────────────────────
  async function loadStages() {
    const res  = await fetch('/api/pipeline/stages').catch(() => null);
    if (!res || !res.ok) return;
    const d = await res.json();
    _stages = d.stages || [];
  }

  async function loadCampaigns() {
    const res = await fetch('/api/campaigns').catch(() => null);
    if (!res || !res.ok) return;
    const d   = await res.json();
    _campaigns = d.campaigns || [];
  }

  // ── Helpers ───────────────────────────────────────────────────────────
  function toast(msg, type = 'success') {
    const el = document.createElement('div');
    el.className   = `toast align-items-center text-white border-0 bg-${type === 'success' ? 'success' : type === 'warning' ? 'warning' : 'danger'}`;
    el.style.cssText = 'position:fixed;bottom:1.5rem;right:1.5rem;z-index:9999';
    el.innerHTML   = `<div class="d-flex"><div class="toast-body">${msg}</div><button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button></div>`;
    document.body.appendChild(el);
    new bootstrap.Toast(el, { delay: 3000 }).show();
    setTimeout(() => el.remove(), 4000);
  }

  function esc(s) { return (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;') }
  function fmtTime(iso) { if (!iso) return '—'; const d = new Date(iso); return isNaN(d) ? iso : d.toLocaleString() }

  // Expose needed methods so template onclick calls work
  WorkflowApp.showCreateModal = showCreateModal;
  WorkflowApp.init = init;

  return WorkflowApp;
})();
