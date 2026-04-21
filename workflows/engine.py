"""
LeadGen — Workflow Engine

WorkflowEngine.evaluate_all() is called every 60s by APScheduler.
Processes schedule triggers + event queue triggers.
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_DB_PATH = os.environ.get("LEADGEN_DB_PATH", "leadgen.db")
_local   = threading.local()
_engine_lock = threading.Lock()  # single-threaded evaluation


def _db() -> sqlite3.Connection:
    if not hasattr(_local, "db") or _local.db is None:
        _local.db = sqlite3.connect(_DB_PATH, timeout=30)
        _local.db.row_factory = sqlite3.Row
        _local.db.execute("PRAGMA journal_mode=WAL")
        _local.db.execute("PRAGMA busy_timeout=10000")
    return _local.db


class WorkflowEngine:
    """
    Trigger-Action workflow evaluator.
    Designed for single-threaded execution to prevent duplicate runs.
    """

    def evaluate_all(self) -> dict:
        if not _engine_lock.acquire(blocking=False):
            log.debug("Workflow engine: previous run still active, skipping")
            return {"skipped": True}

        try:
            schedules = self._process_schedule_triggers()
            events    = self._process_event_queue()
            return {"schedules_fired": schedules, "events_processed": events}
        finally:
            _engine_lock.release()

    # ── Schedule Triggers ──────────────────────────────────────────────────

    def _process_schedule_triggers(self) -> int:
        db  = _db()
        now = datetime.now(timezone.utc).isoformat()
        fired = 0

        due = db.execute("""
            SELECT wt.id, wt.workflow_id, wt.config, w.user_id
            FROM workflow_triggers wt
            JOIN workflows w ON w.id = wt.workflow_id
            WHERE wt.trigger_type = 'schedule'
              AND wt.next_run_at IS NOT NULL
              AND wt.next_run_at <= ?
              AND w.status = 'active'
        """, (now,)).fetchall()

        for trigger in due:
            wid  = trigger["workflow_id"]
            conf = _parse_json(trigger["config"])
            uid  = trigger["user_id"]
            self._execute_workflow(wid, uid, trigger_event={}, idempotency_key=None)
            fired += 1

            # Advance next_run_at via croniter (optional dep)
            cron = conf.get("cron", "0 9 * * *")
            try:
                from croniter import croniter
                nxt = croniter(cron, datetime.now(timezone.utc)).get_next(datetime)
                db.execute("UPDATE workflow_triggers SET next_run_at=? WHERE id=?",
                           (nxt.isoformat(), trigger["id"]))
            except Exception:
                # Without croniter just advance by 24h
                from datetime import timedelta
                nxt = datetime.now(timezone.utc) + timedelta(hours=24)
                db.execute("UPDATE workflow_triggers SET next_run_at=? WHERE id=?",
                           (nxt.isoformat(), trigger["id"]))
            db.commit()

        return fired

    # ── Event Queue ────────────────────────────────────────────────────────

    def _process_event_queue(self) -> int:
        db = _db()
        events = db.execute("""
            SELECT * FROM workflow_event_queue
            WHERE processed = 0
            ORDER BY created_at ASC
            LIMIT 100
        """).fetchall()

        processed_count = 0
        for event in events:
            ev_type  = event["event_type"]
            ev_data  = _parse_json(event["payload"])
            user_id  = event["user_id"]
            event_id = event["id"]

            matching_triggers = db.execute("""
                SELECT wt.id, wt.workflow_id, wt.config, w.user_id
                FROM workflow_triggers wt
                JOIN workflows w ON w.id = wt.workflow_id
                WHERE wt.trigger_type = 'event'
                  AND w.user_id = ?
                  AND w.status = 'active'
                  AND json_extract(wt.config, '$.event_type') = ?
            """, (user_id, ev_type)).fetchall()

            for trigger in matching_triggers:
                conf       = _parse_json(trigger["config"])
                conditions = conf.get("conditions", {})
                idem_key   = f"{trigger['workflow_id']}:ev:{event_id}"

                if self._matches_conditions(ev_data, conditions):
                    self._execute_workflow(
                        trigger["workflow_id"], user_id,
                        trigger_event=ev_data, idempotency_key=idem_key,
                    )

            db.execute(
                "UPDATE workflow_event_queue SET processed=1 WHERE id=?", (event_id,)
            )
            db.commit()
            processed_count += 1

        return processed_count

    # ── Execute Workflow ───────────────────────────────────────────────────

    def _execute_workflow(self, workflow_id: int, user_id: int,
                           trigger_event: dict, idempotency_key: str | None) -> None:
        db = _db()

        # Idempotency check
        if idempotency_key:
            exists = db.execute(
                "SELECT id FROM workflow_runs WHERE idempotency_key=?",
                (idempotency_key,)
            ).fetchone()
            if exists:
                return

        actions = db.execute(
            "SELECT * FROM workflow_actions WHERE workflow_id=? ORDER BY step_order",
            (workflow_id,)
        ).fetchall()

        run_id = db.execute("""
            INSERT INTO workflow_runs
                (workflow_id, trigger_event, status, actions_total, idempotency_key)
            VALUES (?, ?, 'running', ?, ?)
        """, (
            workflow_id,
            json.dumps(trigger_event, default=str),
            len(actions),
            idempotency_key,
        )).lastrowid
        db.commit()

        success = True
        for action in actions:
            act_type = action["action_type"]
            config   = _parse_json(action["config"])
            order    = action["step_order"]

            db.execute("""
                INSERT INTO workflow_run_logs (run_id, step_order, action_type, status)
                VALUES (?, ?, ?, 'started')
            """, (run_id, order, act_type))
            db.commit()

            try:
                result = self._run_action(act_type, config, trigger_event, user_id)
                db.execute("""
                    UPDATE workflow_run_logs SET status='completed', output=?
                    WHERE run_id=? AND step_order=?
                """, (json.dumps(result, default=str), run_id, order))
                db.execute(
                    "UPDATE workflow_runs SET actions_done=actions_done+1 WHERE id=?", (run_id,)
                )
                db.commit()
            except Exception as exc:
                log.error(f"Workflow {workflow_id} action {act_type} failed: {exc}")
                db.execute("""
                    UPDATE workflow_run_logs SET status='failed', output=?
                    WHERE run_id=? AND step_order=?
                """, (str(exc), run_id, order))
                db.execute(
                    "UPDATE workflow_runs SET status='failed', error=? WHERE id=?",
                    (str(exc), run_id)
                )
                db.commit()
                success = False
                break

        if success:
            db.execute("""
                UPDATE workflow_runs SET status='completed', finished_at=datetime('now')
                WHERE id=?
            """, (run_id,))
        db.execute(
            "UPDATE workflows SET run_count=run_count+1, last_run_at=datetime('now') WHERE id=?",
            (workflow_id,)
        )
        db.commit()

    # ── Action Runners ─────────────────────────────────────────────────────

    def _run_action(self, action_type: str, config: dict,
                    event: dict, user_id: int) -> dict:
        if action_type == "scrape_job":
            return self._action_scrape_job(config, user_id)

        elif action_type == "add_to_pipeline":
            return self._action_add_to_pipeline(config, event, user_id)

        elif action_type == "enroll_campaign":
            return self._action_enroll_campaign(config, event, user_id)

        elif action_type == "move_pipeline":
            return self._action_move_pipeline(config, event, user_id)

        elif action_type == "send_notification":
            return self._action_notification(config, event, user_id)

        elif action_type == "update_score":
            return self._action_rescore(event)

        else:
            raise ValueError(f"Unknown action type: {action_type}")

    def _action_scrape_job(self, config: dict, user_id: int) -> dict:
        from jobs.store import create_job
        from jobs.queue import enqueue_job
        job_type = config.get("type", "gmaps")
        payload  = {k: v for k, v in config.items() if k != "type"}
        job_id   = create_job(None, user_id, job_type, payload)
        enqueue_job(job_id, job_type, 1)
        log.info(f"Workflow started scrape job {job_id} (type={job_type})")
        return {"job_id": job_id}

    def _action_add_to_pipeline(self, config: dict, event: dict, user_id: int) -> dict:
        from crm.service import bulk_add_leads
        lead_id  = event.get("lead_id")
        stage_id = config.get("stage_id")
        if lead_id and stage_id:
            result = bulk_add_leads([int(lead_id)], int(stage_id), user_id, source="workflow")
            return result
        return {"skipped": True, "reason": "Missing lead_id or stage_id"}

    def _action_enroll_campaign(self, config: dict, event: dict, user_id: int) -> dict:
        from outreach.sender import enroll_lead
        lead_id     = event.get("lead_id")
        campaign_id = config.get("campaign_id")
        if lead_id and campaign_id:
            ok = enroll_lead(int(campaign_id), int(lead_id), user_id)
            return {"enrolled": ok}
        return {"skipped": True}

    def _action_move_pipeline(self, config: dict, event: dict, user_id: int) -> dict:
        from crm.service import move_item
        db       = _db()
        lead_id  = event.get("lead_id")
        stage_id = config.get("stage_id")
        if lead_id and stage_id:
            item = db.execute(
                "SELECT id FROM pipeline_items WHERE lead_id=? AND user_id=? AND status='active'",
                (lead_id, user_id)
            ).fetchone()
            if item:
                ok = move_item(item["id"], int(stage_id), user_id)
                return {"moved": ok}
        return {"skipped": True}

    def _action_notification(self, config: dict, event: dict, user_id: int) -> dict:
        # Simple in-app notification (email notification is future)
        log.info(f"Workflow notification for user {user_id}: {config.get('message','')}")
        return {"notified": True}

    def _action_rescore(self, event: dict) -> dict:
        lead_id = event.get("lead_id")
        if lead_id:
            try:
                from intelligence.pipeline import _rescore_lead
                _rescore_lead(int(lead_id))
                return {"rescored": True}
            except Exception as exc:
                return {"rescored": False, "error": str(exc)}
        return {"skipped": True}

    # ── Condition Matching ─────────────────────────────────────────────────

    def _matches_conditions(self, event: dict, conditions: dict) -> bool:
        """
        Supports:
          {"tier": "hot"}                 — equality
          {"score": {"gt": 70}}           — greater than
          {"tier": {"in": ["hot","warm"]}}— membership
        """
        for key, condition in conditions.items():
            val = event.get(key)
            if isinstance(condition, dict):
                op, cmp = next(iter(condition.items()))
                if op == "eq" and val != cmp:            return False
                if op == "ne" and val == cmp:            return False
                if op == "gt":
                    try:
                        if not (float(val) > float(cmp)): return False
                    except (TypeError, ValueError):       return False
                if op == "lt":
                    try:
                        if not (float(val) < float(cmp)): return False
                    except (TypeError, ValueError):       return False
                if op == "in" and val not in cmp:        return False
                if op == "not_in" and val in cmp:        return False
            else:
                if val != condition: return False
        return True


# ── Shared emit_event ──────────────────────────────────────────────────────

def emit_event(event_type: str, payload: dict) -> None:
    """
    Shared entry point — called from intelligence, CRM, outreach.
    Inserts into workflow_event_queue for async processing.
    """
    try:
        db = _db()
        db.execute("""
            INSERT INTO workflow_event_queue (user_id, event_type, payload)
            VALUES (?, ?, ?)
        """, (payload.get("user_id"), event_type, json.dumps(payload, default=str)))
        db.commit()
    except Exception as exc:
        log.debug(f"emit_event {event_type}: {exc}")


# ── Helpers ────────────────────────────────────────────────────────────────

def _parse_json(val, default=None):
    if default is None:
        default = {}
    if not val:
        return default
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return default
