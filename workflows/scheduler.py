"""
LeadGen — APScheduler: outreach sender + workflow evaluator

Starts two background jobs:
  - process_campaign_queue()  every 5 minutes
  - WorkflowEngine.evaluate_all()  every 60 seconds

Call start_scheduler() once from app.py after all tables are ensured.
"""
from __future__ import annotations

import logging
import os

log = logging.getLogger(__name__)

_scheduler = None


def start_scheduler() -> None:
    global _scheduler
    if _scheduler is not None:
        return  # Already started

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        from apscheduler.executors.pool import ThreadPoolExecutor
    except ImportError:
        log.warning("APScheduler not installed — background jobs disabled. "
                    "Run: pip install apscheduler")
        return

    executors = {"default": ThreadPoolExecutor(4)}
    _scheduler = BackgroundScheduler(executors=executors, timezone="UTC")

    # 1. Campaign sender (every 5 minutes)
    _scheduler.add_job(
        _run_campaign_queue,
        trigger="interval",
        minutes=5,
        id="campaign_sender",
        replace_existing=True,
        max_instances=1,
    )

    # 2. Workflow evaluator (every 60 seconds)
    _scheduler.add_job(
        _run_workflow_engine,
        trigger="interval",
        seconds=60,
        id="workflow_engine",
        replace_existing=True,
        max_instances=1,
    )

    # 3. Reply detector (every 10 minutes) — only if IMAP configured
    _scheduler.add_job(
        _run_reply_detector,
        trigger="interval",
        minutes=10,
        id="reply_detector",
        replace_existing=True,
        max_instances=1,
    )

    _scheduler.start()
    log.info("Phase 5 scheduler started: campaign_sender(5m) + workflow_engine(60s) + reply_detector(10m)")


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        _scheduler = None


def _run_campaign_queue() -> None:
    try:
        from outreach.sender import process_campaign_queue
        sent = process_campaign_queue()
        if sent:
            log.info(f"Campaign sender: {sent} emails sent")
    except Exception as exc:
        log.error(f"Campaign sender error: {exc}", exc_info=True)


def _run_workflow_engine() -> None:
    try:
        from workflows.engine import WorkflowEngine
        result = WorkflowEngine().evaluate_all()
        if result.get("schedules_fired") or result.get("events_processed"):
            log.info(f"Workflow engine: {result}")
    except Exception as exc:
        log.error(f"Workflow engine error: {exc}", exc_info=True)


def _run_reply_detector() -> None:
    try:
        from jobs.store import _get_db
        db   = _get_db()
        # Get all users with SMTP config that has is_verified=1
        rows = db.execute(
            "SELECT user_id FROM user_smtp_config WHERE is_verified=1 AND smtp_host != ''"
        ).fetchall()
        for row in rows:
            try:
                from outreach.reply_detector import poll_inbox_for_replies
                found = poll_inbox_for_replies(row[0])
                if found:
                    log.info(f"Reply detector: {found} replies for user {row[0]}")
            except Exception as exc:
                log.debug(f"Reply detector user {row[0]}: {exc}")
    except Exception as exc:
        log.error(f"Reply detector error: {exc}", exc_info=True)
