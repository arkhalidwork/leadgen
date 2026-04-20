from __future__ import annotations

import json
import logging
import os
import importlib
from datetime import datetime

log = logging.getLogger(__name__)

try:
    psycopg2 = importlib.import_module("psycopg2")
except Exception:  # pragma: no cover
    psycopg2 = None


_POSTGRES_DSN = os.environ.get("LEADGEN_POSTGRES_DSN", "").strip()


def postgres_enabled() -> bool:
    return bool(_POSTGRES_DSN and psycopg2 is not None)


def _connect():
    if not postgres_enabled():
        return None
    return psycopg2.connect(_POSTGRES_DSN)


def ensure_schema() -> bool:
    if not postgres_enabled():
        return False
    try:
        conn = _connect()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gmaps_sessions (
                session_id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                keyword TEXT DEFAULT '',
                place TEXT DEFAULT '',
                max_leads BIGINT,
                phase TEXT DEFAULT 'extract',
                extraction_status TEXT DEFAULT 'pending',
                contacts_status TEXT DEFAULT 'pending',
                status TEXT DEFAULT 'PENDING',
                progress BIGINT DEFAULT 0,
                message TEXT DEFAULT '',
                results_count BIGINT DEFAULT 0,
                created_at TEXT,
                updated_at TEXT,
                finished_at TEXT
            );
            CREATE TABLE IF NOT EXISTS gmaps_session_events (
                id BIGSERIAL PRIMARY KEY,
                session_id TEXT NOT NULL,
                user_id BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                severity TEXT DEFAULT 'info',
                phase TEXT DEFAULT 'extract',
                status TEXT DEFAULT 'PENDING',
                progress BIGINT DEFAULT 0,
                message TEXT DEFAULT '',
                payload TEXT DEFAULT '{}',
                event_hash TEXT NOT NULL,
                created_at TEXT,
                UNIQUE(session_id, event_hash)
            );
            CREATE TABLE IF NOT EXISTS gmaps_session_tasks (
                id BIGSERIAL PRIMARY KEY,
                session_id TEXT NOT NULL,
                user_id BIGINT NOT NULL,
                task_key TEXT NOT NULL,
                phase TEXT DEFAULT 'extract',
                status TEXT DEFAULT 'pending',
                attempt_count BIGINT DEFAULT 0,
                last_error TEXT DEFAULT '',
                payload TEXT DEFAULT '{}',
                max_attempts BIGINT DEFAULT 3,
                retry_backoff_seconds BIGINT DEFAULT 45,
                retry_cooldown_until TEXT,
                last_retry_reason TEXT DEFAULT '',
                last_retry_at TEXT,
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(session_id, task_key)
            );
            CREATE TABLE IF NOT EXISTS gmaps_task_chunks (
                id BIGSERIAL PRIMARY KEY,
                session_id TEXT NOT NULL,
                user_id BIGINT NOT NULL,
                task_key TEXT NOT NULL,
                chunk_key TEXT NOT NULL,
                status TEXT DEFAULT 'pending',
                attempt_count BIGINT DEFAULT 0,
                last_error TEXT DEFAULT '',
                payload TEXT DEFAULT '{}',
                started_at TEXT,
                finished_at TEXT,
                created_at TEXT,
                updated_at TEXT,
                UNIQUE(session_id, task_key, chunk_key)
            );
            """
        )
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as exc:
        log.error("Postgres mirror schema init failed: %s", exc)
        return False


def mirror_session_state(state: dict) -> None:
    if not postgres_enabled():
        return
    try:
        conn = _connect()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO gmaps_sessions (
                session_id, user_id, keyword, place, max_leads,
                phase, extraction_status, contacts_status, status,
                progress, message, results_count, created_at, updated_at, finished_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(session_id) DO UPDATE SET
                keyword=EXCLUDED.keyword,
                place=EXCLUDED.place,
                max_leads=EXCLUDED.max_leads,
                phase=EXCLUDED.phase,
                extraction_status=EXCLUDED.extraction_status,
                contacts_status=EXCLUDED.contacts_status,
                status=EXCLUDED.status,
                progress=EXCLUDED.progress,
                message=EXCLUDED.message,
                results_count=EXCLUDED.results_count,
                updated_at=EXCLUDED.updated_at,
                finished_at=EXCLUDED.finished_at
            """,
            (
                str(state.get("job_id") or ""),
                int(state.get("user_id") or 0),
                str(state.get("keyword") or ""),
                str(state.get("place") or ""),
                state.get("max_leads"),
                str(state.get("phase") or "extract"),
                str(state.get("extraction_status") or "pending"),
                str(state.get("contacts_status") or "pending"),
                str(state.get("status") or "PENDING"),
                int(state.get("progress") or 0),
                str(state.get("message") or ""),
                int(state.get("results_count") or 0),
                state.get("created_at") or datetime.utcnow().isoformat(),
                state.get("updated_at") or datetime.utcnow().isoformat(),
                state.get("finished_at"),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        log.error("Postgres mirror session upsert failed: %s", exc)


def mirror_event(*, session_id: str, user_id: int, event_type: str, severity: str,
                 phase: str, status: str, progress: int, message: str, payload: dict,
                 event_hash: str, created_at: str) -> None:
    if not postgres_enabled():
        return
    try:
        conn = _connect()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO gmaps_session_events (
                session_id, user_id, event_type, severity,
                phase, status, progress, message, payload, event_hash, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(session_id, event_hash) DO NOTHING
            """,
            (
                session_id,
                int(user_id),
                event_type,
                severity,
                phase,
                status,
                int(progress or 0),
                message,
                json.dumps(payload or {}, default=str),
                event_hash,
                created_at,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        log.error("Postgres mirror event upsert failed: %s", exc)


def mirror_task(*, session_id: str, user_id: int, task_key: str, phase: str, status: str,
                attempt_count: int, last_error: str, payload: dict,
                max_attempts: int, retry_backoff_seconds: int,
                retry_cooldown_until: str | None, last_retry_reason: str,
                last_retry_at: str | None, started_at: str | None,
                finished_at: str | None, now: str) -> None:
    if not postgres_enabled():
        return
    try:
        conn = _connect()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO gmaps_session_tasks (
                session_id, user_id, task_key, phase, status,
                attempt_count, last_error, payload,
                max_attempts, retry_backoff_seconds, retry_cooldown_until,
                last_retry_reason, last_retry_at,
                started_at, finished_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(session_id, task_key) DO UPDATE SET
                phase=EXCLUDED.phase,
                status=EXCLUDED.status,
                attempt_count=EXCLUDED.attempt_count,
                last_error=EXCLUDED.last_error,
                payload=EXCLUDED.payload,
                max_attempts=EXCLUDED.max_attempts,
                retry_backoff_seconds=EXCLUDED.retry_backoff_seconds,
                retry_cooldown_until=EXCLUDED.retry_cooldown_until,
                last_retry_reason=EXCLUDED.last_retry_reason,
                last_retry_at=EXCLUDED.last_retry_at,
                started_at=EXCLUDED.started_at,
                finished_at=EXCLUDED.finished_at,
                updated_at=EXCLUDED.updated_at
            """,
            (
                session_id,
                int(user_id),
                task_key,
                phase,
                status,
                int(attempt_count or 0),
                last_error,
                json.dumps(payload or {}, default=str),
                int(max_attempts or 1),
                int(retry_backoff_seconds or 45),
                retry_cooldown_until,
                last_retry_reason,
                last_retry_at,
                started_at,
                finished_at,
                now,
                now,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        log.error("Postgres mirror task upsert failed: %s", exc)


def mirror_task_chunk(*, session_id: str, user_id: int, task_key: str,
                      chunk_key: str, status: str, attempt_count: int,
                      last_error: str, payload: dict, started_at: str | None,
                      finished_at: str | None, now: str) -> None:
    if not postgres_enabled():
        return
    try:
        conn = _connect()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO gmaps_task_chunks (
                session_id, user_id, task_key, chunk_key, status,
                attempt_count, last_error, payload,
                started_at, finished_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(session_id, task_key, chunk_key) DO UPDATE SET
                status=EXCLUDED.status,
                attempt_count=EXCLUDED.attempt_count,
                last_error=EXCLUDED.last_error,
                payload=EXCLUDED.payload,
                started_at=EXCLUDED.started_at,
                finished_at=EXCLUDED.finished_at,
                updated_at=EXCLUDED.updated_at
            """,
            (
                session_id,
                int(user_id),
                task_key,
                chunk_key,
                status,
                int(attempt_count or 0),
                last_error,
                json.dumps(payload or {}, default=str),
                started_at,
                finished_at,
                now,
                now,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as exc:
        log.error("Postgres mirror task chunk upsert failed: %s", exc)
