from __future__ import annotations

import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable


Runner = Callable[..., None]


class _JobPool:
    def __init__(self, *, max_workers: int, max_pending: int, per_user_active_limit: int, pending_ttl_seconds: int):
        self._executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="leadgen-pool")
        self._max_pending = max_pending
        self._per_user_active_limit = max(1, int(per_user_active_limit))
        self._pending_ttl_seconds = max(30, int(pending_ttl_seconds))
        self._lock = threading.Lock()
        self._pending: dict[str, dict] = {}
        self._active: dict[str, dict] = {}

    def _prune_stale_pending_locked(self):
        now = time.time()
        stale = [
            job_key for job_key, meta in self._pending.items()
            if (now - float(meta.get("enqueued_at") or now)) > self._pending_ttl_seconds
        ]
        for job_key in stale:
            self._pending.pop(job_key, None)

    def _active_count_for_user_locked(self, user_key: str) -> int:
        return sum(1 for meta in self._active.values() if str(meta.get("user_key") or "") == str(user_key or ""))

    def submit(self, job_key: str, user_key: str, fn: Runner, *args, **kwargs) -> tuple[bool, str, dict]:
        with self._lock:
            self._prune_stale_pending_locked()

            if job_key in self._pending or job_key in self._active:
                return False, "already_enqueued", self.stats()

            if len(self._pending) >= self._max_pending:
                return False, "queue_full", self.stats()

            if self._active_count_for_user_locked(user_key) >= self._per_user_active_limit:
                return False, "user_active_quota_reached", self.stats()

            self._pending[job_key] = {
                "user_key": str(user_key or ""),
                "enqueued_at": time.time(),
            }

        def _wrapped():
            with self._lock:
                meta = self._pending.pop(job_key, {"user_key": str(user_key or "")})
                self._active[job_key] = {
                    "user_key": str(meta.get("user_key") or user_key or ""),
                    "started_at": time.time(),
                }
            try:
                fn(*args, **kwargs)
            finally:
                with self._lock:
                    self._active.pop(job_key, None)

        self._executor.submit(_wrapped)
        return True, "accepted", self.stats()

    def stats(self) -> dict:
        with self._lock:
            self._prune_stale_pending_locked()
            pending_by_user: dict[str, int] = {}
            active_by_user: dict[str, int] = {}
            for meta in self._pending.values():
                key = str(meta.get("user_key") or "unknown")
                pending_by_user[key] = pending_by_user.get(key, 0) + 1
            for meta in self._active.values():
                key = str(meta.get("user_key") or "unknown")
                active_by_user[key] = active_by_user.get(key, 0) + 1
            return {
                "pending": len(self._pending),
                "active": len(self._active),
                "max_pending": self._max_pending,
                "per_user_active_limit": self._per_user_active_limit,
                "pending_ttl_seconds": self._pending_ttl_seconds,
                "pending_by_user": pending_by_user,
                "active_by_user": active_by_user,
            }


_EXTRACT_WORKERS = max(1, int(os.environ.get("LEADGEN_EXTRACT_WORKERS", "2")))
_CONTACT_WORKERS = max(1, int(os.environ.get("LEADGEN_CONTACT_WORKERS", "2")))
_EXTRACT_MAX_PENDING = max(1, int(os.environ.get("LEADGEN_EXTRACT_MAX_PENDING", "25")))
_CONTACT_MAX_PENDING = max(1, int(os.environ.get("LEADGEN_CONTACT_MAX_PENDING", "25")))
_EXTRACT_PER_USER_ACTIVE_LIMIT = max(1, int(os.environ.get("LEADGEN_EXTRACT_PER_USER_ACTIVE_LIMIT", "1")))
_CONTACT_PER_USER_ACTIVE_LIMIT = max(1, int(os.environ.get("LEADGEN_CONTACT_PER_USER_ACTIVE_LIMIT", "1")))
_QUEUE_PENDING_TTL_SECONDS = max(30, int(os.environ.get("LEADGEN_QUEUE_PENDING_TTL_SECONDS", "900")))

_extract_pool = _JobPool(
    max_workers=_EXTRACT_WORKERS,
    max_pending=_EXTRACT_MAX_PENDING,
    per_user_active_limit=_EXTRACT_PER_USER_ACTIVE_LIMIT,
    pending_ttl_seconds=_QUEUE_PENDING_TTL_SECONDS,
)
_contact_pool = _JobPool(
    max_workers=_CONTACT_WORKERS,
    max_pending=_CONTACT_MAX_PENDING,
    per_user_active_limit=_CONTACT_PER_USER_ACTIVE_LIMIT,
    pending_ttl_seconds=_QUEUE_PENDING_TTL_SECONDS,
)


def submit_extract_job(job_id: str, user_id: int | str, fn: Runner, *args, **kwargs) -> tuple[bool, str, dict]:
    return _extract_pool.submit(str(job_id), str(user_id), fn, *args, **kwargs)


def submit_contact_job(job_id: str, user_id: int | str, fn: Runner, *args, **kwargs) -> tuple[bool, str, dict]:
    return _contact_pool.submit(str(job_id), str(user_id), fn, *args, **kwargs)


def worker_pool_stats() -> dict:
    return {
        "extract": _extract_pool.stats(),
        "contacts": _contact_pool.stats(),
    }
