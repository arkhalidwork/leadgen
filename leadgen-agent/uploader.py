"""
LeadGen Agent — Result & Checkpoint Uploader

Throttled uploader that:
- Buffers progress updates (max 1 per 5s)
- Uploads checkpoints (max 1 per 30s) after local save
- Uploads final result in 50-lead chunks
- Flushes pending checkpoints on reconnect
"""
from __future__ import annotations

import logging
import time

import api_client
import storage
import config

log = logging.getLogger(__name__)

# Per-job state
_last_progress: dict[str, float] = {}
_last_checkpoint: dict[str, float] = {}


def maybe_upload_progress(
    job_id: str,
    progress: int,
    message: str,
    phase: str = "",
    phase_detail: str = "",
    result_count: int = 0,
    elapsed: int = 0,
) -> dict | None:
    """Upload progress if throttle interval has passed. Returns server response or None."""
    now = time.time()
    if now - _last_progress.get(job_id, 0) < config.PROGRESS_THROTTLE_SECONDS:
        return None
    _last_progress[job_id] = now

    return api_client.job_progress(job_id, progress, message, phase, phase_detail, result_count, elapsed)


def maybe_upload_checkpoint(
    job_id: str,
    seq: int,
    phase: str,
    data: dict,
) -> dict | None:
    """
    Always saves checkpoint locally first, then uploads to server if throttle allows.
    """
    # LOCAL FIRST — survives agent crash
    storage.save_checkpoint(job_id, seq, phase, data)

    now = time.time()
    if now - _last_checkpoint.get(job_id, 0) < config.CHECKPOINT_THROTTLE_SECONDS:
        return None
    _last_checkpoint[job_id] = now

    leads_so_far = data.get("leads_so_far", [])
    resp = api_client.job_checkpoint(job_id, seq, phase, data, leads_partial=leads_so_far)
    if resp and resp.get("ok"):
        storage.mark_checkpoint_uploaded(job_id, seq)
    return resp


def flush_pending_checkpoints(job_id: str) -> None:
    """Upload any checkpoints that weren't uploaded due to network failure."""
    pending = storage.get_pending_checkpoints(job_id)
    for ckpt in pending:
        import json
        try:
            data = json.loads(ckpt.get("data") or "{}")
        except Exception:
            data = {}
        leads = data.get("leads_so_far", [])
        resp = api_client.job_checkpoint(job_id, ckpt["seq"], ckpt["phase"], data, leads_partial=leads)
        if resp and resp.get("ok"):
            storage.mark_checkpoint_uploaded(job_id, ckpt["seq"])
            log.info(f"Flushed pending checkpoint seq={ckpt['seq']} for job {job_id}")


def upload_final_result(job_id: str, status: str, message: str, result: dict) -> bool:
    """
    Upload final result. Chunks leads in batches of 50 for large results.
    Returns True if server confirmed receipt.
    """
    leads = result.get("leads", [])
    BATCH_SIZE = 50

    if len(leads) <= BATCH_SIZE:
        resp = api_client.job_complete(job_id, status, message, result)
        return bool(resp and resp.get("ok"))

    # Chunked upload: send leads in batches, complete on last batch
    log.info(f"Chunked upload: {len(leads)} leads for job {job_id}")
    for i in range(0, len(leads), BATCH_SIZE):
        batch = leads[i:i + BATCH_SIZE]
        is_last = (i + BATCH_SIZE) >= len(leads)

        if is_last:
            chunk_result = dict(result)
            chunk_result["leads"] = batch
            resp = api_client.job_complete(job_id, status, message, chunk_result)
            if not resp or not resp.get("ok"):
                return False
        else:
            api_client.job_checkpoint(job_id, 9000 + i // BATCH_SIZE, "upload",
                                      {"leads_batch": batch}, leads_partial=batch)

    return True


def reset_job_state(job_id: str) -> None:
    """Clear throttle state when a job finishes."""
    _last_progress.pop(job_id, None)
    _last_checkpoint.pop(job_id, None)
