"""
LeadGen — Server-Sent Events (SSE) streaming module.

Replaces frontend polling with push-based real-time event delivery.
Each scraping job gets its own event stream. Scraper threads call
``publish()`` to push events; the ``/api/stream/<job_id>`` endpoint
yields them to the browser via an ``EventSource`` connection.

Thread-safe. No external dependencies (no Redis, no websocket lib).
"""
from __future__ import annotations

import json
import time
import threading
from collections import defaultdict, deque
from typing import Generator

from flask import Blueprint, Response, request

sse_bp = Blueprint("sse", __name__)

# ── Event Bus ────────────────────────────────────────────────────────────
# Per-job deque of events. Each item is {"event": str, "data": dict, "ts": float}.
# Bounded at 2000 events per job — enough for any scrape session.
_lock = threading.Lock()
_streams: dict[str, deque] = defaultdict(lambda: deque(maxlen=2000))
_waiters: dict[str, threading.Event] = defaultdict(threading.Event)


def publish(job_id: str, event_type: str, data: dict | None = None):
    """
    Push an SSE event from any thread (scraper worker, contact crawler, etc.).

    Args:
        job_id: The scraping job ID.
        event_type: One of: job_started, geocell_progress, lead_found,
                    leads_batch, crawl_started, contact_found, job_completed,
                    log_entry, error.
        data: JSON-serializable payload dict.
    """
    entry = {
        "event": str(event_type),
        "data": data or {},
        "ts": time.time(),
    }
    with _lock:
        _streams[job_id].append(entry)
        # Wake all waiters for this job
        if job_id in _waiters:
            _waiters[job_id].set()


def _stream_generator(job_id: str) -> Generator[str, None, None]:
    """
    Yields SSE-formatted strings for a given job.

    The generator blocks (with timeout) until new events arrive,
    then yields them. Sends heartbeat comments every 15s to keep
    the connection alive through proxies/load balancers.
    """
    cursor = 0
    heartbeat_interval = 15  # seconds
    max_idle_seconds = 600   # 10 minutes with no events → close

    last_event_time = time.time()

    while True:
        # Wait for new events or heartbeat timeout
        with _lock:
            waiter = _waiters[job_id]
        waiter.wait(timeout=heartbeat_interval)
        with _lock:
            waiter.clear()

        # Grab new events since our cursor
        with _lock:
            stream = _streams.get(job_id)
            if stream is None:
                # Stream was cleaned up
                yield "event: stream_closed\ndata: {}\n\n"
                return
            events = list(stream)[cursor:]
            cursor = len(stream)

        if events:
            last_event_time = time.time()
            for evt in events:
                event_type = evt["event"]
                payload = json.dumps(evt["data"], default=str, separators=(",", ":"))
                yield f"event: {event_type}\ndata: {payload}\n\n"

                # If terminal event, close the stream
                if event_type in ("job_completed", "job_failed"):
                    return
        else:
            # No new events — send heartbeat
            yield ": heartbeat\n\n"

            # Check idle timeout
            if time.time() - last_event_time > max_idle_seconds:
                yield "event: stream_timeout\ndata: {}\n\n"
                return


def cleanup_stream(job_id: str):
    """Remove a job's event stream to free memory. Called after job completes + grace period."""
    with _lock:
        _streams.pop(job_id, None)
        _waiters.pop(job_id, None)


def get_stream_events(job_id: str, since: int = 0) -> list[dict]:
    """
    Get buffered events for a job (for catch-up on reconnect).

    Args:
        job_id: The scraping job ID.
        since: Cursor position to start from.

    Returns:
        List of event dicts.
    """
    with _lock:
        stream = _streams.get(job_id)
        if stream is None:
            return []
        return list(stream)[since:]


# ── SSE HTTP Endpoint ────────────────────────────────────────────────────

@sse_bp.route("/api/stream/<job_id>")
def sse_stream(job_id: str):
    """
    SSE endpoint. Browser connects via ``new EventSource('/api/stream/<job_id>')``.

    Returns a ``text/event-stream`` response that pushes events in real time.
    Supports ``Last-Event-ID`` header for reconnection (cursor resume).
    """
    # Support reconnect via cursor
    last_id = request.headers.get("Last-Event-ID")
    if last_id:
        try:
            cursor_start = int(last_id)
        except (TypeError, ValueError):
            cursor_start = 0
    else:
        cursor_start = 0

    def generate():
        cursor = cursor_start
        heartbeat_interval = 15
        max_idle = 600
        last_event_time = time.time()

        while True:
            with _lock:
                waiter = _waiters[job_id]
            waiter.wait(timeout=heartbeat_interval)
            with _lock:
                waiter.clear()

            with _lock:
                stream = _streams.get(job_id)
                if stream is None:
                    yield "event: stream_closed\ndata: {}\n\n"
                    return
                events = list(stream)[cursor:]
                new_cursor = len(stream)

            if events:
                last_event_time = time.time()
                for evt in events:
                    event_type = evt["event"]
                    payload = json.dumps(evt["data"], default=str, separators=(",", ":"))
                    cursor += 1
                    yield f"id: {cursor}\nevent: {event_type}\ndata: {payload}\n\n"

                    if event_type in ("job_completed", "job_failed"):
                        return
                cursor = new_cursor
            else:
                yield ": heartbeat\n\n"
                if time.time() - last_event_time > max_idle:
                    yield "event: stream_timeout\ndata: {}\n\n"
                    return

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # Nginx: disable buffering
            "Connection": "keep-alive",
        },
    )


@sse_bp.route("/api/stream/<job_id>/catchup")
def sse_catchup(job_id: str):
    """
    Non-streaming endpoint for reconnection catch-up.
    Returns all buffered events as a JSON array.
    Used by the frontend when EventSource reconnects to avoid missing events.
    """
    since = request.args.get("since", 0, type=int)
    events = get_stream_events(job_id, since=since)
    return {"events": events, "cursor": since + len(events)}
