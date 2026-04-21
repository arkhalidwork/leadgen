"""
LeadGen — Configuration & Feature Flags

Controls per-tool queue migration and worker settings.
Set environment variables to enable Redis-based queue per tool.
"""
from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Feature Flags — Queue Migration
# Set to "1" to route a tool through the Redis queue + worker system.
# Set to "0" (default) to keep using the legacy thread-based system.
# ---------------------------------------------------------------------------
QUEUE_ENABLED: dict[str, bool] = {
    "linkedin": os.environ.get("LEADGEN_QUEUE_LINKEDIN", "0") == "1",
    "instagram": os.environ.get("LEADGEN_QUEUE_INSTAGRAM", "0") == "1",
    "webcrawler": os.environ.get("LEADGEN_QUEUE_WEBCRAWLER", "0") == "1",
    "gmaps": os.environ.get("LEADGEN_QUEUE_GMAPS", "0") == "1",
}

# ---------------------------------------------------------------------------
# Redis Configuration
# ---------------------------------------------------------------------------
REDIS_HOST: str = os.environ.get("REDIS_HOST", "127.0.0.1")
REDIS_PORT: int = int(os.environ.get("REDIS_PORT", "6379"))
REDIS_DB: int = int(os.environ.get("REDIS_DB", "0"))
REDIS_PASSWORD: str = os.environ.get("REDIS_PASSWORD", "")

# ---------------------------------------------------------------------------
# Queue Names
# ---------------------------------------------------------------------------
QUEUE_DEFAULT = "leadgen:queue:default"
QUEUE_HEAVY = "leadgen:queue:heavy"
QUEUE_PROCESSING = "leadgen:queue:processing"

# Tool → queue mapping
TOOL_QUEUE_MAP: dict[str, str] = {
    "gmaps": QUEUE_HEAVY,
    "linkedin": QUEUE_DEFAULT,
    "instagram": QUEUE_DEFAULT,
    "webcrawler": QUEUE_DEFAULT,
}

# ---------------------------------------------------------------------------
# Per-Tool Configuration
# ---------------------------------------------------------------------------
TOOL_CONFIG: dict[str, dict] = {
    "gmaps": {
        "max_attempts": 2,
        "timeout_seconds": 1800,     # 30 minutes
        "backoff_base": 60,
    },
    "linkedin": {
        "max_attempts": 3,
        "timeout_seconds": 300,      # 5 minutes
        "backoff_base": 15,
    },
    "instagram": {
        "max_attempts": 3,
        "timeout_seconds": 300,
        "backoff_base": 15,
    },
    "webcrawler": {
        "max_attempts": 3,
        "timeout_seconds": 600,      # 10 minutes
        "backoff_base": 30,
    },
}

# ---------------------------------------------------------------------------
# Worker Settings
# ---------------------------------------------------------------------------
WORKER_HEARTBEAT_INTERVAL: int = int(os.environ.get("LEADGEN_WORKER_HB_INTERVAL", "10"))
WORKER_HEARTBEAT_TTL: int = int(os.environ.get("LEADGEN_WORKER_HB_TTL", "30"))
WORKER_PROGRESS_THROTTLE: float = float(os.environ.get("LEADGEN_WORKER_PROGRESS_THROTTLE", "2.0"))

# Max active jobs per user (across all tools)
MAX_ACTIVE_JOBS_PER_USER: int = int(os.environ.get("LEADGEN_MAX_ACTIVE_PER_USER", "2"))

# Stale job sweeper interval (seconds)
SWEEPER_INTERVAL: int = int(os.environ.get("LEADGEN_SWEEPER_INTERVAL", "60"))
SWEEPER_STALE_THRESHOLD: int = int(os.environ.get("LEADGEN_SWEEPER_STALE_THRESHOLD", "60"))
