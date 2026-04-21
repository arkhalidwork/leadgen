"""
LeadGen Local Agent — Configuration

Edit this file or pass --api-key / --server flags on the command line.
"""
from __future__ import annotations
import os
import pathlib

AGENT_VERSION = "1.0.0"

# Server connection
SERVER_URL: str = os.environ.get("LEADGEN_SERVER_URL", "https://your-leadgen-server.com")
API_KEY: str = os.environ.get("LEADGEN_API_KEY", "")

# Polling
POLL_INTERVAL_SECONDS: float = float(os.environ.get("LEADGEN_POLL_INTERVAL", "5"))
HEARTBEAT_INTERVAL_SECONDS: float = float(os.environ.get("LEADGEN_HB_INTERVAL", "15"))
MAX_CONCURRENT_JOBS: int = int(os.environ.get("LEADGEN_MAX_CONCURRENT", "1"))

# Local storage
AGENT_DIR = pathlib.Path(os.environ.get("LEADGEN_AGENT_DIR", str(pathlib.Path.home() / ".leadgen-agent")))
DB_PATH = AGENT_DIR / "agent.db"

# Upload throttle
PROGRESS_THROTTLE_SECONDS: float = float(os.environ.get("LEADGEN_PROGRESS_THROTTLE", "5"))
CHECKPOINT_THROTTLE_SECONDS: float = float(os.environ.get("LEADGEN_CHECKPOINT_THROTTLE", "30"))

# Capabilities (what job types this agent can handle)
CAPABILITIES: list[str] = os.environ.get(
    "LEADGEN_CAPABILITIES", "gmaps,linkedin,instagram,webcrawler"
).split(",")
