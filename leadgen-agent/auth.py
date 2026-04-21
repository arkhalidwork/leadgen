"""
LeadGen Agent — Auth Manager

Derives a deterministic agent_id from the API key + machine fingerprint,
and signs every outbound request with HMAC-SHA256.
"""
from __future__ import annotations

import hashlib
import hmac
import platform
import socket
import time

import config


def derive_agent_id(api_key: str) -> str:
    """Deterministic agent_id: stable across restarts on the same machine."""
    machine = f"{socket.gethostname()}-{platform.machine()}-{platform.system()}"
    prefix = hmac.new(
        api_key.encode(), machine.encode(), hashlib.sha256
    ).hexdigest()[:12]
    return f"agt-{prefix}"


def build_auth_headers(agent_id: str, api_key: str, body_bytes: bytes) -> dict:
    """
    Build HMAC-signed headers for every agent request.
    Signature = HMAC-SHA256(api_key, "{agent_id}:{unix_ts}:{sha256(body)}")
    """
    ts = str(int(time.time()))
    body_sha = hashlib.sha256(body_bytes).hexdigest()
    msg = f"{agent_id}:{ts}:{body_sha}"
    sig = hmac.new(api_key.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {
        "X-Agent-ID": agent_id,
        "X-Agent-Version": config.AGENT_VERSION,
        "X-Timestamp": ts,
        "X-Signature": sig,
        "Content-Type": "application/json",
    }


def build_register_headers(api_key: str) -> dict:
    """Registration uses simple API key auth (agent_id is being declared)."""
    return {
        "X-Api-Key": api_key,
        "Content-Type": "application/json",
    }
