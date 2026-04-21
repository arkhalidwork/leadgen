"""
LeadGen Agent — Main Entry Point

Usage:
    python main.py --server https://yourserver.com --api-key YOUR_KEY
    LEADGEN_SERVER_URL=https://... LEADGEN_API_KEY=... python main.py
"""
from __future__ import annotations

import argparse
import hashlib
import logging
import os
import pathlib
import platform
import shutil
import signal
import socket
import sys
import tempfile
import threading
import time

# Bootstrap: parse CLI args before importing modules
parser = argparse.ArgumentParser(description="LeadGen Local Agent")
parser.add_argument("--server", default="", help="Server URL (overrides env)")
parser.add_argument("--api-key", default="", help="API key (overrides env)")
parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
parser.add_argument("--max-concurrent", type=int, default=0)
args, _ = parser.parse_known_args()

# Apply CLI overrides before importing config
if args.server:
    os.environ["LEADGEN_SERVER_URL"] = args.server
if args.api_key:
    os.environ["LEADGEN_API_KEY"] = args.api_key
if args.max_concurrent > 0:
    os.environ["LEADGEN_MAX_CONCURRENT"] = str(args.max_concurrent)

import config
import api_client
import auth
import storage
import poller
import executor

logging.basicConfig(
    level=getattr(logging, args.log_level),
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
log = logging.getLogger("leadgen.agent")

_shutdown = False


def _signal_handler(signum, frame):
    global _shutdown
    log.info(f"Signal {signum} received — shutting down gracefully...")
    _shutdown = True
    sys.exit(0)


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ── Heartbeat Thread ────────────────────────────────────────────────────────

def _heartbeat_loop() -> None:
    log.info(f"Heartbeat started (interval={config.HEARTBEAT_INTERVAL_SECONDS}s)")
    while not _shutdown:
        time.sleep(config.HEARTBEAT_INTERVAL_SECONDS)
        try:
            active = executor.get_active_job_ids()
            resp = api_client.heartbeat(active_jobs=active, cpu_pct=0.0, ram_gb=0.0)
            if resp is None:
                continue

            # Server told us to stop specific jobs
            for job_id in resp.get("stop_jobs", []):
                log.info(f"Server requested stop for job {job_id}")
                executor.request_stop(job_id)

            # Server requires update
            if resp.get("requires_update"):
                check_for_update(resp)

        except Exception as exc:
            log.error(f"Heartbeat error: {exc}")


# ── Auto-Update ────────────────────────────────────────────────────────────

def check_for_update(resp: dict) -> None:
    if not resp.get("requires_update"):
        return

    update_url = resp.get("update_url", "")
    if not update_url:
        log.warning("Update required but no update_url provided")
        return

    expected_sha = resp.get("update_sha256", "")
    log.info(f"Update required (v{resp.get('latest_version', '?')}). Downloading...")

    try:
        import requests
        r = requests.get(update_url, stream=True, timeout=120)
        r.raise_for_status()

        tmp = pathlib.Path(tempfile.mktemp(suffix=".zip"))
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)

        if expected_sha:
            actual_sha = hashlib.sha256(tmp.read_bytes()).hexdigest()
            if actual_sha != expected_sha:
                log.error("Update SHA256 mismatch — aborting update")
                return

        agent_dir = pathlib.Path(__file__).parent
        shutil.unpack_archive(tmp, agent_dir)
        log.info("Update installed — restarting...")
        os.execv(sys.executable, [sys.executable, str(agent_dir / "main.py")])

    except Exception as exc:
        log.error(f"Auto-update failed: {exc}")


# ── Startup ────────────────────────────────────────────────────────────────

def main() -> None:
    if not config.API_KEY:
        print("ERROR: API key required. Use --api-key or set LEADGEN_API_KEY")
        sys.exit(1)

    if not config.SERVER_URL or config.SERVER_URL == "https://your-leadgen-server.com":
        print("ERROR: Server URL required. Use --server or set LEADGEN_SERVER_URL")
        sys.exit(1)

    # Init local storage
    storage.ensure_schema()

    # Derive agent identity
    agent_id = auth.derive_agent_id(config.API_KEY)
    api_client.AGENT_ID = agent_id

    log.info(f"LeadGen Agent v{config.AGENT_VERSION} starting")
    log.info(f"Agent ID: {agent_id}")
    log.info(f"Server: {config.SERVER_URL}")
    log.info(f"Capabilities: {config.CAPABILITIES}")

    # Register with server
    hostname = socket.gethostname()
    plat = f"{platform.system()}-{platform.machine()}"
    resp = api_client.register(agent_id, hostname, plat)

    if resp is None:
        log.error("Failed to register with server. Check API key and server URL.")
        sys.exit(1)

    if resp.get("requires_update"):
        log.warning(f"Server requires update to v{resp.get('min_required_version')}+")
        check_for_update(resp)
        # If update failed, still try to run (server will refuse new jobs)

    log.info(f"Registered: poll_interval={resp.get('poll_interval_seconds', 5)}s")

    # Start heartbeat thread
    hb_thread = threading.Thread(target=_heartbeat_loop, daemon=True, name="heartbeat")
    hb_thread.start()

    # Start poll loop (blocking)
    poller.run_poll_loop()


if __name__ == "__main__":
    main()
