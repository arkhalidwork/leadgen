"""
Database helpers for LeadGen.

Provides get_db() as a thin proxy to the app-level database connection.
Blueprint modules import get_db() from here to avoid circular imports.
The full schema and init_db() live in app.py (authoritative).
"""
from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def get_db():
    """Return a per-request sqlite3 connection (delegates to app.py)."""
    from flask import g, current_app
    import sqlite3

    if "db" not in g:
        db_path = current_app.config.get(
            "LEADGEN_DB_PATH",
            __import__("os").environ.get(
                "LEADGEN_DB_PATH",
                __import__("os").path.join(
                    __import__("os").path.dirname(__import__("os").path.dirname(__file__)),
                    "leadgen.db",
                ),
            ),
        )
        g.db = sqlite3.connect(db_path)
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
    return g.db
