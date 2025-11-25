#!/usr/bin/env python3
"""
session_logger.py

Lightweight JSON-line logger for reddit→x bot.
Safe for async + sync hybrid systems.
Fully crash-proof: all exceptions are swallowed internally.

Log format (one JSON per line):
{
  "slot": "slot_developersIndia",
  "status": "success",
  "event": "tweet_posted",
  "details": {...},
  "timestamp": "2025-01-01T12:30:00Z"
}

This file is safe to import anywhere.
"""

from __future__ import annotations

import json
import os
import threading
from datetime import datetime
from typing import Any, Dict

# Default local log file
LOG_FILE = os.getenv("SESSION_LOG_FILE", "session_log.jsonl")

# Thread-safe lock
_lock = threading.Lock()


def _write_line_safe(line: str) -> None:
    """
    Safely append a line to the log file.
    Never raises exceptions.
    """
    try:
        with _lock:
            with open(LOG_FILE, "a", encoding="utf-8") as f:
                f.write(line + "\n")
    except Exception:
        # NEVER crash — silent fail
        pass


def log_session(slot: str, status: str, event: str | None, details: Dict[str, Any] | None):
    """
    Add a structured log entry.

    slot: unique slot name (e.g., "slot_technology")
    status: "success" | "fail" | "error"
    event: optional short code like "tweet_posted" or "media_upload_failed"
    details: dict with context
    """
    try:
        entry = {
            "slot": slot,
            "status": status,
            "event": event,
            "details": details or {},
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }
        _write_line_safe(json.dumps(entry, ensure_ascii=False))
    except Exception:
        # NEVER break bot
        pass


# If executed directly, test the logger
if __name__ == "__main__":
    log_session("test_slot", "success", "logger_test", {"msg": "Logger OK"})
    print(f"✓ session_logger test written to {LOG_FILE}")
