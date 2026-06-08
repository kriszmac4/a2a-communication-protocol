#!/usr/bin/env python3
"""
Agent Message Bus Push Notifier — sends a real-time notification when a new
message lands on the bus.

Called from create_message() in agent_message_bus/__init__.py via subprocess.Popen
(background, non-blocking). Also called from agent_message_bus_watchdog.py for stale
message escalation.

Also supports --start-thread: opens a thread under the notification message
so users can see the agent working on it.

Default transport uses a configurable webhook/notification command.
Override via AMB_NOTIFY_CMD env var (e.g. "hermes send --target discord:#channel").

Usage (by code):
    python3 notify_target.py <to_agent> <from_agent> <msg_id> <priority> [preview]
    python3 notify_target.py --start-thread <msg_id>
"""

import json
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
from agent_message_bus import DATA_DIR as AMB_DATA_DIR, MESSAGES_DB

# ── Notification command ────────────────────────────────────────────────────
# Override via AMB_NOTIFY_CMD env var. Default: none (notifications disabled).
# Example: AMB_NOTIFY_CMD="hermes send --target discord:#dev"
_NOTIFY_CMD = os.environ.get("AMB_NOTIFY_CMD")


def _get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(MESSAGES_DB))
    conn.row_factory = sqlite3.Row
    return conn


def _send_notification(message: str, target: str = "") -> bool:
    """Send a notification using the configured command, or print to stderr."""
    cmd = _NOTIFY_CMD
    if not cmd:
        # No notification command configured — log to stderr
        print(f"[NOTIFY] {message}", file=sys.stderr)
        return True

    try:
        full_cmd = cmd.replace("{message}", message).replace("{target}", target)
        subprocess.run(
            full_cmd,
            shell=True,
            timeout=15,
            capture_output=True,
        )
        return True
    except Exception as e:
        print(f"[NOTIFY ERROR] {e}", file=sys.stderr)
        return False


def cmd_notify(to_agent: str, from_agent: str, msg_id: int, priority: int, preview: str = ""):
    """Send a notification about a new message."""
    preview = preview or f"New message from {from_agent} to {to_agent}"
    message = (
        f"📬 **Agent Message Bus**\n"
        f"From: `{from_agent}` → To: `{to_agent}`\n"
        f"ID: `{msg_id}` | Priority: `{priority}`\n"
        f"> {preview}"
    )
    _send_notification(message)


def cmd_start_thread(msg_id: int):
    """Start a thread on the notification message (stub — requires platform API)."""
    # Thread creation is platform-specific; subclasses or external scripts
    # can hook into AMB_NOTIFY_CMD to implement it.
    _send_notification(f"[THREAD START] msg_id={msg_id}")


def main():
    if len(sys.argv) < 2:
        print("Usage:", file=sys.stderr)
        print(f"  {sys.argv[0]} <to_agent> <from_agent> <msg_id> <priority> [preview]", file=sys.stderr)
        print(f"  {sys.argv[0]} --start-thread <msg_id>", file=sys.stderr)
        sys.exit(1)

    if sys.argv[1] == "--start-thread" and len(sys.argv) >= 3:
        cmd_start_thread(int(sys.argv[2]))
        return

    if len(sys.argv) >= 5:
        to_agent = sys.argv[1]
        from_agent = sys.argv[2]
        msg_id = int(sys.argv[3])
        priority = int(sys.argv[4])
        preview = " ".join(sys.argv[5:]) if len(sys.argv) > 5 else ""
        cmd_notify(to_agent, from_agent, msg_id, priority, preview)
    else:
        print(f"Usage: {sys.argv[0]} <to_agent> <from_agent> <msg_id> <priority> [preview]", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
