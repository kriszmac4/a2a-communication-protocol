#!/usr/bin/env python3
"""
Marveen Push Notifier — sends a real-time notification to the target agent's
Discord channel when a new message lands on the Marveen bus.

Called from create_message() in marveen/__init__.py via subprocess.Popen
(background, non-blocking). Also called from marveen_watchdog.py for stale
message escalation.

Also supports --start-thread: opens a Discord thread under the notification
message so the user can see the agent is working on it.

Uses `hermes send` CLI + direct Discord API for thread creation.

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

# ── User home (not profile-overridden HOME) ────────────────────────────────
import pwd as _pwd
_USER_HOME = Path(_pwd.getpwuid(os.getuid()).pw_dir)
_HERMES_ROOT = _USER_HOME / ".hermes"

# ── Default Discord channel mapping (fallback if profile config has no home_channel) ──
_DEFAULT_AGENT_CHANNELS = {
    "dev":      "discord:1501148006219776002",   # #dev
    "general":  "discord:1501148038117330995",   # #general
    "research": "discord:1501147842595655721",   # #research
    "study":    "discord:1501188232807972905",   # #study-general
    "ui":       "discord:1504006733826232521",   # #ui
    "devops":   "discord:1501148016713793588",   # #devops
}


def get_agent_home_channel(agent: str) -> str | None:
    """Get the home Discord channel for an agent from its profile config.
    Falls back to hardcoded defaults if not set in config."""
    try:
        cfg_path = _HERMES_ROOT / "profiles" / agent / "config.yaml"
        if cfg_path.exists():
            import yaml
            data = yaml.safe_load(cfg_path.read_text()) or {}
            discord_cfg = data.get("discord", {})
            if isinstance(discord_cfg, dict) and discord_cfg.get("home_channel"):
                return str(discord_cfg["home_channel"])
    except Exception:
        pass
    return _DEFAULT_AGENT_CHANNELS.get(agent)


def get_agent_channel_id(agent: str) -> str | None:
    """Get the pure Discord channel ID for an agent."""
    ch = get_agent_home_channel(agent)
    if ch and ":" in ch:
        return ch.split(":")[1]
    _dc = _DEFAULT_AGENT_CHANNELS.get(agent)
    if _dc and ":" in _dc:
        return _dc.split(":")[1]
    return None

AGENT_EMOJIS = {
    "dev": "💻", "general": "🏛️", "research": "🔬",
    "study": "📚", "ui": "🎨", "devops": "⚙️",
    "kanban": "📋", "news": "📰",
}

ORCHESTRATOR_CHANNEL = "discord:1501148006219776002"  # #dev

# Marveen DB
HERMES_HOME = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))
MARVEEN_DB = HERMES_HOME / "data" / "marveen" / "agent_messages.db"
PROFILES_DIR = _USER_HOME / ".hermes" / "profiles"


# =============================================================================
# DB helpers
# =============================================================================

def _get_db() -> sqlite3.Connection:
    """Open a direct SQLite connection to the Marveen DB (no marveen import)."""
    MARVEEN_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(MARVEEN_DB))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    # Safe migration: check columns exist before adding (PRAGMA table_info is supported on all SQLite versions)
    cursor = conn.execute("PRAGMA table_info(agent_messages)")
    existing_cols = [row[1] for row in cursor.fetchall()]
    if "discord_message_id" not in existing_cols:
        conn.execute("ALTER TABLE agent_messages ADD COLUMN discord_message_id TEXT")
    if "discord_thread_id" not in existing_cols:
        conn.execute("ALTER TABLE agent_messages ADD COLUMN discord_thread_id TEXT")
    conn.commit()
    return conn


def _save_discord_message_id(msg_id: int, discord_message_id: str):
    """Store the Discord message ID for a Marveen bus message."""
    try:
        conn = _get_db()
        conn.execute(
            "UPDATE agent_messages SET discord_message_id = ? WHERE id = ?",
            (discord_message_id, msg_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[notify_target] DB save error: {e}", file=sys.stderr)


def _get_discord_ids(msg_id: int) -> tuple[str | None, str | None]:
    """Get (discord_message_id, discord_thread_id) for a bus message."""
    try:
        conn = _get_db()
        row = conn.execute(
            "SELECT discord_message_id, discord_thread_id FROM agent_messages WHERE id = ?",
            (msg_id,)
        ).fetchone()
        conn.close()
        if row:
            return (row["discord_message_id"], row["discord_thread_id"])
        return (None, None)
    except Exception:
        return (None, None)


def _save_discord_thread_id(msg_id: int, thread_id: str):
    """Store the Discord thread ID after opening one."""
    try:
        conn = _get_db()
        conn.execute(
            "UPDATE agent_messages SET discord_thread_id = ? WHERE id = ?",
            (thread_id, msg_id)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[notify_target] Thread ID save error: {e}", file=sys.stderr)


# =============================================================================
# Discord notification
# =============================================================================

def get_hermes_bin() -> str:
    candidates = [
        "/usr/local/bin/hermes",
        "/usr/bin/hermes",
        str(Path.home() / ".local" / "bin" / "hermes"),
    ]
    for c in candidates:
        if os.path.exists(c):
            return c
    return "hermes"


def send_notification_json(target: str, message: str,
                           profile_home: str | None = None) -> dict | None:
    """Send a message via hermes send CLI and return the JSON result."""
    hermes = get_hermes_bin()
    env = os.environ.copy()
    if profile_home:
        env["HERMES_HOME"] = profile_home
    try:
        result = subprocess.run(
            [hermes, "send", "--json", "--quiet", "--to", target, message],
            capture_output=True, text=True, timeout=10, env=env,
        )
        if result.returncode == 0 and result.stdout.strip():
            return json.loads(result.stdout)
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, json.JSONDecodeError) as e:
        print(f"[notify_target] Error sending to {target}: {e}", file=sys.stderr)
        return None


def get_agent_profile_home(agent: str) -> str | None:
    """Get the HERMES_HOME path for an agent's profile, if it exists."""
    profile_dir = PROFILES_DIR / agent
    if profile_dir.exists():
        return str(profile_dir)
    return None


def send_notification(to_agent: str, from_agent: str, msg_id: int,
                      priority: int, preview: str = ""):
    """Send Discord notification and store the message ID."""
    target_channel = get_agent_home_channel(to_agent)
    if not target_channel:
        return

    from_emoji = AGENT_EMOJIS.get(from_agent, "📨")
    to_emoji = AGENT_EMOJIS.get(to_agent, "📨")
    priority_mark = "🔴" if priority >= 2 else "🟡" if priority >= 1 else ""

    msg = f"{priority_mark}📬 **Marveen Bus** — {from_emoji} **{from_agent}** → {to_emoji} **{to_agent}**"

    if preview:
        short_preview = preview[:100].replace("\n", " ").strip()
        if len(preview) > 100:
            short_preview += "…"
        msg += f"\n> {short_preview}"

    msg += f"\n(id: #{msg_id} — dolgozom rajta… 🧵)"

    # Use the target agent's profile for Discord credentials
    profile_home = get_agent_profile_home(to_agent)
    result = send_notification_json(target_channel, msg, profile_home)

    if result and result.get("success") and result.get("message_id"):
        discord_msg_id = result["message_id"]
        _save_discord_message_id(msg_id, discord_msg_id)
        print(f"[notify_target] ✅ #{msg_id} notified → {to_agent} (msg_id={discord_msg_id})",
              file=sys.stderr)
    else:
        # Fallback: send without JSON
        hermes = get_hermes_bin()
        profile_home = get_agent_profile_home(to_agent)
        env = os.environ.copy()
        if profile_home:
            env["HERMES_HOME"] = profile_home
        try:
            subprocess.run(
                [hermes, "send", "--quiet", "--to", target_channel, msg],
                capture_output=True, timeout=10, env=env,
            )
            print(f"[notify_target] ✅ #{msg_id} notified → {to_agent} (fallback)",
                  file=sys.stderr)
        except Exception as e:
            print(f"[notify_target] ❌ #{msg_id} FAILED → {to_agent}: {e}",
                  file=sys.stderr)


# =============================================================================
# CLI: start-thread mode (delegates to marveen.open_message_thread)
# =============================================================================

def start_thread(msg_id: int):
    """Open a thread for an existing notification via marveen.open_message_thread()."""
    try:
        sys.path.insert(0, str(Path.home() / ".hermes" / "scripts"))
        from marveen import open_message_thread as _open_thread
        success = _open_thread(msg_id)
        if success:
            print(f"[notify_target] ✅ #{msg_id} thread opened via marveen", file=sys.stderr)
        else:
            print(f"[notify_target] ⚠️ #{msg_id} thread open failed", file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"[notify_target] ❌ #{msg_id} start_thread error: {e}", file=sys.stderr)
        sys.exit(1)


# =============================================================================
# Main
# =============================================================================

def main():
    if len(sys.argv) < 2:
        print("Usage:", file=sys.stderr)
        print("  notify_target.py <to_agent> <from_agent> <msg_id> <priority> [preview]", file=sys.stderr)
        print("  notify_target.py --start-thread <msg_id>", file=sys.stderr)
        sys.exit(1)

    if sys.argv[1] == "--start-thread":
        if len(sys.argv) < 3:
            print("Usage: notify_target.py --start-thread <msg_id>", file=sys.stderr)
            sys.exit(1)
        start_thread(int(sys.argv[2]))
        return

    if len(sys.argv) < 5:
        print("Usage: notify_target.py <to_agent> <from_agent> <msg_id> <priority> [preview]", file=sys.stderr)
        sys.exit(1)

    to_agent = sys.argv[1]
    from_agent = sys.argv[2]
    msg_id = int(sys.argv[3])
    priority = int(sys.argv[4])
    preview = sys.argv[5] if len(sys.argv) > 5 else ""

    send_notification(to_agent, from_agent, msg_id, priority, preview)

    # ── Auto-open thread under the notification ──
    # Ensures every bus message gets a Discord thread in the target agent's channel.
    # Non-blocking: if notification failed (no discord_message_id), start_thread
    # gracefully returns False.
    try:
        start_thread(msg_id)
    except SystemExit:
        pass  # Non-blocking - failure is OK for auto-thread


if __name__ == "__main__":
    main()
