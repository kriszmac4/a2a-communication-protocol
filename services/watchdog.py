#!/usr/bin/env python3
"""
Marveen Bus Watchdog — LLM-free monitor for inter-agent messages (A2A upgrade)

Checks the shared Marveen message DB for pending messages that haven't been
picked up by the target agent. Outputs a warning if any message is older than
STALE_SECONDS (default: 60s).

A2A upgrades:
- Checks auto_responder trigger file for high-priority task alerts
- Detects active agent sessions and reports "agent is active but ignoring messages"
- Suggests intervention for stale high-priority messages

Designed to run as a cron job with no_agent=True.
"""
import json
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# --- Configuration ---
STALE_SECONDS = int(os.environ.get("MARVEEN_WATCHDOG_STALE", 60))
MARVEEN_DB = os.environ.get(
    "MARVEEN_DB_PATH",
    "/home/artofphotogrphyy/.hermes/data/marveen/agent_messages.db"
)
HERMES_HOME = Path(os.environ.get("HERMES_HOME", Path.home() / ".hermes"))
TRIGGER_FILE = Path("/tmp/marveen-auto-trigger")

# Agent display names
AGENT_NAMES = {
    "dev": "💻 Dev",
    "study": "📚 Study",
    "general": "🏛️ General",
    "research": "🔬 Research",
    "ui": "🎨 UI",
    "kanban": "📋 Kanban",
    "devops": "⚙️ DevOps (archived)",
    "fitness": "🏋️ Fitness (archived)",
    "news": "📰 News (archived)",
}

NOTIFY_SCRIPT = Path(__file__).parent / "marveen" / "notify_target.py"


def _get_active_sessions() -> list[str]:
    """Check state.db for currently active agent sessions."""
    state_db = HERMES_HOME / "state.db"
    if not state_db.exists():
        return []
    try:
        conn = sqlite3.connect(str(state_db))
        rows = conn.execute(
            "SELECT DISTINCT source FROM sessions WHERE ended_at IS NULL "
            "ORDER BY started_at DESC LIMIT 10"
        ).fetchall()
        conn.close()
        return [r[0] for r in rows if r[0]]
    except Exception:
        return []


def _push_notify_stale(agent: str, stale_messages: list) -> None:
    """Send push notification for stale messages. Non-blocking."""
    if not NOTIFY_SCRIPT.exists() or not stale_messages:
        return
    try:
        newest = max(stale_messages, key=lambda m: m["priority"])
        preview = (newest["content"] or "")[:120].replace("\n", " ")
        subprocess.Popen(
            [sys.executable, str(NOTIFY_SCRIPT),
             agent, newest["from_agent"], str(newest["id"]),
             str(newest["priority"]), preview],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def _read_triggers() -> list[dict]:
    """Read trigger file from auto_responder."""
    if not TRIGGER_FILE.exists():
        return []
    try:
        data = json.loads(TRIGGER_FILE.read_text())
        return data if isinstance(data, list) else []
    except (json.JSONDecodeError, OSError):
        return []


def _check_dead_messages() -> list[dict]:
    """DLQ check: find messages stuck in 'dead' status that need human attention."""
    if not os.path.exists(MARVEEN_DB):
        return []
    try:
        conn = sqlite3.connect(MARVEEN_DB)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, from_agent, to_agent, content, priority, "
            "message_type, retry_count, created_at, completed_at "
            "FROM agent_messages "
            "WHERE status = 'dead' "
            "ORDER BY created_at DESC LIMIT 20"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def _expire_stale_messages(now: float) -> int:
    """TTL expiry: mark pending messages past their expires_at as 'expired'.

    Returns the number of messages expired.
    """
    if not os.path.exists(MARVEEN_DB):
        return 0
    try:
        conn = sqlite3.connect(MARVEEN_DB)
        conn.execute(
            "UPDATE agent_messages SET status = 'expired', completed_at = ? "
            "WHERE status = 'pending' AND expires_at IS NOT NULL AND expires_at < ?",
            (now, now)
        )
        expired = conn.execute("SELECT changes() AS cnt").fetchone()
        conn.commit()
        conn.close()
        return expired[0] if expired else 0
    except Exception:
        return 0


def _retry_failed_messages(now: float) -> int:
    """Retry failed messages that haven't hit max_retries yet.

    Failed messages with retry_count < max_retries are moved back to 'pending'
    so they get another delivery attempt.
    Returns the number of messages retried.
    """
    if not os.path.exists(MARVEEN_DB):
        return 0
    try:
        conn = sqlite3.connect(MARVEEN_DB)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, from_agent, to_agent, retry_count, max_retries "
            "FROM agent_messages "
            "WHERE status = 'failed' AND retry_count < max_retries "
            "LIMIT 20"
        ).fetchall()
        if not rows:
            conn.close()
            return 0
        ids = [r["id"] for r in rows]
        placeholders = ",".join("?" * len(ids))
        conn.execute(
            f"UPDATE agent_messages SET status = 'pending', completed_at = NULL, "
            f"result = NULL WHERE id IN ({placeholders})",
            ids
        )
        conn.commit()
        conn.close()
        for r in rows:
            print(f"🔁 Retry #{r['retry_count']+1}/{r['max_retries']} — {r['from_agent']}→{r['to_agent']} (#{r['id']})")
        return len(rows)
    except Exception:
        return 0


def _check_stale_high_priority_pending(now: float) -> list[dict]:
    """Find pending messages that are high priority and stale."""
    if not os.path.exists(MARVEEN_DB):
        return []
    try:
        conn = sqlite3.connect(MARVEEN_DB)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT id, from_agent, to_agent, content, priority, created_at "
            "FROM agent_messages "
            "WHERE status = 'pending' AND priority >= 1 "
            "ORDER BY priority DESC, created_at ASC"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows if (now - r["created_at"]) > STALE_SECONDS]
    except Exception:
        return []


def check_messages() -> list:
    if not os.path.exists(MARVEEN_DB):
        return []
    conn = sqlite3.connect(MARVEEN_DB)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(
        "SELECT id, from_agent, to_agent, content, priority, created_at, "
        "discord_message_id, discord_thread_id "
        "FROM agent_messages "
        "WHERE status = 'pending' "
        "ORDER BY priority DESC, created_at ASC"
    )
    rows = [dict(r) for r in cursor.fetchall()]
    conn.close()
    return rows


def format_age(ts: float) -> str:
    age = time.time() - ts
    if age < 60:
        return f"{int(age)}mp"
    elif age < 3600:
        return f"{int(age / 60)}perc"
    else:
        return f"{int(age / 3600)}óra"


def main():
    now = time.time()
    messages = check_messages()
    active_sessions = _get_active_sessions()
    triggers = _read_triggers()
    high_priority_pending = _check_stale_high_priority_pending(now)

    # ── DLQ check ──
    dead_messages = _check_dead_messages()

    # ── TTL expiry ──
    expired_count = _expire_stale_messages(now)

    # ── Retry failed messages (DLQ prevention) ──
    retried_count = _retry_failed_messages(now)

    if not messages and not triggers and not high_priority_pending and not dead_messages and expired_count == 0 and retried_count == 0:
        return

    output_lines = []

    # ── High-priority / auto_responder triggers ──
    if triggers:
        recent = [t for t in triggers if (now - t["ts"]) < 300]  # last 5 min
        if recent:
            output_lines.append(f"🚨 **Auto-Responder trigger: {len(recent)} magas prioritású task**")
            for t in recent[:3]:
                age_s = now - t["ts"]
                output_lines.append(f"   ⚡ `#{t['msg_id']}` **{t['from']}→{t['to']}** "
                                    f"(P:{t['priority']}) {int(age_s)}mp")
            output_lines.append("")

    # ── High-priority stale messages ──
    if high_priority_pending:
        output_lines.append(f"🔴 **{len(high_priority_pending)} magas prioritású üzenet régóta olvasatlan!**")
        for msg in high_priority_pending[:3]:
            sender = AGENT_NAMES.get(msg["from_agent"], msg["from_agent"])
            target = AGENT_NAMES.get(msg["to_agent"], msg["to_agent"])
            preview = msg["content"][:120].replace("\n", " ")
            output_lines.append(f"   🚨 #{msg['id']} {sender} → **{target}** ({format_age(msg['created_at'])})")
            output_lines.append(f"      > {preview}")
        output_lines.append("")

    # ── TTL expiry report ──
    if expired_count > 0:
        output_lines.append(f"⏳ **{expired_count} üzenet lejárt (TTL) — automatikusan 'expired' státuszba téve**")
        output_lines.append("")

    # ── Dead Letter Queue report ──
    if dead_messages:
        output_lines.append(f"⚠️ **DLQ / Dead Letter Queue — {len(dead_messages)} üzenet kézbesíthetetlen!**")
        for msg in dead_messages[:5]:
            sender = AGENT_NAMES.get(msg["from_agent"], msg["from_agent"])
            target = AGENT_NAMES.get(msg["to_agent"], msg["to_agent"])
            msg_type = msg.get("message_type", "?")
            retries = msg.get("retry_count", "?")
            preview = (msg["content"] or "")[:100].replace("\n", " ")
            output_lines.append(
                f"   💀 #{msg['id']} {sender} → **{target}** "
                f"(type: {msg_type}, retries: {retries})"
            )
            output_lines.append(f"      > {preview}")
        if len(dead_messages) > 5:
            output_lines.append(f"   ... és még {len(dead_messages) - 5} további")
        output_lines.append("   🔔 **Manuális beavatkozás szükséges!**")
        output_lines.append("")
    if messages:
        by_target: dict[str, list] = {}
        for msg in messages:
            target = msg["to_agent"]
            by_target.setdefault(target, []).append(msg)

        for agent, msgs in sorted(by_target.items()):
            stale = [m for m in msgs if (now - m["created_at"]) > STALE_SECONDS]
            if not stale:
                continue

            agent_name = AGENT_NAMES.get(agent, f"❓ {agent}")
            is_active = agent in active_sessions

            active_mark = " **[SESSION ACTIVE]**" if is_active else ""
            output_lines.append(f"⏰ **{agent_name}{active_mark} — {len(stale)} olvasatlan üzenet**")
            output_lines.append(f"   Legrégebbi: {format_age(stale[0]['created_at'])}")

            for msg in stale[:3]:
                sender = AGENT_NAMES.get(msg["from_agent"], msg["from_agent"])
                preview = msg["content"][:120].replace("\n", " ")
                priority_mark = "🔴" if msg["priority"] >= 2 else "🟡" if msg["priority"] >= 1 else "  "
                output_lines.append(f"   {priority_mark} #{msg['id']} {sender} → {format_age(msg['created_at'])}")
                output_lines.append(f"      > {preview}")

            if is_active:
                output_lines.append(f"   ⚠️ **{agent} aktív session-ben van, de nem olvasta az üzenetet!**")
                output_lines.append(f"   → Utasítás: hívd `agent_read_messages()`-t ha {agent} vagy")
            else:
                output_lines.append("   → Hívd: `agent_read_messages()` a következő turnben")
            output_lines.append("")

            # Push notification for stale messages
            _push_notify_stale(agent, stale)

            # Retry thread open
            for msg in stale:
                if msg.get("discord_message_id") and not msg.get("discord_thread_id"):
                    try:
                        subprocess.Popen(
                            [sys.executable, str(NOTIFY_SCRIPT),
                             "--start-thread", str(msg["id"])],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                        )
                    except Exception:
                        pass

    total_stale = sum(1 for m in messages if (now - m["created_at"]) > STALE_SECONDS) if messages else 0
    has_triggers = len(triggers) > 0 or len(high_priority_pending) > 0
    has_dlq = len(dead_messages) > 0
    has_ttl = expired_count > 0

    if output_lines:
        status_icons = ""
        if has_dlq:
            status_icons += " 💀DLQ"
        if has_ttl:
            status_icons += " ⏳TTL"
        if has_triggers:
            status_icons += " 🚨TRIGGER"
        print(f"📬 **Marveen Bus Watchdog** — {len(messages)} függő üzenetből {total_stale} régi"
              f"{status_icons}"
              if messages else
              f"📬 **Marveen Bus Watchdog** — figyelés aktív{status_icons}")
        print(f"(Frissítve: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')})")
        print()
        print("\n".join(output_lines).strip())


if __name__ == "__main__":
    main()
