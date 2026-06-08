#!/usr/bin/env python3
"""
Agent Message Bus Auto-Responder — no_agent watchdog cron (A2A upgrade)

Pollos the Agent Message Bus bus for pending messages and:
1. Auto-responds with pre-defined templates (existing behavior)
2. Detects task messages ([skill=...]) and notifies General
3. Priority-aware: high/urgent messages trigger Discord push notification
4. Writes trigger flag for the message_router to pick up

Watchdog pattern:
- Empty stdout = silent (nothing to respond to)
- Output when responses are sent or triggers fired
"""

import json
import logging
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

# Resolve agent_message_bus module from the script directory
import os as _os
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from agent_message_bus import (
    create_message,
    get_pending_messages,
    mark_delivered,
    mark_read,
    mark_done,
    mark_failed,
    DATA_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("amb-auto-responder")

TRIGGER_FILE = Path("/tmp/amb-auto-trigger")


# ── Response Templates ──────────────────────────────────────────────────
RESPONSE_RULES = [
    # Any→general: orchestrator acknowledges
    {
        "from_filter": None,
        "to_filter": "general",
        "responder": "orchestrator",
        "response": "📥 **Auto-válasz**: Köszönöm az üzenetet! "
                     "Átadom a megfelelő agent-nek, amint elérhető.\n\n"
                     "---\n*Auto-responder*",
        "response_high": "🚨 **Sürgős üzenet érkezett!**\n"
                         "Azonnal feldolgozásra kerül, amint session aktív.\n\n"
                         "---\n*Auto-responder*",
        "mark_as": "read",
    },
    # Any→dev: dev acknowledges
    {
        "from_filter": None,
        "to_filter": "dev",
        "responder": "dev",
        "response": "📥 **Auto-ack**: Dev üzenetet kapott. Feldolgozás a következő aktív session-ben.",
        "response_high": "🚨 **Dev: Sürgős feladat érkezett!**\n"
                         "Amint aktív vagyok, prioritással feldolgozom.\n\n"
                         "---\n*Auto-responder*",
        "mark_as": "read",
        "notify_orchestrator": True,  # Also tell General "task received"
    },
    # Any→research: research acknowledges
    {
        "from_filter": None,
        "to_filter": "research",
        "responder": "research",
        "response": "📥 **Auto-ack**: Research kérés rögzítve. Feldolgozás a következő session-ben.",
        "response_high": "🚨 **Research: Sürgős kutatás érkezett!**\n"
                         "Amint aktív vagyok, prioritással feldolgozom.",
        "mark_as": "read",
        "notify_orchestrator": True,
    },
    # Any→study: study acknowledges
    {
        "from_filter": None,
        "to_filter": "study",
        "responder": "orchestrator",
        "response": "📥 **Auto-válasz**: Tanulási feladatot fogadtam! "
                     "Amint aktív session-ben vagyok, feldolgozom. "
                     "Addig is tartsd a tanulási terved! 🎯\n\n"
                     "---\n*Auto-responder*",
        "response_high": "🚨 **Study: Sürgős tanulási feladat!**\n"
                         "Amint aktív vagyok, prioritással feldolgozom.",
        "mark_as": "read",
        "notify_orchestrator": True,
    },
]


def get_responder_rule(msg: dict) -> dict | None:
    """Find the first matching rule for a message."""
    for rule in RESPONSE_RULES:
        if rule["to_filter"] and msg["to_agent"] != rule["to_filter"]:
            continue
        if rule.get("from_filter") and msg["from_agent"] != rule["from_filter"]:
            continue
        return rule
    return None


def _has_task_tag(content: str) -> bool:
    """Detect if message contains a [skill=...] task tag."""
    return "[skill=" in (content or "")


def format_response(template: str, msg: dict) -> str:
    """Fill template variables from message."""
    content = msg.get("content", "")
    preview = content[:200] + ("..." if len(content) > 200 else "")
    return template.format(
        from_=msg["from_agent"],
        to=msg["to_agent"],
        content=content,
        preview=preview,
    )


def _notify_discord(msg: dict, reason: str) -> None:
    """Send a Discord push notification for important messages."""
    notify_script = Path(__file__).parent / "notify_target.py"
    if not notify_script.exists():
        return
    preview = (msg.get("content", "") or "")[:120].replace("\n", " ")
    try:
        subprocess.Popen(
            [sys.executable, str(notify_script),
             msg["to_agent"], msg["from_agent"],
             str(msg["id"]), str(msg.get("priority", 0)),
             f"[{reason}] {preview}"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass


def _write_trigger(msg: dict, rule: dict) -> None:
    """Write trigger file so message_router / watchdog knows about important work."""
    try:
        entry = {
            "ts": time.time(),
            "msg_id": msg["id"],
            "from": msg["from_agent"],
            "to": msg["to_agent"],
            "priority": msg.get("priority", 0),
            "has_skill": _has_task_tag(msg.get("content", "")),
            "responder": rule.get("responder", "?"),
        }
        existing = []
        if TRIGGER_FILE.exists():
            try:
                existing = json.loads(TRIGGER_FILE.read_text())
            except (json.JSONDecodeError, OSError):
                existing = []
        existing.append(entry)
        # Keep max 20 entries
        TRIGGER_FILE.write_text(json.dumps(existing[-20:], indent=2))
    except Exception:
        pass


def main() -> int:
    """Main loop — poll and respond."""
    pending = get_pending_messages(limit=50)

    # Also get recently delivered messages (within last 2 min)
    recent_cutoff = time.time() - 120
    conn = sqlite3.connect(str(DATA_DIR / "agent_messages.db"))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM agent_messages WHERE status = 'delivered' AND created_at > ? "
        "ORDER BY priority DESC, created_at ASC LIMIT 50",
        (recent_cutoff,)
    ).fetchall()
    conn.close()
    delivered = [dict(r) for r in rows]

    # Merge: pending first, then delivered (deduped)
    seen_ids = set(m["id"] for m in pending)
    for m in delivered:
        if m["id"] not in seen_ids:
            pending.append(m)
            seen_ids.add(m["id"])

    responses_sent = 0
    skipped = 0
    orchestrator_notifications = []

    for msg in pending:
        msg_id = msg["id"]

        # Skip auto-response loops
        if msg["from_agent"] in ("orchestrator", "dev", "research", "kanban"):
            content = msg.get("content", "")
            if content.startswith("📥 **Auto-válasz") or content.startswith("📥 **Auto-ack"):
                logger.info(f"#{msg_id} skipping auto-response loop from {msg['from_agent']}")
                mark_delivered(msg_id)
                skipped += 1
                continue

        # Find matching rule
        rule = get_responder_rule(msg)
        if not rule:
            logger.debug(f"#{msg_id} no matching rule for {msg['from_agent']}→{msg['to_agent']}")
            continue

        # Skip message-router messages
        if msg["from_agent"] == "message-router":
            mark_delivered(msg_id)
            skipped += 1
            continue

        # Determine priority and pick response template
        is_high = msg.get("priority", 0) >= 1 or _has_task_tag(msg.get("content", ""))
        response_template = rule.get("response_high", rule["response"]) if is_high else rule["response"]
        response_text = format_response(response_template, msg)

        try:
            create_message(
                from_agent=rule["responder"],
                to_agent=msg["from_agent"],
                content=response_text,
                priority=msg.get("priority", 0),
            )
            logger.info(f"#{msg_id} auto-responded: {msg['from_agent']}→{msg['to_agent']} (high={is_high})")

            # For high-priority / task messages — push notification to Discord
            if is_high:
                _notify_discord(msg, "HIGH_PRIORITY_TASK" if _has_task_tag(msg.get("content", "")) else "HIGH_PRIORITY")

            # Write trigger file
            _write_trigger(msg, rule)

            # Notify orchestrator about task received
            if rule.get("notify_orchestrator") and msg["from_agent"] != "general":
                task_preview = (msg.get("content", "") or "")[:150].replace("\n", " ")
                create_message(
                    from_agent="auto_responder",
                    to_agent="general",
                    content=(
                        f"📨 **Auto-visszajelzés**: `{msg['from_agent']}` üzenetet küldött "
                        f"**{msg['to_agent']}**-nak.\n"
                        f"`#{msg_id}` | {task_preview}\n\n"
                        f"A task elérhető, amint {msg['to_agent']} aktív session-ben van."
                    ),
                    priority=0,
                )
                orchestrator_notifications.append(msg_id)

            # Mark original
            mark_as = rule.get("mark_as", "read")
            if mark_as == "read":
                mark_read(msg_id)
            elif mark_as == "done":
                mark_done(msg_id, "Auto-responded")
            else:
                mark_delivered(msg_id)

            responses_sent += 1
            msg["_responded"] = True
        except Exception as e:
            logger.error(f"#{msg_id} failed to respond: {e}")
            mark_failed(msg_id, str(e))

    # Watchdog output — only when something happened
    if responses_sent:
        lines = [
            f"📬 **Auto-Responder — {responses_sent} válasz elküldve**",
        ]
        for msg in pending:
            if msg.get("_responded"):
                rule = get_responder_rule(msg)
                if rule:
                    is_high = msg.get("priority", 0) >= 1 or _has_task_tag(msg.get("content", ""))
                    tpl = rule.get("response_high", rule["response"]) if is_high else rule["response"]
                    rt = format_response(tpl, msg)
                    flag = "🚨" if is_high else "📎"
                    lines.append(f"> {flag} **{msg['from_agent']}→{msg['to_agent']}** #{msg['id']}")
                    lines.append(f"> {rt[:200]}...\n" if len(rt) > 200 else f"> {rt}\n")
        if orchestrator_notifications:
            lines.append(f"📨 General értesítve: {len(orchestrator_notifications)} task visszajelzés")
        print("\n".join(lines))

    return 0


if __name__ == "__main__":
    sys.exit(main())
