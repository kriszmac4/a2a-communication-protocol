#!/usr/bin/env python3
"""
Agent Message Bus MCP Server — Hermes Agent Integration

Exposes tools for:
- Agent Message Bus (inter-agent communication)
- Gradual Autonomy (trust levels)
- Dream Engine (nightly consolidation)

Usage:
    chmod +x ~/.hermes/scripts/agent_message_bus_mcp_server.py
    ~/.hermes/scripts/agent_message_bus_mcp_server.py

Register in ~/.hermes/config.yaml:
    mcp_servers:
      agent_message_bus:
        command: "~/.hermes/scripts/agent_message_bus_mcp_server.py"
        timeout: 30
        connect_timeout: 5
"""

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

# Force global HERMES_HOME so the MCP server always uses the shared DB
# (session HOME override would otherwise point to profile-local DB)
os.environ.setdefault("HERMES_HOME", str(Path.home() / ".hermes"))
from pathlib import Path

# Ensure agent_message_bus module is importable
sys.path.insert(0, str(Path.home() / ".hermes" / "scripts"))
from agent_message_bus import (
    create_message,
    get_messages,
    mark_read,
    mark_done,
    get_all_autonomy_categories,
    set_autonomy_level,
    get_autonomy_level,
    classify_command,
    discover_agents,
    list_agent_cards,
    record_skill_invocation,
    open_message_thread,
    DREAMS_DIR,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("amb-mcp")


def _json_result(data) -> dict:
    return {"content": [{"type": "text", "text": json.dumps(data, ensure_ascii=False)}]}


def _text_result(text: str) -> dict:
    return {"content": [{"type": "text", "text": text}]}


# =============================================================================
# MCP Server — stdio transport
# =============================================================================

import asyncio
from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool

server = Server("agent_message_bus")


def _tool(name: str, description: str, inputSchema: dict) -> Tool:
    return Tool(name=name, description=description, inputSchema=inputSchema)


def _detect_agent() -> str:
    """Auto-detect current agent from environment."""
    agent = os.environ.get("HERMES_PROFILE") or os.environ.get("HERMES_HOME", "").rsplit("/", 1)[-1] or "general"
    if agent == ".hermes":
        agent = "general"
    return agent


def _get_trigger_path(target_agent: str = None) -> Path:
    """Get the wakeup_pending.json trigger file path for a target agent."""
    if target_agent is None:
        target_agent = _detect_agent()
    return (
        Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
        / "profiles" / target_agent / "data" / "agent_message_bus" / "wakeup_pending.json"
    )


# ---------------------------------------------------------------------------
# Permission matrix for inter-agent delegation
# ---------------------------------------------------------------------------
DELEGATION_PERMISSIONS = {
    "dev": ["general", "research", "study", "kanban", "ui"],
    "general": ["dev", "research", "study", "kanban"],
    "research": ["general", "study"],
    "study": ["general", "research"],
    "kanban": ["general", "dev"],
    "ui": ["dev", "general"],
}

MAX_CHAIN_DEPTH = 5


def _check_permission(from_agent: str, to_agent: str) -> bool:
    """Check if from_agent is allowed to delegate to to_agent."""
    allowed = DELEGATION_PERMISSIONS.get(from_agent, [])
    return to_agent in allowed


def _get_chain_depth(message_id: int) -> int:
    """Count how deep the conversation chain is by following parent_message_id."""
    depth = 0
    current_id = message_id
    seen = set()
    db_path = (
        Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
        / "data" / "agent_message_bus" / "agent_messages.db"
    )
    try:
        import sqlite3
        conn = sqlite3.connect(str(db_path))
        while current_id and current_id not in seen:
            seen.add(current_id)
            row = conn.execute(
                "SELECT parent_message_id FROM agent_messages WHERE id = ?",
                (current_id,)
            ).fetchone()
            if row and row[0]:
                current_id = row[0]
                depth += 1
            else:
                break
        conn.close()
    except Exception:
        pass
    return depth


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        # === COMPOSITE A2A TOOLS (replacing low-level counterparts) ===
        _tool(
            "check_inbox",
            "Check your incoming messages on the Agent Message Bus. "
            "This composite tool handles trigger file detection (<60s freshness), "
            "reads all pending messages, auto-acks them, and cleans up stale triggers. "
            "Returns the full list of pending messages in a readable format. "
            "Call this at the start of every turn to process inter-agent messages.",
            {
                "type": "object",
                "properties": {
                    "limit": {"type": "integer", "description": "Max messages to return (default: 20)", "default": 20},
                    "mark_read": {"type": "boolean", "description": "Auto-mark as read (default: true)", "default": True}
                }
            }
        ),
        _tool(
            "delegate_task",
            "Delegate a task to another agent via the Agent Message Bus. "
            "This composite tool checks the permission matrix, sends the message, "
            "triggers wakeup for the target, and notifies the user. "
            "Supported targets: 'general', 'dev', 'research', 'study'.",
            {
                "type": "object",
                "properties": {
                    "target_agent": {"type": "string", "enum": ["general", "dev", "research", "study"],
                                     "description": "Target agent to delegate to"},
                    "task": {"type": "string", "description": "The task description/instructions for the target agent"},
                    "priority": {"type": "integer", "enum": [0, 1, 2], "description": "Priority (0=normal, 1=high, 2=urgent)", "default": 0}
                },
                "required": ["target_agent", "task"]
            }
        ),
        _tool(
            "respond_to_message",
            "Mark a message as completed with a result and optionally send a reply. "
            "This composite tool marks the original message as done, creates a reply "
            "message back to the sender, and tracks conversation chain depth. "
            "Call this after you've processed a delegated task.",
            {
                "type": "object",
                "properties": {
                    "message_id": {"type": "integer", "description": "The message ID to mark as done"},
                    "response": {"type": "string", "description": "Your response/result for the sender"},
                    "send_reply": {"type": "boolean", "description": "Send an auto-reply back to the original sender (default: true)", "default": True}
                },
                "required": ["message_id", "response"]
            }
        ),
        # === LOW-LEVEL SEND (typed messages with full payload) ===
        _tool(
            "send_bus_message",
            "Send a strictly typed async message to another agent via the Agent Message Bus. "
            "This is a low-level tool for advanced use — prefer delegate_task() for most "
            "delegation scenarios. Includes idempotency protection: duplicate messages are "
            "detected and rejected.",
            {
                "type": "object",
                "properties": {
                    "target_agent_id": {"type": "string", "description": "Target agent: 'general', 'dev', 'research', 'study', or dynamically registered"},
                    "message_type": {"type": "string", "enum": ["request_data","handoff","clarification","status_update","alert","task_delegation"]},
                    "payload": {
                        "type": "object",
                        "description": "MUST contain 'summary' and 'body' keys. Optional: 'context'.",
                        "required": ["summary", "body"],
                        "properties": {
                            "summary": {"type": "string"},
                            "body": {"type": "string"},
                            "context": {"type": "object"}
                        }
                    },
                    "priority": {"type": "integer", "enum": [0,1,2], "default": 0},
                    "correlation_id": {"type": "integer", "description": "Optional: root message ID of this chain"},
                    "parent_message_id": {"type": "integer", "description": "Optional: immediate parent message ID"},
                    "idempotency_key": {"type": "string", "description": "UUID v4. Auto-generated if empty."},
                    "expires_at": {"type": "number", "description": "Unix timestamp. Auto-calc from priority."},
                    "max_retries": {"type": "integer", "default": 3, "description": "Max retries before DLQ."}
                },
                "required": ["target_agent_id", "message_type", "payload"]
            }
        ),
        # --- Agent Card / Discovery tools ---
        _tool(
            "agent_discover",
            "Find the best-matching agent(s) for a given task description. "
            "Returns a ranked list of (agent, skill, score, reasoning) based on "
            "the Agent Card registry. Use this BEFORE delegating to pick the "
            "right specialist automatically — no need to ask the user which agent.",
            {
                "type": "object",
                "properties": {
                    "task": {"type": "string", "description": "Natural-language description of the task to route"},
                    "top_k": {"type": "integer", "description": "Max number of agents to return (default: 3)", "default": 3},
                    "min_score": {"type": "number", "description": "Minimum score threshold (default: 1.0). Below this, falls back to orchestrator.", "default": 1.0}
                },
                "required": ["task"]
            }
        ),
        _tool(
            "agent_list_cards",
            "List all registered Agent Cards (orchestrator + all specialists) "
            "with their skills and descriptions. Use this to learn what each "
            "agent can do.",
            {"type": "object", "properties": {}}
        ),
        _tool(
            "agent_record_skill",
            "Record a skill invocation. Call this after a specialist agent "
            "completes work, so the Phase 3 router can learn which skills match "
            "which task patterns.",
            {
                "type": "object",
                "properties": {
                    "agent": {"type": "string", "description": "Agent that ran the skill"},
                    "skill": {"type": "string", "description": "Skill ID that was used"},
                    "task_excerpt": {"type": "string", "description": "Short excerpt of the task (max 200 chars)"}
                },
                "required": ["agent", "skill"]
            }
        ),
        # --- Autonomy tools ---
        _tool(
            "autonomy_get_levels",
            "Get all autonomy category levels. "
            "Each category has a level (1=notify only, 2=suggest+approve, 3=autonomous). "
            "Locked categories cannot be changed.",
            {"type": "object", "properties": {}}
        ),
        _tool(
            "autonomy_set_level",
            "Set the autonomy level for a category. "
            "Level 1 = notify only, Level 2 = suggest + wait for approval, "
            "Level 3 = autonomous + report. Locked categories cannot be changed.",
            {
                "type": "object",
                "properties": {
                    "category": {"type": "string", "description": "Category key (e.g., 'git_push', 'deployment', 'file_write')"},
                    "level": {"type": "integer", "description": "Autonomy level (1=notify, 2=suggest, 3=autonomous)"}
                },
                "required": ["category", "level"]
            }
        ),
        _tool(
            "autonomy_classify_command",
            "Classify a shell command to determine which autonomy category it belongs to. "
            "Useful before deciding whether a command needs approval.",
            {
                "type": "object",
                "properties": {
                    "command": {"type": "string", "description": "The shell command to classify"}
                },
                "required": ["command"]
            }
        ),
        # --- Dream Engine tools ---
        _tool(
            "dream_get_last",
            "Get the most recent Dream Engine report. "
            "Shows last night's consolidation results including "
            "skill suggestions, memory health, and today's top priorities.",
            {"type": "object", "properties": {}}
        ),
        _tool(
            "get_message_trace",
            "Retrieve the full causal trace tree for a message chain. "
            "Given a correlation_id (root message ID), returns all messages in the chain "
            "ordered chronologically with parent-child relationships.",
            {
                "type": "object",
                "properties": {
                    "correlation_id": {"type": "integer", "description": "The root message ID (correlation_id) to trace"}
                },
                "required": ["correlation_id"]
            }
        ),
        # --- System tools ---
        _tool(
            "amb_status",
            "Get overall status of the Agent Message Bus integration system: "
            "message queue stats, autonomy config, and last dream report.",
            {"type": "object", "properties": {}}
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[dict]:
    try:
        me = _detect_agent()

        # ==================================================================
        # COMPOSITE: check_inbox
        # ==================================================================
        if name == "check_inbox":
            limit = arguments.get("limit", 20)
            do_mark_read = arguments.get("mark_read", True)

            # 1. Check trigger file
            trigger_path = _get_trigger_path(me)
            trigger_status = "none"
            if trigger_path.exists():
                try:
                    trigger_data = json.loads(trigger_path.read_text())
                    ts = trigger_data.get("timestamp", 0)
                    age = time.time() - ts  # Note: uses imported `time` namespace
                    if age < 60:
                        trigger_status = "fresh"
                        logger.info("Fresh trigger file found for %s (%.0fs old)", me, age)
                    else:
                        trigger_status = "stale"
                        logger.info("Stale trigger file found for %s (%.0fs old) — deleting", me, age)
                        trigger_path.unlink(missing_ok=True)
                except (json.JSONDecodeError, OSError) as exc:
                    logger.warning("Failed to read trigger file: %s", exc)
                    trigger_path.unlink(missing_ok=True)

            # 2. Query pending/delivered messages
            msgs = get_messages(to_agent=me, status="pending,delivered", limit=limit)

            # 3. Auto-ack
            if do_mark_read:
                for m in msgs:
                    mark_read(m["id"])
                    try:
                        open_message_thread(m["id"])
                    except Exception:
                        pass

            # 4. Clean up trigger file after processing
            if trigger_path.exists():
                trigger_path.unlink(missing_ok=True)

            # 5. Build response
            if not msgs:
                return _json_result({
                    "status": "empty",
                    "trigger_status": trigger_status,
                    "messages": [],
                    "message": "📭 Nincs új üzenet. (trigger: {})".format(trigger_status)
                })

            lines = [f"📬 **Bejövő üzenetek ({len(msgs)})**:\n"]
            for m in msgs:
                created = datetime.fromtimestamp(m["created_at"], tz=timezone.utc)
                lines.append(
                    f"🆔 #{m['id']} | 📤 {m['from_agent']} "
                    f"| 🏷 {m['status']} | 📅 {created.strftime('%H:%M UTC')}\n"
                    f"> {m['content'][:300]}\n"
                )

            return _json_result({
                "status": "ok",
                "trigger_status": trigger_status,
                "message_count": len(msgs),
                "messages": [
                    {
                        "id": m["id"],
                        "from": m["from_agent"],
                        "content": m["content"][:500],
                        "priority": m.get("priority", 0),
                        "created_at": m["created_at"],
                    }
                    for m in msgs
                ],
                "display": "\n".join(lines)
            })

        # ==================================================================
        # COMPOSITE: delegate_task
        # ==================================================================
        elif name == "delegate_task":
            target_agent = arguments["target_agent"]
            task = arguments["task"]
            priority = arguments.get("priority", 0)

            # 1. Permission check
            if not _check_permission(me, target_agent):
                return _json_result({
                    "status": "blocked",
                    "reason": f"❌ Permission denied: {me} → {target_agent} is not in the allowed delegation matrix."
                })

            # 2. Create message
            msg = create_message(me, target_agent, task, priority)

            # 3. Trigger wakeup
            try:
                from agent_message_bus.webhook_handler import handle_wakeup
                handle_wakeup(
                    target_agent=target_agent,
                    message_id=msg["id"],
                    from_agent=me,
                    priority=priority,
                    preview=task[:120],
                )
            except Exception as exc:
                logger.warning("Wakeup trigger failed: %s", exc)

            return _json_result({
                "status": "sent",
                "message_id": msg["id"],
                "to_agent": target_agent,
                "message": f"✅ Delegáltam {target_agent} részére: {task[:100]}..."
            })

        # ==================================================================
        # COMPOSITE: respond_to_message
        # ==================================================================
        elif name == "respond_to_message":
            msg_id = arguments["message_id"]
            response = arguments["response"]
            send_reply = arguments.get("send_reply", True)

            # 1. Try v7.2 ownership-validated response path
            amb_response = None
            try:
                import sys as _sys
                _bus_dir = str(Path(__file__).parent.parent / "bus")
                if _bus_dir not in _sys.path:
                    _sys.path.insert(0, _bus_dir)
                import amb_response as _ar
                amb_response = _ar
            except ImportError:
                pass

            response_written = False
            if amb_response:
                try:
                    # Wrap string response in amb.response.v1 format
                    if isinstance(response, str):
                        structured = {
                            "schema_version": "amb.response.v1",
                            "status": "success",
                            "summary": response[:200],
                            "result": response,
                            "artifacts": [],
                            "error": None,
                            "metadata": {"source": "mcp"},
                        }
                    else:
                        structured = response
                    amb_response.respond_to_message(msg_id, structured)
                    response_written = True
                except (RuntimeError, ValueError) as exc:
                    logger.warning("v7.2 respond_to_message failed for #%d, falling back to mark_done: %s", msg_id, exc)

            # 2. Fallback: legacy mark_done (no ownership validation)
            if not response_written:
                ok = mark_done(msg_id, response)
                if not ok:
                    return _text_result(f"⚠️ #{msg_id} nem található vagy már lezárva.")

            # 2. Get original message for sender info by querying DB directly
            import sqlite3
            db_path = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))) / "data" / "agent_message_bus" / "agent_messages.db"
            original_sender = None
            original_priority = 0
            try:
                conn = sqlite3.connect(str(db_path))
                row = conn.execute("SELECT from_agent, priority, to_agent FROM agent_messages WHERE id = ?", (msg_id,)).fetchone()
                if row:
                    original_sender = row[0]
                    original_priority = row[1] or 0
                conn.close()
            except Exception as exc:
                logger.warning("Failed to query original message #%d: %s", msg_id, exc)

            # 3. Send reply if requested
            reply_id = None
            chain_depth = 0
            if send_reply and original_sender and original_sender != me:
                # Track chain depth
                chain_depth = _get_chain_depth(msg_id)
                if chain_depth >= MAX_CHAIN_DEPTH:
                    logger.warning("Chain depth %d reached for #%d — sending anyway", chain_depth, msg_id)

                reply_msg = create_message(
                    me, original_sender,
                    f"[Válasz #{msg_id}] {response}",
                    priority=original_priority,
                )
                reply_id = reply_msg["id"]

                # Trigger wakeup for the original sender
                try:
                    from agent_message_bus.webhook_handler import handle_wakeup
                    handle_wakeup(
                        target_agent=original_sender,
                        message_id=reply_msg["id"],
                        from_agent=me,
                        priority=0,
                        preview=response[:120],
                    )
                except Exception as exc:
                    logger.warning("Reply wakeup failed: %s", exc)

            result = {
                "status": "done",
                "message_id": msg_id,
                "chain_depth": chain_depth,
            }
            if reply_id:
                result["reply_message_id"] = reply_id
                result["reply_to"] = original_sender

            return _json_result(result)

        # ==================================================================
        # LOW-LEVEL: send_bus_message (keep for advanced typed messaging)
        # ==================================================================
        elif name == "send_bus_message":
            from_agent = me
            target = arguments["target_agent_id"]
            payload_dict = arguments["payload"]
            if "summary" not in payload_dict or "body" not in payload_dict:
                return _json_result({"status": "error", "message": "payload must contain 'summary' and 'body' keys"})
            content = json.dumps(payload_dict, ensure_ascii=False)
            msg_type = arguments["message_type"]
            priority = arguments.get("priority", 0)
            corr_id = arguments.get("correlation_id")
            parent_id = arguments.get("parent_message_id")
            idempotency_key = arguments.get("idempotency_key", "")
            expires_at = arguments.get("expires_at")
            max_retries = arguments.get("max_retries", 3)

            from agent_message_bus import create_typed_message
            msg = create_typed_message(
                from_agent=from_agent, to_agent=target, content=content,
                message_type=msg_type, priority=priority,
                correlation_id=corr_id, parent_message_id=parent_id,
                idempotency_key=idempotency_key, expires_at=expires_at,
                max_retries=max_retries
            )

            from agent_message_bus.webhook_handler import handle_wakeup
            wakeup = handle_wakeup(
                target_agent=target, message_id=msg["id"],
                from_agent=from_agent, priority=priority,
                message_type=msg_type,
                preview=arguments["payload"].get("summary", "")[:120],
                max_retries=max_retries,
            )

            return _json_result({
                "status": "sent",
                "message_id": msg["id"],
                "to_agent": target,
                "wakeup": wakeup["action"],
                "message": f"Message #{msg['id']} sent to {target}. Wakeup: {wakeup['action']}."
            })

        # ==================================================================
        # DISCOVERY TOOLS
        # ==================================================================
        elif name == "agent_discover":
            task = arguments["task"]
            top_k = arguments.get("top_k", 3)
            min_score = arguments.get("min_score", 1.0)
            matches = discover_agents(task, top_k=top_k, min_score=min_score)
            if not matches:
                return _text_result("Nincs regisztrált Agent Card. Hozz létre egyet ~/.hermes/data/agent_message_bus/agent_cards/ alá.")
            lines = [f"**🎯 Agent routing — top {len(matches)} találat**\n"]
            for i, m in enumerate(matches, 1):
                emoji = "🏆" if i == 1 else f"{i}."
                lines.append(
                    f"{emoji} **{m['display_name']}** (`{m['agent']}`) — score: {m['score']}\n"
                    f"   🎯 skill: `{m['skill'] or '—'}`\n"
                    f"   💡 {m['reasoning']}\n"
                    f"   🤖 model: {m['model']} | autonómia: {m['autonomy_level']}\n"
                )
            return _text_result("\n".join(lines))

        elif name == "agent_list_cards":
            cards = list_agent_cards()
            if not cards:
                return _text_result("Nincs regisztrált Agent Card.")
            lines = [f"**📚 Regisztrált Agent Card-ok ({len(cards)}):**\n"]
            for c in cards:
                tag = " [fallback]" if c.get("is_fallback") else ""
                skills = ", ".join(c.get("skills", [])[:5])
                if len(c.get("skills", [])) > 5:
                    skills += f" +{len(c['skills']) - 5}"
                lines.append(
                    f"**{c['display_name']}** (`{c['agent']}`){tag}\n"
                    f"  {c['description'][:120]}\n"
                    f"  🎯 skills: {skills}\n"
                    f"  🤖 model: {c['model']}\n"
                )
            return _text_result("\n".join(lines))

        elif name == "agent_record_skill":
            agent = arguments["agent"]
            skill = arguments["skill"]
            task_excerpt = arguments.get("task_excerpt", "")
            record_skill_invocation(agent, skill, task_excerpt)
            return _text_result(f"✅ Rögzítve: {agent} → {skill}")

        # ==================================================================
        # AUTONOMY TOOLS
        # ==================================================================
        elif name == "autonomy_get_levels":
            cats = get_all_autonomy_categories()
            lines = ["**⚙️ Autonómia szintek:**\n"]
            for c in cats:
                level = c["level"]
                emoji = {1: "🔴", 2: "🟡", 3: "🟢"}.get(level, "⚪")
                lock = "🔒" if c.get("locked") else ""
                lines.append(f"{emoji} {lock}**{c['label']}** → {level}. szint\n")
            return _text_result("\n".join(lines))

        elif name == "autonomy_set_level":
            category = arguments["category"]
            level = arguments["level"]
            ok, msg = set_autonomy_level(category, level)
            if ok:
                return _text_result(f"✅ {msg}")
            return _text_result(f"❌ {msg}")

        elif name == "autonomy_classify_command":
            command = arguments["command"]
            cat = classify_command(command)
            level = get_autonomy_level(cat)
            cats = get_all_autonomy_categories()
            label = cat
            for c in cats:
                if c["key"] == cat:
                    label = c["label"]
                    break
            return _text_result(
                f"Parancs: `{command[:100]}`\n"
                f"Kategória: **{label}** ({cat})\n"
                f"Autonómia szint: **{level}** "
                f"({'autonóm' if level >= 3 else 'jóváhagyás kell' if level >= 2 else 'csak jelzés'})"
            )

        # ==================================================================
        # DREAM ENGINE
        # ==================================================================
        elif name == "dream_get_last":
            dreams = sorted(DREAMS_DIR.glob("*.md"), reverse=True)
            if not dreams:
                return _text_result("Még nincs Dream Engine jelentés. Az első ma éjjel (02:00 UTC) készül.")
            content = dreams[0].read_text()
            return _text_result(f"**🌙 Dream Engine — {dreams[0].stem}**\n\n{content}")

        elif name == "get_message_trace":
            from agent_message_bus import get_message_tree
            cid = arguments["correlation_id"]
            tree = get_message_tree(cid)
            lines = [f"**🔗 Message trace for correlation #{cid}**\n"]
            for m in tree:
                parent = f"← #{m.get('parent_message_id')}" if m.get('parent_message_id') else "🌱 root"
                lines.append(
                    f"#{m['id']} | {m['from_agent']} → {m['to_agent']} | {m.get('message_type','?')} | {parent}\n"
                    f"> {m['content'][:150]}\n"
                )
            return _text_result("\n".join(lines))

        # ==================================================================
        # SYSTEM STATUS
        # ==================================================================
        elif name == "amb_status":
            pending = get_messages(status="pending", limit=0)
            delivered = get_messages(status="delivered", limit=0)
            done_today = get_messages(status="done", limit=100)
            cats = get_all_autonomy_categories()
            level3 = sum(1 for c in cats if c["level"] == 3)
            level2 = sum(1 for c in cats if c["level"] == 2)
            level1 = sum(1 for c in cats if c["level"] == 1)
            dreams = sorted(DREAMS_DIR.glob("*.md"), reverse=True)
            last_dream = dreams[0].stem if dreams else "Még nincs"
            dead = get_messages(status="dead", limit=0)

            cb_agents = ["dev", "general", "research", "study"]
            try:
                from agent_message_bus.circuit_breaker import get_circuit_state as get_cb_state
                cb_states = {a: get_cb_state(a) for a in cb_agents}
            except Exception:
                cb_states = {}

            try:
                from agent_message_bus.metrics import get_metrics_summary
                metrics = get_metrics_summary()
            except Exception:
                metrics = {}

            cb_lines = []
            if cb_states:
                for agent in cb_agents:
                    state = cb_states.get(agent, "unknown")
                    cb_lines.append(f"- {agent}: {state}")
            else:
                cb_lines.append("- (unavailable)")

            metrics_lines = []
            if metrics:
                metrics_lines.append(f"- messages_sent_total: {metrics.get('messages_sent_total', 'N/A')}")
                metrics_lines.append(f"- wakeup_latency_p95: {metrics.get('wakeup_latency_p95', 'N/A')}ms")
                dsr = metrics.get('delivery_success_rate', 'N/A')
                if isinstance(dsr, float):
                    dsr = round(dsr * 100, 2)
                metrics_lines.append(f"- delivery_success_rate: {dsr}%")
            else:
                metrics_lines.append("- (unavailable)")

            return _text_result(
                "**📊 Agent Message Bus Integration Status**\n\n"
                "**📬 Agent Message Bus**\n"
                f"- Függőben lévő üzenetek: {len(pending)}\n"
                f"- Kézbesítve, olvasatlan: {len(delivered)}\n"
                f"- Teljesítve ma: {len(done_today)}\n\n"
                "**⚙️ Autonómia**\n"
                f"- 🟢 Autonóm (3): {level3} kategória\n"
                f"- 🟡 Jóváhagyásos (2): {level2} kategória\n"
                f"- 🔴 Csak jelzés (1): {level1} kategória\n"
                f"- Összesen: {len(cats)} kategória\n\n"
                "**🔌 Circuit Breakers**\n"
                + "\n".join(cb_lines) + "\n\n"
                "**📊 Metrics (last hour)**\n"
                + "\n".join(metrics_lines) + "\n\n"
                "**⚠️ Dead Letter Queue**: " + str(len(dead)) + " messages\n\n"
                "**🌙 Dream Engine**\n"
                f"- Utolsó jelentés: {last_dream}\n"
                f"- Kimenetek: {len(dreams)} éjszaka\n"
            )

        else:
            return _text_result(f"Ismeretlen tool: {name}")

    except Exception as e:
        logger.exception(f"Error in {name}")
        return _text_result(f"Error: {str(e)}")


async def main():
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="agent_message_bus",
                server_version="1.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())
