#!/usr/bin/env python3
"""
Marveen MCP Server — Hermes Agent Integration

Exposes tools for:
- Agent Message Bus (inter-agent communication)
- Gradual Autonomy (trust levels)
- Dream Engine (nightly consolidation)

Usage:
    chmod +x ~/.hermes/scripts/marveen_mcp_server.py
    ~/.hermes/scripts/marveen_mcp_server.py

Register in ~/.hermes/config.yaml:
    mcp_servers:
      marveen:
        command: "/home/artofphotogrphyy/.hermes/scripts/marveen_mcp_server.py"
        timeout: 30
        connect_timeout: 5
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Ensure marveen module is importable from the central location
sys.path.insert(0, str(Path.home() / ".hermes" / "scripts"))
from marveen import (
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
logger = logging.getLogger("marveen-mcp")


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

server = Server("marveen")


def _tool(name: str, description: str, inputSchema: dict) -> Tool:
    return Tool(name=name, description=description, inputSchema=inputSchema)


@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        # --- Agent Message Bus tools ---
        _tool(
            "agent_send_message",
            "Send an async message to another agent via the Marveen Message Bus. "
            "Agents: 'general', 'dev', 'research', 'study'. "
            "The from_agent is auto-detected from your profile. "
            "The message will be delivered when the target agent checks their inbox.",
            {
                "type": "object",
                "properties": {
                    "to_agent": {"type": "string", "description": "Target agent name: general, dev, research, or study"},
                    "content": {"type": "string", "description": "Message content"},
                    "priority": {"type": "integer", "description": "Priority (0=normal, 1=high, 2=urgent)", "default": 0}
                },
                "required": ["to_agent", "content"]
            }
        ),
        _tool(
            "send_bus_message",
            "Send a strictly typed async message to another agent via the Marveen Message Bus. "
            "This is the PRIMARY tool for inter-agent communication — use it whenever you need "
            "data, handoff a task, request clarification, or alert another agent. "
            "Includes idempotency protection: duplicate messages are detected and rejected.",
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
        _tool(
            "agent_read_messages",
            "Read your incoming messages from the agent message bus. "
            "Returns pending (unread) messages by default.",
            {
                "type": "object",
                "properties": {
                    "status": {"type": "string", "enum": ["pending", "delivered", "done", "failed", "read"], "description": "Filter by status (default: pending)"},
                    "limit": {"type": "integer", "description": "Max messages to return (default: 20)", "default": 20},
                    "mark_read": {"type": "boolean", "description": "Mark returned messages as 'read' (default: true)", "default": True}
                }
            }
        ),
        _tool(
            "agent_mark_done",
            "Mark a message as completed with a result. "
            "Call this after you've processed a message.",
            {
                "type": "object",
                "properties": {
                    "message_id": {"type": "integer", "description": "The message ID to mark as done"},
                    "result": {"type": "string", "description": "Optional result/response text"}
                },
                "required": ["message_id"]
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
            "marveen_status",
            "Get overall status of the Marveen integration system: "
            "message queue stats, autonomy config, and last dream report.",
            {"type": "object", "properties": {}}
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict) -> list[dict]:
    try:
        if name == "agent_send_message":
            # Auto-detect sender from HERMES_PROFILE (most reliable) or fallback to HERMES_HOME
            # HERMES_PROFILE=dev → from_agent=dev
            # HERMES_HOME=/home/user/.hermes/profiles/research → from_agent=research
            from_agent = os.environ.get("HERMES_PROFILE") or os.environ.get("HERMES_HOME", "").rsplit("/", 1)[-1] or "general"
            # Fallback: if path is the root .hermes dir, use "general"
            if from_agent == ".hermes":
                from_agent = "general"
            to_agent = arguments["to_agent"]
            content = arguments["content"]
            priority = arguments.get("priority", 0)
            msg = create_message(from_agent, to_agent, content, priority)
            try:
                from marveen.webhook_handler import handle_wakeup
                handle_wakeup(target_agent=to_agent, message_id=msg["id"], from_agent=from_agent, priority=priority)
            except Exception:
                pass
            return _json_result({
                "status": "sent",
                "message_id": msg["id"],
                "to_agent": to_agent,
                "message": f"Üzenet elküldve: {to_agent} részére. "
                          f"Az üzenet a következő alkalommal kerül kézbesítésre, "
                          f"amikor {to_agent} ellenőrzi a bejövő üzeneteit."
            })

        elif name == "send_bus_message":
            from_agent = os.environ.get("HERMES_PROFILE") or os.environ.get("HERMES_HOME", "").rsplit("/", 1)[-1] or "general"
            if from_agent == ".hermes":
                from_agent = "general"
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

            # Validate + insert via create_typed_message
            from marveen import create_typed_message
            msg = create_typed_message(
                from_agent=from_agent, to_agent=target, content=content,
                message_type=msg_type, priority=priority,
                correlation_id=corr_id, parent_message_id=parent_id,
                idempotency_key=idempotency_key, expires_at=expires_at,
                max_retries=max_retries
            )

            # Trigger wakeup via webhook handler
            from marveen.webhook_handler import handle_wakeup
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

        elif name == "agent_read_messages":
            # Auto-detect who is reading from HERMES_PROFILE or HERMES_HOME
            me = os.environ.get("HERMES_PROFILE") or os.environ.get("HERMES_HOME", "").rsplit("/", 1)[-1] or "general"
            if me == ".hermes":
                me = "general"
            status = arguments.get("status", "pending")
            limit = arguments.get("limit", 20)
            do_mark_read = arguments.get("mark_read", True)

            msgs = get_messages(to_agent=me, status=status, limit=limit)
            
            if do_mark_read and status == "pending":
                logger.info(f"agent_read_messages: {len(msgs)} pending, opening threads")
                for m in msgs:
                    mark_read(m["id"])
                    # Open Discord thread so user can see we're working
                    open_message_thread(m["id"])

            if not msgs:
                return _text_result("Nincs új üzenet.")

            lines = [f"📬 **Bejövő üzenetek ({len(msgs)})**:\n"]
            for m in msgs:
                created = datetime.fromtimestamp(m["created_at"], tz=timezone.utc)
                lines.append(
                    f"🆔 #{m['id']} | 📤 {m['from_agent']} "
                    f"| 🏷 {m['status']} | 📅 {created.strftime('%H:%M UTC')}\n"
                    f"> {m['content'][:200]}\n"
                )
            return _text_result("\n".join(lines))

        elif name == "agent_mark_done":
            msg_id = arguments["message_id"]
            result = arguments.get("result", "")
            ok = mark_done(msg_id, result)
            if ok:
                return _text_result(f"✅ #{msg_id} megjelölve done-ként.")
            return _text_result(f"⚠️ #{msg_id} nem található vagy már lezárva.")

        elif name == "agent_discover":
            task = arguments["task"]
            top_k = arguments.get("top_k", 3)
            min_score = arguments.get("min_score", 1.0)
            matches = discover_agents(task, top_k=top_k, min_score=min_score)
            if not matches:
                return _text_result("Nincs regisztrált Agent Card. Hozz létre egyet ~/.hermes/data/marveen/agent_cards/ alá.")
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

        elif name == "dream_get_last":
            dreams = sorted(DREAMS_DIR.glob("*.md"), reverse=True)
            if not dreams:
                return _text_result("Még nincs Dream Engine jelentés. Az első ma éjjel (02:00 UTC) készül.")
            content = dreams[0].read_text()
            return _text_result(f"**🌙 Dream Engine — {dreams[0].stem}**\n\n{content}")

        elif name == "get_message_trace":
            from marveen import get_message_tree
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

        elif name == "marveen_status":
            # Message queue stats
            pending = get_messages(status="pending", limit=0)
            delivered = get_messages(status="delivered", limit=0)
            done_today = get_messages(status="done", limit=100)
            
            # Autonomy
            cats = get_all_autonomy_categories()
            level3 = sum(1 for c in cats if c["level"] == 3)
            level2 = sum(1 for c in cats if c["level"] == 2)
            level1 = sum(1 for c in cats if c["level"] == 1)
            
            # Dream engine
            dreams = sorted(DREAMS_DIR.glob("*.md"), reverse=True)
            last_dream = dreams[0].stem if dreams else "Még nincs"
            
            # Dead Letter Queue
            dead = get_messages(status="dead", limit=0)
            
            # Circuit Breaker states
            cb_agents = ["dev", "general", "research", "study"]
            try:
                from marveen.circuit_breaker import get_circuit_state as get_cb_state
                cb_states = {}
                for agent in cb_agents:
                    cb_states[agent] = get_cb_state(agent)
            except Exception:
                cb_states = {}
            
            # Metrics summary
            try:
                from marveen.metrics import get_metrics_summary
                metrics = get_metrics_summary()
            except Exception:
                metrics = {}
            
            # Build output
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
                "**📊 Marveen Integration Status**\n\n"
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
                server_name="marveen",
                server_version="1.0.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())
