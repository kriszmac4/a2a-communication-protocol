#!/usr/bin/env python3
"""mmb — Agent Message Bus CLI tool."""

import sqlite3
import sys
import os
import textwrap
import time
import json

# ── Standalone paths ────────────────────────────────────────────────────────
# DB is managed by the agent_message_bus module; we read it directly.
_AMB_DATA_DIR = os.environ.get("AMB_DATA_DIR", os.path.expanduser("~/.a2a-protocol"))
DB_PATH = os.path.join(_AMB_DATA_DIR, "agent_messages.db")

# Module is in the parent's agent_message_bus/ directory
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_MODULE_DIR = os.path.join(os.path.dirname(_THIS_DIR), "agent_message_bus")
sys.path.insert(0, os.path.dirname(_MODULE_DIR))
from agent_message_bus.permissions import check_permission, PermissionError, ALLOWED_TYPES
from agent_message_bus.schemas import MessageType

VALID_AGENTS = ["general", "dev", "research", "study"]
VALID_STATUSES = ["pending", "delivered", "done", "failed", "read", "dead", "expired"]


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def cmd_publish(args):
    to_agent = None
    msg = None
    message_type = "INFO"
    priority = 0
    from_agent = "dev"
    i = 1
    while i < len(args):
        if args[i] == "--to" and i + 1 < len(args):
            to_agent = args[i + 1]
            i += 2
        elif args[i] == "--msg" and i + 1 < len(args):
            msg = args[i + 1]
            i += 2
        elif args[i] == "--type" and i + 1 < len(args):
            message_type = args[i + 1]
            i += 2
        elif args[i] == "--priority" and i + 1 < len(args):
            priority = int(args[i + 1])
            i += 2
        elif args[i] == "--from" and i + 1 < len(args):
            from_agent = args[i + 1]
            i += 2
        else:
            i += 1

    if not to_agent or not msg:
        print("Usage: mmb publish --to <agent> --msg <text> [--type TYPE] [--priority N]")
        sys.exit(1)

    if to_agent not in VALID_AGENTS:
        print(f"Error: Invalid target agent '{to_agent}'. Valid: {', '.join(VALID_AGENTS)}", file=sys.stderr)
        sys.exit(1)

    if from_agent not in VALID_AGENTS:
        print(f"Error: Invalid sender agent '{from_agent}'. Valid: {', '.join(VALID_AGENTS)}", file=sys.stderr)
        sys.exit(1)

    if message_type not in ALLOWED_TYPES:
        print(f"Error: Invalid message type '{message_type}'. Valid: {', '.join(ALLOWED_TYPES)}", file=sys.stderr)
        sys.exit(1)

    try:
        check_permission(from_agent, to_agent, message_type)
    except PermissionError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    import time as _time
    now = _time.time()

    conn = get_db()
    c = conn.cursor()
    c.execute(
        """INSERT INTO agent_messages
           (from_agent, to_agent, content, status, message_type, priority, created_at)
           VALUES (?, ?, ?, 'pending', ?, ?, ?)""",
        (from_agent, to_agent, msg, message_type, priority, now),
    )
    msg_id = c.lastrowid
    conn.commit()
    conn.close()

    print(f"✅ #{msg_id} {from_agent}→{to_agent} sent!")


def fmt_age(created_at):
    import time as _time
    delta = _time.time() - created_at
    if delta < 60:
        return f"{int(delta)}s"
    elif delta < 3600:
        return f"{int(delta // 60)}m"
    elif delta < 86400:
        return f"{int(delta // 3600)}h"
    else:
        return f"{int(delta // 86400)}d"


def cmd_list(args):
    status_filter = None
    agent_filter = None
    show_all = False
    i = 1
    while i < len(args):
        if args[i] == "--status" and i + 1 < len(args):
            status_filter = args[i + 1]
            i += 2
        elif args[i] == "--agent" and i + 1 < len(args):
            agent_filter = args[i + 1]
            i += 2
        elif args[i] == "--all":
            show_all = True
            i += 1
        else:
            i += 1

    conn = get_db()
    c = conn.cursor()

    conditions = []
    params = []

    if status_filter:
        if status_filter not in VALID_STATUSES:
            print(f"Error: Invalid status '{status_filter}'. Valid: {', '.join(VALID_STATUSES)}", file=sys.stderr)
            sys.exit(1)
        conditions.append("status = ?")
        params.append(status_filter)
    elif not show_all:
        conditions.append("status = 'pending'")

    if agent_filter:
        conditions.append("to_agent = ?")
        params.append(agent_filter)
    elif not show_all:
        conditions.append("to_agent = 'dev'")

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    c.execute(f"SELECT id, from_agent, to_agent, status, created_at, content FROM agent_messages {where} ORDER BY created_at DESC LIMIT 50", params)
    rows = c.fetchall()
    conn.close()

    if not rows:
        print("No messages found.")
        return

    header = f"{'ID':<5} {'From → To':<18} {'Status':<12} {'Age':<8} Preview"
    print(header)
    print("-" * len(header))
    for row in rows:
        preview = (row["content"][:60] + "...") if len(row["content"]) > 60 else row["content"]
        preview = preview.replace("\n", " ")
        print(f"{row['id']:<5} {row['from_agent']} → {row['to_agent']:<12} {row['status']:<12} {fmt_age(row['created_at']):<8} {preview}")


def cmd_status(args):
    conn = get_db()
    c = conn.cursor()

    print("=== Message Bus Status ===")
    print()

    print("By Status:")
    c.execute("SELECT status, COUNT(*) as cnt FROM agent_messages GROUP BY status ORDER BY cnt DESC")
    rows = c.fetchall()
    if rows:
        for row in rows:
            print(f"  {row['status']:<12} {row['cnt']}")
    else:
        print("  (no messages)")

    print()
    print("By Target Agent:")
    c.execute("SELECT to_agent, COUNT(*) as cnt FROM agent_messages GROUP BY to_agent ORDER BY cnt DESC")
    rows = c.fetchall()
    if rows:
        for row in rows:
            print(f"  {row['to_agent']:<12} {row['cnt']}")
    else:
        print("  (no messages)")

    conn.close()


def cmd_replay(args):
    if len(args) < 1:
        print("Usage: mmb replay <id>")
        sys.exit(1)
    try:
        msg_id = int(args[0])
    except ValueError:
        print("Error: <id> must be an integer", file=sys.stderr)
        sys.exit(1)

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM agent_messages WHERE id = ?", (msg_id,))
    row = c.fetchone()
    conn.close()

    if not row:
        print(f"Message #{msg_id} not found.")
        sys.exit(1)

    print(f"ID:               {row['id']}")
    print(f"From:             {row['from_agent']}")
    print(f"To:               {row['to_agent']}")
    print(f"Status:           {row['status']}")
    print(f"Type:             {row['message_type'] or 'N/A'}")
    print(f"Priority:         {row['priority']}")
    print(f"Created:          {row['created_at']}")
    if row['delivered_at']:
        print(f"Delivered:        {row['delivered_at']}")
    if row['completed_at']:
        print(f"Completed:        {row['completed_at']}")
    print(f"Idempotency Key:  {row['idempotency_key'] or 'N/A'}")
    print(f"Correlation ID:   {row['correlation_id'] or 'N/A'}")
    print(f"Retry Count:      {row['retry_count']}")
    print(f"Max Retries:      {row['max_retries']}")
    print()
    print("--- Content ---")
    print(row['content'])
    if row['result']:
        print()
        print("--- Result ---")
        print(row['result'])


def cmd_watch(args):
    import time as _time
    seen_ids = set()

    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT id FROM agent_messages WHERE to_agent = 'dev' AND status IN ('pending', 'delivered')")
    for row in c.fetchall():
        seen_ids.add(row["id"])
    conn.close()

    print("👀 Watching for new messages targeting 'dev'... (Ctrl+C to stop)")
    try:
        while True:
            conn = get_db()
            c = conn.cursor()
            c.execute(
                "SELECT id, from_agent, to_agent, status, created_at, content, message_type "
                "FROM agent_messages WHERE to_agent = 'dev' AND status IN ('pending', 'delivered') "
                "ORDER BY created_at DESC LIMIT 20"
            )
            rows = c.fetchall()
            conn.close()

            for row in rows:
                if row["id"] not in seen_ids:
                    seen_ids.add(row["id"])
                    preview = (row["content"][:80] + "...") if len(row["content"]) > 80 else row["content"]
                    preview = preview.replace("\n", " ")
                    print(f"\n  [{row['status']}] #{row['id']} {row['from_agent']} → dev | {row['message_type'] or 'N/A'}")
                    print(f"  {preview}")

            _time.sleep(5)
    except KeyboardInterrupt:
        print("\n👋 Watch stopped.")


def print_help():
    print(textwrap.dedent("""\
    mmb — Agent Message Bus CLI

    Usage:
      mmb publish --to <agent> --msg <text>   Send a message
            [--type TYPE] [--priority N] [--from AGENT]
      mmb list                                 List messages (pending, to dev)
            [--status STATUS] [--agent AGENT] [--all]
      mmb status                               Show bus statistics
      mmb replay <id>                          Show full message details
      mmb watch                                Continuously poll for new messages
      mmb help                                 Show this help
    """))


def main():
    args = sys.argv[1:] if len(sys.argv) > 1 else []

    if not args or args[0] in ("help", "--help", "-h"):
        print_help()
        return

    cmd = args[0]
    if cmd == "publish":
        cmd_publish(args)
    elif cmd == "list":
        cmd_list(args)
    elif cmd == "status":
        cmd_status(args)
    elif cmd == "replay":
        cmd_replay(args[1:])
    elif cmd == "watch":
        cmd_watch(args)
    else:
        print(f"Unknown command: {cmd}", file=sys.stderr)
        print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
