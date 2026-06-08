#!/usr/bin/env python3
"""Agent Message Bus Bridge Engine — shared LLM Bridge logic."""

import json
import os
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ── Constants ─────────────────────────────────────────────────────────────────

# Max auto-reply chain depth (0 = original, 1 = first auto-reply, etc.)
MAX_CHAIN_DEPTH = 3

# Rate limiting: max N auto-replies per X seconds per (sender, receiver) pair
RATE_LIMIT_MAX = 3
RATE_LIMIT_WINDOW = 60  # seconds

# Senders we never reply to
SENDERS_BLACKLIST = {"auto_responder", "message-router", "agent_message_bus_llm_bridge"}

# Message types we auto-reply to
AUTO_REPLY_TYPES = {"delegate_task", "task_delegation", "request_data"}

# Where is the shared database
_AMB_DATA_DIR = Path(os.environ.get("AMB_DATA_DIR", Path.home() / ".a2a-protocol"))
DATA_DIR = _AMB_DATA_DIR
DB_PATH = DATA_DIR / "agent_messages.db"


@dataclass
class RateLimitBucket:
    """Token bucket rate limiter per (sender, receiver) pair."""
    timestamps: list = field(default_factory=list)

    def is_limited(self, max_hits: int = RATE_LIMIT_MAX,
                   window: int = RATE_LIMIT_WINDOW) -> bool:
        now = time.time()
        # Filter old timestamps
        self.timestamps = [t for t in self.timestamps if now - t < window]
        if len(self.timestamps) >= max_hits:
            return True
        self.timestamps.append(now)
        return False


# In-memory rate limit store (thread-safe)
_rate_limiters: dict[tuple[str, str], RateLimitBucket] = {}
_rate_lock = threading.Lock()


def _check_rate_limit(sender: str, receiver: str) -> bool:
    """Returns True if under rate limit (not limited)."""
    key = (sender, receiver)
    with _rate_lock:
        if key not in _rate_limiters:
            _rate_limiters[key] = RateLimitBucket()
        return not _rate_limiters[key].is_limited()


# ── Database ──────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    """Open the shared database and migrate if needed."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    _migrate_db(conn)
    return conn


def _migrate_db(conn: sqlite3.Connection):
    """Add missing columns to existing databases."""
    try:
        cur = conn.execute("PRAGMA table_info(agent_messages)")
        existing = {row[1] for row in cur.fetchall()}
        for col, dtype in [
            ("chain_depth", "INTEGER NOT NULL DEFAULT 0"),
            ("reply_to", "INTEGER"),
            ("is_auto_reply", "INTEGER NOT NULL DEFAULT 0"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE agent_messages ADD COLUMN {col} {dtype}")
    except Exception:
        pass  # Non-blocking


def get_pending_messages(conn: sqlite3.Connection, target: str,
                         limit: int = 10) -> list[dict]:
    """Fetch pending/delivered messages addressed to the target agent."""
    rows = conn.execute(
        """SELECT id, from_agent, to_agent, content, priority,
                  status, created_at, message_type, chain_depth, is_auto_reply
           FROM agent_messages
           WHERE to_agent = ? AND status IN ('pending', 'delivered')
           ORDER BY priority DESC, created_at ASC
           LIMIT ?""",
        (target, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def write_bridge_response(
    conn: sqlite3.Connection,
    to_agent: str,
    from_agent: str,
    llm_text: str,
    original_id: int,
    priority: int = 0,
    chain_depth: int = 1,
) -> Optional[int]:
    """Write a bridge response to the bus as AUTO_REPLY type with chain_depth tracking.

    Args:
        conn: DB connection
        to_agent: Target (original sender)
        from_agent: Sender (the target agent responding)
        llm_text: LLM response text
        original_id: Original message ID being replied to
        priority: Priority level
        chain_depth: Chain depth for the new message

    Returns:
        New message ID, or None on error
    """
    now = time.time()
    summary = f"Auto-reply {from_agent}→{to_agent}"
    body = llm_text[:2000]
    content = json.dumps({"summary": summary, "body": body}, ensure_ascii=False)

    try:
        conn.execute(
            """INSERT INTO agent_messages
               (from_agent, to_agent, content, status, priority, created_at,
                message_type, chain_depth, reply_to, is_auto_reply)
               VALUES (?, ?, ?, 'pending', ?, ?, 'auto_reply', ?, ?, 1)""",
            (from_agent, to_agent, content, priority, now,
             chain_depth, original_id),
        )
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Mark original as done
        conn.execute(
            "UPDATE agent_messages SET status = 'done', completed_at = ? WHERE id = ?",
            (now, original_id),
        )
        conn.commit()
        return new_id
    except Exception as e:
        conn.rollback()
        print(f"Bridge response write failed: {e}", file=__import__('sys').stderr)
        return None


def mark_read(conn: sqlite3.Connection, msg_id: int) -> bool:
    """Mark a message as read (don't reply to it)."""
    cur = conn.execute(
        "UPDATE agent_messages SET status = 'read' WHERE id = ? AND status IN ('pending', 'delivered')",
        (msg_id,),
    )
    conn.commit()
    return cur.rowcount > 0


def mark_failed(conn: sqlite3.Connection, msg_id: int) -> bool:
    """Mark a message as failed."""
    cur = conn.execute(
        "UPDATE agent_messages SET status = 'failed' WHERE id = ? AND status IN ('pending', 'delivered')",
        (msg_id,),
    )
    conn.commit()
    return cur.rowcount > 0


# ── Core logic ────────────────────────────────────────────────────────────────

def should_auto_reply(msg: dict, agent_id: str) -> tuple[bool, str]:
    """Check whether we can auto-reply to a message.

    Args:
        msg: Message dict (from get_pending_messages)
        agent_id: Our own agent name (e.g. 'dev', 'research')

    Returns:
        (True, "reason") if we can reply
        (False, "reason") if not
    """
    msg_id = msg["id"]
    from_agent = msg["from_agent"]
    to_agent = msg["to_agent"]
    msg_type = msg.get("message_type") or "unknown"
    content = msg.get("content", "") or ""
    chain_depth = msg.get("chain_depth") or 0
    is_auto_reply = msg.get("is_auto_reply") or 0

    # ── GUARD 1: Never reply to AUTO_REPLY type ──
    if msg_type == "auto_reply":
        return False, f"#{msg_id}: message_type=auto_reply → chain protection"

    # ── GUARD 2: is_auto_reply flag ──
    if is_auto_reply:
        return False, f"#{msg_id}: is_auto_reply=1 → chain protection"

    # ── GUARD 3: Never reply to blacklisted senders ──
    if from_agent in SENDERS_BLACKLIST:
        return False, f"#{msg_id}: from_agent={from_agent} → blacklisted"

    # ── GUARD 4: Don't reply to yourself ──
    if from_agent == agent_id:
        return False, f"#{msg_id}: from_agent={from_agent} → self-message"

    # ── GUARD 5: Only specific message types ──
    if msg_type not in AUTO_REPLY_TYPES:
        return False, f"#{msg_id}: message_type={msg_type} → not auto-reply type (only: {AUTO_REPLY_TYPES})"

    # ── GUARD 6: Chain depth limit ──
    if chain_depth >= MAX_CHAIN_DEPTH:
        return False, f"#{msg_id}: chain_depth={chain_depth} >= MAX({MAX_CHAIN_DEPTH}) → max depth reached"

    # ── GUARD 7: Filter auto-responder messages ──
    if content.startswith("📥") or content.startswith("📨") or content.startswith("🤖"):
        return False, f"#{msg_id}: auto-responder prefix → skipping"

    # ── GUARD 8: Rate limit ──
    if not _check_rate_limit(from_agent, to_agent):
        return False, f"#{msg_id}: rate limit exceeded for {from_agent}→{to_agent}"

    return True, f"#{msg_id}: OK → auto-reply allowed (depth={chain_depth})"


def compute_new_depth(msg: dict) -> int:
    """Calculate the chain_depth for the reply message."""
    return (msg.get("chain_depth") or 0) + 1
