#!/usr/bin/env python3
"""
Marveen Bridge Engine — közös LLM Bridge logika.

Minden ágens (General, Dev, Research, Study) ezt használja az
automatikus üzenetválaszhoz. Védelem a végtelen ciklusok ellen:

  1. AUTO_REPLY típusra SOHA nem válaszol
  2. chain_depth >= MAX_CHAIN_DEPTH esetén blokkol
  3. Rate limiting (N üzenet / X másodperc) ugyanazon sender-receiver pair között
  4. Senders blacklist (saját maguknak nem válaszolnak)
"""

import json
import os
import pwd
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml
import re

# ── Konstansok ─────────────────────────────────────────────────────────────────

# Max auto-reply chain depth (0 = original, 1 = first auto-reply, stb.)
MAX_CHAIN_DEPTH = 3

# Rate limiting: max N auto-reply per X másodperc ugyanazon (sender, receiver) pair között
RATE_LIMIT_MAX = 3
RATE_LIMIT_WINDOW = 60  # másodperc

# Senders akiknek sosem válaszolunk
SENDERS_BLACKLIST = {"auto_responder", "message-router", "agent_message_bus_llm_bridge"}

# Üzenet típusok amikre automatikusan válaszolunk
AUTO_REPLY_TYPES = {"delegate_task", "task_delegation", "request_data"}

# Hol van a közös adatbázis (kikerüli a Hermes HOME override-ot)
_SHARED_HOME = Path(pwd.getpwuid(os.getuid()).pw_dir) / ".hermes"
DATA_DIR = _SHARED_HOME / "data" / "agent_message_bus"
DB_PATH = DATA_DIR / "agent_messages.db"
HERMES_HOME = _SHARED_HOME


@dataclass
class RateLimitBucket:
    """Token bucket rate limiter per (sender, receiver) pair."""
    timestamps: list = field(default_factory=list)

    def is_limited(self, max_hits: int = RATE_LIMIT_MAX,
                   window: int = RATE_LIMIT_WINDOW) -> bool:
        now = time.time()
        # Szűrjük a régieket
        self.timestamps = [t for t in self.timestamps if now - t < window]
        if len(self.timestamps) >= max_hits:
            return True
        self.timestamps.append(now)
        return False


# In-memory rate limit store (thread-safe)
_rate_limiters: dict[tuple[str, str], RateLimitBucket] = {}
_rate_lock = threading.Lock()


def _check_rate_limit(sender: str, receiver: str) -> bool:
    """True ha rate limit alatt vagyunk (nem limitált)."""
    key = (sender, receiver)
    with _rate_lock:
        if key not in _rate_limiters:
            _rate_limiters[key] = RateLimitBucket()
        return not _rate_limiters[key].is_limited()


# ── Adatbázis ──────────────────────────────────────────────────────────────────

def get_db() -> sqlite3.Connection:
    """Megnyitja a közös adatbázist és migrál ha kell."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    # Auto-migration: add missing columns
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
    except Exception as e:
        pass  # Non-blocking


def get_pending_messages(conn: sqlite3.Connection, target: str,
                         limit: int = 10) -> list[dict]:
    """Lekéri a target agent-nek címzett pending/delivered üzeneteket."""
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
    """Bridge választ ír a buszba AUTO_REPLY típussal + chain_depth tracking.

    Args:
        conn: DB kapcsolat
        to_agent: Kinek címezzük (az eredeti feladó)
        from_agent: Kitől jön a válasz (a target, aki válaszol)
        llm_text: Az LLM válasz szövege
        original_id: Az eredeti üzenet ID-ja (amire válaszolunk)
        priority: Prioritás
        chain_depth: Az új üzenet chain_depth értéke

    Returns:
        Az új üzenet ID-ja, vagy None ha hiba
    """
    now = time.time()

    # Válasz JSON-be csomagolása
    summary = f"Auto-válasz {from_agent}→{to_agent}"
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

        # Eredeti üzenet done-ra
        conn.execute(
            "UPDATE agent_messages SET status = 'done', completed_at = ? WHERE id = ?",
            (now, original_id),
        )
        conn.commit()
        return new_id
    except Exception as e:
        conn.rollback()
        print(f"❌ Bridge response write failed: {e}", file=__import__('sys').stderr)
        return None


def mark_read(conn: sqlite3.Connection, msg_id: int) -> bool:
    """Üzenet read-re állítása (nem válaszolunk rá)."""
    cur = conn.execute(
        "UPDATE agent_messages SET status = 'read' WHERE id = ? AND status IN ('pending', 'delivered')",
        (msg_id,),
    )
    conn.commit()
    return cur.rowcount > 0


def mark_failed(conn: sqlite3.Connection, msg_id: int) -> bool:
    """Üzenet failed-re állítása."""
    cur = conn.execute(
        "UPDATE agent_messages SET status = 'failed' WHERE id = ? AND status IN ('pending', 'delivered')",
        (msg_id,),
    )
    conn.commit()
    return cur.rowcount > 0


def _resolve_env(value: str) -> str:
    """Resolve ${VAR} template strings from the environment."""
    import re, os
    def _replacer(m):
        return os.environ.get(m.group(1), m.group(0))
    return re.sub(r'\$\{(\w+)\}', _replacer, value)


def load_provider_config(agent_id: str) -> dict:
    """Load provider config from profile config.yaml."""
    config_path = HERMES_HOME / 'profiles' / agent_id / 'config.yaml'
    if config_path.exists():
        try:
            with open(config_path) as f:
                cfg = yaml.safe_load(f) or {}
            provider_name = cfg.get('model', {}).get('provider', '')
            if provider_name:
                prov = cfg.get('providers', {}).get(provider_name, {})
                return {
                    'base_url': _resolve_env(prov.get('base_url', '')).rstrip('/'),
                    'api_key': _resolve_env(prov.get('api_key', '')),
                    'model': _resolve_env(prov.get('default_model', cfg.get('model', {}).get('default', 'deepseek-v4-flash-free'))),
                }
        except Exception:
            pass
    import os
    return {
        'base_url': os.environ.get('OPENCODE_BASE_URL', 'https://opencode.ai/zen/v1').rstrip('/'),
        'api_key': os.environ.get('OPENCODE_GO_API_KEY', ''),
        'model': os.environ.get('AMB_LLM_MODEL', 'deepseek-v4-flash-free'),
    }


def load_soul_persona(agent_id: str) -> str:
    """Load agent persona from SOUL.md."""
    soul_path = HERMES_HOME / 'profiles' / agent_id / 'SOUL.md'
    if not soul_path.exists():
        return f'Te vagy {agent_id}, a Hermes rendszer agense.'
    try:
        text = soul_path.read_text(encoding='utf-8')
        role_match = re.search(r'## Szerep\\n(.+?)(?:\\n|$)', text)
        if role_match:
            return f'Te vagy {agent_id}. {role_match.group(1).strip()}'
    except Exception:
        pass
    return f'Te vagy {agent_id}, a Hermes rendszer agense.'


# ── Core logika ────────────────────────────────────────────────────────────────

def should_auto_reply(msg: dict, agent_id: str) -> tuple[bool, str]:
    """Ellenőrzi, hogy egy üzenetre automatikusan válaszolhatunk-e.

    Args:
        msg: Az üzenet dict (from get_pending_messages)
        agent_id: A saját ágensünk neve (pl. 'dev', 'research')

    Returns:
        (True, "reason") ha válaszolhatunk
        (False, "reason") ha nem
    """
    msg_id = msg["id"]
    from_agent = msg["from_agent"]
    to_agent = msg["to_agent"]
    msg_type = msg.get("message_type") or "unknown"
    # Ha a message_type None (pl. MCP delegation nem állítja be), treat as task_delegation
    if msg.get("message_type") is None:
        msg_type = "task_delegation"
    content = msg.get("content", "") or ""
    chain_depth = msg.get("chain_depth") or 0
    is_auto_reply = msg.get("is_auto_reply") or 0

    # ── VÉDELEM 1: Soha ne válaszolj AUTO_REPLY típusra ──
    if msg_type == "auto_reply":
        return False, f"#{msg_id}: message_type=auto_reply → láncvédelem (Soha ne válaszolj auto_reply-re)"

    # ── VÉDELEM 2: is_auto_reply flag ──
    if is_auto_reply:
        return False, f"#{msg_id}: is_auto_reply=1 → láncvédelem"

    # ── VÉDELEM 3: Soha ne válaszolj a blacklist-en lévőknek ──
    if from_agent in SENDERS_BLACKLIST:
        return False, f"#{msg_id}: from_agent={from_agent} → blacklist"

    # ── VÉDELEM 4: Ne válaszolj magadnak ──
    if from_agent == agent_id:
        return False, f"#{msg_id}: from_agent={from_agent} → saját magadnak ne válaszolj"

    # ── VÉDELEM 5: Csak bizonyos típusokra ──
    if msg_type not in AUTO_REPLY_TYPES:
        return False, f"#{msg_id}: message_type={msg_type} → nem auto-reply típus (csak: {AUTO_REPLY_TYPES})"

    # ── VÉDELEM 6: Chain depth limit ──
    if chain_depth >= MAX_CHAIN_DEPTH:
        return False, f"#{msg_id}: chain_depth={chain_depth} >= MAX({MAX_CHAIN_DEPTH}) → max mélység"

    # ── VÉDELEM 7: Auto-responder üzenetek kiszűrése ──
    if content.startswith("📥") or content.startswith("📨") or content.startswith("🤖"):
        return False, f"#{msg_id}: auto-responder prefix → nem válaszolunk"

    # ── VÉDELEM 8: Rate limit ──
    if not _check_rate_limit(from_agent, to_agent):
        return False, f"#{msg_id}: rate limit exceeded for {from_agent}→{to_agent}"

    return True, f"#{msg_id}: OK → auto-reply engedélyezve (depth={chain_depth})"


def compute_new_depth(msg: dict) -> int:
    """Kiszámolja az új üzenet chain_depth értékét."""
    return (msg.get("chain_depth") or 0) + 1
