#!/usr/bin/env python3
"""
Agent Message Bus — A2A Communication Protocol Core Module

Three systems:
1. Agent Message Bus (inter-agent communication)
2. Gradual Autonomy (heartbeat + trust levels)
3. Dream Engine (nightly consolidation)

Data directory: configurable via AMB_DATA_DIR env var (default: ~/.a2a-protocol/)
"""

import json
import logging
import os
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from agent_message_bus.permissions import PermissionError, check_permission

logger = logging.getLogger("amb")

# --- Paths ---
# Standalone: use AMB_DATA_DIR or default to ~/.a2a-protocol/
_AMB_DATA_DIR = Path(os.environ.get("AMB_DATA_DIR", Path.home() / ".a2a-protocol"))
DATA_DIR = _AMB_DATA_DIR
DREAMS_DIR = DATA_DIR / "dreams"
MESSAGES_DB = DATA_DIR / "agent_messages.db"
AUTONOMY_CONFIG = DATA_DIR / "autonomy-config.json"
AGENT_CARDS_DIR = DATA_DIR / "agent_cards"
SKILL_REGISTRY = DATA_DIR / "skill_registry.json"

# Thread-local DB connections
_local = threading.local()


# =============================================================================
# DB LAYER (Agent Messages)
# =============================================================================

def _get_db() -> sqlite3.Connection:
    """Get thread-local SQLite connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        DREAMS_DIR.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(MESSAGES_DB))
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        _init_db(conn)
        _local.conn = conn
    return _local.conn


def close_db():
    """Close thread-local connection."""
    if hasattr(_local, "conn") and _local.conn:
        _local.conn.close()
        _local.conn = None


def _init_db(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS agent_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            from_agent TEXT NOT NULL,
            to_agent TEXT NOT NULL,
            content TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending'
                CHECK(status IN ('pending','delivered','done','failed','read','dead','expired')),
            result TEXT,
            priority INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            delivered_at REAL,
            completed_at REAL,
            discord_message_id TEXT,
            discord_thread_id TEXT,
            message_type TEXT,
            idempotency_key TEXT,
            correlation_id INTEGER,
            parent_message_id INTEGER,
            expires_at REAL,
            retry_count INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3
        );
        CREATE INDEX IF NOT EXISTS idx_agent_messages_status
            ON agent_messages(status, to_agent);
        CREATE INDEX IF NOT EXISTS idx_agent_messages_created
            ON agent_messages(created_at);
    """)
    # ── Schema migration: add missing columns for existing DBs ──
    try:
        cur = conn.execute("PRAGMA table_info(agent_messages)")
        existing = {row[1] for row in cur.fetchall()}
        for col, dtype in [
            ("discord_message_id", "TEXT"),
            ("discord_thread_id", "TEXT"),
            ("message_type", "TEXT"),
            ("idempotency_key", "TEXT"),
            ("correlation_id", "INTEGER"),
            ("parent_message_id", "INTEGER"),
            ("expires_at", "REAL"),
            ("retry_count", "INTEGER NOT NULL DEFAULT 0"),
            ("max_retries", "INTEGER NOT NULL DEFAULT 3"),
            ("chain_depth", "INTEGER NOT NULL DEFAULT 0"),
            ("reply_to", "INTEGER"),
            ("is_auto_reply", "INTEGER NOT NULL DEFAULT 0"),
        ]:
            if col not in existing:
                conn.execute(f"ALTER TABLE agent_messages ADD COLUMN {col} {dtype}")
                logger.info(f"Schema migration: added column {col}")
        # Ensure idempotency unique index exists
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency "
            "ON agent_messages(idempotency_key) WHERE idempotency_key IS NOT NULL"
        )
    except Exception as e:
        logger.warning(f"Schema migration error: {e}")

    # ── CHECK constraint migration for existing DBs ──
    try:
        cur = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='agent_messages'"
        )
        row = cur.fetchone()
        if row and row[0]:
            create_sql = row[0]
            needs_check_update = (
                "'dead'" not in create_sql or "'expired'" not in create_sql
            )
            if needs_check_update:
                logger.info("Schema migration: updating CHECK constraint to include 'dead' and 'expired'")
                conn.execute("BEGIN")
                try:
                    conn.execute("""
                        CREATE TABLE agent_messages_new (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            from_agent TEXT NOT NULL,
                            to_agent TEXT NOT NULL,
                            content TEXT NOT NULL,
                            status TEXT NOT NULL DEFAULT 'pending'
                                CHECK(status IN ('pending','delivered','done','failed','read','dead','expired')),
                            result TEXT,
                            priority INTEGER NOT NULL DEFAULT 0,
                            created_at REAL NOT NULL,
                            delivered_at REAL,
                            completed_at REAL,
                            discord_message_id TEXT,
                            discord_thread_id TEXT,
                            message_type TEXT,
                            idempotency_key TEXT,
                            correlation_id INTEGER,
                            parent_message_id INTEGER,
                            expires_at REAL,
                            retry_count INTEGER NOT NULL DEFAULT 0
                        )
                    """)
                    conn.execute("INSERT INTO agent_messages_new SELECT * FROM agent_messages")
                    conn.execute("DROP TABLE agent_messages")
                    conn.execute("ALTER TABLE agent_messages_new RENAME TO agent_messages")
                    # Recreate indexes on the new table
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_agent_messages_status "
                        "ON agent_messages(status, to_agent)"
                    )
                    conn.execute(
                        "CREATE INDEX IF NOT EXISTS idx_agent_messages_created "
                        "ON agent_messages(created_at)"
                    )
                    conn.execute(
                        "CREATE UNIQUE INDEX IF NOT EXISTS idx_idempotency "
                        "ON agent_messages(idempotency_key) WHERE idempotency_key IS NOT NULL"
                    )
                    conn.execute("COMMIT")
                    logger.info("Schema migration: CHECK constraint updated successfully")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
    except Exception as e:
        logger.warning(f"CHECK constraint migration error: {e}")


def create_message(from_agent: str, to_agent: str, content: str,
                   priority: int = 0) -> dict:
    conn = _get_db()
    now = time.time()
    cur = conn.execute(
        "INSERT INTO agent_messages (from_agent, to_agent, content, status, priority, created_at) "
        "VALUES (?, ?, ?, 'pending', ?, ?)",
        (from_agent, to_agent, content, priority, now)
    )
    conn.commit()
    msg_id = cur.lastrowid

    # ── Push notification: trigger real-time notification to target agent ──
    try:
        import subprocess as _sp
        import sys as _sys
        notify_script = Path(__file__).parent / "notify_target.py"
        if notify_script.exists():
            preview = (content or "")[:120].replace("\n", " ")
            _sp.Popen(
                [_sys.executable, str(notify_script),
                 to_agent, from_agent, str(msg_id), str(priority), preview],
                stdout=_sp.DEVNULL,
                stderr=open(str(DATA_DIR / "notify_target.log"), "a"),
            )
    except Exception:
        pass  # Non-blocking — never let notify failure affect message delivery

    return {
        "id": msg_id,
        "from_agent": from_agent,
        "to_agent": to_agent,
        "content": content,
        "status": "pending",
        "priority": priority,
        "created_at": now,
        "message_type": None,
        "idempotency_key": None,
        "correlation_id": None,
        "parent_message_id": None,
        "expires_at": None,
        "max_retries": 3,
        "retry_count": 0,
    }


def get_pending_messages(to_agent: Optional[str] = None,
                         limit: int = 50) -> list[dict]:
    conn = _get_db()
    if to_agent:
        rows = conn.execute(
            "SELECT * FROM agent_messages "
            "WHERE status = 'pending' AND to_agent = ? "
            "ORDER BY priority DESC, created_at ASC LIMIT ?",
            (to_agent, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM agent_messages "
            "WHERE status = 'pending' "
            "ORDER BY priority DESC, created_at ASC LIMIT ?",
            (limit,)
        ).fetchall()
    return [dict(r) for r in rows]


def get_messages(from_agent: Optional[str] = None,
                 to_agent: Optional[str] = None,
                 status: Optional[str] = None,
                 limit: int = 50) -> list[dict]:
    conn = _get_db()
    clauses = []
    params = []
    if from_agent:
        clauses.append("from_agent = ?")
        params.append(from_agent)
    if to_agent:
        clauses.append("to_agent = ?")
        params.append(to_agent)
    if status:
        clauses.append("status = ?")
        params.append(status)
    where = " AND ".join(clauses) if clauses else "1=1"
    rows = conn.execute(
        f"SELECT * FROM agent_messages WHERE {where} "
        "ORDER BY created_at DESC LIMIT ?",
        (*params, limit)
    ).fetchall()
    return [dict(r) for r in rows]


def mark_delivered(msg_id: int) -> bool:
    conn = _get_db()
    now = time.time()
    cur = conn.execute(
        "UPDATE agent_messages SET status = 'delivered', delivered_at = ? "
        "WHERE id = ? AND status = 'pending'",
        (now, msg_id)
    )
    conn.commit()
    return cur.rowcount > 0


def mark_read(msg_id: int) -> bool:
    conn = _get_db()
    cur = conn.execute(
        "UPDATE agent_messages SET status = 'read' "
        "WHERE id = ? AND status IN ('delivered', 'pending')",
        (msg_id,)
    )
    conn.commit()
    return cur.rowcount > 0


def mark_done(msg_id: int, result: str = "") -> bool:
    conn = _get_db()
    now = time.time()
    cur = conn.execute(
        "UPDATE agent_messages SET status = 'done', result = ?, completed_at = ? "
        "WHERE id = ? AND status IN ('pending','delivered')",
        (result, now, msg_id)
    )
    conn.commit()
    return cur.rowcount > 0


def mark_failed(msg_id: int, error: str = "") -> bool:
    """Mark a message as failed with retry-aware logic and DLQ."""
    conn = _get_db()
    now = time.time()

    # Get current retry state
    row = conn.execute(
        "SELECT retry_count, max_retries, status FROM agent_messages WHERE id = ?",
        (msg_id,)
    ).fetchone()
    if not row or row["status"] != "pending":
        return False

    retry_count = row["retry_count"]
    max_retries = row["max_retries"] or 3
    new_retry_count = retry_count + 1

    if new_retry_count >= max_retries:
        # Max retries reached → dead letter queue
        cur = conn.execute(
            "UPDATE agent_messages SET status = 'dead', result = ?, completed_at = ?, "
            "retry_count = ? WHERE id = ? AND status = 'pending'",
            (error, now, new_retry_count, msg_id)
        )
        conn.commit()
        if cur.rowcount > 0:
            logger.warning(
                f"🕊️ DLQ: msg #{msg_id} moved to 'dead' "
                f"(retried {retry_count}/{max_retries}): {error[:100]}"
            )
        return cur.rowcount > 0
    else:
        # Increment retry and keep as failed (watchdog will retry)
        cur = conn.execute(
            "UPDATE agent_messages SET status = 'failed', result = ?, completed_at = ?, "
            "retry_count = ? WHERE id = ? AND status = 'pending'",
            (error, now, new_retry_count, msg_id)
        )
        conn.commit()
        if cur.rowcount > 0:
            logger.info(
                f"🔁 Retry #{new_retry_count}/{max_retries} for msg #{msg_id}: {error[:60]}"
            )
        return cur.rowcount > 0


def cleanup_old_messages(hours: int = 72):
    """Delete messages older than `hours` that are done/failed."""
    conn = _get_db()
    cutoff = time.time() - (hours * 3600)
    conn.execute(
        "DELETE FROM agent_messages WHERE status IN ('done','failed','read') "
        "AND created_at < ?", (cutoff,)
    )
    conn.commit()


# =============================================================================
# TYPED MESSAGE FUNCTIONS (v2.0 — production hardening)
# =============================================================================

# In-memory token bucket for sender rate limiting
_token_buckets: dict[tuple[str, str], tuple[float, float]] = {}
_token_lock = threading.Lock()

_RATE_LIMIT_MAX = 5       # max 5 messages
_RATE_LIMIT_WINDOW = 60   # per 60 seconds
_RATE_LIMIT_BURST = 2     # extra burst allowance


def create_typed_message(
    from_agent: str,
    to_agent: str,
    content: str,
    message_type: str,
    priority: int = 0,
    idempotency_key: Optional[str] = None,
    correlation_id: Optional[int] = None,
    parent_message_id: Optional[int] = None,
    expires_at: Optional[float] = None,
    max_retries: int = 3,
    chain_depth: int = 0,
    reply_to: Optional[int] = None,
    is_auto_reply: int = 0,
) -> dict:
    """Típusos üzenet létrehozása Pydantic validációval és produkciós hardeninggel.

    Validálja a SendBusMessage sémát, majd beszúrja az agent_messages táblába
    az összes új oszloppal (message_type, idempotency_key, correlation_id,
    parent_message_id, expires_at, retry_count). Idempotencia: ha az
    idempotency_key már létezik, a meglévő üzenetet adja vissza hiba nélkül.

    Returns a dict with id, from_agent, to_agent, content, status, priority,
    created_at, message_type, idempotency_key, correlation_id, parent_message_id,
    expires_at — same shape as create_message() but with typed extras.
    """
    from agent_message_bus.schemas import SendBusMessage, MessageType

    # Auto-generate idempotency_key if not provided
    if idempotency_key is None:
        import uuid as _uuid
        idempotency_key = str(_uuid.uuid4())

    # Parse payload from content string; validate through Pydantic
    try:
        payload_dict = json.loads(content)
    except Exception:
        payload_dict = {"summary": content[:120], "body": content}

    msg = SendBusMessage(
        target_agent_id=to_agent,
        message_type=message_type,
        payload=payload_dict,
        priority=priority,
        idempotency_key=idempotency_key,
        correlation_id=correlation_id,
        parent_message_id=parent_message_id,
        expires_at=expires_at,
        max_retries=max_retries,
        chain_depth=chain_depth,
        reply_to=reply_to,
    )

    conn = _get_db()
    now = time.time()
    msg_type_val = msg.message_type.value if isinstance(msg.message_type, MessageType) else str(msg.message_type)

    # AuthZ: check permission
    try:
        check_permission(from_agent, to_agent, message_type)
    except PermissionError as e:
        return {"error": str(e), "message_id": None}

    try:
        cur = conn.execute(
            "INSERT INTO agent_messages "
            "(from_agent, to_agent, content, status, priority, created_at, "
            "message_type, idempotency_key, correlation_id, parent_message_id, "
            "expires_at, retry_count, max_retries, chain_depth, reply_to, is_auto_reply) "
            "VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)",
            (
                from_agent,
                to_agent,
                content,
                priority,
                now,
                msg_type_val,
                msg.idempotency_key,
                msg.correlation_id,
                msg.parent_message_id,
                msg.expires_at,
                msg.max_retries,
                msg.chain_depth,
                msg.reply_to,
                is_auto_reply,
            ),
        )
        conn.commit()
        msg_id = cur.lastrowid
    except sqlite3.IntegrityError:
        # Duplicate idempotency_key — return existing message info instead of crashing
        existing = conn.execute(
            "SELECT id, from_agent, to_agent, content, status, priority, created_at "
            "FROM agent_messages WHERE idempotency_key = ?",
            (msg.idempotency_key,),
        ).fetchone()
        if existing:
            existing_dict = dict(existing)
            return {
                "id": existing_dict["id"],
                "from_agent": existing_dict["from_agent"],
                "to_agent": existing_dict["to_agent"],
                "content": existing_dict["content"],
                "status": "duplicate",
                "existing_status": existing_dict["status"],
                "priority": existing_dict["priority"],
                "created_at": existing_dict["created_at"],
                "message_type": existing_dict.get("message_type"),
                "idempotency_key": msg.idempotency_key,
                "correlation_id": existing_dict.get("correlation_id"),
                "parent_message_id": existing_dict.get("parent_message_id"),
                "expires_at": existing_dict.get("expires_at"),
                "max_retries": existing_dict.get("max_retries", 3),
                "retry_count": existing_dict.get("retry_count", 0),
            }
        raise

    # If correlation_id is None, set it to the new message's own ID (root of chain)
    if msg.correlation_id is None:
        conn.execute(
            "UPDATE agent_messages SET correlation_id = ? WHERE id = ?",
            (msg_id, msg_id),
        )
        conn.commit()

    # ── Push notification: trigger real-time notification to target agent ──
    try:
        import subprocess as _sp
        import sys as _sys
        notify_script = Path(__file__).parent / "notify_target.py"
        if notify_script.exists():
            preview = (content or "")[:120].replace("\n", " ")
            _sp.Popen(
                [
                    _sys.executable,
                    str(notify_script),
                    to_agent,
                    from_agent,
                    str(msg_id),
                    str(priority),
                    preview,
                ],
                stdout=_sp.DEVNULL,
                stderr=open(str(DATA_DIR / "notify_target.log"), "a"),
            )
    except Exception:
        pass  # Non-blocking — never let notify failure affect message delivery

    return {
        "id": msg_id,
        "from_agent": from_agent,
        "to_agent": to_agent,
        "content": content,
        "status": "pending",
        "priority": priority,
        "created_at": now,
        "message_type": msg_type_val,
        "idempotency_key": msg.idempotency_key,
        "correlation_id": msg_id if correlation_id is None else correlation_id,
        "parent_message_id": parent_message_id,
        "expires_at": msg.expires_at,
        "max_retries": msg.max_retries,
        "retry_count": 0,
        "chain_depth": msg.chain_depth,
        "reply_to": msg.reply_to,
        "is_auto_reply": is_auto_reply,
    }


def mark_dead(msg_id: int, error: str = "") -> bool:
    """3 retry után dead státuszba (DLQ) teszi az üzenetet.

    Dead Letter Queue: az üzenet státusza 'dead'-ra vált, és az eredmény
    mezőbe '[DLQ]' prefix-szel kerül a hibaüzenet. Csak 'pending' vagy
    'delivered' státuszú üzeneteket lehet dead-re állítani.

    Returns True if the update succeeded (row was found and updated).
    """
    conn = _get_db()
    now = time.time()
    cur = conn.execute(
        "UPDATE agent_messages SET status = 'dead', result = ?, completed_at = ? "
        "WHERE id = ? AND status IN ('pending', 'delivered')",
        (f"[DLQ] {error}" if error else "[DLQ]", now, msg_id),
    )
    conn.commit()
    return cur.rowcount > 0


def get_message_tree(correlation_id: int) -> list[dict]:
    """Visszaadja a teljes üzenetláncot (causal trace tree) egy correlation_id alapján.

    Lekérdezi az összes üzenetet, amelynek correlation_id-ja megegyezik a megadottal,
    VAGY amelynek az ID-ja maga a correlation_id (a gyökér üzenet).
    Az eredmény időrendben (created_at ASC) rendezett.

    Returns a list of message dicts forming the complete conversation chain.
    """
    conn = _get_db()
    rows = conn.execute(
        "SELECT * FROM agent_messages WHERE correlation_id = ? OR id = ? "
        "ORDER BY created_at ASC",
        (correlation_id, correlation_id),
    ).fetchall()
    return [dict(r) for r in rows]


def check_sender_rate_limit(from_agent: str, to_agent: str) -> bool:
    """Token bucket alapú sender-side rate limiting (in-memory, thread-safe).

    Konfiguráció: max 5 üzenet / 60 másodperc / (sender, target) pár.
    Burst allowance: +2 extra token a csúcsforgalomhoz.
    Token refill minden hívásnál az eltelt idő alapján arányosan.
    Thread-safe: a _token_lock biztosítja az atomicitást.

    Returns True ha a küldés engedélyezett, False ha rate limit elérve.
    """
    key = (from_agent, to_agent)
    now = time.time()

    with _token_lock:
        last_refill, tokens = _token_buckets.get(
            key, (now, float(_RATE_LIMIT_MAX + _RATE_LIMIT_BURST))
        )

        # Token refill based on elapsed time (proportional)
        elapsed = now - last_refill
        refill = (elapsed / _RATE_LIMIT_WINDOW) * _RATE_LIMIT_MAX
        tokens = min(tokens + refill, float(_RATE_LIMIT_MAX + _RATE_LIMIT_BURST))

        if tokens >= 1.0:
            _token_buckets[key] = (now, tokens - 1.0)
            return True
        else:
            # Update timestamp even when denied to prevent time-warp refill spam
            _token_buckets[key] = (now, tokens)
            return False


# =============================================================================
# AUTONOMY CONFIG
# =============================================================================

DEFAULT_AUTONOMY_CATEGORIES = [
    {"key": "kanban_archive_done",  "label": "Kanban archiválás",       "level": 3, "locked": False, "maxLevel": 3},
    {"key": "file_read",            "label": "Fájl olvasás",            "level": 3, "locked": False, "maxLevel": 3},
    {"key": "file_write",           "label": "Fájl írás/módosítás",     "level": 2, "locked": False, "maxLevel": 3},
    {"key": "git_push",             "label": "Git push",                "level": 1, "locked": False, "maxLevel": 2},
    {"key": "git_force_push",       "label": "Git force push",          "level": 1, "locked": True,  "maxLevel": 1},
    {"key": "email_send",           "label": "Email küldés",            "level": 1, "locked": True,  "maxLevel": 1},
    {"key": "payment",              "label": "Pénzügyi művelet",        "level": 1, "locked": True,  "maxLevel": 1},
    {"key": "deployment",           "label": "Deploy",                  "level": 1, "locked": False, "maxLevel": 2},
    {"key": "research",             "label": "Kutatás/Web scraping",    "level": 3, "locked": False, "maxLevel": 3},
    {"key": "system_maintenance",   "label": "Rendszer karbantartás",   "level": 2, "locked": False, "maxLevel": 3},
    {"key": "cron_management",      "label": "Cron feladatok kezelése", "level": 2, "locked": False, "maxLevel": 3},
    {"key": "memory_write",         "label": "Memória írás",            "level": 3, "locked": False, "maxLevel": 3},
    {"key": "code_execution",       "label": "Kód futtatás",            "level": 2, "locked": False, "maxLevel": 2},
    {"key": "api_call",             "label": "API hívás (külső)",       "level": 2, "locked": False, "maxLevel": 2},
    {"key": "secret_access",        "label": "Titkok/API kulcsok",      "level": 1, "locked": True,  "maxLevel": 1},
]


def _load_autonomy_config() -> dict:
    """Load autonomy config, merging defaults with saved overrides."""
    if not AUTONOMY_CONFIG.exists():
        return _save_autonomy_config({"version": 1, "categories": DEFAULT_AUTONOMY_CATEGORIES})
    try:
        data = json.loads(AUTONOMY_CONFIG.read_text())
    except (json.JSONDecodeError, OSError):
        logger.warning("Corrupt autonomy config, re-initializing")
        return _save_autonomy_config({"version": 1, "categories": DEFAULT_AUTONOMY_CATEGORIES})

    # Merge defaults — add any new categories without overwriting levels
    existing = {c["key"]: c for c in data.get("categories", [])}
    merged = []
    for default_cat in DEFAULT_AUTONOMY_CATEGORIES:
        if default_cat["key"] in existing:
            saved = existing[default_cat["key"]]
            # Take saved level but enforce maxLevel
            saved_level = min(saved.get("level", default_cat["level"]),
                              default_cat["maxLevel"])
            merged.append({
                **default_cat,
                "level": saved_level,
                "locked": default_cat["locked"],  # always from defaults
            })
        else:
            merged.append(dict(default_cat))
    data["categories"] = merged
    data["version"] = 1
    return _save_autonomy_config(data)


def _save_autonomy_config(data: dict) -> dict:
    AUTONOMY_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    AUTONOMY_CONFIG.write_text(json.dumps(data, indent=2, ensure_ascii=False))
    return data


def get_autonomy_level(category_key: str) -> int:
    """Get the autonomy level for a category. Returns 1 if not found."""
    config = _load_autonomy_config()
    for cat in config.get("categories", []):
        if cat["key"] == category_key:
            return cat["level"]
    return 1


def get_all_autonomy_categories() -> list[dict]:
    config = _load_autonomy_config()
    return config.get("categories", [])


def set_autonomy_level(category_key: str, level: int) -> tuple[bool, str]:
    """Set the autonomy level for a category. Returns (success, message)."""
    config = _load_autonomy_config()
    for cat in config.get("categories", []):
        if cat["key"] == category_key:
            if cat.get("locked", False):
                return False, f"A '{cat['label']}' kategória zárolva, nem módosítható."
            max_lvl = cat.get("maxLevel", 3)
            if level < 1 or level > max_lvl:
                return False, f"A szint 1 és {max_lvl} között lehet (jelenleg: {level})."
            cat["level"] = level
            _save_autonomy_config(config)
            return True, f"'{cat['label']}' szint beállítva: {level}."
    return False, f"Nincs '{category_key}' kategória."


def classify_command(command: str) -> str:
    """Classify a shell command to determine which autonomy category applies."""
    cmd_lower = command.strip().lower()
    
    # Git operations
    if cmd_lower.startswith("git push --force") or cmd_lower.startswith("git push -f"):
        return "git_force_push"
    if cmd_lower.startswith("git push"):
        return "git_push"
    
    # File operations
    if any(cmd_lower.startswith(x) for x in ("rm ", "mv ", "cp ", "dd ", "mkfs", "format")):
        if any(flag in cmd_lower for flag in ("-rf", "-r", "-f", "--recursive")):
            return "system_maintenance"
        return "file_write"
    
    # Email
    if any(x in cmd_lower for x in ("sendmail", "mail ", "mutt ", "email")):
        return "email_send"
    
    # Deployment
    if any(x in cmd_lower for x in ("deploy", "kubectl", "helm ", "terraform", "cloudformation")):
        return "deployment"
    
    # Code execution
    if any(cmd_lower.startswith(x) for x in ("python", "node ", "npm ", "pip ", "cargo ", "go ")):
        return "code_execution"
    
    # API calls
    if any(x in cmd_lower for x in ("curl ", "wget ", "http ", "api")):
        return "api_call"
    
    # Default to file_read for most operations
    return "file_read"


# =============================================================================
# Agent Card Registry & Capability Discovery
# =============================================================================

def _load_all_agent_cards() -> dict[str, dict]:
    """Load all Agent Card JSON files from the registry directory.

    Returns a dict {agent_name: card_dict}. Missing/empty dir → empty dict.
    """
    if not AGENT_CARDS_DIR.exists():
        return {}
    cards: dict[str, dict] = {}
    for path in sorted(AGENT_CARDS_DIR.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Skipping invalid Agent Card {path.name}: {e}")
            continue
        name = data.get("name")
        if not name:
            continue
        cards[name] = data
    return cards


def _get_agent_card(name: str) -> dict | None:
    """Return a single Agent Card by name, or None."""
    return _load_all_agent_cards().get(name)


def _score_task_against_card(task: str, card: dict) -> tuple[float, str | None, str]:
    """Score a task description against an Agent Card.

    Returns (score, best_skill_id, reasoning).

    Scoring (simple, deterministic — no embeddings needed):
      +3.0  per keyword match (whole-word, case-insensitive)
      +1.0  if the keyword appears as a substring inside the task
      +1.5  if any token from skill.description appears in the task
      +0.5  per token overlap with card.description
      -2.0  if agent is the fallback (we prefer specialists when they match)
    """
    if not task:
        return 0.0, None, ""

    task_lower = task.lower()
    task_tokens = set(task_lower.split())

    best_score = 0.0
    best_skill_id: str | None = None
    best_skill_score = 0.0
    matched_keywords: list[str] = []

    for skill in card.get("skills", []):
        skill_id = skill.get("id", "")
        skill_desc = (skill.get("description", "")).lower()
        skill_score = 0.0

        # Keyword match
        for kw in skill.get("keywords", []):
            kw_lower = kw.lower()
            if not kw_lower:
                continue
            if kw_lower in task_tokens:
                skill_score += 3.0
                matched_keywords.append(kw)
            elif kw_lower in task_lower:
                skill_score += 1.0
                matched_keywords.append(kw)

        # Description overlap (looser)
        for word in skill_desc.split():
            w = word.strip(".,:;()")
            if len(w) > 3 and w in task_lower:
                skill_score += 1.5
                break

        if skill_score > best_skill_score:
            best_skill_score = skill_score
            best_skill_id = skill_id

        best_score += skill_score

    # Card-level description match
    card_desc = (card.get("description", "")).lower()
    for token in card_desc.split():
        w = token.strip(".,:;()")
        if len(w) > 4 and w in task_lower:
            best_score += 0.5

    # Fallback penalty
    if card.get("is_fallback"):
        best_score -= 2.0

    if matched_keywords:
        reasoning = f"matched: {', '.join(matched_keywords[:5])}"
    else:
        reasoning = "no keyword match"

    return best_score, best_skill_id, reasoning


def discover_agents(task: str, top_k: int = 3, min_score: float = 1.0) -> list[dict]:
    """Find the best-matching agents for a given task description.

    Returns a list of result dicts, sorted by score (descending):
        [
          {
            "agent": "dev",
            "display_name": "Dev Agent",
            "score": 7.5,
            "skill": "implement-feature",
            "reasoning": "matched: implementáld, build feature",
            "model": "DS-V4-Flash",
            "autonomy_level": 2
          },
          ...
        ]

    `min_score` filters out noise — if the best match is below threshold,
    returns the orchestrator as a safe default.
    """
    cards = _load_all_agent_cards()
    if not cards:
        return []

    results: list[dict] = []
    for name, card in cards.items():
        score, skill_id, reasoning = _score_task_against_card(task, card)
        results.append({
            "agent": name,
            "display_name": card.get("display_name", name),
            "score": round(score, 2),
            "skill": skill_id,
            "reasoning": reasoning,
            "model": card.get("model"),
            "autonomy_level": card.get("autonomy_level", 3),
            "is_fallback": card.get("is_fallback", False),
        })

    results.sort(key=lambda r: r["score"], reverse=True)

    # Filter below threshold; if nothing qualifies, fall back to orchestrator
    qualified = [r for r in results if r["score"] >= min_score]
    if not qualified:
        orch = next((r for r in results if r["agent"] == "orchestrator"), None)
        return [orch] if orch else results[:1]

    return qualified[:top_k]


def list_agent_cards() -> list[dict]:
    """Return minimal summaries of all registered Agent Cards."""
    cards = _load_all_agent_cards()
    out = []
    for name, card in cards.items():
        out.append({
            "agent": name,
            "display_name": card.get("display_name", name),
            "description": card.get("description", ""),
            "skills": [s.get("id") for s in card.get("skills", [])],
            "model": card.get("model"),
            "is_fallback": card.get("is_fallback", False),
        })
    return out


def record_skill_invocation(agent: str, skill: str, task_excerpt: str = "") -> None:
    """Append a skill invocation record to skill_registry.json.

    Used by Phase 3 router — feeds the skill-match learning loop.
    """
    SKILL_REGISTRY.parent.mkdir(parents=True, exist_ok=True)
    if SKILL_REGISTRY.exists():
        try:
            data = json.loads(SKILL_REGISTRY.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            data = {"version": 1, "invocations": []}
    else:
        data = {"version": 1, "invocations": []}

    data.setdefault("invocations", []).append({
        "ts": datetime.now(timezone.utc).isoformat(),
        "agent": agent,
        "skill": skill,
        "task_excerpt": task_excerpt[:200],
    })
    # Keep last 500 invocations
    data["invocations"] = data["invocations"][-500:]

    SKILL_REGISTRY.write_text(
        json.dumps(data, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# =============================================================================
# Discord thread management
# =============================================================================

# Base Hermes home (always the real root, not profile-overridden)
# HERMES_HOME and HOME can be overridden by the profile runtime, causing
# Path.home() to point to a nested wrong directory. Use the real user home
# from /etc/passwd to reliably find the root .hermes directory.
import pwd as _pwd
_USER_HOME = Path(_pwd.getpwuid(os.getuid()).pw_dir)
_HERMES_ROOT = _USER_HOME / ".hermes"

# Default home channels (used as fallback if profile config doesn't specify)
_DEFAULT_HOME_CHANNELS = {
    "dev":      "discord:1501148006219776002",
    "general":  "discord:1501148038117330995",
    "research": "discord:1501147842595655721",
    "study":    "discord:1501188232807972905",
    "ui":       "discord:1504006733826232521",
    "devops":   "discord:1501148016713793588",
}
_DEFAULT_CHANNEL_IDS = {k: v.split(":")[1] for k, v in _DEFAULT_HOME_CHANNELS.items() if ":" in v}


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
    return _DEFAULT_HOME_CHANNELS.get(agent)


def get_agent_channel_id(agent: str) -> str | None:
    """Get the pure Discord channel ID for an agent."""
    ch = get_agent_home_channel(agent)
    if ch and ":" in ch:
        return ch.split(":")[1]
    return _DEFAULT_CHANNEL_IDS.get(agent)

def open_message_thread(msg_id: int) -> bool:
    """
    Open a Discord thread under the notification for a bus message.

    Called automatically when an agent reads pending messages.
    Returns True if thread was opened successfully.
    """
    try:
        conn = _get_db()
        row = conn.execute(
            "SELECT to_agent, from_agent, content, discord_message_id, discord_thread_id "
            "FROM agent_messages WHERE id = ?",
            (msg_id,)
        ).fetchone()
        if not row:
            logger.warning(f"open_message_thread: #{msg_id} not found")
            return False
        to_agent = row["to_agent"]
        from_agent = row["from_agent"]
        content = row["content"] or ""
        discord_msg_id = row["discord_message_id"]
        existing_thread = row["discord_thread_id"]
        conn.close()
        _local.conn = None  # Reset thread-local so _get_db() creates fresh connection

        if not discord_msg_id:
            logger.warning(f"open_message_thread: #{msg_id} has no discord_message_id")
            return False
        if existing_thread:
            logger.info(f"open_message_thread: #{msg_id} already has thread {existing_thread}")
            return True  # Already has a thread

        # Find the channel ID for this agent
        channel_id = get_agent_channel_id(to_agent)
        if not channel_id:
            logger.warning(f"open_message_thread: no channel for {to_agent}")
            return False

        # Get Discord bot token from the target agent's profile .env
        profile_dir = _HERMES_ROOT / "profiles" / to_agent
        token = None
        env_file = profile_dir / ".env"
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if line.startswith("DISCORD_BOT_TOKEN="):
                    token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

        if not token:
            logger.warning(f"open_message_thread: no DISCORD_BOT_TOKEN for {to_agent}")
            return False

        # Build thread name
        thread_name = f"🧵 {from_agent}→{to_agent}"
        short = content[:60].replace("\n", " ").strip()
        if short:
            if len(content) > 60:
                short += "…"
            thread_name += f": {short}"
        thread_name = thread_name[:100]

        # Discord API: POST /channels/{channel_id}/messages/{message_id}/threads
        import urllib.request
        url = f"https://discord.com/api/v10/channels/{channel_id}/messages/{discord_msg_id}/threads"
        body = json.dumps({
            "name": thread_name,
            "auto_archive_duration": 1440,
        }).encode("utf-8")
        req = urllib.request.Request(
            url, data=body,
            headers={
                "Authorization": f"Bot {token}",
                "Content-Type": "application/json",
                "User-Agent": "MarveenBot/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp_data = json.loads(resp.read())
            thread_id = resp_data.get("id")
            if not thread_id:
                logger.warning(f"open_message_thread: Discord returned no id for #{msg_id}")
                return False

        # Save thread ID to DB
        conn2 = _get_db()
        conn2.execute(
            "UPDATE agent_messages SET discord_thread_id = ? WHERE id = ?",
            (thread_id, msg_id)
        )
        conn2.commit()
        conn2.close()
        _local.conn = None  # Reset thread-local so _get_db() creates fresh connection

        # Send first message in thread via hermes send
        try:
            import subprocess as _sp
            hermes_bin = _sp.run(["which", "hermes"], capture_output=True, text=True).stdout.strip()
            if not hermes_bin:
                hermes_bin = "hermes"
            env = __import__('os').environ.copy()
            env["HERMES_HOME"] = str(profile_dir)
            first_msg = (
                f"👋 **Elkezdtem dolgozni a feladaton!**\n"
                f"📤 **{from_agent}** kérése (#{msg_id})\n"
                f"> {content[:200]}"
            )
            _sp.run(
                [hermes_bin, "send", "--quiet",
                 f"discord:{channel_id}:{thread_id}", first_msg],
                capture_output=True, timeout=10, env=env,
            )
        except Exception:
            pass  # Non-critical

        logger.info(f"open_message_thread: #{msg_id} → thread={thread_id}")
        return True

    except Exception as e:
        logger.error(f"open_message_thread error for #{msg_id}: {e}")
        return False
