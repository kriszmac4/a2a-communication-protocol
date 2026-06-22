"""
AMB v7.2 Runtime — Core dispatch engine.

Claims messages, manages sessions, and launches or wakes agent processes
for the Agent Message Bus.
"""

import json
import logging
import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

# Ensure agent_message_bus module is importable
import sys as _sys
from pathlib import Path as _Path
_SCRIPTS_DIR = str(_Path(__file__).parent.parent)
if _SCRIPTS_DIR not in _sys.path:
    _sys.path.insert(0, _SCRIPTS_DIR)
from agent_message_bus import _get_db, DATA_DIR

logger = logging.getLogger("amb.runtime")

# ---------------------------------------------------------------------------
# Feature flags (checked at call time via os.environ)
# ---------------------------------------------------------------------------


def _is_enabled() -> bool:
    return os.environ.get("AMB_WAKEUP_ENABLED", "true").lower() == "true"


def _is_fast_path_enabled() -> bool:
    return os.environ.get("AMB_FAST_PATH_ENABLED", "true").lower() == "true"


def _is_legacy_tool_fallback() -> bool:
    return os.environ.get("AMB_ALLOW_LEGACY_TOOLSET_FALLBACK", "true").lower() == "true"


def _get_global_max_sessions() -> int:
    return int(os.environ.get("AMB_GLOBAL_MAX_CONCURRENT_SESSIONS", "3"))


def _get_per_agent_max_sessions() -> int:
    return int(os.environ.get("AMB_DEFAULT_PER_AGENT_MAX_CONCURRENT_SESSIONS", "1"))


def _get_batch_size() -> int:
    return int(os.environ.get("AMB_ROUTER_BATCH_SIZE", "1"))


def _is_batch_enabled() -> bool:
    return os.environ.get("AMB_BATCH_ROUTER_ENABLED", "false").lower() == "true"


# ---------------------------------------------------------------------------
# Core dispatch helpers
# ---------------------------------------------------------------------------


def claim_message(message_id: int, session_id: str) -> bool:
    """Atomically claim a message for *session_id*.

    Returns ``True`` if exactly one row was updated (i.e. the claim
    succeeded), ``False`` otherwise.
    """
    sql = """
        UPDATE agent_messages SET
            status = 'claimed',
            claimed_by_session = :session_id,
            claimed_at = datetime('now'),
            lock_owner = :session_id,
            lock_acquired_at = datetime('now'),
            lock_expires_at = datetime('now', '+10 minutes'),
            lock_version = COALESCE(lock_version, 0) + 1,
            updated_at = datetime('now')
        WHERE id = :message_id
          AND status IN ('pending', 'retry_scheduled')
          AND (next_attempt_at IS NULL OR next_attempt_at <= datetime('now'))
          AND (lock_owner IS NULL OR lock_expires_at IS NULL OR lock_expires_at < datetime('now'))
    """
    try:
        conn = _get_db()
        cursor = conn.execute(sql, {"message_id": message_id, "session_id": session_id})
        conn.commit()
        return cursor.rowcount == 1
    except Exception:
        logger.exception("claim_message failed for message_id=%s", message_id)
        return False


def select_eligible_messages(batch_size: int = 1) -> list[int]:
    """Return up to *batch_size* message IDs that are currently claimable."""
    sql = """
        SELECT id FROM agent_messages
        WHERE status IN ('pending', 'retry_scheduled')
          AND (next_attempt_at IS NULL OR next_attempt_at <= datetime('now'))
          AND (lock_owner IS NULL OR lock_expires_at IS NULL OR lock_expires_at < datetime('now'))
        ORDER BY priority DESC, created_at ASC
        LIMIT :batch_size
    """
    try:
        conn = _get_db()
        rows = conn.execute(sql, {"batch_size": batch_size}).fetchall()
        return [row["id"] for row in rows]
    except Exception:
        logger.exception("select_eligible_messages failed")
        return []


def can_dispatch_agent(agent_name: str) -> bool:
    """Check global and per-agent concurrency limits.

    Returns ``True`` if both limits are satisfied.
    """
    try:
        conn = _get_db()
        per_agent_max = _get_per_agent_max_sessions()
        global_max = _get_global_max_sessions()

        agent_count = conn.execute(
            "SELECT COUNT(*) FROM agent_sessions WHERE agent_name = :agent AND status IN ('starting', 'busy')",
            {"agent": agent_name},
        ).fetchone()[0]
        if agent_count >= per_agent_max:
            logger.info("Per-agent limit reached for %s (%d >= %d)", agent_name, agent_count, per_agent_max)
            return False

        global_count = conn.execute(
            "SELECT COUNT(*) FROM agent_sessions WHERE status IN ('starting', 'busy')"
        ).fetchone()[0]
        if global_count >= global_max:
            logger.info("Global session limit reached (%d >= %d)", global_count, global_max)
            return False

        return True
    except Exception:
        logger.exception("can_dispatch_agent failed for agent=%s", agent_name)
        return False


def find_idle_session(agent_name: str) -> dict | None:
    """Return the most-recently-seen session for *agent_name*, or ``None``.

    Stale sessions (dead PID) are marked as ``'stale'`` and skipped.
    """
    sql = """
        SELECT * FROM agent_sessions
        WHERE agent_name = :agent AND status IN ('idle', 'busy', 'starting')
        ORDER BY last_seen_at DESC LIMIT 1
    """
    try:
        conn = _get_db()
        row = conn.execute(sql, {"agent": agent_name}).fetchone()
        if row is None:
            return None

        session = dict(row)
        pid = session.get("pid")
        if pid is not None:
            try:
                os.kill(pid, 0)
            except OSError:
                conn.execute(
                    "UPDATE agent_sessions SET status = 'stale', updated_at = datetime('now') WHERE session_id = :sid",
                    {"sid": session["session_id"]},
                )
                conn.commit()
                return None

        return session
    except Exception:
        logger.exception("find_idle_session failed for agent=%s", agent_name)
        return None


def create_message_attempt(
    message_id: int,
    agent_name: str,
    session_id: str,
    dispatch_mode: str = "cold_popen",
) -> int:
    """Insert a new row into ``agent_message_attempts`` and return its id."""
    sql = """
        INSERT INTO agent_message_attempts (
            message_id, attempt_no, agent_name, session_id,
            runtime_status, dispatch_mode, started_at,
            stdout_path, stderr_path, created_at, updated_at
        )
        SELECT
            :message_id,
            COALESCE(MAX(attempt_no), 0) + 1,
            :agent_name,
            :session_id,
            'created',
            :dispatch_mode,
            datetime('now'),
            :stdout_path,
            :stderr_path,
            datetime('now'),
            datetime('now')
        FROM agent_message_attempts WHERE message_id = :message_id2
    """
    stdout_path = f"/tmp/amb_{session_id}_stdout.log"
    stderr_path = f"/tmp/amb_{session_id}_stderr.log"
    params = {
        "message_id": message_id,
        "message_id2": message_id,
        "agent_name": agent_name,
        "session_id": session_id,
        "dispatch_mode": dispatch_mode,
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
    }
    try:
        conn = _get_db()
        conn.execute(sql, params)
        conn.commit()
        return conn.execute(
            "SELECT MAX(id) FROM agent_message_attempts WHERE message_id = :mid",
            {"mid": message_id},
        ).fetchone()[0]
    except Exception:
        logger.exception("create_message_attempt failed for message_id=%s", message_id)
        raise


def mark_message_running(message_id: int, session_id: str, attempt_id: int) -> None:
    """Transition message and attempt to ``running`` status."""
    try:
        conn = _get_db()
        conn.execute(
            "UPDATE agent_messages SET status = 'running', updated_at = datetime('now') WHERE id = :mid AND lock_owner = :sid",
            {"mid": message_id, "sid": session_id},
        )
        conn.execute(
            "UPDATE agent_message_attempts SET runtime_status = 'running', started_at = datetime('now'), updated_at = datetime('now') WHERE id = :aid",
            {"aid": attempt_id},
        )
        conn.commit()
    except Exception:
        logger.exception("mark_message_running failed for message_id=%s", message_id)


def assign_message_to_session(message_id: int, session_id: str, attempt_id: int) -> bool:
    """Atomically assign a claimed message to an active session (fast path).

    Returns ``True`` if the message was still in ``claimed`` state and was
    assigned successfully.
    """
    try:
        conn = _get_db()
        cursor = conn.execute(
            "UPDATE agent_messages SET status = 'running', lock_owner = :sid, updated_at = datetime('now') WHERE id = :mid AND status = 'claimed'",
            {"mid": message_id, "sid": session_id},
        )
        if cursor.rowcount == 0:
            conn.commit()
            return False

        conn.execute(
            "UPDATE agent_sessions SET current_message_id = :mid, current_attempt_id = :aid, status = 'busy', updated_at = datetime('now') WHERE session_id = :sid",
            {"mid": message_id, "aid": attempt_id, "sid": session_id},
        )
        conn.commit()
        return True
    except Exception:
        logger.exception("assign_message_to_session failed for message_id=%s", message_id)
        return False


def trigger_active_session(session_id: str, message_id: int) -> bool:
    """Write a wakeup file that an active Hermes session will pick up.

    Returns ``True`` if the file was written successfully.
    """
    try:
        payload = {
            "message_id": message_id,
            "session_id": session_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        wakeup_path = DATA_DIR / f"wakeup_{session_id}.json"
        wakeup_path.write_text(json.dumps(payload))
        return True
    except Exception:
        logger.exception("trigger_active_session failed for session_id=%s", session_id)
        return False


def launch_hermes_session(
    message: dict,
    attempt: dict,
    config: dict = None,
) -> dict:
    """Launch a new Hermes subprocess for *message* using *attempt* metadata.

    Builds the command line, opens log files, sets environment variables,
    and starts the process with ``subprocess.Popen``.

    Returns a dict with ``pid``, ``session_id``, ``message_id``,
    ``stdout_path``, and ``stderr_path``.
    """
    config = config or {}
    hermes_bin = os.environ.get("HERMES_BIN", "hermes")
    toolsets = get_agent_toolsets(config, message["to_agent"])

    query = (
        f"[A2A WAKEUP]\n"
        f"Agent: {message['to_agent']}\n"
        f"Message ID: {message['id']}\n"
        f"From: {message['from_agent']}\n"
        f"Task: {message['content']}\n"
        f"\n"
        f"You have been started by AMB Runtime. Process this assigned message. "
        f"Use your tools as needed. When complete, call respond_to_message()."
    )

    stdout_path = f"/tmp/amb_{attempt['session_id']}_stdout.log"
    stderr_path = f"/tmp/amb_{attempt['session_id']}_stderr.log"

    env = os.environ.copy()
    env["AMB_WAKEUP_SESSION_ID"] = attempt["session_id"]
    env["AMB_TARGET_AGENT"] = message["to_agent"]
    env["AMB_MESSAGE_ID"] = str(message["id"])
    env["AMB_ATTEMPT_ID"] = str(attempt["id"])

    try:
        stdout_f = open(stdout_path, "w")
        stderr_f = open(stderr_path, "w")
    except OSError:
        logger.exception("Failed to open log files for session %s", attempt["session_id"])
        raise

    try:
        proc = subprocess.Popen(
            [
                hermes_bin,
                "chat",
                "--query",
                query,
                "--quiet",
                "--skills",
                "a2a-communication-protocol",
                "--toolsets",
                ",".join(toolsets),
            ],
            stdout=stdout_f,
            stderr=stderr_f,
            env=env,
        )
    except Exception:
        stdout_f.close()
        stderr_f.close()
        logger.exception("Failed to launch hermes process for session %s", attempt["session_id"])
        raise

    update_session_pid(attempt["session_id"], proc.pid)

    return {
        "pid": proc.pid,
        "session_id": attempt["session_id"],
        "message_id": message["id"],
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
    }


def get_agent_toolsets(config: dict, agent_name: str) -> list[str]:
    """Resolve the toolset list for *agent_name* from *config*.

    Lookup order:

    1. ``amb_tool_policy.agent_toolsets[agent_name]`` — agent-specific list.
    2. Legacy fallback (when ``AMB_ALLOW_LEGACY_TOOLSET_FALLBACK`` is
       enabled) — ``config.get('toolsets', ['mcp', 'terminal', 'file'])``.
    3. Raise ``RuntimeError`` when no toolset is configured and the legacy
       fallback is disabled.
    """
    policy = config.get("amb_tool_policy", {})
    agent_toolsets = policy.get("agent_toolsets", {})

    if agent_name in agent_toolsets:
        return agent_toolsets[agent_name]

    if _is_legacy_tool_fallback():
        return config.get("toolsets", ["mcp", "terminal", "file"])

    raise RuntimeError(
        f"No toolset configured for agent '{agent_name}' "
        f"and legacy fallback is disabled."
    )


def update_session_pid(session_id: str, pid: int) -> None:
    """Record the OS PID for *session_id* and mark it as ``starting``."""
    try:
        conn = _get_db()
        conn.execute(
            "UPDATE agent_sessions SET pid = :pid, status = 'starting', updated_at = datetime('now') WHERE session_id = :sid",
            {"pid": pid, "sid": session_id},
        )
        conn.commit()
    except Exception:
        logger.exception("update_session_pid failed for session_id=%s", session_id)


def create_session_record(session_id: str, agent_name: str) -> None:
    """Insert a new session row with ``starting`` status."""
    try:
        conn = _get_db()
        conn.execute(
            "INSERT INTO agent_sessions (session_id, agent_name, status, started_at) VALUES (:sid, :agent, 'starting', datetime('now'))",
            {"sid": session_id, "agent": agent_name},
        )
        conn.commit()
    except Exception:
        logger.exception("create_session_record failed for session_id=%s", session_id)


def message_locked_by_session(message_id: int, session_id: str) -> bool:
    """Check whether *message_id* is currently locked by *session_id*."""
    sql = "SELECT 1 FROM agent_messages WHERE id = :mid AND lock_owner = :sid AND status IN ('claimed', 'running')"
    try:
        conn = _get_db()
        return conn.execute(sql, {"mid": message_id, "sid": session_id}).fetchone() is not None
    except Exception:
        logger.exception("message_locked_by_session failed for message_id=%s", message_id)
        return False


def get_next_attempt_no(message_id: int) -> int:
    """Return the next attempt number (1-based) for *message_id*."""
    try:
        conn = _get_db()
        result = conn.execute(
            "SELECT COALESCE(MAX(attempt_no), 0) + 1 FROM agent_message_attempts WHERE message_id = :mid",
            {"mid": message_id},
        ).fetchone()[0]
        return result
    except Exception:
        logger.exception("get_next_attempt_no failed for message_id=%s", message_id)
        return 1
