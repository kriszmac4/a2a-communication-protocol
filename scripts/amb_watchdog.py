"""
AMB v7.2 Watchdog Layer — Health monitoring and cleanup for the Agent Message Bus.

Periodic (or tick-driven) watchdog that detects dead processes, expired locks,
stale sessions, and timed-out tasks, and takes appropriate recovery actions.
"""

import logging
import os
import signal
import time
from datetime import datetime

# Ensure agent_message_bus module is importable
import sys as _sys
from pathlib import Path as _Path
_SCRIPTS_DIR = str(_Path(__file__).parent.parent)
if _SCRIPTS_DIR not in _sys.path:
    _sys.path.insert(0, _SCRIPTS_DIR)
from agent_message_bus import _get_db

logger = logging.getLogger("amb.watchdog")

# ---------------------------------------------------------------------------
# Feature flag
# ---------------------------------------------------------------------------


def _is_enabled() -> bool:
    return os.environ.get("AMB_WATCHDOG_ENABLED", "false").lower() == "true"


# ---------------------------------------------------------------------------
# Timeout defaults (from env, evaluated at import time)
# ---------------------------------------------------------------------------

STARTUP_TIMEOUT_SEC = int(os.environ.get("AMB_STARTUP_TIMEOUT", "60"))
TASK_TIMEOUT_SEC = int(os.environ.get("AMB_TASK_TIMEOUT", "300"))
HEARTBEAT_TIMEOUT_SEC = int(os.environ.get("AMB_HEARTBEAT_TIMEOUT", "120"))
LEASE_TIMEOUT_SEC = int(os.environ.get("AMB_LEASE_TIMEOUT", "600"))
ACTIVE_SESSION_STALE_SEC = int(os.environ.get("AMB_STALE_SESSION", "120"))


# ---------------------------------------------------------------------------
# Main watchdog tick
# ---------------------------------------------------------------------------


def watchdog_tick() -> dict:
    """Run one full watchdog cycle.

    Returns a summary dict with the count of actions taken in each category.
    """
    if not _is_enabled():
        return {"status": "disabled"}

    result = {
        "status": "ok",
        "finalized_exited": 0,
        "released_locks": 0,
        "dead_processes": 0,
        "task_timeouts": 0,
        "startup_timeouts": 0,
        "stale_sessions": 0,
    }

    try:
        result["finalized_exited"] = finalize_exited_attempts()
    except Exception:
        logger.exception("watchdog_tick: finalize_exited_attempts failed")

    try:
        result["released_locks"] = release_expired_locks()
    except Exception:
        logger.exception("watchdog_tick: release_expired_locks failed")

    try:
        result["dead_processes"] = detect_dead_processes()
    except Exception:
        logger.exception("watchdog_tick: detect_dead_processes failed")

    try:
        result["task_timeouts"] = detect_task_timeouts()
    except Exception:
        logger.exception("watchdog_tick: detect_task_timeouts failed")

    try:
        result["startup_timeouts"] = detect_startup_timeouts()
    except Exception:
        logger.exception("watchdog_tick: detect_startup_timeouts failed")

    try:
        result["stale_sessions"] = detect_stale_sessions()
    except Exception:
        logger.exception("watchdog_tick: detect_stale_sessions failed")

    return result


# ---------------------------------------------------------------------------
# Lock expiry
# ---------------------------------------------------------------------------


def release_expired_locks() -> int:
    """Release locks on messages whose lock_expires_at has passed.

    Only releases locks on messages in claimed or running status.
    Leaves responded, dead_letter, cancelled, and expired messages untouched.
    """
    conn = _get_db()
    cursor = conn.execute(
        """
        UPDATE agent_messages SET
          lock_owner = NULL, lock_acquired_at = NULL,
          lock_expires_at = NULL, updated_at = datetime('now')
        WHERE lock_expires_at IS NOT NULL AND lock_expires_at < datetime('now')
          AND status IN ('claimed', 'running')
        """
    )
    conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Finalize exited processes
# ---------------------------------------------------------------------------


def finalize_exited_attempts() -> int:
    """Iterate over in-flight attempts and finalize any whose process has exited.

    Returns the number of attempts that were transitioned to a final state.
    """
    conn = _get_db()
    rows = conn.execute(
        """
        SELECT a.id, a.message_id, a.session_id, a.runtime_status, a.exit_code,
               a.agent_name
        FROM agent_message_attempts a
        JOIN agent_sessions s ON s.session_id = a.session_id
        WHERE a.runtime_status IN ('created', 'running')
          AND s.pid IS NOT NULL
        """
    ).fetchall()

    finalized = 0
    for row in rows:
        result = finalize_attempt_if_process_exited(row["id"])
        if result is not None:
            finalized += 1
    return finalized


def finalize_attempt_if_process_exited(attempt_id: int) -> dict | None:
    """Check if the process for *attempt_id* has exited and finalize it.

    Returns ``None`` if the process is still alive or the attempt could not
    be loaded.  Otherwise returns a dict with the action taken:

    * ``{'action': 'completed'}`` — process exited 0, message has a response.
    * ``{'action': 'no_response'}`` — process exited 0 but no response given.
    * ``{'action': 'crashed'}`` — process exited non-zero without a response.
    """
    conn = _get_db()
    attempt = conn.execute(
        "SELECT * FROM agent_message_attempts WHERE id = :aid",
        {"aid": attempt_id},
    ).fetchone()
    if not attempt:
        logger.warning("Attempt %s not found", attempt_id)
        return None

    if attempt["runtime_status"] not in ("created", "running"):
        return None

    session_id = attempt["session_id"]
    if not session_id:
        return None

    session = conn.execute(
        "SELECT pid, status FROM agent_sessions WHERE session_id = :sid",
        {"sid": session_id},
    ).fetchone()
    if not session:
        return None

    pid = session["pid"]
    if pid is None:
        return None

    alive = True
    try:
        os.kill(pid, 0)
    except OSError:
        alive = False

    if alive:
        return None

    exit_code = attempt.get("exit_code")

    message = conn.execute(
        "SELECT response_payload, status FROM agent_messages WHERE id = :mid",
        {"mid": attempt["message_id"]},
    ).fetchone()

    has_response = message is not None and message["response_payload"] is not None

    if exit_code == 0 and has_response:
        action = "completed"
        conn.execute(
            """
            UPDATE agent_message_attempts SET runtime_status = 'completed',
              completed_at = datetime('now'), updated_at = datetime('now')
            WHERE id = :aid
            """,
            {"aid": attempt_id},
        )
    elif exit_code == 0 and not has_response:
        action = "no_response"
        conn.execute(
            """
            UPDATE agent_message_attempts SET runtime_status = 'no_response',
              completed_at = datetime('now'),
              error = 'Process exited 0 but no response given',
              updated_at = datetime('now')
            WHERE id = :aid
            """,
            {"aid": attempt_id},
        )
    else:
        action = "crashed"
        conn.execute(
            """
            UPDATE agent_message_attempts SET runtime_status = 'crashed',
              completed_at = datetime('now'),
              exit_code = COALESCE(exit_code, -1),
              error = 'Process exited without completing',
              updated_at = datetime('now')
            WHERE id = :aid
            """,
            {"aid": attempt_id},
        )

    conn.commit()
    return {"action": action}


# ---------------------------------------------------------------------------
# Dead process detection
# ---------------------------------------------------------------------------


def detect_dead_processes() -> int:
    """Detect sessions whose PID is no longer alive and mark them as stale.

    Active sessions (starting, busy) with a PID that fails os.kill(pid, 0)
    are marked as stale.  The session's message lock is released so other
    workers can pick up the work.
    """
    conn = _get_db()
    rows = conn.execute(
        """
        SELECT session_id, pid, current_message_id, current_attempt_id,
               agent_name
        FROM agent_sessions
        WHERE status IN ('starting', 'busy') AND pid IS NOT NULL
        """
    ).fetchall()

    dead_count = 0
    for row in rows:
        pid = row["pid"]
        try:
            os.kill(pid, 0)
        except OSError:
            dead_count += 1
            sid = row["session_id"]
            message_id = row["current_message_id"]

            conn.execute(
                """
                UPDATE agent_sessions SET status = 'stale',
                  completed_at = datetime('now'), updated_at = datetime('now')
                WHERE session_id = :sid
                """,
                {"sid": sid},
            )

            if message_id is not None:
                conn.execute(
                    """
                    UPDATE agent_messages SET lock_owner = NULL,
                      lock_acquired_at = NULL, lock_expires_at = NULL,
                      updated_at = datetime('now')
                    WHERE id = :mid AND lock_owner = :sid
                    """,
                    {"mid": message_id, "sid": sid},
                )

                try:
                    from amb_retry import schedule_retry

                    schedule_retry(message_id, "process_died", "Process exited unexpectedly")
                except ImportError:
                    pass
                except Exception:
                    logger.exception(
                        "detect_dead_processes: schedule_retry failed for message %s",
                        message_id,
                    )

    if dead_count:
        conn.commit()

    return dead_count


# ---------------------------------------------------------------------------
# Task timeout detection
# ---------------------------------------------------------------------------


def detect_task_timeouts() -> int:
    """Mark running attempts that have exceeded the task timeout as timed out.

    Looks for attempts whose runtime_status is running or busy and whose
    started_at is older than TASK_TIMEOUT_SEC.
    """
    conn = _get_db()
    cursor = conn.execute(
        """
        UPDATE agent_message_attempts SET
          runtime_status = 'timeout',
          error = 'Task exceeded timeout',
          completed_at = datetime('now'),
          updated_at = datetime('now')
        WHERE runtime_status IN ('running', 'busy')
          AND started_at IS NOT NULL
          AND started_at < datetime('now', :offset)
        """,
        {"offset": f"-{TASK_TIMEOUT_SEC} seconds"},
    )
    conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Startup timeout detection
# ---------------------------------------------------------------------------


def detect_startup_timeouts() -> int:
    """Mark sessions stuck in starting beyond the startup timeout as failed.

    Sessions whose started_at is older than STARTUP_TIMEOUT_SEC and whose
    status is still starting are transitioned to failed.
    """
    conn = _get_db()
    cursor = conn.execute(
        """
        UPDATE agent_sessions SET
          status = 'failed',
          last_error = 'Startup timeout exceeded',
          completed_at = datetime('now')
        WHERE status = 'starting'
          AND started_at < datetime('now', :offset)
        """,
        {"offset": f"-{STARTUP_TIMEOUT_SEC} seconds"},
    )
    conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Stale session detection
# ---------------------------------------------------------------------------


def detect_stale_sessions() -> int:
    """Mark sessions that have stopped heartbeating or haven't been seen.

    Two criteria (OR):
    1. last_heartbeat_at older than HEARTBEAT_TIMEOUT_SEC.
    2. last_seen_at older than ACTIVE_SESSION_STALE_SEC.

    Only touches sessions whose current status is starting or busy.
    """
    conn = _get_db()
    cursor = conn.execute(
        """
        UPDATE agent_sessions SET
          status = 'stale',
          completed_at = datetime('now')
        WHERE status IN ('starting', 'busy')
          AND (
            (last_heartbeat_at IS NOT NULL
             AND last_heartbeat_at < datetime('now', :hb_offset))
            OR (last_seen_at IS NOT NULL
                AND last_seen_at < datetime('now', :seen_offset))
          )
        """,
        {
            "hb_offset": f"-{HEARTBEAT_TIMEOUT_SEC} seconds",
            "seen_offset": f"-{ACTIVE_SESSION_STALE_SEC} seconds",
        },
    )
    conn.commit()
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Heartbeat / lease helpers (called from agent sessions)
# ---------------------------------------------------------------------------


def heartbeat_session(session_id: str) -> None:
    """Update the heartbeat and last-seen timestamps for *session_id*."""
    conn = _get_db()
    conn.execute(
        """
        UPDATE agent_sessions SET
          last_heartbeat_at = datetime('now'),
          last_seen_at = datetime('now')
        WHERE session_id = :sid
        """,
        {"sid": session_id},
    )
    conn.commit()


def renew_message_lease(message_id: int, session_id: str) -> bool:
    """Extend the lock lease on *message_id* for *session_id* by 10 minutes.

    Returns ``True`` if the lease was renewed, ``False`` if the session no
    longer holds the lock (e.g. the lock was already released or taken).
    """
    conn = _get_db()
    cursor = conn.execute(
        """
        UPDATE agent_messages SET
          lock_expires_at = datetime('now', '+10 minutes'),
          lock_version = COALESCE(lock_version, 0) + 1,
          updated_at = datetime('now')
        WHERE id = :mid
          AND lock_owner = :sid
          AND status IN ('claimed', 'running')
        """,
        {"mid": message_id, "sid": session_id},
    )
    conn.commit()
    return cursor.rowcount == 1
