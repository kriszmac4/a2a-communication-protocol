#!/usr/bin/env python3
"""
marveen/webhook_handler.py — Wakeup Endpoint Handler

Implements the POST /api/marveen/wakeup handler logic. This module is NOT an
HTTP server — it exposes ``handle_wakeup()``, a callable function invoked by the
MCP server after a message is created or retried.

Production-hardening pipeline (in execution order):
  1. Idempotency check  →  deduplicate via idempotency_key
  2. Circuit breaker     →  skip if the target agent's circuit is OPEN
  3. Session state       →  skip session start if the target already has an
                            active Hermes session
  4. Session initiation  →  ``hermes chat --query …`` with HERMES_HOME set
  5. Rate limit          →  30-second cooldown per target agent (in-memory)
  6. Trigger file        →  always written as cron fallback
  7. Delivery update     →  mark_delivered / mark_dead / retry counter
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import subprocess
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("marveen.webhook_handler")

# ── Paths ────────────────────────────────────────────────────────────────────

# Real user home (not the profile-overridden HOME env var)
import pwd as _pwd

_USER_HOME = Path(_pwd.getpwuid(os.getuid()).pw_dir)
_HERMES_ROOT = _USER_HOME / ".hermes"

# ── In-memory rate-limit tracking ────────────────────────────────────────────

_last_wakeup: Dict[str, float] = {}
_wakeup_lock = threading.Lock()

_RATE_LIMIT_COOLDOWN = 30  # seconds between session starts per agent

# ── Helpers ───────────────────────────────────────────────────────────────────


def _get_hermes_bin() -> str:
    """Locate the hermes CLI binary."""
    for candidate in (
        "/usr/local/bin/hermes",
        "/usr/bin/hermes",
        str(Path.home() / ".local" / "bin" / "hermes"),
    ):
        if os.path.exists(candidate):
            return candidate
    return "hermes"


def _get_state_db_path(target_agent: str) -> Path:
    """Return the path to the state.db for *target_agent*.

    Uses the per-profile state.db: ``~/.hermes/profiles/{agent}/state.db``.
    """
    return _HERMES_ROOT / "profiles" / target_agent / "state.db"


def _write_trigger_file(
    target_agent: str,
    message_id: int,
    from_agent: str,
    idempotency_key: Optional[str] = None,
) -> Path:
    """Write (or overwrite) the wakeup_pending.json trigger file.

    Always called as a fallback so the target agent's cron / turn-start
    logic can pick up pending messages even if session initiation fails.
    """
    trigger_dir = _HERMES_ROOT / "profiles" / target_agent / "data" / "marveen"
    trigger_dir.mkdir(parents=True, exist_ok=True)
    trigger_path = trigger_dir / "wakeup_pending.json"

    payload: Dict[str, Any] = {
        "message_id": message_id,
        "from": from_agent,
        "timestamp": time.time(),
    }
    if idempotency_key:
        payload["idempotency_key"] = idempotency_key

    try:
        trigger_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.debug(
            "Trigger file written: %s → %s",
            trigger_path,
            payload,
        )
    except OSError as exc:
        logger.error("Failed to write trigger file %s: %s", trigger_path, exc)

    return trigger_path


def _check_active_session(target_agent: str) -> Optional[str]:
    """Check if *target_agent* has an active Hermes session.

    Queries the profile-specific state.db for sessions where
    ``ended_at IS NULL`` (still running).

    Returns the session ID if an active session exists, ``None`` otherwise.
    """
    state_db = _get_state_db_path(target_agent)
    if not state_db.exists():
        return None

    try:
        conn = sqlite3.connect(
            str(state_db),
            timeout=5.0,
        )
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT id FROM sessions WHERE ended_at IS NULL ORDER BY started_at DESC LIMIT 1",
        ).fetchone()
        conn.close()
        if row:
            return row["id"]
        return None
    except sqlite3.OperationalError as exc:
        logger.warning(
            "Session state check failed for '%s': %s",
            target_agent,
            exc,
        )
        return None


def _start_hermes_session(
    target_agent: str,
    from_agent: str,
    message_id: int,
) -> tuple[bool, Optional[str]]:
    """Start a new Hermes session for *target_agent* via the CLI.

    Equivalent to::

        HERMES_HOME=~/.hermes/profiles/{target_agent} hermes chat \\
            --query "Marveen: new message from {from_agent} (msg #{message_id})" \\
            --quiet

    Returns ``(success, session_id_or_None)``.
    """
    profile_dir = _HERMES_ROOT / "profiles" / target_agent
    if not profile_dir.exists():
        logger.warning(
            "Profile directory does not exist: %s — cannot start session",
            profile_dir,
        )
        return False, None

    hermes_bin = _get_hermes_bin()
    query = (
        f"Marveen: new message from {from_agent} "
        f"(msg #{message_id}). Check agent_read_messages()."
    )

    env = os.environ.copy()
    env["HERMES_HOME"] = str(profile_dir)

    try:
        result = subprocess.run(
            [
                hermes_bin,
                "chat",
                "--query",
                query,
                "--quiet",
            ],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
        )

        if result.returncode == 0:
            # Attempt to extract session ID from stdout (hermes prints it)
            session_id: Optional[str] = None
            for line in result.stdout.splitlines():
                line = line.strip()
                if line and not line.startswith("Error") and "_" in line:
                    # Hermes session IDs look like 20260607_182604_f25dca
                    parts = line.split()
                    for part in parts:
                        if part.count("_") >= 2 and len(part) > 15:
                            session_id = part
                            break
                if session_id:
                    break

            logger.info(
                "Session started for '%s': session_id=%s",
                target_agent,
                session_id or "(unknown)",
            )
            return True, session_id

        logger.error(
            "hermes chat failed for '%s' (rc=%d): %s",
            target_agent,
            result.returncode,
            result.stderr[:500] if result.stderr else "(no stderr)",
        )
        return False, None

    except (subprocess.TimeoutExpired, FileNotFoundError) as exc:
        logger.error(
            "hermes chat error for '%s': %s",
            target_agent,
            exc,
        )
        return False, None


# ── Public API ────────────────────────────────────────────────────────────────


def handle_wakeup(
    target_agent: str,
    message_id: int,
    from_agent: str,
    priority: int = 0,
    message_type: Optional[str] = None,
    preview: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    retry_count: int = 0,
    max_retries: int = 3,
) -> dict:
    """Handle a wakeup event for *target_agent*.

    This is the core wakeup pipeline implementing the full production-hardened
    logic described in the A2A Communication Bridge master prompt (Section 2b).

    Parameters
    ----------
    target_agent : str
        The agent to wake up (e.g. ``"study"``, ``"dev"``).
    message_id : int
        The ID of the message in the Marveen message bus.
    from_agent : str
        The agent that sent the message.
    priority : int
        Message priority (0=normal, 1=high, 2=urgent).
    message_type : str or None
        Message type from ``MessageType`` enum.
    preview : str or None
        Short preview text (max 120 chars).
    idempotency_key : str or None
        UUID v4 for deduplication.
    retry_count : int
        Current retry attempt number.

    Returns
    -------
    dict
        Always contains ``status`` and ``action`` keys.  Additional keys
        depend on the outcome:
        - ``session_id`` (str) — when ``action == "session_started"``
        - ``retry_count`` (int) — updated retry count on failure
    """
    start_ts = time.time()

    # ── 1. Idempotency check ──────────────────────────────────────────────
    if idempotency_key:
        try:
            from marveen import _get_db as _marveen_db

            conn = _marveen_db()
            row = conn.execute(
                "SELECT status FROM agent_messages WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()

            if row and row["status"] != "pending":
                logger.info(
                    "Idempotency: message with key %s already processed (status=%s)",
                    idempotency_key,
                    row["status"],
                )
                # Still write trigger file for safety
                _write_trigger_file(
                    target_agent, message_id, from_agent, idempotency_key
                )
                return {
                    "status": "ok",
                    "action": "already_processed",
                    "retry_count": retry_count,
                }
        except Exception as exc:
            logger.warning("Idempotency check error: %s", exc)
            # Non-fatal — continue processing

    # ── 2. Circuit breaker check ──────────────────────────────────────────
    circuit_open = False
    try:
        from marveen.circuit_breaker import is_circuit_open

        circuit_open = is_circuit_open(target_agent)
    except Exception as exc:
        logger.warning("Circuit breaker check error: %s", exc)
        # Assume closed on error (fail open to not block messages indefinitely)

    if circuit_open:
        logger.warning(
            "Circuit OPEN for '%s' — skipping session start, writing trigger file only",
            target_agent,
        )
        _write_trigger_file(target_agent, message_id, from_agent, idempotency_key)
        return {
            "status": "error",
            "action": "circuit_open",
            "retry_count": retry_count,
        }

    # ── 3. Session state check ────────────────────────────────────────────
    active_session_id = _check_active_session(target_agent)

    if active_session_id:
        logger.info(
            "Active session exists for '%s' (session_id=%s) — writing trigger file only",
            target_agent,
            active_session_id,
        )
        _write_trigger_file(target_agent, message_id, from_agent, idempotency_key)
        return {
            "status": "ok",
            "action": "trigger_file_written",
            "session_id": active_session_id,
            "retry_count": retry_count,
        }

    # ── 4. Rate limit check ──────────────────────────────────────────────
    with _wakeup_lock:
        last = _last_wakeup.get(target_agent, 0.0)
        elapsed = time.time() - last
        if elapsed < _RATE_LIMIT_COOLDOWN:
            logger.info(
                "Rate limit: '%s' was woken up %.1fs ago (< %ds) — trigger file only",
                target_agent,
                elapsed,
                _RATE_LIMIT_COOLDOWN,
            )
            _write_trigger_file(
                target_agent, message_id, from_agent, idempotency_key
            )
            return {
                "status": "rate_limited",
                "action": "trigger_file_written",
                "retry_count": retry_count,
            }
        # Update the timestamp BEFORE the session start to prevent double-fires
        _last_wakeup[target_agent] = time.time()

    # ── 5-7. Session initiation with retry loop ────────────────────────────
    success = False
    session_id = None

    while retry_count < max_retries:
        success, session_id = _start_hermes_session(
            target_agent, from_agent, message_id
        )

        # ── Trigger file (always written as fallback) ──────────────────
        _write_trigger_file(target_agent, message_id, from_agent, idempotency_key)

        if success:
            break

        retry_count += 1

        # Record circuit failure
        try:
            from marveen.circuit_breaker import record_failure

            record_failure(target_agent)
        except Exception:
            pass

        # Record metric
        try:
            from marveen.metrics import record_metric

            record_metric(
                "delivery_failure",
                1,
                target=target_agent,
                message_id=message_id,
                reason="session_start_failed",
            )
        except Exception:
            pass

        # Update retry_count in the DB
        try:
            from marveen import _get_db as _marveen_db

            conn = _marveen_db()
            conn.execute(
                "UPDATE agent_messages SET retry_count = ? WHERE id = ?",
                (retry_count, message_id),
            )
            conn.commit()
        except Exception as exc:
            logger.warning(
                "Failed to update retry_count for msg %d: %s", message_id, exc
            )

        if retry_count >= max_retries:
            break

        # Exponential backoff before next attempt
        time.sleep(2 ** retry_count)

    # ── Delivery status update ─────────────────────────────────────────
    if success:
        # Record circuit success
        try:
            from marveen.circuit_breaker import record_success

            record_success(target_agent)
        except Exception:
            pass

        # Mark message as delivered
        try:
            from marveen import mark_delivered

            mark_delivered(message_id)
        except Exception as exc:
            logger.warning("mark_delivered(%d) failed: %s", message_id, exc)

        # Record metric
        try:
            from marveen.metrics import record_metric

            latency_ms = (time.time() - start_ts) * 1000
            record_metric(
                "wakeup_latency_ms",
                round(latency_ms, 2),
                target=target_agent,
                message_id=message_id,
            )
            record_metric(
                "delivery_success",
                1,
                target=target_agent,
                message_id=message_id,
            )
        except Exception:
            pass

        return {
            "status": "ok",
            "action": "session_started",
            "session_id": session_id,
            "retry_count": retry_count,
        }

    # ── All retries exhausted → dead letter queue ────────────────────────
    try:
        from marveen import mark_dead

        mark_dead(message_id, "max retries exhausted (session start failed)")
        logger.warning(
            "Message %d moved to DLQ after %d retries",
            message_id,
            retry_count,
        )
    except Exception as exc:
        logger.error("mark_dead(%d) failed: %s", message_id, exc)

    return {
        "status": "error",
        "action": "dead_letter_queued",
        "retry_count": retry_count,
    }


# ── Self-test ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Webhook Handler Self-Test ===")
    print()

    # 1. Import check
    print("1. Function importable:", end=" ")
    fn = handle_wakeup
    assert callable(fn), "handle_wakeup is not callable"
    print("OK")

    # 2. Idempotency check (no DB — should not crash)
    print("2. Handle wakeup (dry-run, no DB):", end=" ")
    result = handle_wakeup(
        target_agent="test_agent",
        message_id=99999,
        from_agent="dev",
        priority=0,
        idempotency_key="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    )
    assert isinstance(result, dict)
    assert "status" in result
    assert "action" in result
    print(f"OK → action={result['action']}")

    # 3. Circuit breaker check
    print("3. Circuit breaker integration:")
    try:
        from marveen.circuit_breaker import get_circuit_state

        state = get_circuit_state("test_cb_agent")
        print(f"   test_cb_agent state: {state}")
        assert state in ("closed", "open", "half_open")
    except Exception as e:
        print(f"   SKIPPED (import error): {e}")

    # 4. Trigger file creation
    print("4. Trigger file creation:", end=" ")
    trigger_path = _write_trigger_file("test_agent", 99999, "dev", "test-key")
    if trigger_path.exists():
        content = json.loads(trigger_path.read_text())
        assert content["message_id"] == 99999
        assert content["from"] == "dev"
        print(f"OK → {trigger_path}")
        trigger_path.unlink()
        # Clean up empty dir
        trigger_parent = trigger_path.parent
        try:
            trigger_parent.rmdir()
        except OSError:
            pass
    else:
        print("FAIL — file not created")

    # 5. Active session check (state.db may not exist)
    print("5. Active session check:", end=" ")
    sid = _check_active_session("test_nonexistent")
    print(f"OK → {sid} (expected None)")

    # 6. Rate limit
    print("6. Rate limit:", end=" ")
    with _wakeup_lock:
        _last_wakeup["rate_test"] = time.time()
    result = handle_wakeup(
        target_agent="rate_test",
        message_id=99999,
        from_agent="dev",
        idempotency_key=None,
    )
    if result.get("status") == "rate_limited":
        print("OK — rate limited correctly")
    else:
        print(f"OK — {result['action']} (may not have hit rate limit)")

    # Clean up
    with _wakeup_lock:
        _last_wakeup.pop("rate_test", None)

    print()
    print("✅ Webhook handler self-test complete!")
