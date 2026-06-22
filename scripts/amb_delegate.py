"""
AMB v7.2 Delegate-and-Wait Orchestration Layer — Fire-and-wait delegation
for the Agent Message Bus.

``delegate_and_wait`` sends a task to a target agent and polls for the
response until a timeout is reached.  Optional cancellation on timeout
is controlled by the *cancel_on_timeout* parameter.
"""

import json
import os
import time
import uuid

# Ensure agent_message_bus module is importable
import sys as _sys
from pathlib import Path as _Path
_SCRIPTS_DIR = str(_Path(__file__).parent.parent)
if _SCRIPTS_DIR not in _sys.path:
    _sys.path.insert(0, _SCRIPTS_DIR)
from agent_message_bus import _get_db, create_typed_message


__all__ = [
    "delegate_and_wait",
]


# ---------------------------------------------------------------------------
# Feature flag (checked at call time via os.environ)
# ---------------------------------------------------------------------------


def _is_enabled() -> bool:
    """Return True when ``delegate_and_wait`` is enabled.

    Controlled by the environment variable ``AMB_DELEGATE_WAIT_ENABLED``
    (default: ``"false"``).
    """
    return os.environ.get("AMB_DELEGATE_WAIT_ENABLED", "false").lower() == "true"


# ---------------------------------------------------------------------------
# Core orchestration function
# ---------------------------------------------------------------------------


def delegate_and_wait(
    target: str,
    task: str,
    timeout: int = 300,
    cancel_on_timeout: bool = False,
) -> dict:
    """Send *task* to *target* agent and block until a response arrives.

    The caller's identity is determined from ``AMB_TARGET_AGENT`` or
    ``HERMES_PROFILE`` (fallback).  A new message is created with
    ``message_type='delegate_task'`` and a ``correlation_id`` that links the
    request to its eventual response.

    The function polls ``agent_messages`` every second, looking for a
    ``'responded'`` status with a non-null ``response_payload``.  If the
    response arrives before *timeout* seconds it is deserialised and
    returned as a ``dict`` with ``success=True``.

    When no response arrives before the deadline the behaviour depends on
    *cancel_on_timeout*:

    * ``False`` (default) — a timeout result with ``success=False`` is
      returned immediately.
    * ``True`` — the message status is set to ``'cancellation_requested'``
      before the timeout result is returned.

    Parameters
    ----------
    target : str
        The name of the agent to delegate the task to.
    task : str
        The task payload (free-form string).
    timeout : int
        Maximum seconds to wait for a response (default 300).
    cancel_on_timeout : bool
        Whether to request cancellation of the delegated task on timeout.

    Returns
    -------
    dict
        A result dict with keys ``success``, ``status``, ``message_id``,
        ``target``, ``duration_sec``, ``result``, and ``artifacts``.

    Raises
    ------
    NotImplementedError
        If the ``AMB_DELEGATE_WAIT_ENABLED`` feature flag is not ``"true"``.
    """
    if not _is_enabled():
        raise NotImplementedError(
            "delegate_and_wait is disabled. Set AMB_DELEGATE_WAIT_ENABLED=true"
        )

    # Determine the calling agent's identity.
    from_agent = os.environ.get(
        "AMB_TARGET_AGENT", os.environ.get("HERMES_PROFILE", "dev")
    )

    # Create a correlated message so the response can be tracked.
    correlation_id = uuid.uuid4().int >> 96  # 32-bit integer correlation ID

    result = create_typed_message(
        from_agent=from_agent,
        to_agent=target,
        content=task,
        message_type="delegate_task",
        priority=0,
        correlation_id=correlation_id,
    )
    message_id = result["id"]

    start = time.time()
    deadline = start + timeout

    while time.time() < deadline:
        conn = _get_db()
        row = conn.execute(
            "SELECT status, response_payload FROM agent_messages WHERE id = :mid",
            {"mid": message_id},
        ).fetchone()

        if row is not None and row[0] == "responded" and row[1] is not None:
            response = json.loads(row[1])
            return {
                "success": True,
                "status": response.get("status", "success"),
                "message_id": message_id,
                "target": target,
                "duration_sec": round(time.time() - start, 1),
                "result": response.get("result"),
                "artifacts": response.get("artifacts", []),
            }

        time.sleep(1)

    # Timeout path — optionally request cancellation.
    if cancel_on_timeout:
        conn = _get_db()
        conn.execute(
            "UPDATE agent_messages "
            "SET status = 'cancellation_requested', "
            "    updated_at = datetime('now') "
            "WHERE id = :mid",
            {"mid": message_id},
        )
        conn.commit()

    return {
        "success": False,
        "status": "timeout",
        "message_id": message_id,
        "target": target,
        "cancelled": cancel_on_timeout,
        "result": None,
        "artifacts": [],
    }
