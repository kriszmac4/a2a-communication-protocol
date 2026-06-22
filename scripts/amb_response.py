"""
AMB v7.2 Response Contract Layer — Core response dispatch for the Agent Message Bus.

MCP server routes through respond_to_message(). Ownership validation always runs;
schema strictness is controlled by AMB_STRICT_RESPONSE_SCHEMA_ENABLED.
"""

import json
import os

# Ensure agent_message_bus module is importable
import sys as _sys
from pathlib import Path as _Path
_SCRIPTS_DIR = str(_Path(__file__).parent.parent)
if _SCRIPTS_DIR not in _sys.path:
    _sys.path.insert(0, _SCRIPTS_DIR)
from agent_message_bus import _get_db
from amb_runtime import message_locked_by_session


# ---------------------------------------------------------------------------
# Feature flags (checked at call time via os.environ)
# ---------------------------------------------------------------------------


def _is_strict_schema() -> bool:
    """Return True when strict ``amb.response.v1`` schema validation is enabled.

    Controlled by the environment variable ``AMB_STRICT_RESPONSE_SCHEMA_ENABLED``
    (default: ``"true"``).  When disabled the response is normalised to the
    canonical schema with :func:`normalize_legacy_response`.
    """
    return os.environ.get("AMB_STRICT_RESPONSE_SCHEMA_ENABLED", "true").lower() == "true"


# ---------------------------------------------------------------------------
# Core response function
# ---------------------------------------------------------------------------


def respond_to_message(message_id: int, response: dict) -> None:
    """Persist *response* for *message_id* inside an AMB wakeup session.

    This is the **single entry-point** that MCP server routes through.

    Steps
    -----
    1. Verify that ``AMB_WAKEUP_SESSION_ID`` and ``AMB_MESSAGE_ID`` are set in
       the environment (i.e. we are running inside a wake-up session).
    2. Confirm that *message_id* matches ``AMB_MESSAGE_ID`` — you cannot respond
       to an unrelated message.
    3. Assert ownership via :func:`message_locked_by_session` — the calling
       session must still hold the lock on the message.
    4. Validate (or normalise) the response payload according to the strict-
       schema feature flag.
    5. Write the response idempotently — only one final response is accepted.

    Parameters
    ----------
    message_id : int
        The database row id of the message being responded to.
    response : dict
        The response payload.  Must conform to ``amb.response.v1`` when strict
        schema mode is enabled; otherwise it is normalised automatically.

    Raises
    ------
    RuntimeError
        If the environment is not set up correctly, the message is not owned by
        the caller, or the message has already been responded to.
    ValueError
        If the response payload fails schema validation.
    """
    session_id = os.environ.get("AMB_WAKEUP_SESSION_ID")
    assigned_message_id = os.environ.get("AMB_MESSAGE_ID")
    attempt_id = os.environ.get("AMB_ATTEMPT_ID")

    if not session_id:
        raise RuntimeError(
            "Not running inside AMB wakeup session. AMB_WAKEUP_SESSION_ID not set."
        )

    if not assigned_message_id:
        raise RuntimeError(
            "Not running inside AMB wakeup session. AMB_MESSAGE_ID not set."
        )

    if str(message_id) != assigned_message_id:
        raise RuntimeError(
            f"Cannot respond to unrelated message. "
            f"Assigned: #{assigned_message_id}, attempted: #{message_id}"
        )

    if not message_locked_by_session(message_id, session_id):
        raise RuntimeError(
            f"Message #{message_id} is not owned by session {session_id}. "
            f"Lock may have expired or been taken."
        )

    # Schema validation (toggleable):
    if _is_strict_schema():
        validate_response_schema(response)
    else:
        response = normalize_legacy_response(response)

    # Idempotent write:
    write_response_idempotently(
        message_id=message_id,
        session_id=session_id,
        attempt_id=attempt_id,
        response=response,
    )


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------


def validate_response_schema(response: dict) -> None:
    """Validate *response* conforms to the ``amb.response.v1`` schema.

    Required keys
    -------------
    schema_version, status, summary, result, artifacts, error, metadata

    ``status`` must be one of: ``"success"``, ``"partial"``, ``"failed"``.

    ``artifacts`` must be a ``list``.
    ``metadata`` must be a ``dict``.

    Parameters
    ----------
    response : dict
        The response payload to validate.

    Raises
    ------
    ValueError
        If any schema constraint is violated.
    """
    if not isinstance(response, dict):
        raise ValueError("Response must be a dict")

    required_keys = [
        "schema_version",
        "status",
        "summary",
        "result",
        "artifacts",
        "error",
        "metadata",
    ]
    for key in required_keys:
        if key not in response:
            raise ValueError(f"Missing required key in response: {key}")

    if response["schema_version"] != "amb.response.v1":
        raise ValueError(
            f"Invalid schema_version: {response['schema_version']}. "
            f"Expected 'amb.response.v1'"
        )

    allowed_statuses = ["success", "partial", "failed"]
    if response["status"] not in allowed_statuses:
        raise ValueError(
            f"Invalid status: {response['status']}. "
            f"Must be one of: {allowed_statuses}"
        )

    if not isinstance(response.get("artifacts", []), list):
        raise ValueError("artifacts must be a list")

    if not isinstance(response.get("metadata", {}), dict):
        raise ValueError("metadata must be a dict")


def normalize_legacy_response(response: dict) -> dict:
    """Wrap *response* into the canonical ``amb.response.v1`` format.

    When strict schema mode is **disabled**, legacy payloads (or bare dicts
    that do not carry ``schema_version``) are normalised so that downstream
    consumers always see a uniform shape.

    If *response* already has ``schema_version == 'amb.response.v1'`` it is
    returned unchanged.

    Parameters
    ----------
    response : dict
        The raw response payload from the agent.

    Returns
    -------
    dict
        A normalised response dict conforming to ``amb.response.v1``.
    """
    if isinstance(response, dict) and response.get("schema_version") == "amb.response.v1":
        return response

    return {
        "schema_version": "amb.response.v1",
        "status": response.get("status", "success"),
        "summary": response.get(
            "summary", response.get("response", str(response)[:200])
        ),
        "result": response.get("result", response.get("response", str(response))),
        "artifacts": response.get("artifacts", []),
        "error": response.get("error"),
        "metadata": response.get("metadata", {"legacy": True}),
    }


# ---------------------------------------------------------------------------
# Idempotent persistence
# ---------------------------------------------------------------------------


def write_response_idempotently(
    message_id: int,
    session_id: str,
    attempt_id: str,
    response: dict,
) -> None:
    """Persist *response* for *message_id* with an atomic state guard.

    Only one final response is ever accepted.  The SQL ``UPDATE`` targets rows
    whose ``status`` is ``'claimed'`` or ``'running'``, whose ``lock_owner``
    matches *session_id*, and whose ``response_payload`` is still ``NULL``.

    If the update touches exactly one row the attempt is also marked
    ``'completed'``.  Otherwise a ``RuntimeError`` is raised — the message has
    already been responded to, or the caller no longer owns it.

    Parameters
    ----------
    message_id : int
        The database row id of the message.
    session_id : str
        The session that claims ownership.
    attempt_id : str
        The id of the ``agent_message_attempts`` row to mark completed.
    response : dict
        The (already-validated/normalised) response payload.

    Raises
    ------
    RuntimeError
        If no row matched the state guard (already responded, or not owned).
    """
    conn = _get_db()

    sql = """
    UPDATE agent_messages SET
      status = 'responded',
      response_payload = :payload,
      responded_at = datetime('now'),
      updated_at = datetime('now'),
      lock_owner = NULL,
      lock_acquired_at = NULL,
      lock_expires_at = NULL
    WHERE id = :mid
      AND status IN ('claimed', 'running')
      AND lock_owner = :sid
      AND response_payload IS NULL;
    """
    cursor = conn.execute(
        sql,
        {
            "mid": message_id,
            "sid": session_id,
            "payload": json.dumps(response),
        },
    )
    conn.commit()

    if cursor.rowcount != 1:
        raise RuntimeError(
            f"Message #{message_id} already responded or not owned. "
            f"(status must be claimed/running, lock_owner={session_id}, "
            f"response_payload must be NULL)"
        )

    conn.execute(
        """
        UPDATE agent_message_attempts
        SET runtime_status = 'completed',
            completed_at = datetime('now'),
            updated_at = datetime('now')
        WHERE id = :aid
        """,
        {"aid": attempt_id},
    )
    conn.commit()
