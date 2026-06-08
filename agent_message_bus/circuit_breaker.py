#!/usr/bin/env python3
"""
marveen/circuit_breaker.py — Per-agent Circuit Breaker

3-state circuit breaker (CLOSED / OPEN / HALF_OPEN) with JSON file
persistence per agent. Prevents error spirals by blocking requests
to failing agents and probing recovery with a single trial request.

State files: DATA_DIR / "circuit_breakers" / "{agent}.json"
"""

from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("amb.circuit_breaker")

# ── Deferred import: DATA_DIR comes from agent_message_bus package ──
# We use a lazy loader to avoid circular imports at module init time.
_DATA_DIR: Optional[Path] = None
_lock = threading.Lock()


def _get_data_dir() -> Path:
    """Lazy-resolve DATA_DIR from the marveen package."""
    global _DATA_DIR
    if _DATA_DIR is None:
        from agent_message_bus import DATA_DIR as marveen_data_dir

        _DATA_DIR = marveen_data_dir
    return _DATA_DIR


# ── Configuration ───────────────────────────────────────────────────────────

CIRCUIT_BREAKER_CONFIG: Dict[str, int] = {
    "failure_threshold": 3,       # errors before opening
    "failure_window": 300,        # seconds (5 minutes)
    "open_timeout": 60,           # seconds before trying HALF_OPEN
    "half_open_max_requests": 1,  # only 1 probe in HALF_OPEN
}

# ── Internal helpers ────────────────────────────────────────────────────────


def _state_file_path(agent: str) -> Path:
    """Return the JSON state file path for a given agent."""
    cb_dir = _get_data_dir() / "circuit_breakers"
    cb_dir.mkdir(parents=True, exist_ok=True)
    return cb_dir / f"{agent}.json"


def _default_state(agent: str) -> Dict[str, Any]:
    """Return a fresh default state dict for a new agent."""
    return {
        "agent": agent,
        "state": "closed",
        "failure_count": 0,
        "last_failure_time": None,
        "last_state_change": time.time(),
        "success_count_since_open": 0,
    }


def _read_state(agent: str) -> Dict[str, Any]:
    """Read the circuit breaker state from disk (thread-safe)."""
    path = _state_file_path(agent)
    try:
        if path.exists():
            data = json.loads(path.read_text(encoding="utf-8"))
            # Ensure all expected keys exist (backward compat)
            defaults = _default_state(agent)
            for k, v in defaults.items():
                data.setdefault(k, v)
            return data
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning(
            "circuit_breaker: corrupt state file for '%s' (%s), resetting",
            agent,
            exc,
        )
    return _default_state(agent)


def _write_state(state: Dict[str, Any]) -> None:
    """Persist circuit breaker state to disk (caller must hold _lock)."""
    agent = state.get("agent", "unknown")
    path = _state_file_path(agent)
    try:
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except OSError as exc:
        logger.error("circuit_breaker: failed to write state for '%s': %s", agent, exc)


def _maybe_transition(state: Dict[str, Any]) -> None:
    """Evaluate time-based transitions (OPEN → HALF_OPEN after timeout)."""
    now = time.time()

    if state["state"] == "open":
        last_change = state.get("last_state_change", now)
        if (now - last_change) >= CIRCUIT_BREAKER_CONFIG["open_timeout"]:
            state["state"] = "half_open"
            state["last_state_change"] = now
            state["success_count_since_open"] = 0
            logger.info(
                "circuit_breaker: '%s' OPEN → HALF_OPEN (timeout expired)",
                state.get("agent"),
            )


def _prune_old_failures(state: Dict[str, Any]) -> None:
    """Clear failure_count if last failure is outside the window."""
    if state["failure_count"] == 0:
        return
    last_failure = state.get("last_failure_time")
    if last_failure is None:
        state["failure_count"] = 0
        return
    now = time.time()
    if (now - last_failure) > CIRCUIT_BREAKER_CONFIG["failure_window"]:
        state["failure_count"] = 0
        state["last_failure_time"] = None


# ── Public API ──────────────────────────────────────────────────────────────


def get_circuit_state(agent: str) -> str:
    """Return the current circuit state for *agent*: ``"closed"``, ``"open"``,
    or ``"half_open"``.

    This function is fast and safe to call on every request.
    """
    with _lock:
        state = _read_state(agent)
        _prune_old_failures(state)
        _maybe_transition(state)
        # Persist any automatic transition
        _write_state(state)
        return state["state"]


def record_success(agent: str) -> None:
    """Record a successful request for *agent*.

    - **CLOSED**: no-op (failure_count already implicitly low).
    - **HALF_OPEN**: increment ``success_count_since_open``.  When it reaches
      ``half_open_max_requests``, transition to CLOSED.
    """
    with _lock:
        state = _read_state(agent)
        _prune_old_failures(state)
        _maybe_transition(state)

        current = state["state"]

        if current == "half_open":
            state["success_count_since_open"] += 1
            if state["success_count_since_open"] >= CIRCUIT_BREAKER_CONFIG[
                "half_open_max_requests"
            ]:
                state["state"] = "closed"
                state["failure_count"] = 0
                state["last_failure_time"] = None
                state["last_state_change"] = time.time()
                logger.info(
                    "circuit_breaker: '%s' HALF_OPEN → CLOSED (probe succeeded)",
                    agent,
                )

        # CLOSED / OPEN: no-op
        _write_state(state)


def record_failure(agent: str) -> None:
    """Record a failed request for *agent*.

    - **CLOSED**: increment ``failure_count``.  If it reaches *failure_threshold*
      within *failure_window* seconds → OPEN.
    - **HALF_OPEN**: single failure sends it back to OPEN (reset timer).
    - **OPEN**: no-op (already blocking).
    """
    with _lock:
        state = _read_state(agent)
        _prune_old_failures(state)
        _maybe_transition(state)

        current = state["state"]
        now = time.time()

        if current == "closed":
            state["failure_count"] += 1
            state["last_failure_time"] = now

            # Check if threshold is crossed AND the earliest failure is still
            # inside the window (pruning already removed old ones, so if count
            # is at threshold all failures must be recent).
            if state["failure_count"] >= CIRCUIT_BREAKER_CONFIG["failure_threshold"]:
                state["state"] = "open"
                state["last_state_change"] = now
                state["success_count_since_open"] = 0
                logger.warning(
                    "circuit_breaker: '%s' CLOSED → OPEN (%d failures in %ds)",
                    agent,
                    state["failure_count"],
                    CIRCUIT_BREAKER_CONFIG["failure_window"],
                )

        elif current == "half_open":
            # Any failure in HALF_OPEN → back to OPEN, reset the timeout
            state["state"] = "open"
            state["last_state_change"] = now
            state["failure_count"] += 1
            state["last_failure_time"] = now
            state["success_count_since_open"] = 0
            logger.warning(
                "circuit_breaker: '%s' HALF_OPEN → OPEN (probe failed)",
                agent,
            )

        # OPEN: no-op

        _write_state(state)


def is_circuit_open(agent: str) -> bool:
    """Return ``True`` if the circuit for *agent* is OPEN (messages should be
    blocked).  Convenience wrapper around :func:`get_circuit_state`."""
    return get_circuit_state(agent) == "open"


# ── Self-test (runs when executed directly) ─────────────────────────────────

if __name__ == "__main__":
    print("=== Circuit Breaker Self-Test ===")

    # When running standalone, ensure the marveen package is importable
    import sys as _sys

    _scripts_dir = Path(__file__).resolve().parent.parent
    if str(_scripts_dir) not in _sys.path:
        _sys.path.insert(0, str(_scripts_dir))

    test_agent = "test_cb_agent"

    # Clean up from previous runs
    _test_path = _get_data_dir() / "circuit_breakers" / f"{test_agent}.json"
    if _test_path.exists():
        _test_path.unlink()

    print(f"1. Initial state: {get_circuit_state(test_agent)}")
    assert get_circuit_state(test_agent) == "closed"

    print("2. Recording failures...")
    for i in range(3):
        record_failure(test_agent)
        print(f"   failure {i+1}: state={get_circuit_state(test_agent)}")

    assert get_circuit_state(test_agent) == "open"
    assert is_circuit_open(test_agent) is True

    print("3. OPEN → HALF_OPEN (simulate timeout)...")
    # Manually advance the clock by adjusting the state file
    with _lock:
        state = _read_state(test_agent)
        state["state"] = "half_open"
        state["last_state_change"] = time.time()
        _write_state(state)

    assert get_circuit_state(test_agent) == "half_open"

    print("4. HALF_OPEN + success → CLOSED")
    record_success(test_agent)
    assert get_circuit_state(test_agent) == "closed"
    assert is_circuit_open(test_agent) is False

    print("5. HALF_OPEN + failure → OPEN")
    # Re-open then move to half_open
    for _ in range(3):
        record_failure(test_agent)
    assert get_circuit_state(test_agent) == "open"
    with _lock:
        state = _read_state(test_agent)
        state["state"] = "half_open"
        state["last_state_change"] = time.time()
        _write_state(state)
    record_failure(test_agent)
    assert get_circuit_state(test_agent) == "open"

    # Clean up
    if _test_path.exists():
        _test_path.unlink()

    print("\n✅ All circuit breaker tests passed!")
