#!/usr/bin/env python3
"""
marveen/metrics.py — File-based Metrics Collection

Simple append-only JSONL metrics with hourly rotation and 72-hour retention.
Thread-safe (``threading.Lock``).  Zero external dependencies — pure stdlib.

Metrics file: DATA_DIR / "metrics.jsonl"
Rotated to:   DATA_DIR / "metrics-YYYY-MM-DD-HH.jsonl"
"""

from __future__ import annotations

import glob
import json
import logging
import os
import statistics
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger("marveen.metrics")

# ── Deferred import: DATA_DIR comes from marveen package ──
_DATA_DIR: Optional[Path] = None
_lock = threading.Lock()
_rotation_lock = threading.Lock()  # separate lock for rotation to avoid deadlocks


def _get_data_dir() -> Path:
    """Lazy-resolve DATA_DIR from the marveen package."""
    global _DATA_DIR
    if _DATA_DIR is None:
        from marveen import DATA_DIR as marveen_data_dir

        _DATA_DIR = marveen_data_dir
    return _DATA_DIR


# ── Configuration ───────────────────────────────────────────────────────────

RETENTION_HOURS = 72  # how many hours of rotated files to keep

# ── File path helpers ───────────────────────────────────────────────────────


def _live_path() -> Path:
    """Return the path to the *current* live metrics.jsonl file."""
    return _get_data_dir() / "metrics.jsonl"


def _rotate_path(ts: Optional[float] = None) -> Path:
    """Build a rotated filename like ``metrics-2026-06-07-14.jsonl``."""
    if ts is None:
        ts = time.time()
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    return _get_data_dir() / f"metrics-{dt.strftime('%Y-%m-%d-%H')}.jsonl"


def _hour_key(ts: float) -> int:
    """Return an integer hour-key for a timestamp (hours since epoch)."""
    return int(ts // 3600)


# ── Rotation ────────────────────────────────────────────────────────────────

_last_rotation_hour: Optional[int] = None


def _maybe_rotate(now: Optional[float] = None) -> None:
    """Rotate the live metrics file if the hour has changed."""
    global _last_rotation_hour

    if now is None:
        now = time.time()

    current_hour = _hour_key(now)

    # Fast-path: already rotated this hour
    if _last_rotation_hour == current_hour:
        return

    with _rotation_lock:
        # Double-check after acquiring the lock
        if _last_rotation_hour == current_hour:
            return

        live = _live_path()
        if live.exists() and live.stat().st_size > 0:
            rotated = _rotate_path(now)
            # If destination already exists (e.g. multiple processes), append
            if rotated.exists():
                existing = rotated.read_text(encoding="utf-8")
                appended = live.read_text(encoding="utf-8")
                rotated.write_text(
                    existing.rstrip("\n") + "\n" + appended, encoding="utf-8"
                )
                logger.info(
                    "metrics: appended to existing rotated file %s", rotated.name
                )
            else:
                live.rename(rotated)
                logger.info("metrics: rotated live file → %s", rotated.name)
            # Start fresh live file
            live.write_text("", encoding="utf-8")

        _last_rotation_hour = current_hour
        _cleanup_old_rotations()


def _cleanup_old_rotations() -> None:
    """Remove rotated metrics files older than *RETENTION_HOURS*."""
    cutoff = time.time() - (RETENTION_HOURS * 3600)
    pattern = str(_get_data_dir() / "metrics-????-??-??-??.jsonl")

    for fname in glob.glob(pattern):
        try:
            mtime = os.path.getmtime(fname)
            if mtime < cutoff:
                os.remove(fname)
                logger.debug("metrics: removed expired rotated file %s", fname)
        except OSError:
            pass


# ── Public API ──────────────────────────────────────────────────────────────


def record_metric(metric: str, value: Any, **tags: Any) -> None:
    """Append a metric entry to the live JSONL file.

    Parameters
    ----------
    metric : str
        Metric name (e.g. ``"messages_sent_total"``, ``"wakeup_latency_ms"``).
    value : Any
        Numeric or string value for this observation.
    **tags
        Arbitrary key-value pairs attached to the metric line
        (e.g. ``agent="dev"``, ``target="study"``).

    Example
    -------
    >>> record_metric("delivery_success", 1, target="study", message_id=123)
    """
    now = time.time()

    with _lock:
        _maybe_rotate(now)

        entry: Dict[str, Any] = {"ts": now, "metric": metric}
        entry.update(tags)
        entry["value"] = value

        live = _live_path()
        live_dir = live.parent
        live_dir.mkdir(parents=True, exist_ok=True)

        try:
            with open(str(live), "a", encoding="utf-8") as fh:
                fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except OSError as exc:
            logger.error("metrics: failed to write entry: %s", exc)


def _read_metrics_since(since: float) -> List[Dict[str, Any]]:
    """Internal: read all metric entries with ``ts >= since`` from live +
    relevant rotated files."""
    entries: List[Dict[str, Any]] = []

    # Read all rotated files that might contain entries in the window
    pattern = str(_get_data_dir() / "metrics-????-??-??-??.jsonl")
    for fname in sorted(glob.glob(pattern)):
        try:
            mtime = os.path.getmtime(fname)
            # Quick rejection: if the file's mtime is before the window, skip
            if mtime < since:
                continue
        except OSError:
            pass
        _load_jsonl_file(fname, entries, since)

    # Read live file
    live = _live_path()
    if live.exists():
        _load_jsonl_file(str(live), entries, since)

    return entries


def _load_jsonl_file(
    path: str, entries: List[Dict[str, Any]], since: float
) -> None:
    """Load entries from a JSONL file, filtering by ``ts >= since``."""
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = obj.get("ts")
                if ts is not None and ts >= since:
                    entries.append(obj)
    except OSError:
        pass


def get_metrics_last_hour() -> Dict[str, Any]:
    """Aggregate metrics from the last hour into a summary dict.

    Returns a dict with per-metric counts, averages, and p95 values
    for numeric metrics.

    Returns
    -------
    dict
        Keys include ``total_entries``, ``metrics`` (a nested dict of
        metric_name → {count, sum, avg, p95, min, max}), and
        ``window_start`` / ``window_end`` timestamps.
    """
    now = time.time()
    since = now - 3600  # 1 hour

    with _lock:
        _maybe_rotate(now)
        entries = _read_metrics_since(since)

    # Aggregate
    metric_values: Dict[str, List[float]] = {}
    for entry in entries:
        name = entry.get("metric", "unknown")
        val = entry.get("value")
        if val is not None and isinstance(val, (int, float)):
            metric_values.setdefault(name, []).append(float(val))

    summary: Dict[str, Any] = {
        "total_entries": len(entries),
        "window_start": since,
        "window_end": now,
        "metrics": {},
    }

    for name, vals in metric_values.items():
        sorted_vals = sorted(vals)
        n = len(sorted_vals)
        p95_idx = int(n * 0.95)
        p95 = sorted_vals[p95_idx] if n > 0 and p95_idx < n else (
            sorted_vals[-1] if n > 0 else 0
        )

        summary["metrics"][name] = {
            "count": n,
            "sum": sum(sorted_vals),
            "avg": statistics.mean(sorted_vals) if n > 0 else 0,
            "p95": p95,
            "min": sorted_vals[0] if n > 0 else 0,
            "max": sorted_vals[-1] if n > 0 else 0,
        }

    return summary


def get_metrics_summary() -> Dict[str, Any]:
    """Return a high-level summary suitable for ``marveen_status`` display.

    This aggregates the last hour plus circuit breaker state snapshots.

    Returns
    -------
    dict
        Keys:
        - ``messages_sent_total``: dict of agent → count
        - ``wakeup_latency_p95``: float (ms)
        - ``delivery_success_rate``: float (0.0–1.0 or None if no data)
        - ``circuit_breaker_states``: dict of agent → state
        - ``dead_letter_queue_size``: int (approximate, from metrics)
    """
    hour = get_metrics_last_hour()

    # Per-agent message counts
    now = time.time()
    since = now - 3600

    with _lock:
        _maybe_rotate(now)
        entries = _read_metrics_since(since)

    messages_by_agent: Dict[str, int] = {}
    delivery_successes = 0
    delivery_failures = 0
    wakeup_latencies: List[float] = []

    for entry in entries:
        name = entry.get("metric", "")
        val = entry.get("value")
        agent = entry.get("agent", entry.get("target", "unknown"))

        if name == "messages_sent_total":
            messages_by_agent[agent] = messages_by_agent.get(agent, 0) + int(val or 0)

        if name == "delivery_success":
            delivery_successes += 1
        elif name == "delivery_failure":
            delivery_failures += 1

        if name == "wakeup_latency_ms" and isinstance(val, (int, float)):
            wakeup_latencies.append(float(val))

    total_deliveries = delivery_successes + delivery_failures
    success_rate = (
        delivery_successes / total_deliveries if total_deliveries > 0 else None
    )

    sorted_lat = sorted(wakeup_latencies)
    p95_latency = None
    if sorted_lat:
        p95_idx = int(len(sorted_lat) * 0.95)
        p95_latency = sorted_lat[min(p95_idx, len(sorted_lat) - 1)]

    # Circuit breaker states (import inline to avoid circular dep)
    cb_states: Dict[str, str] = {}
    try:
        from marveen.circuit_breaker import get_circuit_state as _cb_state

        for agent in ("general", "dev", "research", "study"):
            cb_states[agent] = _cb_state(agent)
    except Exception:
        pass  # circuit_breaker module may not be loaded yet

    # Approximate DLQ size from metrics
    dlq_size = 0
    for entry in entries:
        if entry.get("metric") == "dead_letter_queue_size":
            dlq_size = max(dlq_size, int(entry.get("value", 0)))

    return {
        "messages_sent_total": messages_by_agent,
        "wakeup_latency_p95": p95_latency,
        "delivery_success_rate": success_rate,
        "circuit_breaker_states": cb_states,
        "dead_letter_queue_size": dlq_size,
    }


# ── Self-test (runs when executed directly) ─────────────────────────────────

if __name__ == "__main__":
    import tempfile

    print("=== Metrics Self-Test ===")

    # Override DATA_DIR with a temp dir for testing
    test_dir = Path(tempfile.mkdtemp(prefix="marveen_metrics_test_"))
    _DATA_DIR = test_dir

    # Write some test metrics
    record_metric("messages_sent_total", 1, agent="dev", message_type="request_data")
    record_metric("messages_sent_total", 1, agent="general", message_type="handoff")
    record_metric("messages_sent_total", 1, agent="dev", message_type="status_update")
    record_metric("wakeup_latency_ms", 120, target="study")
    record_metric("wakeup_latency_ms", 450, target="general")
    record_metric("wakeup_latency_ms", 89, target="research")
    record_metric("delivery_success", 1, target="study", message_id=1)
    record_metric("delivery_success", 1, target="general", message_id=2)
    record_metric("delivery_failure", 1, target="research", reason="timeout")
    record_metric("circuit_breaker_state", "open", agent="research")
    record_metric("dead_letter_queue_size", 3)

    # Test get_metrics_last_hour
    summary = get_metrics_last_hour()
    print(f"Total entries: {summary['total_entries']}")
    for metric_name, stats in summary.get("metrics", {}).items():
        print(f"  {metric_name}: count={stats['count']}, avg={stats['avg']:.2f}")

    assert summary["total_entries"] >= 5, f"Expected >=5 entries, got {summary['total_entries']}"

    # Test get_metrics_summary
    msum = get_metrics_summary()
    print(f"Messages by agent: {msum['messages_sent_total']}")
    print(f"Wake-up latency p95: {msum['wakeup_latency_p95']}")
    print(f"Delivery success rate: {msum['delivery_success_rate']}")
    print(f"DLQ size: {msum['dead_letter_queue_size']}")

    assert "dev" in msum["messages_sent_total"]
    assert msum["messages_sent_total"]["dev"] == 2
    assert msum["delivery_success_rate"] is not None
    assert msum["dead_letter_queue_size"] == 3

    # Test rotation (simulate by calling _maybe_rotate with future hour)
    future_ts = time.time() + 3600
    old_live = _live_path()
    _maybe_rotate(future_ts)
    rotated_pattern = str(test_dir / "metrics-????-??-??-??.jsonl")
    rotated_files = glob.glob(rotated_pattern)
    print(f"Rotated files: {len(rotated_files)}")

    # Clean up
    import shutil
    shutil.rmtree(test_dir, ignore_errors=True)

    print("\n✅ All metrics tests passed!")
