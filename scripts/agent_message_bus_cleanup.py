#!/usr/bin/env python3
"""
Agent Message Bus Cleanup — scheduled message DB housekeeping.

Agent Message Bus Cleanup — scheduled message DB housekeeping.

Deletes old messages from the agent_messages database to prevent bloat.
Runs every 6 hours via cron (no_agent=True, stdout delivered as report).

Cleanup rules:
  - Messages with status 'done', 'failed', 'read', or 'expired' older than 72 hours → deleted
  - Messages with status 'dead' (DLQ) older than 7 days → deleted
    (dead messages kept 7 days for human review before auto-cleanup)
  - Messages with status 'pending' are NEVER deleted automatically

Prints a summary of what was cleaned.
"""

import os
import sqlite3
import sys
import time

AMB_DB = os.environ.get(
    "AMB_DB_PATH",
    "/home/artofphotogrphyy/.hermes/data/agent_message_bus/agent_messages.db",
)


def get_db() -> sqlite3.Connection:
    """Open a connection to the Agent Message Bus message DB."""
    if not os.path.exists(AMB_DB):
        print(f"⚠️  Database not found: {AMB_DB}")
        sys.exit(0)
    conn = sqlite3.connect(AMB_DB)
    conn.row_factory = sqlite3.Row
    return conn


def count_old_messages(conn: sqlite3.Connection, cutoff_72h: float, cutoff_7d: float) -> dict:
    """Count how many messages would be deleted (for reporting)."""
    counts = {}
    # Standard terminal states: clean after 72h
    for status in ("done", "failed", "read", "expired"):
        cur = conn.execute(
            "SELECT COUNT(*) AS cnt FROM agent_messages "
            "WHERE status = ? AND created_at < ?",
            (status, cutoff_72h),
        )
        row = cur.fetchone()
        counts[status] = row["cnt"] if row else 0
    # Dead letter queue: keep 7 days for human review
    cur = conn.execute(
        "SELECT COUNT(*) AS cnt FROM agent_messages "
        "WHERE status = 'dead' AND created_at < ?",
        (cutoff_7d,),
    )
    row = cur.fetchone()
    counts["dead"] = row["cnt"] if row else 0
    return counts


def delete_old_messages(conn: sqlite3.Connection, cutoff_72h: float, cutoff_7d: float) -> dict:
    """Delete old messages. Returns counts deleted."""
    deleted = {}
    # Standard terminal states
    for status in ("done", "failed", "read", "expired"):
        cur = conn.execute(
            "DELETE FROM agent_messages "
            "WHERE status = ? AND created_at < ?",
            (status, cutoff_72h),
        )
        deleted[status] = cur.rowcount
    # Dead letter queue: longer retention
    cur = conn.execute(
        "DELETE FROM agent_messages "
        "WHERE status = 'dead' AND created_at < ?",
        (cutoff_7d,),
    )
    deleted["dead"] = cur.rowcount
    conn.commit()
    return deleted


def main():
    now = time.time()
    cutoff_72h = now - (72 * 3600)  # 72 hours ago
    cutoff_7d = now - (7 * 86400)   # 7 days ago (DLQ retention)

    print("🧹 **Agent Message Bus Cleanup**")
    print(f"   DB: {AMB_DB}")
    print(f"   Cutoff (72h): {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(cutoff_72h))}")
    print(f"   Cutoff (7d):  {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(cutoff_7d))}")
    print()

    conn = get_db()

    # Count before
    before = count_old_messages(conn, cutoff_72h, cutoff_7d)
    total_before = sum(before.values())

    if total_before == 0:
        print("   ✅ No old messages to clean. Database is healthy.")
        conn.close()
        return

    print("   Old messages found before cleanup:")
    for status, count in before.items():
        if count > 0:
            print(f"     - {status}: {count}")
    print()

    # Delete
    deleted = delete_old_messages(conn, cutoff_72h, cutoff_7d)
    total_deleted = sum(deleted.values())

    print(f"   🗑️  Deleted {total_deleted} message(s):")
    for status, count in deleted.items():
        if count > 0:
            print(f"     - {status}: {count}")

    # Quick sanity — count remaining
    cur = conn.execute("SELECT COUNT(*) AS cnt FROM agent_messages")
    remaining = cur.fetchone()["cnt"]
    print(f"\n   📊 Remaining in DB: {remaining} message(s)")

    conn.close()
    print("\n   ✅ Cleanup complete.")


if __name__ == "__main__":
    main()
