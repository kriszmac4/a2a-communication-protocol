#!/usr/bin/env python3
"""
AMB Runtime Cleanup — One-shot maintenance script.

1. Migrates messages from dev profile DB to global DB
2. Cleans stale sessions (ended_at IS NULL) in all state.db files
3. Marks old 'read' messages as 'done' (older than 1 hour)
4. Deletes stale wakeup_pending.json files
5. Reports what was cleaned
"""
import os
import sqlite3
import time
from pathlib import Path

HERMES_HOME = Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes")))
GLOBAL_DB = HERMES_HOME / "data" / "agent_message_bus" / "agent_messages.db"
DEV_DB = HERMES_HOME / "profiles" / "dev" / "data" / "agent_message_bus" / "agent_messages.db"
PROFILES = ["dev", "general", "research", "study", "devops", "telegram", "gf"]

def migrate_dev_to_global():
    """Migrate all messages from dev DB to global DB."""
    if not DEV_DB.exists():
        print(f"  SKIP: dev DB not found at {DEV_DB}")
        return 0
    if not GLOBAL_DB.exists():
        print(f"  SKIP: global DB not found at {GLOBAL_DB}")
        return 0
    
    dev_conn = sqlite3.connect(str(DEV_DB))
    dev_conn.row_factory = sqlite3.Row
    glob_conn = sqlite3.connect(str(GLOBAL_DB))
    
    # Get max ID in global DB to avoid ID conflicts
    glob_cur = glob_conn.cursor()
    glob_cur.execute("SELECT MAX(id) FROM agent_messages")
    max_glob_id = glob_cur.fetchone()[0] or 0
    
    # Get all messages from dev DB
    dev_cur = dev_conn.cursor()
    dev_cur.execute("SELECT * FROM agent_messages")
    rows = dev_cur.fetchall()
    
    migrated = 0
    for row in rows:
        row_dict = dict(row)
        old_id = row_dict.pop("id")
        # Insert into global DB with new auto-incremented ID
        columns = ", ".join(row_dict.keys())
        placeholders = ", ".join(["?"] * len(row_dict))
        try:
            glob_cur.execute(
                f"INSERT INTO agent_messages ({columns}) VALUES ({placeholders})",
                list(row_dict.values())
            )
            migrated += 1
        except sqlite3.IntegrityError:
            pass  # skip duplicates
    
    glob_conn.commit()
    glob_conn.close()
    dev_conn.close()
    print(f"  Migrated {migrated} messages from dev DB to global DB")
    return migrated

def clean_stale_sessions():
    """Mark all stale sessions (ended_at IS NULL) as ended."""
    cleaned = 0
    for profile in PROFILES:
        state_db = HERMES_HOME / "profiles" / profile / "state.db"
        if not state_db.exists():
            # Also check global state.db
            continue
        try:
            conn = sqlite3.connect(str(state_db))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sessions WHERE ended_at IS NULL")
            stale_count = cur.fetchone()[0]
            if stale_count > 0:
                cur.execute(
                    "UPDATE sessions SET ended_at = COALESCE(started_at, strftime('%s','now')) "
                    "WHERE ended_at IS NULL"
                )
                conn.commit()
                print(f"  {profile}: closed {stale_count} stale sessions")
                cleaned += stale_count
            conn.close()
        except Exception as e:
            print(f"  {profile}: error - {e}")
    
    # Also clean global state.db
    glob_state = HERMES_HOME / "state.db"
    if glob_state.exists():
        try:
            conn = sqlite3.connect(str(glob_state))
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM sessions WHERE ended_at IS NULL")
            stale_count = cur.fetchone()[0]
            if stale_count > 0:
                cur.execute(
                    "UPDATE sessions SET ended_at = COALESCE(started_at, strftime('%s','now')) "
                    "WHERE ended_at IS NULL"
                )
                conn.commit()
                print(f"  global: closed {stale_count} stale sessions")
                cleaned += stale_count
            conn.close()
        except Exception as e:
            print(f"  global: error - {e}")
    
    return cleaned

def mark_old_read_as_done():
    """Mark 'read' messages older than 1 hour as 'done'."""
    if not GLOBAL_DB.exists():
        return 0
    cutoff = time.time() - 3600  # 1 hour ago
    conn = sqlite3.connect(str(GLOBAL_DB))
    cur = conn.cursor()
    cur.execute(
        "UPDATE agent_messages SET status='done', completed_at=? "
        "WHERE status='read' AND created_at < ?",
        (time.time(), cutoff)
    )
    updated = cur.rowcount
    conn.commit()
    conn.close()
    print(f"  Marked {updated} old 'read' messages as 'done'")
    return updated

def clean_wakeup_triggers():
    """Delete stale wakeup_pending.json files."""
    deleted = 0
    for profile in PROFILES:
        trigger = HERMES_HOME / "profiles" / profile / "data" / "agent_message_bus" / "wakeup_pending.json"
        if trigger.exists():
            trigger.unlink()
            print(f"  Deleted: {trigger}")
            deleted += 1
    return deleted

def main():
    print("🧹 AMB Runtime Cleanup")
    print(f"  HERMES_HOME: {HERMES_HOME}")
    print()
    
    print("1. Migrating dev DB → global DB...")
    migrate_dev_to_global()
    print()
    
    print("2. Cleaning stale sessions...")
    clean_stale_sessions()
    print()
    
    print("3. Marking old 'read' messages as 'done'...")
    mark_old_read_as_done()
    print()
    
    print("4. Cleaning wakeup triggers...")
    clean_wakeup_triggers()
    print()
    
    print("✅ Cleanup complete!")

if __name__ == "__main__":
    main()
