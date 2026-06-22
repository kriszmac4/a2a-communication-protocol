"""
AMB v7.2 CLI / Doctor Layer — Command-line interface and health diagnostics
for the Agent Message Bus.
"""

import os
import sys
import json
import sqlite3
from datetime import datetime
from pathlib import Path

# Ensure agent_message_bus module is importable
_SCRIPTS_DIR = str(Path(__file__).parent.parent)
if _SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, _SCRIPTS_DIR)
from agent_message_bus import _get_db, DATA_DIR


def _is_enabled() -> bool:
    return os.environ.get('AMB_CLI_ENABLED', 'true').lower() == 'true'


def amb_doctor() -> dict:
    conn = _get_db()

    router_active = conn.execute(
        "SELECT COUNT(*) FROM agent_sessions WHERE status IN ('starting', 'busy', 'idle')"
    ).fetchone()[0]

    try:
        conn.execute("SELECT 1").fetchone()
        db_reachable = True
    except Exception:
        db_reachable = False

    wal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]

    global_active = conn.execute(
        "SELECT COUNT(*) FROM agent_sessions WHERE status IN ('starting', 'busy')"
    ).fetchone()[0]
    global_max = int(os.environ.get('AMB_GLOBAL_MAX_CONCURRENT_SESSIONS', '3'))
    per_agent_max = int(os.environ.get('AMB_DEFAULT_PER_AGENT_MAX_CONCURRENT_SESSIONS', '1'))

    per_agent_rows = conn.execute(
        "SELECT agent_name, COUNT(*) AS cnt FROM agent_sessions WHERE status IN ('starting', 'busy') GROUP BY agent_name"
    ).fetchall()

    autonomy_config_path = DATA_DIR / 'autonomy-config.json'
    tool_policies_configured = False
    if autonomy_config_path.exists():
        try:
            config = json.loads(autonomy_config_path.read_text())
            tool_policies_configured = 'amb_tool_policy' in config
        except Exception:
            pass
    if not tool_policies_configured:
        has_toolsets = conn.execute(
            "SELECT COUNT(*) FROM agent_sessions WHERE toolsets IS NOT NULL"
        ).fetchone()[0]
        tool_policies_configured = has_toolsets > 0

    return {
        'router_status': 'active' if router_active > 0 else 'no_active_sessions',
        'db_reachable': db_reachable,
        'wal_enabled': wal_mode == 'wal',
        'pending_messages': conn.execute(
            "SELECT COUNT(*) FROM agent_messages WHERE status = 'pending'"
        ).fetchone()[0],
        'claimed_running_messages': conn.execute(
            "SELECT COUNT(*) FROM agent_messages WHERE status IN ('claimed', 'running')"
        ).fetchone()[0],
        'retry_scheduled_messages': conn.execute(
            "SELECT COUNT(*) FROM agent_messages WHERE status = 'retry_scheduled'"
        ).fetchone()[0],
        'failed_no_retry_messages': conn.execute(
            "SELECT COUNT(*) FROM agent_messages WHERE status = 'failed_no_retry'"
        ).fetchone()[0],
        'dead_letter_messages': conn.execute(
            "SELECT COUNT(*) FROM agent_messages WHERE status = 'dead_letter'"
        ).fetchone()[0],
        'stale_locks': conn.execute(
            "SELECT COUNT(*) FROM agent_messages WHERE lock_expires_at IS NOT NULL AND lock_expires_at < datetime('now') AND status IN ('claimed', 'running')"
        ).fetchone()[0],
        'running_attempts': conn.execute(
            "SELECT COUNT(*) FROM agent_message_attempts WHERE runtime_status IN ('created', 'running')"
        ).fetchone()[0],
        'active_sessions': global_active,
        'idle_sessions': conn.execute(
            "SELECT COUNT(*) FROM agent_sessions WHERE status = 'idle'"
        ).fetchone()[0],
        'fast_path_enabled': os.environ.get('AMB_FAST_PATH_ENABLED', 'true').lower() == 'true',
        'cold_popen_enabled': os.environ.get('AMB_WAKEUP_ENABLED', 'true').lower() == 'true',
        'tool_policies_configured': tool_policies_configured,
        'watchdog_enabled': os.environ.get('AMB_WATCHDOG_ENABLED', 'false').lower() == 'true',
        'retry_enabled': os.environ.get('AMB_RETRY_ENABLED', 'false').lower() == 'true',
        'delegate_enabled': os.environ.get('AMB_DELEGATE_WAIT_ENABLED', 'false').lower() == 'true',
        'global_concurrency': {
            'current': global_active,
            'max': global_max,
            'utilization_pct': round(global_active / global_max * 100, 1) if global_max > 0 else 0,
        },
        'per_agent_concurrency': {
            row['agent_name']: {'current': row['cnt'], 'max': per_agent_max}
            for row in per_agent_rows
        },
    }


def amb_messages() -> list[dict]:
    conn = _get_db()
    rows = conn.execute(
        """
        SELECT id, from_agent, to_agent, status, priority, created_at,
               retry_count, lock_owner, lock_expires_at, claimed_by_session
        FROM agent_messages
        WHERE status IN ('pending', 'claimed', 'running', 'retry_scheduled', 'failed_no_retry')
        ORDER BY created_at DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def amb_message_show(message_id: int) -> dict:
    conn = _get_db()
    row = conn.execute(
        "SELECT * FROM agent_messages WHERE id = :mid", {'mid': message_id}
    ).fetchone()
    if not row:
        return {'error': 'message_not_found', 'message_id': message_id}

    message = dict(row)
    attempts = conn.execute(
        "SELECT * FROM agent_message_attempts WHERE message_id = :mid ORDER BY attempt_no",
        {'mid': message_id},
    ).fetchall()
    message['attempts'] = [dict(a) for a in attempts]

    session = None
    if message.get('claimed_by_session'):
        srow = conn.execute(
            "SELECT * FROM agent_sessions WHERE session_id = :sid",
            {'sid': message['claimed_by_session']},
        ).fetchone()
        session = dict(srow) if srow else None
    message['session'] = session

    return message


def amb_attempts(message_id: int) -> list[dict]:
    conn = _get_db()
    rows = conn.execute(
        "SELECT * FROM agent_message_attempts WHERE message_id = :mid ORDER BY attempt_no",
        {'mid': message_id},
    ).fetchall()
    return [dict(row) for row in rows]


def amb_sessions() -> list[dict]:
    conn = _get_db()
    rows = conn.execute(
        "SELECT * FROM agent_sessions ORDER BY started_at DESC"
    ).fetchall()
    return [dict(row) for row in rows]


def amb_dead_letter_list() -> list[dict]:
    conn = _get_db()
    rows = conn.execute(
        """
        SELECT id, from_agent, to_agent, dead_letter_reason, dead_letter_at,
               retry_count, dead_letter_replay_count, last_replayed_at, last_error
        FROM agent_messages
        WHERE status = 'dead_letter'
        ORDER BY dead_letter_at DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def amb_dead_letter_show(message_id: int) -> dict:
    conn = _get_db()
    row = conn.execute(
        "SELECT * FROM agent_messages WHERE id = :mid AND status = 'dead_letter'",
        {'mid': message_id},
    ).fetchone()
    if not row:
        return {'error': 'dead_letter_not_found', 'message_id': message_id}
    return dict(row)


def amb_dead_letter_replay(message_id: int) -> dict:
    from amb_retry import replay_dead_letter
    return replay_dead_letter(message_id)


def amb_retry(message_id: int) -> dict:
    conn = _get_db()
    cursor = conn.execute(
        """
        UPDATE agent_messages SET
            status = 'pending',
            retry_count = 0,
            next_attempt_at = NULL,
            lock_owner = NULL,
            lock_acquired_at = NULL,
            lock_expires_at = NULL,
            updated_at = datetime('now')
        WHERE id = :mid
          AND status IN ('failed_no_retry', 'retry_scheduled', 'dead_letter')
        """,
        {'mid': message_id},
    )
    conn.commit()
    if cursor.rowcount == 0:
        return {'error': 'retry_failed', 'message_id': message_id, 'message': 'Message not in retryable status'}
    return {'status': 'pending', 'message_id': message_id, 'message': 'Message reset for retry'}


def amb_unlock(message_id: int) -> dict:
    conn = _get_db()
    cursor = conn.execute(
        """
        UPDATE agent_messages SET
            lock_owner = NULL,
            lock_acquired_at = NULL,
            lock_expires_at = NULL,
            lock_version = COALESCE(lock_version, 0) + 1,
            updated_at = datetime('now')
        WHERE id = :mid
          AND lock_owner IS NOT NULL
        """,
        {'mid': message_id},
    )
    conn.commit()
    if cursor.rowcount == 0:
        return {'error': 'unlock_failed', 'message_id': message_id, 'message': 'No lock found on this message'}
    return {'status': 'unlocked', 'message_id': message_id, 'message': 'Lock released'}


def _handle_dead_letter() -> dict | list[dict]:
    if len(sys.argv) < 3:
        return amb_dead_letter_list()
    sub = sys.argv[2]
    if sub == 'list':
        return amb_dead_letter_list()
    elif sub == 'show':
        if len(sys.argv) < 4:
            return {'error': 'missing_message_id'}
        return amb_dead_letter_show(int(sys.argv[3]))
    elif sub == 'replay':
        if len(sys.argv) < 4:
            return {'error': 'missing_message_id'}
        return amb_dead_letter_replay(int(sys.argv[3]))
    return {'error': f'unknown dead-letter subcommand: {sub}'}


if __name__ == '__main__':
    if not _is_enabled():
        print(json.dumps({'status': 'disabled', 'message': 'AMB_CLI_ENABLED=false'}))
        sys.exit(0)

    if len(sys.argv) < 2:
        result = amb_doctor()
        print(json.dumps(result, indent=2))
        sys.exit(0)

    cmd = sys.argv[1]
    commands = {
        'doctor': lambda: amb_doctor(),
        'messages': lambda: amb_messages(),
        'message': lambda: amb_message_show(int(sys.argv[2])) if len(sys.argv) >= 3 else {'error': 'missing_message_id'},
        'attempts': lambda: amb_attempts(int(sys.argv[2])) if len(sys.argv) >= 3 else {'error': 'missing_message_id'},
        'sessions': lambda: amb_sessions(),
        'dead-letter': lambda: _handle_dead_letter(),
        'retry': lambda: amb_retry(int(sys.argv[2])) if len(sys.argv) >= 3 else {'error': 'missing_message_id'},
        'unlock': lambda: amb_unlock(int(sys.argv[2])) if len(sys.argv) >= 3 else {'error': 'missing_message_id'},
    }

    handler = commands.get(cmd)
    if handler is None:
        print(json.dumps({'error': 'unknown_command', 'command': cmd}))
        sys.exit(1)

    try:
        result = handler()
        print(json.dumps(result, indent=2, default=str))
    except Exception as e:
        print(json.dumps({'error': str(e)}))
        sys.exit(1)
