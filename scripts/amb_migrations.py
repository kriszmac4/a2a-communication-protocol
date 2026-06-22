"""
AMB v7.2 — Idempotent SQLite schema migrations and PRAGMA setup.

Always runs (no feature flag). Handles schema versioning for the
deterministic agent message bus runtime.
"""

import sqlite3
import os
from datetime import datetime
from pathlib import Path

HERMES_HOME = Path(os.environ.get('HERMES_HOME', Path.home() / '.hermes'))
DATA_DIR = HERMES_HOME / 'data' / 'agent_message_bus'
MESSAGES_DB = DATA_DIR / 'agent_messages.db'


def set_sqlite_discipline(conn: sqlite3.Connection) -> None:
    """Apply required SQLite PRAGMAs on the given connection."""
    conn.execute('PRAGMA journal_mode=WAL;')
    conn.execute('PRAGMA busy_timeout=5000;')
    conn.execute('PRAGMA foreign_keys=ON;')


def _safe_add_column(conn: sqlite3.Connection, table: str, column: str, col_type: str) -> bool:
    """Add a column to a table only if it does not already exist.

    Returns True if the column was added, False if it already existed.
    """
    existing = {row['name'] for row in conn.execute(f'PRAGMA table_info({table})').fetchall()}
    if column in existing:
        return False
    conn.execute(f'ALTER TABLE {table} ADD COLUMN {column} {col_type}')
    return True


def _safe_create_index(conn: sqlite3.Connection, table: str, column: str, index_name: str) -> None:
    """Create an index if it does not already exist."""
    conn.execute(f'CREATE INDEX IF NOT EXISTS {index_name} ON {table}({column})')


def _get_db() -> sqlite3.Connection:
    """Import and delegate to the canonical _get_db from agent_message_bus."""
    import sys as _sys
    from pathlib import Path as _Path
    _scripts = str(_Path(__file__).parent.parent)
    if _scripts not in _sys.path:
        _sys.path.insert(0, _scripts)
    from agent_message_bus import _get_db as _amb_get_db
    return _amb_get_db()


def run_migrations() -> dict:
    """Run all pending schema migrations idempotently.

    Returns a dict with keys: migrations_applied, current_version, status.
    """
    conn = _get_db()
    set_sqlite_discipline(conn)

    conn.execute('''
        CREATE TABLE IF NOT EXISTS amb_schema_migrations (
            version INTEGER PRIMARY KEY,
            applied_at TEXT NOT NULL,
            description TEXT NOT NULL
        )
    ''')

    current = conn.execute('SELECT MAX(version) FROM amb_schema_migrations').fetchone()[0]
    current_version = current if current is not None else 0

    migrations = [
        {
            'version': 1,
            'description': 'Enhance agent_messages with claim/lock/retry/dead-letter columns',
            'sql': None,
        },
        {
            'version': 2,
            'description': 'Create agent_message_attempts table',
            'sql': '''
                CREATE TABLE IF NOT EXISTS agent_message_attempts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER NOT NULL,
                    attempt_no INTEGER NOT NULL,
                    agent_name TEXT NOT NULL,
                    session_id TEXT NOT NULL,
                    runtime_status TEXT NOT NULL DEFAULT 'created',
                    dispatch_mode TEXT,
                    started_at TEXT,
                    completed_at TEXT,
                    duration_sec REAL,
                    claimed_at TEXT,
                    launch_started_at TEXT,
                    first_output_at TEXT,
                    response_observed_at TEXT,
                    exit_code INTEGER,
                    error_code TEXT,
                    error TEXT,
                    stdout_path TEXT,
                    stderr_path TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(message_id) REFERENCES agent_messages(id)
                )
            ''',
        },
        {
            'version': 3,
            'description': 'Create agent_sessions table',
            'sql': '''
                CREATE TABLE IF NOT EXISTS agent_sessions (
                    session_id TEXT PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    pid INTEGER,
                    host TEXT,
                    status TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    last_seen_at TEXT,
                    last_heartbeat_at TEXT,
                    completed_at TEXT,
                    exit_code INTEGER,
                    current_message_id INTEGER,
                    current_attempt_id INTEGER,
                    toolsets TEXT,
                    process_start_fingerprint TEXT,
                    last_error_code TEXT,
                    last_error TEXT
                )
            ''',
        },
    ]

    migrations_applied = 0

    for m in migrations:
        if m['version'] <= current_version:
            continue

        if m['version'] == 1:
            conn.execute('BEGIN')
            try:
                _migration_v1(conn)
                conn.execute(
                    'INSERT INTO amb_schema_migrations (version, applied_at, description) VALUES (?, ?, ?)',
                    (1, datetime.utcnow().isoformat(), m['description'])
                )
                conn.commit()
                migrations_applied += 1
            except Exception:
                conn.rollback()
                raise
        else:
            conn.execute('BEGIN')
            try:
                conn.execute(m['sql'])
                conn.execute(
                    'INSERT INTO amb_schema_migrations (version, applied_at, description) VALUES (?, ?, ?)',
                    (m['version'], datetime.utcnow().isoformat(), m['description'])
                )
                conn.commit()
                migrations_applied += 1
            except Exception:
                conn.rollback()
                raise

    _safe_create_index(conn, 'agent_message_attempts', 'message_id', 'idx_attempts_message')
    _safe_create_index(conn, 'agent_message_attempts', 'session_id', 'idx_attempts_session')
    _safe_create_index(conn, 'agent_sessions', 'agent_name', 'idx_sessions_agent')
    _safe_create_index(conn, 'agent_sessions', 'status', 'idx_sessions_status')

    new_current = conn.execute('SELECT MAX(version) FROM amb_schema_migrations').fetchone()[0]
    return {
        'migrations_applied': migrations_applied,
        'current_version': new_current if new_current is not None else 0,
        'status': 'ok',
    }


def _migration_v1(conn: sqlite3.Connection) -> None:
    """Add claim, lock, retry, dead-letter, response, and error columns to agent_messages."""
    columns = [
        ('claimed_by_session', 'TEXT'),
        ('claimed_at', 'TEXT'),
        ('lock_owner', 'TEXT'),
        ('lock_acquired_at', 'TEXT'),
        ('lock_expires_at', 'TEXT'),
        ('lock_version', 'INTEGER DEFAULT 0'),
        ('response_payload', 'TEXT'),
        ('responded_at', 'TEXT'),
        ('retry_count', 'INTEGER DEFAULT 0'),
        ('next_attempt_at', 'TEXT'),
        ('dead_letter_reason', 'TEXT'),
        ('dead_letter_at', 'TEXT'),
        ('dead_letter_replay_count', 'INTEGER DEFAULT 0'),
        ('last_replayed_at', 'TEXT'),
        ('last_error_code', 'TEXT'),
        ('last_error', 'TEXT'),
    ]
    for col_name, col_type in columns:
        _safe_add_column(conn, 'agent_messages', col_name, col_type)
