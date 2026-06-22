import os
import logging
from datetime import datetime, timedelta
# Ensure agent_message_bus module is importable
import sys as _sys
from pathlib import Path as _Path
_SCRIPTS_DIR = str(_Path(__file__).parent.parent)
if _SCRIPTS_DIR not in _sys.path:
    _sys.path.insert(0, _SCRIPTS_DIR)
from agent_message_bus import _get_db


def _is_enabled() -> bool:
    return os.environ.get('AMB_RETRY_ENABLED', 'false').lower() == 'true'


def schedule_retry(message_id: int, error_code: str, error: str) -> dict:
    if not _is_enabled():
        return mark_failed_no_retry(message_id, error_code, error)

    conn = _get_db()
    row = conn.execute(
        'SELECT retry_count FROM agent_messages WHERE id = :mid',
        {'mid': message_id}
    ).fetchone()
    if not row:
        return {'status': 'error', 'error': f'Message #{message_id} not found'}

    retry_count = row[0] or 0
    if retry_count >= 3:
        return move_to_dead_letter(
            message_id, 'Max retries (3) exceeded', error_code, error
        )

    backoff_seconds = {1: 30, 2: 120, 3: 300}.get(retry_count + 1, 300)
    next_attempt = (
        datetime.utcnow() + timedelta(seconds=backoff_seconds)
    ).isoformat()

    conn.execute(
        '''
        UPDATE agent_messages SET
          status = 'retry_scheduled', retry_count = retry_count + 1,
          next_attempt_at = :next, lock_owner = NULL,
          lock_acquired_at = NULL, lock_expires_at = NULL,
          last_error_code = :ec, last_error = :err,
          updated_at = datetime('now')
        WHERE id = :mid AND status IN ('claimed', 'running', 'cancellation_requested')
        ''',
        {'mid': message_id, 'next': next_attempt, 'ec': error_code, 'err': error},
    )
    conn.commit()

    return {
        'status': 'retry_scheduled',
        'message_id': message_id,
        'retry_count': retry_count + 1,
        'next_attempt_at': next_attempt,
    }


def move_to_dead_letter(
    message_id: int, reason: str, error_code: str = None, error: str = None
) -> dict:
    conn = _get_db()
    conn.execute(
        '''
        UPDATE agent_messages SET
          status = 'dead_letter', dead_letter_reason = :reason,
          dead_letter_at = datetime('now'), lock_owner = NULL,
          lock_acquired_at = NULL, lock_expires_at = NULL,
          last_error_code = COALESCE(:ec, last_error_code),
          last_error = COALESCE(:err, last_error),
          updated_at = datetime('now')
        WHERE id = :mid
          AND status IN ('claimed', 'running', 'retry_scheduled', 'failed_no_retry')
        ''',
        {'mid': message_id, 'reason': reason, 'ec': error_code, 'err': error},
    )
    conn.commit()

    return {'status': 'dead_letter', 'message_id': message_id, 'reason': reason}


def mark_failed_no_retry(message_id: int, error_code: str, error: str) -> dict:
    conn = _get_db()
    conn.execute(
        '''
        UPDATE agent_messages SET
          status = 'failed_no_retry', lock_owner = NULL,
          lock_acquired_at = NULL, lock_expires_at = NULL,
          last_error_code = :ec, last_error = :err,
          updated_at = datetime('now')
        WHERE id = :mid AND status IN ('claimed', 'running')
        ''',
        {'mid': message_id, 'ec': error_code, 'err': error},
    )
    conn.commit()

    return {
        'status': 'failed_no_retry',
        'message_id': message_id,
        'error_code': error_code,
    }


def replay_dead_letter(message_id: int) -> dict:
    conn = _get_db()
    conn.execute(
        '''
        UPDATE agent_messages SET
          status = 'pending', retry_count = 0, next_attempt_at = NULL,
          lock_owner = NULL, lock_acquired_at = NULL, lock_expires_at = NULL,
          dead_letter_replay_count = COALESCE(dead_letter_replay_count, 0) + 1,
          last_replayed_at = datetime('now'),
          updated_at = datetime('now')
        WHERE id = :mid AND status = 'dead_letter'
        ''',
        {'mid': message_id},
    )
    conn.commit()

    return {
        'status': 'pending',
        'message_id': message_id,
        'message': 'Replayed from dead-letter',
    }
