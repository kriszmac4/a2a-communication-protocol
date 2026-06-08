#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# Agent Message Bus Event Router — cron wrapper
# ──────────────────────────────────────────────────────────────
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
exec python3 "$SCRIPT_DIR/agent_message_bus_event_router.py" \
    --once --lookback-minutes 60
