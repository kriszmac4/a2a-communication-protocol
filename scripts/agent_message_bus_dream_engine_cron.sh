#!/bin/bash
# Agent Message Bus Dream Engine — nightly consolidation cron wrapper
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
exec python3 "$SCRIPT_DIR/agent_message_bus_dream_engine.py"
