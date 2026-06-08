#!/bin/bash
# Agent Message Bus Message Router wrapper script
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"
PYTHONPATH="$SCRIPT_DIR/..:$PYTHONPATH"
exec python3 "$SCRIPT_DIR/agent_message_bus_message_router.py"
