#!/usr/bin/env python3
"""
Marveen bus notification helper — bridge between bash scripts and the bus.

Usage:
    bus_notify.py <to_agent> <from_agent> <priority> [content...]

Example:
    bus_notify.py dev gateway-monitor 2 "🚨 FATAL: hermes-tutor-web.service leállt, restart sikertelen"

Priority:
    0 = normal
    1 = high
    2 = urgent

Exit code: 0 on success, 1 on failure (silent — no stdout noise for watchdogs).
"""
import sys
import os as _osx

_dir = _osx.path.dirname(_osx.path.abspath(__file__))
sys.path.insert(0, _dir)
sys.path.insert(1, _osx.path.dirname(_dir))

try:
    from agent_message_bus import create_message
except ImportError:
    # Silent fail — don't break the calling script
    sys.exit(1)

if len(sys.argv) < 4:
    sys.exit(1)

to_agent = sys.argv[1]
from_agent = sys.argv[2]

try:
    priority = int(sys.argv[3])
except ValueError:
    priority = 0

content = " ".join(sys.argv[4:]) if len(sys.argv) > 4 else ""

if not content:
    sys.exit(1)

try:
    create_message(from_agent=from_agent, to_agent=to_agent, content=content, priority=priority)
except Exception:
    sys.exit(1)
