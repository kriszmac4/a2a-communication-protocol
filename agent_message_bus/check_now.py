#!/usr/bin/env python3
"""Check pending Marveen messages via DB directly."""
import sys
import os
sys.path.insert(0, os.path.expanduser("~/.hermes/scripts"))
from agent_message_bus import get_pending_messages, get_messages

pending = get_pending_messages()
print(f"PENDING:{len(pending)}")
for m in pending:
    print(f"MSG|{m['id']}|{m['from_agent']}|{m['to_agent']}|{m['status']}|{m['content'][:200]}")

recent = get_messages(limit=5)
print(f"RECENT:{len(recent)}")
for m in recent:
    created = m.get('created_at', 0)
    print(f"RECENT|{m['id']}|{m['from_agent']}|{m['to_agent']}|{m['status']}|{m['content'][:100]}")
