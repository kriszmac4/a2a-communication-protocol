#!/usr/bin/env python3
"""Quick script to check pending Marveen messages."""
import sys
import os
sys.path.insert(0, os.path.expanduser("~/.hermes/scripts"))
from marveen import get_pending_messages

msgs = get_pending_messages()
print(f"PENDING:{len(msgs)}")
for m in msgs:
    print(f"ID:{m['id']}|FROM:{m['from_agent']}|TO:{m['to_agent']}|{m['content'][:150]}")
# Also show recent all-status
from marveen import get_messages
all_msgs = get_messages(limit=10)
print(f"RECENT:{len(all_msgs)}")
for m in all_msgs:
    print(f"ID:{m['id']}|FROM:{m['from_agent']}|TO:{m['to_agent']}|ST:{m['status']}|{m['content'][:80]}")
