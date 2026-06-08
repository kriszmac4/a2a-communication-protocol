#!/home/artofphotogrphyy/.hermes/.venv/bin/python3
"""Quick AMB bus check - reads via import."""
import sys
sys.path.insert(0, '/home/artofphotogrphyy/.hermes/scripts')
from agent_message_bus import get_pending_messages, get_messages
import os

# Check trigger
trigger = "/tmp/amb-trigger"
if os.path.exists(trigger):
    with open(trigger) as f:
        content = f.read().strip()
    print(f"TRIGGER:/tmp/amb-trigger|{repr(content)}")
else:
    print("TRIGGER:NOT_FOUND")

# Check DB
pending = get_pending_messages()
print(f"PENDING:{len(pending)}")
for m in pending:
    print(f"  #{m['id']} {m['from_agent']}→{m['to_agent']}: {m['content'][:200]}")

# Check all recent non-done
recent = get_messages(limit=20)
print(f"TOTAL_RECENT:{len(recent)}")
for m in recent:
    print(f"  #{m['id']} [{m['status']}] {m['from_agent']}→{m['to_agent']}: {m['content'][:100]}")
