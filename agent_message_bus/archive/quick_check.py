#!/home/artofphotogrphyy/.hermes/.venv/bin/python3
"""Quick check of pending AMB messages and trigger state."""
import sys
import os
sys.path.insert(0, '/home/artofphotogrphyy/.hermes/scripts')
from agent_message_bus import get_pending_messages, get_messages, DATA_DIR

# Check DB size
db_path = DATA_DIR / "agent_messages.db"
if db_path.exists():
    size = db_path.stat().st_size
    print(f"DB_SIZE:{size}")
else:
    print("DB_SIZE:NOT_FOUND")

# Check pending
pending = get_pending_messages()
print(f"PENDING_COUNT:{len(pending)}")
for m in pending:
    print(f"MSG|{m['id']}|{m['from_agent']}|{m['to_agent']}|{m['status']}|{m['content'][:200]}")

# Check recent messages (all statuses)
recent = get_messages(limit=10)
print(f"TOTAL_RECENT:{len(recent)}")
for m in recent:
    created = m.get('created_at', 0)
    print(f"RECENT|{m['id']}|{m['from_agent']}|{m['to_agent']}|{m['status']}|{m['content'][:100]}")

# Check trigger
trigger = "/tmp/amb-trigger"
if os.path.exists(trigger):
    with open(trigger) as f:
        content = f.read().strip()
    print(f"TRIGGER_EXISTS:YES|{content or 'EMPTY'}")
else:
    print("TRIGGER_EXISTS:NO")
