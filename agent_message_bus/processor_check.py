#!/home/artofphotogrphyy/.hermes/.venv/bin/python3
"""Quick AMB DB check — used by the processor cron."""
import sys
sys.path.insert(0, '/home/artofphotogrphyy/.hermes/scripts')
from agent_message_bus import get_pending_messages, get_messages

pending = get_pending_messages()
print(f"PENDING_COUNT:{len(pending)}")
for m in pending:
    print(f"MSG|{m['id']}|{m['from_agent']}|{m['to_agent']}|{m['status']}|{m['content'][:200]}")

# Also check recent
recent = get_messages(limit=5)
print(f"RECENT_COUNT:{len(recent)}")
for m in recent:
    print(f"RECENT|{m['id']}|{m['from_agent']}|{m['to_agent']}|{m['status']}|{m['content'][:100]}")
