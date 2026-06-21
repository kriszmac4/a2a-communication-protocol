#!/home/artofphotogrphyy/.hermes/.venv/bin/python3
"""Check Marveen message bus for pending messages and report state."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path.home() / ".hermes" / "scripts"))
from agent_message_bus import get_pending_messages, DATA_DIR

# Check all statuses
pending = get_pending_messages(limit=5)
print(f"PENDING: {len(pending)}")
for m in pending:
    print(f"  #{m['id']}: {m['from_agent']}→{m['to_agent']}: {m['content'][:80]}")

# Check trigger file
trigger = Path("/tmp/amb-trigger")
if trigger.exists():
    content = trigger.read_text().strip()
    print(f"TRIGGER: exists ({len(content)} chars)")
    if content:
        print(f"  Content: {content}")
    else:
        print("  (empty)")
else:
    print("TRIGGER: not found")

# DB stats
from agent_message_bus.agent_message_bus import _get_db
conn = _get_db()
stats = conn.execute("SELECT status, COUNT(*) as cnt FROM agent_messages GROUP BY status").fetchall()
print(f"DB STATS ({DATA_DIR / 'agent_messages.db'}):")
for row in stats:
    print(f"  {row['status']}: {row['cnt']}")
