#!/usr/bin/env python3
"""Clean up stale/empty AMB trigger file."""
import os

trigger = "/tmp/amb-trigger"
if os.path.exists(trigger):
    with open(trigger) as f:
        content = f.read().strip()
    if not content:
        os.remove(trigger)
        print(f"Removed empty trigger file: {trigger}")
    else:
        print(f"Trigger has content, keeping: {content[:200]}")
else:
    print("No trigger file found.")
