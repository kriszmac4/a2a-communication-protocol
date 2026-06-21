#!/usr/bin/env python3
"""LLM Bridge - Study agent. Delegates to bridge_runner."""
import os
import sys

os.environ.setdefault("HERMES_HOME", os.path.expanduser("~/.hermes"))
runner = os.path.join(os.path.dirname(__file__), "bridge_runner.py")
os.execv(sys.executable, [sys.executable, runner, "study"])
