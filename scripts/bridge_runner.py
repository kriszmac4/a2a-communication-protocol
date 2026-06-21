#!/usr/bin/env python3
"""Unified entry point for all Hermes agents. Processes inbox via LLM bridge."""
import os
import sys
import json
import urllib.request
import urllib.error
from pathlib import Path
from dotenv import load_dotenv

_HERMES_HOME = Path.home() / ".hermes"
load_dotenv(_HERMES_HOME / ".env")
os.environ.setdefault("HERMES_HOME", str(_HERMES_HOME))

sys.path.insert(0, str(_HERMES_HOME / "scripts" / "bus"))
sys.path.insert(0, str(_HERMES_HOME / "scripts"))

from bridge_engine import (
    get_db,
    get_pending_messages,
    write_bridge_response,
    should_auto_reply,
    compute_new_depth,
    mark_read,
    mark_failed,
    DB_PATH,
    load_provider_config,
    load_soul_persona,
)

REQUEST_TIMEOUT = 300
MAX_PROMPT_LEN = 4000


def query_llm(user_message: str, system_prompt: str, provider: dict) -> str | None:
    """Call the LLM API using provider config and return the response text."""
    base_url = provider.get("base_url", "https://opencode.ai/zen/v1")
    api_key = provider.get("api_key", "")
    model = provider.get("model", "deepseek-v4-flash-free")

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message[:MAX_PROMPT_LEN]},
        ],
        "temperature": 0.7,
        "max_tokens": 600,
        "stream": False,
    }

    data = json.dumps(payload).encode("utf-8")
    url = f"{base_url.rstrip('/')}/chat/completions"

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if content:
                return content.strip()
            finish = result.get("choices", [{}])[0].get("finish_reason", "?")
            return None
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        return None
    except urllib.error.URLError as e:
        return None
    except TimeoutError:
        return None
    except Exception as e:
        return None


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: bridge_runner.py <agent_id>", file=sys.stderr)
        return 1

    agent_id = sys.argv[1]

    if not DB_PATH.exists():
        return 0

    provider = load_provider_config(agent_id)
    persona = load_soul_persona(agent_id)

    conn = get_db()
    try:
        responses_sent = 0
        total_msgs = 0
        skipped = 0

        messages = get_pending_messages(conn, agent_id)
        total_msgs = len(messages)

        for msg in messages:
            msg_id = msg["id"]
            from_agent = msg["from_agent"]

            allowed, reason = should_auto_reply(msg, agent_id)
            if not allowed:
                mark_read(conn, msg_id)
                skipped += 1
                continue

            system_prompt = (
                f"{persona}\n\n"
                f"A message arrived via the Agent Message Bus from `{from_agent}`. "
                f"Reply back through the bus. Be concise and direct."
            )

            content = msg.get("content", "") or ""
            user_prompt = (
                f"Message from `{from_agent}` (priority: {msg['priority']}):\n\n"
                f"{content}"
            )

            llm_response = query_llm(user_prompt, system_prompt, provider)

            if llm_response:
                new_depth = compute_new_depth(msg)
                new_id = write_bridge_response(
                    conn, from_agent, agent_id,
                    llm_response, msg_id,
                    priority=msg.get("priority", 0),
                    chain_depth=new_depth,
                )
                responses_sent += 1
                label = f"#{new_id}" if new_id else "write failed"
                print(f"#{msg_id} {from_agent}->{agent_id}: OK depth={new_depth} {label}")
            else:
                mark_failed(conn, msg_id)
                print(f"#{msg_id} {from_agent}->{agent_id}: FAIL {llm_response}")

        if responses_sent:
            w = "response" if responses_sent == 1 else "responses"
            print(f"{agent_id}: {responses_sent} {w} sent")

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
