#!/usr/bin/env python3
"""
Marveen LLM Bridge — Research ágens automatikus válaszadó.

no_agent watchdog — közvetlen LLM API hívással válaszol a buszon
érkező üzenetekre.

Védelem a végtelen ciklus ellen: a bridge_engine.should_auto_reply()
gondoskodik az auto_reply típus, chain_depth, rate limit és blacklist
ellenőrzésről.
"""

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

from bridge_engine import (
    get_db,
    get_pending_messages,
    write_bridge_response,
    should_auto_reply,
    compute_new_depth,
    mark_read,
    mark_failed,
    DB_PATH,
)

# ── Konfiguráció ───────────────────────────────────────────────────────────────

AGENT_ID = "research"

API_BASE = os.environ.get("OPENCODE_BASE_URL", "https://opencode.ai/zen/v1")
MODEL = os.environ.get("MARVEEN_LLM_MODEL", "nemotron-3-ultra-free")
REQUEST_TIMEOUT = 300
MAX_PROMPT_LEN = 4000

AGENT_PERSONA = (
    "Te vagy Research, a Hermes rendszer kutatója. "
    "Információt gyűjtesz, elemezel, összefoglalókat készítesz, "
    "trendeket és mintázatokat fedezel fel az adatokban. "
    "Válaszolj tényszerűen, elemző stílusban, magyarul. "
    "Ha forrásokat említesz, adj meg linkeket is."
)

SYSTEM_PROMPT = (
    f"{AGENT_PERSONA}\n\n"
    "Most egy üzenet érkezett hozzád a Marveen Message Buson keresztül. "
    "Válaszolj a buszon keresztül vissza. "
    "A válaszod legyen tömör, lényegretörő, magyar nyelvű."
)


def query_llm(user_message: str) -> str | None:
    """Meghívja az LLM API-t és visszaadja a választ."""
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message[:MAX_PROMPT_LEN]},
        ],
        "temperature": 0.7,
        "max_tokens": 600,
        "stream": False,
    }

    data = json.dumps(payload).encode("utf-8")
    url = f"{API_BASE.rstrip('/')}/chat/completions"

    req = urllib.request.Request(
        url,
        data=data,
        headers={
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            result = json.loads(resp.read().decode("utf-8"))
            content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
            if content:
                return content.strip()
            return f"❌ Research Bridge: üres válasz"
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        return f"❌ Research Bridge HTTP {e.code}: {body}"
    except urllib.error.URLError as e:
        return f"❌ Research Bridge hálózati hiba: {e.reason}"
    except Exception as e:
        return f"❌ Research Bridge ismeretlen hiba: {type(e).__name__}: {e}"


def main() -> int:
    if not DB_PATH.exists():
        return 0

    conn = get_db()
    try:
        responses_sent = 0
        total_msgs = 0
        skipped = 0

        messages = get_pending_messages(conn, AGENT_ID)
        total_msgs = len(messages)

        for msg in messages:
            msg_id = msg["id"]
            from_agent = msg["from_agent"]

            allowed, reason = should_auto_reply(msg, AGENT_ID)
            if not allowed:
                mark_read(conn, msg_id)
                skipped += 1
                continue

            content = msg.get("content", "") or ""
            user_prompt = (
                f"Üzenet `{from_agent}` agent-től (priority: {msg['priority']}):\n\n"
                f"{content}"
            )

            llm_response = query_llm(user_prompt)

            if llm_response and not llm_response.startswith("❌"):
                new_depth = compute_new_depth(msg)
                new_id = write_bridge_response(
                    conn, from_agent, AGENT_ID,
                    llm_response, msg_id,
                    priority=msg.get("priority", 0),
                    chain_depth=new_depth,
                )
                responses_sent += 1
                msg_label = f"✅ #{new_id}" if new_id else "❌ write failed"
                print(f"🤖 #{msg_id} {from_agent}→{AGENT_ID}: Research LLM válasz (depth={new_depth}) {msg_label}")
            else:
                mark_failed(conn, msg_id)
                print(f"❌ #{msg_id} {from_agent}→{AGENT_ID}: LLM hiba — {llm_response}")

        if responses_sent > 0:
            plural = "válasz" if responses_sent == 1 else "válasz"
            print(f"\n📬 **RESEARCH Bridge — {responses_sent} {plural} elküldve**")

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
