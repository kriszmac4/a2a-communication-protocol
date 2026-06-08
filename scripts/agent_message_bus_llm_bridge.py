#!/usr/bin/env python3
"""
Agent Message Bus LLM Bridge — General ágens automatikus válaszadó.

no_agent watchdog — közvetlen LLM API hívással válaszol a buszon
érkező üzenetekre, kikerülve a Hermes agent réteg MCP tool korlátait.

Watchdog pattern:
- Empty stdout = silent (nincs teendő)
- Output amikor válasz készült

Védelem a végtelen ciklus ellen: a bridge_engine.should_auto_reply()
gondoskodik az auto_reply típus, chain_depth, rate limit és blacklist
ellenőrzésről.
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
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

# Melyik ágens nevében válaszolunk
AGENT_ID = "general"

# LLM API konfig
API_BASE = os.environ.get("OPENCODE_BASE_URL", "https://opencode.ai/zen/v1")
MODEL = os.environ.get("AMB_LLM_MODEL", "nemotron-3-ultra-free")
REQUEST_TIMEOUT = 300  # másodperc (LLM API néha lassú)

# Max üzenet hossz amit az LLM-nek küldünk
MAX_PROMPT_LEN = 4000

# Profil személyiségek
AGENT_PERSONAS = {
    "general": (
        "Te vagy General, a Hermes rendszer fő koordinátora és döntéshozója. "
        "Te irányítod a többi specialistát (Dev, Research, Study). "
        "Válaszolj magabiztosan, víziószerűen, magyarul. "
        "Ha véleményt kérnek, mondd el őszintén és konstruktívan."
    ),
    "dev": (
        "Te vagy Dev, a Hermes rendszer fejlesztője. "
        "Kódolással, API integrációval, scriptekkel foglalkozol. "
        "Válaszolj gyakorlatiasan, technikai részletekkel, magyarul."
    ),
    "research": (
        "Te vagy Research, a Hermes rendszer kutatója. "
        "Információt gyűjtesz, elemzel, összefoglalókat készítesz. "
        "Válaszolj tényszerűen, elemző stílusban, magyarul."
    ),
    "study": (
        "Te vagy Study, a Hermes rendszer oktatója és mentorja. "
        "Tananyagokat készítesz, magyarázol, segítesz megérteni dolgokat. "
        "Válaszolj türelmesen, didaktikusan, magyarul."
    ),
}

DEFAULT_PERSONA = (
    "Te vagy egy Hermes rendszerbeli ágens. "
    "Válaszolj tömören, segítőkészen, magyarul."
)


# ── API hívás ────────────────────────────────────────────────────────────────

def query_llm(user_message: str, system_prompt: str) -> str | None:
    """Meghívja az LLM API-t és visszaadja a választ."""
    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
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
            return f"❌ LLM Bridge: üres válasz (finish_reason: {result.get('choices', [{}])[0].get('finish_reason', '?')})"
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")[:200]
        return f"❌ LLM Bridge HTTP {e.code}: {body}"
    except urllib.error.URLError as e:
        return f"❌ LLM Bridge hálózati hiba: {e.reason}"
    except Exception as e:
        return f"❌ LLM Bridge ismeretlen hiba: {type(e).__name__}: {e}"


# ── Fő logika ────────────────────────────────────────────────────────────────

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

            # ── Auto-reply check (végtelen ciklus védelem) ──
            allowed, reason = should_auto_reply(msg, AGENT_ID)
            if not allowed:
                # Mark as read so we don't keep retrying
                mark_read(conn, msg_id)
                skipped += 1
                continue

            # System prompt a target profil alapján
            persona = AGENT_PERSONAS.get(AGENT_ID, DEFAULT_PERSONA)
            system_prompt = (
                f"{persona}\n\n"
                f"Most egy üzenet érkezett hozzád a Agent Message Buson keresztül "
                f"a `{from_agent}` agent-től. "
                f"Válaszolj a buszon keresztül vissza. "
                f"A válaszod legyen tömör, lényegretörő, magyar nyelvű."
            )

            # Felhasználói prompt építése
            content = msg.get("content", "") or ""
            user_prompt = (
                f"Üzenet `{from_agent}` agent-től (priority: {msg['priority']}):\n\n"
                f"{content}"
            )

            # LLM hívás
            llm_response = query_llm(user_prompt, system_prompt)

            if llm_response and not llm_response.startswith("❌"):
                # Chain depth számítás
                new_depth = compute_new_depth(msg)

                new_id = write_bridge_response(
                    conn, from_agent, AGENT_ID,
                    llm_response, msg_id,
                    priority=msg.get("priority", 0),
                    chain_depth=new_depth,
                )
                responses_sent += 1
                msg_label = f"✅ #{new_id}" if new_id else "❌ write failed"
                print(f"🤖 #{msg_id} {from_agent}→{AGENT_ID}: {AGENT_ID} LLM válasz (depth={new_depth}) {msg_label}")
            else:
                mark_failed(conn, msg_id)
                print(f"❌ #{msg_id} {from_agent}→{AGENT_ID}: LLM hiba — {llm_response}")

        # Watchdog output
        if responses_sent > 0:
            plural = "válasz" if responses_sent == 1 else "válasz"
            print(f"\n📬 **{AGENT_ID.upper()} Bridge — {responses_sent} {plural} elküldve**")
        elif total_msgs > 0 and skipped == total_msgs:
            pass  # Minden üzenetet kiszűrtünk (auto-reply, stb.) — nincs mit tenni

    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
