# Agent Message Bus — Architektúra

## Áttekintés

Az Agent Message Bus egy pull-based agent-to-agent kommunikációs busz, ami Hermes profile-ok közötti aszinkron üzenetküldést tesz lehetővé. SQLite adatbázisra épül, MCP tool-okon keresztül érhető el az agentek számára, és cron job-ok biztosítják az autonóm működést.

## Rétegek

### 1. Adatréteg (SQLite)
- `agent_messages.db` — közös adatbázis, minden agent ugyanazt használja
- Egyszerű, relációs séma (nincs szükség külön DB szerverre)
- Thread-safe: SQLite WAL módban

### 2. Service réteg (cron job-ok)
- **Message Router** (30 másodperc) — pending→delivered, trigger fájl
- **Auto-Responder** (5 perc) — auto-ack, task detektálás
- **Watchdog** (2 perc) — régi üzenetek figyelése
- **LLM Bridge-ek** (3 perc) — automatikus LLM válaszok
- **Dream Engine** (02:00 UTC) — napi karbantartás

### 3. MCP Tool réteg
Agentek számára elérhető tool-ok:
- `agent_read_messages()` — bejövő üzenetek olvasása
- `agent_send_message()` — üzenet küldése másik agentnek
- `agent_mark_done()` — feldolgozás jelzése
- `agent_discover()` — agent keresés task alapján
- `agent_list_cards()` — összes agent listázása

## Adatfolyam

```
┌─────────┐     ┌─────────────────────┐     ┌─────────┐
│ Agent A │────▶│   Agent Message Bus  │────▶│ Agent B │
│ (küldő) │     │       (SQLite)       │     │(címzett)│
└─────────┘     └──────────┬───────────┘     └─────────┘
                           │
                   ┌───────▼────────┐
                   │   Cron réteg    │
                   │  • Router      │
                   │  • Responder   │
                   │  • Watchdog    │
                   │  • Bridge-ek   │
                   └────────────────┘
```

## Loop Protection

A végtelen ciklus elkerülésére 3 mechanizmus:

1. **Message Type Filter**: `auto_reply` típusú üzenetekre SOHA nem generálódik újabb válasz
2. **Chain Depth**: Minden auto-válasz növeli a `chain_depth` értéket. Max 10 mélység után a Bridge nem válaszol.
3. **Rate Limit**: Agent páronként max 3 auto-válasz / 60 másodperc. Ezt is a `bridge_engine.py` kényszeríti ki.
4. **Sender Blacklist**: bizonyos service-ek (`auto_responder`, `message-router`, `agent_message_bus_llm_bridge`) soha nem kapnak választ
