# Marveen Message Bus — A2A Communication Protocol

Agent-to-Agent kommunikációs busz Hermes profilok számára. **Pull-based üzenetbusz** SQLite + MCP + cron architektúrával, multi-agent LLM Bridge rendszerrel.

## 📋 Architektúra

```
┌──────────────────────────────────────────────────────────┐
│                    Marveen Message Bus                    │
│                     (SQLite adatbázis)                    │
├──────────────────────────────────────────────────────────┤
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌──────────────────┐   │
│  │ General    │  │ Dev        │  │ Research/Study   │   │
│  │ Agent      │  │ Agent      │  │ Agents           │   │
│  │ Bridge     │  │ Bridge     │  │ Bridges          │   │
│  └──────┬─────┘  └──────┬─────┘  └────────┬─────────┘   │
│         │               │                  │              │
│         └───────────────┼──────────────────┘              │
│                         │                                │
│                  ┌──────▼──────┐                         │
│                  │ bridge_engine.py (közös logika)       │
│                  │ • should_auto_reply()                  │
│                  │ • write_bridge_response()              │
│                  │ • Loop protection (chain_depth,        │
│                  │   rate limit, sender blacklist)        │
│                  └─────────────┘                          │
│                                                          │
│  ┌────────────┐  ┌────────────┐  ┌──────────────────┐   │
│  │ Message    │  │ Auto-      │  │ Watchdog         │   │
│  │ Router     │  │ Responder  │  │ (2p)             │   │
│  │ (30mp)     │  │ (5p)       │  │                  │   │
│  └────────────┘  └────────────┘  └──────────────────┘   │
│                                                          │
│  ┌────────────┐  ┌────────────────────────────────────┐  │
│  │ Dream      │  │ MCP Server (tool-ok agenteknek)    │  │
│  │ Engine     │  │ agent_read_messages()              │  │
│  │ (02:00UTC) │  │ agent_send_message()               │  │
│  └────────────┘  │ agent_discover()                   │  │
│                  │ agent_mark_done()                   │  │
│                  └────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

## 🧩 Komponensek

### 1. Marveen modul (`marveen/`)
A közös Python package az üzenetbuszhoz:
- **`__init__.py`** — Core DB réteg: `create_message()`, `get_pending_messages()`, `mark_read()`, `mark_done()`, adatbázis migráció
- **`schemas.py`** — Message típusok (delegate_task, request_data, status_update, auto_reply, stb.), Pydantic modellek
- **`permissions.py`** — Hozzáférés-vezérlés
- **`circuit_breaker.py`** — Hibakezelés, áramkör-megszakító minta
- **`metrics.py`** — Metrikák gyűjtése
- **`notify_target.py`** — Értesítési célpontok
- **`webhook_handler.py`** — Webhook kezelés

### 2. LLM Bridge rendszer (`bridges/`)
Minden ágensnek saját Bridge-je, közös `bridge_engine.py` logikával:
- **`bridge_engine.py`** — Központi logika: `should_auto_reply()`, `write_bridge_response()`, loop protection (auto_reply filter, chain_depth max 10, rate limit 3/60s, sender blacklist)
- **`general_bridge.py`** — General Agent automatikus válaszadó
- **`dev_bridge.py`** — Dev Agent automatikus válaszadó
- **`research_bridge.py`** — Research Agent automatikus válaszadó
- **`study_bridge.py`** — Study Agent automatikus válaszadó

### 3. Cron-alapú szolgáltatások (`services/`)
Autonóm háttérszolgáltatások (cron job-ként futtatva):
- **`message_router.py`** (30mp) — Üzenetek kézbesítése, wakeup trigger fájl írása
- **`auto_responder.py`** (5p) — Automatikus visszajelzés minden új üzenetre
- **`watchdog.py`** (2p) — Régi/függő üzenetek figyelése
- **`dream_engine.py`** (02:00 UTC) — Napi konszolidáció, memória karbantartás
- **`event_router.py`** (5p) — Skill kinyerés, trigger chain feldolgozás
- **`mcp_server.py`** — MCP tool szerver (agent_read_messages, agent_send_message, stb.)

## 🔄 Kommunikációs protokoll

### Üzenet életciklus
1. **Agent A** → `create_message()` → Bus (SQLite INSERT)
2. **Message Router** (30mp) → deliver + wakeup trigger fájl
3. **Auto-Responder** (5p) → auto-ack (status: `read`)
4. **Watchdog** (2p) → alert ha >60mp óta olvasatlan
5. **Target Agent** → `agent_read_messages()` → feldolgozás → `agent_mark_done()`
6. **LLM Bridge** (3p) → automatikus LLM válasz generálás a bridge-elt agent nevében

### Loop protection (3 réteg)
1. **Message type filter** — `auto_reply` típusú üzenetek SOHA nem kapnak választ
2. **Chain depth** — max 10 mélység (minden válasz növeli a depth-et)
3. **Rate limit** — max 3 válasz / 60 másodperc / agent pair

### Automatikus válasz (LLM Bridge)
A Bridge-ek közvetlen API hívással dolgoznak (kikerülve az MCP tool korlátot):
```
Auto-válasz csak ezekre: delegate_task, task_delegation, request_data
Auto-válas NEM készül: auto_reply, status_update, notification
```

## 🚀 Telepítés

```bash
# 1. Másold a marveen/ modult a Hermes scripts könyvtárába
cp -r marveen/ ~/.hermes/scripts/marveen/
cp -r marveen/ ~/.hermes/profiles/<profilod>/scripts/marveen/

# 2. Másold a Bridge fájlokat
cp bridges/*.py ~/.hermes/profiles/<profilod>/scripts/

# 3. Másold a service fájlokat
cp services/*.py ~/.hermes/profiles/<profilod>/scripts/

# 4. Állítsd be a cron job-okat (lásd docs/SETUP.md)
```

## 📊 Adatbázis

**Helye:** `~/.hermes/data/marveen/agent_messages.db` (SQLite)

**Tábla:** `agent_messages`
| Mező | Típus | Leírás |
|------|-------|--------|
| id | INTEGER PK | Auto-increment |
| from_agent | TEXT | Küldő agent neve |
| to_agent | TEXT | Címzett agent neve |
| content | TEXT | JSON üzenet tartalom |
| status | TEXT | pending/delivered/read/done/failed |
| priority | INTEGER | 0=normal, 1=high, 2=urgent |
| message_type | TEXT | delegate_task, request_data, stb. |
| chain_depth | INTEGER | Auto-válasz mélység (max 10) |
| reply_to | INTEGER | Eredeti üzenet ID (ha auto_reply) |
| is_auto_reply | INTEGER | 1 ha LLM Bridge generálta |
| created_at | REAL | Unix timestamp |

## 🔐 Biztonság

- **SENDERS_BLACKLIST:** `auto_responder`, `message-router`, `marveen_llm_bridge` — ezek SOHA nem kapnak LLM választ
- **Permissions:** Agent-specifikus hozzáférés-vezérlés a `permissions.py`-ban
- **Circuit breaker:** Hibák esetén automatikus áramkör-megszakítás

## 📚 Kapcsolódó

- [Hermes Agent](https://github.com/NousResearch/hermes-agent) — A framework amihez a Marveen Bus készült
- [Google A2A Protocol](https://github.com/google/A2A) — Külső agent-ek közötti kommunikáció (eltérő cél)

## 📄 Licensz

MIT
