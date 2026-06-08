# Telepítési útmutató

## Előfeltételek
- Hermes Agent telepítve és fut
- Python 3.11+
- SQLite3

## 1. Marveen modul telepítése

```bash
# Másold a agent_message_bus/ modult a Hermes scripts könyvtárába
cp -r agent_message_bus/ ~/.hermes/scripts/agent_message_bus/

# És a profilod alá is (minden profilhoz, ahol használni akarod)
cp -r agent_message_bus/ ~/.hermes/profiles/dev/scripts/agent_message_bus/
cp -r agent_message_bus/ ~/.hermes/profiles/research/scripts/agent_message_bus/
cp -r agent_message_bus/ ~/.hermes/profiles/study/scripts/agent_message_bus/
```

## 2. Bridge-ek telepítése

```bash
# Másold a bridge fájlokat
cp bridges/*.py ~/.hermes/profiles/dev/scripts/
```

## 3. Service-ek telepítése

```bash
cp services/*.py ~/.hermes/profiles/dev/scripts/
```

## 4. Cron job-ok beállítása

A következő cron job-okat kell létrehozni:

```bash
# Message Router (30 másodperc)
hermes cron create --name agent_message_bus-message-router \
  --schedule "*/30 * * * * *" \
  --script "agent_message_bus_message_router.py" \
  --no-agent

# Auto-Responder (5 perc)
hermes cron create --name agent_message_bus-auto-responder \
  --schedule "*/5 * * * *" \
  --script "agent_message_bus_auto_responder.py" \
  --no-agent

# Watchdog (2 perc)
hermes cron create --name agent_message_bus-watchdog \
  --schedule "*/2 * * * *" \
  --script "agent_message_bus_watchdog.py" \
  --no-agent

# LLM Bridge-ek (3 perc) — minden ágenshez
for agent in general dev research study; do
  hermes cron create --name "agent_message_bus-llm-bridge-$agent" \
    --schedule "*/3 * * * *" \
    --script "agent_message_bus_llm_bridge_${agent}.py" \
    --no-agent
done
```

## 5. MCP Server regisztráció

Add hozzá a `~/.hermes/profiles/dev/config.yaml`-hoz:

```yaml
mcp_servers:
  agent_message_bus:
    command: python3
    args:
      - ~/.hermes/profiles/dev/scripts/agent_message_bus_mcp_server.py
    enabled: true
```

## 6. Indítás

```bash
# Adatbázis automatikusan létrejön az első használatkor
# MCP szerver újratöltése
hermes gateway restart
```
