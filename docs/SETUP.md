# Telepítési útmutató

## Előfeltételek
- Hermes Agent telepítve és fut
- Python 3.11+
- SQLite3

## 1. Marveen modul telepítése

```bash
# Másold a marveen/ modult a Hermes scripts könyvtárába
cp -r marveen/ ~/.hermes/scripts/marveen/

# És a profilod alá is (minden profilhoz, ahol használni akarod)
cp -r marveen/ ~/.hermes/profiles/dev/scripts/marveen/
cp -r marveen/ ~/.hermes/profiles/research/scripts/marveen/
cp -r marveen/ ~/.hermes/profiles/study/scripts/marveen/
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
hermes cron create --name marveen-message-router \
  --schedule "*/30 * * * * *" \
  --script "marveen_message_router.py" \
  --no-agent

# Auto-Responder (5 perc)
hermes cron create --name marveen-auto-responder \
  --schedule "*/5 * * * *" \
  --script "marveen_auto_responder.py" \
  --no-agent

# Watchdog (2 perc)
hermes cron create --name marveen-watchdog \
  --schedule "*/2 * * * *" \
  --script "marveen_watchdog.py" \
  --no-agent

# LLM Bridge-ek (3 perc) — minden ágenshez
for agent in general dev research study; do
  hermes cron create --name "marveen-llm-bridge-$agent" \
    --schedule "*/3 * * * *" \
    --script "marveen_llm_bridge_${agent}.py" \
    --no-agent
done
```

## 5. MCP Server regisztráció

Add hozzá a `~/.hermes/profiles/dev/config.yaml`-hoz:

```yaml
mcp_servers:
  marveen:
    command: python3
    args:
      - ~/.hermes/profiles/dev/scripts/marveen_mcp_server.py
    enabled: true
```

## 6. Indítás

```bash
# Adatbázis automatikusan létrejön az első használatkor
# MCP szerver újratöltése
hermes gateway restart
```
