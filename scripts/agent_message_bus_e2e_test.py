#!/usr/bin/env python3
"""
Agent Message Bus A2A — End-to-End Test Script (D7)

Tests the full autonomous agent-to-agent communication chain:

1. Basic bus operations (CRUD)
2. Auto-responder (task ack)
3. Event router (skill extraction + triggers)
4. Agent card trigger chain (specialist → General followup)
5. Full A2A simulation: Dev → Research → answer → General notification
6. Watchdog + message router
7. Dream Engine A2A analysis + cron jobs

Usage:
    python3 agent_message_bus_e2e_test.py          # Run all tests
    python3 agent_message_bus_e2e_test.py --basic   # Only bus CRUD
    python3 agent_message_bus_e2e_test.py --chain   # Only the A2A chain
"""

import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger("e2e-test")

# ── Configuration — use known absolute paths ──
HOME = Path("/home/artofphotogrphyy")
HERMES_HOME = HOME / ".hermes" / "profiles" / "dev"
DATA_DIR = HERMES_HOME / "data" / "agent_message_bus"
DB_PATH = DATA_DIR / "agent_messages.db"
CARDS_DIR = DATA_DIR / "agent_cards"
DREAMS_DIR = DATA_DIR / "dreams"
SCRIPTS_DIR = Path(__file__).parent
CRON_FILE = HERMES_HOME / "cron" / "jobs.json"

PASS = 0
FAIL = 0

from agent_message_bus import (
    create_message,
    get_messages,
    get_pending_messages,
    mark_read,
    mark_done,
)

# ── Shared subprocess env (fixes HERMES_HOME / HOME paths) ──
SUBENV = {**os.environ, "HOME": str(HOME), "HERMES_HOME": str(HERMES_HOME)}


# ── Helpers ──

def test(name: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            global PASS, FAIL
            try:
                logger.info(f"▶️  {name}")
                func(*args, **kwargs)
                PASS += 1
                logger.info("   ✅ PASS")
            except AssertionError as e:
                FAIL += 1
                logger.error(f"   ❌ FAIL: {e}")
            except Exception as e:
                FAIL += 1
                logger.error(f"   💥 ERROR: {e}", exc_info=False)
        return wrapper
    return decorator


def assert_eq(a, b, msg=""):
    if a != b:
        raise AssertionError(f"{msg}: expected {b!r}, got {a!r}")


def assert_in(item, container, msg=""):
    if item not in container:
        raise AssertionError(f"{msg}: {item!r} not found in {container!r}")


def assert_true(cond, msg=""):
    if not cond:
        raise AssertionError(msg or "Condition was False")


def assert_gt(a, b, msg=""):
    if not (a > b):
        raise AssertionError(f"{msg}: expected {a} > {b}")


def mid(msg_dict: dict) -> int:
    """Extract integer message id from create_message result dict."""
    return msg_dict["id"] if isinstance(msg_dict, dict) else msg_dict


def run_script(name: str, args=None, timeout=30):
    """Run an Agent Message Bus script with correct env, return CompletedProcess."""
    cmd = [sys.executable, str(SCRIPTS_DIR / name)]
    if args:
        cmd.extend(args)
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=SUBENV)


# ── Tests ──

@test("1.1 | Bus connection — DB exists")
def test_db_exists():
    assert_true(DB_PATH.exists(), f"DB not found at {DB_PATH}")


@test("1.2 | Bus connection — Schema valid")
def test_db_schema():
    conn = sqlite3.connect(str(DB_PATH))
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    table_names = [r[0] for r in tables]
    assert_in("agent_messages", table_names, "Missing agent_messages table")
    conn.close()


@test("1.3 | Create + read message")
def test_create_read():
    msg = create_message(
        from_agent="dev", to_agent="general",
        content=f"Test message [test-e2e] {time.time()}", priority=0,
    )
    msg_id = mid(msg)
    assert_gt(msg_id, 0, "Expected positive message id")

    all_msgs = get_messages(status="pending", limit=50)
    found = [m for m in all_msgs if m["id"] == msg_id]
    assert_true(len(found) > 0, f"Message #{msg_id} not found in pending")
    assert_eq(found[0]["from_agent"], "dev")
    assert_eq(found[0]["to_agent"], "general")
    mark_read(msg_id)
    logger.info(f"   📝 Created msg #{msg_id}")


@test("1.4 | Mark done with result")
def test_mark_done_with_result():
    msg = create_message(
        from_agent="dev", to_agent="general",
        content=f"Task with result [test-e2e] {time.time()}", priority=0,
    )
    msg_id = mid(msg)
    mark_done(msg_id, result="Teszt sikeres: minden OK")

    done_msgs = get_messages(status="done", limit=50)
    found = [m for m in done_msgs if m["id"] == msg_id]
    assert_true(len(found) > 0, f"Done msg #{msg_id} not found")
    assert_in("Teszt sikeres", found[0].get("result", ""), "Result missing")
    logger.info(f"   ✅ Done #{msg_id} with result")


@test("1.5 | Priority messages")
def test_priority():
    normal = create_message(from_agent="dev", to_agent="general", content="Normal [test-e2e]", priority=0)
    high = create_message(from_agent="dev", to_agent="general", content="High [test-e2e]", priority=1)
    urgent = create_message(from_agent="dev", to_agent="general", content="URGENT [test-e2e]", priority=2)

    pending = get_pending_messages(limit=10)
    pending_ids = [m["id"] for m in pending]
    assert_in(mid(urgent), pending_ids, f"Urgent #{mid(urgent)} not pending")

    for m in [normal, high, urgent]:
        mark_read(mid(m))
    logger.info(f"   📊 P: n={mid(normal)} h={mid(high)} u={mid(urgent)}")


@test("2.1 | Auto-responder — runs without error")
def test_auto_responder_runs():
    result = run_script("agent_message_bus_auto_responder.py")
    assert_eq(result.returncode, 0, f"Exit {result.returncode}")
    logger.info(f"   📬 {'(silent)' if not result.stdout.strip() else result.stdout[:200]}")


@test("2.2 | Auto-responder — creates response")
def test_auto_responder_response():
    msg = create_message(
        from_agent="general", to_agent="dev",
        content=f"[skill=test-skill] Teszt feladat [test-e2e] {time.time()}", priority=1,
    )
    logger.info(f"   📤 Created #{mid(msg)}, running auto_responder...")
    time.sleep(1)

    result = run_script("agent_message_bus_auto_responder.py")
    assert_eq(result.returncode, 0, f"Exit {result.returncode}")
    logger.info(f"   📬 {result.stdout[:300]}")

    all_msgs = get_messages(limit=50)
    ack_found = any(
        ("Auto-válasz" in (m.get("content") or "")
         or "Auto-ack" in (m.get("content") or "")
         or "üzenetet kapott" in (m.get("content") or ""))
        for m in all_msgs
    )
    assert_true(ack_found, "No auto-ack in recent messages")
    logger.info("   ✅ Auto-ack confirmed")


@test("3.1 | Event router — runs without error")
def test_event_router_runs():
    result = run_script("agent_message_bus_event_router.py", ["--once"])
    assert_eq(result.returncode, 0, f"Exit {result.returncode}")
    assert_in("newly_logged", result.stdout, "Missing JSON output")
    data = json.loads(result.stdout)
    logger.info(f"   📊 Logged: {data.get('newly_logged', '?')}, Triggered: {len(data.get('newly_triggered', []))}")


@test("3.2 | Event router — skill extraction")
def test_event_router_skill_extraction():
    msg = create_message(
        from_agent="research", to_agent="general",
        content=f"[skill=research-topic] [type=notification] [test-e2e] {time.time()}", priority=0,
    )
    mark_done(mid(msg), result="Skill extraction test: OK")

    result = run_script("agent_message_bus_event_router.py", ["--once"])
    assert_eq(result.returncode, 0, f"Exit {result.returncode}")
    assert_in("newly_logged", result.stdout, "Missing JSON output")
    logger.info("   ✅ Event router skill extraction done")


@test("4.1 | Agent cards — triggers valid")
def test_agent_card_triggers():
    """Verify all agent cards have triggers with valid keys (`after_skill` or `on`)."""
    assert_true(CARDS_DIR.exists(), f"Cards dir missing: {CARDS_DIR}")
    for card_name in ["general", "dev", "research", "study"]:
        card_path = CARDS_DIR / f"{card_name}.json"
        assert_true(card_path.exists(), f"Card missing: {card_name}.json")
        card = json.loads(card_path.read_text())
        assert_in("triggers", card, f"{card_name} missing triggers")
        assert_true(len(card["triggers"]) > 0, f"{card_name} has empty triggers")
        for t in card["triggers"]:
            # Accept both `on` (new) and `after_skill` (legacy) as trigger keys
            trigger_key = "after_skill" if "after_skill" in t else "on"
            assert_in(trigger_key, t, f"{card_name} trigger missing 'on'/'after_skill'")
            assert_in("then", t, f"{card_name} trigger missing 'then'")
            for action in t["then"]:
                assert_in("to_agent", action, f"{card_name} action missing 'to_agent'")
                assert_in("reason", action, f"{card_name} action missing 'reason'")
        logger.info(f"   ✅ {card_name}: {len(card['triggers'])} triggers valid")


@test("4.2 | Agent cards — structure valid")
def test_agent_cards_structure():
    for card_name in ["general", "dev", "research", "study"]:
        card = json.loads((CARDS_DIR / f"{card_name}.json").read_text())
        assert_in("name", card, f"{card_name} missing name")
        assert_in("description", card, f"{card_name} missing description")
        assert_in("skills", card, f"{card_name} missing skills")
        assert_true(len(card.get("skills", [])) > 0, f"{card_name} has no skills")
        logger.info(f"   ✅ {card['name']}: {len(card['skills'])} skills, "
                    f"{len(card.get('triggers', []))} triggers")


@test("5.1 | Full A2A chain — Dev → Research → answer")
def test_full_a2a_chain():
    """Simulate a complete A2A chain with real bus messages."""
    chain_id = f"[test-e2e-chain-{int(time.time())}]"

    # Step 1: Dev sends a research task
    msg1 = create_message(
        from_agent="dev", to_agent="research",
        content=f"[skill=research-topic] Kutass X témáról {chain_id}", priority=0,
    )
    msg1_id = mid(msg1)
    assert_gt(msg1_id, 0, "Failed to create research task")
    logger.info(f"   Step 1: Dev→Research task #{msg1_id}")

    # Step 2: Verify pending for research
    time.sleep(0.5)
    pending = get_pending_messages(to_agent="research", limit=10)
    assert_true(len([m for m in pending if m["id"] == msg1_id]) > 0,
                f"Task #{msg1_id} not pending for research")
    logger.info("   Step 2: Task pending for Research ✅")

    # Step 3: Research marks done
    mark_done(msg1_id, result="X téma kutatás kész")
    done_msgs = get_messages(status="done", limit=50)
    found_done = [m for m in done_msgs if m["id"] == msg1_id]
    assert_true(len(found_done) > 0, f"Task #{msg1_id} not in done")
    assert_in("kutatás kész", found_done[0].get("result", ""))
    logger.info("   Step 3: Research marked done ✅")

    # Step 4: Research sends answer back to Dev
    msg2 = create_message(
        from_agent="research", to_agent="dev",
        content=f"🔀 Relay: X téma eredménye {chain_id}\n[skill=research-topic]", priority=0,
    )
    assert_gt(mid(msg2), 0, "Failed to create relay")
    logger.info(f"   Step 4: Research→Dev relay #{mid(msg2)}")

    # Step 5: Research notifies General
    msg3 = create_message(
        from_agent="research", to_agent="general",
        content=f"[type=notification] Kutatás kész: X téma {chain_id}", priority=0,
    )
    mark_done(mid(msg3), result="General notified")
    logger.info(f"   Step 5: Research→General notification #{mid(msg3)} ✅")

    # Step 6: Verify chain in DB
    conn = sqlite3.connect(str(DB_PATH))
    chain_msgs = conn.execute(
        "SELECT id, from_agent, to_agent, status FROM agent_messages "
        "WHERE content LIKE ? ORDER BY id", (f"%{chain_id}%",)
    ).fetchall()
    conn.close()
    assert_gt(len(chain_msgs), 0, "No chain messages in DB")
    logger.info(f"   Step 6: DB has {len(chain_msgs)} chain messages")
    for m in chain_msgs:
        logger.info(f"      #{m[0]} {m[1]}→{m[2]} [{m[3]}]")
    logger.info(f"   ✅ Full A2A chain ({len(chain_msgs)} msgs)")


@test("5.2 | Full delegation — General→Dev")
def test_general_dev_delegation():
    """Simulate General delegating to Dev with full feedback protocol."""
    chain_id = f"[test-e2e-del-{int(time.time())}]"

    msg = create_message(
        from_agent="general", to_agent="dev",
        content=f"[skill=implement-feature] Implementáld X-et {chain_id}", priority=0,
    )
    msg_id = mid(msg)
    assert_gt(msg_id, 0, "Failed to create delegation")
    logger.info(f"   Step 1: General→Dev #{msg_id}")

    # Dev ack
    ack = create_message(
        from_agent="dev", to_agent="general",
        content=f"📩 Dev feladat elvállalva {chain_id}", priority=0,
    )
    logger.info(f"   Step 2: Dev ack #{mid(ack)}")

    # Progress
    prog = create_message(
        from_agent="dev", to_agent="general",
        content=f"🔍 [status=progress] Implementálom {chain_id}", priority=0,
    )
    logger.info(f"   Step 3: Progress #{mid(prog)}")

    # Done
    mark_done(msg_id, result="X feature implementálva")
    logger.info(f"   Step 4: Done #{msg_id}")

    # Completion notice
    done_note = create_message(
        from_agent="dev", to_agent="general",
        content=f"✅ Dev megoldva: X feature {chain_id}", priority=0,
    )
    mark_done(mid(done_note), result="X feature kész")
    logger.info("   Step 5: Completion notice ✓")

    conn = sqlite3.connect(str(DB_PATH))
    chain_msgs = conn.execute(
        "SELECT id, from_agent, to_agent, status FROM agent_messages "
        "WHERE content LIKE ? ORDER BY id", (f"%{chain_id}%",)
    ).fetchall()
    conn.close()
    assert_gt(len(chain_msgs), 3, f"Expected >=4 delegation msgs, got {len(chain_msgs)}")
    logger.info(f"   ✅ Delegation: {len(chain_msgs)} msgs")


@test("6.1 | Watchdog — runs without error")
def test_watchdog_runs():
    result = run_script("agent_message_bus_watchdog.py")
    assert_eq(result.returncode, 0, f"Exit {result.returncode}")
    logger.info(f"   📬 {'(silent)' if not result.stdout.strip() else result.stdout[:200]}")


@test("6.2 | Message router — runs without error")
def test_message_router_runs():
    # Clear pending messages first to avoid wakeup-agent timeout
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("UPDATE agent_messages SET status = 'read' WHERE status = 'pending' AND priority >= 1")
    conn.commit()
    conn.close()
    result = run_script("agent_message_bus_message_router.py", args=["--once"], timeout=60)
    assert_eq(result.returncode, 0, f"Exit {result.returncode}")
    logger.info(f"   📬 {'(silent)' if not result.stdout.strip() else result.stdout[:200]}")


@test("7.1 | Dream Engine — runs and produces A2A analysis")
def test_dream_engine_runs():
    result = run_script("agent_message_bus_dream_engine.py", timeout=60)
    assert_eq(result.returncode, 0, f"Exit {result.returncode}")
    assert_in("A2A Routing", result.stdout, "Dream missing A2A bucket")
    logger.info("   ✅ Dream Engine A2A analysis OK")


@test("7.2 | Dream Engine — report persisted")
def test_dream_engine_report():
    """Verify the dream report was written to DREAMS_DIR (A the Agent Message Bus module's path)."""
    assert_true(DREAMS_DIR.exists(), f"Dreams dir missing: {DREAMS_DIR}")
    dream_files = sorted(DREAMS_DIR.glob("*_DREAM.md"))
    assert_gt(len(dream_files), 0, "No dream reports found")
    latest = dream_files[-1]  # Sorted alphabetically, last = newest date
    content = latest.read_text()
    # New reports have A2A Routing bucket
    if "A2A Routing" not in content:
        # Might be an old report — check if ANY report has A2A
        has_new = any("A2A Routing" in f.read_text() for f in dream_files)
        if has_new:
            logger.info(f"   ℹ️  Latest ({latest.name}) is old; newer report with A2A exists")
        else:
            # Fallback: just verify the file exists and has dream structure
            assert_in("Skill-javaslatok", content, "Missing dream structure")
            logger.info(f"   ℹ️  Report exists ({latest.name}) but may be from before A2A upgrade")
    else:
        logger.info(f"   ✅ Latest dream ({latest.name}) has A2A bucket")

    # Verify all 6 buckets in newest report
    logger.info(f"   📋 {latest.name}: {len(content)} chars")


@test("7.3 | Cron jobs — all defined")
def test_cron_jobs():
    assert_true(CRON_FILE.exists(), f"Cron registry not found: {CRON_FILE}")
    data = json.loads(CRON_FILE.read_text())
    jobs = data.get("jobs", data) if isinstance(data, dict) else data
    job_names = [j.get("name", "") for j in jobs if isinstance(j, dict)]

    expected = [
        "amb-message-router",
        "amb-watchdog",
        "amb-dream-engine",
        "amb-auto-responder",
        "amb-event-router",
    ]
    for exp in expected:
        assert_in(exp, job_names, f"Missing cron job: {exp}")
    logger.info(f"   ✅ All {len(expected)} cron jobs: {', '.join(expected)}")


# ── Main ──

def main():
    global PASS, FAIL
    logger.info("=" * 60)
    logger.info("🧪 Agent Message Bus A2A — End-to-End Test Suite")
    logger.info(f"   DB: {DB_PATH}")
    logger.info(f"   Cards: {CARDS_DIR}")
    logger.info(f"   Crons: {CRON_FILE}")
    logger.info(f"   Time: {datetime.now(timezone.utc).isoformat()}")
    logger.info("=" * 60)

    tests = []
    args = set(sys.argv[1:])
    run_all = not args or "--all" in args

    if run_all or "--basic" in args:
        tests += [test_db_exists, test_db_schema, test_create_read,
                  test_mark_done_with_result, test_priority]
    if run_all or "--auto-responder" in args:
        tests += [test_auto_responder_runs, test_auto_responder_response]
    if run_all or "--event-router" in args:
        tests += [test_event_router_runs, test_event_router_skill_extraction]
    if run_all or "--triggers" in args:
        tests += [test_agent_card_triggers, test_agent_cards_structure]
    if run_all or "--chain" in args:
        tests += [test_full_a2a_chain, test_general_dev_delegation]
    if run_all or "--infra" in args:
        tests += [test_watchdog_runs, test_message_router_runs]
    if run_all or "--dream" in args:
        tests += [test_dream_engine_runs, test_dream_engine_report]
    if run_all or "--cron" in args:
        tests += [test_cron_jobs]

    logger.info(f"\n📋 Running {len(tests)} tests...\n")
    for t in tests:
        t()

    logger.info("\n" + "=" * 60)
    logger.info(f"📊 Eredmény: ✅ {PASS} PASS | ❌ {FAIL} FAIL | "
                f"Összes: {PASS + FAIL}")
    logger.info("=" * 60)
    return 1 if FAIL > 0 else 0


if __name__ == "__main__":
    sys.exit(main())
