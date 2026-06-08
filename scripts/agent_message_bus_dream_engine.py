#!/usr/bin/env python3
"""
Agent Message Bus Dream Engine — Nightly Consolidation (02:00 UTC)

5 buckets:
1. 💡 Skill suggestions from daily patterns
2. 🧹 Memory health (tier management, vectorization)
3. 🎯 Top-3 priorities for tomorrow (from kanban)
4. 🌐 External opportunity (weekly)
5. 🛠 Skill fleet health

Output: DREAMS_DIR/YYYY-MM-DD_DREAM.md
"""

import logging
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Resolve agent_message_bus module from the script directory
import os as _os
sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
from agent_message_bus import DATA_DIR, DREAMS_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("dream-engine")

AMB_DATA_DIR = Path(os.environ.get("AMB_DATA_DIR", Path.home() / ".a2a-protocol"))


def run(cmd: list[str], timeout: int = 30) -> str:
    """Run a shell command and return stdout, or empty on failure."""
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return r.stdout.strip()
    except Exception as e:
        logger.warning(f"Command failed: {' '.join(cmd)}: {e}")
        return ""


def get_kanban_stats() -> dict:
    """Read kanban DB for task stats."""
    kanban_db = AMB_DATA_DIR / "kanban.db"
    if not kanban_db.exists():
        return {"total": 0, "in_progress": 0, "done_today": 0}
    
    import sqlite3
    try:
        conn = sqlite3.connect(str(kanban_db))
        cur = conn.cursor()
        
        total = cur.execute("SELECT COUNT(*) FROM kanban_tasks").fetchone()[0]
        in_progress = cur.execute(
            "SELECT COUNT(*) FROM kanban_tasks WHERE status = 'in_progress'"
        ).fetchone()[0]
        done_today = cur.execute(
            "SELECT COUNT(*) FROM kanban_tasks WHERE status = 'done' "
            "AND updated_at >= ?",
            (time.time() - 86400,)
        ).fetchone()[0]
        conn.close()
        return {"total": total, "in_progress": in_progress, "done_today": done_today}
    except Exception as e:
        logger.warning(f"Kanban read error: {e}")
        return {"total": 0, "in_progress": 0, "done_today": 0}


def get_skills_list() -> list[dict]:
    """List installed skills with metadata."""
    skills_dir = AMB_DATA_DIR / "skills"
    if not skills_dir.exists():
        return []
    
    skills = []
    for skill_file in skills_dir.rglob("SKILL.md"):
        try:
            content = skill_file.read_text()
            name = ""
            desc = ""
            modified = skill_file.stat().st_mtime
            for line in content.split("\n"):
                if line.startswith("name:"):
                    name = line.split(":", 1)[1].strip()
                if line.startswith("description:"):
                    desc = line.split(":", 1)[1].strip()
            skills.append({
                "name": name or skill_file.parent.name,
                "description": desc[:80],
                "modified": datetime.fromtimestamp(modified).isoformat(),
                "days_since_mod": int((time.time() - modified) / 86400),
            })
        except Exception:
            continue
    return skills


def get_memory_stats() -> dict:
    """Get memory stats from holographic memory_store.db (fact_store)."""
    import sqlite3
    from datetime import datetime

    # NOTE: HERMES_HOME env var may point to the active profile
    # (e.g. .../profiles/dev), and HOME may also be overridden.
    # Always resolve from the real user home via password database.
    import pwd
    real_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    hermes_root = real_home / ".hermes"

    dbs = {
        "root": hermes_root / "memory_store.db",
        "dev": hermes_root / "profiles" / "dev" / "memory_store.db",
    }

    result: dict = {"profiles": {}}
    total_facts = 0
    total_entities = 0

    for label, db_path in dbs.items():
        if not db_path.exists():
            result["profiles"][label] = {"status": "nem_elérhető"}
            continue

        try:
            conn = sqlite3.connect(str(db_path))
            facts = conn.execute("SELECT COUNT(*) FROM facts").fetchone()[0]
            entities = conn.execute("SELECT COUNT(*) FROM entities").fetchone()[0]
            # Most recent fact creation time from the file mtime
            file_mtime = datetime.fromtimestamp(db_path.stat().st_mtime)
            last_modified = file_mtime.strftime("%Y-%m-%d %H:%M")
            conn.close()

            result["profiles"][label] = {
                "facts": facts,
                "entities": entities,
                "last_updated": last_modified,
            }
            total_facts += facts
            total_entities += entities
        except Exception as e:
            result["profiles"][label] = {"status": "error", "error": str(e)}

    result["total_facts"] = total_facts
    result["total_entities"] = total_entities
    result["source"] = "holographic_memory"
    return result


def get_icm_stats() -> dict:
    """Get ICM stats from the ICM memories.db (long-term session memory)."""
    import sqlite3
    import pwd
    from datetime import datetime, timezone

    real_home = Path(pwd.getpwuid(os.getuid()).pw_dir)
    icm_db = real_home / ".local" / "share" / "icm" / "memories.db"

    if not icm_db.exists():
        return {"status": "nem_elérhető"}

    try:
        conn = sqlite3.connect(str(icm_db))

        total = conn.execute("SELECT COUNT(*) FROM memories").fetchone()[0]

        # By topic
        topic_rows = conn.execute(
            "SELECT topic, COUNT(*) as cnt FROM memories GROUP BY topic ORDER BY cnt DESC"
        ).fetchall()

        # By importance
        importance = {}
        for row in conn.execute(
            "SELECT importance, COUNT(*) as cnt FROM memories GROUP BY importance"
        ).fetchall():
            importance[row[0]] = row[1]

        # Recent (last 7 days)
        week_ago = datetime.now(timezone.utc).isoformat()
        recent = conn.execute(
            "SELECT COUNT(*) FROM memories WHERE created_at >= ?",
            (week_ago,)
        ).fetchone()[0]

        # Top accessed
        top_rows = conn.execute(
            "SELECT topic, summary, access_count FROM memories "
            "ORDER BY access_count DESC LIMIT 3"
        ).fetchall()

        # Avg weight & decay
        avg_weight = conn.execute(
            "SELECT AVG(weight) FROM memories"
        ).fetchone()[0] or 0.0

        conn.close()

        return {
            "status": "elérhető",
            "total": total,
            "topics": {r[0]: r[1] for r in topic_rows},
            "topics_count": len(topic_rows),
            "importance": importance,
            "recent_7d": recent,
            "avg_weight": round(avg_weight, 3),
            "top_accessed": [
                {"topic": r[0], "summary": (r[1] or "")[:60], "access_count": r[2]}
                for r in top_rows
            ],
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


def get_message_stats() -> dict:
    """Get agent message bus stats."""
    from agent_message_bus import get_messages
    pending = get_messages(status="pending", limit=0)
    delivered = get_messages(status="delivered", limit=0)
    done = get_messages(status="done", limit=0)
    return {
        "pending": len(pending),
        "delivered": len(delivered),
        "done": len(done),
    }


# =============================================================================
# Dream Engine — 5 Buckets
# =============================================================================

def bucket_1_skill_suggestions(skills: list[dict], message_stats: dict) -> str:
    """Analyze daily patterns: are there repeated patterns that suggest new skills?"""
    lines = []
    
    if message_stats["done"] >= 5:
        lines.append("- 📊 Több mint 5 üzenet lett feldolgozva ma. "
                     "Ha többször ismétlődő minta volt, érdemes skill-t írni hozzá.")
    
    if len(skills) < 5:
        lines.append(f"- 🆕 Csak {len(skills)} skill telepítve. "
                     "Ha vannak ismétlődő manuális lépések, érdemes skill-t létrehozni.")
    
    # Check for very old skills that might need updates
    old_skills = [s for s in skills if s["days_since_mod"] > 30]
    if old_skills:
        lines.append(f"- 📅 {len(old_skills)} skill 30+ napja nem módosult. "
                     f"Érdemes átnézni: {', '.join(s['name'] for s in old_skills[:5])}")
    
    if not lines:
        lines.append("- ✅ Nincs kiemelt skill-javaslat. A meglévő skill-ek fedik a mintákat.")
    
    return "\n".join(lines)


def bucket_2_memory_health(mem_stats: dict, icm_stats: dict) -> str:
    """Check memory health: Holographic fact_store + ICM long-term memory."""
    lines = []
    source = mem_stats.get("source", "unknown")

    if source == "holographic_memory":
        profiles = mem_stats.get("profiles", {})
        total_facts = mem_stats.get("total_facts", 0)
        total_entities = mem_stats.get("total_entities", 0)

        # Root profile
        root = profiles.get("root", {})
        if "facts" in root:
            lines.append(f"- 🏠 Root profil: {root['facts']} fact, "
                         f"{root['entities']} entity "
                         f"(utoljára: {root.get('last_updated', '?')})")
        else:
            lines.append(f"- 🏠 Root profil: {root.get('status', 'ismeretlen')}")

        # Dev profile
        dev = profiles.get("dev", {})
        if "facts" in dev:
            lines.append(f"- 💻 Dev profil: {dev['facts']} fact, "
                         f"{dev['entities']} entity "
                         f"(utoljára: {dev.get('last_updated', '?')})")
        else:
            lines.append(f"- 💻 Dev profil: {dev.get('status', 'ismeretlen')}")

        lines.append(f"- 🧠 Összesen: {total_facts} fact, {total_entities} entity")
        lines.append("- ✅ Holografikus memória aktív")
    elif source == "icm":
        topics = mem_stats.get("topics_count", 0)
        lines.append(f"- 📚 ICM memória: {topics} topic")
        lines.append("- ✅ Memória rendszer aktív")
    elif source == "mem0":
        entries = mem_stats.get("approx_entries", 0)
        lines.append(f"- 📚 Mem0: kb. {entries} bejegyzés")
    else:
        lines.append("- ⚠️ Memória provider nem található")

    # === ICM Long-term Memory Stats ===
    lines.append("")
    icm_status = icm_stats.get("status", "nem_elérhető")
    if icm_status == "elérhető":
        total = icm_stats.get("total", 0)
        topics = icm_stats.get("topics", {})
        topics_count = icm_stats.get("topics_count", 0)
        recent = icm_stats.get("recent_7d", 0)
        avg_weight = icm_stats.get("avg_weight", 0)
        importance = icm_stats.get("importance", {})

        lines.append(f"  **🧠 ICM hosszútávú memória:**")
        lines.append(f"  - 📚 {total} memory | {topics_count} topic | súlyozás: {avg_weight}")
        lines.append(f"  - 🔥 {recent} új bejegyzés az elmúlt 7 napban")
        if importance:
            imp_line = " | ".join(f"{k}: {v} db" for k, v in sorted(importance.items()))
            lines.append(f"  - 📊 Fontosság: {imp_line}")
        if topics:
            topic_line = ", ".join(f"`{t}` ({c})" for t, c in list(topics.items())[:6])
            lines.append(f"  - 🏷️ Topic-ok: {topic_line}")
        # Top accessed
        top_acc = icm_stats.get("top_accessed", [])
        if top_acc:
            for item in top_acc:
                lines.append(f"  - ⭐ Legtöbbet használt: `{item['topic']}` — "
                             f"\"{item['summary']}\" ({item['access_count']}x)")

        # Health assessment
        if recent == 0 and total > 0:
            lines.append("  - ⚠️ 0 új bejegyzés 7 napja — lehet, hogy az agentek "
                         "nem mentenek ICM-be")
        elif total == 0:
            lines.append("  - ⚠️ Nincs ICM bejegyzés — a SOUL.md előírja a használatot, "
                         "de még nem történt mentés")
        elif avg_weight < 0.5:
            lines.append("  - ⚠️ Alacsony átlagsúly — a memóriák régi lehet, "
                         "érdemes újakat menteni")
        else:
            lines.append("  - ✅ ICM memória egészséges")
    else:
        lines.append(f"  **🧠 ICM hosszútávú memória:** ⚠️ Nem elérhető "
                     f"({icm_stats.get('error', icm_status)})")

    # Check dream engine history
    dreams = sorted(DREAMS_DIR.glob("*.md"))
    if dreams:
        days = len(dreams)
        lines.append(f"- 📋 Dream Engine: {days} éjszaka óta aktív")

    return "\n".join(lines)


def bucket_3_top_priorities(kanban: dict) -> str:
    """Top 3 priorities for tomorrow based on kanban state."""
    lines = []
    
    if kanban["in_progress"] > 0:
        lines.append(f"- 🔄 {kanban['in_progress']} folyamatban lévő feladat — folytatás holnap")
    
    if kanban["done_today"] > 0:
        lines.append(f"- ✅ {kanban['done_today']} feladat teljesítve ma")
    
    if kanban["total"] > 0:
        pending = kanban["total"] - kanban["in_progress"]
        lines.append(f"- 📋 {pending} hátralévő feladat a kanban táblában")
    else:
        lines.append("- 📋 Nincs kanban feladat")
    
    return "\n".join(lines)


def bucket_4_external_opportunity(day_of_year: int) -> str:
    """Weekly external opportunity check (every 7 days)."""
    if day_of_year % 7 != 0:
        return "_Heti külső keresés napja még nem esett. (Következő: 7 nap múlva)_"
    
    return ("- 🔍 Heti külső opportunity keresés napja van!\n"
            "- Érdemes új skill-eket vagy eszközöket keresni a piacon.")


def read_skill_audit() -> dict | None:
    """Load skill_audit.json if it exists."""
    audit_path = DATA_DIR / "skill_audit.json"
    if not audit_path.exists():
        return None
    try:
        import json
        return json.loads(audit_path.read_text())
    except Exception as e:
        logger.warning(f"Skill audit read error: {e}")
        return None


def bucket_5_skill_fleet_health(skills: list[dict]) -> str:
    """Check skill fleet health + audit-based improvement plan."""
    lines = []
    
    if not skills:
        return "- ⚠️ Nincsenek telepített skill-ek"
    
    total = len(skills)
    recent = sum(1 for s in skills if s["days_since_mod"] < 7)
    stale = sum(1 for s in skills if s["days_since_mod"] > 60)
    
    lines.append(f"- 📦 {total} skill telepítve")
    lines.append(f"- 🆕 {recent} frissítve az elmúlt 7 napban")
    
    if stale > 0:
        lines.append(f"- ⚠️ {stale} skill 60+ napja nem módosult")
    else:
        lines.append("- ✅ Nincs elavult skill")
    
    # === Audit-based improvement plan ===
    audit = read_skill_audit()
    if audit is not None:
        lines.append("")
        lines.append("**📋 Audit alapú fejlesztési terv:**")
        
        meta = audit.get("meta", {})
        audited_count = meta.get("total_skills_audited", 0)
        avg_score = meta.get("scores", {}).get("average", 0)
        lines.append(f"  - 🧪 Utolsó audit: {meta.get('audit_date', '?')} | "
                     f"{audited_count} skill auditálva | "
                     f"Átlag: {avg_score}/100")
        
        # Count grades
        grades = {}
        for s in audit.get("skills", []):
            g = s.get("grade", "?")
            grades[g] = grades.get(g, 0) + 1
        grade_line = "  - 📊 Értékelés: " + " | ".join(
            f"{g}: {c} db" for g, c in sorted(grades.items())
        )
        lines.append(grade_line)
        
        # Top improvements by effort level
        low = []
        medium = []
        high = []
        for s in audit.get("skills", []):
            for imp in s.get("improvements", []):
                item = f"    - `{s['name']}`: {imp['action']}"
                effort = imp.get("effort", "medium")
                if effort == "low":
                    low.append(item)
                elif effort == "medium":
                    medium.append(item)
                else:
                    high.append(item)
        
        if low:
            lines.append("")
            lines.append("  **✅ Quick win (alacsony erőfeszítés):**")
            lines.extend(low)
        if medium:
            lines.append("")
            lines.append("  **⚡ Közepes erőfeszítés:**")
            lines.extend(medium)
        if high:
            lines.append("")
            lines.append("  **🚧 Nagyobb refaktor:**")
            lines.extend(high)
    
    return "\n".join(lines)


def get_message_flow_stats() -> dict:
    """Analyze A2A message flow patterns from the bus."""
    import sqlite3
    db_path = DATA_DIR / "agent_messages.db"
    if not db_path.exists():
        return {"flows": {}, "total": 0}

    try:
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row

        # Flow analysis: from_agent → to_agent counts
        rows = conn.execute(
            "SELECT from_agent, to_agent, status, COUNT(*) as cnt "
            "FROM agent_messages "
            "WHERE created_at > ? "
            "GROUP BY from_agent, to_agent, status "
            "ORDER BY cnt DESC LIMIT 30",
            (time.time() - 86400 * 7,)  # last 7 days
        ).fetchall()

        from collections import defaultdict
        flows: dict[str, dict] = defaultdict(lambda: {"sent": 0, "received": 0, "pending": 0, "done": 0})
        total = 0
        for r in rows:
            key = f"{r['from_agent']}→{r['to_agent']}"
            st = r["status"]
            cnt = r["cnt"]
            total += cnt
            # Accumulate per agent pair
            if key not in flows:
                flows[key] = {"sent": 0, "received": 0, "pending": 0, "done": 0}
            if st == "pending":
                flows[key]["pending"] = cnt
            elif st == "done":
                flows[key]["done"] = cnt
            else:
                flows[key]["sent"] += cnt

            # Also track per agent
            agent_in = r["to_agent"]
            agent_out = r["from_agent"]
            for a in [agent_in, agent_out]:
                if a not in flows:
                    flows[a] = {"sent": 0, "received": 0, "pending": 0, "done": 0}
            flows[agent_in]["received"] += cnt
            flows[agent_out]["sent"] += cnt

        conn.close()
        return {"flows": dict(flows), "total": total}
    except Exception as e:
        logger.warning(f"Message flow stats error: {e}")
        return {"flows": {}, "total": 0}


def bucket_6_a2a_routing_analysis(flow_stats: dict, message_stats: dict) -> str:
    """Analyze A2A routing patterns and suggest improvements.

    Looks at message flow between agents, pending ratios, and routing gaps.
    """
    lines = []
    flows = flow_stats.get("flows", {})
    total_messages = flow_stats.get("total", 0)

    if total_messages == 0:
        return "- 📭 Nincs üzenetforgalom az elmúlt 7 napban."

    # Total message volume
    lines.append(f"- 📊 **{total_messages}** üzenet az elmúlt 7 napban")

    # Pending ratio
    total_pending = message_stats.get("pending", 0)
    total_delivered = message_stats.get("delivered", 0)
    total_done = message_stats.get("done", 0)
    all_msgs = total_pending + total_delivered + total_done
    if all_msgs > 0:
        done_ratio = total_done / all_msgs * 100
        pending_ratio = total_pending / all_msgs * 100
        lines.append(f"- ✅ {done_ratio:.0f}% üzenet lezárva | ⏳ {pending_ratio:.0f}% függőben")

    # Top agent pairs (who talks to whom)
    flow_pairs = {k: v for k, v in flows.items() if "→" in k}
    sorted_pairs = sorted(flow_pairs.items(), key=lambda x: x[1]["sent"], reverse=True)[:5]
    if sorted_pairs:
        lines.append("")
        lines.append("**🔁 Top üzenet folyamok:**")
        for pair_name, pair_stats in sorted_pairs:
            if pair_stats["sent"] > 0:
                lines.append(f"  - `{pair_name}` → {pair_stats['sent']} küldve")

    # Detect agent pairs with high pending ratio
    high_pending_pairs = []
    for pair_name, pair_stats in flow_pairs.items():
        total_for_pair = pair_stats["sent"] + pair_stats["done"]
        if total_for_pair > 3 and pair_stats.get("pending", 0) > 0:
            pending_ratio_pair = pair_stats["pending"] / (total_for_pair + pair_stats["pending"]) * 100
            if pending_ratio_pair > 30:
                high_pending_pairs.append((pair_name, pending_ratio_pair))
    if high_pending_pairs:
        lines.append("")
        lines.append("**⚠️ Magas függőben arány — routing probléma lehet:**")
        for pair_name, ratio in high_pending_pairs[:3]:
            lines.append(f"  - `{pair_name}` → {ratio:.0f}% pending")
        lines.append("  → Javaslat: ellenőrizd, hogy a target agent olvassa-e az üzeneteket")

    # Check if all agent pairs have configured triggers
    agent_pairs = set()
    for k in flow_pairs:
        parts = k.split("→")
        if len(parts) == 2:
            agent_pairs.add((parts[0], parts[1]))

    # Suggest new trigger patterns
    lines.append("")
    lines.append("**💡 Trigger javaslatok:**")
    suggestions_given = 0
    # Common patterns that should have triggers
    suggested_pairs = [
        ("research", "general", "Kutatás után General értesítése — már definiálva"),
        ("dev", "general", "Fejlesztés után General értesítése — már definiálva"),
        ("study", "general", "Tanulás után General értesítése — már definiálva"),
    ]
    for from_a, to_a, msg in suggested_pairs:
        if (from_a, to_a) in agent_pairs or f"{from_a}→{to_a}" in flow_pairs:
            lines.append(f"  - ✅ `{from_a}→{to_a}`: {msg}")
            suggestions_given += 1

    # Look for "orphaned" messages — from agents with no reply
    if total_pending > 5:
        lines.append(f"  - 🔍 {total_pending} függőben lévő üzenet — "
                     "lehet, hogy új trigger vagy routing szabály kell")

    if not lines:
        lines.append("- ✅ Nincs kiemelt A2A routing javaslat")

    return "\n".join(lines)


def generate_dream() -> str:
    """Generate the full dream report with 7 buckets (including A2A routing + ICM)."""
    now = datetime.now(timezone.utc)
    day_of_year = now.timetuple().tm_yday

    logger.info("🌙 Dream Engine indul — 7 bucket elemzés (A2A routing + ICM)")

    # Collect data
    kanban = get_kanban_stats()
    skills = get_skills_list()
    mem_stats = get_memory_stats()
    icm_stats = get_icm_stats()
    msg_stats = get_message_stats()
    flow_stats = get_message_flow_stats()

    # Generate buckets
    b1 = bucket_1_skill_suggestions(skills, msg_stats)
    b2 = bucket_2_memory_health(mem_stats, icm_stats)
    b3 = bucket_3_top_priorities(kanban)
    b4 = bucket_4_external_opportunity(day_of_year)
    b5 = bucket_5_skill_fleet_health(skills)
    b6 = bucket_6_a2a_routing_analysis(flow_stats, msg_stats)

    # Assemble report
    report = f"""# 🌙 Dream Engine — {now.strftime('%Y-%m-%d')}

> Automatikus éjszakai konszolidáció — {now.strftime('%H:%M UTC')}

---

## 💡 Skill-javaslatok
{b1}

## 🧹 Memória-egészség
{b2}

## 🎯 Holnapi prioritások
{b3}

## 🌐 Külső opportunity
{b4}

## 🛠 Skill-flotta health
{b5}

## 🔀 A2A Routing Elemzés
{b6}

---

*Dream Engine automatikusan fut minden éjjel 02:00 UTC-kor.*
"""
    return report


def main():
    DREAMS_DIR.mkdir(parents=True, exist_ok=True)
    report = generate_dream()
    
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    output_path = DREAMS_DIR / f"{date_str}_DREAM.md"
    output_path.write_text(report)
    
    print(report)
    logger.info(f"✅ Dream Engine jelentés mentve: {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
