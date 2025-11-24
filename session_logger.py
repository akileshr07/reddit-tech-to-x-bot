import os
import json
import requests
from datetime import datetime

LOG_FILE = "logs/sessions.jsonl"
os.makedirs("logs", exist_ok=True)

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = os.getenv("ADMIN_ID")

def notify_admin(msg: str):
    if not BOT_TOKEN or not ADMIN_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
            data={"chat_id": ADMIN_ID, "text": msg},
            timeout=5
        )
    except Exception:
        pass

def log_session(slot_id: str, status: str, reason: str = None, meta: dict = None):
    entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "slot": slot_id,
        "status": status,
        "reason": reason,
        "meta": meta or {}
    }

    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry) + "\n")

    # Telegram alerts
    if status == "success":
        notify_admin(f"✅ Slot {slot_id} succeeded")
    else:
        notify_admin(f"❌ Slot {slot_id} failed\nReason: {reason}\nMeta: {meta}")

    return entry
