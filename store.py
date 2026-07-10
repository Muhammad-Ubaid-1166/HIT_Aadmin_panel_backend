import os
import redis
import json
from datetime import datetime
from typing import List

# ─── Redis Connection ─────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def _key(thread_id: str) -> str:
    return f"chat:{thread_id}"

def _reply_channel(user_uid: str) -> str:
    """Channel admin publishes reply to — user WS listens here"""
    return f"chat:reply:{user_uid}"

def _incoming_channel(user_uid: str) -> str:
    """Channel user publishes message to — admin WS listens here"""
    return f"chat:incoming:{user_uid}"

def _toggle_key(thread_id: str) -> str:
    """Redis key for toggle state — persists across restarts"""
    return f"toggle:{thread_id}"

def _toggle_channel(thread_id: str) -> str:
    """Pub/Sub channel for toggle changes — replaces polling"""
    return f"toggle:change:{thread_id}"


# ─── Toggle state (Redis-backed) ──────────────────────────────────────
def get_toggle_state(thread_id: str) -> bool:
    """Get toggle state from Redis — survives server restarts"""
    return r.exists(_toggle_key(thread_id)) == 1

def set_toggle_state(thread_id: str, active: bool) -> bool:
    """
    Set toggle state in Redis and publish change to Pub/Sub.
    Returns the new state.
    """
    if active:
        r.set(_toggle_key(thread_id), "1")
    else:
        r.delete(_toggle_key(thread_id))

    # ✅ publish toggle change — both user watcher and admin WS receive this
    payload = json.dumps({
        "thread_id":    thread_id,
        "admin_active": active,
    })
    r.publish(_toggle_channel(thread_id), payload)
    return active


# ─── Save message ─────────────────────────────────────────────────────
def save_message(thread_id: str, sender: str, message: str, sender_type: str = "user") -> dict:
    entry = {
        "sender":      sender,
        "sender_type": sender_type,
        "message":     message,
        "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    r.rpush(_key(thread_id), json.dumps(entry))
    return entry

# ─── Get all messages ─────────────────────────────────────────────────
def get_messages(thread_id: str) -> List[dict]:
    raw = r.lrange(_key(thread_id), 0, -1)
    return [json.loads(m) for m in raw]

# ─── Sidebar Pub/Sub ──────────────────────────────────────────────────
SIDEBAR_CHANNEL = "sidebar:updates"

def publish_sidebar_event(thread_id: str, sender: str, message: str, timestamp: str):
    """Publish sidebar update so all admin sidebar WS clients receive it instantly"""
    payload = json.dumps({
        "thread_id": thread_id,
        "sender":    sender,
        "preview":   message,
        "timestamp": timestamp,
    })
    r.publish(SIDEBAR_CHANNEL, payload)

def get_sidebar_data() -> list[dict]:
    """Build full thread list with last-user-message previews for sidebar initial load"""
    keys = r.keys("chat:*")
    threads = []
    for key in keys:
        thread_id = key.replace("chat:", "")
        if "reply" in thread_id or "incoming" in thread_id:
            continue
        raw = r.lrange(key, 0, -1)
        if not raw:
            continue
        messages = [json.loads(m) for m in raw]
        last_user = None
        for msg in reversed(messages):
            if msg.get("sender_type") == "user":
                last_user = msg
                break
        if last_user:
            threads.append({
                "thread_id": thread_id,
                "sender":    last_user["sender"],
                "preview":   last_user["message"],
                "timestamp": last_user["timestamp"],
            })
        else:
            threads.append({
                "thread_id": thread_id,
                "sender":    thread_id[:8],
                "preview":   messages[-1]["message"] if messages else "No messages yet",
                "timestamp": messages[-1]["timestamp"] if messages else "",
            })
    threads.sort(key=lambda t: t["timestamp"], reverse=True)
    return threads


# ─── Pub/Sub: user message ────────────────────────────────────────────
def publish_user_message(user_uid: str, message: str, sender: str):
    """User sends message — publish so admin WS receives it instantly"""
    payload = json.dumps({
        "sender":      sender,
        "sender_type": "user",
        "message":     message,
        "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    r.publish(_incoming_channel(user_uid), payload)

# ─── Pub/Sub: admin reply ─────────────────────────────────────────────
def publish_admin_reply(user_uid: str, message: str):
    """Admin sends reply — publish so user WS receives it instantly"""
    payload = json.dumps({
        "sender":      "admin",
        "sender_type": "admin",
        "message":     message,
        "timestamp":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    r.publish(_reply_channel(user_uid), payload)