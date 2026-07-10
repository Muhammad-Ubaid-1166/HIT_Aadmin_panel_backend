import sys
import os
import json
import asyncio
import redis as redis_lib

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from store import (
    save_message,
    get_messages,
    publish_admin_reply,
    publish_user_message,
    get_toggle_state,
    set_toggle_state,
    _incoming_channel,
    _toggle_channel,
    SIDEBAR_CHANNEL,
    publish_sidebar_event,
    get_sidebar_data,
)

app = FastAPI(title="Receiver App", description="App 2 — Receive and reply to messages")

# ─── CORS Origins from environment variable ─────────────────────────
cors_origins_str = os.getenv("CORS_ORIGINS", "*")
cors_origins = [origin.strip() for origin in cors_origins_str.split(",")]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Schemas ──────────────────────────────────────────────────────────
class ReplyRequest(BaseModel):
    thread_id: str
    sender:    str
    message:   str

class SendMessageRequest(BaseModel):
    thread_id: str
    sender:    str
    message:   str

# ─── Redis client ─────────────────────────────────────────────────────
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis_lib.Redis.from_url(REDIS_URL, decode_responses=True)

# ─── Active admin WebSocket connections ───────────────────────────────
admin_connections: dict[str, WebSocket] = {}


# ─────────────────────────────────────────────────────────────────────
# ADMIN WEBSOCKET
# Subscribes to TWO channels:
#   1. _incoming_channel  → new user messages (pushed instantly)
#   2. _toggle_channel    → toggle state changes (no polling needed)
# ─────────────────────────────────────────────────────────────────────
@app.websocket("/ws/admin/{thread_id}")
async def admin_websocket(websocket: WebSocket, thread_id: str):
    await websocket.accept()
    admin_connections[thread_id] = websocket
    print(f"[ADMIN WS] Connected for thread {thread_id}")

    # ✅ send full history + current toggle state immediately on connect
    messages = get_messages(thread_id)
    await websocket.send_text(json.dumps({
        "type":         "history",
        "messages":     messages,
        "admin_active": get_toggle_state(thread_id),  # ✅ from Redis
    }))

    import redis.asyncio as aioredis
    ar = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    pubsub = ar.pubsub()

    # ✅ subscribe to BOTH channels at once
    await pubsub.subscribe(
        _incoming_channel(thread_id),  # user messages
        _toggle_channel(thread_id),    # toggle state changes
    )

    print(f"[ADMIN WS] Subscribed to incoming + toggle channels for {thread_id}")

    disconnect_event = asyncio.Event()

    async def listen():
        try:
            async for message in pubsub.listen():
                if disconnect_event.is_set():
                    break
                if message["type"] != "message":
                    continue

                channel = message["channel"]
                data    = json.loads(message["data"])

                # ─── user message ──────────────────────────────────
                if channel == _incoming_channel(thread_id):
                    await websocket.send_text(json.dumps({
                        "type":    "new_message",
                        "message": data,
                    }))
                    print(f"[ADMIN WS] → user msg: {data['message']}")

                # ─── toggle change ─────────────────────────────────
                elif channel == _toggle_channel(thread_id):
                    await websocket.send_text(json.dumps({
                        "type":         "toggle_change",
                        "admin_active": data["admin_active"],
                    }))
                    print(f"[ADMIN WS] → toggle: admin_active={data['admin_active']}")

        except Exception as e:
            print(f"[ADMIN WS] Listener error: {e}")
        finally:
            await pubsub.unsubscribe(
                _incoming_channel(thread_id),
                _toggle_channel(thread_id),
            )
            await ar.aclose()

    listener = asyncio.create_task(listen())

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"[ADMIN WS] Disconnected for thread {thread_id}")
    finally:
        disconnect_event.set()
        listener.cancel()
        try:
            await listener
        except asyncio.CancelledError:
            pass
        admin_connections.pop(thread_id, None)



# ─────────────────────────────────────────────────────────────────────
# SIDEBAR WEBSOCKET
# Admin panel connects here ONCE on page load
# Receives push whenever any thread gets a new message
# Replaces GET /threads + GET /inbox polling entirely
# ─────────────────────────────────────────────────────────────────────

sidebar_connections: set[WebSocket] = set()
sidebar_pubsub_cleanup: dict[int, asyncio.Event] = {}

@app.websocket("/ws/sidebar")
async def sidebar_websocket(websocket: WebSocket):
    await websocket.accept()
    sidebar_connections.add(websocket)
    conn_id = id(websocket)
    print(f"[SIDEBAR WS] Connected (total: {len(sidebar_connections)})")

    # ✅ send full thread list immediately on connect
    threads = get_sidebar_data()
    await websocket.send_text(json.dumps({
        "type":    "sidebar_init",
        "threads": threads,
    }))

    import redis.asyncio as aioredis
    ar = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    pubsub = ar.pubsub()
    await pubsub.subscribe(SIDEBAR_CHANNEL)

    disconnect_event = asyncio.Event()
    sidebar_pubsub_cleanup[conn_id] = disconnect_event

    async def listen():
        try:
            async for message in pubsub.listen():
                if disconnect_event.is_set():
                    break
                if message["type"] != "message":
                    continue
                data = json.loads(message["data"])
                payload = json.dumps({
                    "type":       "sidebar_update",
                    "thread_id":  data["thread_id"],
                    "sender":     data["sender"],
                    "preview":    data["preview"],
                    "timestamp":  data["timestamp"],
                })
                # broadcast to all connected sidebar clients
                dead: list[WebSocket] = []
                for ws in sidebar_connections:
                    try:
                        await ws.send_text(payload)
                    except Exception:
                        dead.append(ws)
                for ws in dead:
                    sidebar_connections.discard(ws)
        except Exception as e:
            print(f"[SIDEBAR WS] Listener error: {e}")
        finally:
            await pubsub.unsubscribe(SIDEBAR_CHANNEL)
            await ar.aclose()

    listener = asyncio.create_task(listen())

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        print(f"[SIDEBAR WS] Disconnected (total: {len(sidebar_connections) - 1})")
    finally:
        sidebar_connections.discard(websocket)
        disconnect_event.set()
        listener.cancel()
        try:
            await listener
        except asyncio.CancelledError:
            pass
        sidebar_pubsub_cleanup.pop(conn_id, None)

# ─── Toggle endpoints ─────────────────────────────────────────────────
@app.get("/toggle/{thread_id}")
async def get_toggle(thread_id: str):
    # ✅ reads from Redis — survives server restarts
    state = get_toggle_state(thread_id)
    return {"admin_active": state}

@app.post("/toggle/{thread_id}")
async def toggle_admin(thread_id: str):
    current  = get_toggle_state(thread_id)
    new_state = not current
    # ✅ saves to Redis + publishes to toggle channel
    set_toggle_state(thread_id, new_state)
    print(f"[TOGGLE] thread={thread_id} admin_active={new_state}")
    return {
        "thread_id":    thread_id,
        "admin_active": new_state,
        "message":      "Admin mode ON" if new_state else "Agent mode ON",
    }

# ─── Message endpoints ────────────────────────────────────────────────
@app.post("/send")
async def send_message(payload: SendMessageRequest):
    entry = save_message(
        payload.thread_id,
        payload.sender,
        payload.message,
        sender_type="user"
    )
    publish_user_message(payload.thread_id, payload.message, payload.sender)
    publish_sidebar_event(
        payload.thread_id,
        payload.sender,
        payload.message,
        entry["timestamp"],
    )
    print(f"[SEND] {payload.sender}: {payload.message}")
    return {
        "status":    "message received",
        "thread_id": payload.thread_id,
        "data":      entry,
    }

@app.get("/inbox/{thread_id}")
async def inbox(thread_id: str):
    messages = get_messages(thread_id)
    return {
        "thread_id":      thread_id,
        "total_messages": len(messages),
        "messages":       messages,
    }

@app.post("/reply")
async def reply(payload: ReplyRequest):
    entry = save_message(
        payload.thread_id,
        payload.sender,
        payload.message,
        sender_type="admin"
    )
    publish_admin_reply(payload.thread_id, payload.message)
    print(f"[REPLY] admin → {payload.thread_id}: {payload.message}")
    return {
        "status":    "reply sent",
        "thread_id": payload.thread_id,
        "data":      entry,
    }

@app.get("/threads")
async def get_threads():
    keys = r.keys("chat:*")
    thread_ids = [k.replace("chat:", "") for k in keys]
    return {"threads": thread_ids}
