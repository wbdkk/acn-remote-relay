#!/usr/bin/env python3
"""
ACN Remote - Relay Server (Signaling Server)
Provides room-based connection for remote control.
Deploy to Render.com / Railway / any cloud platform.

Protocol:
  - Host joins:  {"type":"host", "room":"123456", "ws_url":"ws://..."}
  - Client joins: {"type":"client", "room":"123456"}
  - Server pushes host info to client: {"type":"peer_found", "ws_url":"ws://...", "password_hash":"..."}
  - Heartbeat: {"type":"heartbeat"}
"""

import asyncio
import websockets
import json
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("relay")


class RelayServer:
    def __init__(self):
        # room_code -> { "host_ws": ws, "ws_url": str, "pwd_hash": str, "clients": [ws,...] }
        self.rooms: dict[str, dict] = {}
        self.ws_to_room: dict = {}

    async def register(self, ws, msg):
        room = msg.get("room", "").strip()
        if not room:
            await ws.send(json.dumps({"type": "error", "msg": "room required"}))
            return
        ws_url = msg.get("ws_url", "")
        pwd_hash = msg.get("password_hash", "")
        if room in self.rooms:
            await ws.send(json.dumps({"type": "error", "msg": "room already taken"}))
            return
        self.rooms[room] = {
            "host_ws": ws,
            "ws_url": ws_url,
            "pwd_hash": pwd_hash,
            "clients": []
        }
        self.ws_to_room[ws] = room
        await ws.send(json.dumps({"type": "registered", "room": room}))
        logger.info(f"Host registered room={room} ws_url={ws_url[:40]}")

    async def join(self, ws, msg):
        room = msg.get("room", "").strip()
        if not room or room not in self.rooms:
            await ws.send(json.dumps({"type": "error", "msg": "room not found"}))
            return
        entry = self.rooms[room]
        entry["clients"].append(ws)
        self.ws_to_room[ws] = room
        await ws.send(json.dumps({
            "type": "peer_found",
            "ws_url": entry["ws_url"],
            "room": room
        }))
        # Notify host that a client is coming
        try:
            await entry["host_ws"].send(json.dumps({
                "type": "client_joining",
                "room": room
            }))
        except Exception:
            pass
        logger.info(f"Client joined room={room}")

    async def unregister(self, ws):
        room = self.ws_to_room.pop(ws, None)
        if room and room in self.rooms:
            entry = self.rooms[room]
            if entry["host_ws"] is ws:
                # Host disconnected, remove room
                for c in entry["clients"]:
                    try:
                        await c.send(json.dumps({"type": "peer_disconnected"}))
                    except Exception:
                        pass
                del self.rooms[room]
                logger.info(f"Room {room} removed (host left)")
            else:
                if ws in entry["clients"]:
                    entry["clients"].remove(ws)
                logger.info(f"Client left room={room}")

    async def handler(self, ws):
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                    t = msg.get("type", "")
                    if t == "host":
                        await self.register(ws, msg)
                    elif t == "client":
                        await self.join(ws, msg)
                    elif t == "heartbeat":
                        await ws.send(json.dumps({"type": "heartbeat_ack"}))
                except json.JSONDecodeError:
                    pass
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.unregister(ws)


async def main():
    port = int(os.environ.get("PORT", 8765))
    server = RelayServer()
    async with websockets.serve(server.handler, "0.0.0.0", port):
        logger.info(f"Relay server listening on port {port}")
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
