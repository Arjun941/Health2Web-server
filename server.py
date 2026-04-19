#!/usr/bin/env python3
"""
Wear OS Sensor Stream - WebSocket Server
Includes a plain HTTP /ping endpoint for cron-job.org keepalive.
"""
 
import asyncio
import json
import os
import datetime
import signal
from collections import defaultdict
from aiohttp import web
 
import websockets
from websockets.server import WebSocketServerProtocol
 
HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8765))
 
connected_clients: set = set()
packet_counts: dict = defaultdict(int)
 
 
def format_value(v) -> str:
    if isinstance(v, dict):
        return "  ".join(f"{k}={float(val):+.3f}" for k, val in v.items())
    elif isinstance(v, (int, float)):
        return f"{float(v):.4f}"
    return str(v)
 
 
def log_packet(packet: dict):
    ts = packet.get("timestamp", 0)
    device = packet.get("deviceId", "?")
    sensors: dict = packet.get("sensors", {})
    dt = datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc).strftime("%H:%M:%S.%f")[:-3]
    print(f"[{dt}] {device}", flush=True)
    for key, val in sensors.items():
        print(f"  {key:<22}{format_value(val)}", flush=True)
 
 
async def ws_handler(websocket: WebSocketServerProtocol, path: str = "/"):
    addr = websocket.remote_address
    connected_clients.add(websocket)
    print(f"[+] Connected: {addr}", flush=True)
 
    try:
        async for message in websocket:
            try:
                packet = json.loads(message)
                device_id = packet.get("deviceId", "unknown")
                packet_counts[device_id] += 1
                log_packet(packet)
 
                await websocket.send(json.dumps({
                    "ack": True,
                    "received": packet.get("timestamp"),
                    "server_time": int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
                }))
 
            except json.JSONDecodeError as e:
                print(f"JSON error: {e}", flush=True)
 
    except websockets.ConnectionClosed as e:
        print(f"[-] Disconnected: {e.reason or 'closed'}", flush=True)
    finally:
        connected_clients.discard(websocket)
 
 
# ── Plain HTTP handler for cron-job.org ──────────────────────────────────────
 
async def http_ping(request):
    """GET /ping — returns 200 OK. Point cron-job.org here."""
    total = sum(packet_counts.values())
    return web.Response(
        text=json.dumps({
            "status": "ok",
            "clients": len(connected_clients),
            "total_packets": total
        }),
        content_type="application/json"
    )
 
 
async def http_root(request):
    return web.Response(text="Wear OS Sensor Stream — OK")
 
 
# ── Startup ───────────────────────────────────────────────────────────────────
 
async def ping_loop():
    while True:
        await asyncio.sleep(30)
        dead = set()
        for ws in connected_clients.copy():
            try:
                await ws.ping()
            except Exception:
                dead.add(ws)
        connected_clients -= dead
 
 
async def main():
    print(f"Starting on {HOST}:{PORT}", flush=True)
 
    # aiohttp app for HTTP endpoints
    app = web.Application()
    app.router.add_get("/", http_root)
    app.router.add_get("/ping", http_ping)
 
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    print(f"HTTP  listening on port {PORT} (/ping for cron-job.org)", flush=True)
 
    # WebSocket server on PORT+1
    ws_port = PORT + 1
    stop = asyncio.Future()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop.set_result(None))
 
    async with websockets.serve(
        ws_handler,
        HOST,
        ws_port,
        ping_interval=20,
        ping_timeout=60,
        max_size=1_000_000
    ):
        asyncio.create_task(ping_loop())
        print(f"WebSocket listening on port {ws_port}", flush=True)
        await stop
 
    await runner.cleanup()
    print("Stopped.")
 
 
if __name__ == "__main__":
    asyncio.run(main())