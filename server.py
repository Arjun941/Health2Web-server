#!/usr/bin/env python3
"""
Wear OS Sensor Stream - WebSocket Server
Single port handles both HTTP (/ping) and WebSocket connections.
"""

import asyncio
import json
import os
import datetime
import signal
from collections import defaultdict
from aiohttp import web
import aiohttp

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8765))

connected_clients = set()
packet_counts = defaultdict(int)


def log_packet(packet):
    ts = packet.get("timestamp", 0)
    device = packet.get("deviceId", "?")
    dt = datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc).strftime("%H:%M:%S")
    print(f"[{dt}] {device} — {list(packet.get('sensors', {}).keys())}", flush=True)


async def http_ping(request):
    """GET /ping — for cron-job.org"""
    return web.Response(
        text=json.dumps({"status": "ok", "clients": len(connected_clients)}),
        content_type="application/json"
    )


async def ws_handler(request):
    """WebSocket upgrade handler"""
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)

    connected_clients.add(ws)
    print(f"[+] Watch connected: {request.remote}", flush=True)

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                try:
                    packet = json.loads(msg.data)
                    device_id = packet.get("deviceId", "unknown")
                    packet_counts[device_id] += 1
                    log_packet(packet)

                    await ws.send_str(json.dumps({
                        "ack": True,
                        "server_time": int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
                    }))
                except json.JSONDecodeError:
                    pass
            elif msg.type == aiohttp.WSMsgType.ERROR:
                print(f"WS error: {ws.exception()}", flush=True)
    finally:
        connected_clients.discard(ws)
        print(f"[-] Watch disconnected: {request.remote}", flush=True)

    return ws


async def main():
    app = web.Application()
    app.router.add_get("/", http_ping)
    app.router.add_get("/ping", http_ping)
    app.router.add_get("/ws", ws_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    print(f"Server ready on port {PORT}", flush=True)
    print(f"  HTTP:      http://0.0.0.0:{PORT}/ping", flush=True)
    print(f"  WebSocket: ws://0.0.0.0:{PORT}/ws", flush=True)

    stop = asyncio.Future()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: stop.set_result(None))

    await stop
    await runner.cleanup()
    print("Stopped.")


if __name__ == "__main__":
    asyncio.run(main())