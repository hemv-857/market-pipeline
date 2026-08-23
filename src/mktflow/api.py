"""FastAPI service: REST snapshot + live WS fan-out of feature updates."""

from __future__ import annotations

import json
import os

import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, WebSocket
from fastapi.responses import PlainTextResponse
from prometheus_client import Counter, Gauge, generate_latest

app = FastAPI(title="mktflow")
r: aioredis.Redis | None = None

MSG_OUT = Counter("mktflow_ws_messages_total", "WS messages fanned out")
SNAP_TS = Gauge("mktflow_snapshot_ts", "Timestamp of latest feature snapshot")


@app.on_event("startup")
async def startup() -> None:
    global r
    r = aioredis.from_url(os.getenv("MKTFLOW_REDIS", "redis://localhost:6379/0"),
                          decode_responses=True)


@app.get("/healthz")
async def healthz() -> dict:
    try:
        return {"ok": bool(await r.ping())}
    except Exception:
        return {"ok": False}


@app.get("/metrics")
async def metrics() -> PlainTextResponse:
    return PlainTextResponse(generate_latest().decode())


@app.get("/features/latest")
async def latest_features() -> dict:
    rows = await r.xrevrange("mkt:features", count=1)
    if not rows:
        return {"error": "no features yet"}
    _, fields = rows[0]
    snap = json.loads(fields["json"])
    SNAP_TS.set(snap.get("ts", 0))
    return snap


@app.websocket("/ws/features")
async def ws_features(ws: WebSocket) -> None:
    await ws.accept()
    last_id = "$"
    try:
        while True:
            rows = await r.xread({"mkt:features": last_id}, block=1000, count=10)
            for _, entries in rows or []:
                for entry_id, fields in entries:
                    last_id = entry_id
                    MSG_OUT.inc()
                    await ws.send_text(fields["json"])
    except Exception:
        await ws.close()


def main() -> None:
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))


if __name__ == "__main__":
    main()
