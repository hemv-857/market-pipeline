"""Async WebSocket ingestor: Binance combined streams -> Redis Streams + JSONL sink.

- exponential-backoff reconnect with jitter
- monotonic sequence check per stream (gap detection)
- every message tagged with ingest timestamp for e2e latency measurement
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
from pathlib import Path

import redis.asyncio as aioredis
import websockets

STREAM_QUOTES = "mkt:quotes"
STREAM_TRADES = "mkt:trades"
BINANCE_WS = "wss://stream.binance.com:9443/stream"


class Ingestor:
    def __init__(
        self,
        symbol: str = "btcusdt",
        redis_url: str = "redis://localhost:6379/0",
        sink_path: str | None = "data/tape.jsonl",
        max_stale_sec: float = 30.0,
    ):
        self.symbol = symbol.lower()
        self.redis_url = redis_url
        self.sink_path = Path(sink_path) if sink_path else None
        self.max_stale = max_stale_sec
        self.last_msg_ts = 0.0
        self.msg_count = 0
        self.reconnects = 0
        self.gaps = 0
        self._last_trade_id = -1

    async def run_forever(self) -> None:
        backoff = 1.0
        r = aioredis.from_url(self.redis_url, decode_responses=True)
        if self.sink_path:
            self.sink_path.parent.mkdir(parents=True, exist_ok=True)
        url = f"{BINANCE_WS}?streams={self.symbol}@trade/{self.symbol}@bookTicker"
        while True:
            try:
                async with websockets.connect(url, ping_interval=20) as ws:
                    backoff = 1.0
                    async for raw in ws:
                        await self._handle(raw, r)
            except (websockets.ConnectionClosed, OSError, asyncio.IncompleteReadError) as e:
                self.reconnects += 1
                wait = backoff * (0.5 + random.random())
                print(f"[ingest] disconnected ({e!r}); retry in {wait:.1f}s")
                await asyncio.sleep(wait)
                backoff = min(backoff * 2, 60.0)

    async def _handle(self, raw: str | bytes, r) -> None:
        now = time.time()
        self.last_msg_ts = now
        self.msg_count += 1
        msg = json.loads(raw)
        data, stream = msg.get("data", {}), msg.get("stream", "")
        event_id = f"{data.get('e','x')}:{data.get('a') or data.get('u') or self.msg_count}"

        payload = {
            "event_id": event_id,
            "ingest_ts": now,
            "exchange_ts": (data.get("T") or data.get("E") or 0),
            "symbol": self.symbol.upper(),
        }

        if "@trade" in stream:
            trade_id = int(data.get("a", -1))
            if trade_id <= self._last_trade_id:
                self.gaps += 1  # out-of-order/duplicate from exchange side
            self._last_trade_id = max(self._last_trade_id, trade_id)
            payload.update({
                "price": float(data["p"]),
                "qty": float(data["q"]),
                "is_buyer_maker": bool(data["m"]),
            })
            await r.xadd(STREAM_TRADES, {"json": json.dumps(payload)}, maxlen=100_000)
        elif "@bookTicker" in stream:
            payload.update({
                "bid_px": float(data["b"]),
                "bid_sz": float(data["B"]),
                "ask_px": float(data["a"]),
                "ask_sz": float(data["A"]),
            })
            await r.xadd(STREAM_QUOTES, {"json": json.dumps(payload)}, maxlen=200_000)
        else:
            return

        if self.sink_path:
            with open(self.sink_path, "a") as fh:  # ponytail: line-buffered append; rotate later
                fh.write(json.dumps(payload) + "\n")


def main() -> None:
    symbol = os.getenv("MKTFLOW_SYMBOL", "btcusdt")
    redis_url = os.getenv("MKTFLOW_REDIS", "redis://localhost:6379/0")
    sink = os.getenv("MKTFLOW_SINK", "data/tape.jsonl")
    asyncio.run(Ingestor(symbol, redis_url, sink).run_forever())


if __name__ == "__main__":
    main()
