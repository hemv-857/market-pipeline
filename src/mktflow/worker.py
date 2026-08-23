"""Consumer-group worker: reads streams, computes features, publishes snapshots."""

from __future__ import annotations

import asyncio
import json
import os
import time

import redis.asyncio as aioredis

from .features import RollingFeatures
from .ingestor import STREAM_QUOTES, STREAM_TRADES

GROUP = "feat-workers"
OUT_STREAM = "mkt:features"
CONSUMER = "worker-1"


async def ensure_groups(r) -> None:
    for stream in (STREAM_QUOTES, STREAM_TRADES):
        try:
            await r.xgroup_create(stream, GROUP, id="0", mkstream=True)
        except aioredis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise


async def run_worker(poll_ms: int = 100) -> None:
    r = aioredis.from_url(os.getenv("MKTFLOW_REDIS", "redis://localhost:6379/0"),
                          decode_responses=True)
    await ensure_groups(r)
    feats = RollingFeatures(window=500)
    last_publish = 0.0

    while True:
        results = await r.xreadgroup(
            GROUP, CONSUMER,
            {STREAM_QUOTES: ">", STREAM_TRADES: ">"},
            count=200, block=poll_ms,
        )
        got_any = False
        for stream, entries in results or []:
            for entry_id, fields in entries:
                msg = json.loads(fields["json"])
                got_any = True
                if stream == STREAM_QUOTES:
                    feats.update_quote(
                        msg["event_id"], msg["bid_px"], msg["bid_sz"],
                        msg["ask_px"], msg["ask_sz"],
                    )
                else:
                    feats.update_trade(
                        msg["event_id"], msg.get("exchange_ts", 0),
                        msg["price"], msg["qty"], msg["is_buyer_maker"],
                    )
                await r.xack(stream, GROUP, entry_id)
        now = time.time()
        if got_any and now - last_publish >= 0.5:
            snap = feats.snapshot()
            snap["ts"] = now
            snap["ingest_lag_sec"] = round(now - (feats._trades[-1][0] if feats._trades else now), 4)
            await r.xadd(OUT_STREAM, {"json": json.dumps(snap)}, maxlen=1000)
            last_publish = now


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
