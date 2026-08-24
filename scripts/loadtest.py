"""Load test: N concurrent WS clients against the API, latency + throughput.

    python scripts/loadtest.py --clients 20 --seconds 30 --url ws://localhost:8765/ws/features
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time

import websockets


async def client(url: str, seconds: int, results: list) -> None:
    latencies = []
    count = 0
    t0 = time.time()
    async with websockets.connect(url) as ws:
        while time.time() - t0 < seconds:
            raw = await ws.recv()
            msg = json.loads(raw)
            latencies.append(time.time() - msg["ts"])
            count += 1
    results.append((count, latencies))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clients", type=int, default=20)
    ap.add_argument("--seconds", type=int, default=30)
    ap.add_argument("--url", default="ws://localhost:8765/ws/features")
    args = ap.parse_args()

    async def run():
        results: list = []
        await asyncio.gather(*(client(args.url, args.seconds, results) for _ in range(args.clients)))
        return results

    results = asyncio.run(run())
    total = sum(c for c, _ in results)
    all_lat = sorted(l for _, ls in results for l in ls)
    p50 = all_lat[len(all_lat) // 2] * 1000
    p99 = all_lat[int(len(all_lat) * 0.99)] * 1000
    print(f"clients={args.clients} duration={args.seconds}s")
    print(f"total messages fanned out : {total}  ({total/args.seconds:.0f} msg/s)")
    print(f"fan-out latency p50/p99   : {p50:.1f}ms / {p99:.1f}ms  (now - snapshot ts)")


if __name__ == "__main__":
    main()
