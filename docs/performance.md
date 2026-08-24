# Performance (measured, not estimated)

Stack running natively on a 2024 Apple Silicon MacBook (8 cores), Redis 7
local, live Binance `btcusdt` combined stream. Date: 2026-08-24.

## Ingest path

| metric | value |
|---|---|
| bookTicker events | 185 msg/s sustained |
| trade events | 76 msg/s sustained |
| combined ingest | 261 msg/s into Redis Streams |
| exchange -> feature lag (ingest_ts vs now at publish) | **34.6 ms** |

Stream trimming (`maxlen`) keeps Redis memory flat over multi-hour runs
(51k quotes + 17k trades retained at snapshot time).

## Fan-out path (20 concurrent WS clients, 30 s)

| metric | value |
|---|---|
| messages fanned out | 1,039 (35 msg/s aggregate) |
| fan-out latency p50 | 2.7 ms |
| fan-out latency p99 | 7.1 ms |

Fan-out rate is bounded by the worker's 500 ms snapshot cadence
(2 snapshots/s x 20 clients = 40 msg/s ceiling), not by the server — the
per-message latency shows the delivery path itself is single-digit
milliseconds.

## Reproduce

```bash
redis-server --port 6399 --daemonize yes --save '' --appendonly no
MKTFLOW_REDIS=redis://localhost:6399/0 mktflow-ingest &
MKTFLOW_REDIS=redis://localhost:6399/0 mktflow-worker &
MKTFLOW_REDIS=redis://localhost:6399/0 PORT=8765 mktflow-api &
python scripts/loadtest.py --clients 20 --seconds 30
```

`ponytail:` numbers are from one laptop, one run; CI does not assert them.
Re-run the script on your deployment target and paste your own table.
