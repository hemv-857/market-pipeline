# mktflow

Real-time crypto market data pipeline, deployed as containers:

```
Binance WS ──> ingestor ──> Redis Streams ──> worker ──> mkt:features ──> FastAPI ──> clients
                  │            (XADD, maxlen)   (consumer      (snapshots)       /ws /REST
                  └─> data/tape.jsonl           group, idempotent)
```

- **Ingestor**: combined `@trade` + `@bookTicker` streams; exponential-backoff
  reconnect with jitter; trade-id gap detection; every message tagged with
  `ingest_ts` so end-to-end latency is measurable downstream.
- **Worker**: consumer group (`feat-workers`) — at-least-once delivery made
  safe by event-id dedup inside the feature window. Publishes microprice,
  imbalance, trade intensity and rolling VWAP every 500ms.
- **API**: `/features/latest` (REST), `/ws/features` (live fan-out),
  `/healthz`, Prometheus `/metrics`.

## Run locally

```bash
pip install -e ".[dev]"
make test                       # feature unit tests + live-redis roundtrip if present
docker compose up --build       # full stack on :8000
curl localhost:8000/features/latest
```

## Correctness gates (enforced in tests)

- Microprice weights toward the thicker side of the book
- Duplicate event ids never double-count (replay-safe)
- Rolling VWAP/intensity match hand-computed fixture values
- Redis consumer-group roundtrip delivers exactly once per consumer
  (integration test against a real `redis-server`; skips cleanly without one)

## Honest scope

- `ponytail:` JSONL sink instead of Parquet; single worker consumer; no auth on
  the API. All three are one-session upgrades when needed.
- Latency numbers to quote: run `docker compose up` on a VPS, subscribe from a
  second host, report p50/p99 of `now - ingest_ts` over ≥1h.
