"""Integration: real redis-server round trip (skipped when no local server)."""

import asyncio
import json
import os

import pytest

redis = pytest.importorskip("redis.asyncio")

REDIS_URL = os.getenv("MKTFLOW_REDIS", "redis://localhost:6379/0")


async def _roundtrip() -> dict:
    r = redis.from_url(REDIS_URL, decode_responses=True)
    await r.ping()
    from mktflow.ingestor import STREAM_QUOTES
    from mktflow.worker import ensure_groups

    try:
        await r.delete(STREAM_QUOTES)
        await r.xgroup_destroy(STREAM_QUOTES, "feat-workers")
    except redis.ResponseError:
        pass  # stream/group may not exist yet -- ensure_groups creates them next

    await ensure_groups(r)
    payload = {"event_id": "q-test-1", "ingest_ts": 1.0,
               "bid_px": 99.0, "bid_sz": 2.0, "ask_px": 101.0, "ask_sz": 2.0}
    await r.xadd(STREAM_QUOTES, {"json": json.dumps(payload)}, maxlen=100)

    results = await r.xreadgroup("feat-workers", "pytest",
                                 {STREAM_QUOTES: ">"}, count=10, block=2000)
    assert results, "consumer group must receive the entry exactly once"
    _stream, entries = results[0]
    got = json.loads(entries[0][1]["json"])
    # re-read: group cursor advanced -> nothing new for this consumer
    again = await r.xreadgroup("feat-workers", "pytest", {STREAM_QUOTES: ">"},
                               count=10, block=100)
    assert not again
    await r.aclose()
    return got


@pytest.mark.skipif(
    os.getenv("SKIP_REDIS") == "1", reason="SKIP_REDIS set"
)
def test_redis_stream_roundtrip_exactly_once():
    async def run():
        try:
            return await _roundtrip()
        except (ConnectionError, OSError) as e:
            pytest.skip(f"no local redis: {e}")

    got = asyncio.run(run())
    assert got["event_id"] == "q-test-1"
    assert got["bid_px"] == 99.0
