"""Feature computations: pure functions over rolling windows of market events.

Kept free of I/O so they are trivially unit-testable and replayable.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field

import numpy as np

__all__ = ["RollingFeatures"]


@dataclass
class RollingFeatures:
    """Streaming microstructure features over a fixed-length window.

    - microprice: size-weighted mid (bid_sz*ask + ask_sz*bid)/(bid+ask sizes)
    - imbalance:  (bid_sz - ask_sz) / (bid_sz + ask_sz)
    - trade_intensity: trades/sec over the window
    Idempotency: `update` is keyed by caller-supplied event ids; duplicate ids
    are dropped, so at-least-once delivery does not corrupt the window.
    """

    window: int = 500
    _quotes: deque = field(default_factory=deque)
    _trades: deque = field(default_factory=deque)
    _seen_quote_ids: set[str] = field(default_factory=set)
    _seen_trade_ids: set[str] = field(default_factory=set)

    def update_quote(self, event_id: str, bid_px: float, bid_sz: float,
                     ask_px: float, ask_sz: float) -> None:
        if event_id in self._seen_quote_ids:
            return
        self._seen_quote_ids.add(event_id)
        self._quotes.append((bid_px, bid_sz, ask_px, ask_sz))
        while len(self._quotes) > self.window:
            self._quotes.popleft()
        if len(self._seen_quote_ids) > self.window * 4:
            # ponytail: rebuild the dedup set when it balloons; O(n) but rare
            recent = {q for q in list(self._seen_quote_ids)[-self.window * 2:]}
            self._seen_quote_ids = recent

    def update_trade(self, event_id: str, ts: float, price: float, qty: float,
                     is_buyer_maker: bool) -> None:
        if event_id in self._seen_trade_ids:
            return
        self._seen_trade_ids.add(event_id)
        self._trades.append((ts, price, qty, is_buyer_maker))
        while len(self._trades) > self.window:
            self._trades.popleft()

    @property
    def microprice(self) -> float | None:
        if not self._quotes:
            return None
        bpx, bsz, apx, asz = self._quotes[-1]
        denom = bsz + asz
        if denom <= 0:
            return (bpx + apx) / 2
        return float((bsz * apx + asz * bpx) / denom)

    @property
    def imbalance(self) -> float | None:
        if not self._quotes:
            return None
        _, bsz, _, asz = self._quotes[-1]
        denom = bsz + asz
        return float((bsz - asz) / denom) if denom > 0 else 0.0

    @property
    def trade_intensity(self) -> float | None:
        if len(self._trades) < 2:
            return None
        t0, t1 = self._trades[0][0], self._trades[-1][0]
        span = max(t1 - t0, 1e-9)
        return float(len(self._trades) / span)

    @property
    def vwap(self) -> float | None:
        if not self._trades:
            return None
        px = np.array([t[1] for t in self._trades])
        q = np.array([t[2] for t in self._trades])
        return float((px * q).sum() / q.sum())

    def snapshot(self) -> dict:
        return {
            "microprice": self.microprice,
            "imbalance": self.imbalance,
            "trade_intensity": self.trade_intensity,
            "vwap": self.vwap,
            "n_quotes": len(self._quotes),
            "n_trades": len(self._trades),
        }
