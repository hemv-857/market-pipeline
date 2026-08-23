import pytest

from mktflow.features import RollingFeatures


def test_microprice_weighted_toward_larger_side():
    f = RollingFeatures(window=10)
    f.update_quote("q1", 99.0, 1.0, 101.0, 3.0)
    # more size on the ask -> microprice pulled below naive mid (100)
    assert f.microprice < 100.0
    f.update_quote("q2", 99.0, 3.0, 101.0, 1.0)
    assert f.microprice > 100.0


def test_imbalance_bounds_and_sign():
    f = RollingFeatures(window=10)
    f.update_quote("q1", 99.0, 7.0, 101.0, 3.0)
    assert f.imbalance == pytest.approx((7 - 3) / 10)


def test_duplicate_event_ids_are_idempotent():
    f = RollingFeatures(window=10)
    f.update_trade("t1", ts=1.0, price=100.0, qty=5.0, is_buyer_maker=False)
    f.update_trade("t1", ts=1.0, price=999.0, qty=5.0, is_buyer_maker=False)  # replay dupe
    assert f.vwap == pytest.approx(100.0)

    n_after_first = len(f._quotes) + 0
    f.update_quote("q9", 99.0, 2.0, 101.0, 2.0)
    assert len(f._quotes) == n_after_first + 1
    f.update_quote("q9", 99.0, 2.0, 101.0, 2.0)
    assert len(f._quotes) == n_after_first + 1  # no duplicate window entry


def test_intensity_over_window():
    f = RollingFeatures(window=10)
    for i in range(10):
        f.update_trade(f"t{i}", ts=float(i), price=100.0, qty=1.0, is_buyer_maker=True)
    # 10 events spanning 9 seconds -> ~1.11 trades/sec
    assert 0.8 < f.trade_intensity < 1.3


def test_snapshot_has_all_fields():
    f = RollingFeatures(window=10)
    f.update_quote("q1", 99.0, 2.0, 101.0, 2.0)
    snap = f.snapshot()
    assert set(snap) == {"microprice", "imbalance", "trade_intensity", "vwap",
                         "n_quotes", "n_trades"}


def test_window_trim_keeps_latest():
    f = RollingFeatures(window=5)
    for i in range(20):
        f.update_trade(f"t{i}", ts=float(i), price=float(i), qty=1.0, is_buyer_maker=True)
    assert f.vwap == pytest.approx(sum(range(15, 20)) / 5)
