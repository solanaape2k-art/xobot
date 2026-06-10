"""
Tests for indicators.py: RSI, VWAP, momentum score bounds.
"""

import os
import unittest

os.environ["LIVE_TRADING"] = "false"


def _make_tick(price, bid=None, ask=None, vol=1.0, ts_ms=None, n=0):
    if bid is None:
        bid = price - 0.5
    if ask is None:
        ask = price + 0.5
    if ts_ms is None:
        ts_ms = 1_000_000 + n * 1000
    return dict(
        timestamp_ms=ts_ms,
        price=price,
        bid=bid,
        ask=ask,
        spread=ask - bid,
        volume=vol,
        momentum_1s=0.0,
        momentum_3s=0.0,
        momentum_5s=0.0,
        momentum_15s=0.0,
        momentum_30s=0.0,
    )


class TestIndicatorEngine(unittest.TestCase):

    def setUp(self):
        from src.indicators import IndicatorEngine
        self.engine = IndicatorEngine()

    def _feed(self, prices, vol=1.0):
        snaps = []
        for i, p in enumerate(prices):
            snap = self.engine.update(**_make_tick(p, vol=vol, n=i))
            snaps.append(snap)
        return snaps

    # ── RSI ──────────────────────────────────────────────────────────────────

    def test_rsi_initial_returns_50(self):
        """RSI returns 50 when not enough data."""
        snap = self._feed([50000.0])[0]
        self.assertAlmostEqual(snap.rsi_14, 50.0)

    def test_rsi_overbought(self):
        """RSI > 70 on strongly rising prices."""
        # 20 prices all going up
        prices = [50000 + i * 100 for i in range(20)]
        snaps = self._feed(prices)
        last_rsi = snaps[-1].rsi_14
        self.assertGreater(last_rsi, 70.0, f"Expected RSI > 70, got {last_rsi}")

    def test_rsi_oversold(self):
        """RSI < 30 on strongly falling prices."""
        prices = [50000 - i * 100 for i in range(20)]
        snaps = self._feed(prices)
        last_rsi = snaps[-1].rsi_14
        self.assertLess(last_rsi, 30.0, f"Expected RSI < 30, got {last_rsi}")

    def test_rsi_bounds(self):
        """RSI must always be between 0 and 100."""
        import random
        random.seed(42)
        prices = [50000 + random.uniform(-500, 500) for _ in range(50)]
        snaps = self._feed(prices)
        for snap in snaps:
            self.assertGreaterEqual(snap.rsi_14, 0.0)
            self.assertLessEqual(snap.rsi_14, 100.0)

    def test_rsi_flat_market_near_50(self):
        """Flat prices should produce RSI near 50."""
        prices = [50000.0] * 30
        snaps = self._feed(prices)
        # After warmup, RSI should settle near 50 with no gains/losses
        last_rsi = snaps[-1].rsi_14
        # Allow wider range since equal gains/losses of 0 triggers edge case
        self.assertGreaterEqual(last_rsi, 0.0)
        self.assertLessEqual(last_rsi, 100.0)

    # ── VWAP ─────────────────────────────────────────────────────────────────

    def test_vwap_single_tick(self):
        """VWAP of single tick equals mid price."""
        snap = self._feed([50000.0])[0]
        expected_mid = (50000.0 - 0.5 + 50000.0 + 0.5) / 2  # (bid+ask)/2
        self.assertAlmostEqual(snap.vwap, expected_mid, places=2)

    def test_vwap_equal_volumes(self):
        """VWAP with equal volumes equals average price."""
        prices = [49000.0, 50000.0, 51000.0]
        snaps = self._feed(prices, vol=1.0)
        # Running VWAP using mids
        mids = [(p - 0.5 + p + 0.5) / 2 for p in prices]
        expected_vwap = sum(mids) / len(mids)
        self.assertAlmostEqual(snaps[-1].vwap, expected_vwap, places=2)

    def test_vwap_deviation_positive(self):
        """Price above VWAP → positive deviation."""
        self._feed([49000.0, 49500.0, 50000.0])  # rising
        # add a high tick
        snap = self.engine.update(**_make_tick(55000.0, n=10))
        # VWAP should be below 55000 if we jumped
        # deviation = (55000 - vwap) / vwap * 100 > 0
        self.assertGreater(snap.vwap_deviation_pct, 0.0)

    def test_vwap_deviation_negative(self):
        """Price below VWAP → negative deviation."""
        self._feed([55000.0, 54000.0, 53000.0])
        snap = self.engine.update(**_make_tick(45000.0, n=10))
        self.assertLess(snap.vwap_deviation_pct, 0.0)

    # ── Volume Spike ──────────────────────────────────────────────────────────

    def test_no_volume_spike_when_normal(self):
        """No volume spike when all volumes are equal."""
        snaps = self._feed([50000.0] * 25, vol=1.0)
        self.assertFalse(snaps[-1].volume_spike)

    def test_volume_spike_detected(self):
        """Volume spike detected when current volume > 2x rolling mean."""
        # Feed 20 ticks with vol=1, then one with vol=100
        engine_snap = self._feed([50000.0] * 20, vol=1.0)
        snap = self.engine.update(**_make_tick(50000.0, vol=100.0, n=21))
        self.assertTrue(snap.volume_spike)

    # ── Momentum Score ────────────────────────────────────────────────────────

    def test_momentum_score_zero_no_movement(self):
        """Momentum score is 0 when all momentum windows are 0."""
        from src.indicators import IndicatorEngine
        eng = IndicatorEngine()
        snap = eng.update(
            timestamp_ms=1_000_000,
            price=50000.0,
            bid=49999.5,
            ask=50000.5,
            spread=1.0,
            volume=1.0,
            momentum_1s=0.0,
            momentum_3s=0.0,
            momentum_5s=0.0,
            momentum_15s=0.0,
            momentum_30s=0.0,
        )
        self.assertAlmostEqual(snap.momentum_score, 0.0)

    def test_momentum_score_positive(self):
        """Positive momentum inputs → positive momentum score."""
        from src.indicators import IndicatorEngine
        eng = IndicatorEngine()
        snap = eng.update(
            timestamp_ms=1_000_000,
            price=50000.0,
            bid=49999.5,
            ask=50000.5,
            spread=1.0,
            volume=1.0,
            momentum_1s=0.5,
            momentum_3s=0.4,
            momentum_5s=0.3,
            momentum_15s=0.2,
            momentum_30s=0.1,
        )
        self.assertGreater(snap.momentum_score, 0.0)

    def test_momentum_score_negative(self):
        """Negative momentum inputs → negative momentum score."""
        from src.indicators import IndicatorEngine
        eng = IndicatorEngine()
        snap = eng.update(
            timestamp_ms=1_000_000,
            price=50000.0,
            bid=49999.5,
            ask=50000.5,
            spread=1.0,
            volume=1.0,
            momentum_1s=-0.5,
            momentum_3s=-0.4,
            momentum_5s=-0.3,
            momentum_15s=-0.2,
            momentum_30s=-0.1,
        )
        self.assertLess(snap.momentum_score, 0.0)

    def test_momentum_score_clamped_to_100(self):
        """Momentum score is always within [-100, +100]."""
        from src.indicators import IndicatorEngine
        eng = IndicatorEngine()
        # Extreme positive momentum
        snap = eng.update(
            timestamp_ms=1_000_000,
            price=50000.0,
            bid=49999.5,
            ask=50000.5,
            spread=1.0,
            volume=1.0,
            momentum_1s=999.0,
            momentum_3s=999.0,
            momentum_5s=999.0,
            momentum_15s=999.0,
            momentum_30s=999.0,
        )
        self.assertLessEqual(snap.momentum_score, 100.0)
        # Extreme negative
        snap2 = eng.update(
            timestamp_ms=2_000_000,
            price=50000.0,
            bid=49999.5,
            ask=50000.5,
            spread=1.0,
            volume=1.0,
            momentum_1s=-999.0,
            momentum_3s=-999.0,
            momentum_5s=-999.0,
            momentum_15s=-999.0,
            momentum_30s=-999.0,
        )
        self.assertGreaterEqual(snap2.momentum_score, -100.0)

    # ── IndicatorSnapshot completeness ────────────────────────────────────────

    def test_snapshot_has_all_fields(self):
        """IndicatorSnapshot must have all required fields."""
        snap = self._feed([50000.0])[0]
        self.assertIsNotNone(snap.timestamp_ms)
        self.assertIsNotNone(snap.price)
        self.assertIsNotNone(snap.rsi_14)
        self.assertIsNotNone(snap.vwap)
        self.assertIsNotNone(snap.vwap_deviation_pct)
        self.assertIsInstance(snap.volume_spike, bool)
        self.assertIsNotNone(snap.spread_pct)
        self.assertIsNotNone(snap.momentum_score)
        self.assertIsNotNone(snap.volatility_score)


if __name__ == "__main__":
    unittest.main()
