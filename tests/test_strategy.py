"""
Tests for strategy.py: signal generation, condition logic.
"""

import os
import unittest

os.environ["LIVE_TRADING"] = "false"
os.environ["MOMENTUM_THRESHOLD"] = "0.1"
os.environ["IMBALANCE_THRESHOLD"] = "20.0"
os.environ["SPREAD_COMPRESSION_RATIO"] = "0.9"


class TestStrategy(unittest.TestCase):

    def setUp(self):
        from src.strategy import StrategyEngine
        self.engine = StrategyEngine()
        # Seed spread history to establish a rolling mean
        for _ in range(20):
            self.engine.update_spread(0.10)

    def _eval(self, momentum, imbalance, vol_spike, spread=0.08):
        return self.engine.evaluate(
            timestamp_ms=1_000_000,
            btc_momentum_5s=momentum,
            imbalance_score=imbalance,
            volume_spike=vol_spike,
            current_spread=spread,  # compressed vs mean 0.10
            yes_price=0.55,
            market_id="BTC-5M-UP",
        )

    def test_long_yes_all_conditions_met(self):
        """LONG_YES when all 4 bullish conditions are satisfied."""
        from src.strategy import SignalType
        sig = self._eval(momentum=0.5, imbalance=50.0, vol_spike=True, spread=0.08)
        self.assertEqual(sig.signal_type, SignalType.LONG_YES)
        self.assertGreater(sig.confidence, 0.0)

    def test_long_no_all_conditions_met(self):
        """LONG_NO when all 4 bearish conditions are satisfied."""
        from src.strategy import SignalType
        sig = self._eval(momentum=-0.5, imbalance=-50.0, vol_spike=True, spread=0.08)
        self.assertEqual(sig.signal_type, SignalType.LONG_NO)
        self.assertGreater(sig.confidence, 0.0)

    def test_no_trade_missing_momentum(self):
        """NO_TRADE when momentum is below threshold."""
        from src.strategy import SignalType
        sig = self._eval(momentum=0.05, imbalance=50.0, vol_spike=True, spread=0.08)
        self.assertEqual(sig.signal_type, SignalType.NO_TRADE)
        self.assertEqual(sig.confidence, 0.0)

    def test_no_trade_missing_imbalance(self):
        """NO_TRADE when imbalance is below threshold."""
        from src.strategy import SignalType
        sig = self._eval(momentum=0.5, imbalance=5.0, vol_spike=True, spread=0.08)
        self.assertEqual(sig.signal_type, SignalType.NO_TRADE)

    def test_no_trade_missing_volume_spike(self):
        """NO_TRADE when volume_spike is False."""
        from src.strategy import SignalType
        sig = self._eval(momentum=0.5, imbalance=50.0, vol_spike=False, spread=0.08)
        self.assertEqual(sig.signal_type, SignalType.NO_TRADE)

    def test_no_trade_spread_not_compressed(self):
        """NO_TRADE when spread is NOT compressed (spread > mean * 0.9)."""
        from src.strategy import SignalType
        # mean spread = 0.10, ratio = 0.9 → threshold = 0.09
        # pass spread = 0.11 (above mean, definitely not compressed)
        sig = self._eval(momentum=0.5, imbalance=50.0, vol_spike=True, spread=0.11)
        self.assertEqual(sig.signal_type, SignalType.NO_TRADE)

    def test_three_of_four_conditions_no_trade(self):
        """All 4 conditions must be true — 3/4 still produces NO_TRADE."""
        from src.strategy import SignalType
        # Missing volume spike
        sig = self._eval(momentum=0.5, imbalance=50.0, vol_spike=False, spread=0.08)
        self.assertEqual(sig.signal_type, SignalType.NO_TRADE)

        # Missing imbalance
        sig = self._eval(momentum=0.5, imbalance=10.0, vol_spike=True, spread=0.08)
        self.assertEqual(sig.signal_type, SignalType.NO_TRADE)

    def test_confidence_bounds(self):
        """Confidence must always be between 0.0 and 1.0."""
        from src.strategy import SignalType
        for momentum in [0.0, 0.05, 0.1, 0.5, 2.0]:
            for imbalance in [-100, -20, 0, 20, 100]:
                for vol in [True, False]:
                    sig = self._eval(momentum, imbalance, vol, spread=0.08)
                    self.assertGreaterEqual(sig.confidence, 0.0)
                    self.assertLessEqual(sig.confidence, 1.0)

    def test_signal_contains_all_components(self):
        """Signal dataclass contains all required fields."""
        from src.strategy import SignalType
        sig = self._eval(0.5, 50.0, True, 0.08)
        self.assertIsInstance(sig.btc_momentum_5s, float)
        self.assertIsInstance(sig.imbalance_score, float)
        self.assertIsInstance(sig.spread_compression, float)
        self.assertIsInstance(sig.volume_spike, bool)
        self.assertIsInstance(sig.momentum_condition, bool)
        self.assertIsInstance(sig.imbalance_condition, bool)
        self.assertIsInstance(sig.volume_condition, bool)
        self.assertIsInstance(sig.spread_condition, bool)

    def test_rolling_spread_mean_updates(self):
        """Rolling mean spread updates correctly."""
        from src.strategy import StrategyEngine
        eng = StrategyEngine()
        for v in [0.10, 0.20, 0.30]:
            eng.update_spread(v)
        expected_mean = (0.10 + 0.20 + 0.30) / 3
        self.assertAlmostEqual(eng.rolling_mean_spread, expected_mean, places=10)


if __name__ == "__main__":
    unittest.main()
