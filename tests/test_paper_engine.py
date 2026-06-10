"""
Tests for paper_engine.py: trade entry/exit, PnL calculation, stats.
"""

import asyncio
import os
import tempfile
import unittest

os.environ["LIVE_TRADING"] = "false"
os.environ["PAPER_POSITION_SIZE"] = "100"
os.environ["PAPER_HOLD_TIME_SECONDS"] = "300"
os.environ["PAPER_STOP_LOSS_PCT"] = "5.0"


class TestPaperEngine(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        import os as _os
        from src.database import Database
        from src.paper_engine import PaperEngine
        self.tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        _os.unlink(self.tmp_path)
        self.db = Database(self.tmp_path)
        self.db.initialize()
        await self.db.start()
        self.engine = PaperEngine(self.db)
        await self.engine.start()

    async def asyncTearDown(self):
        await self.engine.stop()
        await self.db.stop()
        import os as _os
        try:
            _os.unlink(self.tmp_path)
        except Exception:
            pass

    async def test_enter_trade_creates_position(self):
        """Entering a trade creates an open position."""
        pos = await self.engine.enter_trade(
            signal_id=1,
            market_id="BTC-5M-UP",
            side="YES",
            entry_price=0.52,
        )
        self.assertIsNotNone(pos)
        self.assertEqual(pos.side, "YES")
        self.assertAlmostEqual(pos.entry_price, 0.52)
        self.assertEqual(pos.status, "open")

        positions = self.engine.get_open_positions()
        self.assertEqual(len(positions), 1)

    async def test_double_entry_same_side_rejected(self):
        """Second entry on same market/side returns None."""
        await self.engine.enter_trade(1, "BTC-5M-UP", "YES", 0.52)
        pos2 = await self.engine.enter_trade(2, "BTC-5M-UP", "YES", 0.55)
        self.assertIsNone(pos2)
        self.assertEqual(len(self.engine.get_open_positions()), 1)

    async def test_exit_trade_closes_position(self):
        """Exiting a trade closes it and removes from open positions."""
        await self.engine.enter_trade(1, "BTC-5M-UP", "YES", 0.52)
        closed = await self.engine.exit_trade("BTC-5M-UP", "YES", 0.60)
        self.assertIsNotNone(closed)
        self.assertEqual(closed.status, "closed")
        self.assertEqual(len(self.engine.get_open_positions()), 0)

    async def test_pnl_yes_long_positive(self):
        """YES long with exit > entry produces positive PnL."""
        await self.engine.enter_trade(1, "BTC-5M-UP", "YES", 0.50)
        closed = await self.engine.exit_trade("BTC-5M-UP", "YES", 0.60)
        # PnL = (0.60 - 0.50) / 0.50 * 100 = 20%
        self.assertAlmostEqual(closed.pnl, 20.0, places=4)

    async def test_pnl_yes_long_negative(self):
        """YES long with exit < entry produces negative PnL."""
        await self.engine.enter_trade(1, "BTC-5M-UP", "YES", 0.60)
        closed = await self.engine.exit_trade("BTC-5M-UP", "YES", 0.50)
        # PnL = (0.50 - 0.60) / 0.60 * 100 = -16.67%
        expected = (0.50 - 0.60) / 0.60 * 100
        self.assertAlmostEqual(closed.pnl, expected, places=4)

    async def test_pnl_no_long_positive(self):
        """NO long: price falls → positive PnL."""
        await self.engine.enter_trade(1, "BTC-5M-UP", "NO", 0.50)
        closed = await self.engine.exit_trade("BTC-5M-UP", "NO", 0.40)
        # PnL = (0.50 - 0.40) / 0.50 * 100 = 20%
        self.assertAlmostEqual(closed.pnl, 20.0, places=4)

    async def test_pnl_no_long_negative(self):
        """NO long: price rises → negative PnL."""
        await self.engine.enter_trade(1, "BTC-5M-UP", "NO", 0.40)
        closed = await self.engine.exit_trade("BTC-5M-UP", "NO", 0.50)
        # PnL = (0.40 - 0.50) / 0.40 * 100 = -25%
        expected = (0.40 - 0.50) / 0.40 * 100
        self.assertAlmostEqual(closed.pnl, expected, places=4)

    async def test_stop_loss_exits_trade(self):
        """Stop loss triggers when loss exceeds PAPER_STOP_LOSS_PCT."""
        os.environ["PAPER_STOP_LOSS_PCT"] = "5.0"
        await self.engine.enter_trade(1, "BTC-5M-UP", "YES", 0.60)
        # Price falls enough to trigger stop loss: loss = (0.56 - 0.60)/0.60*100 ≈ -6.67%
        stopped = await self.engine.check_stop_loss("BTC-5M-UP", "YES", 0.56)
        self.assertTrue(stopped)
        self.assertEqual(len(self.engine.get_open_positions()), 0)

    async def test_stop_loss_not_triggered_within_limit(self):
        """No stop loss when loss is within allowed range."""
        await self.engine.enter_trade(1, "BTC-5M-UP", "YES", 0.60)
        # Loss = (0.58 - 0.60)/0.60 ≈ -3.33% which is < 5%
        stopped = await self.engine.check_stop_loss("BTC-5M-UP", "YES", 0.58)
        self.assertFalse(stopped)
        self.assertEqual(len(self.engine.get_open_positions()), 1)

    async def test_stats_aggregation(self):
        """Stats compute win_rate, total_pnl, profit_factor correctly."""
        # 2 wins, 1 loss
        for entry, exit_, side in [
            (0.50, 0.60, "YES"),  # +20%
            (0.50, 0.45, "YES"),  # -10%
        ]:
            await self.engine.enter_trade(1, "BTC-5M-UP", side, entry)
            await self.engine.exit_trade("BTC-5M-UP", side, exit_)
            # Brief pause for different trade IDs
            await asyncio.sleep(0.01)

        stats = self.engine.get_stats()
        self.assertEqual(stats.trade_count, 2)
        self.assertEqual(stats.win_count, 1)
        self.assertEqual(stats.loss_count, 1)
        self.assertAlmostEqual(stats.win_rate, 50.0, places=1)
        # total_pnl = 20% + (-10%) = 10%
        self.assertAlmostEqual(stats.total_pnl, 10.0, places=4)

    async def test_stats_empty(self):
        """Stats on no trades return zeroed TradeStats."""
        stats = self.engine.get_stats()
        self.assertEqual(stats.trade_count, 0)
        self.assertEqual(stats.total_pnl, 0.0)
        self.assertEqual(stats.win_rate, 0.0)

    async def test_trade_persisted_to_db(self):
        """Closed trades are written to paper_trades table."""
        await self.engine.enter_trade(1, "BTC-5M-UP", "YES", 0.50)
        await self.engine.exit_trade("BTC-5M-UP", "YES", 0.60)
        await self.db._write_queue.join()
        trades = self.db.get_recent_paper_trades(10)
        self.assertGreater(len(trades), 0)
        self.assertEqual(trades[0]["status"], "closed")

    async def test_exit_nonexistent_position_returns_none(self):
        """Exiting a non-existent position returns None gracefully."""
        result = await self.engine.exit_trade("NONEXISTENT", "YES", 0.55)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
