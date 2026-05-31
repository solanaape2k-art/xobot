"""
Tests for database.py: table creation, insert, deduplication.
Uses an in-memory DuckDB instance.
"""

import asyncio
import os
import tempfile
import unittest

os.environ.setdefault("LIVE_TRADING", "false")


class TestDatabase(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        import os as _os
        from src.database import Database
        self.tmp = tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False)
        self.tmp_path = self.tmp.name
        self.tmp.close()
        # Remove the empty file so DuckDB can create a fresh database
        _os.unlink(self.tmp_path)
        self.db = Database(self.tmp_path)
        self.db.initialize()
        await self.db.start()

    async def asyncTearDown(self):
        await self.db.stop()
        import os as _os
        try:
            _os.unlink(self.tmp_path)
        except Exception:
            pass

    async def test_tables_created(self):
        """All required tables must exist after initialize()."""
        tables = self.db.query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='main'"
        )
        table_names = {row[0] for row in tables}
        expected = {
            "btc_ticks", "xo_quotes", "xo_orderbooks", "xo_trades",
            "signals", "paper_trades", "latency_metrics",
        }
        for t in expected:
            self.assertIn(t, table_names, f"Table '{t}' not found")

    async def test_insert_btc_tick(self):
        """BTC tick insert round-trip."""
        await self.db.insert_btc_tick(
            timestamp_ms=1_000_000,
            price=50000.0,
            bid=49999.0,
            ask=50001.0,
            spread=2.0,
            volume=1.5,
            trade_side="buy",
            momentum_1s=0.01,
            momentum_3s=0.02,
            momentum_5s=0.03,
            momentum_15s=0.01,
            momentum_30s=0.005,
        )
        # Drain queue
        await self.db._write_queue.join()
        rows = self.db.query("SELECT price, trade_side FROM btc_ticks WHERE timestamp_ms=1000000")
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0][0], 50000.0)
        self.assertEqual(rows[0][1], "buy")

    async def test_insert_xo_quote(self):
        """XO quote insert."""
        await self.db.insert_xo_quote(
            timestamp_ms=2_000_000,
            market_id="BTC-5M-UP",
            yes_price=0.55,
            no_price=0.45,
            spread=0.10,
            volume=1000.0,
            status="active",
        )
        await self.db._write_queue.join()
        rows = self.db.query("SELECT yes_price, market_id FROM xo_quotes WHERE timestamp_ms=2000000")
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0][0], 0.55)
        self.assertEqual(rows[0][1], "BTC-5M-UP")

    async def test_insert_paper_trade(self):
        """Paper trade insert and retrieval."""
        await self.db.insert_paper_trade(
            timestamp_ms=3_000_000,
            market_id="BTC-5M-UP",
            side="YES",
            entry_price=0.52,
            exit_price=0.60,
            size=100.0,
            pnl=15.38,
            hold_time_ms=300_000,
            status="closed",
            signal_id=1,
        )
        await self.db._write_queue.join()
        trades = self.db.get_recent_paper_trades(limit=10)
        self.assertEqual(len(trades), 1)
        self.assertAlmostEqual(trades[0]["pnl"], 15.38)
        self.assertEqual(trades[0]["side"], "YES")

    async def test_insert_latency(self):
        """Latency metric insert."""
        await self.db.insert_latency(
            timestamp_ms=4_000_000,
            source="binance_aggTrade",
            latency_ms=12.5,
        )
        await self.db._write_queue.join()
        rows = self.db.query("SELECT latency_ms, source FROM latency_metrics WHERE timestamp_ms=4000000")
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(rows[0][0], 12.5)

    async def test_paper_trade_stats_empty(self):
        """Stats on empty DB return zeros."""
        stats = self.db.get_paper_trade_stats()
        self.assertEqual(stats["total_pnl"], 0.0)
        self.assertEqual(stats["trade_count"], 0)

    async def test_paper_trade_stats_populated(self):
        """Stats computed correctly from closed trades."""
        # Insert 3 trades: 2 wins, 1 loss
        for pnl, status in [(10.0, "closed"), (5.0, "closed"), (-3.0, "closed")]:
            await self.db.insert_paper_trade(
                timestamp_ms=int(pnl * 1_000_000),
                market_id="BTC-5M-UP",
                side="YES",
                entry_price=0.50,
                exit_price=0.50 + pnl / 1000,
                size=100.0,
                pnl=pnl,
                hold_time_ms=60_000,
                status=status,
                signal_id=0,
            )
        await self.db._write_queue.join()
        stats = self.db.get_paper_trade_stats()
        self.assertEqual(stats["trade_count"], 3)
        self.assertAlmostEqual(stats["total_pnl"], 12.0)
        self.assertAlmostEqual(stats["win_rate"], 200.0 / 3, places=1)

    async def test_get_btc_ticks_range(self):
        """Range query returns correct rows."""
        for ts in [100, 200, 300, 400, 500]:
            await self.db.insert_btc_tick(
                timestamp_ms=ts,
                price=float(ts),
                bid=float(ts) - 1,
                ask=float(ts) + 1,
                spread=2.0,
                volume=1.0,
                trade_side="buy",
                momentum_1s=0.0,
                momentum_3s=0.0,
                momentum_5s=0.0,
                momentum_15s=0.0,
                momentum_30s=0.0,
            )
        await self.db._write_queue.join()
        rows = self.db.get_btc_ticks_range(200, 400)
        timestamps = [r[1] for r in rows]
        self.assertIn(200, timestamps)
        self.assertIn(300, timestamps)
        self.assertIn(400, timestamps)
        self.assertNotIn(100, timestamps)
        self.assertNotIn(500, timestamps)


if __name__ == "__main__":
    unittest.main()
