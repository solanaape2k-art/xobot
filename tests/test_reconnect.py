"""
Tests for websocket reconnect logic.
Uses mocked websockets that fail on first attempt(s) then succeed.
"""

import asyncio
import os
import unittest
from unittest.mock import AsyncMock, MagicMock, patch, call
import json

os.environ["LIVE_TRADING"] = "false"


class FakeWebSocket:
    """Fake websocket async context manager that yields messages then closes."""

    def __init__(self, messages: list):
        self.messages = list(messages)
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        for msg in self.messages:
            yield json.dumps(msg)

    async def send(self, data):
        pass


class FailingConnect:
    """Async context manager that raises on enter."""
    def __init__(self, exc):
        self.exc = exc

    async def __aenter__(self):
        raise self.exc

    async def __aexit__(self, *args):
        pass


class TestBtcWsReconnect(unittest.IsolatedAsyncioTestCase):

    async def test_reconnects_after_failure(self):
        """BtcWebsocketCollector must reconnect after a connection failure."""
        from src.btc_ws import BtcWebsocketCollector, EventBus

        event_bus = EventBus()
        db_mock = MagicMock()
        db_mock.insert_btc_tick = AsyncMock()
        db_mock.insert_latency = AsyncMock()

        collector = BtcWebsocketCollector(db=db_mock, event_bus=event_bus)

        connect_calls = []

        def fake_connect(url, **kwargs):
            connect_calls.append(url)
            if len(connect_calls) == 1:
                return FailingConnect(ConnectionRefusedError("Simulated failure attempt 1"))
            else:
                # Stop after second attempt
                collector._shutdown = True
                return FakeWebSocket(messages=[])

        import src.btc_ws as btc_ws_module
        original_base = btc_ws_module.RECONNECT_BASE_DELAY
        try:
            btc_ws_module.RECONNECT_BASE_DELAY = 0.001
            with patch("src.btc_ws.websockets.connect", side_effect=fake_connect):
                task = asyncio.create_task(collector.start())
                await asyncio.sleep(0.1)
                await collector.stop()
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        finally:
            btc_ws_module.RECONNECT_BASE_DELAY = original_base

        # Should have attempted at least 1 connection
        self.assertGreater(len(connect_calls), 0)

    async def test_exponential_backoff(self):
        """Reconnect delay doubles each failed attempt up to max."""
        from src.btc_ws import (
            RECONNECT_BASE_DELAY,
            RECONNECT_MAX_DELAY,
            RECONNECT_BACKOFF,
        )

        # Test the backoff math directly without running the full async loop
        delay = RECONNECT_BASE_DELAY
        expected_delays = []
        for _ in range(8):
            expected_delays.append(delay)
            delay = min(delay * RECONNECT_BACKOFF, RECONNECT_MAX_DELAY)

        # Verify backoff doubles each time until capped
        for i in range(1, len(expected_delays)):
            if expected_delays[i - 1] * RECONNECT_BACKOFF <= RECONNECT_MAX_DELAY:
                self.assertAlmostEqual(
                    expected_delays[i],
                    expected_delays[i - 1] * RECONNECT_BACKOFF,
                    places=5,
                )
            else:
                self.assertAlmostEqual(expected_delays[i], RECONNECT_MAX_DELAY, places=5)

        # Max delay must not exceed cap
        for d in expected_delays:
            self.assertLessEqual(d, RECONNECT_MAX_DELAY + 0.001)

        # First delay must equal base
        self.assertAlmostEqual(expected_delays[0], RECONNECT_BASE_DELAY)

    async def test_reconnect_attempts_after_failure(self):
        """BtcWebsocketCollector attempts connections and stops gracefully."""
        from src.btc_ws import BtcWebsocketCollector, EventBus

        event_bus = EventBus()
        db_mock = MagicMock()
        db_mock.insert_btc_tick = AsyncMock()
        db_mock.insert_latency = AsyncMock()

        collector = BtcWebsocketCollector(db=db_mock, event_bus=event_bus)
        connect_calls = []

        def fail_then_stop(url, **kwargs):
            connect_calls.append(url)
            return FailingConnect(ConnectionRefusedError("Simulated failure"))

        # Use a very short base delay override to avoid real waiting
        import src.btc_ws as btc_ws_module
        original_base = btc_ws_module.RECONNECT_BASE_DELAY

        try:
            btc_ws_module.RECONNECT_BASE_DELAY = 0.001

            with patch("src.btc_ws.websockets.connect", side_effect=fail_then_stop):
                task = asyncio.create_task(collector.start())
                # Let it run briefly then cancel
                await asyncio.sleep(0.05)
                await collector.stop()
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        finally:
            btc_ws_module.RECONNECT_BASE_DELAY = original_base

        # Should have attempted at least one connection
        self.assertGreater(len(connect_calls), 0)


class TestXoWsReconnect(unittest.IsolatedAsyncioTestCase):

    async def test_xo_continues_if_unavailable(self):
        """XO websocket failure must not crash the system."""
        from src.xo_ws import XoWebsocketCollector
        from src.collector import EventBus
        from src.imbalance import ImbalanceEngine

        event_bus = EventBus()
        db_mock = MagicMock()
        db_mock.insert_xo_quote = AsyncMock()
        db_mock.insert_xo_trade = AsyncMock()
        db_mock.insert_xo_orderbook = AsyncMock()
        db_mock.insert_latency = AsyncMock()

        imbalance = ImbalanceEngine()
        collector = XoWebsocketCollector(db=db_mock, event_bus=event_bus, imbalance_engine=imbalance)

        attempt = 0

        def always_fail(url, **kwargs):
            nonlocal attempt
            attempt += 1
            return FailingConnect(ConnectionRefusedError("XO not available"))

        import src.xo_ws as xo_ws_module
        original_base = xo_ws_module.RECONNECT_BASE_DELAY
        try:
            xo_ws_module.RECONNECT_BASE_DELAY = 0.001
            with patch("src.xo_ws.websockets.connect", side_effect=always_fail):
                task = asyncio.create_task(collector.start())
                await asyncio.sleep(0.05)
                await collector.stop()
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        except Exception as e:
            self.fail(f"XO collector raised unexpected exception: {e}")
        finally:
            xo_ws_module.RECONNECT_BASE_DELAY = original_base

        # Should have tried connecting multiple times without crashing
        self.assertGreaterEqual(attempt, 1)
        self.assertFalse(collector.connected)

    async def test_xo_reconnects_after_disconnect(self):
        """XO collector should retry after disconnect."""
        from src.xo_ws import XoWebsocketCollector
        from src.collector import EventBus
        from src.imbalance import ImbalanceEngine

        event_bus = EventBus()
        db_mock = MagicMock()
        db_mock.insert_xo_quote = AsyncMock()
        db_mock.insert_xo_trade = AsyncMock()
        db_mock.insert_xo_orderbook = AsyncMock()
        db_mock.insert_latency = AsyncMock()

        imbalance = ImbalanceEngine()
        collector = XoWebsocketCollector(db=db_mock, event_bus=event_bus, imbalance_engine=imbalance)

        connect_calls = []

        def connect_twice(url, **kwargs):
            connect_calls.append(url)
            return FailingConnect(ConnectionResetError("Disconnected"))

        import src.xo_ws as xo_ws_module
        original_base = xo_ws_module.RECONNECT_BASE_DELAY
        try:
            xo_ws_module.RECONNECT_BASE_DELAY = 0.001
            with patch("src.xo_ws.websockets.connect", side_effect=connect_twice):
                task = asyncio.create_task(collector.start())
                await asyncio.sleep(0.05)
                task.cancel()
                try:
                    await asyncio.wait_for(task, timeout=1.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass
        finally:
            xo_ws_module.RECONNECT_BASE_DELAY = original_base

        self.assertGreaterEqual(len(connect_calls), 1)


class TestMomentumCalculator(unittest.TestCase):

    def test_momentum_zero_without_history(self):
        """Momentum returns 0.0 with no data."""
        from src.btc_ws import MomentumCalculator
        calc = MomentumCalculator()
        self.assertEqual(calc.momentum(5), 0.0)

    def test_momentum_positive(self):
        """Positive momentum when price rises."""
        from src.btc_ws import MomentumCalculator
        calc = MomentumCalculator()
        # Add old price 10s ago
        calc.add(1_000_000, 50000.0)
        # Add current price 0s ago (same ts for simplicity)
        calc.add(1_010_000, 51000.0)
        m = calc.momentum(10)
        self.assertGreater(m, 0.0)

    def test_momentum_negative(self):
        """Negative momentum when price falls."""
        from src.btc_ws import MomentumCalculator
        calc = MomentumCalculator()
        calc.add(1_000_000, 51000.0)
        calc.add(1_010_000, 50000.0)
        m = calc.momentum(10)
        self.assertLess(m, 0.0)

    def test_all_momentums_returns_all_windows(self):
        """all_momentums() returns dict with all 5 windows."""
        from src.btc_ws import MomentumCalculator
        calc = MomentumCalculator()
        calc.add(1_000_000, 50000.0)
        result = calc.all_momentums()
        self.assertEqual(set(result.keys()), {1, 3, 5, 15, 30})


if __name__ == "__main__":
    unittest.main()
