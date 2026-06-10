"""
Orchestrator for BTC and XO websocket collectors.
Manages the asyncio event bus, health checks, and collector stats.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


# ─── Event Bus ────────────────────────────────────────────────────────────────

class EventBus:
    """Simple asyncio pub/sub. One Queue per (topic, subscriber_id)."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, List[asyncio.Queue]] = {}

    def subscribe(self, topic: str, maxsize: int = 1000) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=maxsize)
        self._subscribers.setdefault(topic, []).append(q)
        return q

    def unsubscribe(self, topic: str, queue: asyncio.Queue) -> None:
        subs = self._subscribers.get(topic, [])
        if queue in subs:
            subs.remove(queue)

    async def publish(self, topic: str, payload) -> None:
        for q in self._subscribers.get(topic, []):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # Slow consumer — drop oldest
                try:
                    q.get_nowait()
                    q.put_nowait(payload)
                except Exception:
                    pass


# ─── Stats ────────────────────────────────────────────────────────────────────

@dataclass
class CollectorStats:
    btc_connected: bool = False
    xo_connected: bool = False
    last_btc_tick_ms: int = 0
    last_xo_quote_ms: int = 0
    btc_messages_per_second: float = 0.0
    xo_messages_per_second: float = 0.0
    btc_total_messages: int = 0
    xo_total_messages: int = 0
    uptime_seconds: float = 0.0
    start_time: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "btc_connected": self.btc_connected,
            "xo_connected": self.xo_connected,
            "last_btc_tick_ms": self.last_btc_tick_ms,
            "last_xo_quote_ms": self.last_xo_quote_ms,
            "btc_messages_per_second": round(self.btc_messages_per_second, 2),
            "xo_messages_per_second": round(self.xo_messages_per_second, 2),
            "btc_total_messages": self.btc_total_messages,
            "xo_total_messages": self.xo_total_messages,
            "uptime_seconds": round(time.time() - self.start_time, 1),
        }


# ─── Collector ────────────────────────────────────────────────────────────────

class Collector:
    """
    Orchestrates BTC and XO websocket collectors.
    Manages the shared event bus and tracks health metrics.
    """

    def __init__(self, db) -> None:
        self._db = db
        self.event_bus = EventBus()
        self.stats = CollectorStats()
        self._btc_collector = None
        self._xo_collector = None
        self._tasks: List[asyncio.Task] = []
        self._shutdown = False
        self._rate_window = 10  # seconds for msg/s calculation
        self._btc_msg_times: List[float] = []
        self._xo_msg_times: List[float] = []

    def _build_collectors(self) -> None:
        from src.btc_ws import BtcWebsocketCollector
        from src.xo_ws import XoWebsocketCollector
        from src.imbalance import ImbalanceEngine

        # Inject the collector's event_bus into the btc_ws EventBus-compatible interface
        self._btc_collector = BtcWebsocketCollector(
            db=self._db,
            event_bus=self.event_bus,
        )
        imbalance_engine = ImbalanceEngine()
        self._xo_collector = XoWebsocketCollector(
            db=self._db,
            event_bus=self.event_bus,
            imbalance_engine=imbalance_engine,
        )

    async def start(self) -> None:
        """Start all collectors and monitoring tasks."""
        self._build_collectors()
        self.stats.start_time = time.time()

        # Subscribe to connection events
        btc_conn_q = self.event_bus.subscribe("btc_connected")
        xo_conn_q = self.event_bus.subscribe("xo_connected")
        btc_tick_q = self.event_bus.subscribe("btc_tick")
        xo_quote_q = self.event_bus.subscribe("xo_quote")

        self._tasks = [
            asyncio.create_task(self._btc_collector.start(), name="btc_ws"),
            asyncio.create_task(self._xo_collector.start(), name="xo_ws"),
            asyncio.create_task(self._monitor_connections(btc_conn_q, xo_conn_q), name="conn_monitor"),
            asyncio.create_task(self._monitor_messages(btc_tick_q, xo_quote_q), name="msg_monitor"),
            asyncio.create_task(self._rate_calculator(), name="rate_calc"),
        ]
        logger.info("Collector started with %d tasks", len(self._tasks))

    async def stop(self) -> None:
        """Gracefully stop all collectors."""
        self._shutdown = True
        if self._btc_collector:
            await self._btc_collector.stop()
        if self._xo_collector:
            await self._xo_collector.stop()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Collector stopped")

    async def _monitor_connections(
        self, btc_q: asyncio.Queue, xo_q: asyncio.Queue
    ) -> None:
        while not self._shutdown:
            done, _ = await asyncio.wait(
                [
                    asyncio.create_task(btc_q.get()),
                    asyncio.create_task(xo_q.get()),
                ],
                return_when=asyncio.FIRST_COMPLETED,
                timeout=5.0,
            )
            for task in done:
                try:
                    result = task.result()
                    if isinstance(result, dict):
                        if "btc" in str(result):
                            self.stats.btc_connected = result.get("connected", False)
                        else:
                            self.stats.xo_connected = result.get("connected", False)
                except Exception:
                    pass

            # Sync from collectors directly
            if self._btc_collector:
                self.stats.btc_connected = self._btc_collector.connected
            if self._xo_collector:
                self.stats.xo_connected = self._xo_collector.connected

    async def _monitor_messages(
        self, btc_q: asyncio.Queue, xo_q: asyncio.Queue
    ) -> None:
        while not self._shutdown:
            try:
                # Process BTC ticks
                while not btc_q.empty():
                    await btc_q.get()
                    self.stats.last_btc_tick_ms = int(time.time() * 1000)
                    self.stats.btc_total_messages += 1
                    self._btc_msg_times.append(time.time())

                # Process XO quotes
                while not xo_q.empty():
                    await xo_q.get()
                    self.stats.last_xo_quote_ms = int(time.time() * 1000)
                    self.stats.xo_total_messages += 1
                    self._xo_msg_times.append(time.time())

                await asyncio.sleep(0.05)
            except asyncio.CancelledError:
                break

    async def _rate_calculator(self) -> None:
        """Update messages/second stats every second."""
        while not self._shutdown:
            try:
                await asyncio.sleep(1.0)
                now = time.time()
                cutoff = now - self._rate_window

                self._btc_msg_times = [t for t in self._btc_msg_times if t > cutoff]
                self._xo_msg_times = [t for t in self._xo_msg_times if t > cutoff]

                self.stats.btc_messages_per_second = len(self._btc_msg_times) / self._rate_window
                self.stats.xo_messages_per_second = len(self._xo_msg_times) / self._rate_window

                # Also sync from collector objects
                if self._btc_collector:
                    self.stats.btc_connected = self._btc_collector.connected
                if self._xo_collector:
                    self.stats.xo_connected = self._xo_collector.connected

            except asyncio.CancelledError:
                break

    def health(self) -> dict:
        """Return health check dict."""
        now_ms = int(time.time() * 1000)
        btc_age_s = (now_ms - self.stats.last_btc_tick_ms) / 1000 if self.stats.last_btc_tick_ms else -1
        xo_age_s = (now_ms - self.stats.last_xo_quote_ms) / 1000 if self.stats.last_xo_quote_ms else -1
        return {
            "status": "ok" if self.stats.btc_connected else "degraded",
            "btc_connected": self.stats.btc_connected,
            "xo_connected": self.stats.xo_connected,
            "btc_last_tick_age_s": round(btc_age_s, 1),
            "xo_last_quote_age_s": round(xo_age_s, 1),
            "btc_msg_per_s": round(self.stats.btc_messages_per_second, 2),
            "xo_msg_per_s": round(self.stats.xo_messages_per_second, 2),
            "uptime_s": round(time.time() - self.stats.start_time, 1),
        }
