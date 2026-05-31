"""
Binance BTCUSDT perpetual futures websocket collector.
Connects to wss://fstream.binance.com/stream, subscribes to aggTrade + bookTicker.
Calculates rolling momentum windows, stores to DB, emits events to event bus.
Auto-reconnects with exponential backoff.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Deque, Dict, Optional

import websockets
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BINANCE_WS_URL = os.getenv("BINANCE_WS_URL", "wss://fstream.binance.com/stream")
BINANCE_SYMBOL = os.getenv("BINANCE_SYMBOL", "btcusdt").lower()

RECONNECT_BASE_DELAY = 1.0
RECONNECT_MAX_DELAY = 60.0
RECONNECT_BACKOFF = 2.0


@dataclass
class BtcTick:
    timestamp_ms: int
    price: float
    bid: float
    ask: float
    spread: float
    volume: float
    trade_side: str  # "buy" | "sell" | "quote"
    momentum_1s: float
    momentum_3s: float
    momentum_5s: float
    momentum_15s: float
    momentum_30s: float
    latency_ms: float


@dataclass
class _PricePoint:
    ts_ms: int
    price: float


class MomentumCalculator:
    """Maintains rolling deques to calculate price momentum over multiple windows."""

    WINDOWS_S = [1, 3, 5, 15, 30]

    def __init__(self) -> None:
        # Store (timestamp_ms, price) pairs; maxlen chosen for 30s at ~10 ticks/s
        self._history: Deque[_PricePoint] = deque(maxlen=3000)

    def add(self, ts_ms: int, price: float) -> None:
        self._history.append(_PricePoint(ts_ms=ts_ms, price=price))

    def momentum(self, window_seconds: int) -> float:
        """
        Momentum = (current_price - price_N_seconds_ago) / price_N_seconds_ago * 100 (%)
        Returns 0.0 if insufficient history.
        """
        if not self._history:
            return 0.0
        now_ts = self._history[-1].ts_ms
        cutoff = now_ts - window_seconds * 1000
        old = None
        for pt in self._history:
            if pt.ts_ms >= cutoff:
                old = pt
                break
        if old is None:
            return 0.0
        current = self._history[-1].price
        if old.price == 0:
            return 0.0
        return (current - old.price) / old.price * 100

    def all_momentums(self) -> Dict[int, float]:
        return {w: self.momentum(w) for w in self.WINDOWS_S}


class BtcWebsocketCollector:
    """
    Connects to Binance futures websocket.
    Emits BtcTick objects via event bus and stores to DB.
    """

    def __init__(
        self,
        db,  # Database instance
        event_bus: "EventBus",
    ) -> None:
        self._db = db
        self._event_bus = event_bus
        self._momentum = MomentumCalculator()
        self._connected = False
        self._shutdown = False
        self._last_tick: Optional[BtcTick] = None
        self._messages_recv = 0
        self._last_price: float = 0.0
        self._last_bid: float = 0.0
        self._last_ask: float = 0.0

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def last_tick(self) -> Optional[BtcTick]:
        return self._last_tick

    @property
    def messages_received(self) -> int:
        return self._messages_recv

    async def start(self) -> None:
        """Start the websocket collector with reconnect loop."""
        delay = RECONNECT_BASE_DELAY
        while not self._shutdown:
            try:
                await self._connect()
                delay = RECONNECT_BASE_DELAY  # reset on success
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._connected = False
                logger.warning(
                    "BTC WS disconnected: %s — reconnecting in %.1fs", exc, delay
                )
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                delay = min(delay * RECONNECT_BACKOFF, RECONNECT_MAX_DELAY)

    async def stop(self) -> None:
        self._shutdown = True
        self._connected = False

    async def _connect(self) -> None:
        streams = f"{BINANCE_SYMBOL}@aggTrade/{BINANCE_SYMBOL}@bookTicker"
        url = f"{BINANCE_WS_URL}?streams={streams}"
        logger.info("Connecting to Binance WS: %s", url)

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._connected = True
            logger.info("BTC WS connected")
            await self._event_bus.publish("btc_connected", {"connected": True})

            async for raw in ws:
                if self._shutdown:
                    break
                recv_ts = int(time.time() * 1000)
                try:
                    msg = json.loads(raw)
                    await self._handle_message(msg, recv_ts)
                except Exception as exc:
                    logger.debug("BTC WS parse error: %s", exc)

        self._connected = False

    async def _handle_message(self, msg: dict, recv_ts: int) -> None:
        data = msg.get("data", msg)
        stream = msg.get("stream", "")

        if "aggTrade" in stream or data.get("e") == "aggTrade":
            await self._handle_agg_trade(data, recv_ts)
        elif "bookTicker" in stream or data.get("e") == "bookTicker" or "b" in data:
            await self._handle_book_ticker(data, recv_ts)

    async def _handle_agg_trade(self, data: dict, recv_ts: int) -> None:
        try:
            event_ts = int(data.get("T", recv_ts))
            price = float(data.get("p", 0))
            qty = float(data.get("q", 0))
            is_buyer_maker = data.get("m", False)
            trade_side = "sell" if is_buyer_maker else "buy"
            latency_ms = recv_ts - event_ts

            self._last_price = price
            self._momentum.add(event_ts, price)
            momentums = self._momentum.all_momentums()

            tick = BtcTick(
                timestamp_ms=event_ts,
                price=price,
                bid=self._last_bid,
                ask=self._last_ask,
                spread=max(0.0, self._last_ask - self._last_bid),
                volume=qty,
                trade_side=trade_side,
                momentum_1s=momentums[1],
                momentum_3s=momentums[3],
                momentum_5s=momentums[5],
                momentum_15s=momentums[15],
                momentum_30s=momentums[30],
                latency_ms=latency_ms,
            )
            self._last_tick = tick
            self._messages_recv += 1

            await self._db.insert_btc_tick(
                timestamp_ms=tick.timestamp_ms,
                price=tick.price,
                bid=tick.bid,
                ask=tick.ask,
                spread=tick.spread,
                volume=tick.volume,
                trade_side=tick.trade_side,
                momentum_1s=tick.momentum_1s,
                momentum_3s=tick.momentum_3s,
                momentum_5s=tick.momentum_5s,
                momentum_15s=tick.momentum_15s,
                momentum_30s=tick.momentum_30s,
            )

            await self._db.insert_latency(
                timestamp_ms=recv_ts,
                source="binance_aggTrade",
                latency_ms=latency_ms,
            )

            await self._event_bus.publish("btc_tick", tick)

        except Exception as exc:
            logger.debug("Error processing aggTrade: %s", exc)

    async def _handle_book_ticker(self, data: dict, recv_ts: int) -> None:
        try:
            bid = float(data.get("b", 0))
            ask = float(data.get("a", 0))
            if bid <= 0 or ask <= 0:
                return

            self._last_bid = bid
            self._last_ask = ask
            event_ts = recv_ts
            spread = ask - bid
            latency_ms = 0.0

            self._momentum.add(event_ts, (bid + ask) / 2)
            momentums = self._momentum.all_momentums()

            tick = BtcTick(
                timestamp_ms=event_ts,
                price=(bid + ask) / 2,
                bid=bid,
                ask=ask,
                spread=spread,
                volume=0.0,
                trade_side="quote",
                momentum_1s=momentums[1],
                momentum_3s=momentums[3],
                momentum_5s=momentums[5],
                momentum_15s=momentums[15],
                momentum_30s=momentums[30],
                latency_ms=latency_ms,
            )
            self._last_tick = tick
            self._messages_recv += 1

            await self._event_bus.publish("btc_book", tick)

        except Exception as exc:
            logger.debug("Error processing bookTicker: %s", exc)


# ─── Minimal EventBus (imported here to avoid circular deps) ──────────────────
# The full EventBus lives in collector.py; this is a forward reference stub.

class EventBus:
    """Simple asyncio pub/sub bus. One queue per topic, broadcast to all subscribers."""

    def __init__(self) -> None:
        self._subscribers: Dict[str, list[asyncio.Queue]] = {}

    def subscribe(self, topic: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        self._subscribers.setdefault(topic, []).append(q)
        return q

    async def publish(self, topic: str, payload) -> None:
        for q in self._subscribers.get(topic, []):
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                pass  # drop if subscriber is slow
