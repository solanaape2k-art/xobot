"""
XO Market websocket collector.
XO Market is a prediction market — exact API format is unknown (TODO: update when docs available).
Uses configurable XO_WS_URL env var. Fails gracefully if unavailable.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional, Dict

import websockets
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

XO_WS_URL = os.getenv("XO_WS_URL", "wss://api.xo.market/ws")
XO_MARKET_ID = os.getenv("XO_MARKET_ID", "BTC-5M-UP")

RECONNECT_BASE_DELAY = 2.0
RECONNECT_MAX_DELAY = 120.0
RECONNECT_BACKOFF = 2.0


@dataclass
class XoQuote:
    timestamp_ms: int
    market_id: str
    yes_price: float
    no_price: float
    spread: float
    volume: float
    status: str
    latency_ms: float


@dataclass
class XoTrade:
    timestamp_ms: int
    market_id: str
    side: str
    price: float
    size: float


@dataclass
class XoOrderbookLevel:
    timestamp_ms: int
    market_id: str
    side: str  # "yes_bid" | "yes_ask" | "no_bid" | "no_ask"
    price: float
    size: float


class XoWebsocketCollector:
    """
    Connects to XO Market websocket and collects prediction market data.

    NOTE: XO Market's exact websocket message format is unknown at time of writing.
    The code below handles a reasonable predicted format and can be updated when
    official XO Market API documentation becomes available.

    Message format assumed (TODO: verify with actual XO Market docs):
      {"type": "quote", "market_id": "BTC-5M-UP", "yes_price": 0.52, "no_price": 0.48, ...}
      {"type": "trade", "market_id": "BTC-5M-UP", "side": "yes", "price": 0.52, "size": 100}
      {"type": "orderbook", "market_id": "BTC-5M-UP", "bids": [...], "asks": [...]}
    """

    def __init__(
        self,
        db,
        event_bus,
        imbalance_engine,
    ) -> None:
        self._db = db
        self._event_bus = event_bus
        self._imbalance = imbalance_engine
        self._connected = False
        self._shutdown = False
        self._last_quote: Optional[XoQuote] = None
        self._messages_recv = 0
        self._market_id = XO_MARKET_ID

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def last_quote(self) -> Optional[XoQuote]:
        return self._last_quote

    @property
    def messages_received(self) -> int:
        return self._messages_recv

    async def start(self) -> None:
        """Start WS collector with reconnect loop. Non-fatal if XO is unavailable."""
        delay = RECONNECT_BASE_DELAY
        while not self._shutdown:
            try:
                await self._connect()
                delay = RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._connected = False
                logger.warning(
                    "XO WS disconnected/unavailable: %s — retrying in %.1fs", exc, delay
                )
                await self._event_bus.publish("xo_connected", {"connected": False, "error": str(exc)})
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                delay = min(delay * RECONNECT_BACKOFF, RECONNECT_MAX_DELAY)

    async def stop(self) -> None:
        self._shutdown = True
        self._connected = False

    async def _connect(self) -> None:
        url = XO_WS_URL
        logger.info("Connecting to XO Market WS: %s", url)

        async with websockets.connect(
            url,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            self._connected = True
            logger.info("XO WS connected")
            await self._event_bus.publish("xo_connected", {"connected": True})

            # Subscribe to market (format TBD by XO Market docs)
            subscribe_msg = json.dumps({
                "action": "subscribe",
                "market_id": self._market_id,
                "channels": ["quotes", "trades", "orderbook"],
            })
            await ws.send(subscribe_msg)

            async for raw in ws:
                if self._shutdown:
                    break
                recv_ts = int(time.time() * 1000)
                try:
                    msg = json.loads(raw)
                    await self._handle_message(msg, recv_ts)
                except Exception as exc:
                    logger.debug("XO WS parse error: %s", exc)

        self._connected = False

    async def _handle_message(self, msg: dict, recv_ts: int) -> None:
        """
        Route incoming messages by type.
        TODO: Update message parsing once XO Market API docs are available.
        """
        msg_type = msg.get("type", "")
        market_id = msg.get("market_id", self._market_id)
        self._messages_recv += 1

        if msg_type == "quote":
            await self._handle_quote(msg, market_id, recv_ts)
        elif msg_type == "trade":
            await self._handle_trade(msg, market_id, recv_ts)
        elif msg_type == "orderbook":
            await self._handle_orderbook(msg, market_id, recv_ts)
        elif msg_type == "market_status":
            await self._handle_market_status(msg, market_id, recv_ts)
        else:
            logger.debug("Unknown XO message type: %s", msg_type)

    async def _handle_quote(self, msg: dict, market_id: str, recv_ts: int) -> None:
        try:
            event_ts = int(msg.get("timestamp", recv_ts))
            yes_price = float(msg.get("yes_price", 0.5))
            no_price = float(msg.get("no_price", 0.5))
            volume = float(msg.get("volume", 0.0))
            status = msg.get("status", "active")
            spread = abs(yes_price - no_price)
            latency_ms = recv_ts - event_ts

            quote = XoQuote(
                timestamp_ms=event_ts,
                market_id=market_id,
                yes_price=yes_price,
                no_price=no_price,
                spread=spread,
                volume=volume,
                status=status,
                latency_ms=latency_ms,
            )
            self._last_quote = quote

            await self._db.insert_xo_quote(
                timestamp_ms=event_ts,
                market_id=market_id,
                yes_price=yes_price,
                no_price=no_price,
                spread=spread,
                volume=volume,
                status=status,
            )

            if latency_ms >= 0:
                await self._db.insert_latency(
                    timestamp_ms=recv_ts,
                    source="xo_quote",
                    latency_ms=latency_ms,
                )

            await self._event_bus.publish("xo_quote", quote)

        except Exception as exc:
            logger.debug("Error processing XO quote: %s", exc)

    async def _handle_trade(self, msg: dict, market_id: str, recv_ts: int) -> None:
        try:
            event_ts = int(msg.get("timestamp", recv_ts))
            side = msg.get("side", "unknown")
            price = float(msg.get("price", 0.0))
            size = float(msg.get("size", 0.0))

            trade = XoTrade(
                timestamp_ms=event_ts,
                market_id=market_id,
                side=side,
                price=price,
                size=size,
            )

            await self._db.insert_xo_trade(
                timestamp_ms=event_ts,
                market_id=market_id,
                side=side,
                price=price,
                size=size,
            )

            await self._event_bus.publish("xo_trade", trade)

        except Exception as exc:
            logger.debug("Error processing XO trade: %s", exc)

    async def _handle_orderbook(self, msg: dict, market_id: str, recv_ts: int) -> None:
        """
        Handle orderbook update.
        Expected format (TODO: verify):
          {
            "type": "orderbook",
            "market_id": "BTC-5M-UP",
            "yes_bids": [{"price": 0.52, "size": 100}, ...],
            "yes_asks": [{"price": 0.54, "size": 200}, ...],
            "no_bids": [{"price": 0.46, "size": 150}, ...],
            "no_asks": [{"price": 0.48, "size": 80}, ...],
          }
        """
        try:
            event_ts = int(msg.get("timestamp", recv_ts))
            from src.imbalance import OrderbookLevel

            levels = []
            for side_key, side_name in [
                ("yes_bids", "yes_bid"),
                ("yes_asks", "yes_ask"),
                ("no_bids", "no_bid"),
                ("no_asks", "no_ask"),
            ]:
                for entry in msg.get(side_key, []):
                    price = float(entry.get("price", 0))
                    size = float(entry.get("size", 0))
                    if price > 0:
                        levels.append(OrderbookLevel(price=price, size=size, side=side_name))
                        await self._db.insert_xo_orderbook(
                            timestamp_ms=event_ts,
                            market_id=market_id,
                            side=side_name,
                            price=price,
                            size=size,
                        )

            if levels:
                self._imbalance.update_snapshot(levels)
                snap = self._imbalance.snapshot(event_ts)
                await self._event_bus.publish("xo_imbalance", snap)

        except Exception as exc:
            logger.debug("Error processing XO orderbook: %s", exc)

    async def _handle_market_status(self, msg: dict, market_id: str, recv_ts: int) -> None:
        status = msg.get("status", "unknown")
        logger.info("XO Market status: %s -> %s", market_id, status)
        await self._event_bus.publish("xo_market_status", {"market_id": market_id, "status": status})
