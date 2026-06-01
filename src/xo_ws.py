"""
XO Market websocket collector — Socket.IO over WebSocket.

XO Market uses Socket.IO (EIO=4), which wraps messages in a specific protocol:
  - "0{...}"   → EIO open handshake (server sends ping interval)
  - "2"        → EIO ping (server → client)
  - "3"        → EIO pong (client → server, keep-alive response)
  - "40"       → Socket.IO connect ACK
  - "42[event, data]" → Socket.IO message (the data we care about)

We log all raw events on first connect so you can see exact event names and
payload shapes from the real XO Market API.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

import websockets
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

XO_WS_URL = os.getenv(
    "XO_WS_URL",
    "wss://api-mainnet.xo.market/socket.io/?EIO=4&transport=websocket",
)
XO_MARKET_ID = os.getenv("XO_MARKET_ID", "BTC-5M-UP")

RECONNECT_BASE_DELAY = 2.0
RECONNECT_MAX_DELAY = 30.0
RECONNECT_BACKOFF = 2.0

# Log every unique raw event name once so we can learn the real API shape
_LOGGED_EVENT_TYPES: set[str] = set()


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
    """Connects to XO Market via Socket.IO WebSocket and collects prediction market data."""

    def __init__(self, db, event_bus, imbalance_engine) -> None:
        self._db = db
        self._event_bus = event_bus
        self._imbalance = imbalance_engine
        self._connected = False
        self._shutdown = False
        self._last_quote: Optional[XoQuote] = None
        self._messages_recv = 0
        self._market_id = XO_MARKET_ID
        self._ping_interval: float = 25.0
        self._ping_task: Optional[asyncio.Task] = None

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
        # Run WS collector and REST poller concurrently
        await asyncio.gather(
            self._ws_loop(),
            self._rest_poll_loop(),
            return_exceptions=True,
        )

    async def _ws_loop(self) -> None:
        delay = RECONNECT_BASE_DELAY
        while not self._shutdown:
            try:
                await self._connect()
                delay = RECONNECT_BASE_DELAY
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._connected = False
                logger.warning("XO WS disconnected: %s — retrying in %.1fs", exc, delay)
                await self._event_bus.publish("xo_connected", {"connected": False, "error": str(exc)})
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    break
                delay = min(delay * RECONNECT_BACKOFF, RECONNECT_MAX_DELAY)

    async def _rest_poll_loop(self) -> None:
        """Poll XO pulse markets API every 2s for active BTC 5-min market prices."""
        import aiohttp
        # Fetch active pulse market with outcomes included
        list_url = "https://api-mainnet.xo.market/api/pulse/markets?status=active&marketConfigId=2&adapterConfigId=2&limit=1&sortBy=startsAt&sortOrder=DESC"
        headers = {"Origin": "https://beta.xo.market", "Referer": "https://beta.xo.market/"}
        current_market_id: Optional[int] = None

        async with aiohttp.ClientSession(headers=headers) as session:
            while not self._shutdown:
                try:
                    # Step 1: get active market ID
                    async with session.get(list_url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                        if resp.status == 200:
                            body = await resp.json()
                            markets = body.get("data", [])
                            if markets:
                                current_market_id = markets[0].get("id")
                                recv_ts = int(time.time() * 1000)
                                await self._handle_pulse_market(markets[0], recv_ts)

                    # Step 2: fetch full market detail with outcomes if we have an ID
                    if current_market_id:
                        detail_url = f"https://api-mainnet.xo.market/api/pulse/markets/{current_market_id}"
                        async with session.get(detail_url, timeout=aiohttp.ClientTimeout(total=3)) as resp2:
                            if resp2.status == 200:
                                detail = await resp2.json()
                                recv_ts = int(time.time() * 1000)
                                # Log detail structure once
                                if "pulse_market_detail" not in _LOGGED_EVENT_TYPES:
                                    _LOGGED_EVENT_TYPES.add("pulse_market_detail")
                                    logger.info("XO pulse detail structure: %s", str(detail)[:2000])
                                await self._handle_pulse_market(detail, recv_ts)

                except asyncio.CancelledError:
                    break
                except Exception as exc:
                    logger.debug("XO REST poll error: %s", exc)

                try:
                    await asyncio.sleep(2.0)
                except asyncio.CancelledError:
                    break

    async def _handle_pulse_market(self, market: dict, recv_ts: int) -> None:
        """Parse a pulse market record into YES(UP)/NO(DOWN) prices."""
        try:
            if "pulse_market" not in _LOGGED_EVENT_TYPES:
                _LOGGED_EVENT_TYPES.add("pulse_market")
                logger.info("XO pulse market structure: %s", str(market)[:2000])

            status = str(market.get("status", "active"))
            opening_price = float(market.get("openingPrice") or 0)
            outcomes = market.get("outcomes", [])

            # outcomes[0]=UP(YES), outcomes[1]=DOWN(NO)
            # currentPrice is in basis points (divide by 1_000_000)
            up_raw = next((o for o in outcomes if o.get("title", "").upper() == "UP"), None)
            down_raw = next((o for o in outcomes if o.get("title", "").upper() == "DOWN"), None)

            if up_raw is None and len(outcomes) >= 2:
                up_raw, down_raw = outcomes[0], outcomes[1]

            yes_price = float(up_raw.get("currentPrice", 500000)) / 1_000_000 if up_raw else 0.5
            no_price = float(down_raw.get("currentPrice", 500000)) / 1_000_000 if down_raw else 0.5
            volume = float(up_raw.get("volumeTradedInUSD", 0) if up_raw else 0) + \
                     float(down_raw.get("volumeTradedInUSD", 0) if down_raw else 0)
            spread = abs(yes_price - no_price)
            market_id = str(market.get("id", self._market_id))

            logger.debug("XO pulse: UP=%.3f DOWN=%.3f spread=%.3f vol=$%.0f status=%s",
                        yes_price, no_price, spread, volume, status)

            quote = XoQuote(
                timestamp_ms=recv_ts,
                market_id=market_id,
                yes_price=yes_price,
                no_price=no_price,
                spread=spread,
                volume=volume,
                status=status,
                latency_ms=0,
            )
            self._last_quote = quote
            await self._db.insert_xo_quote(
                timestamp_ms=recv_ts,
                market_id=market_id,
                yes_price=yes_price,
                no_price=no_price,
                spread=spread,
                volume=volume,
                status=status,
            )
            await self._event_bus.publish("xo_quote", quote)
        except Exception as exc:
            logger.debug("XO pulse market parse error: %s", exc)

    async def stop(self) -> None:
        self._shutdown = True
        self._connected = False
        if self._ping_task:
            self._ping_task.cancel()

    async def _connect(self) -> None:
        logger.info("Connecting to XO Market WS (Socket.IO): %s", XO_WS_URL)

        async with websockets.connect(
            XO_WS_URL,
            extra_headers={"Origin": "https://xo.market"},
            ping_interval=None,   # we handle pings manually via EIO protocol
            close_timeout=5,
        ) as ws:
            self._connected = True
            logger.info("XO WS connected")
            await self._event_bus.publish("xo_connected", {"connected": True})

            if self._ping_task:
                self._ping_task.cancel()
            self._ping_task = asyncio.create_task(self._ping_loop(ws))

            try:
                async for raw in ws:
                    if self._shutdown:
                        break
                    recv_ts = int(time.time() * 1000)
                    await self._handle_raw(ws, raw, recv_ts)
            finally:
                if self._ping_task:
                    self._ping_task.cancel()
                    self._ping_task = None

        self._connected = False

    async def _ping_loop(self, ws) -> None:
        """Respond to EIO server pings and send keep-alive pongs."""
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                await ws.send("3")  # EIO pong
            except Exception:
                break

    async def _handle_raw(self, ws, raw: str, recv_ts: int) -> None:
        """Parse Socket.IO / Engine.IO framing then dispatch."""
        if not raw:
            return

        # EIO packet type is the first character(s)
        if raw.startswith("0"):
            # EIO open — contains server config JSON
            try:
                config = json.loads(raw[1:])
                self._ping_interval = config.get("pingInterval", 25000) / 1000
                logger.info("XO EIO handshake: pingInterval=%.1fs", self._ping_interval)
            except Exception:
                pass
            # Send Socket.IO connect packet
            await ws.send("40")

        elif raw == "2":
            # EIO ping from server — reply with pong
            await ws.send("3")

        elif raw.startswith("40"):
            # Socket.IO connect ACK — connection is fully ready, now subscribe once
            logger.info("XO Socket.IO connected, subscribing to %s", self._market_id)
            await self._subscribe(ws)

        elif raw.startswith("42"):
            # Socket.IO message: 42["event_name", {...}]
            await self._handle_sio_message(raw[2:], recv_ts)

        elif raw.startswith("43"):
            # Socket.IO ACK response — ignore
            pass

        else:
            logger.debug("XO raw (unhandled EIO type): %r", raw[:80])

    async def _subscribe(self, ws) -> None:
        """Subscribe using the exact format observed from XO Market browser client."""
        subscriptions = [
            '42["subscribe",{"topic":"adapter.twap","adapterConfigId":2}]',
            '42["subscribe",{"topic":"adapter.price.tick","adapterConfigId":2}]',
            f'42["subscribe",{{"topic":"market","marketId":"{self._market_id}"}}]',
            f'42["subscribe",{{"topic":"market.odds","marketId":"{self._market_id}"}}]',
            f'42["subscribe",{{"topic":"market.update","marketId":"{self._market_id}"}}]',
            f'42["subscribe",{{"topic":"orderbook","marketId":"{self._market_id}"}}]',
            f'42["subscribe",{{"topic":"trades","marketId":"{self._market_id}"}}]',
            '42["subscribe",{"topic":"markets"}]',
            '42["subscribe",{"topic":"market.list"}]',
        ]
        for msg in subscriptions:
            try:
                await ws.send(msg)
                logger.debug("XO subscribed: %s", msg[30:90])
            except Exception as exc:
                logger.debug("XO subscribe send error: %s", exc)
            await asyncio.sleep(0.05)

    async def _handle_sio_message(self, payload: str, recv_ts: int) -> None:
        """Parse 42["event", data] Socket.IO messages."""
        try:
            parsed = json.loads(payload)
        except Exception:
            logger.debug("XO SIO parse error: %r", payload[:120])
            return

        if not isinstance(parsed, list) or len(parsed) < 2:
            return

        event_name: str = parsed[0]
        data = parsed[1] if len(parsed) > 1 else {}
        self._messages_recv += 1

        # Log every new event type once so we learn the real API shape
        if event_name not in _LOGGED_EVENT_TYPES:
            _LOGGED_EVENT_TYPES.add(event_name)
            logger.info("XO new event type: %r  sample: %s", event_name, str(data)[:200])

        el = event_name.lower()
        if event_name == "adapter.price.tick":
            await self._handle_btc_price_tick(data, recv_ts)
        elif event_name == "adapter.twap":
            await self._handle_twap(data, recv_ts)
        elif any(k in el for k in ("quote", "odds", "market.update", "market_update")):
            await self._handle_quote(data, recv_ts)
        elif any(k in el for k in ("trade", "fill", "match")):
            await self._handle_trade(data, recv_ts)
        elif any(k in el for k in ("orderbook", "book", "depth")):
            await self._handle_orderbook(data, recv_ts)
        else:
            logger.debug("XO unrouted event: %r", event_name)

    async def _handle_btc_price_tick(self, data: dict, recv_ts: int) -> None:
        """Handle adapter.price.tick — BTC spot price from Binance via XO TWAP service."""
        try:
            price = float(data.get("price", 0))
            symbol = data.get("symbol", "BTCUSDT")
            event_ts = int(recv_ts)
            raw_ts = data.get("timestamp", "")
            if raw_ts:
                from datetime import datetime, timezone
                dt = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
                event_ts = int(dt.timestamp() * 1000)
            latency_ms = recv_ts - event_ts
            logger.debug("XO BTC price tick: %s=%.2f latency=%dms", symbol, price, latency_ms)
            await self._event_bus.publish("xo_btc_price", {"price": price, "timestamp_ms": event_ts, "latency_ms": latency_ms})
            await self._db.insert_latency(timestamp_ms=recv_ts, source="xo_btc_tick", latency_ms=max(0, latency_ms))
        except Exception as exc:
            logger.debug("Error processing BTC price tick: %s", exc)

    async def _handle_twap(self, data: dict, recv_ts: int) -> None:
        """Handle adapter.twap — TWAP price used by XO for market settlement."""
        try:
            price = float(data.get("price", data.get("twap", data.get("value", 0))))
            logger.debug("XO TWAP update: %.2f  raw=%s", price, str(data)[:120])
            await self._event_bus.publish("xo_twap", {"price": price, "timestamp_ms": recv_ts, "data": data})
        except Exception as exc:
            logger.debug("Error processing TWAP: %s", exc)

    async def _handle_quote(self, data: dict, recv_ts: int) -> None:
        try:
            event_ts = int(data.get("timestamp", data.get("ts", recv_ts)))
            yes_price = float(data.get("yes_price", data.get("yesPrice", data.get("yes", 0.5))))
            no_price = float(data.get("no_price", data.get("noPrice", data.get("no", 0.5))))
            volume = float(data.get("volume", data.get("vol", 0.0)))
            status = str(data.get("status", "active"))
            spread = abs(yes_price - no_price)
            latency_ms = recv_ts - event_ts if event_ts <= recv_ts else 0.0

            quote = XoQuote(
                timestamp_ms=event_ts,
                market_id=self._market_id,
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
                market_id=self._market_id,
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

    async def _handle_trade(self, data: dict, recv_ts: int) -> None:
        try:
            event_ts = int(data.get("timestamp", data.get("ts", recv_ts)))
            side = str(data.get("side", data.get("outcome", "unknown")))
            price = float(data.get("price", 0.0))
            size = float(data.get("size", data.get("amount", 0.0)))

            trade = XoTrade(
                timestamp_ms=event_ts,
                market_id=self._market_id,
                side=side,
                price=price,
                size=size,
            )
            await self._db.insert_xo_trade(
                timestamp_ms=event_ts,
                market_id=self._market_id,
                side=side,
                price=price,
                size=size,
            )
            await self._event_bus.publish("xo_trade", trade)

        except Exception as exc:
            logger.debug("Error processing XO trade: %s", exc)

    async def _handle_orderbook(self, data: dict, recv_ts: int) -> None:
        try:
            from src.imbalance import OrderbookLevel
            event_ts = int(data.get("timestamp", data.get("ts", recv_ts)))

            levels = []
            for side_key, side_name in [
                ("yes_bids", "yes_bid"), ("yes_asks", "yes_ask"),
                ("no_bids", "no_bid"),   ("no_asks", "no_ask"),
                # alternate naming
                ("yesBids", "yes_bid"),  ("yesAsks", "yes_ask"),
                ("noBids", "no_bid"),    ("noAsks", "no_ask"),
            ]:
                for entry in data.get(side_key, []):
                    price = float(entry.get("price", entry.get("p", 0)))
                    size = float(entry.get("size", entry.get("s", entry.get("amount", 0))))
                    if price > 0:
                        levels.append(OrderbookLevel(price=price, size=size, side=side_name))
                        await self._db.insert_xo_orderbook(
                            timestamp_ms=event_ts,
                            market_id=self._market_id,
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
