"""
XO Market orderbook imbalance calculations.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple


@dataclass
class OrderbookLevel:
    price: float
    size: float
    side: str  # "yes_bid", "yes_ask", "no_bid", "no_ask"


@dataclass
class ImbalanceSnapshot:
    timestamp_ms: int
    yes_bid_liquidity: float
    yes_ask_liquidity: float
    no_bid_liquidity: float
    no_ask_liquidity: float
    net_imbalance: float  # -100 to +100

    @property
    def is_bullish(self) -> bool:
        return self.net_imbalance > 20

    @property
    def is_bearish(self) -> bool:
        return self.net_imbalance < -20


class ImbalanceEngine:
    """
    Calculates XO Market orderbook imbalance from live orderbook data.

    Formula:
        net_imbalance = (yes_bid_liq - no_bid_liq) / (yes_bid_liq + no_bid_liq + 1e-9) * 100
    """

    def __init__(self) -> None:
        # levels keyed by (side, price)
        self._yes_bids: Dict[float, float] = {}  # price -> size
        self._yes_asks: Dict[float, float] = {}
        self._no_bids: Dict[float, float] = {}
        self._no_asks: Dict[float, float] = {}

    def update_level(self, side: str, price: float, size: float) -> None:
        """
        Update a single orderbook level.
        side: one of "yes_bid", "yes_ask", "no_bid", "no_ask"
        size==0 means remove the level.
        """
        book = self._get_book(side)
        if book is None:
            return
        if size <= 0:
            book.pop(price, None)
        else:
            book[price] = size

    def update_snapshot(self, levels: List[OrderbookLevel]) -> None:
        """Replace orderbook with a full snapshot."""
        self._yes_bids.clear()
        self._yes_asks.clear()
        self._no_bids.clear()
        self._no_asks.clear()
        for lvl in levels:
            self.update_level(lvl.side, lvl.price, lvl.size)

    def _get_book(self, side: str) -> "Dict[float, float] | None":
        mapping = {
            "yes_bid": self._yes_bids,
            "yes_ask": self._yes_asks,
            "no_bid": self._no_bids,
            "no_ask": self._no_asks,
        }
        return mapping.get(side)

    def _total(self, book: Dict[float, float]) -> float:
        return sum(book.values())

    def snapshot(self, timestamp_ms: int | None = None) -> ImbalanceSnapshot:
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)

        yes_bid_liq = self._total(self._yes_bids)
        yes_ask_liq = self._total(self._yes_asks)
        no_bid_liq = self._total(self._no_bids)
        no_ask_liq = self._total(self._no_asks)

        # Core formula
        net_imbalance = (
            (yes_bid_liq - no_bid_liq)
            / (yes_bid_liq + no_bid_liq + 1e-9)
            * 100
        )
        net_imbalance = max(-100.0, min(100.0, net_imbalance))

        return ImbalanceSnapshot(
            timestamp_ms=timestamp_ms,
            yes_bid_liquidity=yes_bid_liq,
            yes_ask_liquidity=yes_ask_liq,
            no_bid_liquidity=no_bid_liq,
            no_ask_liquidity=no_ask_liq,
            net_imbalance=round(net_imbalance, 4),
        )
