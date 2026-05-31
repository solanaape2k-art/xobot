"""
Technical indicators calculated from btc_ticks and xo_quotes.
Returns IndicatorSnapshot dataclass.
"""

from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Deque, List, Optional, Tuple


@dataclass
class IndicatorSnapshot:
    timestamp_ms: int
    price: float
    rsi_14: float  # 0-100
    vwap: float
    vwap_deviation_pct: float  # (price - vwap) / vwap * 100
    volume_spike: bool
    spread_pct: float  # spread / mid * 100
    change_1m_pct: float
    change_5m_pct: float
    orderbook_imbalance: float  # -1 to +1
    liquidity_depth: float  # total size within 0.1% of mid
    momentum_score: float  # -100 to +100
    volatility_score: float  # annualized rolling std of returns


@dataclass
class _PriceSample:
    timestamp_ms: int
    price: float
    volume: float
    bid: float
    ask: float


class IndicatorEngine:
    """
    Streaming indicator engine. Call update() with each new BTC tick.
    """

    RSI_PERIOD: int = 14
    VOL_SPIKE_PERIOD: int = 20
    PRICE_HISTORY_SECONDS: int = 400  # keep 5+ min

    def __init__(self) -> None:
        self._samples: Deque[_PriceSample] = deque(maxlen=5000)
        # RSI state
        self._rsi_gains: Deque[float] = deque(maxlen=self.RSI_PERIOD)
        self._rsi_losses: Deque[float] = deque(maxlen=self.RSI_PERIOD)
        self._rsi_avg_gain: float = 0.0
        self._rsi_avg_loss: float = 0.0
        self._rsi_initialized: bool = False
        # VWAP state (session)
        self._session_start_ms: int = 0
        self._vwap_cum_pv: float = 0.0
        self._vwap_cum_vol: float = 0.0
        # Volume spike
        self._vol_window: Deque[float] = deque(maxlen=self.VOL_SPIKE_PERIOD)
        # Returns for volatility
        self._returns: Deque[float] = deque(maxlen=300)

    def _reset_session(self, timestamp_ms: int) -> None:
        self._session_start_ms = timestamp_ms
        self._vwap_cum_pv = 0.0
        self._vwap_cum_vol = 0.0

    def update(
        self,
        timestamp_ms: int,
        price: float,
        bid: float,
        ask: float,
        spread: float,
        volume: float,
        momentum_1s: float,
        momentum_3s: float,
        momentum_5s: float,
        momentum_15s: float,
        momentum_30s: float,
    ) -> IndicatorSnapshot:
        now_ms = timestamp_ms
        sample = _PriceSample(
            timestamp_ms=now_ms,
            price=price,
            volume=volume,
            bid=bid,
            ask=ask,
        )

        # Session reset (UTC midnight)
        now_day = now_ms // 86_400_000
        session_day = self._session_start_ms // 86_400_000 if self._session_start_ms else -1
        if now_day != session_day:
            self._reset_session(now_ms)

        # VWAP
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else price
        self._vwap_cum_pv += mid * volume
        self._vwap_cum_vol += volume
        vwap = self._vwap_cum_pv / self._vwap_cum_vol if self._vwap_cum_vol > 0 else mid
        vwap_dev = (price - vwap) / vwap * 100 if vwap > 0 else 0.0

        # RSI
        if self._samples:
            prev_price = self._samples[-1].price
            delta = price - prev_price
            gain = max(delta, 0.0)
            loss = max(-delta, 0.0)
            self._rsi_gains.append(gain)
            self._rsi_losses.append(loss)
            if len(self._rsi_gains) == self.RSI_PERIOD:
                if not self._rsi_initialized:
                    self._rsi_avg_gain = sum(self._rsi_gains) / self.RSI_PERIOD
                    self._rsi_avg_loss = sum(self._rsi_losses) / self.RSI_PERIOD
                    self._rsi_initialized = True
                else:
                    self._rsi_avg_gain = (self._rsi_avg_gain * (self.RSI_PERIOD - 1) + gain) / self.RSI_PERIOD
                    self._rsi_avg_loss = (self._rsi_avg_loss * (self.RSI_PERIOD - 1) + loss) / self.RSI_PERIOD
            # Return for volatility
            if prev_price > 0:
                ret = (price - prev_price) / prev_price
                self._returns.append(ret)

        self._samples.append(sample)

        rsi = self._calc_rsi()

        # Volume spike
        self._vol_window.append(volume)
        vol_mean = sum(self._vol_window) / len(self._vol_window) if self._vol_window else 1.0
        volume_spike = volume > 2 * vol_mean if vol_mean > 0 else False

        # Spread %
        mid_price = (bid + ask) / 2 if bid > 0 and ask > 0 else price
        spread_pct = spread / mid_price * 100 if mid_price > 0 else 0.0

        # Price changes
        change_1m = self._price_change_pct(now_ms, 60_000)
        change_5m = self._price_change_pct(now_ms, 300_000)

        # Orderbook imbalance proxy from bid/ask size (simplified — no depth here)
        ob_imbalance = 0.0  # Will be overridden by imbalance.py with real depth

        # Liquidity depth proxy (spread-based)
        liquidity_depth = 0.0

        # Momentum score: weighted sum, clamped to [-100, +100]
        momentum_score = self._calc_momentum_score(
            momentum_1s, momentum_3s, momentum_5s, momentum_15s, momentum_30s
        )

        # Volatility (annualized)
        volatility_score = self._calc_volatility()

        return IndicatorSnapshot(
            timestamp_ms=now_ms,
            price=price,
            rsi_14=rsi,
            vwap=vwap,
            vwap_deviation_pct=vwap_dev,
            volume_spike=volume_spike,
            spread_pct=spread_pct,
            change_1m_pct=change_1m,
            change_5m_pct=change_5m,
            orderbook_imbalance=ob_imbalance,
            liquidity_depth=liquidity_depth,
            momentum_score=momentum_score,
            volatility_score=volatility_score,
        )

    def _calc_rsi(self) -> float:
        if not self._rsi_initialized:
            return 50.0
        if self._rsi_avg_loss == 0:
            return 100.0
        rs = self._rsi_avg_gain / self._rsi_avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _price_change_pct(self, now_ms: int, window_ms: int) -> float:
        cutoff = now_ms - window_ms
        old_sample = None
        for s in self._samples:
            if s.timestamp_ms >= cutoff:
                old_sample = s
                break
        if old_sample is None or not self._samples:
            return 0.0
        current = self._samples[-1].price
        old_price = old_sample.price
        if old_price == 0:
            return 0.0
        return (current - old_price) / old_price * 100

    def _calc_momentum_score(
        self,
        m1: float,
        m3: float,
        m5: float,
        m15: float,
        m30: float,
    ) -> float:
        """Weighted combination, normalized to [-100, +100]."""
        # Weights: shorter = more weight
        weighted = m1 * 0.35 + m3 * 0.25 + m5 * 0.20 + m15 * 0.12 + m30 * 0.08
        # Scale: assume max raw momentum ~1.0% => maps to 100
        score = weighted * 100
        return max(-100.0, min(100.0, score))

    def _calc_volatility(self) -> float:
        """Rolling std of returns, annualized (assuming ~1s ticks → 86400 ticks/day)."""
        if len(self._returns) < 2:
            return 0.0
        n = len(self._returns)
        mean = sum(self._returns) / n
        variance = sum((r - mean) ** 2 for r in self._returns) / (n - 1)
        std = math.sqrt(variance)
        # Annualize: sqrt(86400 * 365) for per-second returns
        annualized = std * math.sqrt(86400 * 365)
        return round(annualized * 100, 4)  # as percentage
