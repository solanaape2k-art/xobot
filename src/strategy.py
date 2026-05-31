"""
Signal generation strategy. PAPER TRADING ONLY — never places real orders.

Generates LONG_YES / LONG_NO / NO_TRADE signals based on:
  - BTC momentum
  - XO orderbook imbalance
  - Volume spike
  - Spread compression
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ─── Safety guard ─────────────────────────────────────────────────────────────
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").strip().lower()
if LIVE_TRADING == "true":
    raise RuntimeError(
        "LIVE_TRADING=true is not allowed in this bot. "
        "This is a paper-trading-only system."
    )
# ──────────────────────────────────────────────────────────────────────────────


class SignalType(str, Enum):
    LONG_YES = "LONG_YES"
    LONG_NO = "LONG_NO"
    NO_TRADE = "NO_TRADE"


@dataclass
class Signal:
    timestamp_ms: int
    signal_type: SignalType
    confidence: float  # 0.0 to 1.0
    btc_momentum_5s: float
    imbalance_score: float
    spread_compression: float  # current_spread / rolling_mean_spread
    volume_spike: bool
    entry_price: float
    market_id: str
    # Component scores for logging/debugging
    momentum_condition: bool
    imbalance_condition: bool
    volume_condition: bool
    spread_condition: bool


def _load_thresholds() -> tuple[float, float, float]:
    momentum_threshold = float(os.getenv("MOMENTUM_THRESHOLD", "0.1"))
    imbalance_threshold = float(os.getenv("IMBALANCE_THRESHOLD", "20.0"))
    spread_compression_ratio = float(os.getenv("SPREAD_COMPRESSION_RATIO", "0.9"))
    return momentum_threshold, imbalance_threshold, spread_compression_ratio


class StrategyEngine:
    """
    Evaluates conditions and emits trading signals.
    PAPER ONLY — no HTTP calls, no order placement.
    """

    def __init__(self) -> None:
        # Spread history for rolling mean
        self._spread_history: list[float] = []
        self._spread_history_limit: int = 50

    def update_spread(self, spread: float) -> None:
        """Feed current spread to update rolling mean."""
        self._spread_history.append(spread)
        if len(self._spread_history) > self._spread_history_limit:
            self._spread_history.pop(0)

    @property
    def rolling_mean_spread(self) -> float:
        if not self._spread_history:
            return 1.0
        return sum(self._spread_history) / len(self._spread_history)

    def evaluate(
        self,
        timestamp_ms: int,
        btc_momentum_5s: float,
        imbalance_score: float,
        volume_spike: bool,
        current_spread: float,
        yes_price: float,
        market_id: str,
    ) -> Signal:
        """
        Evaluate all conditions and return a Signal.
        Returns NO_TRADE if conditions are not met.
        """
        # Refresh thresholds each call (allows runtime env changes)
        momentum_threshold, imbalance_threshold, spread_ratio = _load_thresholds()

        self.update_spread(current_spread)
        mean_spread = self.rolling_mean_spread
        spread_compression = current_spread / mean_spread if mean_spread > 0 else 1.0
        spread_compressing = spread_compression < spread_ratio

        # Condition evaluation
        momentum_long = btc_momentum_5s > momentum_threshold
        momentum_short = btc_momentum_5s < -momentum_threshold
        imbalance_long = imbalance_score > imbalance_threshold
        imbalance_short = imbalance_score < -imbalance_threshold

        if all([momentum_long, imbalance_long, volume_spike, spread_compressing]):
            signal_type = SignalType.LONG_YES
            conditions_met = [True, True, True, True]
            confidence = self._calc_confidence(
                btc_momentum_5s, imbalance_score, spread_compression, momentum_threshold, imbalance_threshold
            )
        elif all([momentum_short, imbalance_short, volume_spike, spread_compressing]):
            signal_type = SignalType.LONG_NO
            conditions_met = [True, True, True, True]
            confidence = self._calc_confidence(
                abs(btc_momentum_5s), abs(imbalance_score), spread_compression, momentum_threshold, imbalance_threshold
            )
        else:
            signal_type = SignalType.NO_TRADE
            conditions_met = [
                momentum_long or momentum_short,
                imbalance_long or imbalance_short,
                volume_spike,
                spread_compressing,
            ]
            confidence = 0.0

        if signal_type != SignalType.NO_TRADE:
            logger.info(
                "Signal: %s | momentum=%.4f | imbalance=%.2f | vol_spike=%s | spread_comp=%.3f | confidence=%.2f",
                signal_type.value,
                btc_momentum_5s,
                imbalance_score,
                volume_spike,
                spread_compression,
                confidence,
            )

        return Signal(
            timestamp_ms=timestamp_ms,
            signal_type=signal_type,
            confidence=confidence,
            btc_momentum_5s=btc_momentum_5s,
            imbalance_score=imbalance_score,
            spread_compression=spread_compression,
            volume_spike=volume_spike,
            entry_price=yes_price,
            market_id=market_id,
            momentum_condition=conditions_met[0],
            imbalance_condition=conditions_met[1],
            volume_condition=conditions_met[2],
            spread_condition=conditions_met[3],
        )

    def _calc_confidence(
        self,
        momentum: float,
        imbalance: float,
        spread_compression: float,
        momentum_threshold: float,
        imbalance_threshold: float,
    ) -> float:
        """
        Confidence from 0.0 to 1.0 based on how strongly conditions are met.
        """
        # Momentum: how much above threshold
        m_score = min(1.0, (abs(momentum) - momentum_threshold) / momentum_threshold) if momentum_threshold > 0 else 0.5
        # Imbalance: how far above threshold
        i_score = min(1.0, (abs(imbalance) - imbalance_threshold) / imbalance_threshold) if imbalance_threshold > 0 else 0.5
        # Spread: how compressed (lower is stronger)
        s_score = max(0.0, 1.0 - spread_compression)
        confidence = 0.4 * m_score + 0.4 * i_score + 0.2 * s_score
        return round(min(1.0, max(0.0, confidence)), 4)
