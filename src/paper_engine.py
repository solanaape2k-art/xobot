"""
Paper trading engine. PAPER ONLY — no real money, no real orders.
Tracks virtual trades, calculates PnL and aggregate stats.
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

# ─── Safety guard ─────────────────────────────────────────────────────────────
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").strip().lower()
if LIVE_TRADING == "true":
    raise RuntimeError(
        "LIVE_TRADING=true is forbidden. This is a paper-trading-only system."
    )
# ──────────────────────────────────────────────────────────────────────────────

logger = logging.getLogger(__name__)

PAPER_POSITION_SIZE = float(os.getenv("PAPER_POSITION_SIZE", "100"))
PAPER_HOLD_TIME_SECONDS = float(os.getenv("PAPER_HOLD_TIME_SECONDS", "300"))
PAPER_STOP_LOSS_PCT = float(os.getenv("PAPER_STOP_LOSS_PCT", "5.0"))


@dataclass
class PaperPosition:
    trade_id: str
    market_id: str
    side: str  # "YES" | "NO"
    entry_price: float
    size: float
    entry_ts_ms: int
    signal_id: int
    status: str = "open"  # "open" | "closed"
    exit_price: float = 0.0
    exit_ts_ms: int = 0
    pnl: float = 0.0


@dataclass
class TradeStats:
    total_pnl: float = 0.0
    trade_count: int = 0
    win_count: int = 0
    loss_count: int = 0
    win_rate: float = 0.0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    sharpe_ratio: float = 0.0
    avg_hold_time_s: float = 0.0

    def to_dict(self) -> dict:
        return {
            "total_pnl": round(self.total_pnl, 4),
            "trade_count": self.trade_count,
            "win_count": self.win_count,
            "loss_count": self.loss_count,
            "win_rate": round(self.win_rate, 2),
            "avg_win": round(self.avg_win, 4),
            "avg_loss": round(self.avg_loss, 4),
            "profit_factor": round(self.profit_factor, 4),
            "max_drawdown": round(self.max_drawdown, 4),
            "sharpe_ratio": round(self.sharpe_ratio, 4),
            "avg_hold_time_s": round(self.avg_hold_time_s, 1),
        }


class PaperEngine:
    """
    Virtual paper trading engine.
    Enters and exits positions based on signals. No real orders ever placed.
    """

    def __init__(self, db) -> None:
        self._db = db
        self._open_positions: Dict[str, PaperPosition] = {}
        self._closed_trades: List[PaperPosition] = []
        self._trade_counter = 0
        self._shutdown = False
        self._monitor_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        self._monitor_task = asyncio.create_task(self._position_monitor())
        logger.info("Paper engine started (PAPER TRADING ONLY)")

    async def stop(self) -> None:
        self._shutdown = True
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    async def enter_trade(
        self,
        signal_id: int,
        market_id: str,
        side: str,
        entry_price: float,
        size: float = PAPER_POSITION_SIZE,
    ) -> Optional[PaperPosition]:
        """Enter a paper trade. Returns the position."""
        # Only one open position per market/side
        key = f"{market_id}_{side}"
        if key in self._open_positions:
            logger.debug("Already in position %s — skipping", key)
            return None

        self._trade_counter += 1
        trade_id = f"PT-{self._trade_counter:06d}"
        pos = PaperPosition(
            trade_id=trade_id,
            market_id=market_id,
            side=side,
            entry_price=entry_price,
            size=size,
            entry_ts_ms=int(time.time() * 1000),
            signal_id=signal_id,
        )
        self._open_positions[key] = pos
        logger.info(
            "PAPER ENTER | %s | side=%s | price=%.4f | size=%.0f",
            trade_id, side, entry_price, size,
        )
        return pos

    async def exit_trade(
        self,
        market_id: str,
        side: str,
        exit_price: float,
        reason: str = "hold_time",
    ) -> Optional[PaperPosition]:
        """Exit a paper trade by market/side."""
        key = f"{market_id}_{side}"
        pos = self._open_positions.pop(key, None)
        if pos is None:
            return None

        pos.exit_price = exit_price
        pos.exit_ts_ms = int(time.time() * 1000)
        pos.status = "closed"
        pos.pnl = self._calc_pnl(pos)

        self._closed_trades.append(pos)

        hold_ms = pos.exit_ts_ms - pos.entry_ts_ms

        logger.info(
            "PAPER EXIT  | %s | side=%s | entry=%.4f | exit=%.4f | pnl=%.4f%% | hold=%ds | reason=%s",
            pos.trade_id, pos.side,
            pos.entry_price, pos.exit_price,
            pos.pnl, hold_ms // 1000, reason,
        )

        await self._db.insert_paper_trade(
            timestamp_ms=pos.entry_ts_ms,
            market_id=pos.market_id,
            side=pos.side,
            entry_price=pos.entry_price,
            exit_price=pos.exit_price,
            size=pos.size,
            pnl=pos.pnl,
            hold_time_ms=hold_ms,
            status="closed",
            signal_id=pos.signal_id,
        )

        return pos

    def _calc_pnl(self, pos: PaperPosition) -> float:
        """
        PnL as percentage.
        For YES long: (exit - entry) / entry * 100
        For NO long:  (entry - exit) / entry * 100  (NO price falls when BTC rises)
        """
        if pos.entry_price <= 0:
            return 0.0
        if pos.side == "YES":
            return (pos.exit_price - pos.entry_price) / pos.entry_price * 100
        else:  # NO
            return (pos.entry_price - pos.exit_price) / pos.entry_price * 100

    async def _position_monitor(self) -> None:
        """Background task that closes positions on hold_time or stop_loss."""
        hold_time_ms = PAPER_HOLD_TIME_SECONDS * 1000
        stop_loss_pct = PAPER_STOP_LOSS_PCT

        while not self._shutdown:
            try:
                await asyncio.sleep(1.0)
                now_ms = int(time.time() * 1000)

                to_exit = []
                for key, pos in list(self._open_positions.items()):
                    age_ms = now_ms - pos.entry_ts_ms
                    # Use last known price for stop-loss check (approximate)
                    # In live mode this would use real-time price
                    unrealized_pnl = 0.0  # Can't calculate without current price here

                    if age_ms >= hold_time_ms:
                        to_exit.append((pos, "hold_time"))

                for pos, reason in to_exit:
                    # Exit at entry price as placeholder (no current price available here)
                    # In practice, the strategy loop calls exit_trade with actual price
                    await self.exit_trade(pos.market_id, pos.side, pos.entry_price, reason)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Position monitor error: %s", exc)

    async def check_stop_loss(
        self, market_id: str, side: str, current_price: float
    ) -> bool:
        """Check and execute stop loss if triggered. Returns True if stopped out."""
        key = f"{market_id}_{side}"
        pos = self._open_positions.get(key)
        if pos is None:
            return False

        # Simulate PnL with current price
        temp_pos = PaperPosition(
            trade_id=pos.trade_id,
            market_id=pos.market_id,
            side=pos.side,
            entry_price=pos.entry_price,
            size=pos.size,
            entry_ts_ms=pos.entry_ts_ms,
            signal_id=pos.signal_id,
            exit_price=current_price,
        )
        unrealized_pnl = self._calc_pnl(temp_pos)

        if unrealized_pnl <= -PAPER_STOP_LOSS_PCT:
            await self.exit_trade(market_id, side, current_price, "stop_loss")
            return True
        return False

    async def update_prices(
        self, market_id: str, yes_price: float, no_price: float
    ) -> None:
        """Call this on every price update to check stop losses."""
        await self.check_stop_loss(market_id, "YES", yes_price)
        await self.check_stop_loss(market_id, "NO", no_price)

    def get_stats(self) -> TradeStats:
        """Compute aggregate stats from closed trades."""
        trades = self._closed_trades
        if not trades:
            return TradeStats()

        pnls = [t.pnl for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]

        total_pnl = sum(pnls)
        win_rate = len(wins) / len(pnls) * 100 if pnls else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        # Max drawdown (on cumulative PnL series)
        cum_pnl = 0.0
        peak = 0.0
        max_dd = 0.0
        for p in pnls:
            cum_pnl += p
            if cum_pnl > peak:
                peak = cum_pnl
            dd = peak - cum_pnl
            if dd > max_dd:
                max_dd = dd

        # Sharpe ratio (simplified: mean/std of returns)
        sharpe = 0.0
        if len(pnls) >= 2:
            mean = total_pnl / len(pnls)
            variance = sum((p - mean) ** 2 for p in pnls) / (len(pnls) - 1)
            std = math.sqrt(variance)
            sharpe = mean / std if std > 0 else 0.0

        # Average hold time
        hold_times = [
            (t.exit_ts_ms - t.entry_ts_ms) / 1000
            for t in trades
            if t.exit_ts_ms > 0
        ]
        avg_hold = sum(hold_times) / len(hold_times) if hold_times else 0.0

        return TradeStats(
            total_pnl=total_pnl,
            trade_count=len(pnls),
            win_count=len(wins),
            loss_count=len(losses),
            win_rate=win_rate,
            avg_win=avg_win,
            avg_loss=avg_loss,
            profit_factor=profit_factor,
            max_drawdown=max_dd,
            sharpe_ratio=sharpe,
            avg_hold_time_s=avg_hold,
        )

    def get_open_positions(self) -> List[dict]:
        return [
            {
                "trade_id": p.trade_id,
                "market_id": p.market_id,
                "side": p.side,
                "entry_price": p.entry_price,
                "size": p.size,
                "entry_ts_ms": p.entry_ts_ms,
                "age_s": round((time.time() * 1000 - p.entry_ts_ms) / 1000, 1),
            }
            for p in self._open_positions.values()
        ]
