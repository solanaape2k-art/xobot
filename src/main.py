"""
XO Market BTC Prediction Bot — Main Entry Point.
PAPER TRADING ONLY — no real orders placed.

Usage:
  python -m src.main --mode live       # collectors + strategy + paper engine + dashboard
  python -m src.main --mode collect    # collectors only
  python -m src.main --mode replay     # replay historical data
  python -m src.main --mode dashboard  # dashboard only (reads existing DB)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

load_dotenv()

# Ensure project root is on sys.path so `src.*` imports work whether
# invoked as `python src/main.py` or `python -m src.main`
_project_root = str(Path(__file__).resolve().parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# ─── Safety guard ─────────────────────────────────────────────────────────────
LIVE_TRADING = os.getenv("LIVE_TRADING", "false").strip().lower()
if LIVE_TRADING == "true":
    print("ERROR: LIVE_TRADING=true is not allowed. This is a paper-trading-only system.")
    sys.exit(1)
# ──────────────────────────────────────────────────────────────────────────────

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_PATH = os.getenv("LOG_PATH", "./logs/xobot.log")

Path(LOG_PATH).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_PATH),
    ],
)

logger = logging.getLogger(__name__)


BANNER = """
╔══════════════════════════════════════════════════════╗
║     XO Market BTC 5-Min Prediction Bot               ║
║     *** PAPER TRADING ONLY — NO REAL ORDERS ***      ║
╚══════════════════════════════════════════════════════╝
"""


def print_config_summary() -> None:
    print(BANNER)
    print("Configuration:")
    print(f"  LIVE_TRADING       = {os.getenv('LIVE_TRADING', 'false')} (always false)")
    print(f"  BINANCE_WS_URL     = {os.getenv('BINANCE_WS_URL', 'wss://fstream.binance.com/stream')}")
    print(f"  BINANCE_SYMBOL     = {os.getenv('BINANCE_SYMBOL', 'btcusdt')}")
    print(f"  XO_WS_URL          = {os.getenv('XO_WS_URL', 'wss://api.xo.market/ws')}")
    print(f"  XO_MARKET_ID       = {os.getenv('XO_MARKET_ID', 'BTC-5M-UP')}")
    print(f"  DB_PATH            = {os.getenv('DB_PATH', './data/xobot.duckdb')}")
    print(f"  DASHBOARD_PORT     = {os.getenv('DASHBOARD_PORT', '8000')}")
    print(f"  MOMENTUM_THRESHOLD = {os.getenv('MOMENTUM_THRESHOLD', '0.1')}")
    print(f"  IMBALANCE_THRESHOLD= {os.getenv('IMBALANCE_THRESHOLD', '20.0')}")
    print(f"  POSITION_SIZE      = {os.getenv('PAPER_POSITION_SIZE', '100')}")
    print(f"  HOLD_TIME_S        = {os.getenv('PAPER_HOLD_TIME_SECONDS', '300')}")
    print(f"  STOP_LOSS_PCT      = {os.getenv('PAPER_STOP_LOSS_PCT', '5.0')}")
    print()


class XOBot:
    """Orchestrates all bot components based on mode."""

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self._tasks: List[asyncio.Task] = []
        self._shutdown = asyncio.Event()

    async def run(self) -> None:
        from src.database import get_db
        from src.indicators import IndicatorEngine
        from src.imbalance import ImbalanceEngine
        from src.strategy import StrategyEngine
        from src.paper_engine import PaperEngine
        from src.collector import Collector
        import src.dashboard as dashboard

        db = get_db()
        db.initialize()
        await db.start()

        indicator_engine = IndicatorEngine()
        imbalance_engine = ImbalanceEngine()
        strategy_engine = StrategyEngine()
        paper_engine = PaperEngine(db)
        collector = Collector(db)

        dashboard.set_state_sources(
            collector=collector,
            paper_engine=paper_engine,
            db=db,
            indicator_engine=indicator_engine,
            imbalance_engine=imbalance_engine,
            strategy_engine=strategy_engine,
        )

        market_id = os.getenv("XO_MARKET_ID", "BTC-5M-UP")

        if self.mode == "collect":
            await collector.start()
            logger.info("Collector mode started. Press Ctrl+C to stop.")
            await self._shutdown.wait()
            await collector.stop()

        elif self.mode == "dashboard":
            logger.info("Dashboard-only mode. Serving on port %s", os.getenv("DASHBOARD_PORT", "8000"))
            await dashboard.start_server()

        elif self.mode in ("live", "collect+dashboard"):
            await paper_engine.start()
            await collector.start()

            # Subscribe to events for strategy loop
            btc_q = collector.event_bus.subscribe("btc_tick")
            xo_q = collector.event_bus.subscribe("xo_quote")
            imb_q = collector.event_bus.subscribe("xo_imbalance")

            strategy_task = asyncio.create_task(
                self._strategy_loop(
                    btc_q=btc_q,
                    xo_q=xo_q,
                    imb_q=imb_q,
                    db=db,
                    indicator_engine=indicator_engine,
                    imbalance_engine=imbalance_engine,
                    strategy_engine=strategy_engine,
                    paper_engine=paper_engine,
                    market_id=market_id,
                    dashboard_module=dashboard,
                ),
                name="strategy_loop",
            )

            dashboard_task = asyncio.create_task(
                dashboard.start_server(), name="dashboard"
            )

            logger.info("Live mode started. Dashboard at http://%s:%s",
                        os.getenv("DASHBOARD_HOST", "0.0.0.0"),
                        os.getenv("DASHBOARD_PORT", "8000"))

            await self._shutdown.wait()

            strategy_task.cancel()
            dashboard_task.cancel()
            await asyncio.gather(strategy_task, dashboard_task, return_exceptions=True)
            await collector.stop()
            await paper_engine.stop()

        elif self.mode == "replay":
            from src.replay import run_replay
            await run_replay(db=db)

        await db.stop()

    async def _strategy_loop(
        self,
        btc_q: asyncio.Queue,
        xo_q: asyncio.Queue,
        imb_q: asyncio.Queue,
        db,
        indicator_engine,
        imbalance_engine,
        strategy_engine,
        paper_engine,
        market_id: str,
        dashboard_module,
    ) -> None:
        """Main loop: consume BTC ticks, compute indicators, evaluate strategy."""
        from src.strategy import SignalType
        from src.xo_ws import XoQuote

        last_xo: Optional[XoQuote] = None
        last_latencies: dict = {}

        while True:
            try:
                # Drain XO queue
                while not xo_q.empty():
                    last_xo = await xo_q.get()
                    dashboard_module.update_state("xo", {
                        "yes_price": last_xo.yes_price,
                        "no_price": last_xo.no_price,
                        "spread": last_xo.spread,
                        "volume": last_xo.volume,
                        "status": last_xo.status,
                    })

                # Drain imbalance queue
                while not imb_q.empty():
                    imb_snap = await imb_q.get()
                    imbalance_engine._yes_bids  # already updated in xo_ws

                # Wait for BTC tick
                try:
                    tick = await asyncio.wait_for(btc_q.get(), timeout=0.1)
                except asyncio.TimeoutError:
                    continue

                # Update indicators
                ind_snap = indicator_engine.update(
                    timestamp_ms=tick.timestamp_ms,
                    price=tick.price,
                    bid=tick.bid,
                    ask=tick.ask,
                    spread=tick.spread,
                    volume=tick.volume,
                    momentum_1s=tick.momentum_1s,
                    momentum_3s=tick.momentum_3s,
                    momentum_5s=tick.momentum_5s,
                    momentum_15s=tick.momentum_15s,
                    momentum_30s=tick.momentum_30s,
                )

                # Update dashboard BTC state
                dashboard_module.update_state("btc", {
                    "price": tick.price,
                    "bid": tick.bid,
                    "ask": tick.ask,
                    "spread": tick.spread,
                    "momentum_1s": tick.momentum_1s,
                    "momentum_3s": tick.momentum_3s,
                    "momentum_5s": tick.momentum_5s,
                    "momentum_15s": tick.momentum_15s,
                    "momentum_30s": tick.momentum_30s,
                })

                dashboard_module.update_state("indicators", {
                    "rsi_14": ind_snap.rsi_14,
                    "vwap": ind_snap.vwap,
                    "vwap_deviation_pct": ind_snap.vwap_deviation_pct,
                    "volume_spike": ind_snap.volume_spike,
                    "spread_pct": ind_snap.spread_pct,
                    "change_1m_pct": ind_snap.change_1m_pct,
                    "change_5m_pct": ind_snap.change_5m_pct,
                    "momentum_score": ind_snap.momentum_score,
                    "volatility_score": ind_snap.volatility_score,
                })

                # Get imbalance
                imb_snap = imbalance_engine.snapshot(tick.timestamp_ms)

                # Strategy evaluation
                yes_price = last_xo.yes_price if last_xo else 0.5
                no_price = last_xo.no_price if last_xo else 0.5
                xo_spread = last_xo.spread if last_xo else 0.0
                strategy_mode = os.getenv("STRATEGY_MODE", "4condition").lower()

                if strategy_mode == "momentum_always":
                    signal = strategy_engine.evaluate_momentum_always(
                        timestamp_ms=tick.timestamp_ms,
                        btc_momentum_5s=tick.momentum_5s,
                        btc_momentum_30s=tick.momentum_30s,
                        yes_price=yes_price,
                        no_price=no_price,
                        market_id=market_id,
                    )
                else:
                    signal = strategy_engine.evaluate(
                        timestamp_ms=tick.timestamp_ms,
                        btc_momentum_5s=tick.momentum_5s,
                        imbalance_score=imb_snap.net_imbalance,
                        volume_spike=ind_snap.volume_spike,
                        current_spread=xo_spread,
                        yes_price=yes_price,
                        market_id=market_id,
                    )

                # Update signal state
                dashboard_module.update_state("signal", {
                    "signal_type": signal.signal_type.value,
                    "confidence": signal.confidence,
                    "momentum_condition": signal.momentum_condition,
                    "imbalance_condition": signal.imbalance_condition,
                    "volume_condition": signal.volume_condition,
                    "spread_condition": signal.spread_condition,
                })

                # Execute paper trade on signal
                if signal.signal_type != SignalType.NO_TRADE:
                    sig_id = await db.insert_signal(
                        timestamp_ms=tick.timestamp_ms,
                        signal_type=signal.signal_type.value,
                        confidence=signal.confidence,
                        btc_momentum=tick.momentum_5s,
                        imbalance_score=imb_snap.net_imbalance,
                        spread_compression=signal.spread_compression,
                        volume_spike=signal.volume_spike,
                        entry_price=signal.entry_price,
                        market_id=market_id,
                    )
                    side = "YES" if signal.signal_type == SignalType.LONG_YES else "NO"
                    entry_price = yes_price if side == "YES" else no_price
                    await paper_engine.enter_trade(
                        signal_id=sig_id,
                        market_id=market_id,
                        side=side,
                        entry_price=entry_price,
                    )

                # Check stop losses
                if last_xo:
                    await paper_engine.update_prices(market_id, yes_price, no_price)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Strategy loop error: %s", exc, exc_info=True)

    def trigger_shutdown(self) -> None:
        self._shutdown.set()


def main() -> None:
    parser = argparse.ArgumentParser(description="XO Market BTC Prediction Bot")
    parser.add_argument(
        "--mode",
        choices=["live", "replay", "dashboard", "collect"],
        default="live",
        help="Operating mode",
    )
    args = parser.parse_args()

    print_config_summary()
    logger.info("Starting in mode: %s", args.mode)

    if args.mode == "replay":
        from src.replay import main as replay_main
        replay_main()
        return

    bot = XOBot(mode=args.mode)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def _shutdown_handler(sig, frame):
        logger.info("Received signal %s — shutting down...", sig)
        loop.call_soon_threadsafe(bot.trigger_shutdown)

    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    try:
        loop.run_until_complete(bot.run())
    finally:
        loop.close()
        logger.info("Bot stopped cleanly")


if __name__ == "__main__":
    main()
