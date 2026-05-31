"""
Historical data replay engine.
Loads btc_ticks and xo_quotes from DuckDB and feeds through the full pipeline.
Supports playback speeds: 1x, 5x, 10x, 50x, 100x.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import time
from datetime import datetime
from typing import List, Optional, Tuple

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def parse_date(s: str) -> int:
    """Parse date string YYYY-MM-DD to milliseconds."""
    dt = datetime.strptime(s, "%Y-%m-%d")
    return int(dt.timestamp() * 1000)


async def run_replay(
    db,
    speed: float = 1.0,
    market_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    Replay historical data through the full pipeline.
    Returns final stats dict.
    """
    from src.indicators import IndicatorEngine
    from src.imbalance import ImbalanceEngine
    from src.strategy import StrategyEngine
    from src.paper_engine import PaperEngine

    market_id = market_id or os.getenv("XO_MARKET_ID", "BTC-5M-UP")
    start_ms = parse_date(start_date) if start_date else 0
    end_ms = parse_date(end_date) if end_date else int(time.time() * 1000)

    logger.info(
        "Replay: market=%s | speed=%.0fx | %s -> %s",
        market_id,
        speed,
        start_date or "all",
        end_date or "now",
    )

    # Load data
    btc_ticks = db.get_btc_ticks_range(start_ms, end_ms)
    xo_quotes = db.get_xo_quotes_range(market_id, start_ms, end_ms)

    if not btc_ticks:
        logger.warning("No BTC ticks found for replay range")
        return {"error": "no_data"}

    logger.info(
        "Loaded %d BTC ticks, %d XO quotes",
        len(btc_ticks), len(xo_quotes),
    )

    # Build engines
    indicator_engine = IndicatorEngine()
    imbalance_engine = ImbalanceEngine()
    strategy_engine = StrategyEngine()
    paper_engine = PaperEngine(db)
    await paper_engine.start()

    # Build XO quote lookup: {timestamp_ms -> xo_row}
    xo_by_ts = {}
    for row in xo_quotes:
        # row: (id, timestamp_ms, market_id, yes_price, no_price, spread, volume, status)
        xo_by_ts[row[1]] = row

    # Find the closest XO quote for a given BTC tick ts
    sorted_xo_ts = sorted(xo_by_ts.keys())
    xo_idx = 0

    def get_closest_xo(ts_ms: int):
        nonlocal xo_idx
        if not sorted_xo_ts:
            return None
        # Advance pointer
        while xo_idx < len(sorted_xo_ts) - 1 and sorted_xo_ts[xo_idx + 1] <= ts_ms:
            xo_idx += 1
        return xo_by_ts.get(sorted_xo_ts[xo_idx]) if sorted_xo_ts else None

    # Replay loop
    prev_wall_time: Optional[float] = None
    prev_data_time: Optional[int] = None
    processed = 0

    for row in btc_ticks:
        # row: (id, timestamp_ms, price, bid, ask, spread, volume, trade_side,
        #        momentum_1s, momentum_3s, momentum_5s, momentum_15s, momentum_30s)
        (
            _id, timestamp_ms, price, bid, ask, spread, volume, trade_side,
            momentum_1s, momentum_3s, momentum_5s, momentum_15s, momentum_30s,
        ) = row

        # Timing simulation
        if prev_wall_time is not None and prev_data_time is not None:
            data_delta_ms = timestamp_ms - prev_data_time
            wall_delta_s = data_delta_ms / 1000 / speed
            if wall_delta_s > 0:
                await asyncio.sleep(wall_delta_s)

        prev_wall_time = time.time()
        prev_data_time = timestamp_ms

        # Indicators
        snapshot = indicator_engine.update(
            timestamp_ms=timestamp_ms,
            price=price,
            bid=bid,
            ask=ask,
            spread=spread,
            volume=volume,
            momentum_1s=momentum_1s,
            momentum_3s=momentum_3s,
            momentum_5s=momentum_5s,
            momentum_15s=momentum_15s,
            momentum_30s=momentum_30s,
        )

        # Get XO data
        xo_row = get_closest_xo(timestamp_ms)
        yes_price = 0.5
        no_price = 0.5
        xo_spread = 0.0
        xo_status = "unknown"
        if xo_row is not None:
            _, _, _, yes_price, no_price, xo_spread, xo_volume, xo_status = xo_row

        # Update paper engine with current prices (stop loss checks)
        await paper_engine.update_prices(market_id, yes_price, no_price)

        # Strategy
        imb_snap = imbalance_engine.snapshot(timestamp_ms)
        signal = strategy_engine.evaluate(
            timestamp_ms=timestamp_ms,
            btc_momentum_5s=momentum_5s,
            imbalance_score=imb_snap.net_imbalance,
            volume_spike=snapshot.volume_spike,
            current_spread=xo_spread,
            yes_price=yes_price,
            market_id=market_id,
        )

        from src.strategy import SignalType
        if signal.signal_type != SignalType.NO_TRADE:
            sig_id = await db.insert_signal(
                timestamp_ms=timestamp_ms,
                signal_type=signal.signal_type.value,
                confidence=signal.confidence,
                btc_momentum=momentum_5s,
                imbalance_score=imb_snap.net_imbalance,
                spread_compression=signal.spread_compression,
                volume_spike=signal.volume_spike,
                entry_price=yes_price if signal.signal_type == SignalType.LONG_YES else no_price,
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

        processed += 1
        if processed % 1000 == 0:
            logger.info("Replayed %d ticks...", processed)

    await paper_engine.stop()

    # Final stats
    stats = paper_engine.get_stats()
    result = stats.to_dict()
    result["ticks_processed"] = processed
    result["market_id"] = market_id

    print("\n" + "=" * 50)
    print("REPLAY COMPLETE")
    print("=" * 50)
    for k, v in result.items():
        print(f"  {k:25s}: {v}")
    print("=" * 50)

    return result


def main() -> None:
    """CLI entry point for replay."""
    parser = argparse.ArgumentParser(description="XO Market BTC Bot — Replay Engine")
    parser.add_argument("--speed", type=float, default=10.0, help="Replay speed multiplier (default: 10)")
    parser.add_argument("--market-id", type=str, default=None, help="XO market ID")
    parser.add_argument("--start-date", type=str, default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end-date", type=str, default=None, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    from src.database import get_db
    db = get_db()
    db.initialize()

    asyncio.run(
        run_replay(
            db=db,
            speed=args.speed,
            market_id=args.market_id,
            start_date=args.start_date,
            end_date=args.end_date,
        )
    )


if __name__ == "__main__":
    main()
