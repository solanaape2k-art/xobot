# XO Market BTC 5-Minute Prediction Bot

**PAPER TRADING ONLY — No real orders are placed. No real money is involved.**

A data-driven prediction market bot that monitors Binance BTC perpetual futures and XO Market prediction market data to generate trading signals for the BTC-5M-UP market.

---

## Architecture

```
Binance WS (aggTrade + bookTicker)
        │
        ▼
   btc_ws.py  ──► momentum calculator ──► EventBus
        │                                     │
        ▼                                     ▼
   database.py                        strategy.py
   (DuckDB)                                  │
        ▲                                     ▼
        │                            paper_engine.py
   xo_ws.py  ──► imbalance.py ──►  indicators.py
        │                                     │
        ▼                                     ▼
   collector.py (orchestrator)        dashboard.py (FastAPI)
```

---

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Copy environment config
cp .env.example .env
# Edit .env — ensure LIVE_TRADING=false (always)

# 3. Run in live mode (collectors + strategy + dashboard)
python -m src.main --mode live

# 4. Open dashboard
open http://localhost:8000
```

---

## Modes

| Mode | Description |
|------|-------------|
| `live` | Collectors + strategy + paper engine + dashboard |
| `collect` | BTC + XO websocket collectors only |
| `dashboard` | Dashboard only (reads existing DB data) |
| `replay` | Replay historical data from DuckDB |

```bash
python -m src.main --mode live
python -m src.main --mode collect
python -m src.main --mode dashboard
python -m src.main --mode replay

# Replay with options
python -m src.replay --speed 10 --market-id BTC-5M-UP --start-date 2025-01-01
```

---

## Signal Logic

### LONG YES (BTC will close higher in 5 min)
All 4 conditions must be true:
1. BTC momentum (5s) > `MOMENTUM_THRESHOLD` (default: 0.1%)
2. XO imbalance score > `IMBALANCE_THRESHOLD` (default: 20)
3. Volume spike detected (current vol > 2x 20-period rolling mean)
4. Spread compressing (current spread < rolling mean × 0.9)

### LONG NO (BTC will close lower in 5 min)
All 4 conditions must be true (bearish direction):
1. BTC momentum (5s) < `-MOMENTUM_THRESHOLD`
2. XO imbalance score < `-IMBALANCE_THRESHOLD`
3. Volume spike detected
4. Spread compressing

### Exit Conditions
- Hold time exceeded (`PAPER_HOLD_TIME_SECONDS`, default: 300s)
- Stop loss hit (`PAPER_STOP_LOSS_PCT`, default: 5%)

---

## Configuration (`.env`)

```
LIVE_TRADING=false          # NEVER set to true

BINANCE_WS_URL=wss://fstream.binance.com/stream
BINANCE_SYMBOL=btcusdt
XO_WS_URL=wss://api.xo.market/ws
XO_MARKET_ID=BTC-5M-UP

DB_PATH=./data/xobot.duckdb
DASHBOARD_PORT=8000

MOMENTUM_THRESHOLD=0.1
IMBALANCE_THRESHOLD=20.0
SPREAD_COMPRESSION_RATIO=0.9

PAPER_POSITION_SIZE=100
PAPER_HOLD_TIME_SECONDS=300
PAPER_STOP_LOSS_PCT=5.0

LOG_LEVEL=INFO
LOG_PATH=./logs/xobot.log
```

---

## Modules

| Module | Description |
|--------|-------------|
| `src/btc_ws.py` | Binance WS — aggTrade + bookTicker, momentum calculator |
| `src/xo_ws.py` | XO Market WS — quotes, trades, orderbook |
| `src/collector.py` | Orchestrator — event bus, health check, stats |
| `src/database.py` | DuckDB — thread-safe, async write queue |
| `src/indicators.py` | RSI, VWAP, momentum score, volatility |
| `src/imbalance.py` | XO orderbook imbalance (-100 to +100) |
| `src/strategy.py` | Signal generation (PAPER ONLY) |
| `src/paper_engine.py` | Virtual trade tracking, PnL, stats |
| `src/dashboard.py` | FastAPI dashboard + REST API |
| `src/replay.py` | Historical replay engine |
| `src/main.py` | Entry point, CLI, graceful shutdown |

---

## Running Tests

```bash
# Run all tests
python -m pytest tests/ -v

# Individual test files
python -m pytest tests/test_safety.py -v
python -m pytest tests/test_indicators.py -v
python -m pytest tests/test_strategy.py -v
python -m pytest tests/test_paper_engine.py -v
python -m pytest tests/test_database.py -v
python -m pytest tests/test_reconnect.py -v
```

---

## Dashboard API

| Endpoint | Description |
|----------|-------------|
| `GET /` | Dashboard HTML (auto-refresh 2s) |
| `GET /api/state` | Current bot state JSON |
| `GET /api/trades` | Last 100 paper trades |
| `GET /health` | Health check |

---

## Database Tables

- `btc_ticks` — BTC price, bid/ask, momentum windows
- `xo_quotes` — YES/NO prices, volume, status
- `xo_orderbooks` — Orderbook snapshots
- `xo_trades` — XO market trades
- `signals` — Generated signals with component scores
- `paper_trades` — Virtual trades with PnL
- `latency_metrics` — WS latency tracking

---

## Notes on XO Market API

The XO Market websocket API format is not publicly documented at time of writing.
The message parsing in `src/xo_ws.py` uses a reasonable predicted format.
Update `_handle_message()` and related methods in `src/xo_ws.py` once official
XO Market API documentation becomes available.

The bot runs fully with Binance data even when XO Market WS is unavailable —
it logs a warning and keeps retrying in the background.

---

## Safety

- `LIVE_TRADING=true` raises `RuntimeError` at import in `strategy.py` and `paper_engine.py`
- `LIVE_TRADING=true` calls `sys.exit(1)` in `main.py`
- No HTTP trading endpoints exist anywhere in the codebase
- All trades are simulated with virtual prices and virtual position sizes
