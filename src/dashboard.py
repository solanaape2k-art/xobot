"""
FastAPI dashboard for XO Market BTC prediction bot.
Serves a single-page auto-refreshing dashboard and JSON API endpoints.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, Optional

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DASHBOARD_PORT = int(os.getenv("DASHBOARD_PORT", "8000"))
DASHBOARD_HOST = os.getenv("DASHBOARD_HOST", "0.0.0.0")

# Global state references (set by main.py)
_collector = None
_paper_engine = None
_db = None
_indicator_engine = None
_imbalance_engine = None
_strategy_engine = None

# Latest snapshot data
_state: Dict[str, Any] = {
    "btc": {},
    "xo": {},
    "indicators": {},
    "imbalance": {},
    "signals": [],
    "collector": {},
    "db_status": "unknown",
}


def set_state_sources(
    collector=None,
    paper_engine=None,
    db=None,
    indicator_engine=None,
    imbalance_engine=None,
    strategy_engine=None,
) -> None:
    global _collector, _paper_engine, _db, _indicator_engine, _imbalance_engine, _strategy_engine
    _collector = collector
    _paper_engine = paper_engine
    _db = db
    _indicator_engine = indicator_engine
    _imbalance_engine = imbalance_engine
    _strategy_engine = strategy_engine


def update_state(key: str, value: Any) -> None:
    _state[key] = value


def get_current_state() -> dict:
    state = dict(_state)

    # Enrich with live data
    if _collector:
        state["collector"] = _collector.health()

    if _paper_engine:
        state["stats"] = _paper_engine.get_stats().to_dict()
        state["open_positions"] = _paper_engine.get_open_positions()

    if _db:
        try:
            _db.query("SELECT 1")
            state["db_status"] = "connected"
        except Exception:
            state["db_status"] = "error"

    if _imbalance_engine:
        snap = _imbalance_engine.snapshot()
        state["imbalance"] = {
            "yes_bid_liquidity": snap.yes_bid_liquidity,
            "yes_ask_liquidity": snap.yes_ask_liquidity,
            "no_bid_liquidity": snap.no_bid_liquidity,
            "no_ask_liquidity": snap.no_ask_liquidity,
            "net_imbalance": snap.net_imbalance,
        }

    state["server_time_ms"] = int(time.time() * 1000)
    return state


# ─── FastAPI App ──────────────────────────────────────────────────────────────

app = FastAPI(title="XO Market BTC Bot Dashboard", version="1.0.0")


@app.get("/health")
async def health():
    status = "ok"
    if _collector:
        h = _collector.health()
        status = h.get("status", "ok")
    return {"status": status, "timestamp_ms": int(time.time() * 1000)}


@app.get("/api/state")
async def api_state():
    return JSONResponse(content=get_current_state())


@app.get("/api/trades")
async def api_trades():
    if _db:
        trades = _db.get_recent_paper_trades(limit=100)
        return JSONResponse(content={"trades": trades})
    return JSONResponse(content={"trades": []})


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return HTMLResponse(content=_DASHBOARD_HTML)


# ─── Dashboard HTML ───────────────────────────────────────────────────────────

_DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>XO Market BTC Bot Dashboard</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { background: #0d1117; color: #e6edf3; font-family: 'Courier New', monospace; font-size: 13px; }
    .header { background: #161b22; padding: 12px 20px; border-bottom: 1px solid #30363d; display: flex; align-items: center; justify-content: space-between; }
    .header h1 { font-size: 18px; color: #58a6ff; }
    .header .subtitle { color: #8b949e; font-size: 11px; }
    .paper-badge { background: #1f6feb; color: #fff; padding: 2px 8px; border-radius: 12px; font-size: 11px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 12px; padding: 16px; }
    .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px; }
    .card h3 { color: #8b949e; font-size: 11px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }
    .metric { display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px; }
    .metric .label { color: #8b949e; }
    .metric .value { font-weight: bold; color: #e6edf3; }
    .value.green { color: #3fb950; }
    .value.red { color: #f85149; }
    .value.yellow { color: #d29922; }
    .value.blue { color: #58a6ff; }
    .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 6px; }
    .dot.green { background: #3fb950; }
    .dot.red { background: #f85149; animation: blink 1s infinite; }
    .dot.yellow { background: #d29922; }
    .dot.gray { background: #8b949e; }
    @keyframes blink { 0%,100%{opacity:1} 50%{opacity:0.2} }
    .signal-box { padding: 10px; border-radius: 6px; text-align: center; font-size: 15px; font-weight: bold; margin-bottom: 8px; }
    .signal-long-yes { background: rgba(63,185,80,0.15); border: 1px solid #3fb950; color: #3fb950; animation: blink 0.8s infinite; }
    .signal-long-no { background: rgba(248,81,73,0.15); border: 1px solid #f85149; color: #f85149; animation: blink 0.8s infinite; }
    .signal-none { background: rgba(139,148,158,0.1); border: 1px solid #30363d; color: #8b949e; }
    .imbalance-bar { height: 12px; border-radius: 6px; background: #21262d; overflow: hidden; margin-top: 4px; position: relative; }
    .imbalance-fill { height: 100%; border-radius: 6px; transition: width 0.3s; }
    table { width: 100%; border-collapse: collapse; font-size: 12px; }
    th { color: #8b949e; text-align: left; padding: 4px 8px; border-bottom: 1px solid #21262d; }
    td { padding: 4px 8px; border-bottom: 1px solid #161b22; }
    tr:hover td { background: #21262d; }
    .pnl-pos { color: #3fb950; }
    .pnl-neg { color: #f85149; }
    .refresh-info { text-align: right; padding: 0 16px 8px; color: #8b949e; font-size: 11px; }
    .full-width { grid-column: 1 / -1; }
    .momentum-grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 6px; }
    .momentum-cell { text-align: center; padding: 6px; border-radius: 4px; background: #21262d; }
    .momentum-cell .window { font-size: 10px; color: #8b949e; }
    .momentum-cell .val { font-size: 13px; font-weight: bold; }
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h1>XO Market BTC Bot</h1>
      <div class="subtitle">BTC 5-Minute Prediction Market</div>
    </div>
    <span class="paper-badge">PAPER TRADING ONLY</span>
  </div>
  <div class="refresh-info">Auto-refresh: 2s &nbsp;|&nbsp; <span id="last-update">--</span></div>

  <div class="grid">

    <!-- BTC Price -->
    <div class="card">
      <h3>BTC Price</h3>
      <div class="metric"><span class="label">Price</span><span class="value blue" id="btc-price">--</span></div>
      <div class="metric"><span class="label">Bid</span><span class="value" id="btc-bid">--</span></div>
      <div class="metric"><span class="label">Ask</span><span class="value" id="btc-ask">--</span></div>
      <div class="metric"><span class="label">Spread</span><span class="value" id="btc-spread">--</span></div>
      <div class="metric"><span class="label">1m Change</span><span class="value" id="btc-1m">--</span></div>
      <div class="metric"><span class="label">5m Change</span><span class="value" id="btc-5m">--</span></div>
    </div>

    <!-- Momentum -->
    <div class="card">
      <h3>BTC Momentum (%)</h3>
      <div class="momentum-grid">
        <div class="momentum-cell"><div class="window">1s</div><div class="val" id="m1s">--</div></div>
        <div class="momentum-cell"><div class="window">3s</div><div class="val" id="m3s">--</div></div>
        <div class="momentum-cell"><div class="window">5s</div><div class="val" id="m5s">--</div></div>
        <div class="momentum-cell"><div class="window">15s</div><div class="val" id="m15s">--</div></div>
        <div class="momentum-cell"><div class="window">30s</div><div class="val" id="m30s">--</div></div>
      </div>
      <div class="metric" style="margin-top:10px"><span class="label">Momentum Score</span><span class="value" id="mom-score">--</span></div>
      <div class="metric"><span class="label">Volatility</span><span class="value" id="volatility">--</span></div>
    </div>

    <!-- XO Market -->
    <div class="card">
      <h3>XO Market</h3>
      <div class="metric"><span class="label">YES Price</span><span class="value green" id="yes-price">--</span></div>
      <div class="metric"><span class="label">NO Price</span><span class="value red" id="no-price">--</span></div>
      <div class="metric"><span class="label">Spread</span><span class="value" id="xo-spread">--</span></div>
      <div class="metric"><span class="label">Volume</span><span class="value" id="xo-volume">--</span></div>
      <div class="metric"><span class="label">Status</span><span class="value" id="xo-status">--</span></div>
    </div>

    <!-- Imbalance -->
    <div class="card">
      <h3>Orderbook Imbalance</h3>
      <div class="metric"><span class="label">Net Imbalance</span><span class="value" id="imbalance-val">--</span></div>
      <div class="imbalance-bar"><div class="imbalance-fill" id="imbalance-bar" style="width:50%;background:#8b949e;"></div></div>
      <div class="metric" style="margin-top:8px"><span class="label">YES Bid Liq</span><span class="value green" id="yes-bid-liq">--</span></div>
      <div class="metric"><span class="label">YES Ask Liq</span><span class="value" id="yes-ask-liq">--</span></div>
      <div class="metric"><span class="label">NO Bid Liq</span><span class="value" id="no-bid-liq">--</span></div>
      <div class="metric"><span class="label">NO Ask Liq</span><span class="value" id="no-ask-liq">--</span></div>
    </div>

    <!-- Indicators -->
    <div class="card">
      <h3>Indicators</h3>
      <div class="metric"><span class="label">RSI(14)</span><span class="value" id="rsi">--</span></div>
      <div class="metric"><span class="label">VWAP</span><span class="value" id="vwap">--</span></div>
      <div class="metric"><span class="label">VWAP Dev %</span><span class="value" id="vwap-dev">--</span></div>
      <div class="metric"><span class="label">Volume Spike</span><span class="value" id="vol-spike">--</span></div>
      <div class="metric"><span class="label">Spread %</span><span class="value" id="spread-pct">--</span></div>
    </div>

    <!-- Signal -->
    <div class="card">
      <h3>Current Signal</h3>
      <div class="signal-box signal-none" id="signal-box">NO TRADE</div>
      <div class="metric"><span class="label">Confidence</span><span class="value" id="signal-conf">--</span></div>
      <div class="metric"><span class="label">Momentum Cond</span><span class="value" id="cond-mom">--</span></div>
      <div class="metric"><span class="label">Imbalance Cond</span><span class="value" id="cond-imb">--</span></div>
      <div class="metric"><span class="label">Volume Cond</span><span class="value" id="cond-vol">--</span></div>
      <div class="metric"><span class="label">Spread Cond</span><span class="value" id="cond-spr">--</span></div>
    </div>

    <!-- Stats -->
    <div class="card">
      <h3>Paper Trade Stats</h3>
      <div class="metric"><span class="label">Total PnL</span><span class="value" id="total-pnl">--</span></div>
      <div class="metric"><span class="label">Win Rate</span><span class="value" id="win-rate">--</span></div>
      <div class="metric"><span class="label">Trade Count</span><span class="value" id="trade-count">--</span></div>
      <div class="metric"><span class="label">Profit Factor</span><span class="value" id="profit-factor">--</span></div>
      <div class="metric"><span class="label">Max Drawdown</span><span class="value" id="max-dd">--</span></div>
      <div class="metric"><span class="label">Sharpe Ratio</span><span class="value" id="sharpe">--</span></div>
    </div>

    <!-- Connection Health -->
    <div class="card">
      <h3>System Health</h3>
      <div class="metric"><span class="label"><span class="dot gray" id="dot-btc"></span>BTC WS</span><span class="value" id="health-btc">--</span></div>
      <div class="metric"><span class="label"><span class="dot gray" id="dot-xo"></span>XO WS</span><span class="value" id="health-xo">--</span></div>
      <div class="metric"><span class="label"><span class="dot gray" id="dot-db"></span>Database</span><span class="value" id="health-db">--</span></div>
      <div class="metric"><span class="label">BTC msg/s</span><span class="value" id="btc-mps">--</span></div>
      <div class="metric"><span class="label">XO msg/s</span><span class="value" id="xo-mps">--</span></div>
      <div class="metric"><span class="label">Uptime</span><span class="value" id="uptime">--</span></div>
    </div>

    <!-- Latency -->
    <div class="card">
      <h3>Latency</h3>
      <div class="metric"><span class="label">BTC WS Latency</span><span class="value" id="btc-latency">--</span></div>
      <div class="metric"><span class="label">XO WS Latency</span><span class="value" id="xo-latency">--</span></div>
    </div>

    <!-- Open Positions -->
    <div class="card">
      <h3>Open Positions</h3>
      <div id="open-positions">No open positions</div>
    </div>

    <!-- Recent Trades -->
    <div class="card full-width">
      <h3>Recent Paper Trades (last 20)</h3>
      <table>
        <thead><tr><th>ID</th><th>Time</th><th>Market</th><th>Side</th><th>Entry</th><th>Exit</th><th>PnL %</th><th>Hold</th><th>Status</th></tr></thead>
        <tbody id="trades-table"></tbody>
      </table>
    </div>

  </div>

  <script>
    function fmt(v, decimals=4) { return v !== undefined && v !== null ? Number(v).toFixed(decimals) : '--'; }
    function fmtColor(v, el) {
      const n = parseFloat(v);
      if (isNaN(n)) return;
      el.className = 'value ' + (n > 0 ? 'green' : n < 0 ? 'red' : '');
    }
    function fmtMs(ms) {
      if (!ms) return '--';
      return new Date(ms).toLocaleTimeString();
    }
    function fmtAge(ms) {
      const s = Math.round((Date.now() - ms) / 1000);
      if (s < 60) return s + 's ago';
      return Math.round(s/60) + 'm ago';
    }
    function setDot(id, ok) {
      const el = document.getElementById(id);
      if (!el) return;
      el.className = 'dot ' + (ok ? 'green' : 'red');
    }
    function condIcon(v) { return v ? '<span style="color:#3fb950">✓</span>' : '<span style="color:#f85149">✗</span>'; }

    async function refresh() {
      try {
        const r = await fetch('/api/state');
        const s = await r.json();

        document.getElementById('last-update').textContent = new Date().toLocaleTimeString();

        const btc = s.btc || {};
        document.getElementById('btc-price').textContent = btc.price ? '$' + fmt(btc.price, 2) : '--';
        document.getElementById('btc-bid').textContent = btc.bid ? '$' + fmt(btc.bid, 2) : '--';
        document.getElementById('btc-ask').textContent = btc.ask ? '$' + fmt(btc.ask, 2) : '--';
        document.getElementById('btc-spread').textContent = btc.spread ? '$' + fmt(btc.spread, 2) : '--';

        const ind = s.indicators || {};
        const c1m = document.getElementById('btc-1m');
        c1m.textContent = ind.change_1m_pct !== undefined ? fmt(ind.change_1m_pct, 3) + '%' : '--';
        fmtColor(ind.change_1m_pct, c1m);
        const c5m = document.getElementById('btc-5m');
        c5m.textContent = ind.change_5m_pct !== undefined ? fmt(ind.change_5m_pct, 3) + '%' : '--';
        fmtColor(ind.change_5m_pct, c5m);

        // Momentum
        const moms = btc.momentums || {};
        ['1s','3s','5s','15s','30s'].forEach(w => {
          const key = 'momentum_' + w.replace('s','') + 's';
          const el = document.getElementById('m' + w.replace('s','') + 's');
          if (!el) return;
          const v = btc[key];
          if (v !== undefined) {
            el.textContent = fmt(v, 4) + '%';
            el.className = 'val ' + (v > 0 ? 'green' : v < 0 ? 'red' : '');
          }
        });

        const msEl = document.getElementById('mom-score');
        if (ind.momentum_score !== undefined) {
          msEl.textContent = fmt(ind.momentum_score, 1);
          fmtColor(ind.momentum_score, msEl);
        }
        const volEl = document.getElementById('volatility');
        if (ind.volatility_score !== undefined) {
          volEl.textContent = fmt(ind.volatility_score, 2) + '%';
        }

        // XO
        const xo = s.xo || {};
        const yesEl = document.getElementById('yes-price');
        yesEl.textContent = xo.yes_price !== undefined ? fmt(xo.yes_price, 4) : '--';
        const noEl = document.getElementById('no-price');
        noEl.textContent = xo.no_price !== undefined ? fmt(xo.no_price, 4) : '--';
        document.getElementById('xo-spread').textContent = xo.spread !== undefined ? fmt(xo.spread, 4) : '--';
        document.getElementById('xo-volume').textContent = xo.volume !== undefined ? fmt(xo.volume, 0) : '--';
        document.getElementById('xo-status').textContent = xo.status || '--';

        // Imbalance
        const imb = s.imbalance || {};
        const imbVal = imb.net_imbalance;
        const imbEl = document.getElementById('imbalance-val');
        if (imbVal !== undefined) {
          imbEl.textContent = fmt(imbVal, 1);
          imbEl.className = 'value ' + (imbVal > 20 ? 'green' : imbVal < -20 ? 'red' : 'yellow');
          const pct = ((imbVal + 100) / 200 * 100).toFixed(1);
          const bar = document.getElementById('imbalance-bar');
          bar.style.width = pct + '%';
          bar.style.background = imbVal > 20 ? '#3fb950' : imbVal < -20 ? '#f85149' : '#d29922';
        }
        document.getElementById('yes-bid-liq').textContent = fmt(imb.yes_bid_liquidity, 0);
        document.getElementById('yes-ask-liq').textContent = fmt(imb.yes_ask_liquidity, 0);
        document.getElementById('no-bid-liq').textContent = fmt(imb.no_bid_liquidity, 0);
        document.getElementById('no-ask-liq').textContent = fmt(imb.no_ask_liquidity, 0);

        // Indicators
        const rsiEl = document.getElementById('rsi');
        if (ind.rsi_14 !== undefined) {
          rsiEl.textContent = fmt(ind.rsi_14, 1);
          rsiEl.className = 'value ' + (ind.rsi_14 > 70 ? 'red' : ind.rsi_14 < 30 ? 'green' : '');
        }
        document.getElementById('vwap').textContent = ind.vwap ? '$' + fmt(ind.vwap, 2) : '--';
        const vwapDevEl = document.getElementById('vwap-dev');
        if (ind.vwap_deviation_pct !== undefined) {
          vwapDevEl.textContent = fmt(ind.vwap_deviation_pct, 3) + '%';
          fmtColor(ind.vwap_deviation_pct, vwapDevEl);
        }
        const vsEl = document.getElementById('vol-spike');
        if (ind.volume_spike !== undefined) {
          vsEl.textContent = ind.volume_spike ? 'YES' : 'NO';
          vsEl.className = 'value ' + (ind.volume_spike ? 'green' : '');
        }
        document.getElementById('spread-pct').textContent = ind.spread_pct !== undefined ? fmt(ind.spread_pct, 4) + '%' : '--';

        // Signal
        const sig = s.signal || {};
        const sigBox = document.getElementById('signal-box');
        const sigType = sig.signal_type || 'NO_TRADE';
        sigBox.textContent = sigType;
        sigBox.className = 'signal-box ' + (
          sigType === 'LONG_YES' ? 'signal-long-yes' :
          sigType === 'LONG_NO' ? 'signal-long-no' : 'signal-none'
        );
        document.getElementById('signal-conf').textContent = sig.confidence !== undefined ? (sig.confidence * 100).toFixed(1) + '%' : '--';
        document.getElementById('cond-mom').innerHTML = sig.momentum_condition !== undefined ? condIcon(sig.momentum_condition) : '--';
        document.getElementById('cond-imb').innerHTML = sig.imbalance_condition !== undefined ? condIcon(sig.imbalance_condition) : '--';
        document.getElementById('cond-vol').innerHTML = sig.volume_condition !== undefined ? condIcon(sig.volume_condition) : '--';
        document.getElementById('cond-spr').innerHTML = sig.spread_condition !== undefined ? condIcon(sig.spread_condition) : '--';

        // Stats
        const stats = s.stats || {};
        const pnlEl = document.getElementById('total-pnl');
        if (stats.total_pnl !== undefined) {
          pnlEl.textContent = fmt(stats.total_pnl, 2) + '%';
          fmtColor(stats.total_pnl, pnlEl);
        }
        document.getElementById('win-rate').textContent = stats.win_rate !== undefined ? fmt(stats.win_rate, 1) + '%' : '--';
        document.getElementById('trade-count').textContent = stats.trade_count !== undefined ? stats.trade_count : '--';
        document.getElementById('profit-factor').textContent = stats.profit_factor !== undefined ? fmt(stats.profit_factor, 2) : '--';
        const ddEl = document.getElementById('max-dd');
        if (stats.max_drawdown !== undefined) {
          ddEl.textContent = fmt(stats.max_drawdown, 2) + '%';
          ddEl.className = 'value red';
        }
        document.getElementById('sharpe').textContent = stats.sharpe_ratio !== undefined ? fmt(stats.sharpe_ratio, 2) : '--';

        // Health
        const col = s.collector || {};
        setDot('dot-btc', col.btc_connected);
        document.getElementById('health-btc').textContent = col.btc_connected ? 'Connected' : 'Disconnected';
        document.getElementById('health-btc').className = 'value ' + (col.btc_connected ? 'green' : 'red');
        setDot('dot-xo', col.xo_connected);
        document.getElementById('health-xo').textContent = col.xo_connected ? 'Connected' : 'Disconnected';
        document.getElementById('health-xo').className = 'value ' + (col.xo_connected ? 'green' : 'red');
        setDot('dot-db', s.db_status === 'connected');
        document.getElementById('health-db').textContent = s.db_status || '--';
        document.getElementById('health-db').className = 'value ' + (s.db_status === 'connected' ? 'green' : 'red');
        document.getElementById('btc-mps').textContent = col.btc_msg_per_s !== undefined ? col.btc_msg_per_s.toFixed(1) : '--';
        document.getElementById('xo-mps').textContent = col.xo_msg_per_s !== undefined ? col.xo_msg_per_s.toFixed(1) : '--';
        document.getElementById('uptime').textContent = col.uptime_s !== undefined ? col.uptime_s + 's' : '--';

        // Latency
        const lat = s.latency || {};
        document.getElementById('btc-latency').textContent = lat.binance_aggTrade !== undefined ? lat.binance_aggTrade.toFixed(1) + 'ms' : '--';
        document.getElementById('xo-latency').textContent = lat.xo_quote !== undefined ? lat.xo_quote.toFixed(1) + 'ms' : '--';

        // Open positions
        const positions = s.open_positions || [];
        const posDiv = document.getElementById('open-positions');
        if (positions.length === 0) {
          posDiv.textContent = 'No open positions';
        } else {
          posDiv.innerHTML = positions.map(p =>
            `<div class="metric"><span class="label">${p.trade_id} ${p.side}</span><span class="value blue">Entry: ${fmt(p.entry_price,4)} | Age: ${p.age_s}s</span></div>`
          ).join('');
        }

      } catch (e) {
        console.error('Refresh error:', e);
      }

      // Trades table
      try {
        const r2 = await fetch('/api/trades');
        const td = await r2.json();
        const tbody = document.getElementById('trades-table');
        const trades = (td.trades || []).slice(0, 20);
        tbody.innerHTML = trades.map(t => {
          const pnlClass = t.pnl > 0 ? 'pnl-pos' : 'pnl-neg';
          return `<tr>
            <td>${t.id}</td>
            <td>${fmtMs(t.timestamp_ms)}</td>
            <td>${t.market_id}</td>
            <td>${t.side}</td>
            <td>${fmt(t.entry_price,4)}</td>
            <td>${fmt(t.exit_price,4)}</td>
            <td class="${pnlClass}">${fmt(t.pnl,2)}%</td>
            <td>${t.hold_time_ms ? (t.hold_time_ms/1000).toFixed(0)+'s' : '--'}</td>
            <td>${t.status}</td>
          </tr>`;
        }).join('');
      } catch(e) {}
    }

    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>"""


async def start_server() -> None:
    """Start the uvicorn server."""
    import uvicorn

    config = uvicorn.Config(
        app,
        host=DASHBOARD_HOST,
        port=DASHBOARD_PORT,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    await server.serve()
