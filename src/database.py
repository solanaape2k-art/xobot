"""
DuckDB database layer for XO Market trading bot.
Thread-safe connection pool, async write queue, sequence IDs, deduplication.
"""

from __future__ import annotations

import asyncio
import logging
import os
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb

logger = logging.getLogger(__name__)

# Thread-local storage for connections
_thread_local = threading.local()

_CREATE_SEQUENCES = """
CREATE SEQUENCE IF NOT EXISTS seq_btc_ticks;
CREATE SEQUENCE IF NOT EXISTS seq_xo_quotes;
CREATE SEQUENCE IF NOT EXISTS seq_xo_orderbooks;
CREATE SEQUENCE IF NOT EXISTS seq_xo_trades;
CREATE SEQUENCE IF NOT EXISTS seq_signals;
CREATE SEQUENCE IF NOT EXISTS seq_paper_trades;
CREATE SEQUENCE IF NOT EXISTS seq_latency_metrics;
"""

_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS btc_ticks (
    id BIGINT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    price DOUBLE,
    bid DOUBLE,
    ask DOUBLE,
    spread DOUBLE,
    volume DOUBLE,
    trade_side VARCHAR,
    momentum_1s DOUBLE,
    momentum_3s DOUBLE,
    momentum_5s DOUBLE,
    momentum_15s DOUBLE,
    momentum_30s DOUBLE
);

CREATE TABLE IF NOT EXISTS xo_quotes (
    id BIGINT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    market_id VARCHAR NOT NULL,
    yes_price DOUBLE,
    no_price DOUBLE,
    spread DOUBLE,
    volume DOUBLE,
    status VARCHAR
);

CREATE TABLE IF NOT EXISTS xo_orderbooks (
    id BIGINT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    market_id VARCHAR NOT NULL,
    side VARCHAR,
    price DOUBLE,
    size DOUBLE
);

CREATE TABLE IF NOT EXISTS xo_trades (
    id BIGINT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    market_id VARCHAR NOT NULL,
    side VARCHAR,
    price DOUBLE,
    size DOUBLE
);

CREATE TABLE IF NOT EXISTS signals (
    id BIGINT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    signal_type VARCHAR,
    confidence DOUBLE,
    btc_momentum DOUBLE,
    imbalance_score DOUBLE,
    spread_compression DOUBLE,
    volume_spike BOOL,
    entry_price DOUBLE,
    market_id VARCHAR
);

CREATE TABLE IF NOT EXISTS paper_trades (
    id BIGINT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    market_id VARCHAR NOT NULL,
    side VARCHAR,
    entry_price DOUBLE,
    exit_price DOUBLE,
    size DOUBLE,
    pnl DOUBLE,
    hold_time_ms BIGINT,
    status VARCHAR,
    signal_id BIGINT
);

CREATE TABLE IF NOT EXISTS latency_metrics (
    id BIGINT PRIMARY KEY,
    timestamp_ms BIGINT NOT NULL,
    source VARCHAR NOT NULL,
    latency_ms DOUBLE
);
"""

_CREATE_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_btc_ticks_ts ON btc_ticks(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_xo_quotes_ts ON xo_quotes(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_xo_quotes_market ON xo_quotes(market_id);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_paper_trades_ts ON paper_trades(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_latency_ts ON latency_metrics(timestamp_ms);
"""


class Database:
    """
    Thread-safe DuckDB database manager with async write queue.
    All writes serialized through an asyncio.Queue processed by a background task.
    Reads use per-thread read connections.
    """

    def __init__(self, db_path: str = "./data/xobot.duckdb") -> None:
        self.db_path = str(Path(db_path).resolve())
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._write_conn: Optional[duckdb.DuckDBPyConnection] = None
        self._write_lock = threading.Lock()
        self._write_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self._shutdown = False
        self._write_task: Optional[asyncio.Task] = None
        self._initialized = False

    def _get_write_conn(self) -> duckdb.DuckDBPyConnection:
        if self._write_conn is None:
            self._write_conn = duckdb.connect(self.db_path)
        return self._write_conn

    def _get_read_conn(self) -> duckdb.DuckDBPyConnection:
        """Return the single write connection for reads (DuckDB is single-writer).
        Protected by _write_lock when called from query()."""
        return self._get_write_conn()

    def initialize(self) -> None:
        """Create tables and sequences. Must be called before starting async loop."""
        conn = self._get_write_conn()
        conn.execute(_CREATE_SEQUENCES)
        conn.execute(_CREATE_TABLES)
        conn.execute(_CREATE_INDEXES)
        conn.commit()
        self._initialized = True
        logger.info("Database initialized at %s", self.db_path)

    async def start(self) -> None:
        """Start the background write worker."""
        if not self._initialized:
            self.initialize()
        self._write_task = asyncio.create_task(self._write_worker())
        logger.info("Database write worker started")

    async def stop(self) -> None:
        """Drain queue and stop write worker."""
        self._shutdown = True
        await self._write_queue.join()
        if self._write_task:
            self._write_task.cancel()
            try:
                await self._write_task
            except asyncio.CancelledError:
                pass
        if self._write_conn:
            self._write_conn.close()
            self._write_conn = None
        logger.info("Database stopped")

    async def _write_worker(self) -> None:
        """Process write queue in the background."""
        while not self._shutdown:
            try:
                item = await asyncio.wait_for(self._write_queue.get(), timeout=0.1)
                sql, params = item
                try:
                    with self._write_lock:
                        conn = self._get_write_conn()
                        if params:
                            conn.execute(sql, params)
                        else:
                            conn.execute(sql)
                        conn.commit()
                except Exception as exc:
                    logger.error("DB write error: %s | SQL: %s", exc, sql[:100])
                finally:
                    self._write_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Write worker error: %s", exc)

    async def _enqueue(self, sql: str, params: Optional[List] = None) -> None:
        try:
            self._write_queue.put_nowait((sql, params or []))
        except asyncio.QueueFull:
            logger.warning("Write queue full — dropping record")

    # -------------------------------------------------------------------------
    # Insert methods
    # -------------------------------------------------------------------------

    async def insert_btc_tick(
        self,
        timestamp_ms: int,
        price: float,
        bid: float,
        ask: float,
        spread: float,
        volume: float,
        trade_side: str,
        momentum_1s: float,
        momentum_3s: float,
        momentum_5s: float,
        momentum_15s: float,
        momentum_30s: float,
    ) -> None:
        sql = """
        INSERT OR IGNORE INTO btc_ticks
            (id, timestamp_ms, price, bid, ask, spread, volume, trade_side,
             momentum_1s, momentum_3s, momentum_5s, momentum_15s, momentum_30s)
        VALUES
            (nextval('seq_btc_ticks'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        await self._enqueue(
            sql,
            [
                timestamp_ms, price, bid, ask, spread, volume, trade_side,
                momentum_1s, momentum_3s, momentum_5s, momentum_15s, momentum_30s,
            ],
        )

    async def insert_xo_quote(
        self,
        timestamp_ms: int,
        market_id: str,
        yes_price: float,
        no_price: float,
        spread: float,
        volume: float,
        status: str,
    ) -> None:
        sql = """
        INSERT INTO xo_quotes
            (id, timestamp_ms, market_id, yes_price, no_price, spread, volume, status)
        VALUES
            (nextval('seq_xo_quotes'), ?, ?, ?, ?, ?, ?, ?)
        """
        await self._enqueue(sql, [timestamp_ms, market_id, yes_price, no_price, spread, volume, status])

    async def insert_xo_orderbook(
        self,
        timestamp_ms: int,
        market_id: str,
        side: str,
        price: float,
        size: float,
    ) -> None:
        sql = """
        INSERT INTO xo_orderbooks
            (id, timestamp_ms, market_id, side, price, size)
        VALUES
            (nextval('seq_xo_orderbooks'), ?, ?, ?, ?, ?)
        """
        await self._enqueue(sql, [timestamp_ms, market_id, side, price, size])

    async def insert_xo_trade(
        self,
        timestamp_ms: int,
        market_id: str,
        side: str,
        price: float,
        size: float,
    ) -> None:
        sql = """
        INSERT INTO xo_trades
            (id, timestamp_ms, market_id, side, price, size)
        VALUES
            (nextval('seq_xo_trades'), ?, ?, ?, ?, ?)
        """
        await self._enqueue(sql, [timestamp_ms, market_id, side, price, size])

    async def insert_signal(
        self,
        timestamp_ms: int,
        signal_type: str,
        confidence: float,
        btc_momentum: float,
        imbalance_score: float,
        spread_compression: float,
        volume_spike: bool,
        entry_price: float,
        market_id: str,
    ) -> int:
        """Insert signal and return its ID synchronously."""
        with self._write_lock:
            conn = self._get_write_conn()
            conn.execute(
                """
                INSERT INTO signals
                    (id, timestamp_ms, signal_type, confidence, btc_momentum,
                     imbalance_score, spread_compression, volume_spike, entry_price, market_id)
                VALUES
                    (nextval('seq_signals'), ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    timestamp_ms, signal_type, confidence, btc_momentum,
                    imbalance_score, spread_compression, volume_spike, entry_price, market_id,
                ],
            )
            row = conn.execute("SELECT currval('seq_signals')").fetchone()
            conn.commit()
            return row[0] if row else -1

    async def insert_paper_trade(
        self,
        timestamp_ms: int,
        market_id: str,
        side: str,
        entry_price: float,
        exit_price: float,
        size: float,
        pnl: float,
        hold_time_ms: int,
        status: str,
        signal_id: int,
    ) -> None:
        sql = """
        INSERT INTO paper_trades
            (id, timestamp_ms, market_id, side, entry_price, exit_price,
             size, pnl, hold_time_ms, status, signal_id)
        VALUES
            (nextval('seq_paper_trades'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        await self._enqueue(
            sql,
            [
                timestamp_ms, market_id, side, entry_price, exit_price,
                size, pnl, hold_time_ms, status, signal_id,
            ],
        )

    async def insert_latency(self, timestamp_ms: int, source: str, latency_ms: float) -> None:
        sql = """
        INSERT INTO latency_metrics (id, timestamp_ms, source, latency_ms)
        VALUES (nextval('seq_latency_metrics'), ?, ?, ?)
        """
        await self._enqueue(sql, [timestamp_ms, source, latency_ms])

    # -------------------------------------------------------------------------
    # Query methods
    # -------------------------------------------------------------------------

    def query(self, sql: str, params: Optional[List] = None) -> List[Tuple]:
        """Execute a read query. Uses the single connection, protected by lock."""
        try:
            with self._write_lock:
                conn = self._get_read_conn()
                if params:
                    result = conn.execute(sql, params).fetchall()
                else:
                    result = conn.execute(sql).fetchall()
            return result
        except Exception as exc:
            logger.error("DB query error: %s", exc)
            return []

    def get_recent_btc_ticks(self, limit: int = 100) -> List[Tuple]:
        return self.query(
            "SELECT * FROM btc_ticks ORDER BY timestamp_ms DESC LIMIT ?", [limit]
        )

    def get_recent_xo_quotes(self, market_id: str, limit: int = 100) -> List[Tuple]:
        return self.query(
            "SELECT * FROM xo_quotes WHERE market_id=? ORDER BY timestamp_ms DESC LIMIT ?",
            [market_id, limit],
        )

    def get_recent_paper_trades(self, limit: int = 100) -> List[Dict[str, Any]]:
        rows = self.query(
            """SELECT id, timestamp_ms, market_id, side, entry_price, exit_price,
                      size, pnl, hold_time_ms, status, signal_id
               FROM paper_trades ORDER BY timestamp_ms DESC LIMIT ?""",
            [limit],
        )
        cols = [
            "id", "timestamp_ms", "market_id", "side", "entry_price", "exit_price",
            "size", "pnl", "hold_time_ms", "status", "signal_id",
        ]
        return [dict(zip(cols, row)) for row in rows]

    def get_paper_trade_stats(self) -> Dict[str, Any]:
        rows = self.query(
            "SELECT pnl, side FROM paper_trades WHERE status='closed'"
        )
        if not rows:
            return {
                "total_pnl": 0.0,
                "trade_count": 0,
                "win_rate": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
            }
        pnls = [r[0] for r in rows]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        win_rate = len(wins) / len(pnls) * 100 if pnls else 0.0
        avg_win = sum(wins) / len(wins) if wins else 0.0
        avg_loss = sum(losses) / len(losses) if losses else 0.0
        gross_profit = sum(wins)
        gross_loss = abs(sum(losses))
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
        return {
            "total_pnl": round(sum(pnls), 4),
            "trade_count": len(pnls),
            "win_rate": round(win_rate, 2),
            "avg_win": round(avg_win, 4),
            "avg_loss": round(avg_loss, 4),
            "profit_factor": round(profit_factor, 4),
        }

    def get_recent_latency(self, limit: int = 50) -> List[Tuple]:
        return self.query(
            "SELECT source, latency_ms, timestamp_ms FROM latency_metrics ORDER BY timestamp_ms DESC LIMIT ?",
            [limit],
        )

    def get_btc_ticks_range(
        self,
        start_ms: int,
        end_ms: int,
        limit: int = 100000,
    ) -> List[Tuple]:
        return self.query(
            "SELECT * FROM btc_ticks WHERE timestamp_ms >= ? AND timestamp_ms <= ? ORDER BY timestamp_ms ASC LIMIT ?",
            [start_ms, end_ms, limit],
        )

    def get_xo_quotes_range(
        self,
        market_id: str,
        start_ms: int,
        end_ms: int,
        limit: int = 100000,
    ) -> List[Tuple]:
        return self.query(
            """SELECT * FROM xo_quotes
               WHERE market_id=? AND timestamp_ms >= ? AND timestamp_ms <= ?
               ORDER BY timestamp_ms ASC LIMIT ?""",
            [market_id, start_ms, end_ms, limit],
        )


# Singleton
_db_instance: Optional[Database] = None


def get_db() -> Database:
    global _db_instance
    if _db_instance is None:
        db_path = os.getenv("DB_PATH", "./data/xobot.duckdb")
        _db_instance = Database(db_path)
    return _db_instance
