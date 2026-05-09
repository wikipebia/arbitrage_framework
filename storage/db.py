"""
SQLite storage for logging arbitrage opportunities and lag events.
"""

import aiosqlite
import logging
import time

log = logging.getLogger("arbitrage.db")

CREATE_OPPORTUNITIES = """
CREATE TABLE IF NOT EXISTS opportunities (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL    NOT NULL,
    symbol      TEXT    NOT NULL,
    buy_exchange  TEXT  NOT NULL,
    sell_exchange TEXT  NOT NULL,
    buy_price   REAL    NOT NULL,
    sell_price  REAL    NOT NULL,
    spread_pct  REAL    NOT NULL,
    net_profit_pct REAL NOT NULL,
    volume_24h  REAL,
    buy_fee_pct REAL,
    sell_fee_pct REAL,
    withdrawal_fee REAL,
    network_fee REAL,
    position_usd REAL,
    estimated_profit_usd REAL,
    direction   TEXT,
    status      TEXT    DEFAULT 'detected'
)
"""

CREATE_LAG_EVENTS = """
CREATE TABLE IF NOT EXISTS lag_events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL    NOT NULL,
    symbol      TEXT    NOT NULL,
    exchange    TEXT    NOT NULL,
    lag_seconds REAL    NOT NULL,
    ref_price   REAL,
    exchange_price REAL,
    price_move_pct REAL,
    direction   TEXT
)
"""

CREATE_SCAN_STATS = """
CREATE TABLE IF NOT EXISTS scan_stats (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp   REAL    NOT NULL,
    scan_type   TEXT    NOT NULL,
    symbols_scanned INTEGER,
    opportunities_found INTEGER,
    duration_sec REAL,
    errors      INTEGER DEFAULT 0
)
"""


class ArbitrageDB:
    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        self._conn = await aiosqlite.connect(self._db_path)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.execute(CREATE_OPPORTUNITIES)
        await self._conn.execute(CREATE_LAG_EVENTS)
        await self._conn.execute(CREATE_SCAN_STATS)
        await self._conn.commit()
        log.info("Database connected: %s", self._db_path)

    async def close(self) -> None:
        if self._conn:
            await self._conn.close()
            self._conn = None

    async def log_opportunity(
        self,
        symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        buy_price: float,
        sell_price: float,
        spread_pct: float,
        net_profit_pct: float,
        volume_24h: float = 0.0,
        buy_fee_pct: float = 0.0,
        sell_fee_pct: float = 0.0,
        withdrawal_fee: float = 0.0,
        network_fee: float = 0.0,
        position_usd: float = 0.0,
        estimated_profit_usd: float = 0.0,
        direction: str = "",
    ) -> None:
        if not self._conn:
            return
        await self._conn.execute(
            """INSERT INTO opportunities
               (timestamp, symbol, buy_exchange, sell_exchange, buy_price,
                sell_price, spread_pct, net_profit_pct, volume_24h,
                buy_fee_pct, sell_fee_pct, withdrawal_fee, network_fee,
                position_usd, estimated_profit_usd, direction)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                time.time(), symbol, buy_exchange, sell_exchange,
                buy_price, sell_price, spread_pct, net_profit_pct,
                volume_24h, buy_fee_pct, sell_fee_pct, withdrawal_fee,
                network_fee, position_usd, estimated_profit_usd, direction,
            ),
        )
        await self._conn.commit()

    async def log_lag_event(
        self,
        symbol: str,
        exchange: str,
        lag_seconds: float,
        ref_price: float = 0.0,
        exchange_price: float = 0.0,
        price_move_pct: float = 0.0,
        direction: str = "",
    ) -> None:
        if not self._conn:
            return
        await self._conn.execute(
            """INSERT INTO lag_events
               (timestamp, symbol, exchange, lag_seconds, ref_price,
                exchange_price, price_move_pct, direction)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                time.time(), symbol, exchange, lag_seconds,
                ref_price, exchange_price, price_move_pct, direction,
            ),
        )
        await self._conn.commit()

    async def log_scan_stats(
        self,
        scan_type: str,
        symbols_scanned: int,
        opportunities_found: int,
        duration_sec: float,
        errors: int = 0,
    ) -> None:
        if not self._conn:
            return
        await self._conn.execute(
            """INSERT INTO scan_stats
               (timestamp, scan_type, symbols_scanned, opportunities_found,
                duration_sec, errors)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                time.time(), scan_type, symbols_scanned,
                opportunities_found, duration_sec, errors,
            ),
        )
        await self._conn.commit()

    async def get_recent_opportunities(
        self, limit: int = 50
    ) -> list[dict]:
        if not self._conn:
            return []
        self._conn.row_factory = aiosqlite.Row
        cursor = await self._conn.execute(
            "SELECT * FROM opportunities ORDER BY timestamp DESC LIMIT ?",
            (limit,),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    async def get_stats_summary(self) -> dict:
        if not self._conn:
            return {}
        cursor = await self._conn.execute(
            """SELECT
                 COUNT(*) as total_opportunities,
                 AVG(net_profit_pct) as avg_profit_pct,
                 MAX(net_profit_pct) as max_profit_pct,
                 SUM(estimated_profit_usd) as total_estimated_profit
               FROM opportunities
               WHERE timestamp > ?""",
            (time.time() - 86400,),
        )
        row = await cursor.fetchone()
        if row:
            return {
                "total_opportunities_24h": row[0],
                "avg_profit_pct": round(row[1] or 0, 3),
                "max_profit_pct": round(row[2] or 0, 3),
                "total_estimated_profit": round(row[3] or 0, 2),
            }
        return {}
