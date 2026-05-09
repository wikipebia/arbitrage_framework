"""
Arbitrage Framework — unified entry point.

Launches spread scanner and lag monitor concurrently.
All configuration is loaded from config.json.

Usage:
    python main.py                  # run both scanner and lag monitor
    python main.py --scanner-only   # run only the spread scanner
    python main.py --lag-only       # run only the lag monitor
    python main.py --config my.json # use custom config file
"""

import argparse
import asyncio
import json
import logging
import os
import signal
import sys

from calculator.profit_calculator import ProfitCalculator
from notifications.telegram_bot import TelegramNotifier
from scanner.lag_monitor import LagMonitor
from scanner.spread_scanner import SpreadScanner
from storage.db import ArbitrageDB

log = logging.getLogger("arbitrage")


def load_config(path: str) -> dict:
    if not os.path.exists(path):
        log.error("Config file not found: %s", path)
        sys.exit(1)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def setup_logging(debug: bool = False) -> None:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)-7s] %(name)-25s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )
    logging.getLogger("ccxt").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)
    logging.getLogger("aiosqlite").setLevel(logging.WARNING)


async def run(config: dict, scanner_only: bool, lag_only: bool) -> None:
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    db_cfg = config.get("database", {})
    db = ArbitrageDB(db_cfg.get("path", "arbitrage.db"))
    await db.connect()

    tg_cfg = config.get("telegram", {})
    notifier = TelegramNotifier(
        bot_token=tg_cfg.get("bot_token", ""),
        chat_id=tg_cfg.get("chat_id", ""),
        min_profit_pct=tg_cfg.get("min_profit_pct_alert", 0.5),
        cooldown_sec=tg_cfg.get("alert_cooldown_sec", 60),
        enabled=tg_cfg.get("enabled", False),
    )
    await notifier.start()

    exchange_configs = config.get("exchanges", [])
    enabled_exchanges = [e for e in exchange_configs if e.get("enabled", True)]

    exchange_fees = {
        e["id"]: e.get("trading_fee_pct", 0.1) for e in enabled_exchanges
    }
    withdrawal_fees = {
        e["id"]: e.get("withdrawal_fees", {}) for e in enabled_exchanges
    }

    scanner_cfg = config.get("scanner", {})
    retry_cfg = config.get("retry", {})

    calculator = ProfitCalculator(
        exchange_fees=exchange_fees,
        withdrawal_fees=withdrawal_fees,
        position_usd=scanner_cfg.get("position_size_usd", 500),
        min_profit_pct=scanner_cfg.get("min_spread_pct", 0.3),
        network_fee_usd=0.15,
    )

    tasks: list[asyncio.Task] = []

    if not lag_only:
        scanner = SpreadScanner(
            exchange_configs=enabled_exchanges,
            calculator=calculator,
            db=db,
            notifier=notifier,
            symbols=scanner_cfg.get("symbols") or None,
            quote_currencies=scanner_cfg.get("quote_currencies", ["USDT", "USDC"]),
            min_spread_pct=scanner_cfg.get("min_spread_pct", 0.3),
            max_spread_pct=scanner_cfg.get("max_spread_pct", 20.0),
            min_volume_24h=scanner_cfg.get("min_volume_24h_usd", 50000),
            scan_interval=scanner_cfg.get("scan_interval_sec", 10),
            max_concurrent=scanner_cfg.get("max_concurrent_requests", 10),
            max_retries=retry_cfg.get("max_retries", 3),
            base_delay=retry_cfg.get("base_delay_sec", 1.0),
        )

        exchange_names = [e["id"] for e in enabled_exchanges]
        await notifier.alert_startup(
            exchanges=exchange_names,
            symbols_count=0,
        )

        tasks.append(
            asyncio.create_task(scanner.run(stop_event), name="spread_scanner")
        )
        log.info("Spread scanner task created")

    if not scanner_only:
        lag_cfg = config.get("lag_monitor", {})
        if lag_cfg.get("enabled", True):
            all_exchange_names = [e["id"] for e in enabled_exchanges]
            lag_monitor = LagMonitor(
                exchange_names=all_exchange_names,
                reference_exchanges=lag_cfg.get(
                    "reference_exchanges",
                    ["binance", "okx", "bybit", "coinbase", "kucoin"],
                ),
                db=db,
                notifier=notifier,
                lag_threshold=lag_cfg.get("lag_threshold_sec", 1.0),
                price_move_pct=lag_cfg.get("price_move_pct", 0.05),
                fetch_interval=lag_cfg.get("fetch_interval_sec", 0.5),
                quote_currencies=scanner_cfg.get("quote_currencies", ["USDT", "USDC"]),
                max_symbols=lag_cfg.get("max_symbols", 50),
            )
            tasks.append(
                asyncio.create_task(lag_monitor.run(stop_event), name="lag_monitor")
            )
            log.info("Lag monitor task created")

    if not tasks:
        log.error("No tasks to run. Use --scanner-only or --lag-only, not both.")
        await notifier.stop()
        await db.close()
        return

    log.info("All systems go. Press Ctrl+C to stop.")

    try:
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_EXCEPTION
        )
        for task in done:
            exc = task.exception()
            if exc:
                log.error("Task %s failed: %s", task.get_name(), exc)
    except asyncio.CancelledError:
        pass
    finally:
        stop_event.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        stats = await db.get_stats_summary()
        if stats:
            log.info("Session stats: %s", stats)
            await notifier.alert_summary(stats)

        await notifier.stop()
        await db.close()
        log.info("Shutdown complete.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Arbitrage Framework — multi-exchange spread scanner & lag monitor",
    )
    parser.add_argument(
        "--config",
        default="config.json",
        help="Path to config.json (default: config.json)",
    )
    parser.add_argument(
        "--scanner-only",
        action="store_true",
        help="Run only the spread scanner (no lag monitor)",
    )
    parser.add_argument(
        "--lag-only",
        action="store_true",
        help="Run only the lag monitor (no spread scanner)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    setup_logging(debug=args.debug)

    config = load_config(args.config)
    log.info("Config loaded from %s", args.config)

    try:
        asyncio.run(
            run(config, scanner_only=args.scanner_only, lag_only=args.lag_only)
        )
    except KeyboardInterrupt:
        log.info("Interrupted by user")


if __name__ == "__main__":
    main()
