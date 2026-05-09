"""
Multi-exchange spread scanner.

Merged from zapozdavwie.py (CEX-CEX spread monitoring) and scanner.py (CEX-DEX).
Async architecture with ccxt.async_support, rate-limit awareness, retry logic.
"""

import asyncio
import logging
import time
from collections import defaultdict

import ccxt.async_support as ccxt

from calculator.profit_calculator import ProfitCalculator, Opportunity
from notifications.telegram_bot import TelegramNotifier
from storage.db import ArbitrageDB

log = logging.getLogger("arbitrage.scanner")


def _build_exchange(
    exchange_id: str,
    api_key: str = "",
    secret: str = "",
    options: dict | None = None,
) -> ccxt.Exchange | None:
    cls = getattr(ccxt, exchange_id, None)
    if cls is None:
        log.warning("ccxt does not support exchange '%s'", exchange_id)
        return None
    config: dict = {
        "enableRateLimit": True,
        "timeout": 15000,
        "options": options or {"defaultType": "spot"},
    }
    if api_key:
        config["apiKey"] = api_key
    if secret:
        config["secret"] = secret
    try:
        return cls(config)
    except Exception as e:
        log.warning("Failed to create exchange %s: %s", exchange_id, e)
        return None


async def _retry_async(coro_fn, max_retries: int = 3, base_delay: float = 1.0):
    """Retry an async callable with exponential backoff."""
    last_err = None
    for attempt in range(max_retries + 1):
        try:
            return await coro_fn()
        except (ccxt.RateLimitExceeded, ccxt.RequestTimeout) as e:
            last_err = e
            delay = base_delay * (2 ** attempt)
            log.debug(
                "Rate limit / timeout on attempt %d, retrying in %.1fs: %s",
                attempt + 1, delay, e,
            )
            await asyncio.sleep(delay)
        except (ccxt.NetworkError, ccxt.ExchangeNotAvailable) as e:
            last_err = e
            delay = base_delay * (2 ** attempt)
            log.debug(
                "Network error on attempt %d, retrying in %.1fs: %s",
                attempt + 1, delay, e,
            )
            await asyncio.sleep(delay)
        except ccxt.ExchangeError as e:
            log.debug("Exchange error (not retrying): %s", e)
            return None
    log.warning("All %d retries exhausted: %s", max_retries, last_err)
    return None


class SpreadScanner:
    """
    Scans multiple exchanges for spread-based arbitrage opportunities.

    Workflow per cycle:
      1. Fetch tickers from all exchanges in parallel
      2. Group by symbol, filter by volume
      3. Compare ask/bid across exchanges
      4. Calculate net profit via ProfitCalculator
      5. Log to DB, send Telegram alerts
    """

    def __init__(
        self,
        exchange_configs: list[dict],
        calculator: ProfitCalculator,
        db: ArbitrageDB,
        notifier: TelegramNotifier,
        symbols: list[str] | None = None,
        quote_currencies: list[str] | None = None,
        min_spread_pct: float = 0.3,
        max_spread_pct: float = 20.0,
        min_volume_24h: float = 50000.0,
        scan_interval: float = 10.0,
        max_concurrent: int = 10,
        max_retries: int = 3,
        base_delay: float = 1.0,
    ):
        self._exchange_configs = exchange_configs
        self._calculator = calculator
        self._db = db
        self._notifier = notifier
        self._target_symbols = symbols or []
        self._quote_currencies = quote_currencies or ["USDT", "USDC"]
        self._min_spread = min_spread_pct
        self._max_spread = max_spread_pct
        self._min_volume = min_volume_24h
        self._scan_interval = scan_interval
        self._max_concurrent = max_concurrent
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._exchanges: dict[str, ccxt.Exchange] = {}
        self._running = False

    async def _init_exchanges(self) -> None:
        for cfg in self._exchange_configs:
            if not cfg.get("enabled", True):
                continue
            ex = _build_exchange(
                exchange_id=cfg["id"],
                api_key=cfg.get("api_key", ""),
                secret=cfg.get("secret", ""),
                options=cfg.get("options"),
            )
            if ex:
                self._exchanges[cfg["id"]] = ex

        log.info(
            "Initialized %d exchanges: %s",
            len(self._exchanges),
            ", ".join(self._exchanges.keys()),
        )

    async def _close_exchanges(self) -> None:
        tasks = [ex.close() for ex in self._exchanges.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._exchanges.clear()

    async def _load_markets(self) -> None:
        sem = asyncio.Semaphore(5)

        async def _load_one(name: str, ex: ccxt.Exchange) -> None:
            async with sem:
                try:
                    await asyncio.wait_for(ex.load_markets(), timeout=30)
                    log.info("[%s] loaded %d markets", name, len(ex.markets))
                except Exception as e:
                    log.warning("[%s] load_markets failed: %s", name, e)

        await asyncio.gather(
            *[_load_one(n, e) for n, e in self._exchanges.items()],
            return_exceptions=True,
        )

    def _discover_symbols(self) -> list[str]:
        if self._target_symbols:
            return self._target_symbols

        symbol_exchanges: dict[str, set[str]] = defaultdict(set)

        for name, ex in self._exchanges.items():
            if not ex.markets:
                continue
            for sym in ex.markets:
                quote = sym.split("/")[-1] if "/" in sym else ""
                if quote in self._quote_currencies:
                    symbol_exchanges[sym].add(name)

        symbols = [
            sym
            for sym, exs in symbol_exchanges.items()
            if len(exs) >= 2
        ]
        symbols.sort()
        log.info("Discovered %d symbols tradeable on 2+ exchanges", len(symbols))
        return symbols

    async def _fetch_tickers(
        self, exchange_id: str, exchange: ccxt.Exchange
    ) -> dict[str, dict]:
        """Fetch all tickers from one exchange with retry."""
        result = await _retry_async(
            lambda: exchange.fetch_tickers(),
            max_retries=self._max_retries,
            base_delay=self._base_delay,
        )
        if result is None:
            return {}
        return result

    async def _scan_cycle(self, symbols: list[str]) -> list[Opportunity]:
        """Run one full scan cycle across all exchanges."""
        sem = asyncio.Semaphore(self._max_concurrent)

        async def _fetch_guarded(name: str, ex: ccxt.Exchange) -> tuple[str, dict]:
            async with sem:
                tickers = await self._fetch_tickers(name, ex)
                return name, tickers

        tasks = [
            _fetch_guarded(name, ex)
            for name, ex in self._exchanges.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_tickers: dict[str, dict[str, dict]] = {}
        errors = 0
        for r in results:
            if isinstance(r, Exception):
                errors += 1
                continue
            name, tickers = r
            if tickers:
                all_tickers[name] = tickers

        if len(all_tickers) < 2:
            log.warning("Less than 2 exchanges returned data, skipping cycle")
            return []

        opportunities: list[Opportunity] = []

        for symbol in symbols:
            prices: dict[str, dict] = {}
            for ex_name, tickers in all_tickers.items():
                ticker = tickers.get(symbol)
                if not ticker:
                    continue

                bid = ticker.get("bid")
                ask = ticker.get("ask")
                if not bid or not ask or bid <= 0 or ask <= 0:
                    continue

                base_vol = ticker.get("baseVolume") or 0
                quote_vol = ticker.get("quoteVolume") or 0
                last = ticker.get("last") or ((bid + ask) / 2)
                vol_usd = quote_vol if quote_vol > 0 else (base_vol * last)

                if vol_usd < self._min_volume:
                    continue

                prices[ex_name] = {
                    "bid": bid,
                    "ask": ask,
                    "volume": vol_usd,
                }

            if len(prices) < 2:
                continue

            opps = self._calculator.find_best_pair(
                symbol=symbol,
                prices=prices,
                min_volume=self._min_volume,
            )

            for opp in opps:
                if opp.gross_spread_pct > self._max_spread:
                    continue
                opportunities.append(opp)

        opportunities.sort(key=lambda o: o.net_profit_pct, reverse=True)
        return opportunities

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Main scan loop."""
        self._running = True
        await self._init_exchanges()

        if not self._exchanges:
            log.error("No exchanges initialized, exiting scanner")
            return

        await self._load_markets()

        symbols = self._discover_symbols()
        if not symbols:
            log.error("No tradeable symbols found, exiting scanner")
            await self._close_exchanges()
            return

        log.info(
            "Spread scanner started: %d exchanges, %d symbols, "
            "interval=%ds, min_spread=%.2f%%, min_volume=$%.0f",
            len(self._exchanges), len(symbols),
            self._scan_interval, self._min_spread, self._min_volume,
        )

        cycle_count = 0
        try:
            while self._running:
                if stop_event and stop_event.is_set():
                    break

                cycle_count += 1
                t0 = time.time()
                log.info(
                    "--- Scan cycle #%d | %s ---",
                    cycle_count,
                    time.strftime("%H:%M:%S"),
                )

                try:
                    opportunities = await self._scan_cycle(symbols)
                except Exception as e:
                    log.error("Scan cycle error: %s", e)
                    opportunities = []

                duration = time.time() - t0

                if opportunities:
                    log.info(
                        "Found %d opportunities (cycle took %.1fs):",
                        len(opportunities), duration,
                    )
                    for opp in opportunities[:10]:
                        log.info("  %s", opp)

                    for opp in opportunities:
                        await self._db.log_opportunity(
                            symbol=opp.symbol,
                            buy_exchange=opp.buy_exchange,
                            sell_exchange=opp.sell_exchange,
                            buy_price=opp.buy_price,
                            sell_price=opp.sell_price,
                            spread_pct=opp.gross_spread_pct,
                            net_profit_pct=opp.net_profit_pct,
                            volume_24h=opp.volume_24h,
                            buy_fee_pct=opp.buy_fee_pct,
                            sell_fee_pct=opp.sell_fee_pct,
                            withdrawal_fee=opp.withdrawal_fee_usd,
                            network_fee=opp.network_fee_usd,
                            position_usd=opp.position_usd,
                            estimated_profit_usd=opp.estimated_profit_usd,
                            direction=opp.direction,
                        )

                        await self._notifier.alert_opportunity(
                            symbol=opp.symbol,
                            buy_exchange=opp.buy_exchange,
                            sell_exchange=opp.sell_exchange,
                            buy_price=opp.buy_price,
                            sell_price=opp.sell_price,
                            spread_pct=opp.gross_spread_pct,
                            net_profit_pct=opp.net_profit_pct,
                            volume_24h=opp.volume_24h,
                            estimated_profit_usd=opp.estimated_profit_usd,
                            direction=opp.direction,
                        )
                else:
                    log.info(
                        "No opportunities above threshold (cycle took %.1fs)",
                        duration,
                    )

                await self._db.log_scan_stats(
                    scan_type="spread",
                    symbols_scanned=len(symbols),
                    opportunities_found=len(opportunities),
                    duration_sec=duration,
                )

                sleep_time = max(0, self._scan_interval - duration)
                if stop_event:
                    try:
                        await asyncio.wait_for(
                            stop_event.wait(), timeout=sleep_time
                        )
                        break
                    except asyncio.TimeoutError:
                        pass
                else:
                    await asyncio.sleep(sleep_time)

        except asyncio.CancelledError:
            log.info("Spread scanner cancelled")
        finally:
            self._running = False
            await self._close_exchanges()
            log.info("Spread scanner stopped")
