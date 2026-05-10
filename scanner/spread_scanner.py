"""Multi-exchange spread scanner with signal validation.

Merged from zapozdavwie.py (CEX-CEX spread monitoring) and scanner.py (CEX-DEX).
Async architecture with ccxt.async_support, rate-limit awareness, retry logic.

Signal validation:
  - Ticker freshness: reject tickers older than max_ticker_age_sec
  - Re-confirmation: re-fetch top opportunities to confirm spread still exists
  - Bid-ask spread check: wide bid-ask = illiquid, unreliable
  - Dead exchange detection: skip exchanges with consecutive failures
  - Symbol blacklist: static (config) + auto-blacklist for persistent anomalies
  - Deposit/withdrawal status: skip pairs where transfers are suspended
"""

import asyncio
import logging
import re
import time
from collections import defaultdict

import ccxt.async_support as ccxt

from calculator.profit_calculator import ProfitCalculator, Opportunity
from notifications.telegram_bot import TelegramNotifier
from storage.db import ArbitrageDB

log = logging.getLogger("arbitrage.scanner")

MAX_EXCHANGE_ERRORS = 10
AUTO_BLACKLIST_THRESHOLD = 3  # auto-blacklist after N consecutive anomalous cycles
ANOMALOUS_SPREAD_PCT = 30.0  # spreads above this are considered anomalous

LEVERAGED_TOKEN_RE = re.compile(
    r"\d+[SLsl]/|UP/|DOWN/|BULL/|BEAR/|HALF/|EDGE/"
)


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
      1. Fetch tickers from all exchanges in parallel (record fetch timestamps)
      2. Group by symbol, filter by volume AND ticker freshness
      3. Compare ask/bid across exchanges, check bid-ask spread health
      4. Calculate net profit via ProfitCalculator
      5. Re-confirm top opportunities by re-fetching individual tickers
      6. Log to DB, send Telegram alerts only for confirmed opportunities
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
        max_ticker_age_sec: float = 60.0,
        max_bid_ask_spread_pct: float = 3.0,
        confirm_top_n: int = 5,
        confirm_min_spread_pct: float = 0.5,
        symbol_blacklist: list[str] | None = None,
        filter_leveraged_tokens: bool = True,
        check_deposit_withdraw: bool = True,
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
        self._max_ticker_age = max_ticker_age_sec
        self._max_bid_ask_spread = max_bid_ask_spread_pct
        self._confirm_top_n = confirm_top_n
        self._confirm_min_spread = confirm_min_spread_pct
        self._filter_leveraged = filter_leveraged_tokens
        self._check_deposit_withdraw = check_deposit_withdraw
        self._exchanges: dict[str, ccxt.Exchange] = {}
        self._exchange_errors: dict[str, int] = defaultdict(int)
        self._exchange_fetch_latency: dict[str, float] = {}
        self._running = False
        self._static_blacklist: set[str] = set(symbol_blacklist or [])
        self._auto_blacklist: set[str] = set()
        self._anomaly_streak: dict[str, int] = defaultdict(int)
        self._deposit_withdraw_cache: dict[str, dict[str, bool]] = {}

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
        failed: list[str] = []

        async def _load_one(name: str, ex: ccxt.Exchange) -> None:
            async with sem:
                try:
                    await asyncio.wait_for(ex.load_markets(), timeout=30)
                    log.info("[%s] loaded %d markets", name, len(ex.markets))
                except Exception as e:
                    log.warning("[%s] load_markets failed: %s", name, e)
                    failed.append(name)

        await asyncio.gather(
            *[_load_one(n, e) for n, e in self._exchanges.items()],
            return_exceptions=True,
        )

        for name in failed:
            ex = self._exchanges.pop(name, None)
            if ex:
                try:
                    await ex.close()
                except Exception:
                    pass
            log.info("[%s] removed from active exchanges", name)

        if failed:
            log.info(
                "Removed %d failed exchanges, %d remaining",
                len(failed), len(self._exchanges),
            )

    def _is_blacklisted(self, symbol: str) -> bool:
        """Check static + auto blacklist + leveraged token filter."""
        if symbol in self._static_blacklist or symbol in self._auto_blacklist:
            return True
        if self._filter_leveraged and LEVERAGED_TOKEN_RE.search(symbol):
            return True
        return False

    def _update_auto_blacklist(self, opportunities: list[Opportunity]) -> int:
        """Track symbols with anomalous spreads; auto-blacklist persistent ones."""
        seen_anomalous: set[str] = set()
        for opp in opportunities:
            if opp.gross_spread_pct > ANOMALOUS_SPREAD_PCT:
                seen_anomalous.add(opp.symbol)

        newly_blacklisted = 0
        all_symbols = set(self._anomaly_streak.keys()) | seen_anomalous
        for sym in all_symbols:
            if sym in seen_anomalous:
                self._anomaly_streak[sym] += 1
                if self._anomaly_streak[sym] >= AUTO_BLACKLIST_THRESHOLD:
                    if sym not in self._auto_blacklist:
                        self._auto_blacklist.add(sym)
                        newly_blacklisted += 1
                        log.warning(
                            "Auto-blacklisted %s (anomalous %d cycles)",
                            sym, self._anomaly_streak[sym],
                        )
            else:
                self._anomaly_streak[sym] = 0
        return newly_blacklisted

    async def _load_deposit_withdraw_status(self) -> None:
        """Fetch deposit/withdraw status from exchanges that support it."""
        if not self._check_deposit_withdraw:
            return

        sem = asyncio.Semaphore(3)

        async def _fetch_currencies(name: str, ex: ccxt.Exchange) -> None:
            async with sem:
                try:
                    if not hasattr(ex, 'fetch_currencies') or not ex.has.get('fetchCurrencies'):
                        return
                    currencies = await asyncio.wait_for(
                        ex.fetch_currencies(), timeout=15,
                    )
                    if not currencies:
                        return
                    status: dict[str, bool] = {}
                    for code, info in currencies.items():
                        deposit_ok = info.get('deposit', True)
                        withdraw_ok = info.get('withdraw', True)
                        active = info.get('active', True)
                        status[code] = bool(deposit_ok and withdraw_ok and active)
                    self._deposit_withdraw_cache[name] = status
                    log.info(
                        "[%s] loaded deposit/withdraw status for %d currencies",
                        name, len(status),
                    )
                except Exception as e:
                    log.debug("[%s] fetch_currencies failed: %s", name, e)

        await asyncio.gather(
            *[_fetch_currencies(n, e) for n, e in self._exchanges.items()],
            return_exceptions=True,
        )
        log.info(
            "Deposit/withdraw status loaded for %d/%d exchanges",
            len(self._deposit_withdraw_cache), len(self._exchanges),
        )

    def _is_transfer_ok(self, symbol: str, buy_exchange: str, sell_exchange: str) -> bool:
        """Check if deposit on sell_exchange and withdrawal on buy_exchange are enabled."""
        if not self._check_deposit_withdraw:
            return True
        base = symbol.split("/")[0] if "/" in symbol else symbol

        buy_status = self._deposit_withdraw_cache.get(buy_exchange)
        if buy_status is not None:
            if not buy_status.get(base, True):
                return False

        sell_status = self._deposit_withdraw_cache.get(sell_exchange)
        if sell_status is not None:
            if not sell_status.get(base, True):
                return False
        return True

    def _discover_symbols(self) -> list[str]:
        if self._target_symbols:
            return [
                s for s in self._target_symbols
                if not self._is_blacklisted(s)
            ]

        symbol_exchanges: dict[str, set[str]] = defaultdict(set)

        for name, ex in self._exchanges.items():
            if not ex.markets:
                continue
            for sym in ex.markets:
                quote = sym.split("/")[-1] if "/" in sym else ""
                if quote in self._quote_currencies:
                    symbol_exchanges[sym].add(name)

        blacklisted_count = 0
        symbols = []
        for sym, exs in symbol_exchanges.items():
            if len(exs) < 2:
                continue
            if self._is_blacklisted(sym):
                blacklisted_count += 1
                continue
            symbols.append(sym)

        symbols.sort()
        log.info(
            "Discovered %d symbols tradeable on 2+ exchanges "
            "(%d blacklisted, %d leveraged filtered)",
            len(symbols), blacklisted_count,
            blacklisted_count,
        )
        return symbols

    async def _fetch_tickers(
        self, exchange_id: str, exchange: ccxt.Exchange
    ) -> dict[str, dict]:
        """Fetch all tickers from one exchange with retry. Track latency."""
        if self._exchange_errors[exchange_id] >= MAX_EXCHANGE_ERRORS:
            return {}

        t0 = time.time()
        result = await _retry_async(
            lambda: exchange.fetch_tickers(),
            max_retries=self._max_retries,
            base_delay=self._base_delay,
        )
        latency = time.time() - t0
        self._exchange_fetch_latency[exchange_id] = latency

        if result is None:
            self._exchange_errors[exchange_id] += 1
            return {}

        self._exchange_errors[exchange_id] = 0
        return result

    def _is_ticker_fresh(self, ticker: dict, now_ms: float) -> bool:
        """Check if ticker timestamp is recent enough."""
        ts = ticker.get("timestamp")
        if ts is None:
            return True
        age_sec = (now_ms - ts) / 1000.0
        if age_sec < 0:
            age_sec = abs(age_sec)
        return age_sec <= self._max_ticker_age

    def _is_bid_ask_healthy(self, bid: float, ask: float) -> bool:
        """Check if bid-ask spread is reasonable (not illiquid)."""
        if bid <= 0 or ask <= 0:
            return False
        ba_spread_pct = (ask - bid) / bid * 100.0
        return ba_spread_pct <= self._max_bid_ask_spread

    async def _confirm_opportunity(self, opp: Opportunity) -> Opportunity | None:
        """Re-fetch tickers for buy and sell exchanges to confirm spread."""
        buy_ex = self._exchanges.get(opp.buy_exchange)
        sell_ex = self._exchanges.get(opp.sell_exchange)
        if not buy_ex or not sell_ex:
            return None

        try:
            buy_ticker, sell_ticker = await asyncio.gather(
                _retry_async(
                    lambda be=buy_ex: be.fetch_ticker(opp.symbol),
                    max_retries=1, base_delay=0.5,
                ),
                _retry_async(
                    lambda se=sell_ex: se.fetch_ticker(opp.symbol),
                    max_retries=1, base_delay=0.5,
                ),
            )
        except Exception:
            return None

        if not buy_ticker or not sell_ticker:
            return None

        new_ask = buy_ticker.get("ask")
        new_bid = sell_ticker.get("bid")
        if not new_ask or not new_bid or new_ask <= 0 or new_bid <= 0:
            return None

        if not self._is_bid_ask_healthy(buy_ticker.get("bid", 0), new_ask):
            return None
        if not self._is_bid_ask_healthy(new_bid, sell_ticker.get("ask", 0)):
            return None

        buy_vol = buy_ticker.get("quoteVolume") or 0
        sell_vol = sell_ticker.get("quoteVolume") or 0
        if buy_vol <= 0:
            buy_vol = (buy_ticker.get("baseVolume") or 0) * new_ask
        if sell_vol <= 0:
            sell_vol = (sell_ticker.get("baseVolume") or 0) * new_bid
        min_vol = min(buy_vol, sell_vol)

        confirmed = self._calculator.calculate(
            symbol=opp.symbol,
            buy_exchange=opp.buy_exchange,
            sell_exchange=opp.sell_exchange,
            buy_price=new_ask,
            sell_price=new_bid,
            volume_24h=min_vol,
        )
        if confirmed and confirmed.net_profit_pct >= self._confirm_min_spread:
            return confirmed
        return None

    async def _scan_cycle(self, symbols: list[str]) -> tuple[list[Opportunity], dict]:
        """Run one full scan cycle across all exchanges. Returns (opportunities, stats)."""
        sem = asyncio.Semaphore(self._max_concurrent)
        now_ms = time.time() * 1000.0
        stats = {
            "stale_tickers": 0, "wide_bid_ask": 0,
            "low_volume": 0, "confirmed": 0, "rejected_on_confirm": 0,
            "blacklisted": 0, "transfer_blocked": 0,
        }

        async def _fetch_guarded(name: str, ex: ccxt.Exchange) -> tuple[str, dict]:
            async with sem:
                tickers = await self._fetch_tickers(name, ex)
                return name, tickers

        active_exchanges = {
            name: ex for name, ex in self._exchanges.items()
            if self._exchange_errors[name] < MAX_EXCHANGE_ERRORS
        }
        tasks = [
            _fetch_guarded(name, ex)
            for name, ex in active_exchanges.items()
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        all_tickers: dict[str, dict[str, dict]] = {}
        for r in results:
            if isinstance(r, Exception):
                continue
            name, tickers = r
            if tickers:
                all_tickers[name] = tickers

        if len(all_tickers) < 2:
            log.warning("Less than 2 exchanges returned data, skipping cycle")
            return [], stats

        opportunities: list[Opportunity] = []

        for symbol in symbols:
            if self._is_blacklisted(symbol):
                stats["blacklisted"] += 1
                continue

            prices: dict[str, dict] = {}
            for ex_name, tickers in all_tickers.items():
                ticker = tickers.get(symbol)
                if not ticker:
                    continue

                if not self._is_ticker_fresh(ticker, now_ms):
                    stats["stale_tickers"] += 1
                    continue

                bid = ticker.get("bid")
                ask = ticker.get("ask")
                if not bid or not ask or bid <= 0 or ask <= 0:
                    continue

                if not self._is_bid_ask_healthy(bid, ask):
                    stats["wide_bid_ask"] += 1
                    continue

                base_vol = ticker.get("baseVolume") or 0
                quote_vol = ticker.get("quoteVolume") or 0
                last = ticker.get("last") or ((bid + ask) / 2)
                vol_usd = quote_vol if quote_vol > 0 else (base_vol * last)

                if vol_usd < self._min_volume:
                    stats["low_volume"] += 1
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
                if not self._is_transfer_ok(
                    opp.symbol, opp.buy_exchange, opp.sell_exchange,
                ):
                    stats["transfer_blocked"] += 1
                    continue
                opportunities.append(opp)

        opportunities.sort(key=lambda o: o.net_profit_pct, reverse=True)
        return opportunities, stats

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Main scan loop."""
        self._running = True
        await self._init_exchanges()

        if not self._exchanges:
            log.error("No exchanges initialized, exiting scanner")
            return

        await self._load_markets()
        await self._load_deposit_withdraw_status()

        symbols = self._discover_symbols()
        if not symbols:
            log.error("No tradeable symbols found, exiting scanner")
            await self._close_exchanges()
            return

        bl_count = len(self._static_blacklist) + len(self._auto_blacklist)
        log.info(
            "Spread scanner started: %d exchanges, %d symbols, "
            "interval=%ds, min_spread=%.2f%%, min_volume=$%.0f, "
            "max_ticker_age=%ds, max_bid_ask=%.1f%%, confirm_top=%d, "
            "blacklisted=%d, deposit_withdraw_check=%s",
            len(self._exchanges), len(symbols),
            self._scan_interval, self._min_spread, self._min_volume,
            self._max_ticker_age, self._max_bid_ask_spread, self._confirm_top_n,
            bl_count, self._check_deposit_withdraw,
        )

        cycle_count = 0
        try:
            while self._running:
                if stop_event and stop_event.is_set():
                    break

                cycle_count += 1
                t0 = time.time()
                self._notifier.reset_cycle_counter()
                log.info(
                    "--- Scan cycle #%d | %s ---",
                    cycle_count,
                    time.strftime("%H:%M:%S"),
                )

                try:
                    opportunities, stats = await self._scan_cycle(symbols)
                except Exception as e:
                    log.error("Scan cycle error: %s", e)
                    opportunities = []
                    stats = {}

                duration = time.time() - t0

                if opportunities:
                    top_to_confirm = opportunities[:self._confirm_top_n]
                    confirmed: list[Opportunity] = []

                    if self._confirm_top_n > 0:
                        confirm_tasks = [
                            self._confirm_opportunity(opp)
                            for opp in top_to_confirm
                        ]
                        confirm_results = await asyncio.gather(
                            *confirm_tasks, return_exceptions=True
                        )
                        for r in confirm_results:
                            if isinstance(r, Opportunity):
                                confirmed.append(r)
                        stats["confirmed"] = len(confirmed)
                        stats["rejected_on_confirm"] = (
                            len(top_to_confirm) - len(confirmed)
                        )

                    remaining = opportunities[self._confirm_top_n:]
                    all_valid = confirmed + remaining

                    new_bl = self._update_auto_blacklist(opportunities)
                    if new_bl:
                        symbols = [
                            s for s in symbols if not self._is_blacklisted(s)
                        ]

                    filter_log = (
                        f"stale={stats.get('stale_tickers', 0)} "
                        f"wide_ba={stats.get('wide_bid_ask', 0)} "
                        f"low_vol={stats.get('low_volume', 0)} "
                        f"xfer_blocked={stats.get('transfer_blocked', 0)}"
                    )
                    confirm_log = ""
                    if self._confirm_top_n > 0:
                        confirm_log = (
                            f" | confirmed={stats.get('confirmed', 0)}"
                            f"/{len(top_to_confirm)}"
                        )

                    log.info(
                        "Found %d raw, %d after validation "
                        "(cycle %.1fs) [%s%s]:",
                        len(opportunities), len(all_valid),
                        duration, filter_log, confirm_log,
                    )
                    for opp in all_valid[:10]:
                        tag = "[CONFIRMED]" if opp in confirmed else ""
                        log.info("  %s %s", tag, opp)

                    for opp in all_valid:
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

                    for opp in confirmed:
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
