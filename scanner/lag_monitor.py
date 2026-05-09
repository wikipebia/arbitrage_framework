"""
Price lag monitor — detects when exchanges lag behind reference price movements.

Based on price_lag_monitor.py. Detects when reference exchanges (binance, okx, bybit,
etc.) move, but a smaller exchange hasn't reacted yet — a potential arbitrage signal.
"""

import asyncio
import logging
import statistics
import time
from collections import deque
from dataclasses import dataclass, field

import ccxt.async_support as ccxt

from notifications.telegram_bot import TelegramNotifier
from storage.db import ArbitrageDB

log = logging.getLogger("arbitrage.lag_monitor")

REFERENCE_WINDOW = 10  # seconds — window for detecting reference price movement


@dataclass
class PriceTick:
    ts: float       # time.monotonic()
    price: float
    wall_ts: float  # time.time() for logs


@dataclass
class ExchangeState:
    name: str
    history: deque = field(default_factory=lambda: deque(maxlen=600))
    lag_start_ts: float | None = None
    lag_ref_price: float | None = None
    lag_direction: int | None = None  # +1 / -1
    last_alert_ts: float = 0.0
    last_price: float | None = None
    consecutive_errors: int = 0

    def add_tick(self, tick: PriceTick) -> None:
        self.history.append(tick)
        self.last_price = tick.price

    def price_at(self, ts: float) -> float | None:
        result = None
        for t in self.history:
            if t.ts <= ts:
                result = t.price
            else:
                break
        return result

    def reset_lag(self) -> None:
        self.lag_start_ts = None
        self.lag_ref_price = None
        self.lag_direction = None


def _build_exchange(name: str) -> ccxt.Exchange | None:
    cls = getattr(ccxt, name, None)
    if cls is None:
        log.warning("ccxt does not support exchange '%s'", name)
        return None
    try:
        return cls({"enableRateLimit": True, "timeout": 10000})
    except Exception as e:
        log.warning("Failed to create exchange %s: %s", name, e)
        return None


async def _load_markets_safe(exchange: ccxt.Exchange, name: str) -> set[str]:
    try:
        await asyncio.wait_for(exchange.load_markets(), timeout=15)
        return set(exchange.markets.keys())
    except Exception as e:
        log.warning("[%s] load_markets: %s", name, e)
        return set()


class LagMonitor:
    """Monitors price lags between reference and non-reference exchanges."""

    def __init__(
        self,
        exchange_names: list[str],
        reference_exchanges: list[str],
        db: ArbitrageDB,
        notifier: TelegramNotifier,
        lag_threshold: float = 1.0,
        price_move_pct: float = 0.05,
        fetch_interval: float = 0.5,
        quote_currencies: list[str] | None = None,
        max_symbols: int = 0,
        explicit_symbols: list[str] | None = None,
    ):
        self._exchange_names = exchange_names
        self._reference_names = reference_exchanges
        self._db = db
        self._notifier = notifier
        self._lag_threshold = lag_threshold
        self._price_move_pct = price_move_pct
        self._fetch_interval = fetch_interval
        self._quote_currencies = quote_currencies or ["USDT", "USDC", "USD"]
        self._max_symbols = max_symbols
        self._explicit_symbols = explicit_symbols
        self._exchanges: dict[str, ccxt.Exchange] = {}

    async def _init_exchanges(self) -> None:
        for name in self._exchange_names:
            ex = _build_exchange(name)
            if ex:
                self._exchanges[name] = ex
        log.info(
            "Lag monitor: %d exchanges initialized", len(self._exchanges)
        )

    async def _close_exchanges(self) -> None:
        tasks = [ex.close() for ex in self._exchanges.values()]
        await asyncio.gather(*tasks, return_exceptions=True)
        self._exchanges.clear()

    async def _discover_symbols(self) -> list[str]:
        log.info("Loading markets from all exchanges...")
        market_sets: dict[str, set[str]] = {}
        tasks = {
            name: asyncio.create_task(_load_markets_safe(ex, name))
            for name, ex in self._exchanges.items()
        }
        for name, task in tasks.items():
            market_sets[name] = await task

        if self._explicit_symbols:
            ref_symbols: set[str] = set()
            for name in self._reference_names:
                if name in market_sets:
                    ref_symbols |= market_sets[name]
            available = [s for s in self._explicit_symbols if s in ref_symbols]
            return available

        ref_symbols = set()
        for name in self._reference_names:
            if name in market_sets:
                ref_symbols |= market_sets[name]

        non_ref_symbols: set[str] = set()
        for name, syms in market_sets.items():
            if name not in self._reference_names:
                non_ref_symbols |= syms

        candidate = ref_symbols & non_ref_symbols

        filtered: list[str] = []
        for quote in self._quote_currencies:
            for sym in sorted(candidate):
                if sym.endswith(f"/{quote}") and sym not in filtered:
                    filtered.append(sym)

        if self._max_symbols > 0:
            filtered = filtered[: self._max_symbols]

        log.info("Discovered %d symbols for lag monitoring", len(filtered))
        return filtered

    @staticmethod
    def _get_reference_price(ref_states: dict[str, ExchangeState]) -> float | None:
        prices = [
            s.last_price for s in ref_states.values() if s.last_price is not None
        ]
        return statistics.median(prices) if prices else None

    async def _detect_lags(
        self,
        ref_states: dict[str, ExchangeState],
        lag_states: dict[str, ExchangeState],
        symbol: str,
    ) -> None:
        now = time.monotonic()
        ref_price = self._get_reference_price(ref_states)
        if ref_price is None:
            return

        old_ref_price: float | None = None
        cutoff = now - REFERENCE_WINDOW
        for rs in ref_states.values():
            p = rs.price_at(cutoff)
            if p:
                old_ref_price = p
                break
        if old_ref_price is None:
            return

        pct_move = (ref_price - old_ref_price) / old_ref_price * 100.0
        direction = 1 if pct_move > 0 else -1
        moving = abs(pct_move) >= self._price_move_pct

        for name, state in lag_states.items():
            if state.last_price is None:
                continue

            if not moving:
                state.reset_lag()
                continue

            if state.lag_start_ts is None:
                state.lag_start_ts = now - REFERENCE_WINDOW / 2
                state.lag_ref_price = old_ref_price
                state.lag_direction = direction

            ex_price = state.last_price
            ex_move_pct = (
                (ex_price - state.lag_ref_price) / state.lag_ref_price * 100.0
            )
            caught_up = ex_move_pct * state.lag_direction > self._price_move_pct * 0.5
            lag_sec = now - state.lag_start_ts
            dir_str = "UP" if state.lag_direction == 1 else "DOWN"

            if caught_up:
                if lag_sec >= self._lag_threshold and (now - state.last_alert_ts) > 5.0:
                    log.warning(
                        "LAG DETECTED  %-20s %-16s %s  ref_move=%+.3f%%  "
                        "ex_move=%+.3f%%  lag=%.2fs",
                        name, symbol, dir_str, pct_move, ex_move_pct, lag_sec,
                    )
                    await self._db.log_lag_event(
                        symbol=symbol,
                        exchange=name,
                        lag_seconds=lag_sec,
                        ref_price=ref_price,
                        exchange_price=ex_price,
                        price_move_pct=pct_move,
                        direction=dir_str,
                    )
                    await self._notifier.alert_lag(
                        symbol=symbol,
                        exchange=name,
                        lag_seconds=lag_sec,
                        ref_price=ref_price,
                        exchange_price=ex_price,
                        price_move_pct=pct_move,
                        direction=dir_str,
                    )
                    state.last_alert_ts = now
                state.reset_lag()
            else:
                if (
                    lag_sec >= self._lag_threshold
                    and (now - state.last_alert_ts) > self._lag_threshold
                ):
                    log.warning(
                        "LAGGING NOW   %-20s %-16s %s  ref=%.6g  ex=%.6g  "
                        "ref_move=%+.3f%%  lag=%.2fs (ongoing)",
                        name, symbol, dir_str,
                        ref_price, ex_price, pct_move, lag_sec,
                    )
                    state.last_alert_ts = now

    async def _fetch_loop(
        self,
        exchange: ccxt.Exchange,
        state: ExchangeState,
        symbol: str,
        stop_event: asyncio.Event,
    ) -> None:
        if not exchange.markets or symbol not in exchange.markets:
            return

        while not stop_event.is_set():
            t0 = time.monotonic()
            try:
                ticker = await exchange.fetch_ticker(symbol)
                price = ticker.get("last") or ticker.get("close")
                if price and float(price) > 0:
                    state.add_tick(
                        PriceTick(
                            ts=time.monotonic(),
                            price=float(price),
                            wall_ts=time.time(),
                        )
                    )
                    state.consecutive_errors = 0
            except (ccxt.NetworkError, ccxt.ExchangeError) as e:
                state.consecutive_errors += 1
                if state.consecutive_errors <= 3:
                    log.debug("[%s/%s] %s: %s", state.name, symbol, type(e).__name__, e)
            except Exception as e:
                state.consecutive_errors += 1
                if state.consecutive_errors <= 3:
                    log.debug("[%s/%s] Error: %s", state.name, symbol, e)

            elapsed = time.monotonic() - t0
            wait = max(0.0, self._fetch_interval - elapsed)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=wait)
            except asyncio.TimeoutError:
                pass

    async def _monitor_symbol(
        self,
        symbol: str,
        stop_event: asyncio.Event,
    ) -> None:
        states: dict[str, ExchangeState] = {
            name: ExchangeState(name=name) for name in self._exchanges
        }
        ref_states = {
            n: states[n] for n in self._reference_names if n in states
        }
        lag_states = {
            n: s for n, s in states.items() if n not in self._reference_names
        }

        fetch_tasks = [
            asyncio.create_task(
                self._fetch_loop(
                    self._exchanges[name], states[name], symbol, stop_event
                ),
                name=f"fetch_{name}_{symbol.replace('/', '_')}",
            )
            for name in self._exchanges
            if self._exchanges[name].markets
            and symbol in self._exchanges[name].markets
        ]

        if not fetch_tasks:
            return

        async def detection_loop() -> None:
            while not stop_event.is_set():
                await self._detect_lags(ref_states, lag_states, symbol)
                await asyncio.sleep(0.2)

        async def status_loop() -> None:
            while not stop_event.is_set():
                await asyncio.sleep(60)
                alive = sum(
                    1 for s in states.values() if s.last_price is not None
                )
                ref_p = self._get_reference_price(ref_states)
                log.info(
                    "[%s] alive: %d/%d  ref=%s",
                    symbol,
                    alive,
                    len(states),
                    f"{ref_p:.6g}" if ref_p else "---",
                )

        all_tasks = fetch_tasks + [
            asyncio.create_task(detection_loop()),
            asyncio.create_task(status_loop()),
        ]
        try:
            await asyncio.gather(*all_tasks)
        except asyncio.CancelledError:
            for t in all_tasks:
                t.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True)

    async def run(self, stop_event: asyncio.Event | None = None) -> None:
        """Main lag monitoring loop."""
        if stop_event is None:
            stop_event = asyncio.Event()

        await self._init_exchanges()
        if not self._exchanges:
            log.error("No exchanges initialized for lag monitor")
            return

        symbols = await self._discover_symbols()
        if not symbols:
            log.error("No symbols found for lag monitoring")
            await self._close_exchanges()
            return

        log.info(
            "Lag monitor started: %d symbols | lag>=%.2fs | move>=%.3f%%",
            len(symbols),
            self._lag_threshold,
            self._price_move_pct,
        )

        symbol_tasks = [
            asyncio.create_task(
                self._monitor_symbol(sym, stop_event),
                name=f"sym_{sym.replace('/', '_')}",
            )
            for sym in symbols
        ]

        try:
            await asyncio.gather(*symbol_tasks)
        except (asyncio.CancelledError, KeyboardInterrupt):
            log.info("Lag monitor stopping...")
        finally:
            stop_event.set()
            for t in symbol_tasks:
                t.cancel()
            await asyncio.gather(*symbol_tasks, return_exceptions=True)
            await self._close_exchanges()
            log.info("Lag monitor stopped")
