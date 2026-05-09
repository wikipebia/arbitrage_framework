"""
Telegram notification bot for arbitrage alerts.
Uses raw HTTP API via aiohttp (no extra dependencies).
"""

import asyncio
import logging
import time

import aiohttp

log = logging.getLogger("arbitrage.telegram")


class TelegramNotifier:
    API_BASE = "https://api.telegram.org/bot{token}"

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        min_profit_pct: float = 0.5,
        cooldown_sec: float = 60.0,
        enabled: bool = True,
    ):
        self._token = bot_token
        self._chat_id = chat_id
        self._min_profit = min_profit_pct
        self._cooldown = cooldown_sec
        self._enabled = enabled and bool(bot_token) and bool(chat_id)
        self._last_alerts: dict[str, float] = {}
        self._session: aiohttp.ClientSession | None = None

    async def start(self) -> None:
        if not self._enabled:
            log.info("Telegram notifications disabled")
            return
        self._session = aiohttp.ClientSession()
        me = await self._api_call("getMe")
        if me and me.get("ok"):
            username = me["result"].get("username", "?")
            log.info("Telegram bot connected: @%s", username)
        else:
            log.warning("Telegram bot token invalid, disabling notifications")
            self._enabled = False

    async def stop(self) -> None:
        if self._session:
            await self._session.close()
            self._session = None

    async def _api_call(
        self, method: str, data: dict | None = None
    ) -> dict | None:
        if not self._session:
            return None
        url = f"{self.API_BASE.format(token=self._token)}/{method}"
        try:
            async with self._session.post(
                url,
                json=data or {},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                return await resp.json()
        except Exception as e:
            log.debug("Telegram API error (%s): %s", method, e)
            return None

    def _should_alert(self, key: str) -> bool:
        now = time.time()
        last = self._last_alerts.get(key, 0)
        if now - last < self._cooldown:
            return False
        self._last_alerts[key] = now
        return True

    async def send_message(self, text: str) -> bool:
        if not self._enabled:
            return False
        result = await self._api_call(
            "sendMessage",
            {
                "chat_id": self._chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
        )
        return bool(result and result.get("ok"))

    async def alert_opportunity(
        self,
        symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        buy_price: float,
        sell_price: float,
        spread_pct: float,
        net_profit_pct: float,
        volume_24h: float = 0.0,
        estimated_profit_usd: float = 0.0,
        direction: str = "",
    ) -> None:
        if not self._enabled:
            return
        if net_profit_pct < self._min_profit:
            return

        alert_key = f"{symbol}:{buy_exchange}:{sell_exchange}"
        if not self._should_alert(alert_key):
            log.debug("Cooldown active for %s", alert_key)
            return

        vol_str = f"${volume_24h:,.0f}" if volume_24h else "N/A"
        text = (
            f"<b>ARBITRAGE {direction}</b>\n"
            f"\n"
            f"<b>{symbol}</b>\n"
            f"Buy:  <code>{buy_exchange}</code> @ <code>{buy_price:.6f}</code>\n"
            f"Sell: <code>{sell_exchange}</code> @ <code>{sell_price:.6f}</code>\n"
            f"\n"
            f"Spread:     <b>{spread_pct:+.3f}%</b>\n"
            f"Net profit: <b>{net_profit_pct:+.3f}%</b>\n"
            f"Est. profit: <b>${estimated_profit_usd:.2f}</b>\n"
            f"Volume 24h: {vol_str}\n"
        )
        ok = await self.send_message(text)
        if ok:
            log.info("TG alert sent: %s %s -> %s  NET %.3f%%", symbol, buy_exchange, sell_exchange, net_profit_pct)
        else:
            log.warning("TG alert FAILED: %s", symbol)

    async def alert_lag(
        self,
        symbol: str,
        exchange: str,
        lag_seconds: float,
        ref_price: float,
        exchange_price: float,
        price_move_pct: float,
        direction: str = "",
    ) -> None:
        if not self._enabled:
            return

        alert_key = f"lag:{symbol}:{exchange}"
        if not self._should_alert(alert_key):
            return

        log.info("TG lag alert: %s on %s  lag=%.2fs", symbol, exchange, lag_seconds)

        text = (
            f"<b>LAG DETECTED {direction}</b>\n"
            f"\n"
            f"<b>{symbol}</b> on <code>{exchange}</code>\n"
            f"Lag: <b>{lag_seconds:.2f}s</b>\n"
            f"Ref price:      <code>{ref_price:.6g}</code>\n"
            f"Exchange price: <code>{exchange_price:.6g}</code>\n"
            f"Ref move: <b>{price_move_pct:+.3f}%</b>\n"
        )
        await self.send_message(text)

    async def alert_startup(self, exchanges: list[str], symbols_count: int) -> None:
        if not self._enabled:
            return
        text = (
            f"<b>Arbitrage Scanner Started</b>\n"
            f"\n"
            f"Exchanges: {len(exchanges)}\n"
            f"Symbols: {symbols_count}\n"
            f"Exchanges: <code>{', '.join(exchanges)}</code>\n"
        )
        await self.send_message(text)

    async def alert_summary(self, stats: dict) -> None:
        if not self._enabled:
            return
        text = (
            f"<b>24h Summary</b>\n"
            f"\n"
            f"Opportunities found: {stats.get('total_opportunities_24h', 0)}\n"
            f"Avg profit: {stats.get('avg_profit_pct', 0):.3f}%\n"
            f"Max profit: {stats.get('max_profit_pct', 0):.3f}%\n"
            f"Total est. profit: ${stats.get('total_estimated_profit', 0):.2f}\n"
        )
        await self.send_message(text)
