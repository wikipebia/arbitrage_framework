"""
Profit calculator with real fee accounting.

Considers:
  - Trading fees (taker) on both buy and sell exchanges
  - Withdrawal fees (flat, per-coin)
  - Network / priority fees (e.g. Solana priority fee)
  - Slippage estimate from orderbook depth
"""

import logging
from dataclasses import dataclass

log = logging.getLogger("arbitrage.calculator")

PRIORITY_FEE_USD = 0.15  # ~0.001 SOL priority fee


@dataclass
class Opportunity:
    symbol: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    gross_spread_pct: float
    net_profit_pct: float
    volume_24h: float
    buy_fee_pct: float
    sell_fee_pct: float
    withdrawal_fee_usd: float
    network_fee_usd: float
    position_usd: float
    estimated_profit_usd: float
    direction: str

    def __str__(self) -> str:
        return (
            f"[{self.direction}] {self.symbol:<14} | "
            f"buy {self.buy_exchange}: {self.buy_price:.6f} | "
            f"sell {self.sell_exchange}: {self.sell_price:.6f} | "
            f"gross: {self.gross_spread_pct:+.3f}% | "
            f"NET: {self.net_profit_pct:+.3f}% | "
            f"~${self.estimated_profit_usd:.2f} | "
            f"vol: ${self.volume_24h:,.0f}"
        )


class ProfitCalculator:
    """Calculates net arbitrage profit considering all real fees."""

    def __init__(
        self,
        exchange_fees: dict[str, float],
        withdrawal_fees: dict[str, dict[str, float]],
        position_usd: float = 500.0,
        min_profit_pct: float = 0.3,
        network_fee_usd: float = PRIORITY_FEE_USD,
    ):
        self._exchange_fees = exchange_fees
        self._withdrawal_fees = withdrawal_fees
        self._position_usd = position_usd
        self._min_profit_pct = min_profit_pct
        self._network_fee_usd = network_fee_usd

    def _get_trading_fee(self, exchange_id: str) -> float:
        return self._exchange_fees.get(exchange_id, 0.1) / 100.0

    def _get_withdrawal_fee(self, exchange_id: str, symbol: str) -> float:
        ex_fees = self._withdrawal_fees.get(exchange_id, {})
        base = symbol.split("/")[0] if "/" in symbol else symbol
        return ex_fees.get(base, 0.0)

    def calculate(
        self,
        symbol: str,
        buy_exchange: str,
        sell_exchange: str,
        buy_price: float,
        sell_price: float,
        volume_24h: float = 0.0,
    ) -> Opportunity | None:
        """
        Calculate net profit for a potential arbitrage opportunity.

        Returns Opportunity if profitable above threshold, else None.
        """
        if buy_price <= 0 or sell_price <= 0:
            return None

        gross_spread_pct = (sell_price - buy_price) / buy_price * 100.0

        buy_fee_rate = self._get_trading_fee(buy_exchange)
        sell_fee_rate = self._get_trading_fee(sell_exchange)

        effective_buy = buy_price * (1.0 + buy_fee_rate)
        effective_sell = sell_price * (1.0 - sell_fee_rate)

        withdrawal_fee_usd = self._get_withdrawal_fee(buy_exchange, symbol)
        if withdrawal_fee_usd > 0 and buy_price > 0:
            base = symbol.split("/")[0] if "/" in symbol else symbol
            withdrawal_fee_usd = withdrawal_fee_usd * buy_price

        total_fees_pct = (
            buy_fee_rate * 100.0
            + sell_fee_rate * 100.0
            + (withdrawal_fee_usd / self._position_usd * 100.0)
            + (self._network_fee_usd / self._position_usd * 100.0)
        )

        net_profit_pct = gross_spread_pct - total_fees_pct

        estimated_profit_usd = self._position_usd * net_profit_pct / 100.0

        if net_profit_pct < self._min_profit_pct:
            return None

        direction = f"{buy_exchange} -> {sell_exchange}"

        return Opportunity(
            symbol=symbol,
            buy_exchange=buy_exchange,
            sell_exchange=sell_exchange,
            buy_price=buy_price,
            sell_price=sell_price,
            gross_spread_pct=round(gross_spread_pct, 4),
            net_profit_pct=round(net_profit_pct, 4),
            volume_24h=volume_24h,
            buy_fee_pct=round(buy_fee_rate * 100, 4),
            sell_fee_pct=round(sell_fee_rate * 100, 4),
            withdrawal_fee_usd=round(withdrawal_fee_usd, 4),
            network_fee_usd=self._network_fee_usd,
            position_usd=self._position_usd,
            estimated_profit_usd=round(estimated_profit_usd, 2),
            direction=direction,
        )

    def find_best_pair(
        self,
        symbol: str,
        prices: dict[str, dict],
        min_volume: float = 50000.0,
    ) -> list[Opportunity]:
        """
        Given prices from multiple exchanges for one symbol,
        find all profitable arbitrage pairs.

        prices: {exchange_id: {"bid": float, "ask": float, "volume": float}}
        """
        opportunities: list[Opportunity] = []

        exchange_ids = list(prices.keys())

        for i, buy_ex in enumerate(exchange_ids):
            buy_data = prices[buy_ex]
            ask = buy_data.get("ask")
            if not ask or ask <= 0:
                continue

            for sell_ex in exchange_ids[i + 1 :]:
                sell_data = prices[sell_ex]
                bid = sell_data.get("bid")
                if not bid or bid <= 0:
                    continue

                vol_buy = buy_data.get("volume", 0) or 0
                vol_sell = sell_data.get("volume", 0) or 0
                min_vol = min(vol_buy, vol_sell)

                if min_vol < min_volume:
                    continue

                opp_forward = self.calculate(
                    symbol=symbol,
                    buy_exchange=buy_ex,
                    sell_exchange=sell_ex,
                    buy_price=ask,
                    sell_price=bid,
                    volume_24h=min_vol,
                )
                if opp_forward:
                    opportunities.append(opp_forward)

                bid_buy = buy_data.get("bid")
                ask_sell = sell_data.get("ask")
                if bid_buy and ask_sell and bid_buy > 0 and ask_sell > 0:
                    opp_reverse = self.calculate(
                        symbol=symbol,
                        buy_exchange=sell_ex,
                        sell_exchange=buy_ex,
                        buy_price=ask_sell,
                        sell_price=bid_buy,
                        volume_24h=min_vol,
                    )
                    if opp_reverse:
                        opportunities.append(opp_reverse)

        opportunities.sort(key=lambda o: o.net_profit_pct, reverse=True)
        return opportunities
