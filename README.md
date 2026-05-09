# Arbitrage Framework

Multi-exchange cryptocurrency arbitrage scanner with real-time spread detection, price lag monitoring, and Telegram alerts.

## Features

- **Spread Scanner** — monitors 15+ CEX exchanges via CCXT for spread-based arbitrage
- **Lag Monitor** — detects price update delays between reference and smaller exchanges
- **Real Fee Calculation** — trading fees, withdrawal fees, network fees
- **Liquidity Filter** — ignores pairs with 24h volume below configurable threshold ($50k default)
- **Telegram Alerts** — instant notifications with spread, exchanges, profit estimate, volume
- **SQLite Logging** — all opportunities and lag events stored for analysis
- **Retry Logic** — exponential backoff, rate limit handling per exchange
- **Async Architecture** — asyncio + ccxt.async_support for maximum throughput

## Project Structure

```
arbitrage_framework/
├── main.py                          # Entry point — launches everything
├── config.json                      # All configuration in one place
├── requirements.txt                 # Python dependencies
├── scanner/
│   ├── spread_scanner.py            # Multi-exchange CEX spread scanner
│   └── lag_monitor.py               # Price lag detection between exchanges
├── calculator/
│   └── profit_calculator.py         # Net profit calculation with real fees
├── notifications/
│   └── telegram_bot.py              # Telegram bot for alerts
└── storage/
    └── db.py                        # SQLite logging
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Edit config.json — set your exchanges, Telegram bot token, thresholds
# Then run:
python main.py
```

## Usage

```bash
# Run both scanner and lag monitor
python main.py

# Run only spread scanner
python main.py --scanner-only

# Run only lag monitor
python main.py --lag-only

# Custom config file
python main.py --config my_config.json

# Debug logging
python main.py --debug
```

## Configuration

Edit `config.json`:

- **exchanges** — list of exchanges with API keys, fees, enabled/disabled
- **scanner** — spread thresholds, volume filters, scan interval
- **lag_monitor** — reference exchanges, lag threshold, price movement sensitivity
- **telegram** — bot token, chat ID, alert thresholds
- **database** — SQLite file path
- **retry** — retry count and delay settings

### Telegram Setup

1. Create a bot via [@BotFather](https://t.me/BotFather)
2. Get your chat ID via [@userinfobot](https://t.me/userinfobot)
3. Set `telegram.enabled` to `true` in config.json
4. Set `telegram.bot_token` and `telegram.chat_id`

## Supported Exchanges

binance, bybit, bitget, gateio, kucoin, mexc, huobi, coinex, cryptocom, coinbase, exmo, hitbtc, bitrue, okx, poloniex — and any other exchange supported by CCXT.

## Requirements

- Python 3.11+
- ccxt >= 4.0
- aiohttp >= 3.9
- aiosqlite >= 0.19
