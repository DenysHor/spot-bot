# Spot Bot

Binance Spot trading manager with three execution modes:

- `PAPER` — simulated trading on real Binance market data
- `TESTNET` — Binance Spot Testnet
- `LIVE` — real Binance Spot trading

> Current development version: **0.7.0**. Automatic Grid and Smart DCA execution are PAPER-only. TESTNET/LIVE remain intentionally disabled.

## Implemented

- Binance public Spot market data
- Paper portfolio starting from 10,000 USDT by default
- Simulated market BUY / SELL
- 0.1% simulated trading fee
- Average entry price
- Realized and unrealized P&L
- Trade journal
- Portfolio reset
- Smart Grid planner
- Automatic Paper Grid execution worker
- Initial BUY levels below the market
- Paired SELL creation after every filled BUY
- Replacement BUY creation after a completed SELL
- Per-grid realized cycle P&L and completed-cycle count
- Configurable market polling interval (`GRID_POLL_SECONDS`, default 5 sec)
- One running grid bot per symbol in v0.3 to prevent position conflicts
- SQLite persistence for the paper portfolio, positions, trades, grid bots, levels and events
- Automatic restoration of active bots and paper balances after a server restart
- RiskManager enforcement before every automatic grid BUY
- Historical Grid backtesting on up to 1,000 public Binance candles
- Backtest report with net return, realized/unrealized P&L, fees, drawdown, cycles, trades, equity curve and buy-and-hold comparison
- Responsive web dashboard with portfolio metrics, PAPER bot controls and bot cards
- Binance candlestick chart with active BUY/SELL grid-level overlays
- Browser-based historical backtest form and performance summary
- Smart DCA with scheduled purchases and extra dip-triggered purchases
- RiskManager enforcement and SQLite recovery for every DCA bot and event
- Smart DCA controls, budget usage and purchase statistics in the dashboard
- API via FastAPI / Swagger
- Unit tests for paper trading, grid planning and grid execution cycles

## Run locally

```bash
cd backend
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Linux/macOS:

```bash
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Open Swagger:

```text
http://127.0.0.1:8000/docs
```

Open the dashboard:

```text
http://127.0.0.1:8000/
```

The dashboard is served directly by FastAPI and requires no separate frontend
build. It refreshes portfolio and bot state automatically, while all trading
actions remain PAPER-only.

## Smart DCA

Smart DCA makes a fixed PAPER purchase on the configured interval. After the
first fill, it can also buy early when price falls by `dip_trigger_pct` from the
last DCA fill. Every order is checked against the remaining bot budget and the
same portfolio allocation, position and quote-reserve risk limits used by Grid.

Example: a 1,000 USDT bot buying 100 USDT daily with a 5% dip trigger can be
created from the dashboard or `POST /api/dca/bots/start`:

```json
{
  "symbol": "BTCUSDT",
  "budget_quote": 1000,
  "order_quote": 100,
  "interval_minutes": 1440,
  "dip_trigger_pct": 5
}
```

The first scheduled BUY becomes due immediately. DCA is PAPER-only, persists
across restarts, stops automatically when the remaining budget cannot cover a
full order plus fee, and records `BUY_FILLED`, `BUY_BLOCKED`, `BUDGET_COMPLETED`
and engine lifecycle events.

## Grid workflow

Start a bot:

`POST /api/grid/bots/start`

```json
{
  "symbol": "BTCUSDT",
  "budget_quote": 1000,
  "step_pct": 1.5,
  "levels_each_side": 4
}
```

The server then polls the real Binance price in the background. If price crosses a BUY trigger, the PaperBroker records a simulated BUY and creates a paired SELL one grid step above the fill. When that SELL is crossed, the bot records the cycle P&L and creates a replacement BUY one step below the sell fill.

Before every automatic BUY, RiskManager enforces `MAX_PORTFOLIO_ALLOCATION_PCT`,
`MAX_POSITION_PCT`, and `RESERVE_USDT_PCT`. A rejected order remains open and a
persisted `BUY_BLOCKED` event records the reason and currently allowed maximum.

## Persistence

PAPER state is stored in SQLite at `SQLITE_PATH` (`data/spot_bot.db` by default,
relative to the process working directory). The schema uses plain tables and a
`schema_version` row so future migrations can stay incremental. On startup the
portfolio, trade journal, positions, grid bots, open levels and event history are
restored automatically. `POST /api/paper/reset` clears both persisted portfolio
and grid state and creates a fresh paper balance.

## Historical Grid backtest

`POST /api/backtest/grid` downloads public Binance candles and runs an isolated
simulation. It does not change the SQLite database, paper portfolio, or active bots.

```json
{
  "symbol": "BTCUSDT",
  "budget_quote": 1000,
  "step_pct": 1.5,
  "levels_each_side": 4,
  "interval": "1h",
  "limit": 500
}
```

The result includes portfolio and trade details, completed grid cycles, all fees,
maximum drawdown, an equity curve, and a buy-and-hold benchmark. Binance OHLC
candles do not reveal the exact tick order inside each candle. For reproducibility,
the simulator assumes `open → low → high → close` for bullish candles and
`open → high → low → close` for bearish candles. Backtest results are estimates,
not promises of future performance.

## Useful endpoints

```text
GET  /health
GET  /api/market/BTCUSDT
GET  /api/market/BTCUSDT/klines?interval=1h&limit=120
GET  /api/paper/portfolio
GET  /api/paper/trades
POST /api/paper/buy
POST /api/paper/sell
POST /api/paper/reset
POST /api/grid/plan
POST /api/backtest/grid
GET  /api/dca/bots
GET  /api/dca/bots/{bot_id}
POST /api/dca/bots/start
POST /api/dca/bots/{bot_id}/stop
POST /api/dca/bots/{bot_id}/tick
GET  /api/grid/bots
GET  /api/grid/bots/{bot_id}
POST /api/grid/bots/start
POST /api/grid/bots/{bot_id}/stop
POST /api/grid/bots/{bot_id}/tick
```

The `/tick` endpoint is only for debugging. Normal active grid bots are checked automatically by the background worker.

## Tests

From `backend/`:

```bash
pytest -q
```

## Next milestone

1. Automated Grid parameter comparison
2. Telegram notifications
3. Binance Spot Testnet execution
4. Live execution only after validation

## Safety

- Never commit `.env` or API secrets.
- Live API keys must have **withdrawals disabled**.
- Prefer IP restrictions for live keys.
- `PAPER` is the default mode.
- Never paste Binance Secret Key into chat, issues, logs, or commits.

This project is under active development.
