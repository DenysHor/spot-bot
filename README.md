# Spot Bot

Binance Spot trading manager with three execution modes:

- `PAPER` — simulated trading on real Binance market data
- `TESTNET` — Binance Spot Testnet
- `LIVE` — real Binance Spot trading

> Current development version: **0.28.0**. Automatic Grid and Smart DCA execution are PAPER-only. TESTNET/LIVE remain intentionally disabled.

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
- Optional PAPER-only Trailing Up that recenters unfilled BUY levels after a two-step upward breakout
- Existing inventory and paired SELL levels remain untouched during a Trailing Up recenter
- Per-bot Trailing Up toggle, recenter counter, events, and restart-safe SQLite state
- Three-per-day Trailing Up safety cap with automatic UTC-day reset
- Telegram alerts for recentering and reaching the daily recenter cap
- Dashboard anchor, next trigger, last recenter, daily usage, and post-recenter analytics
- Fixed Grid versus Trailing Up comparison on one identical historical candle dataset
- Level-crossing backtest fills that avoid unrealistically favorable candle-extreme execution
- Searchable catalog of active Binance Spot USDT pairs with server-side launch validation
- Portfolio overview plus dynamically generated per-Grid-bot dashboard tabs
- Bot-specific chart, budget, levels, events, analytics, readiness, recommendations, and backtests
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
- Four-profile historical comparison: Range Grid, Trailing Grid, 20% trend hybrid, and Buy & Hold
- 70/30 walk-forward profile validation that keeps training and validation periods separate
- Smart DCA with scheduled purchases and extra dip-triggered purchases
- RiskManager enforcement and SQLite recovery for every DCA bot and event
- Smart DCA controls, budget usage and purchase statistics in the dashboard
- Grid Optimizer comparing up to 30 parameter combinations on one candle dataset
- Risk-adjusted optimizer ranking with a low-cycle confidence penalty
- 70/30 walk-forward validation on a separate unseen candle period
- Monitoring dashboard with market-data health, last success time and event feed
- Per-Grid open exposure, unrealized P&L and total P&L
- Pause/resume controls and automatic safety pause after three consecutive engine errors
- CSV exports for PAPER trades and Grid/DCA events
- Protection against stopping a Grid bot while paired SELL levels remain open
- Password-protected dashboard with signed HttpOnly session cookies
- Docker deployment with a single execution worker and public health-check
- Persistent SQLite volume support and retained startup backups
- Optional Telegram alerts for fills, risk blocks, engine errors and automatic pauses
- Persistent notification delivery log, dashboard test button and one daily PAPER summary
- Actual 7/30-day Grid analytics with P&L curve, fees, cycles, win rate, volume and Buy & Hold comparison
- Start-aligned benchmark, Grid return on allocated budget, excess return, realized drawdown and profit factor
- Transient Binance price retry with typed Telegram errors and Railway traceback logging
- Evidence-gated PAPER readiness with minimum 7 days and 20 completed cycles
- Transparent readiness criteria, data progress, quality score and non-LIVE recommendations
- Weekly Telegram strategy evaluation and GitHub Actions test workflow
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
actions remain PAPER-only. The market selector includes BTC, ETH, BNB and SOL
quoted in USDT; the API also accepts other valid USDT Spot symbols.

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

`POST /api/backtest/grid/optimize` reuses one identical candle dataset for every
combination, so results are directly comparable. The dashboard default checks
steps `0.5, 1, 1.5, 2, 3%` with `4, 6, 8` levels. Ranking uses return divided by
`1 + max drawdown`, with a confidence penalty when fewer than three cycles were
completed. The score is a comparison aid, not a trading recommendation.

`POST /api/backtest/grid/walk-forward` first runs the optimizer on the initial
70% of candles, then tests only its selected parameters on the final unseen 30%.
The validation passes when the unseen period remains profitable and completes at
least one full cycle. Training and validation return, drawdown, fees and cycles
are reported separately to expose historical overfitting.

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
POST /api/backtest/grid/optimize
POST /api/backtest/grid/walk-forward
GET  /api/dca/bots
GET  /api/dca/bots/{bot_id}
POST /api/dca/bots/start
POST /api/dca/bots/{bot_id}/pause
POST /api/dca/bots/{bot_id}/resume
POST /api/dca/bots/{bot_id}/stop
POST /api/dca/bots/{bot_id}/tick
GET  /api/grid/bots
GET  /api/grid/bots/{bot_id}
POST /api/grid/bots/start
POST /api/grid/bots/{bot_id}/pause
POST /api/grid/bots/{bot_id}/resume
POST /api/grid/bots/{bot_id}/stop
POST /api/grid/bots/{bot_id}/tick
GET  /api/monitoring/status
GET  /api/export/trades.csv
GET  /api/export/events.csv
```

The `/tick` endpoint is only for debugging. Normal active grid bots are checked automatically by the background worker.

## Monitoring and safety

The dashboard shows market-data health, running/paused strategy counts and the
20 latest Grid/DCA events. A bot is automatically changed to `PAUSED` after
three consecutive engine errors and must be explicitly resumed. Pausing keeps
open levels and positions intact. A Grid bot with open paired SELL levels cannot
be stopped; pause it instead so its paper position is not silently orphaned.

Trades and bot events can be downloaded as CSV from the dashboard.

## Cloud deployment

The repository includes a production `Dockerfile`. It starts exactly one Uvicorn
worker because multiple workers would duplicate Grid/DCA background execution.
The public `/health` endpoint can be used by the hosting platform; dashboard and
management APIs require login when `DASHBOARD_PASSWORD` is configured.

For Railway or another Docker host:

1. Deploy this GitHub repository using its root `Dockerfile`.
2. Attach a persistent volume mounted at `/data`.
3. Configure the health-check path as `/health`.
4. Generate a public HTTPS domain.
5. Add these environment variables in the hosting dashboard:

```text
TRADING_MODE=PAPER
SQLITE_PATH=/data/spot_bot.db
SQLITE_BACKUP_COUNT=5
DASHBOARD_USERNAME=admin
DASHBOARD_PASSWORD=<unique password with at least 12 characters>
SESSION_SECRET=<random secret with at least 32 characters>
SECURE_COOKIES=true
TELEGRAM_BOT_TOKEN=<Telegram BotFather token, optional>
TELEGRAM_CHAT_ID=<Telegram chat id, optional>
DAILY_REPORT_HOUR_UTC=20
```

Never commit the real password or session secret. In cloud mode the app refuses
to start with a short password or missing session secret. Backups are written to
`/data/backups` at startup, with the newest `SQLITE_BACKUP_COUNT` files retained.
Local development remains password-free when `DASHBOARD_PASSWORD` is empty.

## Tests

From `backend/`:

```bash
pytest -q
```

## Next milestone

1. Telegram notifications
2. Binance Spot Testnet execution
3. Live execution only after validation

## Safety

- Never commit `.env` or API secrets.
- Live API keys must have **withdrawals disabled**.
- Prefer IP restrictions for live keys.
- `PAPER` is the default mode.
- Never paste Binance Secret Key into chat, issues, logs, or commits.

This project is under active development.
