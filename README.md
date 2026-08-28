# Spot Bot

Binance Spot trading manager with three execution modes:

- `PAPER` — simulated trading on real Binance market data
- `TESTNET` — Binance Spot Testnet
- `LIVE` — real Binance Spot trading

> Current development version: **0.3.0**. Automatic Grid execution is PAPER-only. TESTNET/LIVE remain intentionally disabled.

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
- Basic risk manager foundation
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

## Useful endpoints

```text
GET  /health
GET  /api/market/BTCUSDT
GET  /api/paper/portfolio
GET  /api/paper/trades
POST /api/paper/buy
POST /api/paper/sell
POST /api/paper/reset
POST /api/grid/plan
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

1. SQLite persistence so bots survive server restarts
2. Enforce RiskManager on every automatic BUY
3. Historical candle backtesting with fees
4. Web dashboard with bot cards, chart, levels and P&L
5. Smart DCA
6. Telegram notifications
7. Binance Spot Testnet execution
8. Live execution only after validation

## Safety

- Never commit `.env` or API secrets.
- Live API keys must have **withdrawals disabled**.
- Prefer IP restrictions for live keys.
- `PAPER` is the default mode.
- Never paste Binance Secret Key into chat, issues, logs, or commits.

This project is under active development.
