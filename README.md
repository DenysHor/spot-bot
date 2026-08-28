# Spot Bot

Binance Spot trading manager with three execution modes:

- `PAPER` — simulated trading on real Binance market data
- `TESTNET` — Binance Spot Testnet
- `LIVE` — real Binance Spot trading

> Current development version: **0.2.0**. Only PAPER execution is implemented. TESTNET/LIVE are planned and intentionally not enabled yet.

## Implemented

- Binance public Spot market data
- Paper portfolio starting from 10,000 USDT by default
- Simulated market BUY / SELL
- 0.1% simulated trading fee
- Average entry price
- Realized and unrealized P&L
- Trade journal
- Portfolio reset
- First deterministic Smart Grid planner
- Basic risk manager foundation
- API via FastAPI / Swagger
- Unit tests for paper trading and grid planning

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
```

Example Paper BUY body:

```json
{
  "symbol": "BTCUSDT",
  "quote_amount": 100
}
```

Example Smart Grid plan:

```json
{
  "symbol": "BTCUSDT",
  "budget_quote": 1000,
  "step_pct": 1.5,
  "levels_each_side": 4
}
```

## Tests

From `backend/`:

```bash
pytest -q
```

## Next milestone

1. Persist portfolio/trades in SQLite
2. Grid execution engine that reacts to live prices
3. RiskManager enforcement on every simulated BUY
4. Backtesting with historical candles and fees
5. Web dashboard
6. Smart DCA
7. Testnet execution
8. Live execution only after validation

## Safety

- Never commit `.env` or API secrets.
- Live API keys must have **withdrawals disabled**.
- Prefer IP restrictions for live keys.
- `PAPER` is the default mode.
- Never paste Binance Secret Key into chat, issues, logs, or commits.

This project is under active development.
