# Spot Bot

Binance Spot trading manager with three execution modes:

- `PAPER` — simulated trading on real market data
- `TESTNET` — Binance Spot Testnet
- `LIVE` — real Binance Spot trading

## First milestone

1. Market data from Binance
2. Paper portfolio and order simulator
3. Smart Grid strategy
4. Smart DCA strategy
5. Risk manager
6. Backtesting
7. Web dashboard
8. Testnet
9. Live trading only after validation

## Safety

- Never commit `.env` or API secrets.
- Live API keys must have **withdrawals disabled**.
- Prefer IP restrictions for live keys.
- `PAPER` is the default mode.

## Planned structure

```text
backend/
  app/
    api/
    core/
    exchange/
    strategies/
    risk/
    backtest/
    models/
frontend/
tests/
```

This project is under active development.
