# Project Status

Last updated: 2026-08-29  
Repository: `DenysHor/spot-bot`  
Current version: `0.38.0`
Current implementation: chart axes, pending-order labels, and historical PAPER fill markers

## Non-negotiable safety rules

- `PAPER` is the default and only enabled execution mode.
- Do not enable `TESTNET` or `LIVE` without an explicit user request and a separate safety review.
- Never commit Binance keys, Railway secrets, Telegram tokens, dashboard passwords, or session secrets.
- Run a single application worker because Grid, DCA, and notification loops run in-process.
- Preserve the existing SQLite data model and migration-friendly `CREATE TABLE IF NOT EXISTS` approach.
- Enforce `RiskManager` before every automatic BUY.

## Production deployment

- Public dashboard: `https://spot-bot-production-be2c.up.railway.app/`
- Health check: `https://spot-bot-production-be2c.up.railway.app/health`
- Platform: Railway, region `EU West`.
- The region must remain outside the United States because Binance public market data was unavailable from Railway US West.
- Root `Dockerfile` starts exactly one Uvicorn worker and respects Railway's `PORT`.
- Persistent Railway Volume is mounted at `/data`.
- Production SQLite path is `/data/spot_bot.db`; startup backups are kept in `/data/backups`.
- Dashboard authentication uses an HttpOnly signed cookie and cloud-only Railway variables.
- Telegram credentials exist only in Railway Variables.
- GitHub source is `DenysHor/spot-bot`, branch `main`. If Railway misses a push, use `Settings -> Source -> Check for updates`.

Do not copy production secrets or the Railway SQLite database into Git.

## Known PAPER experiment state

At the time of this handoff, Railway was running one SOLUSDT Grid bot with:

- Budget: `1,000 USDT`
- Step: `0.5%`
- Levels: `8`
- Status: `RUNNING`

The first observed cycle completed successfully:

- BUY around `104.41`
- SELL around `104.99`
- Net cycle P&L around `+0.4193 USDT`
- Telegram delivered both fill notifications.

Treat these values as a historical handoff note. Read the live dashboard/API before assuming the bot is still in the same state.

## Implemented system

- Binance public price, ticker, and kline market data.
- Persistent PAPER portfolio, positions, trades, fees, and P&L.
- Automatic percentage Grid engine with paired SELL and replacement BUY levels.
- Smart DCA engine.
- SQLite recovery for portfolios, Grid/DCA bots, orders, events, and notification state.
- RiskManager enforcement and automatic pause after repeated engine errors.
- Backtesting, parameter optimizer, and 70/30 walk-forward validation.
- Password-protected responsive dashboard and CSV exports.
- Telegram fill, risk, error, auto-pause, test, and daily summary notifications.
- Persistent notification delivery log and deduplication across restarts.
- Actual 7/30-day Grid analytics.
- Start-aligned Buy & Hold benchmark, Grid return, excess return, realized drawdown, profit factor, fees, volume, cycles, and active time.
- Three-attempt retry for transient Binance price timeouts before recording an engine error.
- Typed exception details in SQLite events, Telegram alerts, and Railway logs.
- Readiness stays `COLLECTING_DATA` until at least 7 days and 20 completed cycles.
- Transparent P&L, drawdown, profit factor, win rate, excess-return, and fee-drag criteria.
- Weekly Telegram evaluation with no automatic parameter changes or LIVE promotion.
- GitHub Actions runs the backend test suite for pushes and pull requests.
- Optional Trailing Up recenters only unfilled BUY levels after price rises by two grid steps.
- Trailing Up never changes the budget or level cap and never moves an open paired SELL.
- Existing running bots can enable or disable Trailing Up from their dashboard card.
- Trailing Up is capped at three recenters per UTC day and resets automatically the next day.
- Telegram reports every recenter and the first daily limit event.
- Dashboard shows anchor price, next recenter trigger, last recenter time, daily usage, and post-recenter results.
- Comparison backtest runs Fixed Grid and Trailing Up on the same candles without changing PAPER state.
- Profile comparison ranks Range, Trailing, 10%/20%/30% trend hybrids, and Buy & Hold, then validates both the overall winner and recommended bot profile on the final 30% of candles.
- New PAPER bots can execute Range, Trailing, or Hybrid 10%/20%/30% profiles; AUTO uses validated profile evidence during an uptrend.
- Hybrid bots buy the selected trend allocation once, reserve the remainder for Grid levels, report both P&L components, and liquidate trend inventory safely when stopped.
- Performance analytics evaluates Hybrid Total Return against Buy & Hold and separately reports Grid/Trend P&L, returns, budgets, and fees.
- Fresh-bot Buy & Hold benchmarks use the exact Grid/Hybrid entry and latest bot price with identical round-trip fee assumptions instead of an hourly candle open.
- Grid bots reserve their remaining declared Grid budget against other Grid bots. If unreserved USDT is insufficient, only BUY execution pauses; SELL processing remains active and BUY resumes automatically with a 10% liquidity buffer.
- BUY-side pause/recovery is persisted, visible on bot cards, and emitted through the existing Telegram event channel. The default hard cap is 12 bots, while allocation and reserve limits remain authoritative.
- Market-chart order labels are separated from the price scale and collision-adjusted; historical fills outside the visible candle window no longer distort the chart scale.
- Historical level crossings fill at the configured grid price rather than a favorable candle extreme.
- Binance pair search exposes only active Spot pairs quoted in the configured USDT quote asset.
- Grid/DCA launch validates the selected pair against the cached Binance exchange catalog.
- The overview shows aggregate allocated budget, bot shortcuts, state-derived recommendations, and portfolio metrics.
- Each active Grid bot receives its own dynamic tab with filtered chart, state, events, analytics, readiness, and backtesting.

## Local setup on a new Windows computer

```powershell
git clone https://github.com/DenysHor/spot-bot.git
cd spot-bot\backend
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pytest -q
python -m uvicorn app.main:app --reload --port 8001
```

Local development uses a separate PAPER database. It does not contain the Railway portfolio or bot state.

## Before changing code

```powershell
git pull origin main
git status --short
cd backend
python -m pytest -q
```

Preserve unrelated user changes. Use small commits, run the full test suite, and check `git diff --check` before every push.

## Deployment workflow

1. Implement and test locally.
2. Commit to `main`.
3. Push to GitHub.
4. Confirm Railway deploys the exact new commit; otherwise use `Check for updates`.
5. Verify `/health` reports the new version.
6. Hard-refresh the dashboard and confirm Market data, Telegram, and the running bot state.

## Current validation milestone: v0.20

The evidence-based PAPER readiness gate is implemented without changing trading parameters automatically:

- `COLLECTING_DATA` until at least 7 days and 20 completed cycles.
- `PASSED` or `FAILED` based on net P&L, drawdown, profit factor, win rate, excess return, and fee drag.
- A transparent readiness score with individual criteria.
- A weekly Telegram evaluation report.
- Recommendations limited to continue, stop, or start a separate PAPER experiment.
- GitHub Actions test workflow before Railway deployment.

Trailing Up is intentionally conservative: it moves pending BUY levels closer to a rising market but does not chase with an immediate market BUY. Its recenter events and results must be evaluated in PAPER before considering a separate capped trend allocation.

Possible next milestone: manage separate named PAPER experiments so the fixed Grid and Trailing Up evidence never mix.

Candidate-pair analysis must remain point-in-time and use fresh Binance data. Never hard-code a pair recommendation into automatic execution.

Do not interpret one or a few profitable cycles as evidence that the strategy is ready for real funds.
