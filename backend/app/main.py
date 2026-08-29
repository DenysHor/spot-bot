import csv
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime
from io import StringIO
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.backtest.grid import GridBacktester
from app.analytics.performance import grid_performance
from app.core.config import settings
from app.core.auth import SessionAuth, validate_cloud_security
from app.dca.execution import DcaExecutionEngine
from app.exchange.binance_public import BinancePublicClient
from app.grid.execution import GridExecutionEngine
from app.notifications.telegram import TelegramNotifier
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.persistence.sqlite import SQLiteStore
from app.risk.manager import RiskLimits, RiskManager
from app.strategies.smart_grid import SmartGrid

market = BinancePublicClient()
auth = SessionAuth(
    settings.dashboard_username, settings.dashboard_password,
    settings.session_secret, settings.secure_cookies,
)
store = SQLiteStore(settings.sqlite_path)
portfolio = PaperPortfolio(
    starting_quote=settings.paper_start_balance,
    quote_asset=settings.quote_asset,
    store=store,
)
broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)
grid = SmartGrid()
backtester = GridBacktester(fee_rate=0.001)
market_health = {"last_success_at": "", "last_error": ""}
risk_manager = RiskManager(RiskLimits(
    max_portfolio_allocation_pct=settings.max_portfolio_allocation_pct,
    max_position_pct=settings.max_position_pct,
    reserve_quote_pct=settings.reserve_usdt_pct,
))


async def current_price(symbol: str) -> float:
    try:
        data = await market.price(symbol.upper())
        market_health["last_success_at"] = PaperPortfolio.now_iso()
        market_health["last_error"] = ""
        return float(data["price"])
    except Exception as exc:
        market_health["last_error"] = str(exc)
        raise


async def current_klines(symbol: str, interval: str, limit: int) -> list:
    try:
        rows = await market.klines(symbol.upper(), interval=interval, limit=limit)
        market_health["last_success_at"] = PaperPortfolio.now_iso()
        market_health["last_error"] = ""
        return rows
    except Exception as exc:
        market_health["last_error"] = str(exc)
        raise


grid_engine = GridExecutionEngine(
    broker=broker,
    price_provider=current_price,
    poll_seconds=getattr(settings, "grid_poll_seconds", 5.0),
    store=store,
    risk_manager=risk_manager,
)
dca_engine = DcaExecutionEngine(
    broker=broker, price_provider=current_price, risk_manager=risk_manager,
    poll_seconds=settings.grid_poll_seconds, store=store,
)
notifier = TelegramNotifier(
    settings.telegram_bot_token, settings.telegram_chat_id, grid_engine, dca_engine,
    portfolio=portfolio, store=store, poll_seconds=settings.notification_poll_seconds,
    daily_report_hour_utc=settings.daily_report_hour_utc,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    validate_cloud_security(settings.secure_cookies, settings.dashboard_password, settings.session_secret)
    app.state.last_backup = store.create_backup(settings.sqlite_backup_count)
    if settings.trading_mode == "PAPER":
        grid_engine.start_background()
        dca_engine.start_background()
        notifier.start_background()
    yield
    await notifier.stop_background()
    await grid_engine.stop_background()
    await dca_engine.stop_background()


app = FastAPI(title="Spot Bot API", version="0.15.0", lifespan=lifespan)
static_dir = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.middleware("http")
async def require_authentication(request: Request, call_next):
    path = request.url.path
    public = path == "/" or path == "/health" or path.startswith("/static/") or path.startswith("/api/auth/")
    if not auth.enabled or public:
        return await call_next(request)
    if not auth.verify_token(request.cookies.get(auth.cookie_name)):
        return JSONResponse(status_code=401, content={"detail": "Authentication required"})
    return await call_next(request)


class PaperBuyRequest(BaseModel):
    symbol: str = "BTCUSDT"
    quote_amount: float = Field(gt=0)


class PaperSellRequest(BaseModel):
    symbol: str = "BTCUSDT"
    quantity: float = Field(gt=0)


class GridPlanRequest(BaseModel):
    symbol: str = "BTCUSDT"
    budget_quote: float = Field(default=1000.0, gt=0)
    step_pct: float = Field(default=1.5, gt=0, le=25)
    levels_each_side: int = Field(default=4, ge=1, le=50)


class GridStartRequest(GridPlanRequest):
    pass


class GridBacktestRequest(GridPlanRequest):
    interval: str = Field(default="1h", pattern=r"^(1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)$")
    limit: int = Field(default=500, ge=2, le=1000)


class GridOptimizeRequest(BaseModel):
    symbol: str = "BTCUSDT"
    budget_quote: float = Field(default=1000.0, gt=0)
    interval: str = Field(default="1h", pattern=r"^(1m|3m|5m|15m|30m|1h|2h|4h|6h|8h|12h|1d|3d|1w|1M)$")
    limit: int = Field(default=500, ge=2, le=1000)
    step_pcts: list[float] = Field(default_factory=lambda: [0.5, 1.0, 1.5, 2.0, 3.0], min_length=1, max_length=10)
    levels_options: list[int] = Field(default_factory=lambda: [4, 6, 8], min_length=1, max_length=10)


class GridWalkForwardRequest(GridOptimizeRequest):
    training_pct: float = Field(default=70.0, ge=50, le=90)


class DcaStartRequest(BaseModel):
    symbol: str = "BTCUSDT"
    budget_quote: float = Field(default=1000.0, gt=0)
    order_quote: float = Field(default=100.0, gt=0)
    interval_minutes: int = Field(default=1440, ge=1, le=525600)
    dip_trigger_pct: float = Field(default=5.0, gt=0, le=50)


class LoginRequest(BaseModel):
    username: str
    password: str


@app.get("/", include_in_schema=False)
async def dashboard() -> FileResponse:
    return FileResponse(static_dir / "index.html")


@app.get("/api/auth/status", include_in_schema=False)
async def auth_status(request: Request) -> dict:
    authenticated = not auth.enabled or auth.verify_token(request.cookies.get(auth.cookie_name))
    return {"auth_enabled": auth.enabled, "authenticated": authenticated}


@app.post("/api/auth/login", include_in_schema=False)
async def login(request: LoginRequest, response: Response) -> dict:
    if not auth.enabled:
        return {"authenticated": True, "auth_enabled": False}
    if not auth.valid_credentials(request.username, request.password):
        raise HTTPException(status_code=401, detail="Invalid username or password")
    response.set_cookie(
        auth.cookie_name, auth.create_token(), max_age=86400, httponly=True,
        secure=settings.secure_cookies, samesite="strict", path="/",
    )
    return {"authenticated": True, "auth_enabled": True}


@app.post("/api/auth/logout", include_in_schema=False)
async def logout(response: Response) -> dict:
    response.delete_cookie(auth.cookie_name, path="/")
    return {"authenticated": False}


def base_asset_from_symbol(symbol: str) -> str:
    symbol = symbol.upper()
    quote = settings.quote_asset.upper()
    if not symbol.endswith(quote):
        raise ValueError(f"Only {quote}-quoted symbols are supported")
    base = symbol[: -len(quote)]
    if not base:
        raise ValueError("Invalid symbol")
    return base


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": "0.15.0",
        "trading_mode": settings.trading_mode,
        "live_trading_enabled": settings.trading_mode == "LIVE",
        "grid_background_worker": settings.trading_mode == "PAPER",
        "authentication_enabled": auth.enabled,
    }


@app.get("/api/market/{symbol}")
async def market_snapshot(symbol: str) -> dict:
    try:
        price = await current_price(symbol)
        ticker = await market.ticker_24h(symbol)
        return {
            "symbol": symbol.upper(),
            "price": price,
            "change_24h_pct": float(ticker["priceChangePercent"]),
            "volume_24h": float(ticker["volume"]),
            "quote_volume_24h": float(ticker["quoteVolume"]),
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Binance market-data error: {exc}") from exc


@app.get("/api/market/{symbol}/klines")
async def market_klines(symbol: str, interval: str = "1h", limit: int = 120) -> dict:
    allowed_intervals = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}
    if interval not in allowed_intervals:
        raise HTTPException(status_code=400, detail="Unsupported Binance interval")
    if limit < 2 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 2 and 500")
    try:
        rows = await current_klines(symbol, interval=interval, limit=limit)
        return {"symbol": symbol.upper(), "interval": interval, "candles": [
            {
                "open_time": int(row[0]), "open": float(row[1]), "high": float(row[2]),
                "low": float(row[3]), "close": float(row[4]), "close_time": int(row[6]),
            }
            for row in rows
        ]}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Binance kline error: {exc}") from exc


@app.get("/api/paper/portfolio")
async def paper_portfolio() -> dict:
    prices: dict[str, float] = {}
    for asset, position in portfolio.positions.items():
        if position.quantity <= 0:
            continue
        symbol = f"{asset}{settings.quote_asset}"
        try:
            prices[asset] = await current_price(symbol)
        except Exception:
            prices[asset] = position.avg_price
    return portfolio.snapshot(prices)


@app.get("/api/paper/trades")
async def paper_trades() -> dict:
    return {"trades": portfolio.trade_history()}


@app.post("/api/paper/reset")
async def paper_reset() -> dict:
    portfolio.reset()
    grid_engine.reset()
    dca_engine.reset()
    return {"status": "reset", "portfolio": portfolio.snapshot(), "grid_bots": [], "dca_bots": []}


@app.post("/api/paper/buy")
async def paper_buy(request: PaperBuyRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Paper endpoints require TRADING_MODE=PAPER")
    try:
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        price = await current_price(symbol)
        trade = broker.market_buy(symbol, base_asset, price, request.quote_amount)
        return {"trade": asdict(trade), "portfolio": portfolio.snapshot({base_asset: price})}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Paper BUY failed: {exc}") from exc


@app.post("/api/paper/sell")
async def paper_sell(request: PaperSellRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Paper endpoints require TRADING_MODE=PAPER")
    try:
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        price = await current_price(symbol)
        trade = broker.market_sell(symbol, base_asset, price, request.quantity)
        return {"trade": asdict(trade), "portfolio": portfolio.snapshot({base_asset: price})}
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Paper SELL failed: {exc}") from exc


@app.post("/api/grid/plan")
async def grid_plan(request: GridPlanRequest) -> dict:
    try:
        symbol = request.symbol.upper()
        price = await current_price(symbol)
        plan = grid.build_plan(
            symbol=symbol,
            reference_price=price,
            budget_quote=request.budget_quote,
            step_pct=request.step_pct,
            levels_each_side=request.levels_each_side,
        )
        return asdict(plan)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid plan failed: {exc}") from exc


@app.post("/api/backtest/grid")
async def grid_backtest(request: GridBacktestRequest) -> dict:
    """Run an isolated historical simulation without changing persisted PAPER state."""
    try:
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        candles = await current_klines(symbol, interval=request.interval, limit=request.limit)
        return await backtester.run(
            symbol=symbol,
            base_asset=base_asset,
            raw_candles=candles,
            budget_quote=request.budget_quote,
            step_pct=request.step_pct,
            levels_each_side=request.levels_each_side,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid backtest failed: {exc}") from exc


@app.post("/api/backtest/grid/optimize")
async def grid_optimize(request: GridOptimizeRequest) -> dict:
    """Compare Grid parameters on one identical candle dataset without mutating PAPER state."""
    try:
        if any(step <= 0 or step > 25 for step in request.step_pcts):
            raise ValueError("Every step_pct must be between 0 and 25")
        if any(levels < 1 or levels > 50 for levels in request.levels_options):
            raise ValueError("Every levels option must be between 1 and 50")
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        candles = await current_klines(symbol, interval=request.interval, limit=request.limit)
        return await backtester.optimize(
            symbol=symbol, base_asset=base_asset, raw_candles=candles,
            budget_quote=request.budget_quote, step_pcts=request.step_pcts,
            levels_options=request.levels_options,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid optimization failed: {exc}") from exc


@app.post("/api/backtest/grid/walk-forward")
async def grid_walk_forward(request: GridWalkForwardRequest) -> dict:
    """Select parameters on training candles, then validate on unseen candles."""
    try:
        if any(step <= 0 or step > 25 for step in request.step_pcts):
            raise ValueError("Every step_pct must be between 0 and 25")
        if any(levels < 1 or levels > 50 for levels in request.levels_options):
            raise ValueError("Every levels option must be between 1 and 50")
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        candles = await current_klines(symbol, interval=request.interval, limit=request.limit)
        return await backtester.walk_forward(
            symbol=symbol, base_asset=base_asset, raw_candles=candles,
            budget_quote=request.budget_quote, step_pcts=request.step_pcts,
            levels_options=request.levels_options, training_pct=request.training_pct,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Walk-forward validation failed: {exc}") from exc


@app.get("/api/dca/bots")
async def dca_bots() -> dict:
    return {"bots": dca_engine.list_bots()}


@app.get("/api/dca/bots/{bot_id}")
async def dca_bot(bot_id: str) -> dict:
    try:
        return dca_engine.get_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/dca/bots/start")
async def dca_start(request: DcaStartRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="DCA execution v0.7 is PAPER-only")
    try:
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        price = await current_price(symbol)
        return dca_engine.start_bot(
            symbol=symbol, base_asset=base_asset, reference_price=price,
            budget_quote=request.budget_quote, order_quote=request.order_quote,
            interval_seconds=request.interval_minutes * 60,
            dip_trigger_pct=request.dip_trigger_pct,
        ).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"DCA start failed: {exc}") from exc


@app.post("/api/dca/bots/{bot_id}/stop")
async def dca_stop(bot_id: str) -> dict:
    try:
        return dca_engine.stop_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/dca/bots/{bot_id}/pause")
async def dca_pause(bot_id: str) -> dict:
    try:
        return dca_engine.pause_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/dca/bots/{bot_id}/resume")
async def dca_resume(bot_id: str) -> dict:
    try:
        return dca_engine.resume_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/dca/bots/{bot_id}/tick")
async def dca_tick(bot_id: str) -> dict:
    try:
        return (await dca_engine.tick_bot(bot_id)).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"DCA tick failed: {exc}") from exc


@app.get("/api/grid/bots")
async def grid_bots() -> dict:
    return {"bots": grid_engine.list_bots()}


@app.get("/api/grid/bots/{bot_id}")
async def grid_bot(bot_id: str) -> dict:
    try:
        return grid_engine.get_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc


@app.post("/api/grid/bots/start")
async def grid_start(request: GridStartRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Grid execution v0.3 is PAPER-only")
    try:
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        price = await current_price(symbol)
        bot = grid_engine.start_bot(
            symbol=symbol,
            base_asset=base_asset,
            reference_price=price,
            budget_quote=request.budget_quote,
            step_pct=request.step_pct,
            levels_each_side=request.levels_each_side,
        )
        return bot.snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid start failed: {exc}") from exc


@app.post("/api/grid/bots/{bot_id}/stop")
async def grid_stop(bot_id: str) -> dict:
    try:
        return grid_engine.stop_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/grid/bots/{bot_id}/pause")
async def grid_pause(bot_id: str) -> dict:
    try:
        return grid_engine.pause_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/grid/bots/{bot_id}/resume")
async def grid_resume(bot_id: str) -> dict:
    try:
        return grid_engine.resume_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/grid/bots/{bot_id}/tick")
async def grid_tick(bot_id: str) -> dict:
    """Manual one-shot tick for debugging; background polling runs automatically."""
    try:
        bot = await grid_engine.tick_bot(bot_id)
        return bot.snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid tick failed: {exc}") from exc


@app.get("/api/monitoring/status")
async def monitoring_status() -> dict:
    grid_bots_state = list(grid_engine.bots.values())
    dca_bots_state = list(dca_engine.bots.values())
    return {
        "market_data": {
            "status": "ONLINE" if market_health["last_success_at"] and not market_health["last_error"] else "DEGRADED",
            **market_health,
        },
        "grid": {
            "running": sum(bot.status == "RUNNING" for bot in grid_bots_state),
            "paused": sum(bot.status == "PAUSED" for bot in grid_bots_state),
            "errors": sum(bot.consecutive_errors for bot in grid_bots_state),
        },
        "dca": {
            "running": sum(bot.status == "RUNNING" for bot in dca_bots_state),
            "paused": sum(bot.status == "PAUSED" for bot in dca_bots_state),
            "errors": sum(bot.consecutive_errors for bot in dca_bots_state),
        },
    }


@app.get("/api/notifications/status")
async def notification_status() -> dict:
    return {
        "enabled": notifier.status.enabled,
        "status": "ONLINE" if notifier.status.enabled and not notifier.status.last_error else "ERROR" if notifier.status.enabled else "DISABLED",
        "last_success_at": notifier.status.last_success_at,
        "last_error": notifier.status.last_error,
        "deliveries": store.list_notifications(limit=20),
    }


@app.get("/api/analytics/performance")
async def analytics_performance(symbol: str = "SOLUSDT", days: int = 7) -> dict:
    if days not in {7, 30}:
        raise HTTPException(status_code=400, detail="days must be 7 or 30")
    result = grid_performance(portfolio.trades, grid_engine.bots, days, symbol)
    try:
        candles = await current_klines(symbol, interval="1h", limit=min(1000, days * 24 + 2))
        active_since = result["active_since"]
        if active_since:
            active_ms = int(datetime.fromisoformat(active_since).timestamp() * 1000)
            aligned = [row for row in candles if int(row[0]) >= active_ms]
            if aligned:
                candles = aligned
        first = float(candles[0][1])
        last = float(candles[-1][4])
        result["buy_hold_return_pct"] = (last - first) / first * 100 if first else 0.0
        result["benchmark_from"] = int(candles[0][0])
    except Exception:
        result["buy_hold_return_pct"] = None
        result["benchmark_from"] = None
    benchmark = result["buy_hold_return_pct"]
    result["excess_return_pct"] = result["metrics"]["grid_return_pct"] - benchmark if benchmark is not None else None
    return result


@app.post("/api/notifications/test")
async def notification_test() -> dict:
    try:
        await notifier.send_test()
        return {"status": "sent"}
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Telegram delivery failed: {exc}") from exc


def csv_response(filename: str, fieldnames: list[str], rows: list[dict]) -> StreamingResponse:
    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.get("/api/export/trades.csv")
async def export_trades() -> StreamingResponse:
    fields = ["id", "timestamp", "symbol", "side", "price", "quantity", "quote_amount", "fee_quote", "realized_pnl"]
    return csv_response("paper-trades.csv", fields, [asdict(trade) for trade in portfolio.trades])


@app.get("/api/export/events.csv")
async def export_events() -> StreamingResponse:
    rows = []
    for strategy, bots in (("GRID", grid_engine.bots.values()), ("DCA", dca_engine.bots.values())):
        for bot in bots:
            for event in bot.events:
                row = asdict(event)
                row.update({"strategy": strategy, "bot_id": bot.id, "symbol": bot.symbol})
                rows.append(row)
    rows.sort(key=lambda row: row["timestamp"])
    fields = ["strategy", "bot_id", "symbol", "timestamp", "event", "price", "side", "quantity", "quote_amount", "realized_cycle_pnl", "message"]
    return csv_response("bot-events.csv", fields, rows)
