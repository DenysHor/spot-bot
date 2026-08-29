from contextlib import asynccontextmanager
from dataclasses import asdict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.backtest.grid import GridBacktester
from app.core.config import settings
from app.exchange.binance_public import BinancePublicClient
from app.grid.execution import GridExecutionEngine
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.persistence.sqlite import SQLiteStore
from app.risk.manager import RiskLimits, RiskManager
from app.strategies.smart_grid import SmartGrid

market = BinancePublicClient()
store = SQLiteStore(settings.sqlite_path)
portfolio = PaperPortfolio(
    starting_quote=settings.paper_start_balance,
    quote_asset=settings.quote_asset,
    store=store,
)
broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)
grid = SmartGrid()
backtester = GridBacktester(fee_rate=0.001)


async def current_price(symbol: str) -> float:
    data = await market.price(symbol.upper())
    return float(data["price"])


grid_engine = GridExecutionEngine(
    broker=broker,
    price_provider=current_price,
    poll_seconds=getattr(settings, "grid_poll_seconds", 5.0),
    store=store,
    risk_manager=RiskManager(RiskLimits(
        max_portfolio_allocation_pct=settings.max_portfolio_allocation_pct,
        max_position_pct=settings.max_position_pct,
        reserve_quote_pct=settings.reserve_usdt_pct,
    )),
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.trading_mode == "PAPER":
        grid_engine.start_background()
    yield
    await grid_engine.stop_background()


app = FastAPI(title="Spot Bot API", version="0.5.0", lifespan=lifespan)


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
        "version": "0.5.0",
        "trading_mode": settings.trading_mode,
        "live_trading_enabled": settings.trading_mode == "LIVE",
        "grid_background_worker": settings.trading_mode == "PAPER",
    }


@app.get("/api/market/{symbol}")
async def market_snapshot(symbol: str) -> dict:
    try:
        price = await market.price(symbol)
        ticker = await market.ticker_24h(symbol)
        return {
            "symbol": symbol.upper(),
            "price": float(price["price"]),
            "change_24h_pct": float(ticker["priceChangePercent"]),
            "volume_24h": float(ticker["volume"]),
            "quote_volume_24h": float(ticker["quoteVolume"]),
        }
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Binance market-data error: {exc}") from exc


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
    return {"status": "reset", "portfolio": portfolio.snapshot(), "grid_bots": []}


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
        candles = await market.klines(symbol, interval=request.interval, limit=request.limit)
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
        raise HTTPException(status_code=404, detail=str(exc)) from exc


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
