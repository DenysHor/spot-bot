from dataclasses import asdict

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.core.config import settings
from app.exchange.binance_public import BinancePublicClient
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.strategies.smart_grid import SmartGrid

app = FastAPI(title="Spot Bot API", version="0.2.0")
market = BinancePublicClient()
portfolio = PaperPortfolio(
    starting_quote=settings.paper_start_balance,
    quote_asset=settings.quote_asset,
)
broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)
grid = SmartGrid()


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


def base_asset_from_symbol(symbol: str) -> str:
    symbol = symbol.upper()
    quote = settings.quote_asset.upper()
    if not symbol.endswith(quote):
        raise ValueError(f"Only {quote}-quoted symbols are supported in paper v0.2")
    return symbol[: -len(quote)]


async def current_price(symbol: str) -> float:
    data = await market.price(symbol.upper())
    return float(data["price"])


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "version": "0.2.0",
        "trading_mode": settings.trading_mode,
        "live_trading_enabled": settings.trading_mode == "LIVE",
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
    return {"status": "reset", "portfolio": portfolio.snapshot()}


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
