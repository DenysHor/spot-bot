import asyncio
import csv
import logging
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.backtest.grid import GridBacktester
from app.analytics.performance import grid_performance
from app.analytics.readiness import strategy_readiness
from app.analytics.scanner import analyze_symbol, is_scannable_base
from app.core.config import settings
from app.core.auth import SessionAuth, validate_cloud_security
from app.core.errors import describe_exception
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
symbol_catalog_cache = {"expires_at": datetime.min.replace(tzinfo=timezone.utc), "symbols": []}
symbol_catalog_lock = asyncio.Lock()
market_scanner_cache = {"expires_at": datetime.min.replace(tzinfo=timezone.utc), "result": None}
market_scanner_lock = asyncio.Lock()
logger = logging.getLogger(__name__)
risk_manager = RiskManager(RiskLimits(
    max_portfolio_allocation_pct=settings.max_portfolio_allocation_pct,
    max_position_pct=settings.max_position_pct,
    reserve_quote_pct=settings.reserve_usdt_pct,
))


async def current_price(symbol: str) -> float:
    attempts = max(1, settings.market_retry_attempts)
    for attempt in range(1, attempts + 1):
        try:
            data = await market.price(symbol.upper())
            market_health["last_success_at"] = PaperPortfolio.now_iso()
            market_health["last_error"] = ""
            if attempt > 1:
                logger.info("Binance price recovered for %s on attempt %s", symbol.upper(), attempt)
            return float(data["price"])
        except Exception as exc:
            detail = describe_exception(exc)
            retryable = isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)) or (
                isinstance(exc, httpx.HTTPStatusError)
                and exc.response.status_code in {429, 500, 502, 503, 504}
            )
            if retryable and attempt < attempts:
                delay = max(0.0, settings.market_retry_backoff_seconds) * attempt
                logger.warning(
                    "Transient Binance price error for %s (attempt %s/%s): %s",
                    symbol.upper(), attempt, attempts, detail,
                )
                await asyncio.sleep(delay)
                continue
            message = f"Binance price request failed after {attempt} attempt(s): {detail}"
            market_health["last_error"] = message
            logger.exception("%s", message)
            raise RuntimeError(message) from exc
    raise RuntimeError("Unreachable market retry state")


async def current_klines(symbol: str, interval: str, limit: int) -> list:
    try:
        rows = await market.klines(symbol.upper(), interval=interval, limit=limit)
        market_health["last_success_at"] = PaperPortfolio.now_iso()
        market_health["last_error"] = ""
        return rows
    except Exception as exc:
        market_health["last_error"] = describe_exception(exc)
        raise


async def active_quote_symbols() -> list[dict]:
    now = datetime.now(timezone.utc)
    if symbol_catalog_cache["symbols"] and now < symbol_catalog_cache["expires_at"]:
        return symbol_catalog_cache["symbols"]
    async with symbol_catalog_lock:
        now = datetime.now(timezone.utc)
        if symbol_catalog_cache["symbols"] and now < symbol_catalog_cache["expires_at"]:
            return symbol_catalog_cache["symbols"]
        data = await market.exchange_info()
        symbols = sorted([
            {"symbol": item["symbol"], "base_asset": item["baseAsset"], "quote_asset": item["quoteAsset"]}
            for item in data.get("symbols", [])
            if item.get("status") == "TRADING"
            and item.get("quoteAsset") == settings.quote_asset
            and item.get("isSpotTradingAllowed", True)
        ], key=lambda item: item["symbol"])
        symbol_catalog_cache.update({"symbols": symbols, "expires_at": now + timedelta(minutes=15)})
        return symbols


async def ensure_active_quote_symbol(symbol: str) -> str:
    normalized = symbol.upper().strip()
    if normalized not in {item["symbol"] for item in await active_quote_symbols()}:
        raise ValueError(f"{normalized} is not an active Binance Spot {settings.quote_asset} pair")
    return normalized


async def scan_top_quote_pairs() -> dict:
    now = datetime.now(timezone.utc)
    cached = market_scanner_cache["result"]
    if cached is not None and now < market_scanner_cache["expires_at"]:
        return cached
    async with market_scanner_lock:
        now = datetime.now(timezone.utc)
        cached = market_scanner_cache["result"]
        if cached is not None and now < market_scanner_cache["expires_at"]:
            return cached
        catalog = await active_quote_symbols()
        catalog_by_symbol = {item["symbol"]: item for item in catalog if is_scannable_base(item["base_asset"])}
        tickers = await market.ticker_24h()
        liquid = sorted([
            ticker for ticker in tickers
            if ticker.get("symbol") in catalog_by_symbol
            and float(ticker.get("quoteVolume", 0.0)) > 0
        ], key=lambda ticker: float(ticker.get("quoteVolume", 0.0)), reverse=True)[:50]
        semaphore = asyncio.Semaphore(8)

        async def analyze(ticker: dict):
            async with semaphore:
                try:
                    rows = await market.klines(ticker["symbol"], interval="4h", limit=60)
                    return analyze_symbol(ticker, rows, catalog_by_symbol[ticker["symbol"]]["base_asset"])
                except Exception as exc:
                    logger.warning("Market scanner skipped %s: %s", ticker.get("symbol"), describe_exception(exc))
                    return None

        volume_ranks = {ticker["symbol"]: rank for rank, ticker in enumerate(liquid, start=1)}
        analyzed = [item for item in await asyncio.gather(*(analyze(ticker) for ticker in liquid)) if item]
        for item in analyzed:
            item["volume_rank"] = volume_ranks[item["symbol"]]
        analyzed.sort(key=lambda item: (item["score"], item["quote_volume_24h"]), reverse=True)
        for rank, item in enumerate(analyzed, start=1):
            item["scanner_rank"] = rank
        prices = {ticker["symbol"]: float(ticker["lastPrice"]) for ticker in liquid if ticker.get("lastPrice")}
        store.update_market_signal_outcomes(prices, now)
        observation_bucket = now.replace(minute=0, second=0, microsecond=0).isoformat()
        store.record_market_signals(observation_bucket, analyzed)
        result = {
            "generated_at": now.isoformat(), "refresh_after_seconds": 900,
            "universe": "Top 50 active Binance Spot USDT pairs by 24h quote volume",
            "analyzed_count": len(analyzed),
            "paper_candidates": sum(item["signal"] == "PAPER_CANDIDATE" for item in analyzed),
            "items": analyzed,
            "disclaimer": "Signals are deterministic market observations for PAPER research, not financial advice.",
        }
        market_health["last_success_at"] = PaperPortfolio.now_iso()
        market_health["last_error"] = ""
        market_scanner_cache.update({"result": result, "expires_at": now + timedelta(minutes=15)})
        return result


def automation_budget_status(symbol: str, requested_budget: float) -> dict:
    active_grid = [bot for bot in grid_engine.bots.values() if bot.status != "STOPPED"]
    active_dca = [bot for bot in dca_engine.bots.values() if bot.status != "STOPPED"]
    allocated = sum(bot.budget_quote for bot in active_grid) + sum(bot.budget_quote for bot in active_dca)
    capital = max(portfolio.starting_quote, 0.0)
    portfolio_cap = capital * settings.max_portfolio_allocation_pct / 100
    pair_cap = capital * settings.max_position_pct / 100
    reasons: list[str] = []
    if any(bot.symbol == symbol for bot in active_grid):
        reasons.append(f"Для {symbol} уже існує активний або призупинений Grid-бот")
    if len(active_grid) >= settings.max_grid_bots:
        reasons.append(f"Досягнуто ліміт {settings.max_grid_bots} Grid-ботів")
    if requested_budget > pair_cap + 1e-9:
        reasons.append(f"Бюджет однієї пари не може перевищувати {pair_cap:.2f} {settings.quote_asset}")
    if allocated + requested_budget > portfolio_cap + 1e-9:
        reasons.append(f"Сумарний бюджет автоматизацій не може перевищувати {portfolio_cap:.2f} {settings.quote_asset}")
    return {
        "allowed": not reasons,
        "reasons": reasons,
        "capital": capital,
        "allocated_budget": allocated,
        "requested_budget": requested_budget,
        "remaining_after_start": max(0.0, portfolio_cap - allocated - requested_budget),
        "portfolio_cap": portfolio_cap,
        "portfolio_cap_pct": settings.max_portfolio_allocation_pct,
        "pair_cap": pair_cap,
        "pair_cap_pct": settings.max_position_pct,
        "active_grid_bots": len(active_grid),
        "max_grid_bots": settings.max_grid_bots,
    }


async def grid_preflight_analysis(request) -> dict:
    symbol = await ensure_active_quote_symbol(request.symbol)
    budget = automation_budget_status(symbol, request.budget_quote)
    ticker, rows = await asyncio.gather(
        market.ticker_24h(symbol), current_klines(symbol, interval="4h", limit=60),
    )
    market_analysis = analyze_symbol(ticker, rows, base_asset_from_symbol(symbol))
    average_range = market_analysis["atr_pct"]
    quote_volume = float(ticker.get("quoteVolume", 0.0))
    change_24h = float(ticker.get("priceChangePercent", 0.0))
    recommended_step = market_analysis["recommended_step_pct"]
    recommended_levels = market_analysis["recommended_levels_each_side"]
    fee_drag_pct = round((broker.fee_rate * 2 * 100) / request.step_pct * 100, 1)
    warnings: list[str] = []
    if quote_volume < 1_000_000:
        warnings.append("Низький добовий обсяг: можливі гірші умови виконання")
    if request.step_pct < 0.35:
        warnings.append("Крок надто близький до подвійної торгової комісії")
    if abs(change_24h) >= 10:
        warnings.append("За 24 години стався сильний рух ціни; параметри можуть швидко застаріти")
    if request.step_pct < recommended_step * 0.6 or request.step_pct > recommended_step * 2:
        warnings.append(f"Поточний крок відрізняється від орієнтира {recommended_step:.2f}%")
    regime = market_analysis["regime"]
    profile_evidence = None
    resolved_profile = regime["recommended_profile"] if request.strategy_profile == "AUTO" else request.strategy_profile
    if request.strategy_profile == "AUTO" and regime["name"] == "UPTREND":
        profile_evidence = await backtester.compare_profiles(
            symbol=symbol, base_asset=base_asset_from_symbol(symbol), raw_candles=rows,
            budget_quote=request.budget_quote, step_pct=request.step_pct,
            levels_each_side=request.levels_each_side, training_pct=70,
        )
        evidence_recommendation = profile_evidence["recommendation"]
        if evidence_recommendation["validation_passed"]:
            resolved_profile = evidence_recommendation["recommended_bot_profile"]
    regime_override = request.strategy_profile != "AUTO" and not regime["new_bot_allowed"]
    if regime_override:
        warnings.append("Обраний вручну профіль суперечить рекомендації WAIT")
    launch_allowed = budget["allowed"] and (regime["new_bot_allowed"] or regime_override)
    verdict = "BLOCKED" if not launch_allowed else "CAUTION" if warnings else "SUITABLE"
    return {
        "symbol": symbol,
        "verdict": verdict,
        "budget": budget,
        "market": {
            "price": float(ticker["lastPrice"]),
            "change_24h_pct": change_24h,
            "quote_volume_24h": quote_volume,
            "average_hourly_range_pct": round(average_range, 3),
            "liquidity": "HIGH" if quote_volume >= 10_000_000 else "MEDIUM" if quote_volume >= 1_000_000 else "LOW",
        },
        "parameters": {
            "requested_step_pct": request.step_pct,
            "fee_drag_pct_of_step": fee_drag_pct,
            "recommended_step_pct": recommended_step,
            "recommended_levels_each_side": recommended_levels,
        },
        "strategy": {
            "requested_profile": request.strategy_profile,
            "resolved_profile": resolved_profile,
            "launch_allowed": launch_allowed,
            "manual_override": regime_override,
            "regime": regime,
            "profile_evidence": ({
                "validation_passed": profile_evidence["recommendation"]["validation_passed"],
                "validation_return_pct": profile_evidence["recommendation"]["validation_performance"]["return_pct"],
            } if profile_evidence else None),
        },
        "warnings": warnings,
        "recommendation": (
            "; ".join(budget["reasons"]) if not budget["allowed"]
            else "Рекомендація WAIT: новий бот в AUTO не запускається" if not launch_allowed
            else "Можна запускати в PAPER із підвищеною увагою до попереджень" if warnings
            else "Пара та бюджет відповідають поточним PAPER-обмеженням"
        ),
    }


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
market_scanner_task: asyncio.Task | None = None


async def market_scanner_forever() -> None:
    while True:
        try:
            await scan_top_quote_pairs()
        except Exception as exc:
            logger.warning("Background market scanner failed: %s", describe_exception(exc))
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global market_scanner_task
    validate_cloud_security(settings.secure_cookies, settings.dashboard_password, settings.session_secret)
    app.state.last_backup = store.create_backup(settings.sqlite_backup_count)
    if settings.trading_mode == "PAPER":
        grid_engine.start_background()
        dca_engine.start_background()
        notifier.start_background()
        market_scanner_task = asyncio.create_task(market_scanner_forever())
    yield
    if market_scanner_task and not market_scanner_task.done():
        market_scanner_task.cancel()
        try:
            await market_scanner_task
        except asyncio.CancelledError:
            pass
    market_scanner_task = None
    await notifier.stop_background()
    await grid_engine.stop_background()
    await dca_engine.stop_background()


app = FastAPI(title="Spot Bot API", version="0.32.0", lifespan=lifespan)
static_dir = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.middleware("http")
async def require_authentication(request: Request, call_next):
    path = request.url.path
    public = path == "/" or path == "/health" or path.startswith("/static/") or path.startswith("/api/auth/")
    if not auth.enabled or public:
        response = await call_next(request)
    elif not auth.verify_token(request.cookies.get(auth.cookie_name)):
        response = JSONResponse(status_code=401, content={"detail": "Authentication required"})
    else:
        response = await call_next(request)
    if path == "/" or path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache, max-age=0, must-revalidate"
    return response


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
    trailing_up_enabled: bool = False
    strategy_profile: str = Field(
        default="AUTO",
        pattern=r"^(AUTO|RANGE_GRID|TRAILING_GRID|UPTREND_HYBRID_(10|20|30))$",
    )


class GridTrailingRequest(BaseModel):
    enabled: bool


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
        "version": "0.32.0",
        "trading_mode": settings.trading_mode,
        "live_trading_enabled": settings.trading_mode == "LIVE",
        "grid_background_worker": settings.trading_mode == "PAPER",
        "authentication_enabled": auth.enabled,
    }


@app.get("/api/market/symbols/search")
async def market_symbol_search(query: str = "", limit: int = 20) -> dict:
    normalized = "".join(character for character in query.upper() if character.isalnum())[:20]
    if limit < 1 or limit > 50:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 50")
    try:
        symbols = await active_quote_symbols()
        matches = [
            item for item in symbols
            if not normalized or normalized in item["symbol"] or normalized in item["base_asset"]
        ]
        matches.sort(key=lambda item: (
            not item["base_asset"].startswith(normalized),
            not item["symbol"].startswith(normalized),
            item["symbol"],
        ))
        return {"query": normalized, "quote_asset": settings.quote_asset, "symbols": matches[:limit]}
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Binance symbol catalog error: {exc}") from exc


@app.get("/api/market/scanner")
async def market_scanner() -> dict:
    try:
        return await scan_top_quote_pairs()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Binance market scanner error: {exc}") from exc


@app.get("/api/market/scanner/history")
async def market_scanner_history(limit: int = 100) -> dict:
    if limit < 1 or limit > 500:
        raise HTTPException(status_code=400, detail="limit must be between 1 and 500")
    history = store.market_signal_history(limit)
    quality = store.market_signal_quality()
    return {
        "history": history, "quality": quality,
        "observations": sum(item["observations"] for item in quality),
        "validated_signals": sum(item["validated"] for item in quality),
        "validation_rule": "At least 30 evaluated 7-day observations per signal",
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


@app.post("/api/backtest/grid/compare-trailing")
async def grid_compare_trailing(request: GridBacktestRequest) -> dict:
    """Compare Fixed Grid and Trailing Up on one identical candle dataset."""
    try:
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        candles = await current_klines(symbol, interval=request.interval, limit=request.limit)
        return await backtester.compare_trailing(
            symbol=symbol, base_asset=base_asset, raw_candles=candles,
            budget_quote=request.budget_quote, step_pct=request.step_pct,
            levels_each_side=request.levels_each_side,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid comparison failed: {exc}") from exc


@app.post("/api/backtest/grid/compare-profiles")
async def grid_compare_profiles(request: GridBacktestRequest) -> dict:
    """Compare strategy profiles and validate the training winner on unseen candles."""
    try:
        symbol = request.symbol.upper()
        base_asset = base_asset_from_symbol(symbol)
        candles = await current_klines(symbol, interval=request.interval, limit=request.limit)
        return await backtester.compare_profiles(
            symbol=symbol, base_asset=base_asset, raw_candles=candles,
            budget_quote=request.budget_quote, step_pct=request.step_pct,
            levels_each_side=request.levels_each_side, training_pct=70,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Profile comparison failed: {exc}") from exc


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
        symbol = await ensure_active_quote_symbol(request.symbol)
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


@app.post("/api/grid/preflight")
async def grid_preflight(request: GridStartRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Grid preflight is available for PAPER mode only")
    try:
        return await grid_preflight_analysis(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid preflight failed: {exc}") from exc


@app.post("/api/grid/bots/start")
async def grid_start(request: GridStartRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Grid execution v0.3 is PAPER-only")
    try:
        symbol = await ensure_active_quote_symbol(request.symbol)
        budget_status = automation_budget_status(symbol, request.budget_quote)
        if not budget_status["allowed"]:
            raise ValueError("; ".join(budget_status["reasons"]))
        preflight = await grid_preflight_analysis(request)
        if not preflight["strategy"]["launch_allowed"]:
            raise ValueError(preflight["recommendation"])
        profile = preflight["strategy"]["resolved_profile"]
        seed_position_pct = {
            "UPTREND_HYBRID_10": 10.0,
            "UPTREND_HYBRID_20": 20.0,
            "UPTREND_HYBRID_30": 30.0,
        }.get(profile, 0.0)
        base_asset = base_asset_from_symbol(symbol)
        price = await current_price(symbol)
        bot = grid_engine.start_bot(
            symbol=symbol,
            base_asset=base_asset,
            reference_price=price,
            budget_quote=request.budget_quote,
            step_pct=request.step_pct,
            levels_each_side=request.levels_each_side,
            trailing_up_enabled=profile != "RANGE_GRID",
            strategy_profile=profile,
            seed_position_pct=seed_position_pct,
        )
        return bot.snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid start failed: {exc}") from exc


@app.post("/api/grid/bots/{bot_id}/trailing-up")
async def grid_trailing_up(bot_id: str, request: GridTrailingRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Trailing Up is PAPER-only")
    try:
        return grid_engine.set_trailing_up(bot_id, request.enabled).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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


def grid_bot_health(bot, now: datetime | None = None) -> dict:
    current = now or datetime.now(timezone.utc)
    created = datetime.fromisoformat(bot.created_at.replace("Z", "+00:00"))
    fill_times = [
        datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
        for event in bot.events if event.event in {"BUY_FILLED", "SELL_FILLED"}
    ]
    last_activity = max(fill_times, default=created)
    idle_hours = max(0.0, (current - last_activity).total_seconds() / 3600)
    if bot.status == "PAUSED":
        code, label, message = "PAUSED", "Потребує уваги", bot.paused_reason or "Бот призупинений"
    elif bot.consecutive_errors:
        code, label, message = "ERROR", "Помилка", f"Помилок поспіль: {bot.consecutive_errors}"
    elif bot.recenter_count_today >= bot.max_recenters_per_day:
        code, label, message = "TRAILING_LIMIT", "Потребує уваги", "Досягнуто добовий ліміт Trailing"
    elif idle_hours >= 48:
        code, label, message = "IDLE", "Немає угод", f"Без виконаних ордерів {idle_hours:.0f} год"
    else:
        code, label, message = "NORMAL", "Працює нормально", "Критичних сигналів немає"
    return {
        "code": code, "label": label, "message": message,
        "idle_hours": round(idle_hours, 1), "last_fill_at": max(fill_times).isoformat() if fill_times else None,
        "needs_attention": code != "NORMAL",
    }


def portfolio_comparison(now: datetime | None = None) -> dict:
    current = now or datetime.now(timezone.utc)
    rows = []
    for bot in grid_engine.bots.values():
        if bot.status == "STOPPED":
            continue
        performance = grid_performance(portfolio.trades, grid_engine.bots, 30, bot.symbol, now=current)
        metrics = performance["metrics"]
        elapsed_days = performance["elapsed_hours"] / 24
        eligible = elapsed_days >= 7 and metrics["cycles"] >= 20
        rows.append({
            "bot_id": bot.id, "symbol": bot.symbol, "status": bot.status,
            "budget_quote": bot.budget_quote, "total_pnl": bot.snapshot()["total_pnl"],
            "realized_pnl": metrics["realized_pnl"], "fees": metrics["fees"],
            "cycles": metrics["cycles"], "grid_return_pct": metrics["grid_return_pct"],
            "exposure_quote": bot.snapshot()["open_exposure_quote"],
            "elapsed_days": round(elapsed_days, 2), "eligible_for_ranking": eligible,
            "rank": None, "health": grid_bot_health(bot, current),
        })
    ranked = sorted((row for row in rows if row["eligible_for_ranking"]), key=lambda row: row["grid_return_pct"], reverse=True)
    for rank, row in enumerate(ranked, start=1):
        row["rank"] = rank
    return {
        "minimum_days": 7, "minimum_cycles": 20,
        "eligible_bots": len(ranked), "attention_count": sum(row["health"]["needs_attention"] for row in rows),
        "bots": rows,
    }


@app.get("/api/analytics/portfolio-comparison")
async def analytics_portfolio_comparison() -> dict:
    return portfolio_comparison()


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
        experiment_started_at = result["experiment_started_at"]
        if active_since:
            active_ms = int(datetime.fromisoformat(active_since).timestamp() * 1000)
            aligned = [row for row in candles if int(row[0]) >= active_ms]
            if aligned:
                candles = aligned
        first = float(candles[0][1])
        last = float(candles[-1][4])
        benchmark_method = "ALIGNED_CANDLE_OPEN"
        matching_bots = [
            bot for bot in grid_engine.bots.values()
            if bot.symbol == symbol.upper() and bot.status in {"RUNNING", "PAUSED"}
        ]
        exact_start = (
            active_since and experiment_started_at
            and datetime.fromisoformat(active_since) == datetime.fromisoformat(experiment_started_at)
        )
        if exact_start and matching_bots:
            bot = matching_bots[-1]
            seed_event = next(
                (event for event in bot.events if event.event == "HYBRID_SEED_BOUGHT"), None,
            )
            first = seed_event.price if seed_event is not None else bot.reference_price
            last = bot.last_price
            benchmark_method = "EXACT_BOT_ENTRY"
        if first:
            benchmark_quantity = 1 / (1 + broker.fee_rate) / first
            benchmark_ending = benchmark_quantity * last * (1 - broker.fee_rate)
            result["buy_hold_return_pct"] = (benchmark_ending - 1) * 100
        else:
            result["buy_hold_return_pct"] = 0.0
        result["benchmark_from"] = int(candles[0][0])
        result["benchmark_entry_price"] = first
        result["benchmark_last_price"] = last
        result["benchmark_method"] = benchmark_method
    except Exception:
        result["buy_hold_return_pct"] = None
        result["benchmark_from"] = None
        result["benchmark_entry_price"] = None
        result["benchmark_last_price"] = None
        result["benchmark_method"] = None
    benchmark = result["buy_hold_return_pct"]
    compared_return = (
        result["metrics"]["hybrid_total_return_pct"]
        if result["metrics"]["is_hybrid"] else result["metrics"]["grid_return_pct"]
    )
    result["compared_return_pct"] = compared_return
    result["excess_return_pct"] = compared_return - benchmark if benchmark is not None else None
    result["readiness"] = strategy_readiness(result)
    return result


async def weekly_evaluation_message(symbol: str) -> str:
    result = await analytics_performance(symbol=symbol, days=30)
    readiness = result["readiness"]
    metrics = result["metrics"]
    score = (
        f"Quality score: {readiness['quality_score_pct']:.0f}%"
        if readiness["quality_score_pct"] is not None
        else f"Data progress: {readiness['data_progress_pct']:.0f}%"
    )
    return "\n".join([
        f"Spot Grid Lab · Weekly evaluation · {symbol}",
        f"Status: {readiness['status']}",
        score,
        f"Evidence: {readiness['elapsed_days']:.1f}/7 days, {readiness['cycles']}/20 cycles",
        f"Net P&L: {metrics['realized_pnl']:.4f} USDT",
        f"Grid return: {metrics['grid_return_pct']:.3f}%",
        f"Drawdown: {metrics['realized_max_drawdown_pct']:.3f}%",
        f"Fee drag: {readiness['fee_drag_pct']:.1f}%",
        readiness["recommendation"],
    ])


notifier.weekly_report_provider = weekly_evaluation_message


def telegram_health_alerts(now: datetime) -> list[dict]:
    alerts = []
    for bot in grid_engine.bots.values():
        if bot.status == "STOPPED":
            continue
        health = grid_bot_health(bot, now)
        if health["needs_attention"]:
            alerts.append({
                "key": f"{bot.id}:{health['code']}",
                "message": "\n".join([
                    "Spot Grid Lab · PAPER attention",
                    f"{bot.symbol} · {health['label']}",
                    health["message"],
                    "Автоматичних змін параметрів не виконано.",
                ]),
            })
    return alerts


notifier.health_alert_provider = telegram_health_alerts


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
