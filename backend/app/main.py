import asyncio
import csv
import hashlib
import json
import logging
import secrets
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from zoneinfo import ZoneInfo

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
from app.exchange.binance_testnet import BinanceTestnetClient, BinanceTestnetError
from app.grid.execution import GridExecutionEngine
from app.notifications.telegram import TelegramNotifier
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.persistence.sqlite import SQLiteStore
from app.risk.manager import RiskLimits, RiskManager
from app.signal.execution import SignalExecutionEngine
from app.strategies.smart_grid import SmartGrid
from app.testnet.execution import TestnetGridEngine

market = BinancePublicClient()
testnet = BinanceTestnetClient(settings.binance_api_key, settings.binance_api_secret)
testnet_health = {"verified": False, "last_checked_at": "", "last_error": ""}
testnet_audit_cache: dict | None = None
TESTNET_AUDIT_TIMEOUT_SECONDS = 8
auth = SessionAuth(
    settings.dashboard_username, settings.dashboard_password,
    settings.session_secret, settings.secure_cookies,
)
store = SQLiteStore(settings.sqlite_path)
testnet_engine = TestnetGridEngine(testnet, store=store, poll_seconds=settings.grid_poll_seconds)
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
    max_daily_loss_pct=settings.max_daily_loss_pct,
    max_open_positions=settings.max_open_positions,
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


async def signal_analysis(symbol: str, base_asset: str) -> dict:
    ticker, rows = await asyncio.gather(
        market.ticker_24h(symbol), current_klines(symbol, interval="4h", limit=60),
    )
    return analyze_symbol(ticker, rows, base_asset)


async def dca_preflight_analysis(request) -> dict:
    symbol = await ensure_active_quote_symbol(request.symbol)
    budget = automation_budget_status(symbol, request.budget_quote, check_grid_limit=False)
    ticker, rows = await asyncio.gather(
        market.ticker_24h(symbol), current_klines(symbol, interval="4h", limit=60),
    )
    analysis = analyze_symbol(ticker, rows, base_asset_from_symbol(symbol))
    regime = analysis["regime"]["name"]
    change_24h = float(ticker.get("priceChangePercent", 0.0))
    warnings: list[str] = []
    if regime == "DOWNTREND":
        warnings.append("Спадний режим: новий DCA-запуск заблоковано")
    if change_24h <= -8:
        warnings.append("Падіння за 24 години перевищує 8%")
    if request.order_quote * (1 + broker.fee_rate) > request.budget_quote:
        warnings.append("Одна покупка разом із комісією перевищує бюджет")
    allowed = budget["allowed"] and regime != "DOWNTREND" and change_24h > -8 and not any(
        "перевищує бюджет" in warning for warning in warnings
    )
    return {
        "symbol": symbol, "allowed": allowed,
        "verdict": "SUITABLE" if allowed and not warnings else "CAUTION" if allowed else "BLOCKED",
        "budget": budget,
        "market": {"price": float(ticker["lastPrice"]), "change_24h_pct": change_24h,
                   "rsi14": analysis["rsi14"], "regime": regime},
        "parameters": {"budget_quote": request.budget_quote, "order_quote": request.order_quote,
                       "interval_minutes": request.interval_minutes,
                       "dip_trigger_pct": request.dip_trigger_pct,
                       "max_drawdown_pct": request.max_drawdown_pct},
        "warnings": warnings + budget["reasons"],
    }


def recommend_automation(analysis: dict, min_signal_score: int = 65) -> dict:
    """Choose one PAPER automation family from the same market evidence."""
    regime = analysis["regime"]
    signal_ready = (
        analysis["score"] >= min_signal_score
        and analysis["price"] > analysis["ema20"] > analysis["ema50"]
        and 45 <= analysis["rsi14"] <= 68
        and analysis["volume_ratio"] >= 1.1
    )
    if regime["name"] == "RANGE":
        strategy = "GRID"
        profile = "RANGE_GRID"
        reason = "Боковий режим більше відповідає заробітку на коливаннях сітки"
    elif regime["name"] == "UPTREND" and signal_ready:
        strategy = "SIGNAL"
        profile = "SIGNAL_MOMENTUM"
        reason = "Тренд, RSI та обсяг одночасно підтверджують Сигнальний вхід"
    elif regime["name"] == "UPTREND":
        strategy = "GRID"
        profile = regime["recommended_profile"]
        reason = "Є висхідний тренд, але Сигнальний вхід ще не має всіх підтверджень"
    else:
        strategy = "WAIT"
        profile = "WAIT"
        reason = "Поточний режим не дає достатньо безпечної переваги для нового PAPER-входу"
    return {
        "strategy": strategy, "profile": profile, "reason": reason,
        "signal_ready": signal_ready, "signal_score": analysis["score"],
        "evaluated": ["RANGE_GRID", "TRAILING_GRID", "HYBRID", "SIGNAL", "WAIT"],
    }


def automation_budget_status(symbol: str, requested_budget: float, check_grid_limit: bool = True) -> dict:
    active_grid = [bot for bot in grid_engine.bots.values() if bot.status != "STOPPED"]
    active_dca = [bot for bot in dca_engine.bots.values() if bot.status != "STOPPED"]
    active_signal = [bot for bot in signal_engine.bots.values() if bot.status != "STOPPED"]
    allocated = (sum(bot.budget_quote for bot in active_grid) + sum(bot.budget_quote for bot in active_dca)
                 + sum(bot.budget_quote for bot in active_signal))
    capital = max(portfolio.starting_quote, 0.0)
    portfolio_cap = capital * settings.max_portfolio_allocation_pct / 100
    pair_cap = capital * settings.max_position_pct / 100
    reasons: list[str] = []
    if any(bot.symbol == symbol for bot in active_grid):
        reasons.append(f"Для {symbol} уже існує активний або призупинений Grid-бот")
    if any(bot.symbol == symbol for bot in active_dca):
        reasons.append(f"Для {symbol} уже існує активний або призупинений DCA-бот")
    if any(bot.symbol == symbol for bot in active_signal):
        reasons.append(f"Для {symbol} уже існує активний або призупинений Сигнальний бот")
    if check_grid_limit and len(active_grid) >= settings.max_grid_bots:
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
    market_price = float(ticker["lastPrice"])
    corridor_half_width_pct = min(30.0, max(
        5.0, request.step_pct * request.levels_each_side, average_range * 3,
    ))
    recommended_floor = market_price * (1 - corridor_half_width_pct / 100)
    recommended_ceiling = market_price * (1 + corridor_half_width_pct / 100)
    if (request.price_floor is None) != (request.price_ceiling is None):
        raise ValueError("Вкажіть обидві межі коридору або залиште обидві в AUTO")
    if request.price_floor is not None and request.price_floor >= request.price_ceiling:
        raise ValueError("Нижня межа коридору має бути меншою за верхню")
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
    automation = recommend_automation(market_analysis)
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
    grid_is_recommended = automation["strategy"] == "GRID"
    launch_allowed = budget["allowed"] and ((regime["new_bot_allowed"] and grid_is_recommended) or regime_override)
    verdict = "BLOCKED" if not launch_allowed else "CAUTION" if warnings else "SUITABLE"
    return {
        "symbol": symbol,
        "verdict": verdict,
        "budget": budget,
        "market": {
            "price": market_price,
            "rsi14": market_analysis["rsi14"],
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
            "price_floor": request.price_floor if request.price_floor is not None else recommended_floor,
            "price_ceiling": request.price_ceiling if request.price_ceiling is not None else recommended_ceiling,
            "corridor_mode": "MANUAL" if request.price_floor is not None else "AUTO",
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
            "automation": automation,
        },
        "warnings": warnings,
        "recommendation": (
            "; ".join(budget["reasons"]) if not budget["allowed"]
            else "Рекомендовано Сигнальну PAPER-стратегію замість Grid" if automation["strategy"] == "SIGNAL" and request.strategy_profile == "AUTO"
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
signal_engine = SignalExecutionEngine(
    broker=broker, analysis_provider=signal_analysis, risk_manager=risk_manager,
    poll_seconds=60, store=store,
)
notifier = TelegramNotifier(
    settings.telegram_bot_token, settings.telegram_chat_id, grid_engine, dca_engine,
    signal_engine=signal_engine, portfolio=portfolio, store=store, poll_seconds=settings.notification_poll_seconds,
    daily_report_hour_utc=settings.daily_report_hour_utc,
)


async def notify_testnet_event(event: str, bot, order) -> None:
    if not notifier.enabled:
        return
    if event == "SYNC_BATCH":
        fills = order.get("fills", [])
        buys = sum(row["event"] == "BUY_FILLED" for row in fills)
        sells = sum(row["event"] == "SELL_FILLED" for row in fills)
        prices = [float(row["price"]) for row in fills if row.get("price")]
        result = float(order.get("realized_pnl_delta", 0))
        price_range = ""
        if prices:
            low, high = min(prices), max(prices)
            price_range = f"\nДіапазон цін: {low:,.2f}–{high:,.2f} USDT".replace(",", " ")
        await notifier.send(
            "TESTNET_SYNC_BATCH",
            f"🧪 {bot.symbol} · TESTNET\n"
            f"Синхронізовано виконані заявки:\n"
            f"• Покупок: {buys}\n• Продажів: {sells}\n"
            f"• Завершено циклів: {sells}\n"
            f"• Результат циклів: {result:+.2f} USDT"
            f"{price_range}\nТільки віртуальні кошти.",
        )
        return
    labels = {
        "BUY_FILLED": "Купівлю виконано; створено парний продаж",
        "SELL_FILLED": "Продаж виконано; цикл завершено",
        "BUY_AUTO_PAUSED": "Нові покупки автоматично призупинено через нестачу тестового USDT",
        "SYNC_ERROR": f"Помилка синхронізації: {bot.last_error}",
        "EMERGENCY_STOP": "Аварійна зупинка: усі відкриті TESTNET-заявки скасовано",
    }
    label = labels.get(event)
    if not label:
        return
    price_line = f"\nЦіна: {order.price:,.2f} USDT".replace(",", " ") if order else ""
    await notifier.send(
        f"TESTNET_{event}",
        f"🧪 {bot.symbol} · TESTNET\n{label}{price_line}\nТільки віртуальні кошти.",
    )


testnet_engine.event_sink = notify_testnet_event
market_scanner_task: asyncio.Task | None = None
market_guard_last_run = datetime.min.replace(tzinfo=timezone.utc)
SCHEDULED_GRID_STORE_KEY = "scheduled_grid_launches_v1"


def _load_scheduled_grid_launches() -> list[dict]:
    raw = store.get_notification_state(SCHEDULED_GRID_STORE_KEY, "[]")
    try:
        rows = json.loads(raw)
        return rows if isinstance(rows, list) else []
    except (TypeError, ValueError):
        return []


scheduled_grid_launches: list[dict] = _load_scheduled_grid_launches()


def _save_scheduled_grid_launches() -> None:
    store.set_notification_state(
        SCHEDULED_GRID_STORE_KEY,
        json.dumps(scheduled_grid_launches, ensure_ascii=False),
    )


async def scheduled_grid_launch_once(now: datetime | None = None) -> None:
    current = now or datetime.now(timezone.utc)
    changed = False
    for item in scheduled_grid_launches:
        if item.get("status") != "PENDING":
            continue
        if any(
            bot.symbol == item["symbol"] and bot.status != "STOPPED"
            for bot in grid_engine.bots.values()
        ):
            item.update(
                status="CANCELLED",
                last_checked_at=current.isoformat(),
                last_reason="Скасовано автоматично: для пари вже існує активний PAPER Grid-бот",
            )
            changed = True
            if notifier.enabled:
                await notifier.send(
                    "SCHEDULED_GRID_CANCELLED",
                    f"ℹ️ {item['symbol']} · відкладений PAPER-запуск скасовано\n"
                    "Для цієї пари вже працює Grid-бот. Дублікат не створено.",
                )
            continue
        starts_at = datetime.fromisoformat(item["starts_at"])
        expires_at = datetime.fromisoformat(item["expires_at"])
        if current < starts_at:
            continue
        if current > expires_at:
            previous_reason = item.get("last_reason") or "умови не виконані"
            item.update(status="EXPIRED", last_checked_at=current.isoformat(), last_reason="Вікно запуску завершилося")
            changed = True
            if notifier.enabled:
                await notifier.send(
                    "SCHEDULED_GRID_EXPIRED",
                    f"⏱ {item['symbol']} · PAPER-запуск не виконано\n"
                    f"Вікно завершилося. Остання причина: {previous_reason}.",
                )
            continue
        item["attempts"] = int(item.get("attempts", 0)) + 1
        item["last_checked_at"] = current.isoformat()
        changed = True
        request = GridStartRequest(**item["request"])
        try:
            preflight = await grid_preflight_analysis(request)
            price = preflight["market"]["price"]
            rsi = preflight["market"]["rsi14"]
            regime = preflight["strategy"]["regime"]["name"]
            change_24h = preflight["market"]["change_24h_pct"]
            conditions = item["conditions"]
            reasons = []
            if not preflight["strategy"]["launch_allowed"]:
                reasons.append(preflight["recommendation"])
            if regime not in conditions["allowed_regimes"]:
                reasons.append(f"режим {regime}")
            if not conditions["min_rsi14"] <= rsi <= conditions["max_rsi14"]:
                reasons.append(f"RSI {rsi:.1f} поза межами {conditions['min_rsi14']:.0f}–{conditions['max_rsi14']:.0f}")
            if abs(change_24h) > conditions["max_abs_change_24h_pct"]:
                reasons.append(f"рух за 24г {change_24h:+.2f}% надто сильний")
            if request.price_floor is not None and not request.price_floor <= price <= request.price_ceiling:
                reasons.append(f"ціна {price:.6f} поза коридором")
            if reasons:
                item["last_reason"] = "; ".join(reasons)
                continue
            bot = await start_paper_grid_bot(request, preflight=preflight)
            item.update(
                status="STARTED", bot_id=bot.id, launched_at=current.isoformat(),
                last_reason="Усі умови та preflight виконані",
            )
            if notifier.enabled:
                await notifier.send(
                    "SCHEDULED_GRID_STARTED",
                    f"✅ {bot.symbol} · PAPER Grid запущено за планом\n"
                    f"Бюджет: {bot.budget_quote:.2f} USDT · крок {bot.step_pct:.2f}% · "
                    f"рівнів {bot.levels_each_side}.\nРеальні кошти не використовуються.",
                )
        except Exception as exc:
            item["last_reason"] = describe_exception(exc)
    if changed:
        _save_scheduled_grid_launches()


def market_guard_assessment(bot, analysis: dict) -> dict:
    state = bot.snapshot()
    deployed = state["grid_open_exposure_quote"] + state["seed_value_quote"]
    deployed_pct = deployed / bot.budget_quote * 100 if bot.budget_quote else 0.0
    unrealized_pct = state["unrealized_pnl"] / bot.budget_quote * 100 if bot.budget_quote else 0.0
    points, reasons = 0, []
    if analysis["regime"]["name"] == "DOWNTREND":
        points += 2
        reasons.append("ціна та EMA підтверджують спадний тренд")
    if analysis["rsi14"] < 32:
        points += 1
        reasons.append(f"RSI {analysis['rsi14']:.1f}")
    if analysis["change_24h_pct"] <= -3:
        points += 1
        reasons.append(f"24г {analysis['change_24h_pct']:+.2f}%")
    if analysis["atr_pct"] >= 4:
        points += 1
        reasons.append(f"ATR {analysis['atr_pct']:.2f}%")
    if deployed_pct >= 80:
        points += 1
        reasons.append(f"залучено {deployed_pct:.0f}% бюджету")
    if unrealized_pct <= -5:
        points += 1
        reasons.append(f"відкрита просадка {unrealized_pct:.2f}%")
    return {
        "critical": analysis["regime"]["name"] == "DOWNTREND" and points >= 4,
        "points": points, "reasons": reasons,
    }


async def market_guard_once() -> None:
    active = [
        bot for bot in grid_engine.bots.values()
        if bot.status == "RUNNING" and not bot.drain_mode and not bot.manual_buy_paused
    ]
    results = await asyncio.gather(*(
        signal_analysis(bot.symbol, bot.base_asset) for bot in active
    ), return_exceptions=True)
    for bot, analysis in zip(active, results):
        if isinstance(analysis, Exception):
            logger.warning("Market guard skipped %s: %s", bot.symbol, describe_exception(analysis))
            continue
        assessment = market_guard_assessment(bot, analysis)
        if not assessment["critical"]:
            continue
        grid_engine.set_manual_buy_pause(bot.id, True, source="MARKET_GUARD")
        store.set_notification_state(f"market_guard:{bot.id}", "PAUSED")
        if notifier.enabled:
            reasons = "; ".join(assessment["reasons"])
            await notifier.send(
                "MARKET_GUARD_ACTION",
                f"🛡 {bot.symbol} · Антипадіння\nНові покупки автоматично вимкнено.\n"
                f"Причини: {reasons}.\nПродажі відкритих позицій залишаються активними.\n"
                f"Для повторної перевірки: /resume_buys {bot.symbol}",
            )


async def market_scanner_forever() -> None:
    global market_guard_last_run
    while True:
        try:
            await scan_top_quote_pairs()
            now = datetime.now(timezone.utc)
            if now - market_guard_last_run >= timedelta(minutes=15):
                await market_guard_once()
                market_guard_last_run = now
            await scheduled_grid_launch_once(now)
        except Exception as exc:
            logger.warning("Background market scanner failed: %s", describe_exception(exc))
        await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global market_scanner_task
    validate_cloud_security(settings.secure_cookies, settings.dashboard_password, settings.session_secret)
    app.state.last_backup = store.create_backup(settings.sqlite_backup_count)
    if settings.trading_mode in {"PAPER", "TESTNET"}:
        grid_engine.start_background()
        dca_engine.start_background()
        signal_engine.start_background()
        notifier.start_background()
        market_scanner_task = asyncio.create_task(market_scanner_forever())
    if settings.trading_mode == "TESTNET":
        testnet_engine.start_background()
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
    await signal_engine.stop_background()
    await testnet_engine.stop_background()


app = FastAPI(title="Spot Bot API", version="0.72.0", lifespan=lifespan)
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
    price_floor: float | None = Field(default=None, gt=0)
    price_ceiling: float | None = Field(default=None, gt=0)


class ScheduledGridLaunchRequest(GridStartRequest):
    start_after_hours: float = Field(default=12.0, ge=0.1, le=168)
    launch_window_hours: float = Field(default=12.0, ge=1, le=168)
    min_rsi14: float = Field(default=40.0, ge=0, le=100)
    max_rsi14: float = Field(default=65.0, ge=0, le=100)
    max_abs_change_24h_pct: float = Field(default=8.0, gt=0, le=50)
    allowed_regimes: list[str] = Field(default_factory=lambda: ["RANGE", "UPTREND"])


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
    budget_quote: float = Field(default=500.0, gt=0)
    order_quote: float = Field(default=50.0, gt=0)
    interval_minutes: int = Field(default=1440, ge=1, le=525600)
    dip_trigger_pct: float = Field(default=5.0, gt=0, le=50)
    max_drawdown_pct: float = Field(default=12.0, gt=0, le=50)


class SignalStartRequest(BaseModel):
    symbol: str = "BTCUSDT"
    budget_quote: float = Field(default=100.0, gt=0)
    min_score: int = Field(default=65, ge=50, le=90)


class TestnetGridStartRequest(BaseModel):
    symbol: str = "BTCUSDT"
    budget_quote: float = Field(default=100.0, gt=0, le=10000)
    step_pct: float = Field(default=1.5, gt=0, le=10)
    levels: int = Field(default=4, ge=1, le=10)


class TestnetRecoveryRequest(BaseModel):
    expected_order_ids: list[int]
    confirmed: bool = False


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
        "version": "0.72.0",
        "trading_mode": settings.trading_mode,
        "live_trading_enabled": False,
        "grid_background_worker": settings.trading_mode in {"PAPER", "TESTNET"},
        "paper_background_worker": settings.trading_mode in {"PAPER", "TESTNET"},
        "testnet_background_worker": settings.trading_mode == "TESTNET",
        "authentication_enabled": auth.enabled,
    }


@app.get("/api/testnet/readiness")
async def testnet_readiness() -> dict:
    missing_variables = []
    if not settings.binance_api_key.strip():
        missing_variables.append("BINANCE_API_KEY")
    if not settings.binance_api_secret.strip():
        missing_variables.append("BINANCE_API_SECRET")
    return {
        "configured": testnet.configured,
        "missing_variables": missing_variables,
        "verified": testnet_health["verified"],
        "last_checked_at": testnet_health["last_checked_at"],
        "last_error": testnet_health["last_error"],
        "trading_mode": settings.trading_mode,
        "testnet_execution_enabled": settings.trading_mode == "TESTNET",
        "live_execution_enabled": False,
        "next_step": "Verify credentials and order.test while TRADING_MODE remains PAPER",
    }


@app.post("/api/testnet/verify")
async def testnet_verify() -> dict:
    if not testnet.configured:
        missing = []
        if not settings.binance_api_key.strip():
            missing.append("BINANCE_API_KEY")
        if not settings.binance_api_secret.strip():
            missing.append("BINANCE_API_SECRET")
        raise HTTPException(
            status_code=400,
            detail=f"Railway не передав змінні: {', '.join(missing)}. Додайте їх до цього сервісу та виконайте Redeploy.",
        )
    try:
        result = await testnet.verify()
        testnet_health.update({
            "verified": True, "last_checked_at": datetime.now(timezone.utc).isoformat(), "last_error": "",
        })
        return {**result, **testnet_health, "trading_mode": settings.trading_mode}
    except BinanceTestnetError as exc:
        testnet_health.update({
            "verified": False, "last_checked_at": datetime.now(timezone.utc).isoformat(),
            "last_error": str(exc),
        })
        raise HTTPException(status_code=502, detail=f"Testnet verification failed: {exc}") from exc


def require_testnet_mode() -> None:
    if settings.trading_mode != "TESTNET":
        raise HTTPException(status_code=409, detail="Для тестових заявок встановіть TRADING_MODE=TESTNET і виконайте Redeploy")
    if not testnet.configured:
        raise HTTPException(status_code=400, detail="Ключі Binance Spot Testnet не налаштовані")


@app.get("/api/testnet/account")
async def testnet_account() -> dict:
    require_testnet_mode()
    try:
        return {"balances": await testnet_engine.balances(), "virtual_funds": True}
    except BinanceTestnetError as exc:
        raise HTTPException(status_code=502, detail=f"Testnet: {exc}") from exc


@app.get("/api/testnet/grid-bot")
async def testnet_grid_bot() -> dict:
    return {
        "enabled": settings.trading_mode == "TESTNET",
        "trading_mode": settings.trading_mode,
        "bot": testnet_engine.bot.snapshot() if testnet_engine.bot else None,
        "virtual_funds": True, "live_execution_enabled": False,
    }


@app.post("/api/testnet/grid-bot/sync")
async def testnet_grid_sync() -> dict:
    require_testnet_mode()
    if not testnet_engine.bot:
        raise HTTPException(status_code=404, detail="TESTNET-бота немає")
    bot = await testnet_engine.sync()
    return bot.snapshot()


@app.get("/api/testnet/reconciliation")
async def testnet_reconciliation() -> dict:
    global testnet_audit_cache
    require_testnet_mode()
    if not testnet_engine.bot:
        raise HTTPException(status_code=404, detail="TESTNET-бота немає")
    try:
        result = await asyncio.wait_for(
            testnet_engine.reconciliation(), timeout=TESTNET_AUDIT_TIMEOUT_SECONDS,
        )
        result.update(cached=False, stale=False)
        testnet_audit_cache = result
        return result
    except (TimeoutError, ValueError, BinanceTestnetError) as exc:
        reason = (
            f"Binance Testnet не відповів за {TESTNET_AUDIT_TIMEOUT_SECONDS} с"
            if isinstance(exc, TimeoutError) else describe_exception(exc)
        )
        if isinstance(exc, TimeoutError):
            testnet_engine._record_sync_health(
                testnet_engine.bot, False, TESTNET_AUDIT_TIMEOUT_SECONDS * 1000, reason,
            )
            testnet_engine._save()
        if testnet_audit_cache and testnet_audit_cache.get("symbol") == testnet_engine.bot.symbol:
            fallback = dict(testnet_audit_cache)
            fallback["issues"] = [
                *testnet_audit_cache.get("issues", []),
                {"severity": "WARNING", "message": f"Показано останній успішний аудит: {reason}"},
            ]
            fallback["stability"] = testnet_engine._stability_summary()
            fallback["live_readiness"] = testnet_engine._live_readiness("ERROR")
            fallback.update(status="WARNING", cached=True, stale=True, cache_reason=reason)
            return fallback
        return {
            "status": "ERROR", "checked_at": datetime.now(timezone.utc).isoformat(),
            "symbol": testnet_engine.bot.symbol, "tracked_open_orders": None,
            "exchange_open_orders": None, "used_budget": None,
            "issues": [
                {"severity": "WARNING", "message": reason},
                {"severity": "WARNING", "message": "Готовність оцінюється консервативно до свіжої відповіді Binance"},
            ],
            "cached": False, "stale": True, "cache_reason": reason,
            "live_readiness": testnet_engine._live_readiness("ERROR"),
            "stability": testnet_engine._stability_summary(),
        }


@app.get("/api/testnet/recovery-preview")
async def testnet_recovery_preview() -> dict:
    require_testnet_mode()
    if not testnet_engine.bot:
        raise HTTPException(status_code=404, detail="TESTNET-бота немає")
    try:
        return await testnet_engine.recovery_preview()
    except (ValueError, BinanceTestnetError) as exc:
        raise HTTPException(status_code=409, detail=f"Відновлення TESTNET: {exc}") from exc


@app.post("/api/testnet/recover-after-reset")
async def testnet_recover_after_reset(request: TestnetRecoveryRequest) -> dict:
    require_testnet_mode()
    if not request.confirmed:
        raise HTTPException(status_code=400, detail="Потрібне явне підтвердження відновлення")
    try:
        return await testnet_engine.recover_after_reset(request.expected_order_ids)
    except (ValueError, BinanceTestnetError) as exc:
        raise HTTPException(status_code=409, detail=f"Відновлення TESTNET: {exc}") from exc


@app.get("/api/testnet/market/{symbol}")
async def testnet_market_snapshot(symbol: str) -> dict:
    require_testnet_mode()
    try:
        ticker = await testnet.ticker_24h(symbol)
        return {
            "symbol": symbol.upper(), "price": float(ticker["lastPrice"]),
            "change_24h_pct": float(ticker["priceChangePercent"]),
            "volume_24h": float(ticker["volume"]), "quote_volume_24h": float(ticker["quoteVolume"]),
            "environment": "TESTNET",
        }
    except BinanceTestnetError as exc:
        raise HTTPException(status_code=502, detail=f"Testnet market data: {exc}") from exc


@app.get("/api/testnet/market/{symbol}/klines")
async def testnet_market_klines(symbol: str, interval: str = "1h", limit: int = 120) -> dict:
    require_testnet_mode()
    allowed = {"1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"}
    if interval not in allowed or limit < 2 or limit > 500:
        raise HTTPException(status_code=400, detail="Некоректний інтервал або кількість свічок")
    try:
        rows = await testnet.klines(symbol, interval, limit)
        return {"symbol": symbol.upper(), "interval": interval, "environment": "TESTNET", "candles": [
            {"open_time": int(row[0]), "open": float(row[1]), "high": float(row[2]),
             "low": float(row[3]), "close": float(row[4]), "close_time": int(row[6])}
            for row in rows
        ]}
    except BinanceTestnetError as exc:
        raise HTTPException(status_code=502, detail=f"Testnet candles: {exc}") from exc


@app.post("/api/testnet/grid-bot/start")
async def testnet_grid_start(request: TestnetGridStartRequest) -> dict:
    require_testnet_mode()
    try:
        symbol = request.symbol.upper()
        base_asset_from_symbol(symbol)
        price = await testnet.price(symbol)
        bot = await testnet_engine.start(symbol, request.budget_quote, request.step_pct, request.levels, price)
        if notifier.enabled:
            await notifier.send("TESTNET_BOT_STARTED", f"🧪 {symbol} · TESTNET\nСітку запущено на віртуальних коштах.\nБюджет: {request.budget_quote:.2f} USDT")
        return bot.snapshot()
    except (ValueError, BinanceTestnetError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/testnet/grid-bot/stop-buys")
async def testnet_grid_stop_buys() -> dict:
    require_testnet_mode()
    try:
        return (await testnet_engine.stop_buys()).snapshot()
    except (ValueError, BinanceTestnetError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/testnet/grid-bot/soft-complete")
async def testnet_grid_soft_complete() -> dict:
    require_testnet_mode()
    try:
        return (await testnet_engine.stop_buys(soft_complete=True)).snapshot()
    except (ValueError, BinanceTestnetError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/testnet/grid-bot/stop")
async def testnet_grid_stop() -> dict:
    require_testnet_mode()
    try:
        return (await testnet_engine.stop()).snapshot()
    except (ValueError, BinanceTestnetError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/testnet/grid-bot/emergency-stop")
async def testnet_grid_emergency_stop() -> dict:
    require_testnet_mode()
    try:
        return (await testnet_engine.emergency_stop()).snapshot()
    except (ValueError, BinanceTestnetError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


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
    signal_engine.reset()
    return {"status": "reset", "portfolio": portfolio.snapshot(), "grid_bots": [], "dca_bots": []}


@app.get("/api/signal/bots")
async def signal_bots() -> dict:
    return {"bots": signal_engine.list_bots()}


@app.post("/api/signal/analyze")
async def signal_analyze(request: SignalStartRequest) -> dict:
    try:
        symbol = await ensure_active_quote_symbol(request.symbol)
        data = await signal_analysis(symbol, base_asset_from_symbol(symbol))
        atr = max(0.1, float(data["atr_pct"]))
        expected_pct = round(max(1.5, min(8.0, atr * 1.5)), 2)
        risk_pct = round(max(1.0, min(5.0, atr)), 2)
        ready = (data["score"] >= request.min_score and data["price"] > data["ema20"] > data["ema50"]
                 and 45 <= data["rsi14"] <= 68 and data["volume_ratio"] >= 1.1)
        fees = request.budget_quote * broker.fee_rate * 2
        return {
            "symbol": symbol, "entry_ready": ready, "analysis": data,
            "expected_return_pct": expected_pct, "risk_pct": risk_pct,
            "target_price": data["price"] * (1 + expected_pct / 100),
            "stop_price": data["price"] * (1 - risk_pct / 100),
            "possible_net_profit_quote": request.budget_quote * expected_pct / 100 - fees,
            "possible_loss_quote": request.budget_quote * risk_pct / 100 + fees,
            "budget": automation_budget_status(symbol, request.budget_quote),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Signal analysis failed: {exc}") from exc


@app.post("/api/signal/bots/start")
async def signal_start(request: SignalStartRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Сигнальний бот доступний лише в PAPER")
    try:
        symbol = await ensure_active_quote_symbol(request.symbol)
        budget = automation_budget_status(symbol, request.budget_quote)
        if not budget["allowed"]:
            raise ValueError("; ".join(budget["reasons"]))
        bot = signal_engine.start_bot(symbol, base_asset_from_symbol(symbol), request.budget_quote, request.min_score)
        return (await signal_engine.tick_bot(bot.id)).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Signal bot start failed: {exc}") from exc


@app.post("/api/signal/bots/{bot_id}/pause")
async def signal_pause(bot_id: str) -> dict:
    try:
        return signal_engine.pause_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/signal/bots/{bot_id}/resume")
async def signal_resume(bot_id: str) -> dict:
    return signal_engine.resume_bot(bot_id).snapshot()


@app.post("/api/signal/bots/{bot_id}/stop")
async def signal_stop(bot_id: str) -> dict:
    try:
        return signal_engine.stop_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


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


@app.post("/api/dca/preflight")
async def dca_preflight(request: DcaStartRequest) -> dict:
    try:
        return await dca_preflight_analysis(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"DCA preflight failed: {exc}") from exc


@app.post("/api/dca/bots/start")
async def dca_start(request: DcaStartRequest) -> dict:
    if settings.trading_mode not in {"PAPER", "TESTNET"}:
        raise HTTPException(status_code=409, detail="DCA execution is PAPER-only")
    try:
        checked = await dca_preflight_analysis(request)
        if not checked["allowed"]:
            reasons = checked["warnings"] or ["ринкові або портфельні обмеження"]
            raise ValueError("DCA launch blocked: " + "; ".join(reasons))
        symbol = await ensure_active_quote_symbol(request.symbol)
        base_asset = base_asset_from_symbol(symbol)
        price = await current_price(symbol)
        return dca_engine.start_bot(
            symbol=symbol, base_asset=base_asset, reference_price=price,
            budget_quote=request.budget_quote, order_quote=request.order_quote,
            interval_seconds=request.interval_minutes * 60,
            dip_trigger_pct=request.dip_trigger_pct,
            max_drawdown_pct=request.max_drawdown_pct,
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
    if settings.trading_mode not in {"PAPER", "TESTNET"}:
        raise HTTPException(status_code=409, detail="Grid preflight is available for the PAPER circuit only")
    try:
        return await grid_preflight_analysis(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid preflight failed: {exc}") from exc


async def start_paper_grid_bot(request: GridStartRequest, preflight: dict | None = None):
    symbol = await ensure_active_quote_symbol(request.symbol)
    budget_status = automation_budget_status(symbol, request.budget_quote)
    if not budget_status["allowed"]:
        raise ValueError("; ".join(budget_status["reasons"]))
    checked = preflight or await grid_preflight_analysis(request)
    if not checked["strategy"]["launch_allowed"]:
        raise ValueError(checked["recommendation"])
    profile = checked["strategy"]["resolved_profile"]
    seed_position_pct = {
        "UPTREND_HYBRID_10": 10.0,
        "UPTREND_HYBRID_20": 20.0,
        "UPTREND_HYBRID_30": 30.0,
    }.get(profile, 0.0)
    return grid_engine.start_bot(
        symbol=symbol,
        base_asset=base_asset_from_symbol(symbol),
        reference_price=await current_price(symbol),
        budget_quote=request.budget_quote,
        step_pct=request.step_pct,
        levels_each_side=request.levels_each_side,
        trailing_up_enabled=request.trailing_up_enabled or profile != "RANGE_GRID",
        strategy_profile=profile,
        seed_position_pct=seed_position_pct,
        price_floor=checked["parameters"]["price_floor"],
        price_ceiling=checked["parameters"]["price_ceiling"],
    )


@app.post("/api/grid/bots/start")
async def grid_start(request: GridStartRequest) -> dict:
    if settings.trading_mode not in {"PAPER", "TESTNET"}:
        raise HTTPException(status_code=409, detail="Grid execution is PAPER-only")
    try:
        return (await start_paper_grid_bot(request)).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Grid start failed: {exc}") from exc


@app.get("/api/grid/schedules")
async def grid_schedules() -> dict:
    return {"schedules": scheduled_grid_launches, "paper_only": True}


@app.post("/api/grid/schedules")
async def grid_schedule(request: ScheduledGridLaunchRequest) -> dict:
    if settings.trading_mode not in {"PAPER", "TESTNET"}:
        raise HTTPException(status_code=409, detail="Scheduled Grid is PAPER-only")
    if request.min_rsi14 > request.max_rsi14:
        raise HTTPException(status_code=400, detail="Мінімальний RSI не може перевищувати максимальний")
    allowed_regimes = sorted(set(request.allowed_regimes))
    if not allowed_regimes or not set(allowed_regimes) <= {"RANGE", "UPTREND", "UNCERTAIN"}:
        raise HTTPException(status_code=400, detail="Некоректні дозволені режими ринку")
    now = datetime.now(timezone.utc)
    starts_at = now + timedelta(hours=request.start_after_hours)
    expires_at = starts_at + timedelta(hours=request.launch_window_hours)
    launch_request = request.model_dump(exclude={
        "start_after_hours", "launch_window_hours", "min_rsi14", "max_rsi14",
        "max_abs_change_24h_pct", "allowed_regimes",
    })
    launch_request["symbol"] = launch_request["symbol"].upper().strip()
    if any(
        bot.symbol == launch_request["symbol"] and bot.status != "STOPPED"
        for bot in grid_engine.bots.values()
    ):
        raise HTTPException(
            status_code=409,
            detail=f"Для {launch_request['symbol']} вже працює PAPER Grid-бот; дубль не заплановано",
        )
    duplicate = next((
        row for row in scheduled_grid_launches
        if row.get("status") == "PENDING" and row.get("symbol") == launch_request["symbol"]
    ), None)
    if duplicate is not None:
        raise HTTPException(
            status_code=409,
            detail=f"Для {launch_request['symbol']} вже є активний план запуску {duplicate['id']}",
        )
    item = {
        "id": secrets.token_hex(6), "symbol": launch_request["symbol"], "status": "PENDING",
        "created_at": now.isoformat(), "starts_at": starts_at.isoformat(), "expires_at": expires_at.isoformat(),
        "last_checked_at": "", "last_reason": "Очікує початку вікна", "attempts": 0,
        "request": launch_request,
        "conditions": {
            "min_rsi14": request.min_rsi14, "max_rsi14": request.max_rsi14,
            "max_abs_change_24h_pct": request.max_abs_change_24h_pct,
            "allowed_regimes": allowed_regimes,
        },
    }
    scheduled_grid_launches.append(item)
    _save_scheduled_grid_launches()
    return item


@app.delete("/api/grid/schedules/{schedule_id}")
async def grid_schedule_cancel(schedule_id: str) -> dict:
    item = next((row for row in scheduled_grid_launches if row["id"] == schedule_id), None)
    if item is None:
        raise HTTPException(status_code=404, detail="План запуску не знайдено")
    if item["status"] != "PENDING":
        raise HTTPException(status_code=409, detail="Цей план уже не очікує запуску")
    item.update(status="CANCELLED", last_reason="Скасовано користувачем")
    _save_scheduled_grid_launches()
    return item


@app.post("/api/grid/bots/{bot_id}/trailing-up")
async def grid_trailing_up(bot_id: str, request: GridTrailingRequest) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Trailing Up is PAPER-only")
    try:
        return grid_engine.set_trailing_up(bot_id, request.enabled).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/grid/bots/{bot_id}/soft-complete")
async def grid_soft_complete(bot_id: str) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Soft completion is PAPER-only")
    try:
        bot = grid_engine.start_draining(bot_id)
        store.record_decision(kind="ACTION", source="WEB", status="EXECUTED",
                              symbol=bot.symbol, bot_id=bot.id, action="SOFT_COMPLETE")
        return bot.snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/grid/bots/{bot_id}/buy-control")
async def grid_buy_control(bot_id: str, request: GridTrailingRequest) -> dict:
    try:
        bot = grid_engine.set_manual_buy_pause(bot_id, not request.enabled)
        store.record_decision(kind="ACTION", source="WEB", status="EXECUTED",
                              symbol=bot.symbol, bot_id=bot.id,
                              action="RESUME_BUYS" if request.enabled else "STOP_BUYS")
        return bot.snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/api/grid/bots/{bot_id}/stop")
async def grid_stop(bot_id: str) -> dict:
    try:
        return grid_engine.stop_bot(bot_id).snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/grid/bots/{bot_id}/liquidate")
async def grid_liquidate(bot_id: str) -> dict:
    if settings.trading_mode != "PAPER":
        raise HTTPException(status_code=409, detail="Emergency liquidation is PAPER-only")
    try:
        bot = grid_engine.liquidate_bot(bot_id)
        store.record_decision(kind="ACTION", source="WEB", status="EXECUTED",
                              symbol=bot.symbol, bot_id=bot.id, action="LIQUIDATE")
        return bot.snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/api/grid/bots/{bot_id}/pause")
async def grid_pause(bot_id: str) -> dict:
    try:
        bot = grid_engine.pause_bot(bot_id)
        store.record_decision(kind="ACTION", source="WEB", status="EXECUTED",
                              symbol=bot.symbol, bot_id=bot.id, action="PAUSE")
        return bot.snapshot()
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
    snapshot = bot.snapshot() if callable(getattr(bot, "snapshot", None)) else {}
    manually_idle_and_empty = (
        bool(snapshot.get("manual_buy_paused", getattr(bot, "manual_buy_paused", False)))
        and float(snapshot.get("grid_open_exposure_quote", 0.0)) <= 1e-9
        and float(snapshot.get("seed_value_quote", 0.0)) <= 1e-9
        and int(snapshot.get("open_sell_orders", 0)) == 0
    )
    if bot.status == "PAUSED":
        code, label, message = "PAUSED", "Потребує уваги", bot.paused_reason or "Бот призупинений"
    elif bot.consecutive_errors:
        code, label, message = "ERROR", "Помилка", f"Помилок поспіль: {bot.consecutive_errors}"
    elif bot.recenter_count_today >= bot.max_recenters_per_day:
        code, label, message = "TRAILING_LIMIT", "Потребує уваги", "Досягнуто добовий ліміт Trailing"
    elif manually_idle_and_empty:
        code, label, message = "INACTIVE_EMPTY", "Очікує рішення", "Покупки вимкнені, відкритих позицій немає"
    elif idle_hours >= 48:
        code, label, message = "IDLE", "Немає угод", f"Без виконаних ордерів {idle_hours:.0f} год"
    else:
        code, label, message = "NORMAL", "Працює нормально", "Критичних сигналів немає"
    return {
        "code": code, "label": label, "message": message,
        "idle_hours": round(idle_hours, 1), "last_fill_at": max(fill_times).isoformat() if fill_times else None,
        "needs_attention": code not in {"NORMAL", "INACTIVE_EMPTY"},
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


@app.get("/api/analytics/advisor")
async def analytics_advisor() -> dict:
    active = [bot for bot in grid_engine.bots.values() if bot.status != "STOPPED"]
    fresh_results = await asyncio.gather(*(
        signal_analysis(bot.symbol, bot.base_asset) for bot in active
    ), return_exceptions=True)
    fresh_analysis = {
        bot.symbol: result for bot, result in zip(active, fresh_results)
        if not isinstance(result, Exception)
    }
    prices = {bot.base_asset: bot.last_price for bot in active if bot.last_price > 0}
    portfolio_state = portfolio.snapshot(prices)
    scanner_items = ((market_scanner_cache.get("result") or {}).get("items") or [])
    scanner_by_symbol = {item["symbol"]: item for item in scanner_items}
    now = datetime.now(timezone.utc)
    rows = []
    for bot in active:
        state = bot.snapshot()
        performance = grid_performance(portfolio.trades, grid_engine.bots, 30, bot.symbol)
        metrics = performance["metrics"]
        elapsed_days = performance["elapsed_hours"] / 24
        deployed = state["grid_open_exposure_quote"] + state["seed_value_quote"]
        deployed_pct = deployed / bot.budget_quote * 100 if bot.budget_quote else 0
        total_pct = state["total_pnl"] / bot.budget_quote * 100 if bot.budget_quote else 0
        unrealized_pct = state["unrealized_pnl"] / bot.budget_quote * 100 if bot.budget_quote else 0
        evaluated_pnl = metrics.get("hybrid_total_pnl", metrics["realized_pnl"])
        gross_cycle_profit = evaluated_pnl + metrics["fees"]
        fee_drag = metrics["fees"] / gross_cycle_profit * 100 if gross_cycle_profit > 0 else (100.0 if metrics["fees"] else 0.0)
        open_buy_prices = [
            order.source_buy_price for order in bot.open_orders
            if order.side == "SELL" and order.source_buy_price > 0
        ]
        matching_buys = [
            datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
            for event in bot.events
            if event.event == "BUY_FILLED" and any(
                abs(event.price - price) <= max(1e-9, price * 1e-8) for price in open_buy_prices
            )
        ]
        oldest_open_hours = max(0.0, (now - min(matching_buys)).total_seconds() / 3600) if matching_buys else 0.0
        market_item = fresh_analysis.get(bot.symbol) or scanner_by_symbol.get(bot.symbol)
        automation = recommend_automation(market_item) if market_item else {
            "strategy": "UNKNOWN", "profile": "UNKNOWN", "reason": "Актуальний аналіз ринку недоступний",
            "signal_ready": False, "signal_score": 0,
        }
        if not market_item:
            market_regime = "НЕ ОЦІНЕНО"
        else:
            market_regime = {
                "DOWNTREND": "СПАД", "UPTREND": "ЗРОСТАННЯ", "RANGE": "БОКОВИЙ",
                "OVERHEATED": "ПЕРЕГРІТИЙ", "UNCERTAIN": "НЕВИЗНАЧЕНИЙ",
            }.get(market_item.get("regime", {}).get("name"), "НЕВИЗНАЧЕНИЙ")
        severity = "GOOD"
        title = "Продовжувати PAPER-збір"
        reasons = []
        actions = [{"code": "OPEN", "label": "Відкрити бота"}]

        if bot.consecutive_errors:
            severity, title = "DANGER", "Перевірити помилки й призупинити"
            reasons.append(f"Помилок поспіль: {bot.consecutive_errors}")
            if bot.status == "RUNNING":
                actions.append({"code": "PAUSE", "label": "Пауза"})
        elif total_pct <= -3:
            severity, title = "DANGER", "Не збільшувати позицію; розглянути м’яке завершення"
            reasons.append(f"Загальний результат становить {total_pct:.2f}% бюджету")
            if bot.status == "RUNNING" and not state["manual_buy_paused"]:
                actions.append({"code": "STOP_BUYS", "label": "Зупинити нові покупки"})
            if bot.status == "RUNNING" and not state["drain_mode"]:
                actions.append({"code": "SOFT_COMPLETE", "label": "М’яко завершити"})
        elif deployed_pct >= 90 and market_regime == "СПАД":
            severity, title = "DANGER", "Увесь бюджет залучений під час спаду — зупинити нові покупки"
            reasons.append(f"Залучено {deployed_pct:.0f}% бюджету, ринковий режим: спад")
            reasons.append(f"Незакритий результат: {unrealized_pct:.2f}% бюджету")
            if bot.status == "RUNNING" and not state["manual_buy_paused"]:
                actions.append({"code": "STOP_BUYS", "label": "Зупинити нові покупки"})
        elif deployed_pct >= 90 and unrealized_pct <= -1:
            severity, title = "WARNING", "Увесь бюджет залучений — зупинити нові покупки"
            reasons.append(f"Залучено {deployed_pct:.0f}% бюджету, незакритий результат {unrealized_pct:.2f}%")
            if bot.status == "RUNNING" and not state["manual_buy_paused"]:
                actions.append({"code": "STOP_BUYS", "label": "Зупинити нові покупки"})
        elif state["total_pnl"] > 0 and fee_drag <= 40:
            severity, title = "GOOD", "Поточні параметри виглядають здорово — продовжувати"
            reasons.append("Загальний результат позитивний, вплив комісій прийнятний")
        elif metrics["fees"] > 0 and fee_drag > 40:
            severity, title = "WARNING", "Комісії поглинають прибуток — не змінювати поточний експеримент"
            reasons.append(f"Вплив комісій на зафіксований результат: {fee_drag:.0f}%")
            if bot.status == "RUNNING" and not state["manual_buy_paused"]:
                actions.append({"code": "STOP_BUYS", "label": "Зупинити нові покупки"})
        else:
            severity, title = "NEUTRAL", "Недостатньо даних — спостерігати"
            reasons.append("Поточний результат близький до нуля")

        if automation["strategy"] == "SIGNAL":
            reasons.append(f"Нова оцінка рекомендує Сигнальну стратегію: {automation['reason']}")
            if severity in {"GOOD", "NEUTRAL"}:
                severity, title = "WARNING", "Поведінка ринку змінилася — розглянути Сигнальну стратегію"
            actions.append({"code": "PREPARE_SIGNAL", "label": "Підготувати Signal"})
        elif automation["strategy"] == "WAIT" and market_regime != "НЕ ОЦІНЕНО":
            reasons.append("Для нового входу зараз рекомендовано чекати, а не змінювати стратегію поспіхом")

        if elapsed_days < 7 or metrics["cycles"] < 20:
            reasons.append(f"Статистика ще не готова: {elapsed_days:.1f}/7 днів і {metrics['cycles']}/20 циклів")
        if metrics["win_rate_pct"] == 100 and state["unrealized_pnl"] < 0:
            reasons.append("100% виграшних циклів не враховує збиток відкритих позицій")
        if bot.recenter_count_today >= bot.max_recenters_per_day:
            reasons.append("Досягнуто добовий ліміт зсувів сітки")
        if oldest_open_hours >= 24:
            reasons.append(f"Найстаріша відкрита позиція утримується близько {oldest_open_hours:.0f} год")
        if severity == "DANGER" and deployed > 0:
            actions.append({"code": "LIQUIDATE", "label": "Продати все зараз"})

        rows.append({
            "bot_id": bot.id, "symbol": bot.symbol, "profile": state["strategy_profile"],
            "status": bot.status, "severity": severity, "recommendation": title,
            "reasons": reasons, "actions": actions,
            "metrics": {
                "budget_quote": bot.budget_quote, "total_pnl": state["total_pnl"],
                "total_return_pct": total_pct, "realized_pnl": evaluated_pnl,
                "unrealized_pnl": state["unrealized_pnl"], "fees": metrics["fees"],
                "fee_drag_pct": fee_drag, "cycles": metrics["cycles"],
                "elapsed_days": elapsed_days, "deployed_quote": deployed,
                "deployed_pct": deployed_pct, "win_rate_pct": metrics["win_rate_pct"],
                "oldest_open_hours": oldest_open_hours, "market_regime": market_regime,
                "recommended_strategy": automation["strategy"],
                "signal_score": automation["signal_score"],
            },
        })
    rows.sort(key=lambda row: ({"DANGER": 0, "WARNING": 1, "NEUTRAL": 2, "GOOD": 3}[row["severity"]], row["metrics"]["total_pnl"]))
    store.evaluate_decisions({row["bot_id"]: row["metrics"]["total_pnl"] for row in rows})
    journal_day = datetime.now(timezone.utc).date().isoformat()
    for row in rows:
        action_codes = sorted(action["code"] for action in row["actions"] if action["code"] != "OPEN")
        fingerprint_source = "|".join([
            journal_day, row["bot_id"], row["severity"], row["recommendation"], *action_codes,
        ])
        store.record_decision(
            kind="RECOMMENDATION", source="ADVISOR", status="PROPOSED",
            symbol=row["symbol"], bot_id=row["bot_id"], action=",".join(action_codes),
            recommendation=row["recommendation"], reasons=row["reasons"],
            context={"metrics": row["metrics"]},
            fingerprint=hashlib.sha256(fingerprint_source.encode()).hexdigest(),
        )
    allocated = sum(bot.budget_quote for bot in active)
    deployed = sum(row["metrics"]["deployed_quote"] for row in rows)
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "active_bots": len(active), "starting_balance": portfolio.starting_quote,
            "total_equity": portfolio_state["total_equity"],
            "total_result": portfolio_state["total_equity"] - portfolio.starting_quote,
            "realized_pnl": portfolio_state["realized_pnl"],
            "unrealized_pnl": portfolio_state["unrealized_pnl"],
            "fees": portfolio_state["fees_paid"], "allocated_budget": allocated,
            "deployed_quote": deployed,
            "capital_utilization_pct": deployed / allocated * 100 if allocated else 0,
        },
        "bots": rows,
        "caveat": (
            "Загальний результат дорівнює чистому зафіксованому та ринковому результату відкритих позицій. "
            "У картках значення «Якщо продати зараз» додатково враховує очікувану комісію продажу та облік окремих лотів."
        ),
    }


def portfolio_risk_status() -> dict:
    active = [bot for bot in grid_engine.bots.values() if bot.status != "STOPPED"]
    active_dca = [bot for bot in dca_engine.bots.values() if bot.status not in {"STOPPED", "COMPLETED"}]
    prices = {
        **{bot.base_asset: bot.last_price for bot in active if bot.last_price > 0},
        **{bot.base_asset: bot.last_price for bot in active_dca if bot.last_price > 0},
    }


def telegram_risk_message() -> str:
    data = portfolio_risk_status()
    metrics = data["metrics"]
    lines = [
        f"🛡 Портфельний ризик: {'БЕЗПЕЧНО' if data['status'] == 'SAFE' else 'ПОКУПКИ ЗАБЛОКОВАНО'}",
        f"Залучено: {metrics['allocated_quote']:.2f} / "
        f"{metrics['total_equity'] * data['limits']['max_portfolio_allocation_pct'] / 100:.2f} USDT",
        f"Вільно: {metrics['free_quote']:.2f} USDT",
        f"Результат за добу: {metrics['daily_realized_pnl']:+.2f} USDT",
        f"Позиції: {metrics['active_positions']} / {data['limits']['max_open_positions']}",
    ]
    if data["reasons"]:
        lines.extend(f"• {reason}" for reason in data["reasons"])
    return "\n".join(lines)


def telegram_journal_message(limit: int = 10) -> str:
    rows = store.list_decisions(limit=limit)
    if not rows:
        return "📒 Журнал рішень поки порожній. Відкрийте Аналітику для формування рекомендацій."
    labels = {
        "PROPOSED": "запропоновано", "AWAITING_CONFIRMATION": "очікує підтвердження",
        "EXECUTED": "виконано", "SKIPPED": "пропущено",
    }
    lines = ["📒 Останні рішення"]
    for row in rows:
        created = datetime.fromisoformat(row["created_at"].replace("Z", "+00:00"))
        when = created.astimezone(ZoneInfo("Europe/Kiev")).strftime("%d.%m %H:%M")
        subject = row["symbol"] or "Портфель"
        decision = row["action"] or row["recommendation"] or row["kind"]
        outcome = ""
        if row.get("outcome_72h_status"):
            outcome = f" · 72г {row['outcome_72h_status']} {row['outcome_72h_delta']:+.2f}"
        elif row.get("outcome_24h_status"):
            outcome = f" · 24г {row['outcome_24h_status']} {row['outcome_24h_delta']:+.2f}"
        lines.append(f"• {when} · {subject} · {decision} · {labels.get(row['status'], row['status'])}{outcome}")
    return "\n".join(lines)
    snapshot = portfolio.snapshot(prices)
    today = datetime.now(timezone.utc).date()
    daily_realized = sum(
        trade.realized_pnl - trade.fee_quote
        for trade in portfolio.trades
        if datetime.fromisoformat(trade.timestamp.replace("Z", "+00:00")).date() == today
    )
    allocated = sum(
        position["market_value"] for position in snapshot["positions"]
    )
    result = risk_manager.status(
        total_equity=snapshot["total_equity"], free_quote=snapshot["quote_balance"],
        allocated_quote=allocated, daily_realized_pnl=daily_realized,
        active_positions=len(snapshot["positions"]),
    )
    return {
        **result,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "metrics": {
            "total_equity": snapshot["total_equity"], "free_quote": snapshot["quote_balance"],
            "allocated_quote": allocated, "daily_realized_pnl": daily_realized,
            "active_positions": len(snapshot["positions"]),
        },
    }


@app.get("/api/risk/status")
async def risk_status() -> dict:
    return portfolio_risk_status()


@app.get("/api/decisions")
async def decision_journal(limit: int = 100, symbol: str = "") -> dict:
    return {"decisions": store.list_decisions(limit=limit, symbol=symbol)}


async def telegram_daily_advisor() -> str:
    data = await analytics_advisor()
    lines = ["🧭 Рекомендації аналітичного помічника"]
    actionable = 0
    for row in data["bots"]:
        actions = [
            action["label"] for action in row["actions"]
            if action["code"] not in {"OPEN", "LIQUIDATE"}
        ]
        if not actions and row["severity"] not in {"DANGER", "WARNING"}:
            continue
        actionable += 1
        lines.append(f"• {row['symbol']}: {row['recommendation']}")
        if actions:
            lines.append(f"  Пропозиція: {', '.join(actions)}")
        elif row["severity"] == "DANGER":
            lines.append("  Пропозиція: спостерігати; продаж зі збитком лише за окремим рішенням")
    if not actionable:
        lines.append("Термінових змін не рекомендовано.")
    lines.extend([
        "",
        "Нічого не виконується автоматично.",
        "Напишіть у чаті: «виконай рекомендацію для ПАРА» або «не виконувати».",
    ])
    return "\n".join(lines)


notifier.daily_advisor_provider = telegram_daily_advisor


telegram_pending_actions: dict[str, dict] = {}
TELEGRAM_CONFIRMATION_TTL = timedelta(minutes=10)


def _telegram_confirmation(kind: str, payload: dict) -> str:
    token = secrets.token_hex(4).upper()
    telegram_pending_actions.clear()
    telegram_pending_actions[token] = {
        "kind": kind, "payload": payload,
        "expires_at": datetime.now(timezone.utc) + TELEGRAM_CONFIRMATION_TTL,
    }
    return token


def _testnet_audit_message(audit: dict) -> str:
    issues = audit.get("issues") or []
    lines = [
        f"🔄 TESTNET синхронізовано: {audit['status']}",
        f"Заявки: локально {audit['tracked_open_orders']} / Binance {audit['exchange_open_orders']}",
    ]
    if issues:
        lines.extend(f"• {row['message']}" for row in issues)
    else:
        lines.append("Розбіжностей не виявлено.")
    return "\n".join(lines)


async def _telegram_recommendation_plan() -> tuple[list[dict], list[str]]:
    data = await analytics_advisor()
    plan, notes = [], []
    for row in data["bots"]:
        codes = {action["code"] for action in row["actions"]}
        action = None
        if "SOFT_COMPLETE" in codes:
            action = "SOFT_COMPLETE"
        elif "STOP_BUYS" in codes:
            action = "STOP_BUYS"
        if action:
            plan.append({
                "bot_id": row["bot_id"], "symbol": row["symbol"],
                "action": action, "recommendation": row["recommendation"],
            })
        elif "PAUSE" in codes:
            notes.append(f"{row['symbol']}: паузу не включено до масового плану, щоб не зупинити продажі")
    return plan, notes


def _telegram_grid_bot(symbol: str):
    normalized = symbol.upper().strip()
    return next((
        bot for bot in grid_engine.bots.values()
        if bot.symbol == normalized and bot.status != "STOPPED"
    ), None)


async def telegram_command(text: str) -> str:
    parts = text.strip().split()
    command = parts[0].split("@", 1)[0].lower()
    if command == "/help":
        return "\n".join([
            "🤖 Команди Spot Grid Lab",
            "/status — стан PAPER-портфеля",
            "/recommendations — актуальні рекомендації",
            "/risk — портфельні ліміти та дозвіл нових покупок",
            "/journal — останні рекомендації, дії та їх результат",
            "/apply_recommendations — план безпечних масових дій",
            "/pause_buys ПАРА — вимкнути нові покупки",
            "/resume_buys ПАРА — перевірити ринок і дозволити покупки",
            "/sync_testnet — синхронізувати і перевірити Binance Testnet",
            "/repair_testnet — безпечний план відновлення після reset",
            "/schedules — заплановані PAPER-запуски",
            "/cancel_schedule ID — скасувати очікуваний запуск",
            "/confirm КОД — підтвердити показаний план (чинний 10 хв)",
            "/cancel — скасувати очікуване підтвердження",
            "Продаж зі збитком і LIVE-операції через Telegram недоступні.",
        ])
    if command == "/status":
        return notifier.daily_report()
    if command == "/recommendations":
        return await telegram_daily_advisor()
    if command == "/risk":
        return telegram_risk_message()
    if command == "/journal":
        return telegram_journal_message()
    if command == "/schedules":
        pending = [row for row in scheduled_grid_launches if row["status"] == "PENDING"]
        recent = [row for row in reversed(scheduled_grid_launches) if row["status"] != "PENDING"][:3]
        if not pending and not recent:
            return "Планів PAPER-запуску ще немає."
        lines = ["🗓 PAPER-плани запуску"]
        for row in pending:
            starts = datetime.fromisoformat(row["starts_at"]).astimezone(ZoneInfo("Europe/Kiev")).strftime("%d.%m %H:%M")
            expires = datetime.fromisoformat(row["expires_at"]).astimezone(ZoneInfo("Europe/Kiev")).strftime("%d.%m %H:%M")
            lines.append(f"• ⏳ {row['symbol']} · {row['id']} · {starts}—{expires}\n  {row['last_reason']}")
        if recent:
            labels = {"STARTED": "✅ запущено", "CANCELLED": "↩️ скасовано", "EXPIRED": "⌛ вікно минуло"}
            lines.append("Останні завершені:")
            for row in recent:
                lines.append(f"• {row['symbol']} · {labels.get(row['status'], row['status'])}\n  {row['last_reason']}")
        return "\n".join(lines)
    if command == "/cancel_schedule":
        if len(parts) != 2:
            return "Формат: /cancel_schedule ID"
        row = next((item for item in scheduled_grid_launches if item["id"] == parts[1]), None)
        if row is None or row["status"] != "PENDING":
            return "Активний план із таким ID не знайдено."
        row.update(status="CANCELLED", last_reason="Скасовано через Telegram")
        _save_scheduled_grid_launches()
        return f"✅ {row['symbol']}: запланований PAPER-запуск скасовано."
    if command == "/cancel":
        telegram_pending_actions.clear()
        return "✅ Очікувану дію скасовано."
    if command == "/sync_testnet":
        if settings.trading_mode != "TESTNET" or not testnet_engine.bot:
            return "TESTNET-бот не активний або середовище не в режимі TESTNET."
        try:
            return _testnet_audit_message(await testnet_engine.reconciliation())
        except Exception as exc:
            return f"❌ TESTNET-синхронізація не вдалася: {describe_exception(exc)}"
    if command == "/repair_testnet":
        if settings.trading_mode != "TESTNET" or not testnet_engine.bot:
            return "TESTNET-бот не активний або середовище не в режимі TESTNET."
        try:
            preview = await testnet_engine.recovery_preview()
        except Exception as exc:
            return f"❌ Перевірка відновлення не вдалася: {describe_exception(exc)}"
        if not preview["recoverable"]:
            return f"⛔ Автоматичне відновлення заблоковано. {preview['reason']}"
        ids = preview["confirmed_missing_order_ids"]
        token = _telegram_confirmation("TESTNET_RECOVERY", {"order_ids": ids})
        return (
            f"⚠️ Binance підтвердив відсутність {len(ids)} локальних заявок. "
            f"Бота буде безпечно архівовано без нових заявок.\n"
            f"Для виконання: /confirm {token}"
        )
    if command == "/apply_recommendations":
        plan, notes = await _telegram_recommendation_plan()
        if not plan:
            return "✅ Безпечних масових дій зараз немає. Нічого не змінено."
        token = _telegram_confirmation("PAPER_RECOMMENDATIONS", {"plan": plan})
        for item in plan:
            store.record_decision(
                kind="ACTION", source="TELEGRAM", status="AWAITING_CONFIRMATION",
                symbol=item["symbol"], bot_id=item["bot_id"], action=item["action"],
                recommendation=item["recommendation"], context={"confirmation_token": token},
            )
        labels = {"STOP_BUYS": "вимкнути нові покупки", "SOFT_COMPLETE": "м’яко завершити"}
        lines = ["📋 План виконання:"]
        lines.extend(f"• {row['symbol']}: {labels[row['action']]}" for row in plan)
        lines.extend(f"• Примітка: {note}" for note in notes)
        lines.extend([
            "Продажу за ринком, LIVE-дії та відновлення покупок не включено.",
            f"Для виконання протягом 10 хв: /confirm {token}",
            "Скасувати: /cancel",
        ])
        return "\n".join(lines)
    if command == "/confirm":
        if len(parts) != 2:
            return "Формат: /confirm КОД"
        pending = telegram_pending_actions.pop(parts[1].upper(), None)
        if not pending or pending["expires_at"] < datetime.now(timezone.utc):
            return "Код недійсний або строк його дії минув. Створіть новий план."
        if pending["kind"] == "TESTNET_RECOVERY":
            try:
                result = await testnet_engine.recover_after_reset(pending["payload"]["order_ids"])
            except Exception as exc:
                return f"❌ Стан Binance змінився, відновлення не виконано: {describe_exception(exc)}"
            return f"✅ TESTNET-відновлення завершено. Архівовано заявок: {len(result['recovered_order_ids'])}."
        fresh_plan, _ = await _telegram_recommendation_plan()
        fresh_actions = {(row["bot_id"], row["action"]) for row in fresh_plan}
        completed, skipped = [], []
        for item in pending["payload"]["plan"]:
            bot = grid_engine.bots.get(item["bot_id"])
            if (
                not bot or bot.status != "RUNNING"
                or (item["bot_id"], item["action"]) not in fresh_actions
            ):
                skipped.append(f"{item['symbol']} (стан або рекомендація змінилися)")
                continue
            if item["action"] == "SOFT_COMPLETE":
                grid_engine.start_draining(bot.id)
                completed.append(f"{bot.symbol}: м’яке завершення")
            elif item["action"] == "STOP_BUYS":
                grid_engine.set_manual_buy_pause(bot.id, True, source="TELEGRAM_RECOMMENDATION")
                completed.append(f"{bot.symbol}: покупки вимкнено")
            store.record_decision(
                kind="ACTION", source="TELEGRAM", status="EXECUTED",
                symbol=bot.symbol, bot_id=bot.id, action=item["action"],
                recommendation=item["recommendation"],
            )
        lines = ["✅ План виконано."] + [f"• {row}" for row in completed]
        if skipped:
            lines += ["Пропущено:"] + [f"• {row}" for row in skipped]
        return "\n".join(lines)
    if command not in {"/pause_buys", "/resume_buys"}:
        return "Невідома команда. Надішліть /help."
    if len(parts) != 2:
        return f"Формат: {command} ПАРА, наприклад {command} SOLUSDT"
    symbol = parts[1].upper()
    bot = _telegram_grid_bot(symbol)
    if bot is None:
        return f"Активний PAPER Grid-бот {symbol} не знайдено."
    if command == "/pause_buys":
        grid_engine.set_manual_buy_pause(bot.id, True)
        return f"⏸ {symbol}: нові покупки вимкнено, продажі залишаються активними."
    analysis = await signal_analysis(bot.symbol, bot.base_asset)
    assessment = market_guard_assessment(bot, analysis)
    if assessment["critical"] or analysis["regime"]["name"] == "DOWNTREND":
        reasons = "; ".join(assessment["reasons"] or analysis["regime"]["reasons"])
        return f"⚠️ {symbol}: покупки не відновлено — спад ще не завершився. {reasons}."
    grid_engine.set_manual_buy_pause(bot.id, False)
    store.set_notification_state(f"market_guard:{bot.id}", "")
    return f"▶️ {symbol}: перевірка пройдена, нові PAPER-покупки увімкнено."


notifier.command_handler = telegram_command


@app.get("/api/monitoring/status")
async def monitoring_status() -> dict:
    grid_bots_state = list(grid_engine.bots.values())
    dca_bots_state = list(dca_engine.bots.values())
    signal_bots_state = list(signal_engine.bots.values())
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
        "signal": {
            "running": sum(bot.status == "RUNNING" for bot in signal_bots_state),
            "paused": sum(bot.status == "PAUSED" for bot in signal_bots_state),
            "errors": sum(bot.consecutive_errors for bot in signal_bots_state),
        },
        "testnet": {
            "configured": testnet.configured, "verified": testnet_health["verified"],
            "last_checked_at": testnet_health["last_checked_at"],
            "missing_variables": [
                name for name, value in (
                    ("BINANCE_API_KEY", settings.binance_api_key),
                    ("BINANCE_API_SECRET", settings.binance_api_secret),
                ) if not value.strip()
            ],
            "execution_enabled": settings.trading_mode == "TESTNET",
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
        f"Оцінка якості: {readiness['quality_score_pct']:.0f}%"
        if readiness["quality_score_pct"] is not None
        else f"Зібрано даних: {readiness['data_progress_pct']:.0f}%"
    )
    return "\n".join([
        f"📈 Spot Grid Lab · Тижнева оцінка · {symbol}",
        f"Стан: {readiness['status']}",
        score,
        f"Дані: {readiness['elapsed_days']:.1f}/7 днів, {readiness['cycles']}/20 циклів",
        f"Чистий результат: {metrics['realized_pnl']:+.2f} USDT",
        f"Дохідність сітки: {metrics['grid_return_pct']:.3f}%",
        f"Просадка: {metrics['realized_max_drawdown_pct']:.3f}%",
        f"Вплив комісій: {readiness['fee_drag_pct']:.1f}%",
        readiness["recommendation"],
    ])


notifier.weekly_report_provider = weekly_evaluation_message


def telegram_health_alerts(now: datetime) -> list[dict]:
    alerts = []
    risk = portfolio_risk_status()
    previous_risk = store.get_notification_state("portfolio_risk_status")
    if risk["status"] != previous_risk:
        store.set_notification_state("portfolio_risk_status", risk["status"])
        if risk["status"] == "BLOCKED":
            alerts.append({
                "key": "portfolio:RISK_BLOCKED",
                "message": "\n".join([
                    "🚨 Spot Grid Lab · Нові покупки заблоковано",
                    *risk["reasons"],
                    "Продажі та захисне завершення залишаються доступними.",
                    "Команда /risk покаже актуальні ліміти.",
                ]),
            })
    for bot in grid_engine.bots.values():
        if bot.status == "STOPPED":
            continue
        health = grid_bot_health(bot, now)
        if health["needs_attention"]:
            alerts.append({
                "key": f"{bot.id}:{health['code']}",
                "message": "\n".join([
                    "⚠️ Spot Grid Lab · Потрібна увага",
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
