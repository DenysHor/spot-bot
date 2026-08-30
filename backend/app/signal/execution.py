import asyncio
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable
from uuid import uuid4

from app.core.errors import describe_exception
from app.paper.broker import PaperBroker
from app.risk.manager import RiskManager


@dataclass
class SignalEvent:
    timestamp: str
    event: str
    price: float
    quote_amount: float = 0.0
    quantity: float = 0.0
    realized_cycle_pnl: float = 0.0
    message: str = ""


@dataclass
class SignalBotState:
    id: str
    symbol: str
    base_asset: str
    status: str
    budget_quote: float
    min_score: int
    created_at: str
    last_price: float
    last_score: int = 0
    expected_return_pct: float = 0.0
    risk_pct: float = 0.0
    entry_price: float = 0.0
    target_price: float = 0.0
    stop_price: float = 0.0
    quantity: float = 0.0
    position_cost_quote: float = 0.0
    opened_at: str = ""
    realized_pnl: float = 0.0
    completed_trades: int = 0
    last_success_at: str = ""
    consecutive_errors: int = 0
    paused_reason: str = ""
    reasons: list[str] = field(default_factory=list)
    events: list[SignalEvent] = field(default_factory=list)

    def snapshot(self) -> dict:
        result = asdict(self)
        result["has_position"] = self.quantity > 0
        result["unrealized_pnl"] = ((self.last_price - self.entry_price) * self.quantity) if self.quantity else 0.0
        result["current_return_pct"] = ((self.last_price / self.entry_price - 1) * 100) if self.entry_price and self.quantity else 0.0
        return result


AnalysisProvider = Callable[[str, str], Awaitable[dict]]
logger = logging.getLogger(__name__)


class SignalExecutionEngine:
    """Multi-confirmation, one-position-at-a-time PAPER signal engine."""

    def __init__(self, broker: PaperBroker, analysis_provider: AnalysisProvider, risk_manager: RiskManager,
                 poll_seconds: float = 60.0, store=None) -> None:
        self.broker = broker
        self.analysis_provider = analysis_provider
        self.risk_manager = risk_manager
        self.poll_seconds = max(30.0, poll_seconds)
        self.store = store
        self.bots = store.load_signal_bots() if store is not None else {}
        self._task = None
        self._running = False

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def _persist(self, bot: SignalBotState) -> None:
        if self.store is not None:
            self.store.save_signal_bot(bot)

    def start_bot(self, symbol: str, base_asset: str, budget_quote: float, min_score: int = 65) -> SignalBotState:
        if budget_quote <= 0 or not 50 <= min_score <= 90:
            raise ValueError("Бюджет має бути додатним, а мінімальна оцінка — від 50 до 90")
        if any(bot.symbol == symbol and bot.status != "STOPPED" for bot in self.bots.values()):
            raise ValueError(f"Для {symbol} уже існує Сигнальний бот")
        now = self._now().isoformat()
        bot = SignalBotState(uuid4().hex[:12], symbol, base_asset, "RUNNING", budget_quote, min_score, now, 0.0)
        bot.events.append(SignalEvent(now, "SIGNAL_BOT_STARTED", 0.0, message="Очікування підтвердженого сигналу"))
        self.bots[bot.id] = bot
        self._persist(bot)
        return bot

    def get_bot(self, bot_id: str) -> SignalBotState:
        if bot_id not in self.bots:
            raise ValueError("Сигнального бота не знайдено")
        return self.bots[bot_id]

    def list_bots(self) -> list[dict]:
        return [bot.snapshot() for bot in self.bots.values()]

    def pause_bot(self, bot_id: str) -> SignalBotState:
        bot = self.get_bot(bot_id)
        if bot.quantity:
            raise ValueError("Не можна призупинити захисний супровід відкритої позиції")
        bot.status = "PAUSED"
        bot.paused_reason = "Призупинено користувачем"
        bot.events.append(SignalEvent(self._now().isoformat(), "BOT_PAUSED", bot.last_price, message=bot.paused_reason))
        self._persist(bot)
        return bot

    def resume_bot(self, bot_id: str) -> SignalBotState:
        bot = self.get_bot(bot_id)
        bot.status = "RUNNING"
        bot.paused_reason = ""
        bot.consecutive_errors = 0
        bot.events.append(SignalEvent(self._now().isoformat(), "BOT_RESUMED", bot.last_price))
        self._persist(bot)
        return bot

    def stop_bot(self, bot_id: str) -> SignalBotState:
        bot = self.get_bot(bot_id)
        if bot.quantity:
            raise ValueError("Спочатку дочекайтеся закриття позиції або поставте бота на паузу")
        bot.status = "STOPPED"
        bot.events.append(SignalEvent(self._now().isoformat(), "BOT_STOPPED", bot.last_price))
        self._persist(bot)
        return bot

    async def tick_bot(self, bot_id: str, analysis: dict | None = None, now: datetime | None = None) -> SignalBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "RUNNING":
            return bot
        current_time = now or self._now()
        data = analysis or await self.analysis_provider(bot.symbol, bot.base_asset)
        price, score = float(data["price"]), int(data["score"])
        bot.last_price, bot.last_score = price, score
        bot.reasons = list(data.get("reasons", []))
        atr = max(0.1, float(data.get("atr_pct", 0)))
        bot.expected_return_pct = round(max(1.5, min(8.0, atr * 1.5)), 2)
        bot.risk_pct = round(max(1.0, min(5.0, atr)), 2)
        bot.last_success_at = current_time.isoformat()
        bot.consecutive_errors = 0

        if not bot.quantity:
            confirmed = (score >= bot.min_score and price > data["ema20"] > data["ema50"]
                         and 45 <= data["rsi14"] <= 68 and data["volume_ratio"] >= 1.1)
            if confirmed:
                snapshot = self.broker.portfolio.snapshot({bot.base_asset: price})
                decision = self.risk_manager.check_buy(
                    total_equity=snapshot["total_equity"], free_quote=self.broker.portfolio.quote_balance,
                    current_bot_allocation=snapshot["assets_value"], current_position_value=0,
                    requested_quote=bot.budget_quote * (1 + self.broker.fee_rate),
                )
                if not decision.allowed:
                    bot.events.append(SignalEvent(current_time.isoformat(), "BUY_BLOCKED", price, bot.budget_quote, message=decision.reason))
                else:
                    trade = self.broker.market_buy(bot.symbol, bot.base_asset, price, bot.budget_quote)
                    bot.entry_price, bot.quantity, bot.position_cost_quote = price, trade.quantity, trade.quote_amount + trade.fee_quote
                    bot.target_price = price * (1 + bot.expected_return_pct / 100)
                    bot.stop_price = price * (1 - bot.risk_pct / 100)
                    bot.opened_at = current_time.isoformat()
                    bot.events.append(SignalEvent(current_time.isoformat(), "SIGNAL_BUY_FILLED", price, trade.quote_amount, trade.quantity,
                                                  message=f"Оцінка {score}/100; ціль {bot.target_price:.8f}; стоп {bot.stop_price:.8f}"))
        else:
            opened = datetime.fromisoformat(bot.opened_at)
            reason = ""
            if price >= bot.target_price:
                reason = "Досягнуто ціль прибутку"
            elif price <= bot.stop_price:
                reason = "Спрацював захисний стоп"
            elif score < 35:
                reason = "Сигнал суттєво ослаб"
            elif current_time >= opened + timedelta(hours=72):
                reason = "Завершено максимальний час утримання 72 години"
            if reason:
                trade = self.broker.market_sell(bot.symbol, bot.base_asset, price, bot.quantity)
                bot.realized_pnl += trade.realized_pnl
                bot.completed_trades += 1
                bot.events.append(SignalEvent(current_time.isoformat(), "SIGNAL_SELL_FILLED", price, trade.quote_amount,
                                              bot.quantity, trade.realized_pnl, reason))
                bot.quantity = bot.entry_price = bot.target_price = bot.stop_price = bot.position_cost_quote = 0.0
                bot.opened_at = ""
        self._persist(bot)
        return bot

    async def tick_all(self) -> None:
        for bot in list(self.bots.values()):
            if bot.status != "RUNNING":
                continue
            try:
                await self.tick_bot(bot.id)
            except Exception as exc:
                bot.consecutive_errors += 1
                bot.events.append(SignalEvent(self._now().isoformat(), "ENGINE_ERROR", bot.last_price, message=describe_exception(exc)))
                if bot.consecutive_errors >= 3:
                    bot.status, bot.paused_reason = "PAUSED", "Автопауза після 3 послідовних помилок"
                self._persist(bot)
                logger.exception("Signal engine error for %s", bot.symbol)

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            await self.tick_all()
            await asyncio.sleep(self.poll_seconds)

    def start_background(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.run_forever())

    async def stop_background(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    def reset(self) -> None:
        self.bots = {}
        if self.store is not None:
            self.store.clear_signal_bots()
