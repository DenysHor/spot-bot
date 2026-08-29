import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable
from uuid import uuid4

from app.paper.broker import PaperBroker
from app.risk.manager import RiskManager


@dataclass
class DcaEvent:
    timestamp: str
    event: str
    price: float
    quote_amount: float = 0.0
    quantity: float = 0.0
    message: str = ""


@dataclass
class DcaBotState:
    id: str
    symbol: str
    base_asset: str
    status: str
    budget_quote: float
    order_quote: float
    interval_seconds: int
    dip_trigger_pct: float
    created_at: str
    next_buy_at: str
    last_price: float
    last_buy_price: float = 0.0
    spent_quote: float = 0.0
    buy_count: int = 0
    last_success_at: str = ""
    consecutive_errors: int = 0
    paused_reason: str = ""
    events: list[DcaEvent] = field(default_factory=list)

    def snapshot(self) -> dict:
        result = asdict(self)
        result["remaining_budget"] = max(0.0, self.budget_quote - self.spent_quote)
        return result


PriceProvider = Callable[[str], Awaitable[float]]


class DcaExecutionEngine:
    """PAPER-only scheduled and dip-triggered DCA execution engine."""

    def __init__(self, broker: PaperBroker, price_provider: PriceProvider, risk_manager: RiskManager,
                 poll_seconds: float = 5.0, store=None) -> None:
        self.broker = broker
        self.price_provider = price_provider
        self.risk_manager = risk_manager
        self.poll_seconds = max(1.0, poll_seconds)
        self.store = store
        self.bots: dict[str, DcaBotState] = store.load_dca_bots() if store is not None else {}
        self._task: asyncio.Task | None = None
        self._running = False

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _iso(value: datetime) -> str:
        return value.isoformat()

    def _persist(self, bot: DcaBotState) -> None:
        if self.store is not None:
            self.store.save_dca_bot(bot)

    def start_bot(self, symbol: str, base_asset: str, reference_price: float, budget_quote: float,
                  order_quote: float, interval_seconds: int, dip_trigger_pct: float) -> DcaBotState:
        symbol = symbol.upper()
        if any(bot.symbol == symbol and bot.status == "RUNNING" for bot in self.bots.values()):
            raise ValueError(f"A running DCA bot already exists for {symbol}")
        if min(reference_price, budget_quote, order_quote, dip_trigger_pct) <= 0:
            raise ValueError("price, budget, order and dip trigger must be positive")
        if order_quote * (1 + self.broker.fee_rate) > budget_quote:
            raise ValueError("DCA order including fee cannot exceed bot budget")
        if interval_seconds < 60:
            raise ValueError("interval_seconds must be at least 60")
        now = self._now()
        bot = DcaBotState(
            id=uuid4().hex[:12], symbol=symbol, base_asset=base_asset, status="RUNNING",
            budget_quote=budget_quote, order_quote=order_quote, interval_seconds=interval_seconds,
            dip_trigger_pct=dip_trigger_pct, created_at=self._iso(now), next_buy_at=self._iso(now),
            last_price=reference_price,
        )
        bot.events.append(DcaEvent(self._iso(now), "BOT_STARTED", reference_price,
                                   message="DCA bot started; first scheduled BUY is due"))
        self.bots[bot.id] = bot
        self._persist(bot)
        return bot

    def get_bot(self, bot_id: str) -> DcaBotState:
        if bot_id not in self.bots:
            raise ValueError("DCA bot not found")
        return self.bots[bot_id]

    def list_bots(self) -> list[dict]:
        return [bot.snapshot() for bot in self.bots.values()]

    def stop_bot(self, bot_id: str) -> DcaBotState:
        bot = self.get_bot(bot_id)
        bot.status = "STOPPED"
        bot.events.append(DcaEvent(self._iso(self._now()), "BOT_STOPPED", bot.last_price))
        self._persist(bot)
        return bot

    def pause_bot(self, bot_id: str, reason: str = "Paused by user") -> DcaBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "RUNNING":
            raise ValueError("Only a running DCA bot can be paused")
        bot.status = "PAUSED"
        bot.paused_reason = reason
        bot.events.append(DcaEvent(self._iso(self._now()), "BOT_PAUSED", bot.last_price, message=reason))
        self._persist(bot)
        return bot

    def resume_bot(self, bot_id: str) -> DcaBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "PAUSED":
            raise ValueError("Only a paused DCA bot can be resumed")
        bot.status = "RUNNING"
        bot.paused_reason = ""
        bot.consecutive_errors = 0
        bot.events.append(DcaEvent(self._iso(self._now()), "BOT_RESUMED", bot.last_price))
        self._persist(bot)
        return bot

    def reset(self) -> None:
        self.bots = {}
        if self.store is not None:
            self.store.clear_dca_bots()

    async def tick_bot(self, bot_id: str, price: float | None = None,
                       now: datetime | None = None) -> DcaBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "RUNNING":
            return bot
        current = price if price is not None else await self.price_provider(bot.symbol)
        if current <= 0:
            raise ValueError("Market price must be positive")
        current_time = now or self._now()
        bot.last_price = current
        bot.last_success_at = self._iso(current_time)
        bot.consecutive_errors = 0
        scheduled = current_time >= datetime.fromisoformat(bot.next_buy_at)
        dip = bot.last_buy_price > 0 and current <= bot.last_buy_price * (1 - bot.dip_trigger_pct / 100)
        if not scheduled and not dip:
            self._persist(bot)
            return bot

        total_cost = bot.order_quote * (1 + self.broker.fee_rate)
        trigger = "SCHEDULED" if scheduled else "DIP"
        if bot.spent_quote + total_cost > bot.budget_quote + 1e-9:
            bot.status = "COMPLETED"
            bot.events.append(DcaEvent(self._iso(current_time), "BUDGET_COMPLETED", current,
                                       message="Remaining DCA budget cannot fund another BUY"))
            self._persist(bot)
            return bot

        portfolio = self.broker.portfolio
        snapshot = portfolio.snapshot({bot.base_asset: current})
        position_value = portfolio.position(bot.base_asset).quantity * current
        decision = self.risk_manager.check_buy(
            total_equity=snapshot["total_equity"], free_quote=portfolio.quote_balance,
            current_bot_allocation=snapshot["assets_value"], current_position_value=position_value,
            requested_quote=total_cost,
        )
        if not decision.allowed:
            bot.events.append(DcaEvent(
                self._iso(current_time), "BUY_BLOCKED", current, quote_amount=bot.order_quote,
                message=f"{trigger}: {decision.reason}; max_order_quote={decision.max_order_quote:.8f}",
            ))
            if scheduled:
                bot.next_buy_at = self._iso(current_time + timedelta(seconds=bot.interval_seconds))
            self._persist(bot)
            return bot

        try:
            trade = self.broker.market_buy(bot.symbol, bot.base_asset, current, bot.order_quote)
        except ValueError as exc:
            bot.events.append(DcaEvent(self._iso(current_time), "BUY_BLOCKED", current,
                                       quote_amount=bot.order_quote, message=f"{trigger}: {exc}"))
            self._persist(bot)
            return bot
        bot.spent_quote += trade.quote_amount + trade.fee_quote
        bot.buy_count += 1
        bot.last_buy_price = current
        if scheduled:
            bot.next_buy_at = self._iso(current_time + timedelta(seconds=bot.interval_seconds))
        bot.events.append(DcaEvent(self._iso(current_time), "BUY_FILLED", current,
                                   trade.quote_amount, trade.quantity, f"{trigger} DCA BUY"))
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
                bot.events.append(DcaEvent(self._iso(self._now()), "ENGINE_ERROR", bot.last_price,
                                           message=str(exc)))
                if bot.consecutive_errors >= 3:
                    bot.status = "PAUSED"
                    bot.paused_reason = "Auto-paused after 3 consecutive engine errors"
                    bot.events.append(DcaEvent(self._iso(self._now()), "AUTO_PAUSED", bot.last_price,
                                               message=bot.paused_reason))
                self._persist(bot)

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
