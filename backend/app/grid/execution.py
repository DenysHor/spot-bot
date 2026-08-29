import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Awaitable, Callable
from uuid import uuid4

from app.paper.broker import PaperBroker
from app.risk.manager import RiskManager


@dataclass
class GridOrder:
    id: str
    side: str
    trigger_price: float
    quote_amount: float = 0.0
    quantity: float = 0.0
    source_buy_price: float = 0.0
    source_buy_cost: float = 0.0


@dataclass
class GridEvent:
    timestamp: str
    event: str
    price: float
    side: str | None = None
    quantity: float = 0.0
    quote_amount: float = 0.0
    realized_cycle_pnl: float = 0.0
    message: str = ""


@dataclass
class GridBotState:
    id: str
    symbol: str
    base_asset: str
    status: str
    reference_price: float
    budget_quote: float
    step_pct: float
    levels_each_side: int
    quote_per_level: float
    created_at: str
    last_price: float
    spent_quote: float = 0.0
    realized_pnl: float = 0.0
    completed_cycles: int = 0
    open_orders: list[GridOrder] = field(default_factory=list)
    events: list[GridEvent] = field(default_factory=list)

    def snapshot(self) -> dict:
        result = asdict(self)
        result["open_buy_orders"] = sum(1 for x in self.open_orders if x.side == "BUY")
        result["open_sell_orders"] = sum(1 for x in self.open_orders if x.side == "SELL")
        return result


PriceProvider = Callable[[str], Awaitable[float]]


class GridExecutionEngine:
    """Paper-only percentage grid execution engine.

    Initial BUY levels are placed below the reference price. When a BUY level is
    crossed, a paper market BUY is recorded and a paired SELL is created one
    grid step above the fill. After that SELL is crossed, the cycle P&L is
    recorded and a replacement BUY is created one step below the sell fill.

    This is deterministic execution logic, not a price prediction model.
    """

    def __init__(self, broker: PaperBroker, price_provider: PriceProvider, poll_seconds: float = 5.0,
                 store=None, risk_manager: RiskManager | None = None) -> None:
        self.broker = broker
        self.price_provider = price_provider
        self.poll_seconds = max(1.0, poll_seconds)
        self.store = store
        self.risk_manager = risk_manager
        self.bots: dict[str, GridBotState] = store.load_bots() if store is not None else {}
        self._task: asyncio.Task | None = None
        self._running = False

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def start_bot(
        self,
        symbol: str,
        base_asset: str,
        reference_price: float,
        budget_quote: float,
        step_pct: float,
        levels_each_side: int,
    ) -> GridBotState:
        symbol = symbol.upper()
        if any(bot.symbol == symbol and bot.status == "RUNNING" for bot in self.bots.values()):
            raise ValueError(f"A running grid bot already exists for {symbol}")
        if reference_price <= 0 or budget_quote <= 0 or step_pct <= 0:
            raise ValueError("reference_price, budget_quote and step_pct must be positive")
        if levels_each_side < 1 or levels_each_side > 50:
            raise ValueError("levels_each_side must be between 1 and 50")

        # Budget includes simulated fees, so a full set of BUY fills cannot exceed it.
        tranche_total = budget_quote / levels_each_side
        quote_per_level = tranche_total / (1 + self.broker.fee_rate)
        step = step_pct / 100.0
        orders = [
            GridOrder(
                id=uuid4().hex[:12],
                side="BUY",
                trigger_price=reference_price * (1 - step * i),
                quote_amount=quote_per_level,
            )
            for i in range(1, levels_each_side + 1)
        ]
        bot = GridBotState(
            id=uuid4().hex[:12],
            symbol=symbol,
            base_asset=base_asset,
            status="RUNNING",
            reference_price=reference_price,
            budget_quote=budget_quote,
            step_pct=step_pct,
            levels_each_side=levels_each_side,
            quote_per_level=quote_per_level,
            created_at=self._now(),
            last_price=reference_price,
            open_orders=orders,
        )
        bot.events.append(GridEvent(
            timestamp=self._now(), event="BOT_STARTED", price=reference_price,
            message=f"Grid started with {levels_each_side} BUY levels",
        ))
        self.bots[bot.id] = bot
        self._persist(bot)
        return bot

    def stop_bot(self, bot_id: str) -> GridBotState:
        bot = self.get_bot(bot_id)
        bot.status = "STOPPED"
        bot.events.append(GridEvent(timestamp=self._now(), event="BOT_STOPPED", price=bot.last_price))
        self._persist(bot)
        return bot

    def get_bot(self, bot_id: str) -> GridBotState:
        if bot_id not in self.bots:
            raise ValueError("Grid bot not found")
        return self.bots[bot_id]

    def list_bots(self) -> list[dict]:
        return [bot.snapshot() for bot in self.bots.values()]

    def reset(self) -> None:
        self.bots = {}
        if self.store is not None:
            self.store.clear_bots()

    def _persist(self, bot: GridBotState) -> None:
        if self.store is not None:
            self.store.save_bot(bot)

    def _risk_decision(self, bot: GridBotState, requested_quote: float, current_price: float):
        if self.risk_manager is None:
            return None
        portfolio = self.broker.portfolio
        position = portfolio.position(bot.base_asset)
        prices = {bot.base_asset: current_price}
        snapshot = portfolio.snapshot(prices)
        equity = snapshot["total_equity"]
        return self.risk_manager.check_buy(
            total_equity=equity,
            free_quote=portfolio.quote_balance,
            current_bot_allocation=snapshot["assets_value"],
            current_position_value=position.quantity * current_price,
            requested_quote=requested_quote * (1 + self.broker.fee_rate),
        )

    async def tick_bot(self, bot_id: str, price: float | None = None) -> GridBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "RUNNING":
            return bot

        current = price if price is not None else await self.price_provider(bot.symbol)
        if current <= 0:
            raise ValueError("Market price must be positive")
        bot.last_price = current

        # Process BUYs from highest trigger downward, allowing gap moves to fill several levels.
        buys = sorted(
            [o for o in bot.open_orders if o.side == "BUY" and current <= o.trigger_price],
            key=lambda o: o.trigger_price,
            reverse=True,
        )
        for order in buys:
            total_cost = order.quote_amount * (1 + self.broker.fee_rate)
            if bot.spent_quote + total_cost > bot.budget_quote + 1e-9:
                continue
            decision = self._risk_decision(bot, order.quote_amount, current)
            if decision is not None and not decision.allowed:
                bot.events.append(GridEvent(
                    timestamp=self._now(), event="BUY_BLOCKED", price=current, side="BUY",
                    quote_amount=order.quote_amount,
                    message=f"{decision.reason}; max_order_quote={decision.max_order_quote:.8f}",
                ))
                continue
            try:
                trade = self.broker.market_buy(bot.symbol, bot.base_asset, current, order.quote_amount)
            except ValueError as exc:
                bot.events.append(GridEvent(
                    timestamp=self._now(), event="BUY_BLOCKED", price=current,
                    side="BUY", quote_amount=order.quote_amount, message=str(exc),
                ))
                continue

            bot.open_orders.remove(order)
            bot.spent_quote += trade.quote_amount + trade.fee_quote
            sell_trigger = current * (1 + bot.step_pct / 100.0)
            bot.open_orders.append(GridOrder(
                id=uuid4().hex[:12], side="SELL", trigger_price=sell_trigger,
                quantity=trade.quantity, source_buy_price=current,
                source_buy_cost=trade.quote_amount + trade.fee_quote,
            ))
            bot.events.append(GridEvent(
                timestamp=self._now(), event="BUY_FILLED", price=current,
                side="BUY", quantity=trade.quantity, quote_amount=trade.quote_amount,
                message=f"Paired SELL created at {sell_trigger:.8f}",
            ))

        sells = sorted(
            [o for o in bot.open_orders if o.side == "SELL" and current >= o.trigger_price],
            key=lambda o: o.trigger_price,
        )
        for order in sells:
            try:
                trade = self.broker.market_sell(bot.symbol, bot.base_asset, current, order.quantity)
            except ValueError as exc:
                bot.events.append(GridEvent(
                    timestamp=self._now(), event="SELL_BLOCKED", price=current,
                    side="SELL", quantity=order.quantity, message=str(exc),
                ))
                continue

            bot.open_orders.remove(order)
            bot.spent_quote = max(0.0, bot.spent_quote - order.source_buy_cost)
            net_sell = trade.quote_amount - trade.fee_quote
            cycle_pnl = net_sell - order.source_buy_cost
            bot.realized_pnl += cycle_pnl
            bot.completed_cycles += 1

            replacement_buy = current / (1 + bot.step_pct / 100.0)
            bot.open_orders.append(GridOrder(
                id=uuid4().hex[:12], side="BUY", trigger_price=replacement_buy,
                quote_amount=bot.quote_per_level,
            ))
            bot.events.append(GridEvent(
                timestamp=self._now(), event="SELL_FILLED", price=current,
                side="SELL", quantity=trade.quantity, quote_amount=trade.quote_amount,
                realized_cycle_pnl=cycle_pnl,
                message=f"Replacement BUY created at {replacement_buy:.8f}",
            ))

        self._persist(bot)
        return bot

    async def tick_all(self) -> None:
        for bot in list(self.bots.values()):
            if bot.status != "RUNNING":
                continue
            try:
                await self.tick_bot(bot.id)
            except Exception as exc:
                bot.events.append(GridEvent(
                    timestamp=self._now(), event="ENGINE_ERROR", price=bot.last_price,
                    message=str(exc),
                ))
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
