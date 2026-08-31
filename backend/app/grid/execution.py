import asyncio
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Awaitable, Callable
from uuid import uuid4

from app.paper.broker import PaperBroker
from app.core.errors import describe_exception
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
    last_success_at: str = ""
    consecutive_errors: int = 0
    paused_reason: str = ""
    trailing_up_enabled: bool = False
    trailing_trigger_steps: float = 2.0
    recenter_count: int = 0
    last_recenter_at: str = ""
    recenter_day: str = ""
    recenter_count_today: int = 0
    max_recenters_per_day: int = 3
    recenter_limit_event_day: str = ""
    strategy_profile: str = "RANGE_GRID"
    seed_position_pct: float = 0.0
    seed_quantity: float = 0.0
    seed_cost_quote: float = 0.0
    seed_realized_pnl: float = 0.0
    buy_paused: bool = False
    buy_paused_reason: str = ""
    buy_paused_since: str = ""
    buy_required_quote: float = 0.0
    buy_available_quote: float = 0.0
    price_floor: float = 0.0
    price_ceiling: float = 0.0
    out_of_range: bool = False
    drain_mode: bool = False
    max_deployed_quote: float = 0.0
    manual_buy_paused: bool = False
    open_orders: list[GridOrder] = field(default_factory=list)
    events: list[GridEvent] = field(default_factory=list)

    def snapshot(self) -> dict:
        result = asdict(self)
        today = datetime.now(timezone.utc).date().isoformat()
        result["recenter_count_today"] = self.recenter_count_today if self.recenter_day == today else 0
        sell_orders = [order for order in self.open_orders if order.side == "SELL"]
        result["grid_open_exposure_quote"] = sum(order.source_buy_cost for order in sell_orders)
        result["open_exposure_quote"] = result["grid_open_exposure_quote"] + self.seed_cost_quote
        result["unrealized_pnl"] = sum(
            order.quantity * self.last_price * (1 - 0.001) - order.source_buy_cost
            for order in sell_orders
        )
        result["grid_pnl"] = self.realized_pnl + result["unrealized_pnl"]
        result["seed_value_quote"] = self.seed_quantity * self.last_price * (1 - 0.001)
        result["seed_unrealized_pnl"] = result["seed_value_quote"] - self.seed_cost_quote
        result["trend_pnl"] = self.seed_realized_pnl + result["seed_unrealized_pnl"]
        result["total_pnl"] = result["grid_pnl"] + result["trend_pnl"]
        result["grid_budget_quote"] = self.budget_quote * (1 - self.seed_position_pct / 100)
        result["execution_status"] = (
            "DRAINING" if self.drain_mode else "OUT_OF_RANGE" if self.out_of_range
            else "BUYS_DISABLED" if self.manual_buy_paused
            else "BUY_PAUSED" if self.buy_paused else self.status
        )
        result["return_on_max_deployed_pct"] = (
            result["total_pnl"] / self.max_deployed_quote * 100 if self.max_deployed_quote else 0.0
        )
        result["open_positions"] = [{
            "buy_price": order.source_buy_price,
            "quantity": order.quantity,
            "cost_quote": order.source_buy_cost,
            "target_sell_price": order.trigger_price,
            "current_pnl": order.quantity * self.last_price * (1 - 0.001) - order.source_buy_cost,
            "expected_net_profit": order.quantity * order.trigger_price * (1 - 0.001) - order.source_buy_cost,
        } for order in sell_orders]
        open_quantity = sum(order.quantity for order in sell_orders)
        result["average_open_buy_price"] = (
            sum(order.source_buy_price * order.quantity for order in sell_orders) / open_quantity
            if open_quantity else 0.0
        )
        result["open_position_value_quote"] = sum(
            order.quantity * self.last_price * (1 - 0.001) for order in sell_orders
        )
        result["open_buy_orders"] = sum(1 for x in self.open_orders if x.side == "BUY")
        result["open_sell_orders"] = sum(1 for x in self.open_orders if x.side == "SELL")
        buy_prices = [order.trigger_price for order in self.open_orders if order.side == "BUY"]
        sell_prices = [order.trigger_price for order in sell_orders]
        result["nearest_buy_price"] = max(buy_prices, default=None)
        result["nearest_sell_price"] = min(sell_prices, default=None)
        result["work_stage"] = (
            "DRAINING" if self.drain_mode else "OUT_OF_RANGE" if self.out_of_range
            else "BUYS_DISABLED" if self.manual_buy_paused else "WAITING_SELL"
            if sell_orders else "WAITING_BUY"
        )
        step = self.step_pct / 100.0
        result["next_recenter_price"] = (
            self.reference_price * (1 + step * self.trailing_trigger_steps)
            if self.trailing_up_enabled else None
        )
        return result


PriceProvider = Callable[[str], Awaitable[float]]
logger = logging.getLogger(__name__)


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
        trailing_up_enabled: bool = False,
        strategy_profile: str = "RANGE_GRID",
        seed_position_pct: float = 0.0,
        price_floor: float = 0.0,
        price_ceiling: float = 0.0,
    ) -> GridBotState:
        symbol = symbol.upper()
        if any(bot.symbol == symbol and bot.status == "RUNNING" for bot in self.bots.values()):
            raise ValueError(f"A running grid bot already exists for {symbol}")
        if reference_price <= 0 or budget_quote <= 0 or step_pct <= 0:
            raise ValueError("reference_price, budget_quote and step_pct must be positive")
        if levels_each_side < 1 or levels_each_side > 50:
            raise ValueError("levels_each_side must be between 1 and 50")
        if seed_position_pct < 0 or seed_position_pct > 30:
            raise ValueError("seed_position_pct must be between 0 and 30")
        if price_floor < 0 or price_ceiling < 0 or (price_floor and price_ceiling and price_floor >= price_ceiling):
            raise ValueError("price corridor must satisfy 0 < floor < ceiling")

        # Budget includes simulated fees, so a full set of BUY fills cannot exceed it.
        seed_total = budget_quote * seed_position_pct / 100
        grid_budget = budget_quote - seed_total
        tranche_total = grid_budget / levels_each_side
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
            trailing_up_enabled=trailing_up_enabled,
            strategy_profile=strategy_profile,
            seed_position_pct=seed_position_pct,
            price_floor=price_floor,
            price_ceiling=price_ceiling,
            open_orders=orders,
        )
        if seed_total > 0:
            seed_quote = seed_total / (1 + self.broker.fee_rate)
            trade = self.broker.market_buy(symbol, base_asset, reference_price, seed_quote)
            bot.seed_quantity = trade.quantity
            bot.seed_cost_quote = trade.quote_amount + trade.fee_quote
            bot.max_deployed_quote = bot.seed_cost_quote
            bot.events.append(GridEvent(
                timestamp=self._now(), event="HYBRID_SEED_BOUGHT", price=reference_price,
                side="BUY", quantity=trade.quantity, quote_amount=trade.quote_amount,
                message=f"Hybrid trend position opened with {seed_position_pct:.0f}% of bot budget",
            ))
        bot.events.append(GridEvent(
            timestamp=self._now(), event="BOT_STARTED", price=reference_price,
            message=(f"{strategy_profile} started with {levels_each_side} BUY levels; "
                     f"Grid budget {grid_budget:.2f}; trend allocation {seed_position_pct:.0f}%"),
        ))
        self.bots[bot.id] = bot
        self._persist(bot)
        return bot

    def start_draining(self, bot_id: str) -> GridBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "RUNNING":
            raise ValueError("Only a running Grid bot can start soft completion")
        if bot.drain_mode:
            return bot
        removed = sum(order.side == "BUY" for order in bot.open_orders)
        bot.open_orders = [order for order in bot.open_orders if order.side != "BUY"]
        bot.drain_mode = True
        bot.events.append(GridEvent(
            timestamp=self._now(), event="DRAIN_MODE_STARTED", price=bot.last_price,
            message=f"Soft completion started; {removed} BUY levels cancelled; SELL remains active",
        ))
        if not any(order.side == "SELL" for order in bot.open_orders):
            bot.events.append(GridEvent(
                timestamp=self._now(), event="DRAIN_MODE_COMPLETED", price=bot.last_price,
                message="No Grid positions remained; strategy completed",
            ))
            return self.stop_bot(bot.id)
        self._persist(bot)
        return bot

    def set_manual_buy_pause(self, bot_id: str, paused: bool) -> GridBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "RUNNING" or bot.drain_mode:
            raise ValueError("Buy control is available only for a running Grid bot")
        if bot.manual_buy_paused == paused:
            return bot
        bot.manual_buy_paused = paused
        bot.events.append(GridEvent(
            timestamp=self._now(),
            event="MANUAL_BUYS_DISABLED" if paused else "MANUAL_BUYS_ENABLED",
            price=bot.last_price,
            message=("New BUY fills disabled by user; SELL remains active"
                     if paused else "New BUY fills enabled by user"),
        ))
        self._persist(bot)
        return bot

    def set_trailing_up(self, bot_id: str, enabled: bool) -> GridBotState:
        bot = self.get_bot(bot_id)
        if bot.status == "STOPPED":
            raise ValueError("Trailing Up cannot be changed for a stopped Grid bot")
        if bot.seed_position_pct > 0 and not enabled:
            raise ValueError("Trailing Up is required while a Hybrid trend position is active")
        bot.trailing_up_enabled = enabled
        bot.events.append(GridEvent(
            timestamp=self._now(), event="TRAILING_UP_ENABLED" if enabled else "TRAILING_UP_DISABLED",
            price=bot.last_price,
            message=("Unfilled BUY levels will follow upward moves"
                     if enabled else "BUY levels remain at their current prices"),
        ))
        self._persist(bot)
        return bot

    def _recenter_buys_up(
        self, bot: GridBotState, current: float, now: datetime | None = None
    ) -> None:
        """Move only unfilled BUY levels up; never touch acquired inventory or SELLs."""
        if not bot.trailing_up_enabled:
            return
        step = bot.step_pct / 100.0
        trigger = bot.reference_price * (1 + step * bot.trailing_trigger_steps)
        if current < trigger:
            return
        current_time = now or datetime.now(timezone.utc)
        today = current_time.date().isoformat()
        if bot.recenter_day != today:
            bot.recenter_day = today
            bot.recenter_count_today = 0
            bot.recenter_limit_event_day = ""
        if bot.recenter_count_today >= bot.max_recenters_per_day:
            if bot.recenter_limit_event_day != today:
                bot.recenter_limit_event_day = today
                bot.events.append(GridEvent(
                    timestamp=current_time.isoformat(), event="RECENTER_LIMIT_REACHED", price=current,
                    message=f"Daily Trailing Up limit reached: {bot.max_recenters_per_day}",
                ))
            return
        buy_orders = [order for order in bot.open_orders if order.side == "BUY"]
        if not buy_orders:
            return
        sell_count = sum(order.side == "SELL" for order in bot.open_orders)
        target_buy_count = max(0, bot.levels_each_side - sell_count)
        bot.open_orders = [order for order in bot.open_orders if order.side != "BUY"]
        for level in range(1, target_buy_count + 1):
            bot.open_orders.append(GridOrder(
                id=uuid4().hex[:12], side="BUY",
                trigger_price=current * (1 - step * level), quote_amount=bot.quote_per_level,
            ))
        previous_reference = bot.reference_price
        bot.reference_price = current
        bot.recenter_count += 1
        bot.recenter_count_today += 1
        bot.last_recenter_at = current_time.isoformat()
        bot.events.append(GridEvent(
            timestamp=bot.last_recenter_at, event="GRID_RECENTERED", price=current,
            message=(f"Trailing Up shifted {target_buy_count} BUY levels from anchor "
                     f"{previous_reference:.8f} to {current:.8f}"),
        ))

    def stop_bot(self, bot_id: str) -> GridBotState:
        bot = self.get_bot(bot_id)
        if any(order.side == "SELL" for order in bot.open_orders):
            raise ValueError("Cannot stop a bot with open SELL levels; pause it instead")
        if bot.seed_quantity > 0:
            trade = self.broker.market_sell(
                bot.symbol, bot.base_asset, bot.last_price, bot.seed_quantity,
            )
            proceeds = trade.quote_amount - trade.fee_quote
            bot.seed_realized_pnl += proceeds - bot.seed_cost_quote
            bot.events.append(GridEvent(
                timestamp=self._now(), event="HYBRID_SEED_SOLD", price=bot.last_price,
                side="SELL", quantity=trade.quantity, quote_amount=trade.quote_amount,
                realized_cycle_pnl=proceeds - bot.seed_cost_quote,
                message="Hybrid trend position closed when bot stopped",
            ))
            bot.seed_quantity = 0.0
            bot.seed_cost_quote = 0.0
        bot.status = "STOPPED"
        bot.events.append(GridEvent(timestamp=self._now(), event="BOT_STOPPED", price=bot.last_price))
        self._persist(bot)
        return bot

    def liquidate_bot(self, bot_id: str) -> GridBotState:
        """Sell every position owned by this PAPER bot at its latest known price."""
        bot = self.get_bot(bot_id)
        if bot.status == "STOPPED":
            raise ValueError("Stopped Grid bot cannot be liquidated")
        if bot.last_price <= 0:
            raise ValueError("Current market price is unavailable")
        sell_orders = [order for order in bot.open_orders if order.side == "SELL"]
        grid_quantity = sum(order.quantity for order in sell_orders)
        total_quantity = grid_quantity + bot.seed_quantity
        if total_quantity <= 0:
            raise ValueError("Grid bot has no open position to sell")

        trade = self.broker.market_sell(
            bot.symbol, bot.base_asset, bot.last_price, total_quantity,
        )
        net_price = bot.last_price * (1 - self.broker.fee_rate)
        forced_grid_pnl = sum(
            order.quantity * net_price - order.source_buy_cost for order in sell_orders
        )
        forced_seed_pnl = bot.seed_quantity * net_price - bot.seed_cost_quote
        bot.realized_pnl += forced_grid_pnl
        bot.seed_realized_pnl += forced_seed_pnl
        bot.events.append(GridEvent(
            timestamp=self._now(), event="EMERGENCY_LIQUIDATION", price=bot.last_price,
            side="SELL", quantity=trade.quantity, quote_amount=trade.quote_amount,
            realized_cycle_pnl=forced_grid_pnl + forced_seed_pnl,
            message="All open PAPER positions sold immediately at the current price",
        ))
        bot.open_orders.clear()
        bot.spent_quote = 0.0
        bot.seed_quantity = 0.0
        bot.seed_cost_quote = 0.0
        bot.status = "STOPPED"
        bot.drain_mode = False
        bot.events.append(GridEvent(
            timestamp=self._now(), event="BOT_STOPPED", price=bot.last_price,
            message="Stopped after emergency PAPER liquidation",
        ))
        self._persist(bot)
        return bot

    def pause_bot(self, bot_id: str, reason: str = "Paused by user") -> GridBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "RUNNING":
            raise ValueError("Only a running Grid bot can be paused")
        bot.status = "PAUSED"
        bot.paused_reason = reason
        bot.events.append(GridEvent(timestamp=self._now(), event="BOT_PAUSED", price=bot.last_price, message=reason))
        self._persist(bot)
        return bot

    def resume_bot(self, bot_id: str) -> GridBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "PAUSED":
            raise ValueError("Only a paused Grid bot can be resumed")
        bot.status = "RUNNING"
        bot.paused_reason = ""
        bot.consecutive_errors = 0
        bot.events.append(GridEvent(timestamp=self._now(), event="BOT_RESUMED", price=bot.last_price))
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

    def _available_quote_for_bot(self, bot: GridBotState, current_price: float) -> float:
        portfolio = self.broker.portfolio
        reserve = 0.0
        if self.risk_manager is not None:
            snapshot = portfolio.snapshot({bot.base_asset: current_price})
            reserve = snapshot["total_equity"] * self.risk_manager.limits.reserve_quote_pct / 100
        reserved_by_others = 0.0
        for other in self.bots.values():
            if other.id == bot.id or other.status == "STOPPED":
                continue
            other_grid_budget = other.budget_quote * (1 - other.seed_position_pct / 100)
            reserved_by_others += max(0.0, other_grid_budget - other.spent_quote)
        return max(0.0, portfolio.quote_balance - reserve - reserved_by_others)

    def _pause_buys(self, bot: GridBotState, required: float, available: float, reason: str) -> None:
        bot.buy_required_quote = required
        bot.buy_available_quote = available
        if bot.buy_paused:
            return
        bot.buy_paused = True
        bot.buy_paused_reason = reason
        bot.buy_paused_since = self._now()
        bot.events.append(GridEvent(
            timestamp=bot.buy_paused_since, event="BUY_SIDE_PAUSED", price=bot.last_price,
            quote_amount=required,
            message=f"{reason}; required={required:.4f} USDT; available={available:.4f} USDT; SELL remains active",
        ))

    def _maybe_resume_buys(self, bot: GridBotState, current_price: float) -> None:
        buy_orders = [order for order in bot.open_orders if order.side == "BUY"]
        required = min(
            (order.quote_amount * (1 + self.broker.fee_rate) for order in buy_orders),
            default=0.0,
        )
        available = self._available_quote_for_bot(bot, current_price)
        bot.buy_required_quote = required
        bot.buy_available_quote = available
        if bot.buy_paused and (required == 0 or available >= required * 1.1):
            bot.buy_paused = False
            bot.buy_paused_reason = ""
            bot.buy_paused_since = ""
            bot.events.append(GridEvent(
                timestamp=self._now(), event="BUY_SIDE_RESUMED", price=current_price,
                quote_amount=available,
                message=f"BUY liquidity restored with 10% safety buffer; available={available:.4f} USDT",
            ))

    def _update_range_state(self, bot: GridBotState, current_price: float) -> None:
        outside = bool(
            (bot.price_floor and current_price < bot.price_floor)
            or (bot.price_ceiling and current_price > bot.price_ceiling)
        )
        if outside and not bot.out_of_range:
            bot.out_of_range = True
            bot.events.append(GridEvent(
                timestamp=self._now(), event="PRICE_RANGE_EXITED", price=current_price,
                message=(f"Price left corridor {bot.price_floor:.8f}–{bot.price_ceiling:.8f}; "
                         "BUY disabled; SELL remains active"),
            ))
        elif not outside and bot.out_of_range:
            bot.out_of_range = False
            bot.events.append(GridEvent(
                timestamp=self._now(), event="PRICE_RANGE_REENTERED", price=current_price,
                message="Price returned to corridor; BUY eligibility restored",
            ))

    async def tick_bot(
        self, bot_id: str, price: float | None = None, now: datetime | None = None
    ) -> GridBotState:
        bot = self.get_bot(bot_id)
        if bot.status != "RUNNING":
            return bot

        current = price if price is not None else await self.price_provider(bot.symbol)
        if current <= 0:
            raise ValueError("Market price must be positive")
        bot.last_price = current
        bot.last_success_at = self._now()
        bot.consecutive_errors = 0
        bot.max_deployed_quote = max(
            bot.max_deployed_quote, bot.spent_quote + bot.seed_cost_quote,
        )

        self._update_range_state(bot, current)
        if not bot.out_of_range and not bot.drain_mode and not bot.manual_buy_paused:
            self._recenter_buys_up(bot, current, now)
        self._maybe_resume_buys(bot, current)

        # Process BUYs from highest trigger downward, allowing gap moves to fill several levels.
        buys = sorted(
            [o for o in bot.open_orders if o.side == "BUY" and current <= o.trigger_price],
            key=lambda o: o.trigger_price,
            reverse=True,
        )
        for order in buys:
            if bot.buy_paused or bot.out_of_range or bot.drain_mode or bot.manual_buy_paused:
                break
            total_cost = order.quote_amount * (1 + self.broker.fee_rate)
            grid_budget = bot.budget_quote * (1 - bot.seed_position_pct / 100)
            if bot.spent_quote + total_cost > grid_budget + 1e-9:
                continue
            available = self._available_quote_for_bot(bot, current)
            if available + 1e-9 < total_cost:
                self._pause_buys(bot, total_cost, available, "Insufficient unreserved USDT")
                break
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
                self._pause_buys(bot, total_cost, self.broker.portfolio.quote_balance, str(exc))
                bot.events.append(GridEvent(
                    timestamp=self._now(), event="BUY_BLOCKED", price=current,
                    side="BUY", quote_amount=order.quote_amount, message=str(exc),
                ))
                continue

            bot.open_orders.remove(order)
            bot.spent_quote += trade.quote_amount + trade.fee_quote
            bot.max_deployed_quote = max(
                bot.max_deployed_quote, bot.spent_quote + bot.seed_cost_quote,
            )
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
            if not bot.drain_mode:
                bot.open_orders.append(GridOrder(
                    id=uuid4().hex[:12], side="BUY", trigger_price=replacement_buy,
                    quote_amount=bot.quote_per_level,
                ))
            bot.events.append(GridEvent(
                timestamp=self._now(), event="SELL_FILLED", price=current,
                side="SELL", quantity=trade.quantity, quote_amount=trade.quote_amount,
                realized_cycle_pnl=cycle_pnl,
                message=("Soft completion: no replacement BUY"
                         if bot.drain_mode else f"Replacement BUY created at {replacement_buy:.8f}"),
            ))

        if bot.drain_mode and not any(order.side == "SELL" for order in bot.open_orders):
            bot.events.append(GridEvent(
                timestamp=self._now(), event="DRAIN_MODE_COMPLETED", price=current,
                message="All Grid positions sold; strategy completed",
            ))
            self.stop_bot(bot.id)

        self._persist(bot)
        return bot

    async def tick_all(self) -> None:
        for bot in list(self.bots.values()):
            if bot.status != "RUNNING":
                continue
            try:
                await self.tick_bot(bot.id)
            except Exception as exc:
                detail = describe_exception(exc)
                logger.exception("Grid engine error for bot %s (%s): %s", bot.id, bot.symbol, detail)
                bot.consecutive_errors += 1
                bot.events.append(GridEvent(
                    timestamp=self._now(), event="ENGINE_ERROR", price=bot.last_price,
                    message=detail,
                ))
                if bot.consecutive_errors >= 3:
                    bot.status = "PAUSED"
                    bot.paused_reason = "Auto-paused after 3 consecutive engine errors"
                    bot.events.append(GridEvent(
                        timestamp=self._now(), event="AUTO_PAUSED", price=bot.last_price,
                        message=bot.paused_reason,
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
