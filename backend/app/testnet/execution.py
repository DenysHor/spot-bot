import asyncio
import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from app.exchange.binance_testnet import BinanceTestnetClient


@dataclass
class TestnetOrder:
    order_id: int
    side: str
    price: float
    quantity: float
    status: str = "NEW"
    source_price: float = 0.0
    source_cost: float = 0.0
    executed_quantity: float = 0.0
    cumulative_quote: float = 0.0
    commission_amount: float = 0.0
    commission_asset: str = ""
    created_at: str = ""


@dataclass
class TestnetBot:
    symbol: str
    budget_quote: float
    step_pct: float
    levels: int
    status: str
    created_at: str
    buy_enabled: bool = True
    soft_complete: bool = False
    completed_cycles: int = 0
    last_sync_at: str = ""
    last_error: str = ""
    last_price: float = 0.0
    realized_pnl: float = 0.0
    fees: dict[str, float] = field(default_factory=dict)
    events: list[dict] = field(default_factory=list)
    orders: list[TestnetOrder] = field(default_factory=list)

    def snapshot(self) -> dict:
        data = asdict(self)
        data["environment"] = "BINANCE_SPOT_TESTNET"
        data["virtual_funds"] = True
        active = [order for order in self.orders if order.status in {"NEW", "PARTIALLY_FILLED"}]
        buys = [order.price for order in active if order.side == "BUY"]
        sells = [order.price for order in active if order.side == "SELL"]
        data["nearest_buy_price"] = max(buys, default=None)
        data["nearest_sell_price"] = min(sells, default=None)
        data["unrealized_pnl"] = sum(
            order.quantity * self.last_price - order.source_cost
            for order in active if order.side == "SELL" and order.source_cost
        )
        data["total_pnl"] = self.realized_pnl + data["unrealized_pnl"]
        sync_age = (
            (datetime.now(timezone.utc) - datetime.fromisoformat(self.last_sync_at)).total_seconds()
            if self.last_sync_at else None
        )
        data["sync_age_seconds"] = sync_age
        data["sync_stale"] = sync_age is None or sync_age > 60
        return data


class TestnetGridEngine:
    STORE_KEY = "testnet_grid_bot_v1"

    def __init__(self, client: BinanceTestnetClient, store=None, poll_seconds: float = 10.0) -> None:
        self.client, self.store = client, store
        self.poll_seconds = max(5.0, poll_seconds)
        self.bot: TestnetBot | None = self._load()
        self._task: asyncio.Task | None = None
        self._running = False
        self.event_sink = None

    @staticmethod
    def now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _load(self) -> TestnetBot | None:
        if self.store is None:
            return None
        raw = self.store.get_notification_state(self.STORE_KEY, "")
        if not raw:
            return None
        data = json.loads(raw)
        data["orders"] = [TestnetOrder(**row) for row in data.get("orders", [])]
        return TestnetBot(**data)

    def _save(self) -> None:
        if self.store is not None:
            self.store.set_notification_state(self.STORE_KEY, json.dumps(asdict(self.bot)) if self.bot else "")

    async def balances(self) -> list[dict]:
        account = await self.client.account()
        return [row for row in account.get("balances", []) if float(row["free"]) or float(row["locked"])]

    async def _notify(self, event: str, bot: TestnetBot, payload=None) -> None:
        if self.event_sink is not None:
            try:
                await self.event_sink(event, bot, payload)
            except Exception:
                pass

    async def _emit(self, event: str, bot: TestnetBot, order: TestnetOrder | None = None, notify: bool = True) -> None:
        bot.events.append({
            "timestamp": self.now(), "event": event,
            "side": order.side if order else "", "price": order.price if order else 0.0,
            "order_id": order.order_id if order else 0,
        })
        bot.events = bot.events[-100:]
        if notify:
            await self._notify(event, bot, order)

    async def start(self, symbol: str, budget_quote: float, step_pct: float, levels: int, reference_price: float) -> TestnetBot:
        if self.bot and self.bot.status in {"RUNNING", "BUY_PAUSED", "DRAINING"}:
            raise ValueError("Одночасно дозволено лише одного TESTNET-бота")
        rules = await self.client.symbol_rules(symbol)
        if rules["status"] != "TRADING" or rules["quote_asset"] != "USDT":
            raise ValueError("Пара недоступна для Spot Testnet USDT-торгівлі")
        tranche = budget_quote / levels
        if tranche < float(rules["min_notional"]):
            raise ValueError(f"Одна заявка має бути не меншою за {rules['min_notional']} USDT")
        bot = TestnetBot(symbol=symbol.upper(), budget_quote=budget_quote, step_pct=step_pct, levels=levels,
                         status="RUNNING", created_at=self.now(), last_price=reference_price)
        try:
            for level in range(1, levels + 1):
                price = reference_price * (1 - step_pct / 100 * level)
                price_text = self.client.floor_to_step(price, rules["tick_size"])
                qty_text = self.client.floor_to_step(tranche / float(price_text), rules["step_size"])
                if float(qty_text) < float(rules["min_qty"]):
                    raise ValueError("Розрахована кількість нижча за мінімум Binance")
                result = await self.client.create_limit_order(bot.symbol, "BUY", qty_text, price_text)
                bot.orders.append(TestnetOrder(
                    int(result["orderId"]), "BUY", float(price_text), float(qty_text),
                    created_at=self.now(),
                ))
        except Exception:
            for order in bot.orders:
                try:
                    await self.client.cancel_order(bot.symbol, order.order_id)
                except Exception:
                    pass
            raise
        self.bot = bot
        await self._emit("BOT_STARTED", bot)
        self._save()
        return bot

    async def _fill_details(self, bot: TestnetBot, tracked: TestnetOrder, current: dict) -> tuple[float, float, float]:
        quantity = float(current["executedQty"])
        quote = float(current.get("cummulativeQuoteQty", 0))
        trades = await self.client.trades(bot.symbol, tracked.order_id)
        commissions: dict[str, float] = {}
        for trade in trades:
            asset = trade.get("commissionAsset", "")
            commissions[asset] = commissions.get(asset, 0.0) + float(trade.get("commission", 0))
        for asset, amount in commissions.items():
            bot.fees[asset] = bot.fees.get(asset, 0.0) + amount
        tracked.executed_quantity = quantity
        tracked.cumulative_quote = quote
        tracked.commission_amount = sum(commissions.values())
        tracked.commission_asset = ", ".join(f"{asset} {amount:.8f}" for asset, amount in commissions.items())
        quote_fee = commissions.get("USDT", 0.0)
        base_asset = bot.symbol.removesuffix("USDT")
        net_quantity = quantity - commissions.get(base_asset, 0.0)
        return quote / quantity, net_quantity, quote_fee

    async def sync(self) -> TestnetBot | None:
        bot = self.bot
        if not bot or bot.status == "STOPPED":
            return bot
        try:
            rules = await self.client.symbol_rules(bot.symbol)
            bot.last_price = await self.client.price(bot.symbol)
            fill_events: list[tuple[str, TestnetOrder]] = []
            realized_before = bot.realized_pnl
            for tracked in list(bot.orders):
                current = await self.client.order(bot.symbol, tracked.order_id)
                if not tracked.created_at and current.get("time"):
                    tracked.created_at = datetime.fromtimestamp(int(current["time"]) / 1000, timezone.utc).isoformat()
                previous = tracked.status
                tracked.status = current["status"]
                if previous != "FILLED" and tracked.status == "FILLED":
                    fill_price, net_quantity, quote_fee = await self._fill_details(bot, tracked, current)
                    if tracked.side == "BUY":
                        sell_price = self.client.floor_to_step(fill_price * (1 + bot.step_pct / 100), rules["tick_size"])
                        qty = self.client.floor_to_step(net_quantity, rules["step_size"])
                        result = await self.client.create_limit_order(bot.symbol, "SELL", qty, sell_price)
                        source_cost = tracked.cumulative_quote + quote_fee
                        bot.orders.append(TestnetOrder(int(result["orderId"]), "SELL", float(sell_price), float(qty), source_price=fill_price, source_cost=source_cost, created_at=self.now()))
                        await self._emit("BUY_FILLED", bot, tracked, notify=False)
                        fill_events.append(("BUY_FILLED", tracked))
                    else:
                        bot.completed_cycles += 1
                        bot.realized_pnl += tracked.cumulative_quote - quote_fee - tracked.source_cost
                        if bot.buy_enabled and not bot.soft_complete:
                            buy_price = self.client.floor_to_step(fill_price / (1 + bot.step_pct / 100), rules["tick_size"])
                            qty = self.client.floor_to_step(net_quantity, rules["step_size"])
                            result = await self.client.create_limit_order(bot.symbol, "BUY", qty, buy_price)
                            bot.orders.append(TestnetOrder(int(result["orderId"]), "BUY", float(buy_price), float(qty), created_at=self.now()))
                        await self._emit("SELL_FILLED", bot, tracked, notify=False)
                        fill_events.append(("SELL_FILLED", tracked))
            if len(fill_events) == 1:
                await self._notify(fill_events[0][0], bot, fill_events[0][1])
            elif fill_events:
                await self._notify("SYNC_BATCH", bot, {
                    "fills": [{"event": event, "price": order.price, "order_id": order.order_id} for event, order in fill_events],
                    "realized_pnl_delta": bot.realized_pnl - realized_before,
                })
            if bot.soft_complete and not any(x.side == "SELL" and x.status in {"NEW", "PARTIALLY_FILLED"} for x in bot.orders):
                bot.status = "STOPPED"
            bot.last_sync_at, bot.last_error = self.now(), ""
        except Exception as exc:
            message = str(exc)
            changed = message != bot.last_error
            bot.last_error = message
            if "insufficient balance" in message.lower():
                bot.buy_enabled = False
                bot.status = "BUY_PAUSED"
                if changed:
                    await self._emit("BUY_AUTO_PAUSED", bot)
            elif changed:
                await self._emit("SYNC_ERROR", bot)
        self._save()
        return bot

    async def stop_buys(self, soft_complete: bool = False) -> TestnetBot:
        if not self.bot:
            raise ValueError("TESTNET-бота немає")
        rules = await self.client.symbol_rules(self.bot.symbol)
        for order in list(self.bot.orders):
            if order.side == "BUY" and order.status in {"NEW", "PARTIALLY_FILLED"}:
                current = await self.client.order(self.bot.symbol, order.order_id)
                canceled = await self.client.cancel_order(self.bot.symbol, order.order_id)
                order.status = "CANCELED"
                executed = float(canceled.get("executedQty", current.get("executedQty", 0)))
                quote = float(canceled.get("cummulativeQuoteQty", current.get("cummulativeQuoteQty", 0)))
                if executed > 0 and quote > 0:
                    fill_price = quote / executed
                    sell_price = self.client.floor_to_step(fill_price * (1 + self.bot.step_pct / 100), rules["tick_size"])
                    qty = self.client.floor_to_step(executed, rules["step_size"])
                    result = await self.client.create_limit_order(self.bot.symbol, "SELL", qty, sell_price)
                    self.bot.orders.append(TestnetOrder(int(result["orderId"]), "SELL", float(sell_price), float(qty), source_price=fill_price, created_at=self.now()))
        self.bot.buy_enabled = False
        self.bot.soft_complete = soft_complete
        self.bot.status = "DRAINING" if soft_complete else "BUY_PAUSED"
        self._save()
        return self.bot

    async def stop(self) -> TestnetBot:
        if not self.bot:
            raise ValueError("TESTNET-бота немає")
        for order in self.bot.orders:
            if order.status in {"NEW", "PARTIALLY_FILLED"}:
                try:
                    await self.client.cancel_order(self.bot.symbol, order.order_id)
                    order.status = "CANCELED"
                except Exception:
                    pass
        self.bot.status, self.bot.buy_enabled = "STOPPED", False
        self._save()
        return self.bot

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            await self.sync()
            await asyncio.sleep(self.poll_seconds)

    def start_background(self) -> None:
        if not self._task or self._task.done():
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
