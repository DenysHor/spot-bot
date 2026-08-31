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
    orders: list[TestnetOrder] = field(default_factory=list)

    def snapshot(self) -> dict:
        data = asdict(self)
        data["environment"] = "BINANCE_SPOT_TESTNET"
        data["virtual_funds"] = True
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

    async def _emit(self, event: str, bot: TestnetBot, order: TestnetOrder | None = None) -> None:
        if self.event_sink is not None:
            try:
                await self.event_sink(event, bot, order)
            except Exception:
                pass

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
                         status="RUNNING", created_at=self.now())
        try:
            for level in range(1, levels + 1):
                price = reference_price * (1 - step_pct / 100 * level)
                price_text = self.client.floor_to_step(price, rules["tick_size"])
                qty_text = self.client.floor_to_step(tranche / float(price_text), rules["step_size"])
                if float(qty_text) < float(rules["min_qty"]):
                    raise ValueError("Розрахована кількість нижча за мінімум Binance")
                result = await self.client.create_limit_order(bot.symbol, "BUY", qty_text, price_text)
                bot.orders.append(TestnetOrder(int(result["orderId"]), "BUY", float(price_text), float(qty_text)))
        except Exception:
            for order in bot.orders:
                try:
                    await self.client.cancel_order(bot.symbol, order.order_id)
                except Exception:
                    pass
            raise
        self.bot = bot
        self._save()
        return bot

    async def sync(self) -> TestnetBot | None:
        bot = self.bot
        if not bot or bot.status == "STOPPED":
            return bot
        rules = await self.client.symbol_rules(bot.symbol)
        try:
            for tracked in list(bot.orders):
                current = await self.client.order(bot.symbol, tracked.order_id)
                previous = tracked.status
                tracked.status = current["status"]
                if previous != "FILLED" and tracked.status == "FILLED":
                    fill_price = float(current.get("cummulativeQuoteQty", 0)) / float(current["executedQty"])
                    quantity = float(current["executedQty"])
                    if tracked.side == "BUY":
                        sell_price = self.client.floor_to_step(fill_price * (1 + bot.step_pct / 100), rules["tick_size"])
                        qty = self.client.floor_to_step(quantity, rules["step_size"])
                        result = await self.client.create_limit_order(bot.symbol, "SELL", qty, sell_price)
                        bot.orders.append(TestnetOrder(int(result["orderId"]), "SELL", float(sell_price), float(qty), source_price=fill_price))
                        await self._emit("BUY_FILLED", bot, tracked)
                    else:
                        bot.completed_cycles += 1
                        if bot.buy_enabled and not bot.soft_complete:
                            buy_price = self.client.floor_to_step(fill_price / (1 + bot.step_pct / 100), rules["tick_size"])
                            qty = self.client.floor_to_step(quantity, rules["step_size"])
                            result = await self.client.create_limit_order(bot.symbol, "BUY", qty, buy_price)
                            bot.orders.append(TestnetOrder(int(result["orderId"]), "BUY", float(buy_price), float(qty)))
                        await self._emit("SELL_FILLED", bot, tracked)
            if bot.soft_complete and not any(x.side == "SELL" and x.status in {"NEW", "PARTIALLY_FILLED"} for x in bot.orders):
                bot.status = "STOPPED"
            bot.last_sync_at, bot.last_error = self.now(), ""
        except Exception as exc:
            bot.last_error = str(exc)
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
                    self.bot.orders.append(TestnetOrder(int(result["orderId"]), "SELL", float(sell_price), float(qty), source_price=fill_price))
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
