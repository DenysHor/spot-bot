import asyncio

from app.exchange.binance_testnet import BinanceTestnetClient
from app.testnet.execution import TestnetGridEngine as GridEngine


class FakeTestnetClient:
    def __init__(self):
        self.next_id = 1
        self.created = []
        self.statuses = {}
        self.trade_rows = {}
        self.extra_open_orders = []

    floor_to_step = staticmethod(BinanceTestnetClient.floor_to_step)

    async def symbol_rules(self, symbol):
        return {"symbol": symbol, "base_asset": "BTC", "quote_asset": "USDT", "status": "TRADING",
                "tick_size": "0.01", "step_size": "0.00001", "min_qty": "0.00001", "min_notional": "5"}

    async def price(self, symbol):
        return 100.0

    async def trades(self, symbol, order_id):
        return self.trade_rows.get(order_id, [])

    async def create_limit_order(self, symbol, side, quantity, price):
        order_id = self.next_id
        self.next_id += 1
        self.created.append((order_id, side, quantity, price))
        self.statuses[order_id] = {"status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0"}
        return {"orderId": order_id}

    async def order(self, symbol, order_id):
        return {"orderId": order_id, **self.statuses[order_id]}

    async def cancel_order(self, symbol, order_id):
        self.statuses[order_id]["status"] = "CANCELED"
        return {"orderId": order_id, "status": "CANCELED"}

    async def open_orders(self, symbol):
        rows = []
        for order_id, side, quantity, price in self.created:
            status = self.statuses[order_id]
            if status["status"] in {"NEW", "PARTIALLY_FILLED"}:
                rows.append({"orderId": order_id, "side": side, "origQty": quantity, "price": price,
                             "executedQty": status.get("executedQty", "0")})
        return rows + self.extra_open_orders

    async def account(self):
        orders = await self.open_orders("BTCUSDT")
        quote_locked = sum(float(row["price"]) * (float(row["origQty"]) - float(row.get("executedQty", 0)))
                           for row in orders if row["side"] == "BUY")
        base_locked = sum(float(row["origQty"]) - float(row.get("executedQty", 0))
                          for row in orders if row["side"] == "SELL")
        return {"balances": [
            {"asset": "USDT", "free": "1000", "locked": str(quote_locked)},
            {"asset": "BTC", "free": "1", "locked": str(base_locked)},
        ]}

    async def cancel_open_orders(self, symbol):
        canceled = []
        for order_id, status in self.statuses.items():
            if status["status"] in {"NEW", "PARTIALLY_FILLED"}:
                status["status"] = "CANCELED"
                canceled.append({"orderId": order_id, "status": "CANCELED"})
        self.extra_open_orders = []
        return canceled


def test_floor_to_step_never_rounds_up():
    assert BinanceTestnetClient.floor_to_step("1.239", "0.01") == "1.23"
    assert BinanceTestnetClient.floor_to_step("0.123456", "0.0001") == "0.1234"


def test_testnet_grid_creates_buy_levels_and_paired_sell():
    async def scenario():
        client = FakeTestnetClient()
        engine = GridEngine(client)
        bot = await engine.start("BTCUSDT", 100, 1, 2, 100)
        assert len(bot.orders) == 2
        assert all(order.side == "BUY" for order in bot.orders)
        assert all(order.created_at for order in bot.orders)
        first = bot.orders[0]
        client.statuses[first.order_id] = {"status": "FILLED", "executedQty": "0.50505", "cummulativeQuoteQty": "50"}
        client.trade_rows[first.order_id] = [{"commissionAsset": "BTC", "commission": "0.00005"}]
        await engine.sync()
        sell = next(order for order in bot.orders if order.side == "SELL")
        assert sell.quantity == 0.505
        assert bot.fees["BTC"] == 0.00005
        client.statuses[sell.order_id] = {"status": "FILLED", "executedQty": "0.505", "cummulativeQuoteQty": "51"}
        client.trade_rows[sell.order_id] = [{"commissionAsset": "USDT", "commission": "0.05"}]
        await engine.sync()
        assert bot.completed_cycles == 1
        assert round(bot.realized_pnl, 8) == 0.95
        assert bot.last_price == 100
        snapshot = bot.snapshot()
        assert snapshot["virtual_funds"] is True
        assert snapshot["sync_stale"] is False
        assert snapshot["sync_age_seconds"] >= 0
    asyncio.run(scenario())


def test_testnet_grid_stop_buys_keeps_sell_orders():
    async def scenario():
        client = FakeTestnetClient()
        engine = GridEngine(client)
        bot = await engine.start("BTCUSDT", 100, 1, 2, 100)
        bot.orders.append(type(bot.orders[0])(99, "SELL", 101, 0.5))
        client.statuses[99] = {"status": "NEW", "executedQty": "0", "cummulativeQuoteQty": "0"}
        await engine.stop_buys(soft_complete=True)
        assert bot.status == "DRAINING"
        assert all(order.status == "CANCELED" for order in bot.orders if order.side == "BUY")
        assert next(order for order in bot.orders if order.side == "SELL").status == "NEW"
    asyncio.run(scenario())


def test_testnet_grid_batches_multiple_fills_into_one_notification():
    async def scenario():
        client = FakeTestnetClient()
        engine = GridEngine(client)
        notifications = []

        async def sink(event, bot, payload):
            notifications.append((event, payload))

        engine.event_sink = sink
        bot = await engine.start("BTCUSDT", 100, 1, 2, 100)
        notifications.clear()
        for order in bot.orders:
            client.statuses[order.order_id] = {
                "status": "FILLED", "executedQty": "0.5", "cummulativeQuoteQty": "50",
            }
        await engine.sync()

        assert len(notifications) == 1
        event, payload = notifications[0]
        assert event == "SYNC_BATCH"
        assert len(payload["fills"]) == 2
        assert all(row["event"] == "BUY_FILLED" for row in payload["fills"])

    asyncio.run(scenario())


def test_testnet_reconciliation_is_safe_when_orders_and_balances_match():
    async def scenario():
        client = FakeTestnetClient()
        engine = GridEngine(client)
        await engine.start("BTCUSDT", 100, 1, 2, 100)
        audit = await engine.reconciliation()

        assert audit["status"] == "SAFE"
        assert audit["tracked_open_orders"] == 2
        assert audit["exchange_open_orders"] == 2
        assert audit["issues"] == []

    asyncio.run(scenario())


def test_testnet_reconciliation_accounts_for_partial_fill_without_false_alert():
    async def scenario():
        client = FakeTestnetClient()
        engine = GridEngine(client)
        bot = await engine.start("BTCUSDT", 100, 1, 2, 100)
        first = bot.orders[0]
        client.statuses[first.order_id] = {
            "status": "PARTIALLY_FILLED", "executedQty": str(first.quantity / 2),
            "cummulativeQuoteQty": str(first.price * first.quantity / 2),
        }
        audit = await engine.reconciliation()

        assert audit["status"] == "SAFE"
        assert first.executed_quantity > 0

    asyncio.run(scenario())


def test_testnet_reconciliation_detects_untracked_order_and_emergency_cancels_all():
    async def scenario():
        client = FakeTestnetClient()
        engine = GridEngine(client)
        bot = await engine.start("BTCUSDT", 100, 1, 2, 100)
        client.extra_open_orders.append({
            "orderId": 999, "side": "BUY", "origQty": "0.1", "price": "90", "executedQty": "0",
        })
        audit = await engine.reconciliation()
        assert audit["status"] == "CRITICAL"
        assert audit["untracked_order_ids"] == [999]

        stopped = await engine.emergency_stop()
        assert stopped.status == "STOPPED"
        assert stopped.buy_enabled is False
        assert all(order.status == "CANCELED" for order in bot.orders)
        assert await client.open_orders("BTCUSDT") == []

    asyncio.run(scenario())
