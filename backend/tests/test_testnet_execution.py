import asyncio

from app.exchange.binance_testnet import BinanceTestnetClient
from app.testnet.execution import TestnetGridEngine as GridEngine


class FakeTestnetClient:
    def __init__(self):
        self.next_id = 1
        self.created = []
        self.statuses = {}

    floor_to_step = staticmethod(BinanceTestnetClient.floor_to_step)

    async def symbol_rules(self, symbol):
        return {"symbol": symbol, "base_asset": "BTC", "quote_asset": "USDT", "status": "TRADING",
                "tick_size": "0.01", "step_size": "0.00001", "min_qty": "0.00001", "min_notional": "5"}

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
        first = bot.orders[0]
        client.statuses[first.order_id] = {"status": "FILLED", "executedQty": "0.50505", "cummulativeQuoteQty": "50"}
        await engine.sync()
        assert any(order.side == "SELL" for order in bot.orders)
        assert bot.snapshot()["virtual_funds"] is True
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
