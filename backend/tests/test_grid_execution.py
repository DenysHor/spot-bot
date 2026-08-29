import asyncio

from app.grid.execution import GridExecutionEngine
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio


def test_grid_buy_sell_cycle_and_replacement_buy():
    portfolio = PaperPortfolio(starting_quote=10_000.0, quote_asset="USDT")
    broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price, poll_seconds=60)
    bot = engine.start_bot(
        symbol="BTCUSDT",
        base_asset="BTC",
        reference_price=100.0,
        budget_quote=1_000.0,
        step_pct=10.0,
        levels_each_side=2,
    )

    assert len([o for o in bot.open_orders if o.side == "BUY"]) == 2
    assert bot.completed_cycles == 0

    # Cross first BUY at 90.
    asyncio.run(engine.tick_bot(bot.id, price=90.0))
    assert portfolio.position("BTC").quantity > 0
    assert len([o for o in bot.open_orders if o.side == "SELL"]) == 1

    sell_order = next(o for o in bot.open_orders if o.side == "SELL")
    assert round(sell_order.trigger_price, 8) == 99.0

    # Cross paired SELL and complete one profitable cycle.
    asyncio.run(engine.tick_bot(bot.id, price=100.0))
    assert bot.completed_cycles == 1
    assert bot.realized_pnl > 0
    assert len([o for o in bot.open_orders if o.side == "BUY"]) >= 2

    replacement = max(o.trigger_price for o in bot.open_orders if o.side == "BUY")
    assert round(replacement, 8) == round(100.0 / 1.1, 8)


def test_only_one_running_bot_per_symbol():
    portfolio = PaperPortfolio()
    broker = PaperBroker(portfolio=portfolio)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    engine.start_bot("BTCUSDT", "BTC", 100.0, 1000.0, 2.0, 4)

    try:
        engine.start_bot("BTCUSDT", "BTC", 100.0, 1000.0, 2.0, 4)
        assert False, "Expected duplicate running bot to be rejected"
    except ValueError as exc:
        assert "already exists" in str(exc)


def test_grid_auto_pauses_after_three_engine_errors():
    portfolio = PaperPortfolio()
    broker = PaperBroker(portfolio=portfolio)

    async def failing_price(symbol: str) -> float:
        raise RuntimeError("market unavailable")

    engine = GridExecutionEngine(broker=broker, price_provider=failing_price)
    bot = engine.start_bot("SOLUSDT", "SOL", 100.0, 1000.0, 1.0, 4)
    for _ in range(3):
        asyncio.run(engine.tick_all())

    assert bot.status == "PAUSED"
    assert bot.consecutive_errors == 3
    assert bot.events[-1].event == "AUTO_PAUSED"
    engine.resume_bot(bot.id)
    assert bot.status == "RUNNING"
    assert bot.consecutive_errors == 0


def test_grid_with_open_sell_must_be_paused_not_stopped():
    portfolio = PaperPortfolio()
    broker = PaperBroker(portfolio=portfolio)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    bot = engine.start_bot("SOLUSDT", "SOL", 100.0, 1000.0, 10.0, 2)
    asyncio.run(engine.tick_bot(bot.id, price=90.0))

    try:
        engine.stop_bot(bot.id)
        assert False, "Expected stop with open SELL to be rejected"
    except ValueError as exc:
        assert "pause" in str(exc).lower()
    engine.pause_bot(bot.id)
    assert bot.status == "PAUSED"
