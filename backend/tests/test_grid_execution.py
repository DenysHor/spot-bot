import asyncio

from app.grid.execution import GridExecutionEngine
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.risk.manager import RiskLimits, RiskManager


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


def test_hybrid_bot_opens_seed_and_keeps_grid_budget_separate():
    portfolio = PaperPortfolio(starting_quote=10_000.0, quote_asset="USDT")
    broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    bot = engine.start_bot(
        "BTCUSDT", "BTC", 100.0, 1_000.0, 2.0, 4,
        trailing_up_enabled=True, strategy_profile="UPTREND_HYBRID_30",
        seed_position_pct=30.0,
    )

    snapshot = bot.snapshot()
    assert snapshot["strategy_profile"] == "UPTREND_HYBRID_30"
    assert snapshot["grid_budget_quote"] == 700.0
    assert round(snapshot["seed_cost_quote"], 8) == 300.0
    assert snapshot["seed_quantity"] > 0
    assert len(bot.open_orders) == 4
    assert round(sum(order.quote_amount * 1.001 for order in bot.open_orders), 8) == 700.0
    assert bot.events[0].event == "HYBRID_SEED_BOUGHT"

    engine.stop_bot(bot.id)
    stopped = bot.snapshot()
    assert stopped["seed_quantity"] == 0
    assert bot.events[-2].event == "HYBRID_SEED_SOLD"
    assert bot.events[-1].event == "BOT_STOPPED"


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
    assert bot.events[-2].message == "RuntimeError: market unavailable"
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


def test_trailing_up_recenters_unfilled_buys_without_exceeding_level_count():
    portfolio = PaperPortfolio()
    broker = PaperBroker(portfolio=portfolio)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    bot = engine.start_bot("SOLUSDT", "SOL", 100.0, 1000.0, 1.0, 4, trailing_up_enabled=True)

    asyncio.run(engine.tick_bot(bot.id, price=101.9))
    assert bot.recenter_count == 0
    asyncio.run(engine.tick_bot(bot.id, price=102.0))

    buys = sorted((order.trigger_price for order in bot.open_orders if order.side == "BUY"), reverse=True)
    assert bot.recenter_count == 1
    assert bot.reference_price == 102.0
    assert len(buys) == 4
    assert round(buys[0], 8) == round(102.0 * 0.99, 8)
    assert bot.events[-1].event == "GRID_RECENTERED"
    assert portfolio.trades == []


def test_trailing_up_preserves_open_sell_and_total_level_count():
    portfolio = PaperPortfolio()
    broker = PaperBroker(portfolio=portfolio)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    bot = engine.start_bot("SOLUSDT", "SOL", 100.0, 1000.0, 10.0, 4, trailing_up_enabled=True)
    asyncio.run(engine.tick_bot(bot.id, price=90.0))
    sell = next(order for order in bot.open_orders if order.side == "SELL")
    asyncio.run(engine.tick_bot(bot.id, price=120.0))

    # The SELL may fill at 120; any replacement BUY is safely rebuilt around the new anchor.
    assert bot.recenter_count == 1
    assert len(bot.open_orders) == bot.levels_each_side
    assert sell not in bot.open_orders


def test_trailing_up_stops_at_daily_recenter_limit():
    portfolio = PaperPortfolio()
    broker = PaperBroker(portfolio=portfolio)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    bot = engine.start_bot("SOLUSDT", "SOL", 100.0, 1000.0, 1.0, 4, trailing_up_enabled=True)
    for price in (102.0, 104.04, 106.1208):
        asyncio.run(engine.tick_bot(bot.id, price=price))

    capped_anchor = bot.reference_price
    asyncio.run(engine.tick_bot(bot.id, price=108.5))
    asyncio.run(engine.tick_bot(bot.id, price=109.0))

    assert bot.recenter_count_today == 3
    assert bot.reference_price == capped_anchor
    assert sum(event.event == "RECENTER_LIMIT_REACHED" for event in bot.events) == 1
    assert bot.snapshot()["next_recenter_price"] > capped_anchor


def test_buy_side_pauses_for_low_unreserved_quote_while_sell_remains_active():
    portfolio = PaperPortfolio(starting_quote=1_000.0, quote_asset="USDT")
    broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)

    async def fake_price(symbol: str) -> float:
        return 100.0

    risk = RiskManager(RiskLimits(
        max_portfolio_allocation_pct=100, max_position_pct=100, reserve_quote_pct=20,
    ))
    engine = GridExecutionEngine(broker=broker, price_provider=fake_price, risk_manager=risk)
    bot = engine.start_bot("SOLUSDT", "SOL", 100.0, 400.0, 10.0, 2)

    asyncio.run(engine.tick_bot(bot.id, price=90.0))
    assert any(order.side == "SELL" for order in bot.open_orders)
    broker.market_buy("OTHERUSDT", "OTHER", 10.0, 500.0)

    asyncio.run(engine.tick_bot(bot.id, price=80.0))
    assert bot.buy_paused is True
    assert bot.snapshot()["execution_status"] == "BUY_PAUSED"
    assert any(event.event == "BUY_SIDE_PAUSED" for event in bot.events)

    asyncio.run(engine.tick_bot(bot.id, price=100.0))
    assert bot.completed_cycles == 1
    assert bot.buy_paused is True

    asyncio.run(engine.tick_bot(bot.id, price=100.0))
    assert bot.buy_paused is False
    assert any(event.event == "BUY_SIDE_RESUMED" for event in bot.events)


def test_price_corridor_blocks_buys_but_keeps_sells_active():
    portfolio = PaperPortfolio(starting_quote=10_000.0, quote_asset="USDT")
    broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    bot = engine.start_bot(
        "SOLUSDT", "SOL", 100.0, 1_000.0, 10.0, 2,
        price_floor=80.0, price_ceiling=120.0,
    )
    asyncio.run(engine.tick_bot(bot.id, price=90.0))
    assert bot.snapshot()["open_positions"]

    asyncio.run(engine.tick_bot(bot.id, price=70.0))
    assert bot.out_of_range is True
    assert bot.snapshot()["execution_status"] == "OUT_OF_RANGE"
    assert sum(trade.side == "BUY" for trade in portfolio.trades) == 1
    assert any(event.event == "PRICE_RANGE_EXITED" for event in bot.events)

    asyncio.run(engine.tick_bot(bot.id, price=100.0))
    assert bot.out_of_range is False
    assert bot.completed_cycles == 1
    assert any(event.event == "PRICE_RANGE_REENTERED" for event in bot.events)


def test_soft_completion_sells_positions_without_replacement_buy():
    portfolio = PaperPortfolio(starting_quote=10_000.0, quote_asset="USDT")
    broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    bot = engine.start_bot("BTCUSDT", "BTC", 100.0, 1_000.0, 10.0, 2)
    asyncio.run(engine.tick_bot(bot.id, price=90.0))
    engine.start_draining(bot.id)

    assert bot.drain_mode is True
    assert all(order.side == "SELL" for order in bot.open_orders)
    asyncio.run(engine.tick_bot(bot.id, price=100.0))
    assert bot.status == "STOPPED"
    assert bot.completed_cycles == 1
    assert bot.open_orders == []
    assert any(event.event == "DRAIN_MODE_COMPLETED" for event in bot.events)


def test_manual_buy_control_keeps_sells_active_and_can_resume():
    portfolio = PaperPortfolio(starting_quote=10_000.0, quote_asset="USDT")
    broker = PaperBroker(portfolio=portfolio, fee_rate=0.001)

    async def fake_price(symbol: str) -> float:
        return 100.0

    engine = GridExecutionEngine(broker=broker, price_provider=fake_price)
    bot = engine.start_bot("SOLUSDT", "SOL", 100.0, 1_000.0, 10.0, 2)
    asyncio.run(engine.tick_bot(bot.id, price=90.0))
    engine.set_manual_buy_pause(bot.id, True)

    asyncio.run(engine.tick_bot(bot.id, price=80.0))
    assert sum(trade.side == "BUY" for trade in portfolio.trades) == 1
    snapshot = bot.snapshot()
    assert snapshot["execution_status"] == "BUYS_DISABLED"
    assert snapshot["work_stage"] == "BUYS_DISABLED"
    assert snapshot["average_open_buy_price"] == 90.0
    assert round(snapshot["nearest_sell_price"], 8) == 99.0

    asyncio.run(engine.tick_bot(bot.id, price=100.0))
    assert bot.completed_cycles == 1
    engine.set_manual_buy_pause(bot.id, False)
    assert bot.manual_buy_paused is False
    assert any(event.event == "MANUAL_BUYS_ENABLED" for event in bot.events)
