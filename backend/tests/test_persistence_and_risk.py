import asyncio

from app.grid.execution import GridExecutionEngine
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.persistence.sqlite import SQLiteStore
from app.risk.manager import RiskLimits, RiskManager


async def fake_price(symbol: str) -> float:
    return 100.0


def test_portfolio_and_grid_state_restore_after_restart(tmp_path):
    db_path = str(tmp_path / "restart.db")
    store = SQLiteStore(db_path)
    portfolio = PaperPortfolio(starting_quote=10_000, store=store)
    broker = PaperBroker(portfolio)
    engine = GridExecutionEngine(broker, fake_price, store=store)
    bot = engine.start_bot("BTCUSDT", "BTC", 100, 1_000, 10, 2)

    asyncio.run(engine.tick_bot(bot.id, price=90))
    original_trade = portfolio.trades[0]
    original_orders = bot.open_orders.copy()
    original_events = bot.events.copy()

    restarted_store = SQLiteStore(db_path)
    restarted_portfolio = PaperPortfolio(starting_quote=123, store=restarted_store)
    restarted_engine = GridExecutionEngine(
        PaperBroker(restarted_portfolio), fake_price, store=restarted_store
    )
    restored_bot = restarted_engine.get_bot(bot.id)

    assert restarted_portfolio.starting_quote == 10_000
    assert restarted_portfolio.quote_balance == portfolio.quote_balance
    assert restarted_portfolio.position("BTC").quantity == portfolio.position("BTC").quantity
    assert restarted_portfolio.trades[0] == original_trade
    assert restored_bot.status == "RUNNING"
    assert restored_bot.spent_quote == bot.spent_quote
    assert restored_bot.open_orders == original_orders
    assert restored_bot.events == original_events


def test_automatic_buy_is_blocked_by_risk_limits(tmp_path):
    store = SQLiteStore(str(tmp_path / "risk.db"))
    portfolio = PaperPortfolio(starting_quote=10_000, store=store)
    broker = PaperBroker(portfolio)
    risk = RiskManager(RiskLimits(
        max_portfolio_allocation_pct=50,
        max_position_pct=2,
        reserve_quote_pct=20,
    ))
    engine = GridExecutionEngine(broker, fake_price, store=store, risk_manager=risk)
    bot = engine.start_bot("BTCUSDT", "BTC", 100, 1_000, 10, 2)

    asyncio.run(engine.tick_bot(bot.id, price=90))

    assert portfolio.trades == []
    assert portfolio.position("BTC").quantity == 0
    assert len(bot.open_orders) == 2
    blocked = [event for event in bot.events if event.event == "BUY_BLOCKED"]
    assert len(blocked) == 1
    assert "risk limit" in blocked[0].message.lower()

    restarted = GridExecutionEngine(PaperBroker(PaperPortfolio(store=store)), fake_price, store=store)
    assert any(event.event == "BUY_BLOCKED" for event in restarted.get_bot(bot.id).events)
