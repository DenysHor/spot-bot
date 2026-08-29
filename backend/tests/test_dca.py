import asyncio
from datetime import datetime, timedelta, timezone

from app.dca.execution import DcaExecutionEngine
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.persistence.sqlite import SQLiteStore
from app.risk.manager import RiskLimits, RiskManager


async def fake_price(symbol: str) -> float:
    return 100.0


def risk(max_position_pct=50):
    return RiskManager(RiskLimits(80, max_position_pct, 10))


def test_scheduled_and_dip_dca_buys_with_fees(tmp_path):
    store = SQLiteStore(str(tmp_path / "dca.db"))
    portfolio = PaperPortfolio(10_000, store=store)
    engine = DcaExecutionEngine(PaperBroker(portfolio), fake_price, risk(), store=store)
    bot = engine.start_bot("BTCUSDT", "BTC", 100, 1_000, 100, 3600, 5)
    start = datetime.fromisoformat(bot.created_at)

    asyncio.run(engine.tick_bot(bot.id, price=100, now=start))
    assert bot.buy_count == 1
    assert round(bot.spent_quote, 2) == 100.10

    asyncio.run(engine.tick_bot(bot.id, price=96, now=start + timedelta(minutes=10)))
    assert bot.buy_count == 1

    asyncio.run(engine.tick_bot(bot.id, price=95, now=start + timedelta(minutes=11)))
    assert bot.buy_count == 2
    assert bot.events[-1].message == "DIP DCA BUY"
    assert round(portfolio.fees_paid, 2) == 0.20


def test_dca_state_restores_after_restart(tmp_path):
    path = str(tmp_path / "restart.db")
    store = SQLiteStore(path)
    portfolio = PaperPortfolio(10_000, store=store)
    engine = DcaExecutionEngine(PaperBroker(portfolio), fake_price, risk(), store=store)
    bot = engine.start_bot("ETHUSDT", "ETH", 2_000, 500, 50, 60, 3)
    asyncio.run(engine.tick_bot(bot.id, price=2_000, now=datetime.fromisoformat(bot.created_at)))

    restarted_store = SQLiteStore(path)
    restarted_portfolio = PaperPortfolio(store=restarted_store)
    restarted = DcaExecutionEngine(PaperBroker(restarted_portfolio), fake_price, risk(), store=restarted_store)
    restored = restarted.get_bot(bot.id)

    assert restored.buy_count == 1
    assert restored.spent_quote == bot.spent_quote
    assert restored.events == bot.events
    assert restarted_portfolio.position("ETH").quantity == portfolio.position("ETH").quantity


def test_dca_buy_is_blocked_by_risk_manager():
    portfolio = PaperPortfolio(10_000)
    engine = DcaExecutionEngine(PaperBroker(portfolio), fake_price, risk(max_position_pct=0.5))
    bot = engine.start_bot("BTCUSDT", "BTC", 100, 1_000, 100, 60, 5)

    asyncio.run(engine.tick_bot(bot.id, price=100, now=datetime.fromisoformat(bot.created_at)))

    assert bot.buy_count == 0
    assert portfolio.trades == []
    assert bot.events[-1].event == "BUY_BLOCKED"
    assert "risk limit" in bot.events[-1].message.lower()
