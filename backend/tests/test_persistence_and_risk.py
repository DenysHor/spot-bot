import asyncio
from datetime import datetime, timedelta, timezone

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
    bot = engine.start_bot("BTCUSDT", "BTC", 100, 1_000, 10, 2, trailing_up_enabled=True)

    asyncio.run(engine.tick_bot(bot.id, price=90))
    engine.set_manual_buy_pause(bot.id, True)
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
    assert restored_bot.trailing_up_enabled is True
    assert restored_bot.recenter_count == bot.recenter_count
    assert restored_bot.manual_buy_paused is True


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


def test_portfolio_risk_status_and_daily_loss_guard():
    risk = RiskManager(RiskLimits(
        max_portfolio_allocation_pct=50, max_position_pct=10, reserve_quote_pct=20,
        max_daily_loss_pct=2, max_open_positions=3,
    ))

    status = risk.status(
        total_equity=10_000, free_quote=7_000, allocated_quote=3_000,
        daily_realized_pnl=-250, active_positions=2,
    )
    decision = risk.check_buy(
        total_equity=10_000, free_quote=7_000, current_bot_allocation=3_000,
        current_position_value=0, requested_quote=100, daily_realized_pnl=-250,
        active_positions=2, opens_new_position=True,
    )

    assert status["status"] == "BLOCKED"
    assert any(check["code"] == "DAILY_LOSS" and not check["passed"] for check in status["checks"])
    assert decision.allowed is False
    assert "Daily loss" in decision.reason


def test_decision_journal_persists_structured_context(tmp_path):
    store = SQLiteStore(str(tmp_path / "journal.db"))
    decision_id = store.record_decision(
        kind="RECOMMENDATION", source="ADVISOR", status="PROPOSED",
        symbol="BTCUSDT", action="STOP_BUYS", recommendation="Pause buys",
        reasons=["downtrend"], context={"score": 42}, fingerprint="daily-btc",
    )
    duplicate = store.record_decision(
        kind="RECOMMENDATION", source="ADVISOR", status="PROPOSED",
        symbol="BTCUSDT", fingerprint="daily-btc",
    )

    rows = store.list_decisions(symbol="BTCUSDT")
    assert decision_id is not None
    assert duplicate is None
    assert len(rows) == 1
    assert rows[0]["reasons"] == ["downtrend"]
    assert rows[0]["context"] == {"score": 42}


def test_decision_journal_evaluates_24h_and_72h_outcomes(tmp_path):
    store = SQLiteStore(str(tmp_path / "outcomes.db"))
    decision_id = store.record_decision(
        kind="RECOMMENDATION", source="ADVISOR", status="PROPOSED",
        symbol="BTCUSDT", bot_id="bot-1", recommendation="Continue",
        context={"metrics": {"total_pnl": 5.0}},
    )
    now = datetime.now(timezone.utc)
    with store.connect() as db:
        db.execute("UPDATE decision_journal SET created_at = ? WHERE id = ?",
                   ((now - timedelta(hours=73)).isoformat(), decision_id))

    assert store.evaluate_decisions({"bot-1": 8.5}, now=now) == 2
    row = store.list_decisions()[0]
    assert row["outcome_24h_status"] == "IMPROVED"
    assert row["outcome_72h_status"] == "IMPROVED"
    assert row["outcome_24h_delta"] == 3.5
    assert row["outcome_72h_delta"] == 3.5
