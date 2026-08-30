import asyncio
from datetime import datetime, timedelta, timezone

from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.risk.manager import RiskLimits, RiskManager
from app.signal.execution import SignalExecutionEngine
from app.persistence.sqlite import SQLiteStore


def analysis(price=100.0, score=75, rsi=55.0, volume=1.3, atr=2.0):
    return {
        "price": price, "score": score, "ema20": price * 0.99, "ema50": price * 0.97,
        "rsi14": rsi, "volume_ratio": volume, "atr_pct": atr,
        "reasons": ["ціна вище EMA20", "обсяг прискорюється"],
    }


def engine():
    portfolio = PaperPortfolio(1000)
    broker = PaperBroker(portfolio)

    async def provider(symbol, base_asset):
        return analysis()

    risk = RiskManager(RiskLimits(max_portfolio_allocation_pct=90, max_position_pct=50, reserve_quote_pct=10))
    return SignalExecutionEngine(broker, provider, risk), portfolio


def test_signal_bot_buys_only_after_multiple_confirmations():
    subject, portfolio = engine()
    bot = subject.start_bot("SOLUSDT", "SOL", 100, 65)

    asyncio.run(subject.tick_bot(bot.id, analysis(score=80, volume=1.0)))
    assert bot.quantity == 0

    asyncio.run(subject.tick_bot(bot.id, analysis(score=80, volume=1.4)))
    assert bot.quantity == 1
    assert bot.target_price == 103
    assert bot.stop_price == 98
    assert portfolio.quote_balance == 899.9
    assert bot.events[-1].event == "SIGNAL_BUY_FILLED"


def test_signal_bot_sells_at_target_and_records_result():
    subject, _ = engine()
    bot = subject.start_bot("SOLUSDT", "SOL", 100, 65)
    opened = datetime(2026, 1, 1, tzinfo=timezone.utc)
    asyncio.run(subject.tick_bot(bot.id, analysis(), opened))

    asyncio.run(subject.tick_bot(bot.id, analysis(price=103.1, score=70), opened + timedelta(hours=4)))

    assert bot.quantity == 0
    assert bot.completed_trades == 1
    assert bot.realized_pnl > 0
    assert bot.events[-1].event == "SIGNAL_SELL_FILLED"
    assert "ціль" in bot.events[-1].message


def test_signal_bot_exits_after_maximum_holding_time():
    subject, _ = engine()
    bot = subject.start_bot("SOLUSDT", "SOL", 100, 65)
    opened = datetime(2026, 1, 1, tzinfo=timezone.utc)
    asyncio.run(subject.tick_bot(bot.id, analysis(), opened))

    asyncio.run(subject.tick_bot(bot.id, analysis(price=100.5, score=60), opened + timedelta(hours=73)))

    assert bot.quantity == 0
    assert "72 години" in bot.events[-1].message


def test_signal_bot_cannot_pause_protection_for_open_position():
    subject, _ = engine()
    bot = subject.start_bot("SOLUSDT", "SOL", 100, 65)
    asyncio.run(subject.tick_bot(bot.id, analysis()))

    try:
        subject.pause_bot(bot.id)
        assert False, "pause must be rejected"
    except ValueError as exc:
        assert "захисний супровід" in str(exc)
    assert bot.status == "RUNNING"


def test_signal_bot_state_survives_restart(tmp_path):
    store = SQLiteStore(str(tmp_path / "signal.db"))
    portfolio = PaperPortfolio(1000, store=store)
    broker = PaperBroker(portfolio)

    async def provider(symbol, base_asset):
        return analysis()

    risk = RiskManager(RiskLimits(max_portfolio_allocation_pct=90, max_position_pct=50, reserve_quote_pct=10))
    first = SignalExecutionEngine(broker, provider, risk, store=store)
    bot = first.start_bot("SOLUSDT", "SOL", 100, 65)
    asyncio.run(first.tick_bot(bot.id, analysis()))

    restored = SignalExecutionEngine(broker, provider, risk, store=store).get_bot(bot.id)
    assert restored.quantity == 1
    assert restored.target_price == 103
    assert restored.events[-1].event == "SIGNAL_BUY_FILLED"
