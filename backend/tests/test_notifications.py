import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from app.grid.execution import GridEvent
from app.notifications.telegram import TelegramNotifier


class FakeStore:
    def __init__(self):
        self.state = {}
        self.log = []

    def get_notification_state(self, key, default=""):
        return self.state.get(key, default)

    def set_notification_state(self, key, value):
        self.state[key] = value

    def record_notification(self, kind, status, message, error=""):
        self.log.append((kind, status, message, error))


def test_notifier_sends_only_new_important_events():
    sent = []

    async def sender(text):
        sent.append(text)

    bot = SimpleNamespace(
        id="bot1", symbol="SOLUSDT",
        events=[GridEvent("now", "BOT_STARTED", 100.0)],
    )
    grid = SimpleNamespace(bots={bot.id: bot})
    dca = SimpleNamespace(bots={})
    notifier = TelegramNotifier("token", "chat", grid, dca, sender=sender)
    notifier.seed_existing()

    bot.events.append(GridEvent("later", "BUY_FILLED", 99.0, quote_amount=100.0))
    noon = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    asyncio.run(notifier.scan_once(noon))
    asyncio.run(notifier.scan_once(noon))

    assert len(sent) == 1
    assert "BUY_FILLED · SOLUSDT" in sent[0]
    assert "100.0000 USDT" in sent[0]


def test_cursors_and_daily_report_survive_restart():
    sent = []

    async def sender(text):
        sent.append(text)

    store = FakeStore()
    bot = SimpleNamespace(
        id="bot1", symbol="SOLUSDT", base_asset="SOL", status="RUNNING",
        last_price=100.0, completed_cycles=2,
        events=[GridEvent("now", "BOT_STARTED", 100.0)],
    )
    grid = SimpleNamespace(bots={bot.id: bot})
    dca = SimpleNamespace(bots={})
    portfolio = SimpleNamespace(snapshot=lambda prices: {
        "total_equity": 10010.0, "realized_pnl": 8.0,
        "unrealized_pnl": 2.0, "fees_paid": 1.0,
    })
    first = TelegramNotifier("token", "chat", grid, dca, portfolio=portfolio, store=store, sender=sender)
    first.seed_existing()
    bot.events.append(GridEvent("later", "BUY_FILLED", 99.0, quote_amount=100.0))
    noon = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    asyncio.run(first.scan_once(noon))

    restarted = TelegramNotifier("token", "chat", grid, dca, portfolio=portfolio, store=store, sender=sender)
    restarted.seed_existing()
    asyncio.run(restarted.scan_once(noon))
    report_time = datetime(2026, 1, 1, 21, tzinfo=timezone.utc)
    asyncio.run(restarted.scan_once(report_time))
    asyncio.run(restarted.scan_once(report_time))

    assert sum("BUY_FILLED" in message for message in sent) == 1
    assert sum("Daily PAPER report" in message for message in sent) == 1
    assert store.state["daily_report_date"] == "2026-01-01"
    assert len(store.log) == 2
