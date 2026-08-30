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


def test_notifier_sends_trailing_recenter_event():
    sent = []

    async def sender(text):
        sent.append(text)

    bot = SimpleNamespace(id="bot1", symbol="SOLUSDT", events=[])
    notifier = TelegramNotifier(
        "token", "chat", SimpleNamespace(bots={bot.id: bot}),
        SimpleNamespace(bots={}), sender=sender,
    )
    notifier.seed_existing()
    bot.events.append(GridEvent(
        "later", "GRID_RECENTERED", 105.0,
        message="Trailing Up shifted 8 BUY levels from anchor 100.0 to 105.0",
    ))

    asyncio.run(notifier.scan_once(datetime(2026, 1, 1, 12, tzinfo=timezone.utc)))

    assert len(sent) == 1
    assert "GRID_RECENTERED · SOLUSDT" in sent[0]
    assert "shifted 8 BUY levels" in sent[0]


def test_notifier_sends_buy_side_pause_and_resume_events():
    sent = []

    async def sender(text):
        sent.append(text)

    bot = SimpleNamespace(id="bot1", symbol="SOLUSDT", events=[])
    notifier = TelegramNotifier(
        "token", "chat", SimpleNamespace(bots={bot.id: bot}),
        SimpleNamespace(bots={}), sender=sender,
    )
    notifier.seed_existing()
    bot.events.extend([
        GridEvent("later1", "BUY_SIDE_PAUSED", 90.0, message="SELL remains active"),
        GridEvent("later2", "BUY_SIDE_RESUMED", 95.0, message="liquidity restored"),
    ])

    asyncio.run(notifier.scan_once(datetime(2026, 1, 1, 12, tzinfo=timezone.utc)))

    assert len(sent) == 2
    assert "BUY_SIDE_PAUSED" in sent[0]
    assert "BUY_SIDE_RESUMED" in sent[1]


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


def test_weekly_evaluation_is_sent_once_per_week():
    sent = []

    async def sender(text):
        sent.append(text)

    async def weekly_provider(symbol):
        return f"Weekly evaluation · {symbol}"

    store = FakeStore()
    bot = SimpleNamespace(
        id="bot1", symbol="SOLUSDT", base_asset="SOL", status="RUNNING",
        last_price=100.0, completed_cycles=1, events=[],
    )
    notifier = TelegramNotifier(
        "token", "chat", SimpleNamespace(bots={bot.id: bot}), SimpleNamespace(bots={}),
        portfolio=SimpleNamespace(snapshot=lambda prices: {}), store=store, sender=sender,
    )
    notifier.weekly_report_provider = weekly_provider
    monday = datetime(2026, 1, 5, 21, tzinfo=timezone.utc)

    asyncio.run(notifier.scan_once(monday))
    asyncio.run(notifier.scan_once(monday))

    assert sum("Weekly evaluation" in message for message in sent) == 1
    assert store.state["weekly_report_week"] == "2026-W02"


def test_health_attention_alert_is_sent_once_per_day():
    sent = []

    async def sender(text):
        sent.append(text)

    store = FakeStore()
    notifier = TelegramNotifier(
        "token", "chat", SimpleNamespace(bots={}), SimpleNamespace(bots={}),
        store=store, sender=sender,
    )
    notifier.health_alert_provider = lambda now: [{
        "key": "bot1:IDLE", "message": "SOLUSDT · Немає угод 48 год",
    }]
    noon = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)

    asyncio.run(notifier.scan_once(noon))
    asyncio.run(notifier.scan_once(noon))

    assert sent == ["SOLUSDT · Немає угод 48 год"]
    assert store.state["health_alert:bot1:IDLE"] == "2026-01-01"
