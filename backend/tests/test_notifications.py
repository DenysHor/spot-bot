import asyncio
from types import SimpleNamespace

from app.grid.execution import GridEvent
from app.notifications.telegram import TelegramNotifier


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
    asyncio.run(notifier.scan_once())
    asyncio.run(notifier.scan_once())

    assert len(sent) == 1
    assert "BUY_FILLED · SOLUSDT" in sent[0]
    assert "100.0000 USDT" in sent[0]
