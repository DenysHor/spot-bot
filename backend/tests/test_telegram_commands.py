import asyncio
import re
from types import SimpleNamespace

from app import main


def test_telegram_help_lists_safe_operational_commands():
    response = asyncio.run(main.telegram_command("/help"))

    assert "/apply_recommendations" in response
    assert "/sync_testnet" in response
    assert "/repair_testnet" in response
    assert "/confirm" in response
    assert "LIVE-операції" in response


def test_apply_recommendations_requires_confirmation(monkeypatch):
    bot = SimpleNamespace(id="bot-1", symbol="SOLUSDT", status="RUNNING")
    calls = []

    async def plan():
        return ([{
            "bot_id": bot.id, "symbol": bot.symbol,
            "action": "STOP_BUYS", "recommendation": "зупинити покупки",
        }], [])

    monkeypatch.setattr(main, "_telegram_recommendation_plan", plan)
    monkeypatch.setattr(main.grid_engine, "bots", {bot.id: bot})
    monkeypatch.setattr(
        main.grid_engine, "set_manual_buy_pause",
        lambda bot_id, paused, source="USER": calls.append((bot_id, paused, source)),
    )
    main.telegram_pending_actions.clear()

    preview = asyncio.run(main.telegram_command("/apply_recommendations"))
    assert calls == []
    token = re.search(r"/confirm ([A-F0-9]+)", preview).group(1)

    result = asyncio.run(main.telegram_command(f"/confirm {token}"))
    assert "План виконано" in result
    assert calls == [(bot.id, True, "TELEGRAM_RECOMMENDATION")]


def test_confirmation_is_single_use(monkeypatch):
    async def no_plan():
        return ([], [])

    monkeypatch.setattr(main, "_telegram_recommendation_plan", no_plan)
    token = main._telegram_confirmation("PAPER_RECOMMENDATIONS", {"plan": []})

    first = asyncio.run(main.telegram_command(f"/confirm {token}"))
    second = asyncio.run(main.telegram_command(f"/confirm {token}"))

    assert "План виконано" in first
    assert "Код недійсний" in second


def test_testnet_audit_message_reports_issues():
    message = main._testnet_audit_message({
        "status": "CRITICAL", "tracked_open_orders": 4, "exchange_open_orders": 0,
        "issues": [{"severity": "CRITICAL", "message": "4 заявки відсутні"}],
    })

    assert "CRITICAL" in message
    assert "локально 4 / Binance 0" in message
    assert "4 заявки відсутні" in message
