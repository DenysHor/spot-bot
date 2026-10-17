import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app import main


def scheduled_item(now):
    return {
        "id": "schedule-1", "symbol": "PUMPUSDT", "status": "PENDING",
        "created_at": now.isoformat(), "starts_at": (now - timedelta(minutes=1)).isoformat(),
        "expires_at": (now + timedelta(hours=1)).isoformat(), "last_checked_at": "",
        "last_reason": "", "attempts": 0,
        "request": {
            "symbol": "PUMPUSDT", "budget_quote": 100.0, "step_pct": 2.4,
            "levels_each_side": 6, "trailing_up_enabled": True,
            "strategy_profile": "TRAILING_GRID", "price_floor": 0.0042,
            "price_ceiling": 0.00535,
        },
        "conditions": {
            "min_rsi14": 40.0, "max_rsi14": 65.0,
            "max_abs_change_24h_pct": 8.0, "allowed_regimes": ["RANGE", "UPTREND"],
        },
    }


def preflight(regime="RANGE", rsi=52.0, price=0.0046, change=1.0):
    return {
        "market": {"price": price, "rsi14": rsi, "change_24h_pct": change},
        "strategy": {"launch_allowed": True, "regime": {"name": regime}},
        "recommendation": "OK",
    }


def test_scheduler_starts_only_after_all_conditions_pass(monkeypatch):
    now = datetime(2026, 10, 17, 18, tzinfo=timezone.utc)
    item = scheduled_item(now)
    started = SimpleNamespace(
        id="bot-1", symbol="PUMPUSDT", budget_quote=100.0,
        step_pct=2.4, levels_each_side=6,
    )

    async def fake_preflight(request):
        return preflight()

    async def fake_start(request, preflight=None):
        return started

    monkeypatch.setattr(main, "scheduled_grid_launches", [item])
    monkeypatch.setattr(main, "grid_preflight_analysis", fake_preflight)
    monkeypatch.setattr(main, "start_paper_grid_bot", fake_start)
    monkeypatch.setattr(main.grid_engine, "bots", {})
    monkeypatch.setattr(main, "_save_scheduled_grid_launches", lambda: None)
    monkeypatch.setattr(main.notifier, "enabled", False)

    asyncio.run(main.scheduled_grid_launch_once(now))

    assert item["status"] == "STARTED"
    assert item["bot_id"] == "bot-1"
    assert item["attempts"] == 1


def test_scheduler_waits_when_rsi_is_overheated(monkeypatch):
    now = datetime(2026, 10, 17, 18, tzinfo=timezone.utc)
    item = scheduled_item(now)

    async def fake_preflight(request):
        return preflight(rsi=72.0)

    async def should_not_start(*args, **kwargs):
        raise AssertionError("bot must not start")

    monkeypatch.setattr(main, "scheduled_grid_launches", [item])
    monkeypatch.setattr(main, "grid_preflight_analysis", fake_preflight)
    monkeypatch.setattr(main, "start_paper_grid_bot", should_not_start)
    monkeypatch.setattr(main.grid_engine, "bots", {})
    monkeypatch.setattr(main, "_save_scheduled_grid_launches", lambda: None)
    monkeypatch.setattr(main.notifier, "enabled", False)

    asyncio.run(main.scheduled_grid_launch_once(now))

    assert item["status"] == "PENDING"
    assert "RSI 72.0" in item["last_reason"]


def test_scheduler_cancels_duplicate_before_launch_window(monkeypatch):
    now = datetime(2026, 10, 17, 6, tzinfo=timezone.utc)
    item = scheduled_item(now + timedelta(hours=12))
    active = SimpleNamespace(symbol="PUMPUSDT", status="RUNNING")

    monkeypatch.setattr(main, "scheduled_grid_launches", [item])
    monkeypatch.setattr(main.grid_engine, "bots", {"bot-1": active})
    monkeypatch.setattr(main, "_save_scheduled_grid_launches", lambda: None)

    asyncio.run(main.scheduled_grid_launch_once(now))

    assert item["status"] == "CANCELLED"
    assert "вже існує активний" in item["last_reason"]
