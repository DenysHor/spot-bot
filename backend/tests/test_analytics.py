from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from fastapi.testclient import TestClient

from app import main
from app.analytics.performance import grid_performance
from app.grid.execution import GridEvent
from app.paper.portfolio import Trade


def test_grid_performance_aggregates_period_metrics_and_series():
    now = datetime(2026, 1, 8, 12, tzinfo=timezone.utc)
    timestamp = (now - timedelta(days=1)).isoformat()
    trades = [
        Trade(1, timestamp, "SOLUSDT", "BUY", 100, 1, 100, 0.1),
        Trade(2, timestamp, "SOLUSDT", "SELL", 101, 1, 101, 0.101, 0.9),
    ]
    bot = SimpleNamespace(symbol="SOLUSDT", budget_quote=1000.0, events=[
        GridEvent(timestamp, "SELL_FILLED", 101, realized_cycle_pnl=0.799),
    ])

    result = grid_performance(trades, {"bot": bot}, 7, "SOLUSDT", now)

    assert round(result["metrics"]["realized_pnl"], 3) == 0.799
    assert round(result["metrics"]["fees"], 3) == 0.201
    assert result["metrics"]["cycles"] == 1
    assert result["metrics"]["win_rate_pct"] == 100
    assert round(result["metrics"]["grid_return_pct"], 4) == 0.0799
    assert result["metrics"]["profit_factor"] is None
    assert result["series"][-2]["cycles"] == 1


def test_analytics_endpoint_adds_buy_and_hold(monkeypatch):
    async def fake_klines(symbol, interval, limit):
        assert (symbol, interval, limit) == ("SOLUSDT", "1h", 170)
        return [[i, "100", "112", "95", "110", "1", i + 1] for i in range(8)]

    monkeypatch.setattr(main.market, "klines", fake_klines)
    response = TestClient(main.app).get("/api/analytics/performance?symbol=SOLUSDT&days=7")

    assert response.status_code == 200
    assert response.json()["buy_hold_return_pct"] == 10.0


def test_risk_metrics_include_cycle_drawdown_and_profit_factor():
    now = datetime(2026, 1, 8, 12, tzinfo=timezone.utc)
    bot = SimpleNamespace(symbol="SOLUSDT", budget_quote=1000.0, events=[
        GridEvent((now - timedelta(hours=2)).isoformat(), "SELL_FILLED", 101, realized_cycle_pnl=1.0),
        GridEvent((now - timedelta(hours=1)).isoformat(), "SELL_FILLED", 100, realized_cycle_pnl=-0.5),
    ])

    metrics = grid_performance([], {"bot": bot}, 7, "SOLUSDT", now)["metrics"]

    assert metrics["profit_factor"] == 2.0
    assert metrics["realized_max_drawdown"] == 0.5
    assert metrics["realized_max_drawdown_pct"] == 0.05
