from fastapi.testclient import TestClient

from app import main


def test_dashboard_and_static_assets_are_served():
    client = TestClient(main.app)

    page = client.get("/")
    script = client.get("/static/app.js")
    styles = client.get("/static/styles.css")

    assert page.status_code == 200
    assert "Spot Grid Lab" in page.text
    assert script.status_code == 200
    assert "/api/backtest/grid" in script.text
    assert "/api/dca/bots/start" in script.text
    assert "/api/backtest/grid/optimize" in script.text
    assert "/api/backtest/grid/walk-forward" in script.text
    assert "/api/grid/bots/${id}/pause" in script.text
    assert "Smart DCA" in page.text
    assert "Grid Optimizer" in page.text
    assert "Walk-forward 70/30" in page.text
    assert "SAFETY &amp; MONITORING" in page.text
    assert "Вхід у Spot Grid Lab" in page.text
    assert "SOLUSDT" in page.text
    assert styles.status_code == 200
    assert "--green" in styles.text


def test_kline_endpoint_normalizes_binance_rows(monkeypatch):
    async def fake_klines(symbol, interval, limit):
        assert (symbol, interval, limit) == ("BTCUSDT", "1h", 2)
        return [
            [1000, "100", "110", "90", "105", "12", 1999],
            [2000, "105", "115", "95", "108", "14", 2999],
        ]

    monkeypatch.setattr(main.market, "klines", fake_klines)
    main.market_health.update({"last_success_at": "", "last_error": "old error"})
    response = TestClient(main.app).get("/api/market/BTCUSDT/klines?interval=1h&limit=2")

    assert response.status_code == 200
    data = response.json()
    assert data["symbol"] == "BTCUSDT"
    assert data["candles"][0]["close"] == 105.0
    assert data["candles"][1]["high"] == 115.0
    status = TestClient(main.app).get("/api/monitoring/status").json()
    assert status["market_data"]["status"] == "ONLINE"
    assert status["market_data"]["last_success_at"]


def test_monitoring_and_csv_exports_are_available():
    client = TestClient(main.app)
    status = client.get("/api/monitoring/status")
    trades = client.get("/api/export/trades.csv")
    events = client.get("/api/export/events.csv")

    assert status.status_code == 200
    assert "market_data" in status.json()
    assert trades.status_code == 200
    assert trades.text.startswith("id,timestamp,symbol")
    assert events.status_code == 200
    assert events.text.startswith("strategy,bot_id,symbol")
