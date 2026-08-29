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
    response = TestClient(main.app).get("/api/market/BTCUSDT/klines?interval=1h&limit=2")

    assert response.status_code == 200
    data = response.json()
    assert data["symbol"] == "BTCUSDT"
    assert data["candles"][0]["close"] == 105.0
    assert data["candles"][1]["high"] == 115.0
