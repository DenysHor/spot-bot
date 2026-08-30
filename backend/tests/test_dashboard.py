from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app import main


def test_dashboard_and_static_assets_are_served():
    client = TestClient(main.app)

    page = client.get("/")
    script = client.get("/static/app.js")
    styles = client.get("/static/styles.css")

    assert page.status_code == 200
    assert "Spot Grid Lab" in page.text
    assert "/static/styles.css?v=0.32.0" in page.text
    assert "/static/app.js?v=0.32.0" in page.text
    assert page.headers["cache-control"] == "no-cache, max-age=0, must-revalidate"
    assert script.status_code == 200
    assert script.headers["cache-control"] == "no-cache, max-age=0, must-revalidate"
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
    assert "monitor-telegram" in page.text
    assert "/api/notifications/test" in script.text
    assert "/api/backtest/grid/compare-trailing" in script.text
    assert "/api/backtest/grid/compare-profiles" in script.text
    assert "compare-profiles-button" in page.text
    assert "UPTREND_HYBRID_20" in script.text
    assert "UPTREND_HYBRID_10" in script.text
    assert "UPTREND_HYBRID_30" in script.text
    assert "AUTO-РЕКОМЕНДАЦІЯ" in script.text
    assert "HYBRID 10% — 10% тренд / 90% Grid" in page.text
    assert "applyRecommendedProfile" in script.text
    assert "Trend P&amp;L" in script.text
    assert "B — купівля" in page.text
    assert "Зелена/червона свічка — рух ціни" in page.text
    assert "`${shortSide} ${num(o.trigger_price" in script.text
    assert "labelX=w-6" in script.text
    assert 'id="step" type="number" value="1.5" min="0.1" max="25" step="0.01"' in page.text
    assert "Hybrid Total Return" in script.text
    assert "Grid-комісії" in script.text
    assert "Trend-комісії" in script.text
    assert "buy_paused" in script.text
    assert "SELL продовжують працювати" in script.text
    assert "Grid-аналітика" in page.text
    assert "/api/analytics/performance" in script.text
    assert "Strategy readiness" in script.text
    assert "/api/market/symbols/search" in script.text
    assert "/api/grid/preflight" in script.text
    assert "/api/analytics/portfolio-comparison" in script.text
    assert "/api/market/scanner" in script.text
    assert "/api/market/scanner/history" in script.text
    assert "grid-preflight" in page.text
    assert "bot-comparison" in page.text
    assert "market-scanner" in page.text
    assert "scanner-preset" in page.text
    assert "scanner-min-volume" in page.text
    assert "signal-history" in page.text
    assert "strategy-profile" in page.text
    assert "chart-tooltip" in page.text
    assert "state.chartMarkers.push" in script.text
    assert "showChartTrade" in script.text
    assert "dashboard-tabs" in page.text
    assert "symbol-options" in page.text
    assert "SOLUSDT" in page.text
    assert styles.status_code == 200
    assert styles.headers["cache-control"] == "no-cache, max-age=0, must-revalidate"
    assert "--green" in styles.text


def test_symbol_search_returns_only_active_spot_usdt_pairs(monkeypatch):
    async def fake_exchange_info():
        return {"symbols": [
            {"symbol": "LINKUSDT", "baseAsset": "LINK", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "LINKBUSD", "baseAsset": "LINK", "quoteAsset": "BUSD", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "OLDUSDT", "baseAsset": "OLD", "quoteAsset": "USDT", "status": "BREAK", "isSpotTradingAllowed": True},
            {"symbol": "FUTUSDT", "baseAsset": "FUT", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": False},
        ]}

    monkeypatch.setattr(main.market, "exchange_info", fake_exchange_info)
    main.symbol_catalog_cache.update({
        "expires_at": datetime.min.replace(tzinfo=timezone.utc), "symbols": [],
    })
    response = TestClient(main.app).get("/api/market/symbols/search?query=link&limit=10")

    assert response.status_code == 200
    assert response.json()["symbols"] == [
        {"symbol": "LINKUSDT", "base_asset": "LINK", "quote_asset": "USDT"}
    ]


def test_grid_preflight_reports_market_and_budget_guidance(monkeypatch):
    async def fake_exchange_info():
        return {"symbols": [{
            "symbol": "LINKUSDT", "baseAsset": "LINK", "quoteAsset": "USDT",
            "status": "TRADING", "isSpotTradingAllowed": True,
        }]}

    async def fake_ticker(symbol):
        assert symbol == "LINKUSDT"
        return {"symbol": "LINKUSDT", "lastPrice": "15", "priceChangePercent": "0.5", "quoteVolume": "25000000"}

    async def fake_klines(symbol, interval, limit):
        assert (symbol, interval, limit) == ("LINKUSDT", "4h", 60)
        return [
            [index, "15", "15.15", "14.85", "14.9" if index % 2 else "15.1", "1", index + 1, "1000000"]
            for index in range(60)
        ]

    monkeypatch.setattr(main.market, "exchange_info", fake_exchange_info)
    monkeypatch.setattr(main.market, "ticker_24h", fake_ticker)
    monkeypatch.setattr(main.market, "klines", fake_klines)
    monkeypatch.setattr(main.grid_engine, "bots", {})
    monkeypatch.setattr(main.dca_engine, "bots", {})
    main.symbol_catalog_cache.update({
        "expires_at": datetime.min.replace(tzinfo=timezone.utc), "symbols": [],
    })

    response = TestClient(main.app).post("/api/grid/preflight", json={
        "symbol": "LINKUSDT", "budget_quote": 500, "step_pct": 1.5,
        "levels_each_side": 8, "strategy_profile": "AUTO",
    })

    assert response.status_code == 200
    data = response.json()
    assert data["verdict"] == "SUITABLE"
    assert data["budget"]["allowed"] is True
    assert data["market"]["liquidity"] == "HIGH"
    assert data["strategy"]["regime"]["name"] == "RANGE"
    assert data["strategy"]["resolved_profile"] == "RANGE_GRID"
    assert data["strategy"]["launch_allowed"] is True


def test_grid_start_is_blocked_above_per_pair_budget_limit(monkeypatch):
    async def active_symbol(symbol):
        return symbol.upper()

    monkeypatch.setattr(main, "ensure_active_quote_symbol", active_symbol)
    monkeypatch.setattr(main.grid_engine, "bots", {})
    monkeypatch.setattr(main.dca_engine, "bots", {})
    response = TestClient(main.app).post("/api/grid/bots/start", json={
        "symbol": "LINKUSDT", "budget_quote": 1001, "step_pct": 1,
        "levels_each_side": 4,
    })

    assert response.status_code == 400
    assert "не може перевищувати" in response.json()["detail"]


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
