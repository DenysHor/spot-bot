from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app import main


def test_dashboard_and_static_assets_are_served():
    client = TestClient(main.app)

    page = client.get("/")
    script = client.get("/static/app.js")
    styles = client.get("/static/styles.css")
    advisor_styles = client.get("/static/advisor.css")

    assert page.status_code == 200
    assert "Spot Grid Lab" in page.text
    assert "/static/styles.css?v=0.54.0" in page.text
    assert "/static/advisor.css?v=0.54.0" in page.text
    assert "/static/app.js?v=0.54.0" in page.text
    assert "TESTNET-бот уже працює" in script.text
    assert "Ціна Testnet" in script.text
    assert "Найближча купівля" in script.text
    assert "Аналітика портфеля" in page.text
    assert 'id="advisor-bots"' in page.text
    assert "/api/analytics/advisor" in script.text
    assert "Сигнальні PAPER-боти" in page.text
    assert 'id="signal-preflight"' in page.text
    assert 'id="signal-symbol-status"' in page.text
    assert page.headers["cache-control"] == "no-cache, max-age=0, must-revalidate"
    assert script.status_code == 200
    assert advisor_styles.status_code == 200
    assert script.headers["cache-control"] == "no-cache, max-age=0, must-revalidate"
    assert "/api/backtest/grid" in script.text
    assert "/api/dca/bots/start" in script.text
    assert "/api/backtest/grid/optimize" in script.text
    assert "/api/backtest/grid/walk-forward" in script.text
    assert "/api/grid/bots/${id}/pause" in script.text
    assert "Зупинити лише нові покупки" in script.text
    assert "/api/grid/bots/${id}/${route}" in script.text
    assert "Продати ВСІ відкриті позиції" in script.text
    assert "Перейти до Сигнальної стратегії" in script.text
    assert "Чинний Grid не буде змінено автоматично" in script.text
    assert "setupStrategyLayout" in script.text
    assert ".strategy-launch-layout" in advisor_styles.text
    assert "Binance Spot Testnet" in page.text
    assert "Запустити TESTNET-сітку" in page.text
    assert "/api/testnet/grid-bot/start" in script.text
    assert "/api/testnet/verify" in script.text
    assert "Smart DCA" in page.text
    assert "Оптимізація сітки" in page.text
    assert "Перевірка 70/30" in page.text
    assert "БЕЗПЕКА ТА МОНІТОРИНГ" in page.text
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
    assert "ГІБРИД 10% — 10% тренд / 90% сітка" in page.text
    assert "applyRecommendedProfile" in script.text
    assert "Результат тренду" in script.text
    assert "B — купівля" in page.text
    assert "Зелена/червона свічка — рух ціни" in page.text
    assert "`${shortSide} ${priceNum(o.trigger_price" in script.text
    assert "labelX=w-right+(w<600?10:42)" in script.text
    assert 'id="step" type="number" value="1.5" min="0.1" max="25" step="0.01"' in page.text
    assert "Загальна дохідність" in script.text
    assert "Комісії сітки" in script.text
    assert "Комісії тренду" in script.text
    assert "buy_paused" in script.text
    assert "Продажі продовжують працювати" in script.text
    assert "Grid-аналітика" in page.text
    assert "/api/analytics/performance" in script.text
    assert "ЗБІР ДАНИХ" in script.text
    assert "Готовність стратегії" in script.text
    assert "Залишок бюджету бота" in script.text
    assert "Вільно в портфелі" in script.text
    assert "statusName" in script.text
    assert "profileName" in script.text
    assert 'data-days="1"' in page.text
    assert "/api/grid/bots/${id}/buy-control" in script.text
    assert "Середня ціна купівлі" in script.text
    assert "Незакритий результат" in script.text
    assert "Поточний етап" in script.text
    assert "chart-alert" in page.text
    assert "Межі коридору" in page.text
    assert "Зарезервовано на купівлі" in script.text
    assert "localizeEventFeed" in script.text
    assert "priceNum" in script.text
    assert "right=w<600?72:155" in script.text
    assert "mobile-bottom-nav" in page.text
    assert "mobile-bot-select" in script.text
    assert "Детальніше" in script.text
    assert "chart-history-nav" in page.text
    assert "shiftChart" in script.text
    assert "limit=300" in script.text
    assert "Зафіксований результат" in page.text
    assert "Ринкові дані" in page.text
    assert "Внутрішня помилка сервера" in script.text
    assert ".comparison-table td:nth-child(8)::before" in styles.text
    assert ".health-reason" in styles.text
    assert ".bot-detail .bot-stats{grid-template-columns:repeat(4" in styles.text
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


def test_portfolio_advisor_is_available_without_active_bots(monkeypatch):
    monkeypatch.setattr(main.grid_engine, "bots", {})

    response = TestClient(main.app).get("/api/analytics/advisor")

    assert response.status_code == 200
    data = response.json()
    assert data["summary"]["active_bots"] == 0
    assert data["summary"]["capital_utilization_pct"] == 0
    assert data["bots"] == []
    assert "відкритих позицій" in data["caveat"]


def test_testnet_readiness_never_exposes_credentials():
    response = TestClient(main.app).get("/api/testnet/readiness")

    assert response.status_code == 200
    data = response.json()
    assert data["testnet_execution_enabled"] is False
    assert data["live_execution_enabled"] is False
    assert "api_key" not in data
    assert "api_secret" not in data
    assert "missing_variables" in data


def test_testnet_verify_explains_missing_railway_variables(monkeypatch):
    monkeypatch.setattr(main.testnet, "api_key", "")
    monkeypatch.setattr(main.testnet, "api_secret", "")

    response = TestClient(main.app).post("/api/testnet/verify")

    assert response.status_code == 400
    assert "BINANCE_API_KEY" in response.json()["detail"]
    assert "BINANCE_API_SECRET" in response.json()["detail"]


def test_testnet_verify_uses_validation_only(monkeypatch):
    async def fake_verify():
        return {
            "connected": True, "can_trade": True, "account_type": "SPOT",
            "permissions": ["SPOT"], "non_zero_balances": [{"asset": "USDT", "free": "1000", "locked": "0"}],
            "order_test_passed": True, "commission_preview_available": True,
            "execution_enabled": False,
        }

    monkeypatch.setattr(main.testnet, "api_key", "test-key")
    monkeypatch.setattr(main.testnet, "api_secret", "test-secret")
    monkeypatch.setattr(main.testnet, "verify", fake_verify)
    main.testnet_health.update({"verified": False, "last_checked_at": "", "last_error": ""})
    response = TestClient(main.app).post("/api/testnet/verify")

    assert response.status_code == 200
    data = response.json()
    assert data["order_test_passed"] is True
    assert data["execution_enabled"] is False
    assert data["verified"] is True


def test_testnet_order_routes_are_locked_in_paper_mode():
    client = TestClient(main.app)

    state = client.get("/api/testnet/grid-bot")
    start = client.post("/api/testnet/grid-bot/start", json={
        "symbol": "BTCUSDT", "budget_quote": 100, "step_pct": 1.5, "levels": 4,
    })

    assert state.status_code == 200
    assert state.json()["enabled"] is False
    assert state.json()["live_execution_enabled"] is False
    assert start.status_code == 409
    assert "TRADING_MODE=TESTNET" in start.json()["detail"]
