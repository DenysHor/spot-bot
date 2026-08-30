from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app import main
from app.analytics.scanner import analyze_symbol, is_scannable_base


def candles(start: float = 100.0) -> list[list]:
    rows = []
    for index in range(60):
        close = start + index * 0.3
        rows.append([index, str(close - 0.1), str(close + 0.8), str(close - 0.8), str(close), "1", index + 1, str(1_000_000 + index * 20_000)])
    return rows


def test_scanner_excludes_stables_and_leveraged_tokens():
    assert is_scannable_base("BTC") is True
    assert is_scannable_base("USDC") is False
    assert is_scannable_base("ETHUP") is False


def test_symbol_analysis_produces_explainable_paper_observation():
    result = analyze_symbol({
        "symbol": "LINKUSDT", "priceChangePercent": "3.0", "quoteVolume": "25000000",
    }, candles(), "LINK")

    assert result["symbol"] == "LINKUSDT"
    assert 0 <= result["score"] <= 100
    assert result["signal"] in {"PAPER_CANDIDATE", "WATCH", "OVERHEATED", "SKIP"}
    assert result["recommended_step_pct"] >= 0.35
    assert result["reasons"]


def test_market_scanner_uses_top_active_usdt_pairs_and_cache(monkeypatch):
    async def fake_exchange_info():
        return {"symbols": [
            {"symbol": "LINKUSDT", "baseAsset": "LINK", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
            {"symbol": "USDCUSDT", "baseAsset": "USDC", "quoteAsset": "USDT", "status": "TRADING", "isSpotTradingAllowed": True},
        ]}

    calls = {"tickers": 0}

    async def fake_tickers(symbol=None):
        calls["tickers"] += 1
        assert symbol is None
        return [
            {"symbol": "LINKUSDT", "priceChangePercent": "3", "quoteVolume": "25000000"},
            {"symbol": "USDCUSDT", "priceChangePercent": "0", "quoteVolume": "50000000"},
        ]

    async def fake_klines(symbol, interval, limit):
        assert (symbol, interval, limit) == ("LINKUSDT", "4h", 60)
        return candles()

    monkeypatch.setattr(main.market, "exchange_info", fake_exchange_info)
    monkeypatch.setattr(main.market, "ticker_24h", fake_tickers)
    monkeypatch.setattr(main.market, "klines", fake_klines)
    main.symbol_catalog_cache.update({"expires_at": datetime.min.replace(tzinfo=timezone.utc), "symbols": []})
    main.market_scanner_cache.update({"expires_at": datetime.min.replace(tzinfo=timezone.utc), "result": None})
    client = TestClient(main.app)

    first = client.get("/api/market/scanner")
    second = client.get("/api/market/scanner")

    assert first.status_code == 200
    assert first.json()["analyzed_count"] == 1
    assert first.json()["items"][0]["symbol"] == "LINKUSDT"
    assert second.status_code == 200
    assert calls["tickers"] == 1
