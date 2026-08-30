import asyncio

import httpx

from app import main
from app.core.errors import describe_exception


def test_empty_exception_message_keeps_exception_type():
    detail = describe_exception(httpx.ReadTimeout(""))
    assert detail.startswith("ReadTimeout:")


def test_current_price_recovers_before_engine_error(monkeypatch):
    calls = 0

    async def flaky_price(symbol):
        nonlocal calls
        calls += 1
        if calls < 3:
            raise httpx.ReadTimeout("")
        return {"symbol": symbol, "price": "105.50"}

    async def no_sleep(delay):
        return None

    monkeypatch.setattr(main.market, "price", flaky_price)
    monkeypatch.setattr(main.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(main.settings, "market_retry_attempts", 3)

    price = asyncio.run(main.current_price("SOLUSDT"))

    assert price == 105.5
    assert calls == 3
    assert main.market_health["last_error"] == ""


def test_current_price_reports_type_after_retries_exhausted(monkeypatch):
    async def failing_price(symbol):
        raise httpx.ReadTimeout("")

    async def no_sleep(delay):
        return None

    monkeypatch.setattr(main.market, "price", failing_price)
    monkeypatch.setattr(main.asyncio, "sleep", no_sleep)
    monkeypatch.setattr(main.settings, "market_retry_attempts", 3)

    try:
        asyncio.run(main.current_price("SOLUSDT"))
        assert False, "Expected exhausted market retries"
    except RuntimeError as exc:
        assert "after 3 attempt(s)" in str(exc)
        assert "ReadTimeout:" in str(exc)
