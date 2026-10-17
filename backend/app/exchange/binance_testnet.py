import hashlib
import hmac
import asyncio
import time
from decimal import Decimal, ROUND_DOWN
from typing import Any
from urllib.parse import urlencode

import httpx


class BinanceTestnetError(RuntimeError):
    pass


class BinanceTestnetClient:
    """Signed Spot Testnet checks. This client intentionally exposes no real order method."""

    BASE_URL = "https://testnet.binance.vision"

    def __init__(self, api_key: str, api_secret: str, timeout: float = 10.0) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.timeout = timeout
        self._server_time_offset_ms = 0
        self._server_time_cached_at = 0.0

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    async def _public(self, path: str) -> Any:
        return await self._request_with_retry("GET", path)

    async def _request_with_retry(self, method: str, path: str, **kwargs) -> Any:
        last_error = None
        attempts = 2 if method == "GET" else 1
        for attempt in range(attempts):
            try:
                async with httpx.AsyncClient(base_url=self.BASE_URL, timeout=self.timeout) as client:
                    response = await client.request(method, path, **kwargs)
                if response.status_code not in {429, 500, 502, 503, 504} or attempt + 1 == attempts:
                    return self._decode(response)
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                last_error = exc
                if attempt + 1 == attempts:
                    raise BinanceTestnetError(f"Binance Testnet network error: {exc}") from exc
            await asyncio.sleep(0.35 * (attempt + 1))
        raise BinanceTestnetError(f"Binance Testnet request failed: {last_error}")

    async def _server_time(self) -> int:
        now = time.monotonic()
        if now - self._server_time_cached_at > 30:
            data = await self._request_with_retry("GET", "/api/v3/time")
            self._server_time_offset_ms = int(data["serverTime"]) - int(time.time() * 1000)
            self._server_time_cached_at = now
        return int(time.time() * 1000) + self._server_time_offset_ms

    async def _signed(self, method: str, path: str, params: dict) -> Any:
        if not self.configured:
            raise BinanceTestnetError("Testnet API key and secret are not configured")
        server_time = await self._server_time()
        signed_params = {**params, "recvWindow": 5000, "timestamp": server_time}
        payload = urlencode(signed_params, encoding="utf-8")
        signature = hmac.new(
            self.api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256,
        ).hexdigest()
        body = f"{payload}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key, "Content-Type": "application/x-www-form-urlencoded"}
        if method == "GET":
            return await self._request_with_retry("GET", f"{path}?{body}", headers=headers)
        if method == "DELETE":
            return await self._request_with_retry(method, f"{path}?{body}", headers=headers)
        return await self._request_with_retry(method, path, headers=headers, content=body)

    @staticmethod
    def _decode(response: httpx.Response) -> Any:
        try:
            data = response.json()
        except ValueError as exc:
            raise BinanceTestnetError(f"Testnet returned HTTP {response.status_code}") from exc
        if response.is_error:
            raise BinanceTestnetError(f"Binance {data.get('code', response.status_code)}: {data.get('msg', 'request failed')}")
        return data

    async def account(self) -> dict:
        return await self._signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})

    async def symbol_rules(self, symbol: str) -> dict:
        data = await self._public(f"/api/v3/exchangeInfo?symbol={symbol.upper()}")
        item = data["symbols"][0]
        filters = {row["filterType"]: row for row in item["filters"]}
        notional = filters.get("NOTIONAL", filters.get("MIN_NOTIONAL", {}))
        return {
            "symbol": item["symbol"], "base_asset": item["baseAsset"], "quote_asset": item["quoteAsset"],
            "status": item["status"], "tick_size": filters["PRICE_FILTER"]["tickSize"],
            "step_size": filters["LOT_SIZE"]["stepSize"], "min_qty": filters["LOT_SIZE"]["minQty"],
            "min_notional": notional.get("minNotional", "0"),
        }

    async def price(self, symbol: str) -> float:
        data = await self._public(f"/api/v3/ticker/price?symbol={symbol.upper()}")
        return float(data["price"])

    async def ticker_24h(self, symbol: str) -> dict:
        return await self._public(f"/api/v3/ticker/24hr?symbol={symbol.upper()}")

    async def klines(self, symbol: str, interval: str = "1h", limit: int = 120) -> list[list]:
        safe_limit = max(2, min(limit, 500))
        return await self._public(
            f"/api/v3/klines?symbol={symbol.upper()}&interval={interval}&limit={safe_limit}"
        )

    @staticmethod
    def floor_to_step(value: float | str, step: float | str) -> str:
        number, quantum = Decimal(str(value)), Decimal(str(step))
        return format((number / quantum).to_integral_value(rounding=ROUND_DOWN) * quantum, "f")

    async def create_limit_order(self, symbol: str, side: str, quantity: str, price: str) -> dict:
        return await self._signed("POST", "/api/v3/order", {
            "symbol": symbol.upper(), "side": side.upper(), "type": "LIMIT", "timeInForce": "GTC",
            "quantity": quantity, "price": price, "newOrderRespType": "RESULT",
        })

    async def create_market_sell(self, symbol: str, quantity: str) -> dict:
        return await self._signed("POST", "/api/v3/order", {
            "symbol": symbol.upper(), "side": "SELL", "type": "MARKET", "quantity": quantity,
            "newOrderRespType": "FULL",
        })

    async def order(self, symbol: str, order_id: int) -> dict:
        return await self._signed("GET", "/api/v3/order", {"symbol": symbol.upper(), "orderId": order_id})

    async def open_orders(self, symbol: str) -> list[dict]:
        return await self._signed("GET", "/api/v3/openOrders", {"symbol": symbol.upper()})

    async def cancel_order(self, symbol: str, order_id: int) -> dict:
        return await self._signed("DELETE", "/api/v3/order", {"symbol": symbol.upper(), "orderId": order_id})

    async def cancel_open_orders(self, symbol: str) -> list[dict]:
        return await self._signed("DELETE", "/api/v3/openOrders", {"symbol": symbol.upper()})

    async def trades(self, symbol: str, order_id: int) -> list[dict]:
        return await self._signed("GET", "/api/v3/myTrades", {
            "symbol": symbol.upper(), "orderId": order_id, "limit": 100,
        })

    async def verify(self, symbol: str = "BTCUSDT", quote_order_qty: float = 10.0) -> dict:
        account = await self._signed("GET", "/api/v3/account", {"omitZeroBalances": "true"})
        order_test = await self._signed("POST", "/api/v3/order/test", {
            "symbol": symbol, "side": "BUY", "type": "MARKET",
            "quoteOrderQty": f"{quote_order_qty:.2f}", "computeCommissionRates": "true",
        })
        balances = [
            {"asset": row["asset"], "free": row["free"], "locked": row["locked"]}
            for row in account.get("balances", []) if float(row.get("free", 0)) or float(row.get("locked", 0))
        ]
        return {
            "connected": True, "can_trade": bool(account.get("canTrade")),
            "account_type": account.get("accountType", "SPOT"),
            "permissions": account.get("permissions", []),
            "non_zero_balances": balances[:20],
            "order_test_passed": True,
            "commission_preview_available": bool(order_test),
            "execution_enabled": False,
        }
