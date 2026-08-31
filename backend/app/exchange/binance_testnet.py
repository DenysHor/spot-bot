import hashlib
import hmac
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

    @property
    def configured(self) -> bool:
        return bool(self.api_key and self.api_secret)

    async def _public(self, path: str) -> dict:
        async with httpx.AsyncClient(base_url=self.BASE_URL, timeout=self.timeout) as client:
            response = await client.get(path)
        return self._decode(response)

    async def _signed(self, method: str, path: str, params: dict) -> dict:
        if not self.configured:
            raise BinanceTestnetError("Testnet API key and secret are not configured")
        server_time = int((await self._public("/api/v3/time"))["serverTime"])
        signed_params = {**params, "recvWindow": 5000, "timestamp": server_time}
        payload = urlencode(signed_params, encoding="utf-8")
        signature = hmac.new(
            self.api_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256,
        ).hexdigest()
        body = f"{payload}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key, "Content-Type": "application/x-www-form-urlencoded"}
        async with httpx.AsyncClient(base_url=self.BASE_URL, timeout=self.timeout) as client:
            if method == "GET":
                response = await client.get(f"{path}?{body}", headers=headers)
            else:
                response = await client.request(method, path, headers=headers, content=body)
        return self._decode(response)

    @staticmethod
    def _decode(response: httpx.Response) -> dict:
        try:
            data = response.json()
        except ValueError as exc:
            raise BinanceTestnetError(f"Testnet returned HTTP {response.status_code}") from exc
        if response.is_error:
            raise BinanceTestnetError(f"Binance {data.get('code', response.status_code)}: {data.get('msg', 'request failed')}")
        return data

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
