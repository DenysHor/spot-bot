from typing import Any

import httpx


class BinancePublicClient:
    """Public Binance Spot market-data client. No API key is required."""

    BASE_URL = "https://api.binance.com"

    def __init__(self, timeout: float = 10.0) -> None:
        self.timeout = timeout

    async def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        async with httpx.AsyncClient(base_url=self.BASE_URL, timeout=self.timeout) as client:
            response = await client.get(path, params=params)
            response.raise_for_status()
            return response.json()

    async def price(self, symbol: str) -> dict[str, Any]:
        return await self._get("/api/v3/ticker/price", {"symbol": symbol.upper()})

    async def klines(
        self,
        symbol: str,
        interval: str = "15m",
        limit: int = 200,
    ) -> list[list[Any]]:
        limit = max(1, min(limit, 1000))
        return await self._get(
            "/api/v3/klines",
            {"symbol": symbol.upper(), "interval": interval, "limit": limit},
        )

    async def ticker_24h(self, symbol: str | None = None) -> Any:
        params = {"symbol": symbol.upper()} if symbol else None
        return await self._get("/api/v3/ticker/24hr", params)

    async def exchange_info(self) -> dict[str, Any]:
        return await self._get("/api/v3/exchangeInfo")
