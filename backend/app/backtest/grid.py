from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any

from app.grid.execution import GridExecutionEngine
from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio


@dataclass(frozen=True)
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    close_time: int

    @classmethod
    def from_binance(cls, row: list[Any]) -> "Candle":
        if len(row) < 7:
            raise ValueError("Invalid Binance kline row")
        candle = cls(
            open_time=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            close_time=int(row[6]),
        )
        if min(candle.open, candle.high, candle.low, candle.close) <= 0:
            raise ValueError("Candle prices must be positive")
        if candle.low > min(candle.open, candle.close) or candle.high < max(candle.open, candle.close):
            raise ValueError("Invalid candle OHLC range")
        return candle

    def price_path(self) -> list[float]:
        # A reproducible approximation where exact intrabar tick order is unavailable.
        if self.close >= self.open:
            return [self.open, self.low, self.high, self.close]
        return [self.open, self.high, self.low, self.close]


class GridBacktester:
    def __init__(self, fee_rate: float = 0.001) -> None:
        if fee_rate < 0:
            raise ValueError("fee_rate cannot be negative")
        self.fee_rate = fee_rate

    @staticmethod
    def _iso(timestamp_ms: int) -> str:
        return datetime.fromtimestamp(timestamp_ms / 1000, timezone.utc).isoformat()

    async def run(
        self,
        symbol: str,
        base_asset: str,
        raw_candles: list[list[Any]],
        budget_quote: float,
        step_pct: float,
        levels_each_side: int,
    ) -> dict:
        candles = [Candle.from_binance(row) for row in raw_candles]
        if len(candles) < 2:
            raise ValueError("At least two candles are required")
        if any(candles[index].open_time <= candles[index - 1].open_time for index in range(1, len(candles))):
            raise ValueError("Candles must be in chronological order")

        portfolio = PaperPortfolio(starting_quote=budget_quote)
        broker = PaperBroker(portfolio, fee_rate=self.fee_rate)

        async def unused_price_provider(_: str) -> float:
            return candles[-1].close

        engine = GridExecutionEngine(broker, unused_price_provider, poll_seconds=60)
        bot = engine.start_bot(
            symbol=symbol,
            base_asset=base_asset,
            reference_price=candles[0].close,
            budget_quote=budget_quote,
            step_pct=step_pct,
            levels_each_side=levels_each_side,
        )

        peak_equity = budget_quote
        max_drawdown_pct = 0.0
        equity_curve: list[dict] = []
        for candle in candles[1:]:
            for price in candle.price_path():
                await engine.tick_bot(bot.id, price=price)
                equity = portfolio.snapshot({base_asset: price})["total_equity"]
                peak_equity = max(peak_equity, equity)
                if peak_equity > 0:
                    max_drawdown_pct = max(max_drawdown_pct, (peak_equity - equity) / peak_equity * 100)
            close_equity = portfolio.snapshot({base_asset: candle.close})["total_equity"]
            equity_curve.append({"timestamp": self._iso(candle.close_time), "equity": close_equity})

        final_price = candles[-1].close
        snapshot = portfolio.snapshot({base_asset: final_price})
        net_profit = snapshot["total_equity"] - budget_quote
        first_price = candles[0].close
        buy_hold_return_pct = (final_price - first_price) / first_price * 100
        return {
            "symbol": symbol.upper(),
            "period": {
                "start": self._iso(candles[0].open_time),
                "end": self._iso(candles[-1].close_time),
                "candles": len(candles),
            },
            "configuration": {
                "budget_quote": budget_quote,
                "step_pct": step_pct,
                "levels_each_side": levels_each_side,
                "fee_rate_pct": self.fee_rate * 100,
                "intrabar_path": "O-L-H-C for bullish / O-H-L-C for bearish",
            },
            "performance": {
                "starting_equity": budget_quote,
                "ending_equity": snapshot["total_equity"],
                "net_profit": net_profit,
                "return_pct": net_profit / budget_quote * 100,
                "realized_pnl": snapshot["realized_pnl"],
                "unrealized_pnl": snapshot["unrealized_pnl"],
                "fees_paid": snapshot["fees_paid"],
                "max_drawdown_pct": max_drawdown_pct,
                "buy_hold_return_pct": buy_hold_return_pct,
                "completed_cycles": bot.completed_cycles,
                "trade_count": len(portfolio.trades),
            },
            "final_portfolio": snapshot,
            "trades": portfolio.trade_history(),
            "grid_events": [asdict(event) for event in bot.events],
            "equity_curve": equity_curve,
        }
