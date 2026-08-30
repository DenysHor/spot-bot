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

    @staticmethod
    async def _tick_segment(engine, bot, start: float, target: float, when: datetime) -> None:
        """Walk through crossed levels so OHLC extremes do not become favorable fill prices."""
        current = start
        for _ in range(500):
            if target > current:
                candidates = [
                    order.trigger_price for order in bot.open_orders
                    if order.side == "SELL" and current < order.trigger_price <= target
                ]
                if bot.trailing_up_enabled and any(o.side == "BUY" for o in bot.open_orders):
                    step = bot.step_pct / 100.0
                    recenter_trigger = bot.reference_price * (1 + step * bot.trailing_trigger_steps)
                    if current < recenter_trigger <= target:
                        candidates.append(recenter_trigger)
                next_price = min(candidates) if candidates else target
            elif target < current:
                candidates = [
                    order.trigger_price for order in bot.open_orders
                    if order.side == "BUY" and target <= order.trigger_price < current
                ]
                next_price = max(candidates) if candidates else target
            else:
                return
            await engine.tick_bot(bot.id, price=next_price, now=when)
            if next_price == target:
                return
            current = next_price
        raise RuntimeError("Backtest segment exceeded the level-crossing safety limit")

    async def run(
        self,
        symbol: str,
        base_asset: str,
        raw_candles: list[list[Any]],
        budget_quote: float,
        step_pct: float,
        levels_each_side: int,
        trailing_up_enabled: bool = False,
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
            trailing_up_enabled=trailing_up_enabled,
        )

        peak_equity = budget_quote
        max_drawdown_pct = 0.0
        equity_curve: list[dict] = []
        for candle in candles[1:]:
            candle_time = datetime.fromtimestamp(candle.close_time / 1000, timezone.utc)
            path = candle.price_path()
            await engine.tick_bot(bot.id, price=path[0], now=candle_time)
            for start_price, price in zip(path, path[1:]):
                await self._tick_segment(engine, bot, start_price, price, candle_time)
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
                "intrabar_path": ("O-L-H-C for bullish / O-H-L-C for bearish; "
                                  "fills occur at crossed grid levels, not candle extremes"),
                "trailing_up_enabled": trailing_up_enabled,
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
                "recenter_count": bot.recenter_count,
            },
            "final_portfolio": snapshot,
            "trades": portfolio.trade_history(),
            "grid_events": [asdict(event) for event in bot.events],
            "equity_curve": equity_curve,
        }

    async def compare_trailing(
        self,
        symbol: str,
        base_asset: str,
        raw_candles: list[list[Any]],
        budget_quote: float,
        step_pct: float,
        levels_each_side: int,
    ) -> dict:
        fixed = await self.run(
            symbol=symbol, base_asset=base_asset, raw_candles=raw_candles,
            budget_quote=budget_quote, step_pct=step_pct,
            levels_each_side=levels_each_side, trailing_up_enabled=False,
        )
        trailing = await self.run(
            symbol=symbol, base_asset=base_asset, raw_candles=raw_candles,
            budget_quote=budget_quote, step_pct=step_pct,
            levels_each_side=levels_each_side, trailing_up_enabled=True,
        )
        fixed_performance = fixed["performance"]
        trailing_performance = trailing["performance"]
        return_delta = trailing_performance["return_pct"] - fixed_performance["return_pct"]
        drawdown_delta = (
            trailing_performance["max_drawdown_pct"] - fixed_performance["max_drawdown_pct"]
        )
        return {
            "symbol": symbol.upper(),
            "period": fixed["period"],
            "configuration": fixed["configuration"],
            "fixed": fixed_performance,
            "trailing": trailing_performance,
            "difference": {
                "return_pct_points": return_delta,
                "max_drawdown_pct_points": drawdown_delta,
                "cycles": trailing_performance["completed_cycles"] - fixed_performance["completed_cycles"],
                "fees_paid": trailing_performance["fees_paid"] - fixed_performance["fees_paid"],
            },
            "historical_winner": "TRAILING_UP" if return_delta > 0 else "FIXED_GRID" if return_delta < 0 else "TIE",
            "warning": "Historical simulation is not a forecast and does not change the running PAPER bot.",
        }

    async def optimize(
        self,
        symbol: str,
        base_asset: str,
        raw_candles: list[list[Any]],
        budget_quote: float,
        step_pcts: list[float],
        levels_options: list[int],
    ) -> dict:
        if not step_pcts or not levels_options:
            raise ValueError("Optimizer parameter lists cannot be empty")
        if len(step_pcts) * len(levels_options) > 30:
            raise ValueError("Optimizer supports at most 30 parameter combinations")
        results = []
        for step_pct in step_pcts:
            for levels in levels_options:
                report = await self.run(
                    symbol=symbol, base_asset=base_asset, raw_candles=raw_candles,
                    budget_quote=budget_quote, step_pct=step_pct, levels_each_side=levels,
                )
                performance = report["performance"]
                # Prefer return with low drawdown, but penalize results based on fewer than 3 cycles.
                cycle_confidence = min(1.0, performance["completed_cycles"] / 3)
                score = (performance["return_pct"] / (1 + performance["max_drawdown_pct"])) * cycle_confidence
                results.append({
                    "rank": 0,
                    "step_pct": step_pct,
                    "levels_each_side": levels,
                    "score": score,
                    **performance,
                })
        results.sort(key=lambda item: (item["score"], item["return_pct"], item["completed_cycles"]), reverse=True)
        for rank, result in enumerate(results, start=1):
            result["rank"] = rank
        return {
            "symbol": symbol.upper(),
            "tested_combinations": len(results),
            "ranking_method": "return / (1 + max_drawdown_pct), with confidence penalty below 3 cycles",
            "best": results[0],
            "results": results,
        }

    async def walk_forward(
        self,
        symbol: str,
        base_asset: str,
        raw_candles: list[list[Any]],
        budget_quote: float,
        step_pcts: list[float],
        levels_options: list[int],
        training_pct: float = 70.0,
    ) -> dict:
        if len(raw_candles) < 10:
            raise ValueError("Walk-forward validation requires at least 10 candles")
        if training_pct < 50 or training_pct > 90:
            raise ValueError("training_pct must be between 50 and 90")
        split_index = int(len(raw_candles) * training_pct / 100)
        training_candles = raw_candles[:split_index]
        # Keep the split candle as the validation reference price; no trades occur on it.
        validation_candles = raw_candles[split_index - 1:]
        optimization = await self.optimize(
            symbol=symbol, base_asset=base_asset, raw_candles=training_candles,
            budget_quote=budget_quote, step_pcts=step_pcts, levels_options=levels_options,
        )
        best = optimization["best"]
        validation = await self.run(
            symbol=symbol, base_asset=base_asset, raw_candles=validation_candles,
            budget_quote=budget_quote, step_pct=best["step_pct"],
            levels_each_side=best["levels_each_side"],
        )
        performance = validation["performance"]
        passed = performance["return_pct"] > 0 and performance["completed_cycles"] >= 1
        return {
            "symbol": symbol.upper(),
            "split": {
                "training_pct": training_pct,
                "training_candles": len(training_candles),
                "validation_candles": len(validation_candles),
            },
            "selected_parameters": {
                "step_pct": best["step_pct"],
                "levels_each_side": best["levels_each_side"],
            },
            "training_performance": {
                key: best[key] for key in (
                    "return_pct", "max_drawdown_pct", "fees_paid", "completed_cycles", "trade_count"
                )
            },
            "validation_performance": performance,
            "return_degradation_pct_points": best["return_pct"] - performance["return_pct"],
            "status": "PASSED" if passed else "FAILED",
            "pass_rule": "validation return > 0 and at least one completed cycle",
        }
