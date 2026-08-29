import asyncio

from app.backtest.grid import Candle, GridBacktester


def kline(open_time, open_price, high, low, close):
    return [open_time, str(open_price), str(high), str(low), str(close), "0", open_time + 59_999]


def test_grid_backtest_completes_cycle_and_charges_fees():
    candles = [
        kline(0, 100, 100, 100, 100),
        kline(60_000, 100, 101, 89, 90),
        kline(120_000, 90, 101, 90, 100),
    ]

    result = asyncio.run(GridBacktester(fee_rate=0.001).run(
        symbol="BTCUSDT",
        base_asset="BTC",
        raw_candles=candles,
        budget_quote=1_000,
        step_pct=10,
        levels_each_side=2,
    ))

    performance = result["performance"]
    assert performance["completed_cycles"] >= 1
    assert performance["trade_count"] >= 2
    assert performance["fees_paid"] > 0
    assert performance["net_profit"] > 0
    assert len(result["equity_curve"]) == 2
    assert result["configuration"]["fee_rate_pct"] == 0.1


def test_candle_path_and_validation_are_deterministic():
    bullish = Candle.from_binance(kline(0, 100, 110, 90, 105))
    bearish = Candle.from_binance(kline(0, 105, 110, 90, 100))
    assert bullish.price_path() == [100, 90, 110, 105]
    assert bearish.price_path() == [105, 110, 90, 100]

    try:
        Candle.from_binance(kline(0, 100, 99, 90, 105))
        assert False, "Expected invalid OHLC range"
    except ValueError as exc:
        assert "OHLC" in str(exc)


def test_optimizer_ranks_identical_candle_combinations():
    candles = [
        kline(0, 100, 100, 100, 100),
        kline(60_000, 100, 101, 98, 99),
        kline(120_000, 99, 102, 99, 101),
        kline(180_000, 101, 101, 98, 99),
        kline(240_000, 99, 102, 99, 101),
    ]
    result = asyncio.run(GridBacktester().optimize(
        symbol="SOLUSDT", base_asset="SOL", raw_candles=candles,
        budget_quote=1_000, step_pcts=[1, 3], levels_options=[4, 6],
    ))

    assert result["tested_combinations"] == 4
    assert result["results"][0]["rank"] == 1
    assert result["best"] == result["results"][0]
    assert result["best"]["step_pct"] == 1
    assert result["results"][-1]["completed_cycles"] == 0
