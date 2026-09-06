from app.paper.broker import PaperBroker
from app.paper.portfolio import PaperPortfolio
from app.strategies.smart_grid import SmartGrid


def test_paper_buy_and_sell_with_fees():
    portfolio = PaperPortfolio(starting_quote=10_000.0)
    broker = PaperBroker(portfolio, fee_rate=0.001)

    buy = broker.market_buy("BTCUSDT", "BTC", price=50_000.0, quote_amount=1_000.0)
    assert round(buy.quantity, 8) == 0.02
    assert round(portfolio.quote_balance, 2) == 8999.00
    assert round(portfolio.position("BTC").quantity, 8) == 0.02

    sell = broker.market_sell("BTCUSDT", "BTC", price=55_000.0, quantity=0.02)
    assert round(sell.realized_pnl, 2) == 98.90
    assert portfolio.position("BTC").quantity == 0.0
    assert round(portfolio.quote_balance, 2) == 10097.90
    assert round(portfolio.fees_paid, 2) == 2.10
    snapshot = portfolio.snapshot()
    assert round(snapshot["gross_realized_pnl"], 2) == 98.90
    assert round(snapshot["buy_fees_paid"], 2) == 1.00
    assert round(snapshot["realized_pnl"], 2) == 97.90
    assert round(snapshot["accounting_delta"], 10) == 0


def test_portfolio_snapshot_reconciles_open_position_and_buy_fee():
    portfolio = PaperPortfolio(starting_quote=10_000.0)
    broker = PaperBroker(portfolio, fee_rate=0.001)

    broker.market_buy("BTCUSDT", "BTC", price=50_000.0, quote_amount=1_000.0)
    snapshot = portfolio.snapshot({"BTC": 49_000.0})

    assert round(snapshot["realized_pnl"], 2) == -1.00
    assert round(snapshot["unrealized_pnl"], 2) == -20.00
    assert round(snapshot["total_equity"] - snapshot["starting_balance"], 2) == -21.00
    assert round(snapshot["accounting_delta"], 10) == 0


def test_smart_grid_plan():
    plan = SmartGrid().build_plan(
        symbol="BTCUSDT",
        reference_price=50_000.0,
        budget_quote=1_000.0,
        step_pct=2.0,
        levels_each_side=4,
    )

    assert plan.lower_price == 46_000.0
    assert plan.upper_price == 54_000.0
    assert plan.quote_per_level == 250.0
    assert len(plan.levels) == 8
    assert [level.side for level in plan.levels].count("BUY") == 4
    assert [level.side for level in plan.levels].count("SELL") == 4
