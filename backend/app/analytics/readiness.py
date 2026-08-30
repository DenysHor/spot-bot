MIN_DAYS = 7
MIN_CYCLES = 20


def strategy_readiness(performance: dict) -> dict:
    metrics = performance["metrics"]
    elapsed_days = performance.get("elapsed_hours", 0.0) / 24
    cycles = metrics["cycles"]
    data_progress = min(1.0, elapsed_days / MIN_DAYS) * 50 + min(1.0, cycles / MIN_CYCLES) * 50
    gross_profit = metrics["realized_pnl"] + metrics["fees"]
    fee_drag_pct = metrics["fees"] / gross_profit * 100 if gross_profit > 0 else 100.0
    profit_factor = metrics["profit_factor"]
    excess_return = performance.get("excess_return_pct")
    criteria = [
        {"key": "net_pnl", "label": "Net P&L", "value": metrics["realized_pnl"], "target": "> 0 USDT", "passed": metrics["realized_pnl"] > 0},
        {"key": "drawdown", "label": "Realized drawdown", "value": metrics["realized_max_drawdown_pct"], "target": "<= 5%", "passed": metrics["realized_max_drawdown_pct"] <= 5},
        {"key": "profit_factor", "label": "Profit factor", "value": profit_factor, "target": ">= 1.2", "passed": (profit_factor is None and metrics["profitable_cycles"] > 0) or (profit_factor is not None and profit_factor >= 1.2)},
        {"key": "win_rate", "label": "Win rate", "value": metrics["win_rate_pct"], "target": ">= 50%", "passed": metrics["win_rate_pct"] >= 50},
        {"key": "excess_return", "label": "Excess return", "value": excess_return, "target": ">= 0%", "passed": excess_return is not None and excess_return >= 0},
        {"key": "fee_drag", "label": "Fee drag", "value": fee_drag_pct, "target": "<= 40%", "passed": fee_drag_pct <= 40},
    ]
    enough_data = elapsed_days >= MIN_DAYS and cycles >= MIN_CYCLES
    passed_count = sum(item["passed"] for item in criteria)
    quality_score = passed_count / len(criteria) * 100
    if not enough_data:
        status = "COLLECTING_DATA"
        recommendation = f"Continue PAPER collection: {elapsed_days:.1f}/{MIN_DAYS} days and {cycles}/{MIN_CYCLES} cycles."
    elif passed_count == len(criteria):
        status = "PASSED"
        recommendation = "Keep the current PAPER parameters and continue validation; LIVE remains disabled."
    else:
        status = "FAILED"
        recommendation = "Do not enable LIVE. Review failed criteria and test changes as a separate PAPER experiment."
    return {
        "status": status,
        "enough_data": enough_data,
        "data_progress_pct": data_progress,
        "quality_score_pct": quality_score if enough_data else None,
        "elapsed_days": elapsed_days,
        "minimum_days": MIN_DAYS,
        "cycles": cycles,
        "minimum_cycles": MIN_CYCLES,
        "fee_drag_pct": fee_drag_pct,
        "criteria": criteria,
        "recommendation": recommendation,
    }
