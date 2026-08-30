from datetime import datetime, timedelta, timezone


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def grid_performance(trades, bots, days: int, symbol: str, now: datetime | None = None) -> dict:
    current = now or datetime.now(timezone.utc)
    start = current - timedelta(days=days)
    symbol = symbol.upper()
    matching_bots = [bot for bot in bots.values() if bot.symbol == symbol]
    evaluation_bots = [
        bot for bot in matching_bots
        if getattr(bot, "status", "RUNNING") in {"RUNNING", "PAUSED"}
    ] or matching_bots
    filtered_trades = [t for t in trades if t.symbol == symbol and _parse(t.timestamp) >= start]
    cycle_events = [
        event
        for bot in matching_bots
        for event in bot.events
        if event.event == "SELL_FILLED" and _parse(event.timestamp) >= start
    ]
    recenter_events = sorted([
        event
        for bot in matching_bots
        for event in bot.events
        if event.event == "GRID_RECENTERED" and _parse(event.timestamp) >= start
    ], key=lambda item: _parse(item.timestamp))
    first_recenter_at = _parse(recenter_events[0].timestamp) if recenter_events else None
    post_recenter_cycles = [
        event for event in cycle_events
        if first_recenter_at is not None and _parse(event.timestamp) >= first_recenter_at
    ]
    realized = sum(event.realized_cycle_pnl for event in cycle_events)
    fees = sum(trade.fee_quote for trade in filtered_trades)
    volume = sum(trade.quote_amount for trade in filtered_trades)
    profitable = sum(event.realized_cycle_pnl > 0 for event in cycle_events)
    positive_pnl = sum(max(0.0, event.realized_cycle_pnl) for event in cycle_events)
    negative_pnl = abs(sum(min(0.0, event.realized_cycle_pnl) for event in cycle_events))
    allocated_budget = max((bot.budget_quote for bot in evaluation_bots), default=0.0)
    created_times = [
        _parse(bot.created_at) for bot in evaluation_bots
        if getattr(bot, "created_at", "")
    ]
    activity_times = [_parse(t.timestamp) for t in filtered_trades]
    activity_times.extend(
        _parse(event.timestamp) for bot in matching_bots for event in bot.events
        if _parse(event.timestamp) >= start
    )
    experiment_started_at = min(created_times or activity_times, default=None)
    active_since = max(start, experiment_started_at) if experiment_started_at else None

    daily = {}
    for offset in range(days):
        day = (start.date() + timedelta(days=offset + 1)).isoformat()
        daily[day] = {"date": day, "pnl": 0.0, "fees": 0.0, "cycles": 0}
    for event in cycle_events:
        day = _parse(event.timestamp).date().isoformat()
        if day in daily:
            daily[day]["pnl"] += event.realized_cycle_pnl
            daily[day]["cycles"] += 1
    for trade in filtered_trades:
        day = _parse(trade.timestamp).date().isoformat()
        if day in daily:
            daily[day]["fees"] += trade.fee_quote
    running_cycle_pnl = 0.0
    cycle_peak = 0.0
    max_drawdown = 0.0
    for event in sorted(cycle_events, key=lambda item: _parse(item.timestamp)):
        running_cycle_pnl += event.realized_cycle_pnl
        cycle_peak = max(cycle_peak, running_cycle_pnl)
        max_drawdown = max(max_drawdown, cycle_peak - running_cycle_pnl)
    cumulative = 0.0
    series = []
    for item in daily.values():
        cumulative += item["pnl"]
        series.append({**item, "cumulative_pnl": cumulative})

    cycles = len(cycle_events)
    return {
        "symbol": symbol,
        "days": days,
        "active_since": active_since.isoformat() if active_since else None,
        "experiment_started_at": experiment_started_at.isoformat() if experiment_started_at else None,
        "elapsed_hours": (current - experiment_started_at).total_seconds() / 3600 if experiment_started_at else 0.0,
        "metrics": {
            "realized_pnl": realized,
            "fees": fees,
            "volume": volume,
            "trades": len(filtered_trades),
            "cycles": cycles,
            "profitable_cycles": profitable,
            "win_rate_pct": profitable / cycles * 100 if cycles else 0.0,
            "avg_cycle_pnl": realized / cycles if cycles else 0.0,
            "allocated_budget": allocated_budget,
            "grid_return_pct": realized / allocated_budget * 100 if allocated_budget else 0.0,
            "realized_max_drawdown": max_drawdown,
            "realized_max_drawdown_pct": max_drawdown / allocated_budget * 100 if allocated_budget else 0.0,
            "profit_factor": positive_pnl / negative_pnl if negative_pnl else None,
            "trailing_recenters": len(recenter_events),
            "post_recenter_cycles": len(post_recenter_cycles),
            "post_recenter_pnl": sum(event.realized_cycle_pnl for event in post_recenter_cycles),
        },
        "series": series,
    }
