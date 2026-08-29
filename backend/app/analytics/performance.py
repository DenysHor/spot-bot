from datetime import datetime, timedelta, timezone


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def grid_performance(trades, bots, days: int, symbol: str, now: datetime | None = None) -> dict:
    current = now or datetime.now(timezone.utc)
    start = current - timedelta(days=days)
    symbol = symbol.upper()
    filtered_trades = [t for t in trades if t.symbol == symbol and _parse(t.timestamp) >= start]
    cycle_events = [
        event
        for bot in bots.values() if bot.symbol == symbol
        for event in bot.events
        if event.event == "SELL_FILLED" and _parse(event.timestamp) >= start
    ]
    realized = sum(event.realized_cycle_pnl for event in cycle_events)
    fees = sum(trade.fee_quote for trade in filtered_trades)
    volume = sum(trade.quote_amount for trade in filtered_trades)
    profitable = sum(event.realized_cycle_pnl > 0 for event in cycle_events)

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
    cumulative = 0.0
    series = []
    for item in daily.values():
        cumulative += item["pnl"]
        series.append({**item, "cumulative_pnl": cumulative})

    cycles = len(cycle_events)
    return {
        "symbol": symbol,
        "days": days,
        "metrics": {
            "realized_pnl": realized,
            "fees": fees,
            "volume": volume,
            "trades": len(filtered_trades),
            "cycles": cycles,
            "profitable_cycles": profitable,
            "win_rate_pct": profitable / cycles * 100 if cycles else 0.0,
            "avg_cycle_pnl": realized / cycles if cycles else 0.0,
        },
        "series": series,
    }
