from statistics import mean


STABLE_BASES = {"USDC", "FDUSD", "TUSD", "USDP", "DAI", "AEUR", "EURI", "USD1", "BFUSD"}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")


def is_scannable_base(base_asset: str) -> bool:
    base = base_asset.upper()
    return base not in STABLE_BASES and not base.endswith(LEVERAGED_SUFFIXES)


def ema(values: list[float], period: int) -> float:
    if not values:
        return 0.0
    factor = 2 / (period + 1)
    result = values[0]
    for value in values[1:]:
        result = value * factor + result * (1 - factor)
    return result


def rsi(values: list[float], period: int = 14) -> float:
    changes = [current - previous for previous, current in zip(values, values[1:])][-period:]
    if not changes:
        return 50.0
    gains = mean(max(change, 0.0) for change in changes)
    losses = mean(max(-change, 0.0) for change in changes)
    if losses == 0:
        return 100.0
    return 100 - 100 / (1 + gains / losses)


def analyze_symbol(ticker: dict, rows: list[list], base_asset: str) -> dict:
    closes = [float(row[4]) for row in rows]
    if len(closes) < 50:
        raise ValueError("At least 50 candles are required")
    highs = [float(row[2]) for row in rows]
    lows = [float(row[3]) for row in rows]
    quote_volumes = [float(row[7]) for row in rows]
    last = closes[-1]
    ema20 = ema(closes[-50:], 20)
    ema50 = ema(closes[-50:], 50)
    rsi14 = rsi(closes, 14)
    true_ranges = []
    for index in range(1, len(rows)):
        true_ranges.append(max(
            highs[index] - lows[index],
            abs(highs[index] - closes[index - 1]),
            abs(lows[index] - closes[index - 1]),
        ))
    atr_pct = mean(true_ranges[-14:]) / last * 100 if last and true_ranges else 0.0
    recent_volume = mean(quote_volumes[-6:])
    previous_volume = mean(quote_volumes[-24:-6]) if len(quote_volumes) >= 24 else recent_volume
    volume_ratio = recent_volume / previous_volume if previous_volume else 1.0
    change_24h = float(ticker.get("priceChangePercent", 0.0))
    change_7d = (last / closes[-43] - 1) * 100 if closes[-43] else 0.0
    quote_volume = float(ticker.get("quoteVolume", 0.0))
    score = 20
    reasons = []
    if last > ema20:
        score += 15
        reasons.append("ціна вище EMA20")
    else:
        score -= 10
        reasons.append("ціна нижче EMA20")
    if ema20 > ema50:
        score += 15
        reasons.append("EMA20 вище EMA50")
    else:
        score -= 10
    if 0 < change_24h <= 8:
        score += 10
        reasons.append("помірне зростання за 24г")
    elif change_24h > 12:
        score -= 25
        reasons.append("різкий рух за 24г")
    elif change_24h < 0:
        score -= 10
    if 0 < change_7d <= 20:
        score += 10
    elif change_7d > 30:
        score -= 15
        reasons.append("ціна сильно відірвалася за 7 днів")
    if volume_ratio >= 1.2:
        score += 15
        reasons.append("обсяг прискорюється")
    if 0.5 <= atr_pct <= 4:
        score += 10
        reasons.append("волатильність придатна для Grid")
    elif atr_pct > 7:
        score -= 15
        reasons.append("надмірна волатильність")
    if rsi14 >= 75:
        score -= 20
        reasons.append("RSI показує перегрів")
    elif rsi14 < 40:
        score -= 10
    score = max(0, min(100, round(score)))
    overextended = rsi14 >= 75 or change_24h > 12 or change_7d > 30
    downtrend = last < ema20 and ema20 < ema50
    if overextended:
        signal, recommendation = "OVERHEATED", "Не наздоганяти ціну; чекати охолодження"
    elif downtrend:
        signal, recommendation = "SKIP", "Спадний тренд; для нового Grid поки пропустити"
    elif score >= 60:
        signal, recommendation = "PAPER_CANDIDATE", "Кандидат лише для PAPER-перевірки та preflight"
    elif score >= 40:
        signal, recommendation = "WATCH", "Спостерігати; підтвердження поки недостатньо"
    else:
        signal, recommendation = "SKIP", "Слабке співвідношення тренду, обсягу та ризику"
    return {
        "symbol": ticker["symbol"], "base_asset": base_asset, "price": last,
        "quote_volume_24h": quote_volume, "change_24h_pct": change_24h,
        "change_7d_pct": round(change_7d, 3), "ema20": ema20, "ema50": ema50,
        "rsi14": round(rsi14, 1), "atr_pct": round(atr_pct, 3),
        "volume_ratio": round(volume_ratio, 2), "score": score,
        "signal": signal, "recommendation": recommendation,
        "reasons": reasons[:4], "recommended_step_pct": round(min(5.0, max(0.35, atr_pct * 0.6)), 2),
        "recommended_levels_each_side": 6 if atr_pct >= 2 else 8,
    }
