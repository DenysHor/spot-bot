from dataclasses import dataclass


@dataclass(frozen=True)
class RiskLimits:
    max_portfolio_allocation_pct: float
    max_position_pct: float
    reserve_quote_pct: float


@dataclass(frozen=True)
class RiskDecision:
    allowed: bool
    reason: str
    max_order_quote: float


class RiskManager:
    def __init__(self, limits: RiskLimits) -> None:
        self.limits = limits

    def check_buy(
        self,
        total_equity: float,
        free_quote: float,
        current_bot_allocation: float,
        current_position_value: float,
        requested_quote: float,
    ) -> RiskDecision:
        if requested_quote <= 0 or total_equity <= 0:
            return RiskDecision(False, "Invalid order/equity value", 0.0)

        reserve = total_equity * self.limits.reserve_quote_pct / 100
        spendable_quote = max(0.0, free_quote - reserve)

        portfolio_cap = total_equity * self.limits.max_portfolio_allocation_pct / 100
        portfolio_room = max(0.0, portfolio_cap - current_bot_allocation)

        position_cap = total_equity * self.limits.max_position_pct / 100
        position_room = max(0.0, position_cap - current_position_value)

        allowed_quote = min(spendable_quote, portfolio_room, position_room)
        if allowed_quote <= 0:
            return RiskDecision(False, "Risk limits block this BUY", 0.0)

        if requested_quote > allowed_quote:
            return RiskDecision(False, "Requested BUY exceeds risk limit", allowed_quote)

        return RiskDecision(True, "BUY is within configured risk limits", requested_quote)
