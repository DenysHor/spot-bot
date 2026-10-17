from dataclasses import dataclass


@dataclass(frozen=True)
class RiskLimits:
    max_portfolio_allocation_pct: float
    max_position_pct: float
    reserve_quote_pct: float
    max_daily_loss_pct: float = 2.0
    max_open_positions: int = 8


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
        daily_realized_pnl: float = 0.0,
        active_positions: int = 0,
        opens_new_position: bool = False,
    ) -> RiskDecision:
        if requested_quote <= 0 or total_equity <= 0:
            return RiskDecision(False, "Invalid order/equity value", 0.0)

        daily_loss_limit = total_equity * self.limits.max_daily_loss_pct / 100
        if daily_realized_pnl <= -daily_loss_limit:
            return RiskDecision(False, "Daily loss limit blocks new BUY orders", 0.0)
        if opens_new_position and active_positions >= self.limits.max_open_positions:
            return RiskDecision(False, "Maximum number of open positions reached", 0.0)

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

    def status(
        self,
        *,
        total_equity: float,
        free_quote: float,
        allocated_quote: float,
        daily_realized_pnl: float,
        active_positions: int,
    ) -> dict:
        allocation_cap = total_equity * self.limits.max_portfolio_allocation_pct / 100
        reserve_required = total_equity * self.limits.reserve_quote_pct / 100
        daily_loss_limit = total_equity * self.limits.max_daily_loss_pct / 100
        checks = [
            {
                "code": "ALLOCATION",
                "passed": allocated_quote <= allocation_cap + 1e-9,
                "current": allocated_quote,
                "limit": allocation_cap,
                "message": "Сумарне залучення в межах ліміту",
            },
            {
                "code": "RESERVE",
                "passed": free_quote >= reserve_required - 1e-9,
                "current": free_quote,
                "limit": reserve_required,
                "message": "Резерв вільних коштів збережено",
            },
            {
                "code": "DAILY_LOSS",
                "passed": daily_realized_pnl > -daily_loss_limit,
                "current": daily_realized_pnl,
                "limit": -daily_loss_limit,
                "message": "Добовий ліміт збитку не вичерпано",
            },
            {
                "code": "POSITIONS",
                "passed": active_positions <= self.limits.max_open_positions,
                "current": active_positions,
                "limit": self.limits.max_open_positions,
                "message": "Кількість відкритих позицій у межах ліміту",
            },
        ]
        failed = [check for check in checks if not check["passed"]]
        return {
            "status": "BLOCKED" if failed else "SAFE",
            "new_buys_allowed": not failed,
            "checks": checks,
            "reasons": [check["message"] for check in failed],
            "limits": {
                "max_portfolio_allocation_pct": self.limits.max_portfolio_allocation_pct,
                "max_position_pct": self.limits.max_position_pct,
                "reserve_quote_pct": self.limits.reserve_quote_pct,
                "max_daily_loss_pct": self.limits.max_daily_loss_pct,
                "max_open_positions": self.limits.max_open_positions,
            },
        }
