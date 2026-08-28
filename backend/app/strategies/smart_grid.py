from dataclasses import dataclass


@dataclass
class GridLevel:
    index: int
    side: str
    price: float
    quote_amount: float


@dataclass
class SmartGridPlan:
    symbol: str
    reference_price: float
    lower_price: float
    upper_price: float
    step_pct: float
    levels_each_side: int
    quote_per_level: float
    levels: list[GridLevel]


class SmartGrid:
    """First deterministic grid planner for paper trading.

    It does not predict the market. It builds symmetric BUY/SELL levels around
    the current price using a configurable percentage step.
    """

    def build_plan(
        self,
        symbol: str,
        reference_price: float,
        budget_quote: float,
        step_pct: float = 1.5,
        levels_each_side: int = 4,
    ) -> SmartGridPlan:
        if reference_price <= 0:
            raise ValueError("reference_price must be positive")
        if budget_quote <= 0:
            raise ValueError("budget_quote must be positive")
        if step_pct <= 0:
            raise ValueError("step_pct must be positive")
        if levels_each_side < 1 or levels_each_side > 50:
            raise ValueError("levels_each_side must be between 1 and 50")

        quote_per_level = budget_quote / levels_each_side
        step = step_pct / 100.0
        levels: list[GridLevel] = []

        for i in range(1, levels_each_side + 1):
            levels.append(GridLevel(
                index=i,
                side="BUY",
                price=reference_price * (1 - step * i),
                quote_amount=quote_per_level,
            ))
            levels.append(GridLevel(
                index=i,
                side="SELL",
                price=reference_price * (1 + step * i),
                quote_amount=quote_per_level,
            ))

        levels.sort(key=lambda level: level.price)
        return SmartGridPlan(
            symbol=symbol.upper(),
            reference_price=reference_price,
            lower_price=reference_price * (1 - step * levels_each_side),
            upper_price=reference_price * (1 + step * levels_each_side),
            step_pct=step_pct,
            levels_each_side=levels_each_side,
            quote_per_level=quote_per_level,
            levels=levels,
        )
