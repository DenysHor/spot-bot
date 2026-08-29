from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Dict, List, Protocol


class PortfolioStore(Protocol):
    def load_portfolio(self, portfolio: "PaperPortfolio") -> bool: ...
    def save_portfolio(self, portfolio: "PaperPortfolio") -> None: ...


@dataclass
class Trade:
    id: int
    timestamp: str
    symbol: str
    side: str
    price: float
    quantity: float
    quote_amount: float
    fee_quote: float
    realized_pnl: float = 0.0


@dataclass
class Position:
    asset: str
    quantity: float = 0.0
    avg_price: float = 0.0

    def market_value(self, price: float) -> float:
        return self.quantity * price


class PaperPortfolio:
    def __init__(self, starting_quote: float = 10_000.0, quote_asset: str = "USDT", store: PortfolioStore | None = None) -> None:
        self.store = store
        self.quote_asset = quote_asset
        self.starting_quote = starting_quote
        self.quote_balance = starting_quote
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.realized_pnl = 0.0
        self.fees_paid = 0.0
        self._next_trade_id = 1
        if self.store is not None and not self.store.load_portfolio(self):
            self.store.save_portfolio(self)

    def reset(self) -> None:
        self.quote_balance = self.starting_quote
        self.positions = {}
        self.trades = []
        self.realized_pnl = 0.0
        self.fees_paid = 0.0
        self._next_trade_id = 1
        self.persist()

    def position(self, asset: str) -> Position:
        if asset not in self.positions:
            self.positions[asset] = Position(asset=asset)
        return self.positions[asset]

    def record_trade(self, trade: Trade) -> None:
        self.trades.append(trade)
        self._next_trade_id += 1

    def persist(self) -> None:
        if self.store is not None:
            self.store.save_portfolio(self)

    def next_trade_id(self) -> int:
        return self._next_trade_id

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    def snapshot(self, prices: Dict[str, float] | None = None) -> dict:
        prices = prices or {}
        positions = []
        assets_value = 0.0
        unrealized = 0.0

        for asset, position in self.positions.items():
            if position.quantity <= 0:
                continue
            price = prices.get(asset, position.avg_price)
            value = position.market_value(price)
            pnl = (price - position.avg_price) * position.quantity
            assets_value += value
            unrealized += pnl
            positions.append({
                "asset": asset,
                "quantity": position.quantity,
                "avg_price": position.avg_price,
                "price": price,
                "market_value": value,
                "unrealized_pnl": pnl,
            })

        total_equity = self.quote_balance + assets_value
        return {
            "quote_asset": self.quote_asset,
            "starting_balance": self.starting_quote,
            "quote_balance": self.quote_balance,
            "assets_value": assets_value,
            "total_equity": total_equity,
            "realized_pnl": self.realized_pnl,
            "unrealized_pnl": unrealized,
            "fees_paid": self.fees_paid,
            "return_pct": ((total_equity - self.starting_quote) / self.starting_quote * 100) if self.starting_quote else 0.0,
            "positions": positions,
            "trade_count": len(self.trades),
        }

    def trade_history(self) -> list[dict]:
        return [asdict(t) for t in reversed(self.trades)]
