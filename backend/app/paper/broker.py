from app.paper.portfolio import PaperPortfolio, Trade


class PaperBroker:
    def __init__(self, portfolio: PaperPortfolio, fee_rate: float = 0.001) -> None:
        self.portfolio = portfolio
        self.fee_rate = fee_rate

    def market_buy(self, symbol: str, base_asset: str, price: float, quote_amount: float) -> Trade:
        if price <= 0 or quote_amount <= 0:
            raise ValueError("price and quote_amount must be positive")

        fee = quote_amount * self.fee_rate
        total_cost = quote_amount + fee
        if total_cost > self.portfolio.quote_balance:
            raise ValueError("insufficient paper quote balance")

        quantity = quote_amount / price
        position = self.portfolio.position(base_asset)
        previous_cost = position.quantity * position.avg_price
        new_quantity = position.quantity + quantity
        position.avg_price = (previous_cost + quote_amount) / new_quantity
        position.quantity = new_quantity

        self.portfolio.quote_balance -= total_cost
        self.portfolio.fees_paid += fee

        trade = Trade(
            id=self.portfolio.next_trade_id(),
            timestamp=self.portfolio.now_iso(),
            symbol=symbol,
            side="BUY",
            price=price,
            quantity=quantity,
            quote_amount=quote_amount,
            fee_quote=fee,
        )
        self.portfolio.record_trade(trade)
        return trade

    def market_sell(self, symbol: str, base_asset: str, price: float, quantity: float) -> Trade:
        if price <= 0 or quantity <= 0:
            raise ValueError("price and quantity must be positive")

        position = self.portfolio.position(base_asset)
        if quantity > position.quantity + 1e-12:
            raise ValueError("insufficient paper asset balance")

        gross_quote = quantity * price
        fee = gross_quote * self.fee_rate
        net_quote = gross_quote - fee
        realized_pnl = (price - position.avg_price) * quantity - fee

        position.quantity -= quantity
        if position.quantity <= 1e-12:
            position.quantity = 0.0
            position.avg_price = 0.0

        self.portfolio.quote_balance += net_quote
        self.portfolio.fees_paid += fee
        self.portfolio.realized_pnl += realized_pnl

        trade = Trade(
            id=self.portfolio.next_trade_id(),
            timestamp=self.portfolio.now_iso(),
            symbol=symbol,
            side="SELL",
            price=price,
            quantity=quantity,
            quote_amount=gross_quote,
            fee_quote=fee,
            realized_pnl=realized_pnl,
        )
        self.portfolio.record_trade(trade)
        return trade
