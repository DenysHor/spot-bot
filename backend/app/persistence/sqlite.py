import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.grid.execution import GridBotState
    from app.paper.portfolio import PaperPortfolio


class SQLiteStore:
    """Small, migration-friendly SQLite persistence layer for PAPER state."""

    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        return connection

    def _initialize(self) -> None:
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY
                );
                INSERT OR IGNORE INTO schema_version(version) VALUES (1);

                CREATE TABLE IF NOT EXISTS paper_portfolio (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    quote_asset TEXT NOT NULL,
                    starting_quote REAL NOT NULL,
                    quote_balance REAL NOT NULL,
                    realized_pnl REAL NOT NULL,
                    fees_paid REAL NOT NULL,
                    next_trade_id INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_positions (
                    asset TEXT PRIMARY KEY,
                    quantity REAL NOT NULL,
                    avg_price REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS paper_trades (
                    id INTEGER PRIMARY KEY,
                    timestamp TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    price REAL NOT NULL,
                    quantity REAL NOT NULL,
                    quote_amount REAL NOT NULL,
                    fee_quote REAL NOT NULL,
                    realized_pnl REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS grid_bots (
                    id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    base_asset TEXT NOT NULL,
                    status TEXT NOT NULL,
                    reference_price REAL NOT NULL,
                    budget_quote REAL NOT NULL,
                    step_pct REAL NOT NULL,
                    levels_each_side INTEGER NOT NULL,
                    quote_per_level REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    last_price REAL NOT NULL,
                    spent_quote REAL NOT NULL,
                    realized_pnl REAL NOT NULL,
                    completed_cycles INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS grid_orders (
                    id TEXT PRIMARY KEY,
                    bot_id TEXT NOT NULL REFERENCES grid_bots(id) ON DELETE CASCADE,
                    side TEXT NOT NULL,
                    trigger_price REAL NOT NULL,
                    quote_amount REAL NOT NULL,
                    quantity REAL NOT NULL,
                    source_buy_price REAL NOT NULL,
                    source_buy_cost REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS grid_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id TEXT NOT NULL REFERENCES grid_bots(id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    event TEXT NOT NULL,
                    price REAL NOT NULL,
                    side TEXT,
                    quantity REAL NOT NULL,
                    quote_amount REAL NOT NULL,
                    realized_cycle_pnl REAL NOT NULL,
                    message TEXT NOT NULL,
                    UNIQUE(bot_id, sequence)
                );
            """)

    def load_portfolio(self, portfolio: "PaperPortfolio") -> bool:
        from app.paper.portfolio import Position, Trade

        with self.connect() as db:
            state = db.execute("SELECT * FROM paper_portfolio WHERE id = 1").fetchone()
            if state is None:
                return False
            portfolio.quote_asset = state["quote_asset"]
            portfolio.starting_quote = state["starting_quote"]
            portfolio.quote_balance = state["quote_balance"]
            portfolio.realized_pnl = state["realized_pnl"]
            portfolio.fees_paid = state["fees_paid"]
            portfolio._next_trade_id = state["next_trade_id"]
            portfolio.positions = {
                row["asset"]: Position(row["asset"], row["quantity"], row["avg_price"])
                for row in db.execute("SELECT * FROM paper_positions")
            }
            portfolio.trades = [Trade(**dict(row)) for row in db.execute("SELECT * FROM paper_trades ORDER BY id")]
            return True

    def save_portfolio(self, portfolio: "PaperPortfolio") -> None:
        with self.connect() as db:
            db.execute("""INSERT OR REPLACE INTO paper_portfolio
                VALUES (1, ?, ?, ?, ?, ?, ?)""", (
                portfolio.quote_asset, portfolio.starting_quote, portfolio.quote_balance,
                portfolio.realized_pnl, portfolio.fees_paid, portfolio._next_trade_id,
            ))
            db.execute("DELETE FROM paper_positions")
            db.executemany("INSERT INTO paper_positions VALUES (?, ?, ?)", [
                (position.asset, position.quantity, position.avg_price)
                for position in portfolio.positions.values()
            ])
            db.execute("DELETE FROM paper_trades")
            db.executemany("INSERT INTO paper_trades VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", [
                (trade.id, trade.timestamp, trade.symbol, trade.side, trade.price, trade.quantity,
                 trade.quote_amount, trade.fee_quote, trade.realized_pnl)
                for trade in portfolio.trades
            ])

    def load_bots(self) -> dict[str, "GridBotState"]:
        from app.grid.execution import GridBotState, GridEvent, GridOrder

        bots: dict[str, GridBotState] = {}
        with self.connect() as db:
            for row in db.execute("SELECT * FROM grid_bots ORDER BY created_at"):
                bot = GridBotState(**dict(row))
                bot.open_orders = [GridOrder(**{k: v for k, v in dict(order).items() if k != "bot_id"})
                                   for order in db.execute("SELECT * FROM grid_orders WHERE bot_id = ?", (bot.id,))]
                bot.events = [GridEvent(**{k: v for k, v in dict(event).items() if k not in {"id", "bot_id", "sequence"}})
                              for event in db.execute("SELECT * FROM grid_events WHERE bot_id = ? ORDER BY sequence", (bot.id,))]
                bots[bot.id] = bot
        return bots

    def save_bot(self, bot: "GridBotState") -> None:
        with self.connect() as db:
            db.execute("""INSERT OR REPLACE INTO grid_bots VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                bot.id, bot.symbol, bot.base_asset, bot.status, bot.reference_price,
                bot.budget_quote, bot.step_pct, bot.levels_each_side, bot.quote_per_level,
                bot.created_at, bot.last_price, bot.spent_quote, bot.realized_pnl, bot.completed_cycles,
            ))
            db.execute("DELETE FROM grid_orders WHERE bot_id = ?", (bot.id,))
            db.executemany("INSERT INTO grid_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?)", [
                (order.id, bot.id, order.side, order.trigger_price, order.quote_amount,
                 order.quantity, order.source_buy_price, order.source_buy_cost)
                for order in bot.open_orders
            ])
            db.execute("DELETE FROM grid_events WHERE bot_id = ?", (bot.id,))
            db.executemany("""INSERT INTO grid_events
                (bot_id, sequence, timestamp, event, price, side, quantity, quote_amount, realized_cycle_pnl, message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", [
                (bot.id, sequence, event.timestamp, event.event, event.price, event.side,
                 event.quantity, event.quote_amount, event.realized_cycle_pnl, event.message)
                for sequence, event in enumerate(bot.events)
            ])

    def reset(self) -> None:
        with self.connect() as db:
            db.execute("DELETE FROM grid_bots")
            db.execute("DELETE FROM paper_trades")
            db.execute("DELETE FROM paper_positions")
            db.execute("DELETE FROM paper_portfolio")

    def clear_bots(self) -> None:
        with self.connect() as db:
            db.execute("DELETE FROM grid_bots")
