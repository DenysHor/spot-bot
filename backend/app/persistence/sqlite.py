import sqlite3
from contextlib import closing
from datetime import datetime, timezone
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

    def create_backup(self, retain: int = 5) -> str | None:
        if self.path == ":memory:" or not Path(self.path).exists() or retain <= 0:
            return None
        source_path = Path(self.path).resolve()
        backup_dir = source_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        target = backup_dir / f"{source_path.stem}-{timestamp}.db"
        with closing(sqlite3.connect(source_path)) as source, closing(sqlite3.connect(target)) as destination:
            source.backup(destination)
        backups = sorted(backup_dir.glob(f"{source_path.stem}-*.db"), reverse=True)
        for old_backup in backups[retain:]:
            old_backup.unlink()
        return str(target)

    def _initialize(self) -> None:
        with self.connect() as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version INTEGER PRIMARY KEY
                );
                INSERT INTO schema_version(version)
                SELECT 1 WHERE NOT EXISTS (SELECT 1 FROM schema_version);

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
                    completed_cycles INTEGER NOT NULL,
                    last_success_at TEXT NOT NULL DEFAULT '',
                    consecutive_errors INTEGER NOT NULL DEFAULT 0,
                    paused_reason TEXT NOT NULL DEFAULT ''
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

                CREATE TABLE IF NOT EXISTS dca_bots (
                    id TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    base_asset TEXT NOT NULL,
                    status TEXT NOT NULL,
                    budget_quote REAL NOT NULL,
                    order_quote REAL NOT NULL,
                    interval_seconds INTEGER NOT NULL,
                    dip_trigger_pct REAL NOT NULL,
                    created_at TEXT NOT NULL,
                    next_buy_at TEXT NOT NULL,
                    last_price REAL NOT NULL,
                    last_buy_price REAL NOT NULL,
                    spent_quote REAL NOT NULL,
                    buy_count INTEGER NOT NULL,
                    last_success_at TEXT NOT NULL DEFAULT '',
                    consecutive_errors INTEGER NOT NULL DEFAULT 0,
                    paused_reason TEXT NOT NULL DEFAULT ''
                );
                CREATE TABLE IF NOT EXISTS dca_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bot_id TEXT NOT NULL REFERENCES dca_bots(id) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    event TEXT NOT NULL,
                    price REAL NOT NULL,
                    quote_amount REAL NOT NULL,
                    quantity REAL NOT NULL,
                    message TEXT NOT NULL,
                    UNIQUE(bot_id, sequence)
                );
                CREATE TABLE IF NOT EXISTS notification_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS notification_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT NOT NULL,
                    error TEXT NOT NULL DEFAULT ''
                );
                DELETE FROM schema_version
                WHERE version < (SELECT MAX(version) FROM schema_version);
                UPDATE schema_version SET version = 4 WHERE version < 4;
            """)
            self._ensure_column(db, "grid_bots", "last_success_at", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(db, "grid_bots", "consecutive_errors", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(db, "grid_bots", "paused_reason", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(db, "grid_bots", "trailing_up_enabled", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(db, "grid_bots", "trailing_trigger_steps", "REAL NOT NULL DEFAULT 2.0")
            self._ensure_column(db, "grid_bots", "recenter_count", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(db, "grid_bots", "last_recenter_at", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(db, "dca_bots", "last_success_at", "TEXT NOT NULL DEFAULT ''")
            self._ensure_column(db, "dca_bots", "consecutive_errors", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column(db, "dca_bots", "paused_reason", "TEXT NOT NULL DEFAULT ''")

    @staticmethod
    def _ensure_column(db: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {row[1] for row in db.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

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
                values = dict(row)
                values["trailing_up_enabled"] = bool(values["trailing_up_enabled"])
                bot = GridBotState(**values)
                bot.open_orders = [GridOrder(**{k: v for k, v in dict(order).items() if k != "bot_id"})
                                   for order in db.execute("SELECT * FROM grid_orders WHERE bot_id = ?", (bot.id,))]
                bot.events = [GridEvent(**{k: v for k, v in dict(event).items() if k not in {"id", "bot_id", "sequence"}})
                              for event in db.execute("SELECT * FROM grid_events WHERE bot_id = ? ORDER BY sequence", (bot.id,))]
                bots[bot.id] = bot
        return bots

    def save_bot(self, bot: "GridBotState") -> None:
        with self.connect() as db:
            db.execute("""INSERT OR REPLACE INTO grid_bots
                (id, symbol, base_asset, status, reference_price, budget_quote, step_pct,
                 levels_each_side, quote_per_level, created_at, last_price, spent_quote,
                 realized_pnl, completed_cycles, last_success_at, consecutive_errors,
                 paused_reason, trailing_up_enabled, trailing_trigger_steps, recenter_count,
                 last_recenter_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                bot.id, bot.symbol, bot.base_asset, bot.status, bot.reference_price,
                bot.budget_quote, bot.step_pct, bot.levels_each_side, bot.quote_per_level,
                bot.created_at, bot.last_price, bot.spent_quote, bot.realized_pnl, bot.completed_cycles,
                bot.last_success_at, bot.consecutive_errors, bot.paused_reason,
                bot.trailing_up_enabled, bot.trailing_trigger_steps, bot.recenter_count,
                bot.last_recenter_at,
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

    def load_dca_bots(self):
        from app.dca.execution import DcaBotState, DcaEvent

        bots = {}
        with self.connect() as db:
            for row in db.execute("SELECT * FROM dca_bots ORDER BY created_at"):
                bot = DcaBotState(**dict(row))
                bot.events = [DcaEvent(**{k: v for k, v in dict(event).items()
                                        if k not in {"id", "bot_id", "sequence"}})
                              for event in db.execute(
                                  "SELECT * FROM dca_events WHERE bot_id = ? ORDER BY sequence", (bot.id,)
                              )]
                bots[bot.id] = bot
        return bots

    def save_dca_bot(self, bot) -> None:
        with self.connect() as db:
            db.execute("""INSERT OR REPLACE INTO dca_bots VALUES
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                bot.id, bot.symbol, bot.base_asset, bot.status, bot.budget_quote,
                bot.order_quote, bot.interval_seconds, bot.dip_trigger_pct, bot.created_at,
                bot.next_buy_at, bot.last_price, bot.last_buy_price, bot.spent_quote, bot.buy_count,
                bot.last_success_at, bot.consecutive_errors, bot.paused_reason,
            ))
            db.execute("DELETE FROM dca_events WHERE bot_id = ?", (bot.id,))
            db.executemany("""INSERT INTO dca_events
                (bot_id, sequence, timestamp, event, price, quote_amount, quantity, message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", [
                (bot.id, sequence, event.timestamp, event.event, event.price,
                 event.quote_amount, event.quantity, event.message)
                for sequence, event in enumerate(bot.events)
            ])

    def clear_dca_bots(self) -> None:
        with self.connect() as db:
            db.execute("DELETE FROM dca_bots")

    def get_notification_state(self, key: str, default: str = "") -> str:
        with self.connect() as db:
            row = db.execute("SELECT value FROM notification_state WHERE key = ?", (key,)).fetchone()
            return row["value"] if row else default

    def set_notification_state(self, key: str, value: str) -> None:
        with self.connect() as db:
            db.execute("INSERT OR REPLACE INTO notification_state(key, value) VALUES (?, ?)", (key, value))

    def record_notification(self, kind: str, status: str, message: str, error: str = "") -> None:
        with self.connect() as db:
            db.execute("""INSERT INTO notification_log(timestamp, kind, status, message, error)
                VALUES (?, ?, ?, ?, ?)""", (
                datetime.now(timezone.utc).isoformat(), kind, status, message, error,
            ))

    def list_notifications(self, limit: int = 50) -> list[dict]:
        with self.connect() as db:
            return [dict(row) for row in db.execute(
                "SELECT * FROM notification_log ORDER BY id DESC LIMIT ?", (limit,)
            )]
