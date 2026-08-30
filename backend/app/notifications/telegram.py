import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone

import httpx


IMPORTANT_EVENTS = {
    "BUY_FILLED", "SELL_FILLED", "BUY_BLOCKED", "SELL_BLOCKED",
    "BUDGET_COMPLETED", "ENGINE_ERROR", "AUTO_PAUSED",
    "GRID_RECENTERED", "RECENTER_LIMIT_REACHED",
    "BUY_SIDE_PAUSED", "BUY_SIDE_RESUMED",
    "PRICE_RANGE_EXITED", "PRICE_RANGE_REENTERED",
    "DRAIN_MODE_STARTED", "DRAIN_MODE_COMPLETED",
    "MANUAL_BUYS_DISABLED", "MANUAL_BUYS_ENABLED",
}

EVENT_LABELS = {
    "BUY_FILLED": "🟢 Купівля виконана", "SELL_FILLED": "🔴 Продаж виконано",
    "BUY_BLOCKED": "⚠️ Купівлю заблоковано", "SELL_BLOCKED": "⚠️ Продаж заблоковано",
    "BUDGET_COMPLETED": "✅ Бюджет використано", "ENGINE_ERROR": "❌ Помилка бота",
    "AUTO_PAUSED": "⏸ Бота автоматично призупинено", "GRID_RECENTERED": "↗️ Сітку пересунуто",
    "RECENTER_LIMIT_REACHED": "⚠️ Досягнуто ліміт зсувів",
    "BUY_SIDE_PAUSED": "⏸ Нові купівлі призупинено", "BUY_SIDE_RESUMED": "▶️ Нові купівлі відновлено",
    "PRICE_RANGE_EXITED": "⚠️ Ціна вийшла з коридору", "PRICE_RANGE_REENTERED": "✅ Ціна повернулася в коридор",
    "DRAIN_MODE_STARTED": "⏳ Почато м’яке завершення", "DRAIN_MODE_COMPLETED": "✅ М’яке завершення виконано",
    "MANUAL_BUYS_DISABLED": "⏸ Покупки вимкнено", "MANUAL_BUYS_ENABLED": "▶️ Покупки увімкнено",
}


def _price(value: float) -> str:
    decimals = 8 if abs(value) < 0.01 else 6 if abs(value) < 1 else 4 if abs(value) < 100 else 2
    return f"{value:.{decimals}f}".rstrip("0").rstrip(".")


def _event_message(message: str) -> str:
    translations = {"SELL remains active": "Продажі залишаються активними.", "liquidity restored": "Доступний бюджет відновлено."}
    if message in translations:
        return translations[message]
    if message.startswith("Paired SELL created at "):
        return f"Створено парний продаж за {_price(float(message.rsplit(' ', 1)[-1]))} USDT."
    if message.startswith("Replacement BUY created at "):
        return f"Створено наступну купівлю за {_price(float(message.rsplit(' ', 1)[-1]))} USDT."
    if message.startswith("Trailing Up shifted "):
        return "Сітку піднято слідом за ціною."
    return message


@dataclass
class NotificationStatus:
    enabled: bool
    last_success_at: str = ""
    last_error: str = ""


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, grid_engine, dca_engine,
                 portfolio=None, store=None, poll_seconds: float = 2.0,
                 daily_report_hour_utc: int = 20, sender=None) -> None:
        self.token = token
        self.chat_id = chat_id
        self.grid_engine = grid_engine
        self.dca_engine = dca_engine
        self.portfolio = portfolio
        self.store = store
        self.poll_seconds = max(1.0, poll_seconds)
        self.daily_report_hour_utc = min(23, max(0, daily_report_hour_utc))
        self.enabled = bool(token and chat_id)
        self.sender = sender or self._send_telegram
        self.weekly_report_provider = None
        self.health_alert_provider = None
        self.status = NotificationStatus(enabled=self.enabled)
        self._seen: dict[str, int] = {}
        self._task: asyncio.Task | None = None
        self._running = False

    def _sources(self):
        return (("GRID", self.grid_engine.bots), ("DCA", self.dca_engine.bots))

    def _state(self, key: str, default: str = "") -> str:
        return self.store.get_notification_state(key, default) if self.store is not None else default

    def _save_state(self, key: str, value: str) -> None:
        if self.store is not None:
            self.store.set_notification_state(key, value)

    def seed_existing(self) -> None:
        for strategy, bots in self._sources():
            for bot in bots.values():
                key = f"event_cursor:{strategy}:{bot.id}"
                saved = self._state(key)
                count = int(saved) if saved else len(bot.events)
                self._seen[key] = min(count, len(bot.events))
                if not saved:
                    self._save_state(key, str(len(bot.events)))

    @staticmethod
    def format_event(strategy: str, bot, event) -> str:
        strategy_name = "Сітка" if strategy == "GRID" else "Накопичення"
        lines = [f"🤖 {bot.symbol} · {strategy_name}", EVENT_LABELS.get(event.event, event.event)]
        if event.price:
            lines.append(f"Ціна: {_price(event.price)} USDT")
        if getattr(event, "quote_amount", 0):
            lines.append(f"Сума: {event.quote_amount:.2f} USDT")
        if getattr(event, "realized_cycle_pnl", 0):
            lines.append(f"Результат циклу: {event.realized_cycle_pnl:+.2f} USDT")
        if event.message:
            lines.append(_event_message(event.message))
        return "\n".join(lines)

    def daily_report(self) -> str:
        prices = {}
        for _, bots in self._sources():
            for bot in bots.values():
                base_asset = getattr(bot, "base_asset", "")
                if base_asset and bot.last_price:
                    prices[base_asset] = bot.last_price
        snapshot = self.portfolio.snapshot(prices) if self.portfolio is not None else {}
        running_grid = sum(bot.status == "RUNNING" for bot in self.grid_engine.bots.values())
        running_dca = sum(bot.status == "RUNNING" for bot in self.dca_engine.bots.values())
        cycles = sum(bot.completed_cycles for bot in self.grid_engine.bots.values())
        return "\n".join([
            "📊 Spot Grid Lab · Щоденний PAPER-звіт",
            f"Капітал: {snapshot.get('total_equity', 0):.2f} USDT",
            f"Зафіксований результат: {snapshot.get('realized_pnl', 0):+.2f} USDT",
            f"Незакритий результат: {snapshot.get('unrealized_pnl', 0):+.2f} USDT",
            f"Комісії: {snapshot.get('fees_paid', 0):.2f} USDT",
            f"Завершено циклів: {cycles}",
            f"Працюють боти: сітка — {running_grid}, накопичення — {running_dca}",
        ])

    async def _send_telegram(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json={"chat_id": self.chat_id, "text": text})
            response.raise_for_status()

    async def send(self, kind: str, text: str) -> None:
        try:
            await self.sender(text)
            self.status.last_success_at = datetime.now(timezone.utc).isoformat()
            self.status.last_error = ""
            if self.store is not None:
                self.store.record_notification(kind, "SENT", text)
        except Exception as exc:
            self.status.last_error = str(exc)
            if self.store is not None:
                self.store.record_notification(kind, "FAILED", text, str(exc))
            raise

    async def send_test(self) -> None:
        if not self.enabled:
            raise ValueError("Telegram notifications are not configured")
        await self.send("TEST", "✅ Spot Grid Lab\nTelegram-сповіщення працюють.")

    async def scan_once(self, now: datetime | None = None) -> None:
        if not self.enabled:
            return
        for strategy, bots in self._sources():
            for bot in bots.values():
                key = f"event_cursor:{strategy}:{bot.id}"
                start = self._seen.get(key, int(self._state(key, "0")))
                for index, event in enumerate(bot.events[start:], start=start):
                    if event.event in IMPORTANT_EVENTS:
                        await self.send(event.event, self.format_event(strategy, bot, event))
                    self._seen[key] = index + 1
                    self._save_state(key, str(index + 1))
        current = now or datetime.now(timezone.utc)
        report_date = current.date().isoformat()
        if self.health_alert_provider is not None:
            for alert in self.health_alert_provider(current):
                state_key = f"health_alert:{alert['key']}"
                if self._state(state_key) != report_date:
                    await self.send("BOT_HEALTH_ALERT", alert["message"])
                    self._save_state(state_key, report_date)
        if current.hour >= self.daily_report_hour_utc and self._state("daily_report_date") != report_date:
            await self.send("DAILY_REPORT", self.daily_report())
            self._save_state("daily_report_date", report_date)
        week = current.isocalendar()
        week_key = f"{week.year}-W{week.week:02d}"
        if (
            current.weekday() == 0
            and current.hour >= self.daily_report_hour_utc
            and self.weekly_report_provider is not None
            and self._state("weekly_report_week") != week_key
        ):
            symbols = sorted({
                bot.symbol for bot in self.grid_engine.bots.values()
                if bot.status in {"RUNNING", "PAUSED"}
            })
            for symbol in symbols:
                text = await self.weekly_report_provider(symbol)
                await self.send("WEEKLY_EVALUATION", text)
            self._save_state("weekly_report_week", week_key)

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            try:
                await self.scan_once()
            except Exception:
                pass
            await asyncio.sleep(self.poll_seconds)

    def start_background(self) -> None:
        if not self.enabled or (self._task is not None and not self._task.done()):
            return
        self.seed_existing()
        self._task = asyncio.create_task(self.run_forever())

    async def stop_background(self) -> None:
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
