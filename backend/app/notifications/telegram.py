import asyncio
from dataclasses import dataclass

import httpx


IMPORTANT_EVENTS = {
    "BUY_FILLED", "SELL_FILLED", "BUY_BLOCKED", "SELL_BLOCKED",
    "BUDGET_COMPLETED", "ENGINE_ERROR", "AUTO_PAUSED",
}


@dataclass
class NotificationStatus:
    enabled: bool
    last_success_at: str = ""
    last_error: str = ""


class TelegramNotifier:
    def __init__(self, token: str, chat_id: str, grid_engine, dca_engine,
                 poll_seconds: float = 2.0, sender=None) -> None:
        self.token = token
        self.chat_id = chat_id
        self.grid_engine = grid_engine
        self.dca_engine = dca_engine
        self.poll_seconds = max(1.0, poll_seconds)
        self.enabled = bool(token and chat_id)
        self.sender = sender or self._send_telegram
        self.status = NotificationStatus(enabled=self.enabled)
        self._seen: dict[str, int] = {}
        self._task: asyncio.Task | None = None
        self._running = False

    def _sources(self):
        return (("GRID", self.grid_engine.bots), ("DCA", self.dca_engine.bots))

    def seed_existing(self) -> None:
        for strategy, bots in self._sources():
            for bot in bots.values():
                self._seen[f"{strategy}:{bot.id}"] = len(bot.events)

    @staticmethod
    def format_event(strategy: str, bot, event) -> str:
        lines = [f"Spot Grid Lab · {strategy}", f"{event.event} · {bot.symbol}"]
        if event.price:
            lines.append(f"Price: {event.price:.8f}")
        if getattr(event, "quote_amount", 0):
            lines.append(f"Quote: {event.quote_amount:.4f} USDT")
        if getattr(event, "realized_cycle_pnl", 0):
            lines.append(f"Cycle P&L: {event.realized_cycle_pnl:.4f} USDT")
        if event.message:
            lines.append(event.message)
        return "\n".join(lines)

    async def _send_telegram(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(url, json={"chat_id": self.chat_id, "text": text})
            response.raise_for_status()

    async def scan_once(self) -> None:
        if not self.enabled:
            return
        for strategy, bots in self._sources():
            for bot in bots.values():
                key = f"{strategy}:{bot.id}"
                start = self._seen.get(key, 0)
                for index, event in enumerate(bot.events[start:], start=start):
                    if event.event in IMPORTANT_EVENTS:
                        await self.sender(self.format_event(strategy, bot, event))
                    self._seen[key] = index + 1

    async def run_forever(self) -> None:
        self._running = True
        while self._running:
            try:
                await self.scan_once()
                self.status.last_error = ""
            except Exception as exc:
                self.status.last_error = str(exc)
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
