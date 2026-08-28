from enum import Enum
from pydantic_settings import BaseSettings, SettingsConfigDict


class TradingMode(str, Enum):
    PAPER = "PAPER"
    TESTNET = "TESTNET"
    LIVE = "LIVE"


class Settings(BaseSettings):
    trading_mode: TradingMode = TradingMode.PAPER
    binance_api_key: str = ""
    binance_api_secret: str = ""

    quote_asset: str = "USDT"
    paper_start_balance: float = 10_000.0
    max_portfolio_allocation_pct: float = 50.0
    max_position_pct: float = 10.0
    reserve_usdt_pct: float = 20.0
    grid_poll_seconds: float = 5.0

    app_host: str = "0.0.0.0"
    app_port: int = 8000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()
