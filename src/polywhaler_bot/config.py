from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Milestone 1 configuration for:
    - runtime paths
    - database/log locations
    - Playwright persistent browser profile
    - Polywhaler feed URL + refresh cadence
    - milestone logging toggles

    Config is loaded from environment variables and optionally a local .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Core app identity
    app_name: str = "polywhaler-bot"
    environment: Literal["dev", "test", "prod"] = "dev"

    # Base paths
    project_root: Path = Field(default_factory=lambda: Path.cwd())
    data_dir: Path = Field(default_factory=lambda: Path.cwd() / "data")
    logs_dir: Path = Field(default_factory=lambda: Path.cwd() / "data" / "logs")
    playwright_profile_dir: Path = Field(
        default_factory=lambda: Path.cwd() / "data" / "playwright"
    )
    database_path: Path = Field(default_factory=lambda: Path.cwd() / "data" / "bot.db")

    # Polywhaler session / browsing
    polywhaler_feed_url: str = "https://polywhaler.com/"
    browser_headless: bool = False
    browser_slow_mo_ms: int = 0
    browser_timeout_ms: int = 30_000

    # Feed polling
    feed_refresh_interval_seconds: int = 5
    feed_source_page_name: str = "deep_trades_feed"

    # Milestone 1 extraction selectors
    # These are intentionally configurable because Polywhaler's DOM may change.
    # We will use these exact selectors in feed.py and can adjust them in .env
    # without changing code.
    feed_row_selector: str = "[data-qa='trade-row']"
    login_required_selector: str = "input[type='email'], button[type='submit'], form"

    # Logging
    verbose_row_logging: bool = True
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    # Optional behavior flags
    create_missing_directories: bool = True

    @field_validator(
        "project_root",
        "data_dir",
        "logs_dir",
        "playwright_profile_dir",
        "database_path",
        mode="before",
    )
    @classmethod
    def _coerce_path(cls, value: object) -> Path:
        if isinstance(value, Path):
            return value
        return Path(str(value)).expanduser().resolve()

    @field_validator("feed_refresh_interval_seconds")
    @classmethod
    def _validate_refresh_interval(cls, value: int) -> int:
        if value < 1:
            raise ValueError("feed_refresh_interval_seconds must be >= 1")
        return value

    @field_validator("browser_slow_mo_ms", "browser_timeout_ms")
    @classmethod
    def _validate_non_negative_ints(cls, value: int) -> int:
        if value < 0:
            raise ValueError("browser timing values must be >= 0")
        return value

    def ensure_runtime_directories(self) -> None:
        """
        Creates the runtime directories needed for milestone 1 if enabled.
        """
        if not self.create_missing_directories:
            return

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.playwright_profile_dir.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Cached settings loader for simple import-time reuse.

    This keeps configuration loading deterministic and avoids repeatedly parsing
    environment/.env state.
    """
    settings = Settings()
    settings.ensure_runtime_directories()
    return settings
