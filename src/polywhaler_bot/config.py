from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Project configuration for:
    - runtime paths
    - database/log locations
    - Playwright session/profile settings
    - Polywhaler feed settings
    - Polymarket environment + credential contract

    Config is loaded from environment variables and optionally a local .env file.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # -------------------------------------------------------------------------
    # Core app identity
    # -------------------------------------------------------------------------
    app_name: str = "polywhaler-bot"
    environment: Literal["dev", "test", "prod"] = "dev"

    # -------------------------------------------------------------------------
    # Base paths
    # -------------------------------------------------------------------------
    project_root: Path = Field(default_factory=lambda: Path.cwd())
    data_dir: Path = Field(default_factory=lambda: Path.cwd() / "data")
    logs_dir: Path = Field(default_factory=lambda: Path.cwd() / "data" / "logs")
    playwright_profile_dir: Path = Field(
        default_factory=lambda: Path.cwd() / "data" / "playwright"
    )
    database_path: Path = Field(default_factory=lambda: Path.cwd() / "data" / "bot.db")

    # -------------------------------------------------------------------------
    # Polywhaler session / browsing
    # -------------------------------------------------------------------------
    polywhaler_feed_url: str = "https://www.polywhaler.com/deep"
    browser_headless: bool = False
    browser_slow_mo_ms: int = 0
    browser_timeout_ms: int = 30_000

    # -------------------------------------------------------------------------
    # Feed polling
    # -------------------------------------------------------------------------
    feed_refresh_interval_seconds: int = 5
    feed_source_page_name: str = "deep_trades_feed"

    # Milestone 1 extraction selectors retained for compatibility/debug use.
    feed_row_selector: str = "[data-qa='trade-row']"
    login_required_selector: str = "input[type='email'], button[type='submit'], form"

    # -------------------------------------------------------------------------
    # Logging
    # -------------------------------------------------------------------------
    verbose_row_logging: bool = True
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    # -------------------------------------------------------------------------
    # Optional behavior flags
    # -------------------------------------------------------------------------
    create_missing_directories: bool = True

    # -------------------------------------------------------------------------
    # Polymarket public connectivity defaults
    # -------------------------------------------------------------------------
    polymarket_clob_host: str = "https://clob.polymarket.com"
    polymarket_gamma_host: str = "https://gamma-api.polymarket.com"
    polymarket_data_host: str = "https://data-api.polymarket.com"
    polymarket_chain_id: int = 137

    # -------------------------------------------------------------------------
    # Polymarket L1 / L2 credential contract
    # All auth-bearing values are optional for now.
    # Secret-bearing values use SecretStr so they are not accidentally printed.
    # -------------------------------------------------------------------------
    polymarket_private_key: SecretStr | None = None
    polymarket_signature_type: int | None = None
    polymarket_funder_address: str | None = None

    polymarket_api_key: str | None = None
    polymarket_api_secret: SecretStr | None = None
    polymarket_api_passphrase: SecretStr | None = None

    polymarket_profile_address: str | None = None
    polymarket_test_market_slug: str | None = None
    polymarket_test_token_id: str | None = None

    # -------------------------------------------------------------------------
    # Validators
    # -------------------------------------------------------------------------
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

    @field_validator("polymarket_chain_id")
    @classmethod
    def _validate_chain_id(cls, value: int) -> int:
        if value <= 0:
            raise ValueError("polymarket_chain_id must be > 0")
        return value

    @field_validator("polymarket_signature_type")
    @classmethod
    def _validate_signature_type(cls, value: int | None) -> int | None:
        if value is None:
            return value
        if value not in {0, 1, 2}:
            raise ValueError("polymarket_signature_type must be one of: 0, 1, 2")
        return value

    @field_validator(
        "polymarket_clob_host",
        "polymarket_gamma_host",
        "polymarket_data_host",
        "polywhaler_feed_url",
    )
    @classmethod
    def _validate_http_url_like(cls, value: str) -> str:
        value = value.strip()
        if not (value.startswith("http://") or value.startswith("https://")):
            raise ValueError("URL-like settings must start with http:// or https://")
        return value.rstrip("/")

    @field_validator(
        "polymarket_funder_address",
        "polymarket_profile_address",
        mode="before",
    )
    @classmethod
    def _normalize_address_like(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @field_validator(
        "polymarket_api_key",
        "polymarket_test_market_slug",
        "polymarket_test_token_id",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    # -------------------------------------------------------------------------
    # Runtime helpers
    # -------------------------------------------------------------------------
    def ensure_runtime_directories(self) -> None:
        """
        Creates the runtime directories needed for the current milestones if enabled.
        """
        if not self.create_missing_directories:
            return

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.playwright_profile_dir.mkdir(parents=True, exist_ok=True)
        self.database_path.parent.mkdir(parents=True, exist_ok=True)

    # -------------------------------------------------------------------------
    # Polymarket capability helpers
    # -------------------------------------------------------------------------
    @property
    def has_polymarket_l1_auth(self) -> bool:
        return (
            self.polymarket_private_key is not None
            and self.polymarket_signature_type is not None
            and bool(self.polymarket_funder_address)
        )

    @property
    def has_polymarket_l2_creds(self) -> bool:
        return (
            bool(self.polymarket_api_key)
            and self.polymarket_api_secret is not None
            and self.polymarket_api_passphrase is not None
        )

    @property
    def has_polymarket_profile_config(self) -> bool:
        return bool(self.polymarket_profile_address)

    def safe_summary(self) -> dict[str, Any]:
        """
        Returns a safe-to-print config summary that never includes secrets.
        """
        return {
            "app_name": self.app_name,
            "environment": self.environment,
            "project_root": str(self.project_root),
            "data_dir": str(self.data_dir),
            "logs_dir": str(self.logs_dir),
            "database_path": str(self.database_path),
            "polywhaler_feed_url": self.polywhaler_feed_url,
            "polymarket": {
                "clob_host": self.polymarket_clob_host,
                "gamma_host": self.polymarket_gamma_host,
                "data_host": self.polymarket_data_host,
                "chain_id": self.polymarket_chain_id,
                "signature_type": self.polymarket_signature_type,
                "funder_address": self.polymarket_funder_address,
                "profile_address": self.polymarket_profile_address,
                "test_market_slug": self.polymarket_test_market_slug,
                "test_token_id": self.polymarket_test_token_id,
                "has_l1_auth": self.has_polymarket_l1_auth,
                "has_l2_creds": self.has_polymarket_l2_creds,
                "has_profile_config": self.has_polymarket_profile_config,
            },
        }


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Cached settings loader for simple import-time reuse.
    """
    settings = Settings()
    settings.ensure_runtime_directories()
    return settings
