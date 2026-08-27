"""Environment-based application configuration."""

from functools import lru_cache
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings loaded from environment variables (and a local .env file)."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "job-queue-service"
    environment: Literal["local", "development", "production", "test"] = "local"
    log_level: str = "INFO"
    log_format: Literal["json", "text"] = "json"

    database_url: str = "postgresql+asyncpg://jobqueue:jobqueue@postgres:5432/jobqueue"
    db_echo: bool = False
    db_pool_size: int = Field(default=5, ge=1)
    db_max_overflow: int = Field(default=10, ge=0)

    redis_url: str = "redis://redis:6379/0"

    @property
    def sync_database_url(self) -> str:
        """Driver-less URL for tools that require a synchronous DBAPI."""
        return self.database_url.replace("+asyncpg", "")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
