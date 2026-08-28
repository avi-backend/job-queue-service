"""Environment-based application configuration."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, model_validator
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

    #: How long a worker waits before polling the ready queue again.
    worker_poll_interval_seconds: float = Field(default=1.0, gt=0)

    #: How long a claim owns a job before crash recovery may take it away.
    job_lease_seconds: float = Field(default=60.0, gt=0)
    #: How often the owning worker extends its lease. Must stay below the lease.
    job_heartbeat_seconds: float = Field(default=20.0, gt=0)

    #: How often each worker looks for due SCHEDULED jobs, and how many it takes.
    scheduler_interval_seconds: float = Field(default=1.0, gt=0)
    scheduler_batch_size: int = Field(default=100, ge=1)

    #: How often each worker looks for expired leases, and how many it recovers.
    recovery_interval_seconds: float = Field(default=5.0, gt=0)
    recovery_batch_size: int = Field(default=100, ge=1)

    @model_validator(mode="after")
    def _heartbeat_must_beat_before_the_lease_expires(self) -> "Settings":
        """A heartbeat slower than the lease would let a healthy worker be recovered.

        Enforced here rather than documented, because the failure it prevents
        (two workers executing the same job) is the one this phase exists to
        rule out. Some headroom is required so a single slow beat is survivable.
        """
        if self.job_heartbeat_seconds >= self.job_lease_seconds:
            raise ValueError(
                "JOB_HEARTBEAT_SECONDS must be smaller than JOB_LEASE_SECONDS "
                f"(got {self.job_heartbeat_seconds} >= {self.job_lease_seconds})"
            )
        return self

    @property
    def sync_database_url(self) -> str:
        """Driver-less URL for tools that require a synchronous DBAPI."""
        return self.database_url.replace("+asyncpg", "")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
