"""Settings read once per execution environment, from the environment."""

from __future__ import annotations

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGEST_", extra="ignore")

    # Postgres. The password arrives as a Secrets Manager value injected by
    # Terraform, so it is an environment variable here and nothing else.
    database_url: str = "postgresql://localhost/ingest"
    connect_timeout: int = 5
    statement_timeout_ms: int = 25_000

    # Queues. The replay handler needs both, the load handler needs neither.
    main_queue_url: str = ""
    dlq_url: str = ""
    max_replays: int = 3

    # Reconcile.
    bucket: str = ""
    prefix: str = "raw/"

    metrics_namespace: str = "ServerlessIngest"
    metrics_enabled: bool = True


@lru_cache(maxsize=1)
def settings() -> Settings:
    """Cached, because a warm invocation should not re-read the environment."""
    return Settings()
