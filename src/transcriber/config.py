"""Typed configuration, loaded from the environment / .env file."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS_", extra="ignore")

    url: str = "redis://redis:6379/0"
    #: Must match the `prefix` used by the producing (Node) services so we
    #: read from the same BullMQ key space.
    prefix: str = "bull"


class QueueSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="QUEUE_", extra="ignore")

    name: str = "transcription"
    #: Where completion/failure events are published for downstream services.
    events_name: str = "transcription-events"
    publish_events: bool = True
    concurrency: int = Field(default=2, ge=1, le=64)
    #: Seconds a job may run before BullMQ considers the worker stalled.
    lock_duration_seconds: int = Field(default=300, ge=30)


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DATABASE_", extra="ignore")

    url: str = "postgresql+asyncpg://transcriber:transcriber@postgres:5432/transcriber"
    pool_size: int = Field(default=5, ge=1)
    max_overflow: int = Field(default=5, ge=0)
    echo: bool = False

    @field_validator("url")
    @classmethod
    def _require_async_driver(cls, value: str) -> str:
        if value.startswith("postgresql://"):
            # Fail loudly rather than blocking the event loop on psycopg2.
            return value.replace("postgresql://", "postgresql+asyncpg://", 1)
        return value


class DeepgramSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DEEPGRAM_", extra="ignore")

    api_key: SecretStr = SecretStr("")
    base_url: str = "https://api.deepgram.com"
    model: str = "nova-3"
    language: str = "en"
    smart_format: bool = True
    punctuate: bool = True
    diarize: bool = False
    #: Hard ceiling per request. Anything longer is chunked by this service.
    max_chunk_seconds: float = Field(default=600.0, gt=0)
    request_timeout_seconds: float = Field(default=300.0, gt=0)
    max_retries: int = Field(default=3, ge=0)


class AudioSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AUDIO_", extra="ignore")

    #: Root that every incoming audio path must resolve inside of. Guards
    #: against a malicious/buggy producer handing us `/etc/passwd`.
    root: Path = Path("/data/audio")
    enforce_root: bool = True
    work_dir: Path = Path("/tmp/transcriber")
    ffmpeg_path: str = "ffmpeg"
    ffprobe_path: str = "ffprobe"
    #: Chunks are normalised to mono 16 kHz FLAC: lossless, small, and what
    #: ASR models expect anyway.
    chunk_sample_rate: int = 16_000
    chunk_format: str = "flac"
    #: How many chunks of one job may be in flight at the provider at once.
    max_parallel_chunks: int = Field(default=3, ge=1, le=16)


class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", extra="ignore")

    host: str = "0.0.0.0"
    port: int = 8000


class Settings(BaseSettings):
    """Composition root for every other settings object."""

    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    environment: str = "development"
    log_level: str = "INFO"
    json_logs: bool = True

    redis: RedisSettings = Field(default_factory=RedisSettings)
    queue: QueueSettings = Field(default_factory=QueueSettings)
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    deepgram: DeepgramSettings = Field(default_factory=DeepgramSettings)
    audio: AudioSettings = Field(default_factory=AudioSettings)
    api: APISettings = Field(default_factory=APISettings)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Process-wide settings singleton."""
    return Settings()
