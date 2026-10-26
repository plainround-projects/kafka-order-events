"""Configuration from the environment, validated once at startup (twelve-factor)."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

_SERVERS = re.compile(r"^[\w.\-]+:\d{1,5}(,[\w.\-]+:\d{1,5})*$")
_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class ConfigurationError(ValueError):
    """An environment variable is missing or has an unusable value."""


def _text(environ: Mapping[str, str], name: str, default: str) -> str:
    value = environ.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _positive_int(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, str(default)).strip()
    if not raw.isdigit() or int(raw) < 1:
        raise ConfigurationError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)


@dataclass(frozen=True, slots=True)
class Settings:
    """Everything the programs read from the environment. See .env.example for each variable."""

    bootstrap_servers: str
    topic: str
    group_id: str
    partitions: int
    log_level: str
    request_timeout_ms: int
    retry_backoff_ms: int

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        servers = _text(env, "BOOTSTRAP_SERVERS", "localhost:9092")
        if not _SERVERS.match(servers):
            raise ConfigurationError(f"BOOTSTRAP_SERVERS must be host:port[,host:port], got {servers!r}")
        level = _text(env, "LOG_LEVEL", "INFO").upper()
        if level not in _LEVELS:
            raise ConfigurationError(f"LOG_LEVEL must be one of {sorted(_LEVELS)}, got {level!r}")
        return cls(
            bootstrap_servers=servers,
            topic=_text(env, "TOPIC", "order-events"),
            group_id=_text(env, "GROUP_ID", "order-history"),
            partitions=_positive_int(env, "PARTITIONS", 6),
            log_level=level,
            request_timeout_ms=_positive_int(env, "KAFKA_REQUEST_TIMEOUT_MS", 30_000),
            retry_backoff_ms=_positive_int(env, "KAFKA_RETRY_BACKOFF_MS", 100),
        )
