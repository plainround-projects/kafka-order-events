"""Adapter: publish order events with kafka-python."""

from __future__ import annotations

from typing import Any

from kafka import KafkaProducer

from ..domain.events import OrderEvent
from .codec import encode_event
from .settings import Settings


class KafkaEventPublisher:
    """Implements `EventPublisher`. The client waits for all in-sync replicas (acks=all) and retries."""

    def __init__(self, settings: Settings, topic: str | None = None, flush_timeout_seconds: float = 60.0) -> None:
        self._topic = topic or settings.topic
        self._flush_timeout = flush_timeout_seconds
        self._producer: Any = KafkaProducer(
            bootstrap_servers=settings.bootstrap_servers,
            acks="all",
            request_timeout_ms=settings.request_timeout_ms,
            retry_backoff_ms=settings.retry_backoff_ms,
        )

    def publish(self, event: OrderEvent, key: str | None) -> None:
        self._producer.send(self._topic, key=None if key is None else key.encode(), value=encode_event(event))

    def flush(self) -> None:
        self._producer.flush(timeout=self._flush_timeout)

    def close(self) -> None:
        self._producer.close(timeout=self._flush_timeout)
