"""Adapter: is the broker reachable and does the topic have the partitions the design calls for?"""

from __future__ import annotations

from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ..application.health import ProbeFailedError
from .retry import retry
from .settings import Settings


class KafkaTopicProbe:
    """Implements `HealthProbe` for the broker and the order-events topic."""

    name = "kafka"

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def check(self) -> str:
        try:
            partitions = retry(
                self._partitions,
                attempts=3,
                base_delay_seconds=self._settings.retry_backoff_ms / 1000,
                retry_on=(KafkaError,),
            )
        except KafkaError as error:
            raise ProbeFailedError(f"cannot reach {self._settings.bootstrap_servers}: {error}") from error
        if len(partitions) != self._settings.partitions:
            raise ProbeFailedError(
                f"topic {self._settings.topic} has {len(partitions)} partitions, expected {self._settings.partitions}"
            )
        return f"topic {self._settings.topic} has {len(partitions)} partitions"

    def _partitions(self) -> set[int]:
        consumer: Any = KafkaConsumer(
            bootstrap_servers=self._settings.bootstrap_servers,
            enable_auto_commit=False,
            request_timeout_ms=self._settings.request_timeout_ms,
        )
        try:
            return set(consumer.partitions_for_topic(self._settings.topic) or ())
        finally:
            consumer.close(autocommit=False)
