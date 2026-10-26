"""Adapter: committed and log-end offsets, read without joining the group."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from .logging import fields
from .retry import retry
from .settings import Settings

logger = logging.getLogger(__name__)


class KafkaOffsetReader:
    """Implements `OffsetReader`. Every call retries with backoff on Kafka errors."""

    def __init__(self, settings: Settings, attempts: int = 4) -> None:
        self._settings = settings
        self._attempts = attempts

    def _with_retry(self, operation: Any) -> Any:
        return retry(
            operation,
            attempts=self._attempts,
            base_delay_seconds=self._settings.retry_backoff_ms / 1000,
            retry_on=(KafkaError,),
            on_retry=lambda attempt, error: logger.warning(
                "retrying offset read", extra=fields(attempt=attempt, error=error)
            ),
        )

    def _consumer(self, group_id: str | None) -> Any:
        return KafkaConsumer(
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=False,
            request_timeout_ms=self._settings.request_timeout_ms,
        )

    def end_offsets(self, topic: str) -> Mapping[int, int]:
        def read() -> Mapping[int, int]:
            consumer = self._consumer(None)
            try:
                partitions = [
                    TopicPartition(topic, number) for number in sorted(consumer.partitions_for_topic(topic) or ())
                ]
                return {partition.partition: end for partition, end in consumer.end_offsets(partitions).items()}
            finally:
                consumer.close(autocommit=False)

        result: Mapping[int, int] = self._with_retry(read)
        return result

    def committed_offsets(self, group_id: str, topic: str) -> Mapping[int, int | None]:
        def read() -> Mapping[int, int | None]:
            # Same group id, never subscribed: asks the broker for committed offsets without joining.
            consumer = self._consumer(group_id)
            try:
                numbers = sorted(consumer.partitions_for_topic(topic) or ())
                return {number: consumer.committed(TopicPartition(topic, number)) for number in numbers}
            finally:
                consumer.close(autocommit=False)

        result: Mapping[int, int | None] = self._with_retry(read)
        return result
