"""Adapter: read order events as a member of a consumer group with kafka-python."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import Any

from kafka import ConsumerRebalanceListener, KafkaConsumer

from ..application.ports import Delivery
from ..domain.errors import MalformedEventError
from .codec import decode_event
from .logging import fields
from .settings import Settings

logger = logging.getLogger(__name__)


class _LogAssignment(ConsumerRebalanceListener):  # type: ignore[misc]
    def __init__(self, member: str) -> None:
        self._member = member

    def on_partitions_revoked(self, revoked: Iterable[Any]) -> None:
        logger.info("partitions revoked", extra=fields(member=self._member, count=len(list(revoked))))

    def on_partitions_assigned(self, assigned: Iterable[Any]) -> None:
        numbers = sorted(partition.partition for partition in assigned)
        logger.info("partitions assigned", extra=fields(member=self._member, partitions=numbers or "none (idle)"))


class KafkaEventSource:
    """Implements `EventSource`. Offsets auto-commit every second and once more on `close`."""

    def __init__(self, settings: Settings, member: str, group_id: str | None = None, topic: str | None = None) -> None:
        self._member = member
        self._consumer: Any = KafkaConsumer(
            bootstrap_servers=settings.bootstrap_servers,
            group_id=group_id or settings.group_id,
            client_id=member,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            request_timeout_ms=settings.request_timeout_ms,
            retry_backoff_ms=settings.retry_backoff_ms,
        )
        self._consumer.subscribe([topic or settings.topic], listener=_LogAssignment(member))

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        deliveries: list[Delivery] = []
        for batch in self._consumer.poll(timeout_ms=int(timeout_seconds * 1000)).values():
            for record in batch:
                try:
                    deliveries.append(Delivery(decode_event(record.value), record.partition, record.offset))
                except MalformedEventError as error:
                    logger.warning(
                        "skipping malformed record",
                        extra=fields(
                            member=self._member, partition=record.partition, offset=record.offset, error=error
                        ),
                    )
        return deliveries

    @property
    def assigned_partition_count(self) -> int:
        """How many partitions the group currently gives this member (0 means idle)."""
        return len(self._consumer.assignment())

    def close(self) -> None:
        self._consumer.close()
