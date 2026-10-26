"""The ports the use cases need. Infrastructure adapters implement them; tests use fakes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from ..domain.events import OrderEvent


@dataclass(frozen=True, slots=True)
class Delivery:
    """An event as read from the log, with where it was read."""

    event: OrderEvent
    partition: int
    offset: int


class EventPublisher(Protocol):
    """Sends order events to the log."""

    def publish(self, event: OrderEvent, key: str | None) -> None:
        """Send one event. `key` decides the partition; None lets the client choose."""

    def flush(self) -> None:
        """Block until every published event is acknowledged."""


class EventSource(Protocol):
    """Reads order events from the log as a member of a consumer group."""

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        """Return the next deliveries, or an empty sequence when none arrived in time."""

    def close(self) -> None:
        """Commit progress and leave the consumer group."""


class OffsetReader(Protocol):
    """Reads log-end and committed offsets without joining a group."""

    def end_offsets(self, topic: str) -> Mapping[int, int]:
        """Log-end offset per partition."""

    def committed_offsets(self, group_id: str, topic: str) -> Mapping[int, int | None]:
        """The group's committed offset per partition, None where it never committed."""


class HealthProbe(Protocol):
    """A dependency check that reports a short detail or fails with ProbeFailedError."""

    name: str

    def check(self) -> str:
        """Return a short detail on success; raise `ProbeFailedError` when the dependency is not usable."""
