"""Order events and the pure functions that build and interleave them."""

from __future__ import annotations

import random
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from enum import StrEnum

from .errors import MalformedEventError


class EventType(StrEnum):
    """The three events of an order's lifecycle, in the order they happen."""

    PLACED = "OrderPlaced"
    PAID = "OrderPaid"
    SHIPPED = "OrderShipped"


LIFECYCLE: tuple[EventType, ...] = tuple(EventType)


@dataclass(frozen=True, slots=True)
class OrderEvent:
    """One thing that happened to one order. `sequence` counts from 1 within the order."""

    order_id: str
    type: EventType
    sequence: int
    ts: float

    def __post_init__(self) -> None:
        if not self.order_id:
            raise MalformedEventError("order id is empty")
        if self.sequence < 1:
            raise MalformedEventError(f"sequence must start at 1, got {self.sequence}")


def order_id_for(number: int, prefix: str = "order") -> str:
    """`order-00042`: a zero-padded, prefixed id. A different prefix keeps two runs' ids apart."""
    return f"{prefix}-{number:05d}"


def lifecycle_events(order_id: str, ts: float) -> list[OrderEvent]:
    """The three events of one order, all stamped `ts`."""
    return [OrderEvent(order_id, event_type, sequence, ts) for sequence, event_type in enumerate(LIFECYCLE, start=1)]


def interleave(
    order_ids: Iterable[str],
    window: int,
    rng: random.Random,
    clock: Callable[[], float],
) -> Iterator[OrderEvent]:
    """Yield every event of every order, mixing up to `window` open orders.

    Each order's own events stay in sequence; only events of different orders mix, which is what a
    busy order service sends. Each event is stamped with `clock()` when it is yielded.
    """
    if window < 1:
        raise ValueError("window must be at least 1")
    pending = iter(order_ids)
    open_orders: list[tuple[str, list[EventType]]] = []

    def refill() -> None:
        while len(open_orders) < window:
            order_id = next(pending, None)
            if order_id is None:
                return
            open_orders.append((order_id, list(LIFECYCLE)))

    refill()
    while open_orders:
        entry = open_orders[rng.randrange(len(open_orders))]
        order_id, remaining = entry
        sequence = len(LIFECYCLE) - len(remaining) + 1
        yield OrderEvent(order_id, remaining.pop(0), sequence, clock())
        if not remaining:
            open_orders.remove(entry)
            refill()
