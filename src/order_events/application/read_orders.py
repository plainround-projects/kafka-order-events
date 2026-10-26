"""Use case: read the order stream as one member of a group and check every order's sequence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from ..domain.ordering import OrderChecker, OrderingReport
from .ports import Delivery, EventSource


@dataclass(frozen=True, slots=True)
class ReadPlan:
    idle_timeout_seconds: float | None = None  # None: run until `stop` says so
    work_delay_seconds: float = 0.0  # artificial work per event, to build lag on purpose
    order_prefix: str = ""  # only check orders whose id starts with this
    poll_seconds: float = 0.5


@dataclass(frozen=True, slots=True)
class ReadResult:
    events_read: int
    report: OrderingReport
    first_histories: tuple[tuple[str, tuple[int, ...], tuple[str, ...]], ...]


def read_orders(
    source: EventSource,
    plan: ReadPlan,
    *,
    stop: Callable[[], bool],
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    on_out_of_order: Callable[[Delivery], None],
    histories: int = 0,
) -> ReadResult:
    """Poll until `stop()` or until `plan.idle_timeout_seconds` pass without a matching event."""
    checker = OrderChecker()
    events_read = 0
    last_event_at = clock()
    while not stop():
        deliveries = source.poll(plan.poll_seconds)
        for delivery in deliveries:
            if not delivery.event.order_id.startswith(plan.order_prefix):
                continue
            if not checker.observe(delivery.event, delivery.partition):
                on_out_of_order(delivery)
            events_read += 1
            if plan.work_delay_seconds:
                sleep(plan.work_delay_seconds)
        if deliveries:
            last_event_at = clock()
        elif plan.idle_timeout_seconds is not None and clock() - last_event_at > plan.idle_timeout_seconds:
            break
    first = tuple(
        (order_id, checker.partitions_of(order_id), checker.history(order_id))
        for order_id in checker.first_orders(histories)
    )
    return ReadResult(events_read, checker.report(), first)
