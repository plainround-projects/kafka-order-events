"""Use case: publish the lifecycle events of a range of orders."""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass

from ..domain.events import interleave, order_id_for
from .ports import EventPublisher


@dataclass(frozen=True, slots=True)
class ProducePlan:
    """What to publish: how many orders, from which number, keyed or not, how widely interleaved."""

    orders: int
    start: int = 1
    prefix: str = "order"
    keyed: bool = True
    window: int = 50
    max_pause_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.orders < 0 or self.start < 1 or self.window < 1 or self.max_pause_seconds < 0:
            raise ValueError(f"invalid produce plan: {self}")


def produce_orders(
    publisher: EventPublisher,
    plan: ProducePlan,
    rng: random.Random,
    clock: Callable[[], float],
    pause: Callable[[float], None],
) -> int:
    """Publish every event of `plan.orders` orders and return how many events were sent.

    Keyed events use the order id as the key, so one order's events share a partition. Unkeyed events
    are the failure mode the lesson demonstrates.
    """
    order_ids = (order_id_for(number, plan.prefix) for number in range(plan.start, plan.start + plan.orders))
    sent = 0
    for event in interleave(order_ids, plan.window, rng, clock):
        publisher.publish(event, event.order_id if plan.keyed else None)
        sent += 1
        if plan.max_pause_seconds:
            pause(rng.uniform(0, plan.max_pause_seconds))
    publisher.flush()
    return sent
