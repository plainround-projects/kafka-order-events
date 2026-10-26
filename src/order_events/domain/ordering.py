"""Per-order sequence checking: the proof that a key keeps an order's events in order."""

from __future__ import annotations

from dataclasses import dataclass, field

from .events import OrderEvent


@dataclass(frozen=True, slots=True)
class OutOfOrder:
    """An event whose sequence was not the previous one plus one."""

    order_id: str
    expected: int
    got: int


@dataclass(frozen=True, slots=True)
class OrderingReport:
    """What a checker saw: how many orders, which events were out of order, which orders were split."""

    orders: int
    out_of_order: tuple[OutOfOrder, ...]
    orders_split_across_partitions: int

    def summary(self) -> str:
        return (
            f"orders: {self.orders}  out-of-order events logged: {len(self.out_of_order)}  "
            f"orders spread over more than one partition: {self.orders_split_across_partitions}"
        )


@dataclass
class OrderChecker:
    """Tracks the last sequence per order and the partitions each order was read from."""

    _last_sequence: dict[str, int] = field(default_factory=dict)
    _partitions: dict[str, set[int]] = field(default_factory=dict)
    _history: dict[str, list[str]] = field(default_factory=dict)
    _out_of_order: list[OutOfOrder] = field(default_factory=list)

    def observe(self, event: OrderEvent, partition: int) -> bool:
        """Record one event; return False when it arrived out of order."""
        previous = self._last_sequence.get(event.order_id, 0)
        in_order = event.sequence == previous + 1
        if not in_order:
            self._out_of_order.append(OutOfOrder(event.order_id, previous + 1, event.sequence))
        # Keep the highest sequence seen, so one late event is counted once and not again for the
        # events that follow it.
        self._last_sequence[event.order_id] = max(event.sequence, previous)
        self._partitions.setdefault(event.order_id, set()).add(partition)
        self._history.setdefault(event.order_id, []).append(event.type.value)
        return in_order

    @property
    def out_of_order_count(self) -> int:
        return len(self._out_of_order)

    def history(self, order_id: str) -> tuple[str, ...]:
        return tuple(self._history.get(order_id, ()))

    def partitions_of(self, order_id: str) -> tuple[int, ...]:
        return tuple(sorted(self._partitions.get(order_id, ())))

    def first_orders(self, count: int) -> tuple[str, ...]:
        return tuple(list(self._history)[:count])

    def report(self) -> OrderingReport:
        split = sum(1 for seen in self._partitions.values() if len(seen) > 1)
        return OrderingReport(len(self._last_sequence), tuple(self._out_of_order), split)
