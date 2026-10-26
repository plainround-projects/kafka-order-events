import itertools
import random

import pytest

from order_events.domain.errors import MalformedEventError
from order_events.domain.events import (
    LIFECYCLE,
    EventType,
    OrderEvent,
    interleave,
    lifecycle_events,
    order_id_for,
)


def counting_clock(start: float = 100.0):
    ticks = iter(range(10_000))
    return lambda: start + next(ticks)


def test_order_ids_are_zero_padded_and_prefixed():
    assert order_id_for(42) == "order-00042"
    assert order_id_for(7, "unkeyed-1") == "unkeyed-1-00007"


def test_one_order_has_the_three_events_in_sequence():
    events = lifecycle_events("order-00001", ts=100.0)
    assert [event.type for event in events] == list(LIFECYCLE)
    assert [event.sequence for event in events] == [1, 2, 3]
    assert {event.order_id for event in events} == {"order-00001"}
    assert {event.ts for event in events} == {100.0}


def test_an_event_rejects_an_empty_order_id_and_a_sequence_below_one():
    with pytest.raises(MalformedEventError):
        OrderEvent("", EventType.PLACED, 1, 0.0)
    with pytest.raises(MalformedEventError):
        OrderEvent("order-1", EventType.PLACED, 0, 0.0)


def test_events_are_immutable_value_objects():
    event = OrderEvent("order-1", EventType.PAID, 2, 1.5)
    with pytest.raises(AttributeError):
        event.sequence = 3  # type: ignore[misc]
    assert event == OrderEvent("order-1", EventType.PAID, 2, 1.5)


def test_interleave_yields_every_event_once():
    order_ids = [order_id_for(number) for number in range(1, 101)]
    events = list(interleave(order_ids, 10, random.Random(1), counting_clock()))
    assert len(events) == 300
    assert {(event.order_id, event.sequence) for event in events} == {
        (order_id, sequence) for order_id in order_ids for sequence in (1, 2, 3)
    }


def test_interleave_keeps_each_orders_own_sequence_and_type_order():
    seen: dict[str, int] = {}
    for event in interleave([order_id_for(number) for number in range(1, 51)], 10, random.Random(2), counting_clock()):
        assert event.sequence == seen.get(event.order_id, 0) + 1
        assert event.type == LIFECYCLE[event.sequence - 1]
        seen[event.order_id] = event.sequence


def test_interleave_mixes_orders_when_the_window_is_wider_than_one():
    events = list(interleave([order_id_for(number) for number in range(1, 21)], 5, random.Random(3), counting_clock()))
    assert any(left.order_id != right.order_id and left.sequence != 3 for left, right in itertools.pairwise(events))


def test_interleave_with_a_window_of_one_sends_order_by_order():
    events = list(interleave(["a-1", "a-2"], 1, random.Random(0), counting_clock()))
    assert [event.order_id for event in events] == ["a-1"] * 3 + ["a-2"] * 3


def test_the_same_seed_gives_the_same_order_of_events():
    order_ids = [order_id_for(number) for number in range(1, 31)]
    first = [(e.order_id, e.sequence) for e in interleave(order_ids, 8, random.Random(9), counting_clock())]
    second = [(e.order_id, e.sequence) for e in interleave(order_ids, 8, random.Random(9), counting_clock())]
    assert first == second


def test_interleave_stamps_each_event_when_it_is_yielded():
    events = list(interleave(["a-1"], 1, random.Random(0), counting_clock(100.0)))
    assert [event.ts for event in events] == [100.0, 101.0, 102.0]


def test_interleave_rejects_a_window_below_one():
    with pytest.raises(ValueError, match="window"):
        list(interleave(["a-1"], 0, random.Random(0), counting_clock()))
