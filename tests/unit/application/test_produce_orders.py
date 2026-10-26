import random

import pytest

from order_events.application.produce_orders import ProducePlan, produce_orders
from tests.unit.application.fakes import RecordingPublisher


def run(plan: ProducePlan, pauses: list[float] | None = None) -> tuple[int, RecordingPublisher]:
    publisher = RecordingPublisher()
    sent = produce_orders(publisher, plan, random.Random(4), lambda: 1.0, (pauses if pauses is not None else []).append)
    return sent, publisher


def test_keyed_events_use_the_order_id_as_the_key():
    sent, publisher = run(ProducePlan(orders=10))
    assert sent == 30
    assert publisher.flushed == 1
    assert all(key == event.order_id for event, key in publisher.published)


def test_unkeyed_events_have_no_key():
    _, publisher = run(ProducePlan(orders=10, keyed=False))
    assert {key for _, key in publisher.published} == {None}


def test_orders_start_at_the_given_number_with_the_given_prefix():
    _, publisher = run(ProducePlan(orders=2, start=1001, prefix="run-b"))
    assert {event.order_id for event, _ in publisher.published} == {"run-b-01001", "run-b-01002"}


def test_a_pause_follows_every_event_when_a_maximum_is_set():
    pauses: list[float] = []
    sent, _ = run(ProducePlan(orders=4, max_pause_seconds=0.05), pauses)
    assert len(pauses) == sent
    assert all(0 <= pause <= 0.05 for pause in pauses)


def test_no_pause_without_a_maximum():
    pauses: list[float] = []
    run(ProducePlan(orders=4), pauses)
    assert pauses == []


def test_zero_orders_sends_nothing_but_still_flushes():
    sent, publisher = run(ProducePlan(orders=0))
    assert sent == 0
    assert publisher.flushed == 1


@pytest.mark.parametrize(
    "kwargs",
    [{"orders": -1}, {"orders": 1, "start": 0}, {"orders": 1, "window": 0}, {"orders": 1, "max_pause_seconds": -1}],
)
def test_an_invalid_plan_is_rejected(kwargs):
    with pytest.raises(ValueError, match="invalid produce plan"):
        ProducePlan(**kwargs)


def test_a_stop_request_ends_the_run_after_flushing_what_was_sent():
    stops = iter([False, False, False, True])
    publisher = RecordingPublisher()
    sent = produce_orders(
        publisher, ProducePlan(orders=10), random.Random(4), lambda: 1.0, lambda _: None, stop=lambda: next(stops)
    )
    assert sent == 3
    assert len(publisher.published) == 3
    assert publisher.flushed == 1
