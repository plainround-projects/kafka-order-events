from order_events.application.ports import Delivery
from order_events.application.read_orders import ReadPlan, read_orders
from order_events.domain.events import EventType, OrderEvent
from tests.unit.application.fakes import FakeClock, ScriptedSource


def delivery(order_id: str, sequence: int, partition: int = 0, offset: int = 0) -> Delivery:
    type_ = list(EventType)[sequence - 1]
    return Delivery(OrderEvent(order_id, type_, sequence, 0.0), partition, offset)


def read(
    batches, plan: ReadPlan, clock: FakeClock | None = None, histories: int = 0, stop_after_polls: int | None = None
):
    clock = clock or FakeClock()
    out_of_order: list[Delivery] = []
    polls = 0

    def stop() -> bool:
        nonlocal polls
        polls += 1
        return stop_after_polls is not None and polls > stop_after_polls

    source = ScriptedSource(batches)
    result = read_orders(
        source,
        plan,
        stop=stop,
        clock=clock,
        sleep=clock.sleep,
        on_out_of_order=out_of_order.append,
        histories=histories,
    )
    return result, out_of_order, clock


def test_events_in_order_leave_a_clean_report_and_the_histories_are_returned():
    batches = [[delivery("a", 1, 2), delivery("a", 2, 2)], [delivery("a", 3, 2)]]
    result, out_of_order, _ = read(batches, ReadPlan(idle_timeout_seconds=3), histories=1)
    assert result.events_read == 3
    assert out_of_order == []
    assert result.report.out_of_order == ()
    assert result.first_histories == (("a", (2,), ("OrderPlaced", "OrderPaid", "OrderShipped")),)


def test_an_event_out_of_order_is_reported_to_the_callback():
    batches = [[delivery("a", 1, 0), delivery("a", 3, 1)]]
    result, out_of_order, _ = read(batches, ReadPlan(idle_timeout_seconds=3))
    assert [item.event.sequence for item in out_of_order] == [3]
    assert len(result.report.out_of_order) == 1


def test_orders_outside_the_prefix_are_ignored():
    batches = [[delivery("run-1-a", 1), delivery("other-b", 1)]]
    result, _, _ = read(batches, ReadPlan(idle_timeout_seconds=3, order_prefix="run-1"))
    assert result.events_read == 1
    assert result.report.orders == 1


def test_work_delay_sleeps_once_per_event_read():
    batches = [[delivery("a", 1), delivery("a", 2)]]
    _, _, clock = read(batches, ReadPlan(idle_timeout_seconds=3, work_delay_seconds=0.01))
    assert clock.slept == [0.01, 0.01]


def test_it_exits_after_the_idle_timeout_with_no_events():
    result, _, clock = read([], ReadPlan(idle_timeout_seconds=2.5))
    assert result.events_read == 0
    assert clock.now > 2.5


def test_it_runs_until_stopped_when_there_is_no_idle_timeout():
    result, _, _ = read([[delivery("a", 1)]], ReadPlan(idle_timeout_seconds=None), stop_after_polls=5)
    assert result.events_read == 1


def test_the_idle_clock_restarts_whenever_events_arrive():
    # three polls one second apart carry events; the timeout is only measured from the last of them
    batches = [[delivery("a", 1)], [delivery("a", 2)], [delivery("a", 3)]]
    result, _, _ = read(batches, ReadPlan(idle_timeout_seconds=2.5))
    assert result.events_read == 3
