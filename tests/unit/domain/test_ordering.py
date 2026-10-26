from order_events.domain.events import EventType, OrderEvent
from order_events.domain.ordering import OrderChecker, OutOfOrder


def event(order_id: str, sequence: int, type_: EventType = EventType.PLACED) -> OrderEvent:
    return OrderEvent(order_id, type_, sequence, 0.0)


def test_events_in_sequence_log_nothing():
    checker = OrderChecker()
    assert all(checker.observe(event("order-1", sequence), partition=3) for sequence in (1, 2, 3))
    assert checker.out_of_order_count == 0
    assert checker.report().orders_split_across_partitions == 0


def test_an_event_that_skips_ahead_is_out_of_order():
    checker = OrderChecker()
    checker.observe(event("order-1", 1), partition=0)
    assert checker.observe(event("order-1", 3), partition=1) is False
    assert checker.report().out_of_order == (OutOfOrder("order-1", expected=2, got=3),)


def test_a_late_event_is_counted_once_and_does_not_flag_the_next_one_again():
    checker = OrderChecker()
    for sequence, partition in ((1, 0), (3, 1), (2, 0)):
        checker.observe(event("order-1", sequence), partition)
    # 3 arrived before 2 (one report); then 2 itself arrives behind the highest seen (second report).
    assert checker.out_of_order_count == 2


def test_orders_are_tracked_independently():
    checker = OrderChecker()
    for order_id, sequence, partition in (("a", 1, 0), ("b", 1, 1), ("a", 2, 0), ("b", 2, 1)):
        checker.observe(event(order_id, sequence), partition)
    assert checker.out_of_order_count == 0


def test_an_order_read_from_two_partitions_is_counted_as_split():
    checker = OrderChecker()
    checker.observe(event("a", 1), 0)
    checker.observe(event("a", 2), 4)
    checker.observe(event("b", 1), 2)
    assert checker.report().orders_split_across_partitions == 1
    assert checker.partitions_of("a") == (0, 4)


def test_history_lists_the_event_types_in_arrival_order_and_unknown_orders_are_empty():
    checker = OrderChecker()
    for sequence, type_ in enumerate((EventType.PLACED, EventType.PAID, EventType.SHIPPED), start=1):
        checker.observe(event("a", sequence, type_), 2)
    assert checker.history("a") == ("OrderPlaced", "OrderPaid", "OrderShipped")
    assert checker.history("missing") == ()
    assert checker.partitions_of("missing") == ()
    assert checker.first_orders(5) == ("a",)


def test_the_summary_line_names_the_three_numbers():
    checker = OrderChecker()
    checker.observe(event("a", 1), 0)
    assert checker.report().summary() == (
        "orders: 1  out-of-order events logged: 0  orders spread over more than one partition: 0"
    )
