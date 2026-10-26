import json

import pytest

from order_events.domain.errors import MalformedEventError
from order_events.domain.events import EventType, OrderEvent
from order_events.infrastructure.codec import decode_event, encode_event


def test_an_event_round_trips_through_json_bytes():
    event = OrderEvent("order-00005", EventType.PAID, 2, 1793043251.3)
    assert decode_event(encode_event(event)) == event


def test_the_wire_format_uses_the_lessons_field_names():
    payload = json.loads(encode_event(OrderEvent("order-1", EventType.SHIPPED, 3, 2.0)))
    assert payload == {"orderId": "order-1", "type": "OrderShipped", "sequence": 3, "ts": 2.0}


@pytest.mark.parametrize(
    "raw",
    [
        b"x",
        b"[]",
        b"{}",
        b'{"orderId": "a", "type": "OrderRefunded", "sequence": 1, "ts": 1}',
        b'{"orderId": "a", "type": "OrderPaid", "sequence": "two", "ts": 1}',
        b'{"orderId": "", "type": "OrderPaid", "sequence": 1, "ts": 1}',
        b'{"orderId": "a", "type": "OrderPaid", "sequence": 0, "ts": 1}',
    ],
)
def test_anything_that_is_not_a_valid_event_is_rejected(raw):
    with pytest.raises(MalformedEventError):
        decode_event(raw)
