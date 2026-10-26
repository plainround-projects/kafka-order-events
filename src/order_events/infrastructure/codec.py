"""The JSON wire format of an order event, with validation on the way in."""

from __future__ import annotations

import json

from ..domain.errors import MalformedEventError
from ..domain.events import EventType, OrderEvent


def encode_event(event: OrderEvent) -> bytes:
    """The JSON bytes of an order event."""
    payload = {"orderId": event.order_id, "type": event.type.value, "sequence": event.sequence, "ts": event.ts}
    return json.dumps(payload).encode()


def decode_event(raw: bytes) -> OrderEvent:
    """Parse a record value. Raises MalformedEventError for anything that is not a valid event."""
    try:
        data = json.loads(raw)
        return OrderEvent(
            order_id=str(data["orderId"]),
            type=EventType(data["type"]),
            sequence=int(data["sequence"]),
            ts=float(data["ts"]),
        )
    except MalformedEventError:
        raise
    except (ValueError, KeyError, TypeError) as error:
        raise MalformedEventError(f"not an order event: {error}") from error
