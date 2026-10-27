# JSON

## What it is

JSON (JavaScript Object Notation) is a plain-text format for structured data. A JSON value is a number, a piece of text in double quotes, `true`, `false`, `null`, a list in square brackets, or an object in curly braces made of `"name": value` pairs. Almost every language can read and write it.

## Why this repository uses it

An order event has to cross the network as bytes. JSON is the lesson's chosen format: easy to read in a terminal and to produce from any language.

## Where it appears

| Path | What it does with JSON |
| --- | --- |
| `src/order_events/infrastructure/codec.py` | Turns an `OrderEvent` into JSON bytes and back. |
| Kafka records | Each record's value is one JSON object. |
| `tests/unit/infrastructure/test_codec.py` | Checks the round trip and the rejection of bad input. |

## How it works underneath

Python's `json.dumps(object)` produces text; `.encode()` turns the text into bytes (UTF-8, the usual way to store text as bytes). Reading reverses it: `json.loads(bytes)` accepts bytes and returns Python dictionaries, lists, text and numbers. Nothing in JSON says which fields are required or what type they have. The program must check that itself.

## Walkthrough of this repository's use

```python
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
```

- `encode_event` builds a dictionary with the four field names the lesson uses (`orderId`, `type`, `sequence`, `ts`) and returns its JSON as bytes. A real record looks like `{"orderId": "order-00042", "type": "OrderPaid", "sequence": 2, "ts": 1793043251.3}`.
- `decode_event` parses and then builds an `OrderEvent`. `str(...)`, `EventType(...)`, `int(...)`, `float(...)` convert and check each field. `EventType("OrderRefunded")` fails because that is not one of the three allowed names.
- The `except` line catches the ways this can go wrong: invalid JSON (`ValueError`), a missing field (`KeyError`), or a wrong shape such as a list instead of an object (`TypeError`). Every one becomes the same named error `MalformedEventError`, so the caller handles one case. `except MalformedEventError: raise` lets the event's own check pass through unchanged.

## Try it yourself

1. `.venv/bin/python -c "from order_events.domain.events import *; from order_events.infrastructure.codec import *; print(encode_event(lifecycle_events('order-00042', 1.5)[1]))"` prints `b'{"orderId": "order-00042", "type": "OrderPaid", "sequence": 2, "ts": 1.5}'`.
2. `.venv/bin/python -c "from order_events.infrastructure.codec import decode_event; decode_event(b'[]')"` ends with `MalformedEventError: not an order event: ...`.
3. With Kafka running, `make order ORDER=order-00042` prints the three stored events as JSON lines.

## Mistakes people make

- Trusting parsed JSON without checking fields. Here every field is converted and checked.
- Forgetting that JSON has no date type or decimal type. This repository uses a float for the timestamp (seconds since 1970) and no money fields.
- Printing bytes and text as if they were the same. `b'...'` is bytes.

## Check yourself

1. What is the difference between `str` and `bytes`? *Text versus raw data; `.encode()` converts text to bytes.*
2. What does `json.loads` return for `{"a": 1}`? *A Python dictionary.*
3. What happens to a record whose `type` is `OrderRefunded`? *`decode_event` raises `MalformedEventError`; the consumer logs it and skips the record.*
4. Why does the decoder catch three kinds of error? *Bad JSON, missing fields and wrong shapes are different failures with one meaning: not a valid event.*
5. Which fields does an event have? *`orderId`, `type`, `sequence`, `ts`.*

## Further reading

- JSON standard (ECMA-404): https://ecma-international.org/publications-and-standards/standards/ecma-404/
- Python 3.12 `json` module: https://docs.python.org/3.12/library/json.html
