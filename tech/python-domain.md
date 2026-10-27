# Python: the domain layer, file by file

## What this note covers

The folder `src/order_events/domain/` holds the **business rules** of the project: what an order event is, how to check that events arrive in order, and how to compute lag. "Business rules" here means rules that would still be true if you threw Kafka away. This is the innermost layer, and the one rule for it is: it may import only from Python's own standard library (the set of modules that ship with Python). `make lint` fails if a file here imports `kafka`.

This note walks through **every file in that folder**, top to bottom, with every line quoted and explained. If you have never programmed in Python before, read `tech/python.md` first for the general ideas; each idea is also explained again right here, at the line where it first appears, so you can read this note on its own.

Two words you need before starting:

- A **module** is one file whose name ends in `.py`. A **package** is a folder of modules that contains a file called `__init__.py`. Here `order_events` is the package, and `order_events.domain` is a package inside it.
- A **docstring** is a piece of text in triple quotes (`"""..."""`) as the first thing in a file, class or function. Python keeps it so tools and people can read what the thing is for. It does nothing when the program runs.

## The files

| File | What it holds |
| --- | --- |
| `src/order_events/__init__.py` | The package's name tag and version. |
| `src/order_events/domain/__init__.py` | Marks the folder as a package. |
| `src/order_events/domain/errors.py` | One named error. |
| `src/order_events/domain/events.py` | The order event, its three types, and the function that mixes many orders' events together. |
| `src/order_events/domain/ordering.py` | The checker that proves each order's events arrived in sequence. |
| `src/order_events/domain/lag.py` | The one-line lag calculation. |

## `src/order_events/__init__.py`

```python
"""Order events on Kafka: a keyed producer, a consumer group that checks ordering, and lag reading."""

__version__ = "0.1.0"
```

- Line 1 is the docstring: one sentence saying what the whole package does.
- Line 3 creates a variable named `__version__` and gives it the text `"0.1.0"`. A **variable** is a name that points at a value. The double underscores on both sides are a convention meaning "this name is read by tools, not by everyday code". `0.1.0` is a version number in the usual "major.minor.patch" form. The same number is written in `pyproject.toml`.
- Nothing else is here. Python runs this file the first time any module of the package is imported, so the file also marks `order_events` as a package.

## `src/order_events/domain/__init__.py`

```python
"""Pure business rules: events, ordering, lag. Depends on nothing outside the standard library."""
```

The file contains only a docstring. A file with this name tells Python "this folder is a package", which is what lets other files write `from order_events.domain.events import OrderEvent`. The docstring also states the layer's rule in one sentence: no imports from outside the standard library. The other three layer folders (`application`, `infrastructure`, `interfaces`) have an `__init__.py` of the same kind.

## `src/order_events/domain/errors.py`

```python
"""Named errors of the domain."""


class MalformedEventError(ValueError):
    """A record's bytes or fields do not describe a valid order event."""
```

- Line 1: the docstring.
- Line 4: `class MalformedEventError(ValueError):` creates a new kind of error. A **class** is a recipe for making objects; the name in brackets after it, `ValueError`, is the class this one is built on (its **parent**). `ValueError` is one of Python's built-in error types and means "the value has the right type but is not acceptable". Because `MalformedEventError` is built on it, any code that already catches `ValueError` also catches this one.
- Line 5: the docstring says when it is used: a record's bytes or fields do not describe a valid order event.
- The class has no other lines. It does not need any: its value is its **name**. When a log line says `MalformedEventError`, a reader knows exactly what went wrong, instead of seeing a vague `ValueError`. Naming errors this way is one of the "production-grade" rules of this repository.

## `src/order_events/domain/events.py`

This file has the most ideas. It is read in six pieces.

### The imports

```python
"""Order events and the pure functions that build and interleave them."""

from __future__ import annotations

import random
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from enum import StrEnum

from .errors import MalformedEventError
```

- Line 1: the docstring.
- Line 3: `from __future__ import annotations` tells Python to keep **type hints** (the `: str` and `-> None` parts you will see later) as plain text and not to work them out while the file loads. A type hint is a note saying what kind of value you expect. Python itself ignores hints when running; the tool `mypy` reads them to find mistakes before the program runs. Keeping them as text lets a hint mention a class that is defined further down the file.
- Line 5: `import random` loads Python's random-number module. Here it is only used to describe a type (`random.Random`); the function never makes its own random numbers, it is *given* a random-number generator by its caller. That choice is what makes the code testable (see `interleave` below).
- Line 6: `from collections.abc import Callable, Iterable, Iterator` loads three names that describe shapes of values. `Callable` means "something you can call like a function". `Iterable` means "something you can loop over" (a list, a tuple, a generator). `Iterator` means "something that hands out its items one at a time".
- Line 7: `from dataclasses import dataclass` loads a decorator (explained at line 24).
- Line 8: `from enum import StrEnum` loads the base class for a fixed list of named text values (explained at line 13).
- Line 10: `from .errors import MalformedEventError` loads the error from the file next to this one. The single dot means "from this same package". An **import** runs the other module the first time and then lets you use the names it defines.

### The three event types

```python
class EventType(StrEnum):
    """The three events of an order's lifecycle, in the order they happen."""

    PLACED = "OrderPlaced"
    PAID = "OrderPaid"
    SHIPPED = "OrderShipped"


LIFECYCLE: tuple[EventType, ...] = tuple(EventType)
```

- Line 13: `class EventType(StrEnum):` makes a class built on `StrEnum`. An **enumeration** (enum) is a fixed set of named values. A `StrEnum` is an enum whose members are also ordinary text, so `EventType.PLACED == "OrderPlaced"` is true. That is handy because the event type is sent over the network as text.
- Lines 16 to 18: three members. The name on the left (`PLACED`) is what code uses; the text on the right (`"OrderPlaced"`) is what is written to the log. Writing the members in lifecycle order matters, because line 21 relies on it.
- Line 21: `LIFECYCLE: tuple[EventType, ...] = tuple(EventType)`. Reading left to right: `LIFECYCLE` is a name; `: tuple[EventType, ...]` is its type hint, "a tuple whose items are all `EventType`, of any length" (the three dots mean "any number"); `tuple(EventType)` turns the enum into a **tuple**, which is a fixed, read-only list, in the order the members were written. So `LIFECYCLE` is `(PLACED, PAID, SHIPPED)`. Writing the name in capital letters is the convention for a value that never changes.

### The event itself

```python
@dataclass(frozen=True, slots=True)
class OrderEvent:
    """One thing that happened to one order. `sequence` counts from 1 within the order."""

    order_id: str
    type: EventType
    sequence: int
    ts: float

    def __post_init__(self) -> None:
        if not self.order_id:
            raise MalformedEventError("order id is empty")
        if self.sequence < 1:
            raise MalformedEventError(f"sequence must start at 1, got {self.sequence}")
```

- Line 24: `@dataclass(frozen=True, slots=True)`. A line that starts with `@` and sits above a class or function is a **decorator**: it takes the thing below it and returns an improved version. `@dataclass` writes the repetitive methods of a plain data-holding class for you: `__init__` (how to build one), `__eq__` (how to compare two) and `__repr__` (how to print one). `frozen=True` makes the object read-only after it is built, so assigning `event.sequence = 5` raises an error; no part of the program can change an event by accident. `slots=True` stores the fields in a fixed layout, which uses less memory and forbids adding fields that were not declared.
- Lines 28 to 31: the four **fields**. Each line is `name: type`. `order_id: str` is text such as `order-00042`. `type: EventType` is one of the three members above. `sequence: int` is a whole number, counted from 1 inside each order. `ts: float` is a timestamp, a number with a decimal part, in seconds since 1 January 1970 (the usual computer clock).
- Line 33: `def __post_init__(self) -> None:`. A **method** is a function inside a class; its first parameter `self` is the object itself. `__post_init__` is a name the dataclass machinery knows: it is run automatically right after the object is built. `-> None` says it returns nothing.
- Lines 34 and 35: `if not self.order_id:` is true when the text is empty (an empty string counts as "false" in Python). `raise MalformedEventError("order id is empty")` stops the building of the object by throwing the named error. To **raise** an error is to interrupt the program at this point and hand the error to whichever caller has said it will deal with it (with `try` and `except`); if nobody does, the program stops and prints the error.
- Lines 36 and 37: the same for `sequence < 1`. The text starts with `f"` : an **f-string**, which is text containing `{expression}` holes that Python fills in. Here `{self.sequence}` is replaced by the bad value, so the message says `sequence must start at 1, got 0`.

Why do the check here? Because the event is a **value object**: if you hold an `OrderEvent`, it is already known to be valid. No later code has to re-check.

### Building ids and one order's three events

```python
def order_id_for(number: int, prefix: str = "order") -> str:
    """`order-00042`: a zero-padded, prefixed id. A different prefix keeps two runs' ids apart."""
    return f"{prefix}-{number:05d}"


def lifecycle_events(order_id: str, ts: float) -> list[OrderEvent]:
    """The three events of one order, all stamped `ts`."""
    return [OrderEvent(order_id, event_type, sequence, ts) for sequence, event_type in enumerate(LIFECYCLE, start=1)]
```

- Line 40: `def order_id_for(number: int, prefix: str = "order") -> str:`. A function with two **parameters** (inputs). `prefix: str = "order"` gives the second one a **default value**: if the caller omits it, it is `"order"`. `-> str` says it returns text.
- Line 41: the docstring gives an example. A different prefix keeps two runs' ids apart (the end-to-end test uses this).
- Line 42: `return f"{prefix}-{number:05d}"`. The f-string has two holes. `{number:05d}` is a **format specification**: after the colon, `d` means "write as a whole number", `5` means "at least five characters wide", and the leading `0` means "pad with zeros". So `42` becomes `00042` and the result is `order-00042`.
- Line 45: `lifecycle_events(order_id: str, ts: float) -> list[OrderEvent]` returns a **list** (a changeable ordered collection) of events.
- Line 47: `[OrderEvent(order_id, event_type, sequence, ts) for sequence, event_type in enumerate(LIFECYCLE, start=1)]` is a **list comprehension**: a one-line way to build a list. Read it from the `for`: loop over `enumerate(LIFECYCLE, start=1)`. `enumerate` hands out pairs of (counter, item); `start=1` makes the counter begin at 1 instead of 0. For each pair, name the counter `sequence` and the item `event_type`, and build one `OrderEvent` from it. The result is three events with sequences 1, 2, 3 and types PLACED, PAID, SHIPPED, all stamped with the same `ts`.

### Mixing many orders together: `interleave`

```python
def interleave(
    order_ids: Iterable[str],
    window: int,
    rng: random.Random,
    clock: Callable[[], float],
) -> Iterator[OrderEvent]:
    """Yield every event of every order, mixing up to `window` open orders.

    Each order's own events stay in sequence; only events of different orders mix, which is what a
    busy order service sends. Each event is stamped with `clock()` when it is yielded.
    """
```

- Lines 50 to 55: the function takes four inputs. `order_ids: Iterable[str]` is any source of order ids. `window: int` is how many orders may be "open" (started but not finished) at the same time. `rng: random.Random` is a random-number generator that the caller passes in. `clock: Callable[[], float]` is a function that takes no input and returns the current time. The result type `Iterator[OrderEvent]` says it hands out events one at a time.
- Why are `rng` and `clock` *parameters* and not things the function makes for itself? Because then a test can pass a generator with a fixed seed and a fake clock that always returns the same number, and the output becomes exactly predictable. This is called **dependency injection**: instead of reaching out to the world, the function is handed what it needs.
- Lines 56 to 60: the docstring explains the idea. In real life a busy order service handles many orders at once, so events of *different* orders are mixed, while each order's *own* events stay in sequence.

```python
    if window < 1:
        raise ValueError("window must be at least 1")
    pending = iter(order_ids)
    open_orders: list[tuple[str, list[EventType]]] = []

```

- Lines 61 and 62: refuse a window smaller than 1 with the built-in `ValueError`. (There would be no way to make progress.)
- Line 63: `pending = iter(order_ids)` turns the input into an **iterator**, an object remembering its place in the sequence. Each time you ask it for the next item with `next(...)`, it gives one and moves on.
- Line 64: `open_orders` is a list of pairs. Each pair is the order id and the list of event types that order has *not yet* sent. The hint `list[tuple[str, list[EventType]]]` spells that out.

```python
    def refill() -> None:
        while len(open_orders) < window:
            order_id = next(pending, None)
            if order_id is None:
                return
            open_orders.append((order_id, list(LIFECYCLE)))

    refill()
```

- Line 66: `def refill() -> None:` defines a function *inside* the function (a **nested function**). It can see and use `open_orders`, `pending` and `window` from the surrounding function, which is called a **closure**.
- Line 67: `while len(open_orders) < window:` repeats the indented lines as long as fewer than `window` orders are open.
- Line 68: `next(pending, None)` asks for the next order id. The second argument, `None`, is what to return if there is nothing left (otherwise `next` would raise an error). `None` is Python's "no value".
- Lines 69 and 70: if there was no next id, `return` ends the nested function.
- Line 71: otherwise open the order: add the pair (id, a fresh copy of all three event types). `list(LIFECYCLE)` makes a new list each time, so removing items from one order's list does not affect another's.
- Line 73: call `refill()` once to open the first batch of orders.

```python
    while open_orders:
        entry = open_orders[rng.randrange(len(open_orders))]
        order_id, remaining = entry
        sequence = len(LIFECYCLE) - len(remaining) + 1
        yield OrderEvent(order_id, remaining.pop(0), sequence, clock())
        if not remaining:
            open_orders.remove(entry)
            refill()
```

- Line 74: `while open_orders:` keeps going while the list is not empty.
- Line 75: `rng.randrange(len(open_orders))` picks a random whole number from 0 up to (but not including) the number of open orders; indexing the list with it picks one open order at random. `entry` is the pair.
- Line 76: `order_id, remaining = entry` **unpacks** the pair into two names.
- Line 77: the sequence number is how many of the three have already been sent, plus one: `3 - len(remaining) + 1`. With three remaining it is 1; with one remaining it is 3.
- Line 78: `yield OrderEvent(order_id, remaining.pop(0), sequence, clock())`. `remaining.pop(0)` removes and returns the **first** remaining type, so each order's types go out in the lifecycle order. `clock()` stamps the time. `yield` is the key word: it turns this function into a **generator**. Calling `interleave(...)` does not run the body. It returns an object; each time the caller asks for the next item, the body runs until the next `yield`, hands the value out and pauses right there. That lets the producer take events one by one without ever building a list of all 3,000.
- Lines 79 to 81: when an order has sent all three, remove it from the open list and call `refill()` to open another. The loop ends when no orders are open and no ids remain.

The result: each order's events are in sequence; events from up to `window` orders are mixed; and the same seed gives the same mix.

## `src/order_events/domain/ordering.py`

This file answers the lesson's question: *did each order's events arrive in sequence?*

### Imports and the two small result types

```python
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
```

- Lines 5 and 7: the imports. `field` is used below to give each dictionary its own fresh empty value; `OrderEvent` comes from the file just described.
- Lines 10 to 16: `OutOfOrder` is a frozen dataclass (read-only, as explained above) with three fields: which order, which sequence number was expected, and which one actually arrived.
- Lines 19 to 25: `OrderingReport` is another frozen dataclass with the final numbers: how many orders were seen, the tuple of out-of-order events, and how many orders were spread over more than one partition. A **partition** is one of the six ordered logs a Kafka topic is split into.
- Lines 27 to 31: the method `summary` returns the one line that `make run` prints. Two f-strings written next to each other (inside brackets) are joined into one. `len(self.out_of_order)` counts the items.

### The checker

```python
@dataclass
class OrderChecker:
    """Tracks the last sequence per order and the partitions each order was read from."""

    _last_sequence: dict[str, int] = field(default_factory=dict)
    _partitions: dict[str, set[int]] = field(default_factory=dict)
    _history: dict[str, list[str]] = field(default_factory=dict)
    _out_of_order: list[OutOfOrder] = field(default_factory=list)
```

- Line 34: `@dataclass` without arguments: a normal, changeable dataclass (this one has to change as events arrive).
- Lines 38 to 41: four fields, each a container the checker keeps: the last sequence number seen per order (`dict[str, int]`, a **dictionary**: look up a value by its key), the set of partitions per order (`dict[str, set[int]]`; a **set** holds each value at most once), the history of event types per order, and the list of out-of-order events. The leading underscore on each name is a convention for "private to this class; do not touch from outside".
- `field(default_factory=dict)` means "when building a checker, call `dict()` to make a *new* empty dictionary". It must be written this way: a plain `= {}` would share a single dictionary between all checkers, and Python's dataclasses refuse it for that reason.

### Recording one event

```python
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
```

- Line 43: the method takes the event and the partition it was read from, and returns `bool` (true or false).
- Line 45: `self._last_sequence.get(event.order_id, 0)` looks up the order; if the order was never seen, the answer is `0`. So the first event of an order should have sequence `0 + 1 = 1`.
- Line 46: `in_order = event.sequence == previous + 1`. `==` asks "are these equal?" and gives true or false.
- Lines 47 and 48: if not in order, store a record of what was expected and what came.
- Lines 49 to 51: the comment (a line starting with `#`, ignored by Python) explains why line 51 keeps the *highest* sequence seen, using `max(...)`. Suppose events arrive as 1, 3, 2, 4. Event 3 is early: reported (expected 2, got 3). The checker now remembers 3. Event 2 is late: reported too (expected 4, got 2). Event 4 is then in order and is not blamed. So one swap costs two reports, and nothing after it is reported again.
- Line 52: `self._partitions.setdefault(event.order_id, set()).add(partition)`. `setdefault(key, value)` returns the existing value for the key, or stores and returns the given value if the key is new. Then `.add(partition)` puts the partition number in the set. Afterwards the set holds every partition this order was seen on. With a key, the set always has one number; without a key, it often has several.
- Line 53: the same idea for the history list: append the type's text (`event.type.value`).
- Line 54: return whether it was in order.

### Reading the answers back

```python
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
```

- Lines 56 to 58: `@property` is a decorator that lets you write `checker.out_of_order_count` without brackets, as if it were a field, while a function runs underneath.
- Lines 60 and 61: `history` returns a tuple of the event-type texts for one order. `self._history.get(order_id, ())` returns an empty tuple for an unknown order. Returning a tuple, not the inner list, means the caller cannot change the checker's data.
- Lines 63 and 64: `partitions_of` returns the partitions sorted from low to high.
- Lines 66 and 67: `first_orders(count)` returns the first `count` order ids in the order they were first seen. Dictionaries remember insertion order, so `list(self._history)` is the ids in that order, and `[:count]` is a **slice** that takes the first `count` of them.
- Lines 69 to 71: `report` counts orders seen in more than one partition with a **generator expression** inside `sum(...)`: `1 for seen in ... if len(seen) > 1` produces a `1` for each order that qualifies, and `sum` adds them. Then it builds the report.

## `src/order_events/domain/lag.py`

```python
"""Consumer lag: the same numbers `kafka-consumer-groups.sh --describe` prints."""

from __future__ import annotations

from collections.abc import Mapping


def compute_lag(end_offsets: Mapping[int, int], committed: Mapping[int, int | None]) -> dict[int, int]:
    """Lag per partition: the log-end offset minus the committed offset.

    A partition the group never committed on counts from offset 0, which is where a group with
    auto.offset.reset=earliest would start.
    """
    return {partition: end - (committed.get(partition) or 0) for partition, end in end_offsets.items()}
```

**Lag** is how far behind a consumer group is: for each partition, the offset of the end of the log minus the offset the group last committed. An **offset** is the number of a record inside a partition, counted from 0.

- Line 5: `Mapping` is the hint for "something you look values up in by key", such as a dictionary, without saying it can be changed.
- Line 8: the function takes the end offsets (partition number to offset) and the committed offsets (partition number to offset, or `None` if the group never committed on that partition) and returns a dictionary of lag per partition. `int | None` means "a whole number or no value".
- Lines 9 to 13: the docstring says that a partition the group never committed on counts from 0, which is where a group set to start from the earliest record would begin.
- Line 14: a **dictionary comprehension**: `{key: value for ... in ...}` builds a dictionary in one line. For each pair in `end_offsets.items()` it computes `end - (committed.get(partition) or 0)`. `committed.get(partition)` gives the committed offset or `None`; `None or 0` is `0`, because `or` returns its right side when the left side is false-like. (A committed offset of exactly 0 also becomes 0, which is the same number, so nothing is lost.)

## Try it yourself

Run these from the repository root after `make up` has built the virtual environment (`make up` does it; or run `make check` once).

```bash
.venv/bin/python -c "
from order_events.domain.events import order_id_for, lifecycle_events
print(order_id_for(42))
for e in lifecycle_events('order-00042', 0.0): print(e)
"
```

Expected output: `order-00042` and then three lines such as `OrderEvent(order_id='order-00042', type=<EventType.PLACED: 'OrderPlaced'>, sequence=1, ts=0.0)`. The `__repr__` that `@dataclass` wrote is what printed them.

```bash
.venv/bin/python -c "
from order_events.domain.events import OrderEvent, EventType
OrderEvent('', EventType.PLACED, 1, 0.0)
"
```

Expected: the program stops with `order_events.domain.errors.MalformedEventError: order id is empty`.

## Mistakes people make

- Changing a frozen object: `event.sequence = 2` raises `FrozenInstanceError`. Build a new event instead.
- Writing `field(default_factory=dict)` as `= {}`: Python refuses it on purpose, because every object would share the one dictionary.
- Using `or 0` on a number that can legitimately be 0: `0 or 0` is still 0, so it is harmless here, but `x or default` replaces *every* false-like value (0, empty text, empty list), not only `None`. Use `x if x is not None else default` when 0 must stay 0 and the default is not 0.

## Check yourself

1. What does `{number:05d}` print for `7`? (`00007`.)
2. Why is `rng` a parameter of `interleave` and not created inside it? (So a test can control the randomness.)
3. Events of one order arrive as 1, 3, 2, 4. How many out-of-order events does the checker record, and why? (Two: the early 3 and the late 2. It keeps the highest sequence seen, so 4 is in order.)
