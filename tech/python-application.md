# Python: the application layer, file by file

## What it is

The folder `src/order_events/application/` holds the **use cases**: the things the program does, written as plain functions. "Produce 1,000 orders", "read the stream and check the order", "work out the lag", "check the health". A use case does not know about Kafka. It only knows the **ports**, small descriptions of what it needs from the outside world ("something that can publish an event", "something that can poll events"). The real Kafka code (in `infrastructure/`) and the fake in-memory versions used in tests both fit those descriptions.

This layer may import from `domain/` and from the standard library. It may not import `kafka`. `make lint` checks that.

Words used below, explained once here and again where they first appear:

- A **function** is a named piece of code that takes inputs (**parameters**) and gives back an output (its **return value**).
- A **type hint** is a note such as `count: int` saying what kind of value is expected. Python ignores hints while running; `mypy` reads them.
- A **dataclass** is a class whose repetitive methods (how to build it, compare it, print it) Python writes for you from a list of fields; `frozen=True` makes it read-only.

## Why this repository uses it

The use cases are what the program does: produce orders, read them and check the order, work out lag, check health. Writing them against small descriptions (ports) of what they need, and not against `kafka-python`, means a test can run the whole produce or read loop with in-memory fakes, with a fake clock, in milliseconds, and the real Kafka code can change without touching them.

## Where it appears

| File | What it holds |
| --- | --- |
| `application/__init__.py` | Marks the folder as a package. |
| `application/ports.py` | The four descriptions (ports) and the `Delivery` record. |
| `application/produce_orders.py` | The use case "publish the events of N orders". |
| `application/read_orders.py` | The use case "read the stream as a group member and check the order". |
| `application/group_lag.py` | The use case "how far behind is the group". |
| `application/health.py` | The use case "ask every dependency if it works". |

## How it works underneath

A **port** is written as a `Protocol` class: a list of method names and signatures. Python does not check at run time that an object has them; `mypy` checks, before the program runs, that whatever is passed where a port is expected has the right methods (so an adapter or a fake needs no "implements" word). A use case is an ordinary function that receives its collaborators as parameters, including the clock, the sleep function and the stop test as small functions (**dependency injection**), so a test passes fakes and a real program passes `time.time` and `time.sleep`. Control flow is plain Python: loops, `try`/`except`, early `return`. The rule that this layer imports only the domain and the standard library is checked by `import-linter`. Here `produce_orders` and `read_orders` take `pause`, `sleep`, `clock` and `stop` as parameters, which is how a test finishes a loop that would otherwise wait for real time.

## Walkthrough of this repository's use

Every source file of this layer is read below, in the order of the table above. Each block is a piece of the real file, copied exactly, followed by an explanation in plain words.

### `application/__init__.py`

```python
"""Use cases and the ports they need. Depends on the domain only."""
```

Only a docstring. Its presence turns the folder into a package, and its sentence states the layer's rule: it depends on the domain only.

### `application/ports.py`

A **port** is a description of what a use case needs, written without saying how it is done. The word comes from the "ports and adapters" design (also called hexagonal architecture): the use cases sit in the middle, the ports are the sockets on their wall, and **adapters** (in `infrastructure/`) are the plugs that fit the sockets. You can swap plugs without touching the middle.

```python
"""The ports the use cases need. Infrastructure adapters implement them; tests use fakes."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from ..domain.events import OrderEvent


@dataclass(frozen=True, slots=True)
class Delivery:
    """An event as read from the log, with where it was read."""

    event: OrderEvent
    partition: int
    offset: int
```

- Line 5: `Mapping` (a read-only dictionary-like value) and `Sequence` (an ordered, indexable collection such as a list or tuple) are hint names.
- Line 7: `Protocol` is the key import. A class that inherits from `Protocol` describes a **shape**: the methods something must have. Any class that has those methods fits, without having to say so. (This is called **structural typing**, or "duck typing with checking": if it walks like a duck and quacks like a duck, `mypy` accepts it as a duck.)
- Line 9: `from ..domain.events import OrderEvent`. The two dots mean "go up one package, then into `domain`": from `order_events.application` up to `order_events`, then `domain.events`.
- Lines 12 to 17: `Delivery` is a frozen dataclass: an event as it was read from the log, with where it was read (`partition`, and `offset`, the record's number inside that partition). It is not part of the domain because the domain does not know about partitions of a log.

```python
class EventPublisher(Protocol):
    """Sends order events to the log."""

    def publish(self, event: OrderEvent, key: str | None) -> None:
        """Send one event. `key` decides the partition; None lets the client choose."""

    def flush(self) -> None:
        """Block until every published event is acknowledged."""
```

- Line 21: `EventPublisher` is a protocol. Whatever sends events must offer two methods.
- Line 24: `publish(self, event, key)`. `key: str | None` means "text, or no value". The docstring on line 25 says what it means: the key decides the partition; `None` lets the client choose.
- Lines 24 to 28: the method bodies contain only a docstring. In a Protocol, that is the whole body: there is nothing to run, because the protocol only *describes*.
- Line 27: `flush()` blocks (waits) until every published event is acknowledged by Kafka. The producer collects events in a buffer and sends them in batches, so before a program ends it must flush or the last events may be lost.

```python
class EventSource(Protocol):
    """Reads order events from the log as a member of a consumer group."""

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        """Return the next deliveries, or an empty sequence when none arrived in time."""

    def close(self) -> None:
        """Commit progress and leave the consumer group."""


class OffsetReader(Protocol):
    """Reads log-end and committed offsets without joining a group."""

    def end_offsets(self, topic: str) -> Mapping[int, int]:
        """Log-end offset per partition."""

    def committed_offsets(self, group_id: str, topic: str) -> Mapping[int, int | None]:
        """The group's committed offset per partition, None where it never committed."""
```

- Lines 31 to 38: `EventSource`. `poll(timeout_seconds)` returns the next `Delivery` objects, or an empty sequence if none came in time. `close()` commits progress and leaves the group. A **consumer group** is a set of consumers that share a topic's partitions; **committing** means telling Kafka "I have processed up to here".
- Lines 41 to 48: `OffsetReader` reads two kinds of numbers without joining a group: the log-end offset per partition (the number the next new record will get) and the group's committed offset per partition (or `None` where the group never committed).

```python
class HealthProbe(Protocol):
    """A dependency check that reports a short detail or fails with ProbeFailedError."""

    name: str

    def check(self) -> str:
        """Return a short detail on success; raise `ProbeFailedError` when the dependency is not usable."""
```

- Lines 51 to 57: `HealthProbe` has a **class attribute** `name: str` (every probe must have a name) and a method `check()` that returns a short detail text on success and raises `ProbeFailedError` otherwise. `ProbeFailedError` is defined in `health.py` next to it.

### `application/produce_orders.py`

#### The plan

```python
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

```

- Lines 5 to 10: imports. `random` and `Callable` are for hints; `interleave` and `order_id_for` come from the domain; `EventPublisher` from the ports file in this folder (`.ports` means "the file `ports` in this same package").
- Lines 13 to 22: `ProducePlan` is a frozen dataclass holding everything the caller can choose: how many `orders`; the number to `start` at (default 1); the id `prefix` (default `"order"`); whether `keyed` (default true: use the order id as the Kafka key); the `window` of open orders (default 50); and `max_pause_seconds` (default 0.0, meaning no pause).
- A field with `= value` has a **default**, so the caller may leave it out. Fields without a default must come first.
- Lines 24 to 26: `__post_init__` runs right after the object is built and refuses nonsense. `or` joins conditions: if any one is true, the whole line is true. `{self}` inside the f-string prints the plan, so the message shows the bad values.

#### The function

```python
def produce_orders(
    publisher: EventPublisher,
    plan: ProducePlan,
    rng: random.Random,
    clock: Callable[[], float],
    pause: Callable[[float], None],
    stop: Callable[[], bool] = lambda: False,
) -> int:
    """Publish every event of `plan.orders` orders and return how many events were sent.

    Keyed events use the order id as the key, so one order's events share a partition. Unkeyed events
    are the failure mode the lesson demonstrates. When `stop()` turns true (a shutdown signal), the
    events already published are flushed and the count so far is returned.
    """
    order_ids = (order_id_for(number, plan.prefix) for number in range(plan.start, plan.start + plan.orders))
    sent = 0
    for event in interleave(order_ids, plan.window, rng, clock):
        if stop():
            break
        publisher.publish(event, event.order_id if plan.keyed else None)
        sent += 1
        if plan.max_pause_seconds:
            pause(rng.uniform(0, plan.max_pause_seconds))
    publisher.flush()
    return sent
```

- Lines 29 to 36: six parameters. `publisher` is any `EventPublisher`. `plan` is the plan above. `rng` is the random-number generator. `clock` returns the time. `pause` is a function that waits for the given number of seconds (the real one is `time.sleep`; a test passes one that records the call and does not wait). `stop` is a function returning true when the program should stop early; its default `lambda: False` is a **lambda**, a tiny nameless function written on one line, that always answers `False`.
- All time and randomness arrive as parameters, none are created inside. That is **dependency injection** again, and it makes the function fully testable without sleeping.
- Line 43: a **generator expression** in round brackets builds the order ids one by one, only when asked: `order_id_for(number, plan.prefix) for number in range(...)`. `range(a, b)` counts from `a` up to `b - 1`.
- Line 44: `sent = 0` is a counter.
- Line 45: `for event in interleave(...)` loops over the events the generator hands out.
- Lines 46 and 47: before sending each event, ask `stop()`. If it says yes (a shutdown signal arrived), `break` leaves the loop.
- Line 48: `publisher.publish(event, event.order_id if plan.keyed else None)`. The part `A if condition else B` is a one-line choice. Keyed: the key is the order id, so every event of one order goes to the same partition. Unkeyed: the key is `None`, which is the failure mode the lesson demonstrates.
- Line 49: `sent += 1` adds one to the counter.
- Lines 50 and 51: if a pause was asked for (`0.0` counts as false), wait a random time between 0 and the maximum (`rng.uniform(0, x)`).
- Line 52: after the loop, whether it finished or was stopped, `publisher.flush()` waits until everything sent has been acknowledged.
- Line 53: return the count.

### `application/read_orders.py`

#### The two small records

```python
"""Use case: read the order stream as one member of a group and check every order's sequence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from ..domain.ordering import OrderChecker, OrderingReport
from .ports import Delivery, EventSource


@dataclass(frozen=True, slots=True)
class ReadPlan:
    """How a consumer reads: when to stop, how much artificial work per event, which orders to check."""

    idle_timeout_seconds: float | None = None  # None: run until `stop` says so
    work_delay_seconds: float = 0.0  # artificial work per event, to build lag on purpose
    order_prefix: str = ""  # only check orders whose id starts with this
    poll_seconds: float = 0.5


@dataclass(frozen=True, slots=True)
class ReadResult:
    """What a read found: events read, the ordering report and the histories of the first orders."""

    events_read: int
    report: OrderingReport
    first_histories: tuple[tuple[str, tuple[int, ...], tuple[str, ...]], ...]

```

- Lines 12 to 19: `ReadPlan`, a frozen dataclass of choices, each with a default. `idle_timeout_seconds: float | None = None` means "a number of seconds, or `None`, meaning run until told to stop". The `#` after each field is a **comment**: text Python ignores. `work_delay_seconds` is artificial work per event; the lag demo sets it to build a backlog on purpose. `order_prefix` limits the check to orders whose id starts with it (so a second run's ids do not mix with a first). `poll_seconds` is how long one poll waits.
- Lines 22 to 28: `ReadResult` holds the number of events read, the ordering report from the domain, and `first_histories`, a tuple of the first few orders, each as (order id, tuple of partitions, tuple of event type texts). The long hint is only a description of that shape.

#### The function

```python
def read_orders(
    source: EventSource,
    plan: ReadPlan,
    *,
    stop: Callable[[], bool],
    clock: Callable[[], float],
    sleep: Callable[[float], None],
    on_out_of_order: Callable[[Delivery], None],
    histories: int = 0,
) -> ReadResult:
    """Poll until `stop()` or until `plan.idle_timeout_seconds` pass without a matching event."""
```

- Line 34: the lone `*` in the parameter list means every parameter after it must be passed **by name**: `stop=...`, `clock=...`. Callers write `read_orders(source, plan, stop=stop, clock=clock, ...)`. That removes any confusion about the order of five similar-looking functions. These are **keyword-only parameters**.
- Lines 35 to 38: `stop` (are we done?), `clock` (the time), `sleep` (wait), `on_out_of_order` (a function to call with the delivery whenever an event is out of order; the real one writes a log line). `histories` is how many orders' histories to keep (default 0).

```python
    checker = OrderChecker()
    events_read = 0
    last_event_at = clock()
    while not stop():
        deliveries = source.poll(plan.poll_seconds)
        for delivery in deliveries:
            if not delivery.event.order_id.startswith(plan.order_prefix):
                continue
            if not checker.observe(delivery.event, delivery.partition):
                on_out_of_order(delivery)
            events_read += 1
            if plan.work_delay_seconds:
                sleep(plan.work_delay_seconds)
        if deliveries:
            last_event_at = clock()
        elif plan.idle_timeout_seconds is not None and clock() - last_event_at > plan.idle_timeout_seconds:
            break
    first = tuple(
        (order_id, checker.partitions_of(order_id), checker.history(order_id))
        for order_id in checker.first_orders(histories)
    )
    return ReadResult(events_read, checker.report(), first)
```

- Line 42: a fresh `OrderChecker` from the domain.
- Line 44: remember when the last matching event came: `clock()` now.
- Line 45: `while not stop():` repeats until a stop is requested.
- Line 46: `source.poll(plan.poll_seconds)` returns the next batch, or an empty one after the wait.
- Lines 47 to 54: for each delivery in the batch, skip it if its order id does not start with the wanted prefix (`continue` jumps to the next delivery); otherwise give it to the checker (which returns false when it was out of order, and then `on_out_of_order` is called), add one to `events_read`, and if work delay is set, sleep for that long.
- Lines 55 and 56: if the batch was not empty, note the time.
- Lines 57 and 58: otherwise (`elif`), if an idle timeout is set and more than that many seconds have passed since the last event, `break`: the stream has gone quiet, so stop. `make run` uses this to end after reading the 3,000 events.
- Lines 59 to 62: build the tuple of histories for the first `histories` orders, using a generator expression.
- Line 63: return the result with the checker's report.

### `application/group_lag.py`

```python
"""Use case: the lag of a consumer group on a topic."""

from __future__ import annotations

from ..domain.lag import compute_lag
from .ports import OffsetReader


def group_lag(reader: OffsetReader, group_id: str, topic: str) -> dict[int, int]:
    """The lag per partition of a consumer group on a topic."""
    return compute_lag(reader.end_offsets(topic), reader.committed_offsets(group_id, topic))
```

The whole use case is one line (line 11): ask the reader for the end offsets and the committed offsets, hand both to the domain's `compute_lag`, return the answer. It is a function of three values: the reader (any `OffsetReader`), the group id and the topic. The use case exists so the command-line program does not have to know which two calls to make.

### `application/health.py`

```python
"""Use case: ask every dependency whether it is usable."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .ports import HealthProbe


class ProbeFailedError(Exception):
    """A dependency is not usable; the message says why."""


@dataclass(frozen=True, slots=True)
class ProbeResult:
    """The outcome of one health probe."""

    name: str
    ok: bool
    detail: str


@dataclass(frozen=True, slots=True)
class HealthReport:
    """The outcomes of every probe; healthy only when all of them are."""

    results: tuple[ProbeResult, ...]

    @property
    def healthy(self) -> bool:
        return all(result.ok for result in self.results)

```

- Line 11: `class ProbeFailedError(Exception):` is a named error built on Python's most general error class. A probe raises it when its dependency is not usable, and the message says why.
- Lines 15 to 21: `ProbeResult` records a probe's name, whether it worked, and the detail text.
- Lines 24 to 32: `HealthReport` holds the tuple of results. The `healthy` property uses `all(...)`, which is true only when every item of the generator is true. With no results, `all` is true, so an empty report counts as healthy.

```python
def check_health(probes: Iterable[HealthProbe]) -> HealthReport:
    """Run every probe and collect the outcomes; a failed probe is reported, not raised."""
    results = []
    for probe in probes:
        try:
            results.append(ProbeResult(probe.name, True, probe.check()))
        except ProbeFailedError as error:
            results.append(ProbeResult(probe.name, False, str(error)))
    return HealthReport(tuple(results))
```

- Line 35: the function accepts any collection of probes.
- Line 37: `results = []` starts an empty list.
- Lines 38 to 42: for each probe, `try:` runs the check. If it returns, add a result marked `True` with the detail it returned. If `ProbeFailedError` is raised, the `except` block catches it and adds a result marked `False` with the error's text (`str(error)`). A failed probe is *reported*, not allowed to crash the check: the command prints every probe's state, not only the first failure. Any other kind of error (a real bug) is not caught and still stops the program, which is what you want.
- Line 43: return the report.

## Try it yourself

```bash
.venv/bin/python -c "
import random
from order_events.application.produce_orders import ProducePlan, produce_orders

class Recorder:
    def __init__(self): self.sent = []
    def publish(self, event, key): self.sent.append((event.order_id, event.sequence, key))
    def flush(self): pass

recorder = Recorder()
count = produce_orders(recorder, ProducePlan(orders=2, window=2), random.Random(1), lambda: 0.0, lambda s: None)
print(count)
for row in recorder.sent: print(row)
"
```

Expected: `6`, then six tuples. Each order's sequences appear as 1, 2, 3 in that order while the two orders are mixed, and the key equals the order id. The `Recorder` has the two methods of the `EventPublisher` protocol and never says it implements anything: that is the structural typing described above.

## Mistakes people make

- Forgetting `flush()` in a producer: the last events stay in the buffer and the program exits without them.
- Using a mutable default such as `def f(items=[])`: the list is shared by every call. This repository uses `None` or `field(default_factory=...)` instead.
- Catching `Exception` everywhere to "make it robust": it hides real bugs. `check_health` catches only `ProbeFailedError`.

## Check yourself

1. Why does `produce_orders` take `pause` as a parameter instead of calling `time.sleep`? (A test can pass a fake and not wait.)
2. What does the `*` in `read_orders`'s parameter list do? (Forces the following parameters to be passed by name.)
3. What does an empty `HealthReport` say about health, and why? (Healthy, because `all` of nothing is true.)
4. What does `read_orders` do when the idle timeout passes with no matching event? (It leaves the loop with `break` and returns the result.)
5. Why does `check_health` catch only `ProbeFailedError`? (A failed probe is an expected answer to report; any other error is a bug and should stop the program.)

## Further reading

- PEP 544, Protocols (structural typing): https://peps.python.org/pep-0544/
- Python 3.12 `typing`: https://docs.python.org/3.12/library/typing.html
- Python 3.12 `dataclasses`: https://docs.python.org/3.12/library/dataclasses.html
- Python 3.12 `logging`: https://docs.python.org/3.12/library/logging.html
