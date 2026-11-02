# Python: the tests, file by file

## What it is

Everything under `tests/` is Python written to check the rest of the code. This note walks through **every test file and every test**. How the tests are *started* (the `pytest` command, markers, coverage numbers) is in `tech/pytest.md` and `tech/pytest-cov.md`; here the focus is on what each test proves and how it is written.

Ideas you need first:

- A **test** is a function whose name starts with `test_`. pytest finds such functions in files whose names start with `test_`, calls each one, and marks it passed if it returns without an error and failed if an error is raised.
- **`assert condition`** raises an error if the condition is false. That is how a test says "this must be true". pytest rewrites assertions so a failure shows both sides of `==`.
- **`with pytest.raises(SomeError):`** says "the lines inside must raise this error; the test fails if they do not". `match="text"` also requires the error message to contain that text (it is a regular expression).
- A **fake** (or stand-in) is a simple object that plays the role of something real so the test needs no network. The fakes live in `tests/unit/application/fakes.py`.
- **Unit tests** (in `tests/unit/`) need no Kafka and run in about a second. **Integration tests** (`tests/integration/`) use the real broker from `docker-compose.yml`. **End-to-end tests** (`tests/e2e/`) start the real command-line programs as separate processes.
- The folders of tests are packages (each has an empty `__init__.py`), which is why a test can write `from tests.unit.application.fakes import ...`. The eight empty `__init__.py` files under `tests/` contain nothing at all; their presence is the whole job.

## Why this repository uses it

The lesson's claims (a key keeps an order on one partition, an unkeyed run breaks the order, eight consumers leave two idle, lag grows and drains) are only claims until something checks them. The tests check them at three levels: fast unit tests with fakes, adapter tests on the real broker, and end-to-end tests through the command-line programs.

## Where it appears

| File | What it checks |
| --- | --- |
| `tests/conftest.py` | Shared fixtures for the tests that need the broker. |
| `tests/unit/domain/test_events.py` | Events, ids and the interleaving function. |
| `tests/unit/domain/test_ordering.py` | The order checker. |
| `tests/unit/domain/test_lag.py` | The lag arithmetic. |
| `tests/unit/application/fakes.py` | The in-memory stand-ins (not a test file itself). |
| `tests/unit/application/test_produce_orders.py` | The produce use case. |
| `tests/unit/application/test_read_orders.py` | The read use case. |
| `tests/unit/application/test_group_lag_and_health.py` | The lag and health use cases. |
| `tests/unit/infrastructure/test_codec.py` | JSON encoding and decoding. |
| `tests/unit/infrastructure/test_settings.py` | Environment settings. |
| `tests/unit/infrastructure/test_retry.py` | Retry with backoff. |
| `tests/unit/infrastructure/test_logging_and_shutdown.py` | Log format and the signal flag. |
| `tests/unit/interfaces/test_arguments_and_clis.py` | Argument checks and exit codes. |
| `tests/integration/test_kafka_adapters.py` | The Kafka adapters on a real broker. |
| `tests/e2e/test_lesson_scenario.py` | The whole lesson through the command-line programs. |

## How it works underneath

pytest imports every file named `test_*.py`, calls every function named `test_*`, and counts a call that returns as a pass and a call that raises as a failure. It rewrites plain `assert` statements so a failure shows both sides. A **fixture** is a function marked `@pytest.fixture`; pytest looks at a test's parameter names, runs the fixture of each name first and passes the result in; code after a `yield` in a fixture runs as clean-up after the test. `@pytest.mark.parametrize` turns one function into several test cases. **Marks** such as `integration` and `e2e` label tests so they can be selected with `-m`; `--strict-markers` makes an unknown mark an error. A **fake** is a small class with the methods of a port that records calls, which is why unit tests need no broker. Here the unit tests use a fake clock and a scripted source; the integration tests create a topic with a random name per test and delete it afterwards; the end-to-end tests start the real programs as separate processes with `subprocess`.

## Walkthrough of this repository's use

Every test file is read below, in the order of the table above. Each block is a piece of the real file, copied exactly, followed by an explanation in plain words.

### `tests/conftest.py`: fixtures

```python
"""Fixtures for the tests that need the broker from docker-compose.yml."""

import os
import uuid
from collections.abc import Iterator

import pytest
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from order_events.infrastructure.settings import Settings

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env({"BOOTSTRAP_SERVERS": BOOTSTRAP})


@pytest.fixture
def topic() -> Iterator[str]:
    """A fresh six-partition topic per test, so no test sees another's events."""
    name = f"test-order-events-{uuid.uuid4().hex[:8]}"
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    admin.create_topics([NewTopic(name, num_partitions=6, replication_factor=1)])
    yield name
    admin.delete_topics([name])
    admin.close()
```

**`conftest.py`** is a special file name: pytest loads it automatically and makes the **fixtures** in it available to every test below it. A fixture is a function that prepares something a test needs. A test asks for one by putting its name as a parameter.

- Line 3 to 5: `os` for the environment, `uuid` for random unique ids, `Iterator` for a hint.
- Lines 7 to 9: `pytest`, and from `kafka` the admin client and `NewTopic`, used to create and delete topics.
- Line 13: `BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")`: the broker address from the environment, with the compose default.
- Lines 16 to 18: the `settings` fixture. `@pytest.fixture` turns the function into a fixture. It builds a `Settings` from a dictionary holding only the address, so the real environment does not leak in.
- Lines 21 to 29: the `topic` fixture. It makes a unique topic name with a random 8-character suffix (`uuid.uuid4().hex[:8]`), creates it with six partitions and one replica, then runs `yield name`. A fixture with `yield` runs everything before the `yield` as set-up and gives the value to the test; when the test ends, pytest resumes the function after the `yield`, which is the clean-up: delete the topic and close the admin client. Each test thus gets a fresh topic and cannot see another test's events.

### `tests/unit/domain/test_events.py`

```python
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

```

- Line 17 to 19: `counting_clock` is a helper that returns a fake clock. `iter(range(10_000))` is an iterator over 0 to 9,999; each call of the returned `lambda` gives `start + next(ticks)`: 100.0, then 101.0, then 102.0. Time that moves in known steps makes timestamps predictable.
- Lines 22 to 24: `test_order_ids_are_zero_padded_and_prefixed` checks `order_id_for(42)` is `order-00042` and that a prefix is used.
- Lines 27 to 32: one order has three events, with types equal to `LIFECYCLE`, sequences 1, 2, 3, one order id, and one timestamp. Braces around a comprehension (`{event.ts for event in events}`) make a **set**; a set with one member proves all values were the same.
- Lines 35 to 39: the event refuses an empty id and a sequence of 0. Two `with pytest.raises` blocks.
- Lines 42 to 46: events are immutable. Assigning to a field of a frozen dataclass raises `AttributeError` (the actual class, `FrozenInstanceError`, is a kind of `AttributeError`). The comment `# type: ignore[misc]` tells `mypy` this deliberate wrong assignment is expected. The last line shows two events with the same fields are equal (the `__eq__` that `@dataclass` wrote).

```python
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
```

- Lines 49 to 55: `interleave` over 100 orders yields exactly 300 events and every (order, sequence) pair exactly once. The set built from the events is compared with the set of all expected pairs.
- Lines 58 to 63: for 50 orders, each event's sequence is the previous one for that order plus one, and the event type matches the lifecycle position. `seen.get(event.order_id, 0) + 1` is the expected next sequence.
- Lines 66 to 68: with a window of five, the output really mixes orders. `itertools.pairwise(events)` gives each event with the next one; the test looks for any neighbouring pair that belongs to different orders where the left one is not a last event. That can only happen if orders are interleaved.
- Lines 71 to 73: with a window of one, orders go one after another: `["a-1"] * 3 + ["a-2"] * 3` is the list `a-1, a-1, a-1, a-2, a-2, a-2`.
- Lines 76 to 80: the same seed gives the same order. Two separate runs with `random.Random(9)` give equal lists.
- Lines 83 to 85: timestamps are taken at the moment each event is yielded: `100.0, 101.0, 102.0` from the counting clock.
- Lines 88 to 90: a window of 0 raises `ValueError` mentioning "window". `list(...)` is needed because a generator does not run until something asks it for items.

### `tests/unit/domain/test_ordering.py`

```python
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
```

- Lines 5 and 6: a helper `event(...)` builds an `OrderEvent` with fewer arguments.
- Lines 9 to 13: events 1, 2, 3 in order from one partition: `observe` returns true for each (`all(...)` is true only when every item is), zero out-of-order, zero split orders.
- Lines 16 to 20: event 1 then event 3: `observe` returns `False` for event 3 (`is False` tests it is exactly that value), and the report holds one `OutOfOrder` with expected 2, got 3.
- Lines 23 to 28: the sequence 1, 3, 2 produces **two** reports: the early 3, and the late 2. The comment says so. The loop unpacks pairs `(sequence, partition)`.
- Lines 31 to 35: two orders interleaved, each in order, produce no reports: orders are tracked independently.
- Lines 38 to 44: order `a` seen on partition 0 and 4 counts as split; order `b` on one partition does not; `partitions_of("a")` is `(0, 4)`.
- Lines 47 to 54: the history lists the type texts in arrival order; unknown orders give empty tuples; `first_orders(5)` returns just `("a",)`.
- Lines 57 to 62: the summary line has exactly the expected text.

### `tests/unit/domain/test_lag.py`

```python
from order_events.domain.lag import compute_lag


def test_lag_is_log_end_minus_committed():
    assert compute_lag({0: 100, 1: 80}, {0: 40, 1: 80}) == {0: 60, 1: 0}


def test_a_partition_with_no_commit_counts_from_zero():
    assert compute_lag({0: 25}, {0: None}) == {0: 25}
    assert compute_lag({0: 25}, {}) == {0: 25}
```

- Lines 4 and 5: end minus committed per partition: 100 - 40 = 60 and 80 - 80 = 0.
- Lines 8 to 10: a partition with committed `None`, and one missing from the committed dictionary, both count from zero. The second assertion proves `committed.get(partition) or 0` handles a missing key.

### `tests/unit/application/fakes.py`

```python
"""In-memory stand-ins for the application's ports."""

from collections.abc import Mapping, Sequence

from order_events.application.ports import Delivery
from order_events.domain.events import OrderEvent


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[OrderEvent, str | None]] = []
        self.flushed = 0

    def publish(self, event: OrderEvent, key: str | None) -> None:
        self.published.append((event, key))

    def flush(self) -> None:
        self.flushed += 1


class ScriptedSource:
    """Returns the scripted batches one per poll, then empty polls forever."""

    def __init__(self, batches: Sequence[Sequence[Delivery]]) -> None:
        self._batches = list(batches)
        self.closed = False

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        return self._batches.pop(0) if self._batches else []

    def close(self) -> None:
        self.closed = True


class FakeClock:
    """A clock that moves only when told: `sleep` and every `poll` advance it by a fixed step."""

    def __init__(self, step: float = 1.0) -> None:
        self.now = 0.0
        self.step = step
        self.slept: list[float] = []

    def __call__(self) -> float:
        self.now += self.step
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)


class FixedOffsets:
    def __init__(self, end: Mapping[int, int], committed: Mapping[int, int | None]) -> None:
        self._end, self._committed = end, committed

    def end_offsets(self, topic: str) -> Mapping[int, int]:
        return self._end

    def committed_offsets(self, group_id: str, topic: str) -> Mapping[int, int | None]:
        return self._committed
```

- Lines 9 to 18: `RecordingPublisher` has the two methods of the `EventPublisher` port. It stores what was published as pairs (event, key) in a list, and counts flushes.
- Lines 21 to 32: `ScriptedSource` is given a list of batches; each `poll` returns the next batch, and `[]` forever after. `self._batches.pop(0) if self._batches else []` is a one-line choice. `close` records it was called.
- Lines 35 to 48: `FakeClock` is a clock that moves only when asked. `__call__` is a special method that makes the object callable like a function: `clock()` adds `step` (default 1.0) to `now` and returns it. So every reading of the time advances it by one second. `sleep` does not wait; it records the requested seconds in a list.
- Lines 51 to 59: `FixedOffsets` returns the two offset dictionaries it was built with. `self._end, self._committed = end, committed` assigns two attributes in one line.
- None of these classes names the protocol it fits. They have the right methods, which is enough.

### `tests/unit/application/test_produce_orders.py`

```python
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
```

- Lines 9 to 12: the `run` helper calls `produce_orders` with a fixed random seed (4), a clock that always says 1.0, and `pauses.append` as the pause function (so each requested pause is recorded in the list).
- Lines 15 to 19: 10 orders send 30 events, one flush, and for every event the key equals its order id.
- Lines 22 to 24: unkeyed sends only `None` keys; the set of keys is `{None}`.
- Lines 27 to 29: start 1001 with prefix `run-b` gives exactly the ids `run-b-01001` and `run-b-01002`.
- Lines 32 to 36: with a maximum pause set, one pause per event, each between 0 and 0.05.
- Lines 39 to 42: no maximum means no pauses at all.
- Lines 45 to 48: zero orders sends nothing but still flushes once.

```python
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
```

- Lines 51 to 57: `@pytest.mark.parametrize("kwargs", [...])` runs the same test once for each item in the list, each time passing that dictionary as `kwargs`. The four dictionaries are four bad plans; `ProducePlan(**kwargs)` unpacks a dictionary into named arguments. Each must raise `ValueError` mentioning "invalid produce plan".
- Lines 60 to 68: a stop request. `stops = iter([False, False, False, True])` is an iterator, and `stop=lambda: next(stops)` answers false three times, then true. So three events are sent, three are recorded, and the flush still happens once.

### `tests/unit/application/test_read_orders.py`

```python
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

```

- Lines 7 to 9: helper `delivery(...)` builds a `Delivery` with the event type chosen from the sequence (`list(EventType)[sequence - 1]`).
- Lines 12 to 34: helper `read(...)` runs `read_orders` with a scripted source and a fake clock. Its inner `stop()` counts how many times it was asked (`nonlocal polls` allows changing the outer variable) and returns true after `stop_after_polls` calls, if that was given. It returns the result, the list of out-of-order deliveries (the callback is `out_of_order.append`), and the clock.

```python
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
```

- Lines 37 to 43: in-order events: three read, none out of order, and the first order's history is `("a", (2,), ("OrderPlaced", "OrderPaid", "OrderShipped"))`.
- Lines 46 to 50: an event 3 right after 1 reaches the callback; one out-of-order in the report.
- Lines 53 to 57: only orders whose id starts with `run-1` are counted; the other is skipped.
- Lines 60 to 63: the work delay sleeps once per event, recorded as `[0.01, 0.01]`.
- Lines 66 to 69: with no events and an idle timeout of 2.5, it ends, and the fake clock has passed 2.5 seconds.
- Lines 72 to 74: with no idle timeout it keeps polling until `stop` says so (after five polls).
- Lines 77 to 81: the idle clock restarts whenever events arrive: three batches one second apart do not trigger a 2.5-second timeout.

### `tests/unit/application/test_group_lag_and_health.py`

```python
from order_events.application.group_lag import group_lag
from order_events.application.health import ProbeFailedError, check_health
from tests.unit.application.fakes import FixedOffsets


def test_group_lag_is_end_minus_committed_per_partition():
    offsets = FixedOffsets({0: 100, 1: 50}, {0: 90, 1: None})
    assert group_lag(offsets, "g", "t") == {0: 10, 1: 50}


class Healthy:
    name = "kafka"

    def check(self) -> str:
        return "six partitions"


class Broken:
    name = "postgres"

    def check(self) -> str:
        raise ProbeFailedError("connection refused")


def test_a_report_is_healthy_only_when_every_probe_is():
    assert check_health([Healthy()]).healthy
    report = check_health([Healthy(), Broken()])
    assert not report.healthy
    assert [(result.name, result.ok, result.detail) for result in report.results] == [
        ("kafka", True, "six partitions"),
        ("postgres", False, "connection refused"),
    ]


def test_no_probes_is_healthy():
    assert check_health([]).healthy
```

- Lines 6 to 8: `FixedOffsets({0: 100, 1: 50}, {0: 90, 1: None})` gives lag `{0: 10, 1: 50}`.
- Lines 11 to 22: two tiny probe classes: `Healthy` returns a detail; `Broken` raises `ProbeFailedError`.
- Lines 25 to 32: a report is healthy only if every probe is; the results list shows each probe's name, ok flag and detail.
- Lines 35 and 36: no probes is healthy.

### `tests/unit/infrastructure/test_codec.py`

```python
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
```

- Lines 10 to 12: **round trip**: encoding then decoding gives an equal event.
- Lines 15 to 17: the JSON uses the lesson's field names (`orderId`, `type`, `sequence`, `ts`).
- Lines 20 to 34: seven bad inputs, each of which must raise `MalformedEventError`: not JSON (`b"x"`), JSON of the wrong shape (`b"[]"`, `b"{}"`), an unknown type (`OrderRefunded`), a sequence that is not a number (`"two"`), an empty id, and a sequence of 0. The `b'...'` prefix makes bytes literals.

### `tests/unit/infrastructure/test_settings.py`

```python
import pytest

from order_events.infrastructure.settings import ConfigurationError, Settings


def test_defaults_describe_the_local_compose_setup():
    settings = Settings.from_env({})
    assert settings.bootstrap_servers == "localhost:9092"
    assert (settings.topic, settings.group_id, settings.partitions) == ("order-events", "order-history", 6)
    assert settings.log_level == "INFO"


def test_every_variable_can_be_overridden():
    settings = Settings.from_env(
        {
            "BOOTSTRAP_SERVERS": "a:1,b:2",
            "TOPIC": "t",
            "GROUP_ID": "g",
            "PARTITIONS": "3",
            "LOG_LEVEL": "debug",
            "KAFKA_REQUEST_TIMEOUT_MS": "5000",
            "KAFKA_RETRY_BACKOFF_MS": "50",
        }
    )
    assert (settings.bootstrap_servers, settings.topic, settings.group_id, settings.partitions) == (
        "a:1,b:2",
        "t",
        "g",
        3,
    )
    assert (settings.log_level, settings.request_timeout_ms, settings.retry_backoff_ms) == ("DEBUG", 5000, 50)


@pytest.mark.parametrize(
    "environ",
    [
        {"BOOTSTRAP_SERVERS": "no-port"},
        {"BOOTSTRAP_SERVERS": ""},
        {"TOPIC": "  "},
        {"PARTITIONS": "0"},
        {"PARTITIONS": "six"},
        {"LOG_LEVEL": "LOUD"},
        {"KAFKA_REQUEST_TIMEOUT_MS": "-1"},
    ],
)
def test_an_unusable_value_is_rejected_with_the_variable_name(environ):
    with pytest.raises(ConfigurationError, match=next(iter(environ))):
        Settings.from_env(environ)


def test_settings_read_the_process_environment_by_default(monkeypatch):
    monkeypatch.setenv("TOPIC", "from-the-process")
    assert Settings.from_env().topic == "from-the-process"
```

- Lines 6 to 10: defaults match the compose setup.
- Lines 13 to 31: every variable can be overridden; `"debug"` becomes `"DEBUG"`.
- Lines 34 to 48: seven unusable settings are rejected with a message containing the variable's name (`match=next(iter(environ))` takes the first key of the dictionary).
- Lines 51 to 53: `monkeypatch` is a built-in fixture that changes the environment for one test and puts it back afterwards; `monkeypatch.setenv("TOPIC", ...)` proves `Settings.from_env()` with no argument reads the real process environment.

### `tests/unit/infrastructure/test_retry.py`

```python
import random

import pytest

from order_events.infrastructure.retry import retry


class Flaky:
    def __init__(self, failures: int) -> None:
        self.failures, self.calls = failures, 0

    def __call__(self) -> str:
        self.calls += 1
        if self.calls <= self.failures:
            raise ConnectionError("down")
        return "ok"


def test_it_returns_the_first_success_without_sleeping():
    pauses: list[float] = []
    assert retry(Flaky(0), attempts=3, base_delay_seconds=1, retry_on=(ConnectionError,), sleep=pauses.append) == "ok"
    assert pauses == []


def test_it_retries_with_doubling_pauses_inside_the_jitter_band():
    pauses: list[float] = []
    operation = Flaky(3)
    result = retry(
        operation,
        attempts=5,
        base_delay_seconds=1,
        retry_on=(ConnectionError,),
        sleep=pauses.append,
        rng=random.Random(1),
    )
    assert result == "ok"
    assert operation.calls == 4
    for pause, ceiling in zip(pauses, (1, 2, 4), strict=True):
        assert ceiling * 0.5 <= pause <= ceiling


def test_the_pause_is_capped():
    pauses: list[float] = []
    retry(
        Flaky(4),
        attempts=6,
        base_delay_seconds=10,
        retry_on=(ConnectionError,),
        sleep=pauses.append,
        max_delay_seconds=15,
    )
    assert max(pauses) <= 15


def test_the_last_failure_is_raised_after_the_final_attempt():
    operation = Flaky(10)
    with pytest.raises(ConnectionError):
        retry(operation, attempts=3, base_delay_seconds=0, retry_on=(ConnectionError,), sleep=lambda _: None)
    assert operation.calls == 3


def test_other_errors_are_not_retried():
    def broken() -> str:
        raise KeyError("x")

    with pytest.raises(KeyError):
        retry(broken, attempts=3, base_delay_seconds=0, retry_on=(ConnectionError,), sleep=lambda _: None)


def test_each_retry_is_reported():
    seen: list[int] = []
    retry(
        Flaky(2),
        attempts=4,
        base_delay_seconds=0,
        retry_on=(ConnectionError,),
        sleep=lambda _: None,
        on_retry=lambda attempt, _: seen.append(attempt),
    )
    assert seen == [1, 2]


def test_at_least_one_attempt_is_required():
    with pytest.raises(ValueError, match="attempts"):
        retry(Flaky(0), attempts=0, base_delay_seconds=0, retry_on=(ConnectionError,))
```

- Lines 8 to 16: `Flaky` is a callable object that fails a set number of times and then returns `"ok"`; `__call__` makes `Flaky(3)()` work like a function.
- Lines 19 to 22: no failure, no pause.
- Lines 25 to 39: three failures, then success on the fourth call; the recorded pauses sit between half of, and the whole of, 1, 2 and 4 seconds. `zip(..., strict=True)` pairs the two lists and raises an error if their lengths differ.
- Lines 42 to 52: the cap of 15 seconds holds although the formula would give 10, 20, 40, 80.
- Lines 55 to 59: if every attempt fails, the last error is raised and there were exactly three calls.
- Lines 62 to 67: a `KeyError` is not in `retry_on`, so it passes through at once.
- Lines 70 to 80: `on_retry` is called with attempts 1 and 2.
- Lines 83 to 85: zero attempts raises `ValueError`.

### `tests/unit/infrastructure/test_logging_and_shutdown.py`

```python
import io
import logging
import signal

from order_events.infrastructure.logging import configure_logging, fields
from order_events.infrastructure.shutdown import ShutdownSignal


def test_a_record_becomes_one_logfmt_line_with_its_fields():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    logging.getLogger("demo").info("partitions assigned", extra=fields(member="consumer-1", partitions=[0, 1]))
    line = stream.getvalue().strip()
    assert line.startswith("ts=")
    assert "level=INFO" in line
    assert "logger=demo" in line
    assert 'msg="partitions assigned"' in line
    assert "member=consumer-1" in line
    assert 'partitions="[0, 1]"' in line


def test_records_below_the_level_are_dropped_and_kafka_stays_at_warning():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    logging.getLogger("demo").debug("hidden")
    assert stream.getvalue() == ""
    assert logging.getLogger("kafka").level == logging.WARNING


def test_an_exception_is_attached_to_the_line():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("demo").exception("failed")
    assert "exc=" in stream.getvalue()
    assert "boom" in stream.getvalue()


def test_configuring_twice_does_not_duplicate_lines():
    stream = io.StringIO()
    configure_logging("INFO", stream)
    configure_logging("INFO", stream)
    logging.getLogger("demo").info("once")
    assert stream.getvalue().count("msg=once") == 1


def test_a_shutdown_signal_starts_unset_and_is_set_by_request():
    shutdown = ShutdownSignal()
    assert not shutdown.requested
    shutdown.request()
    assert shutdown.requested


def test_sigterm_sets_the_flag_once_installed():
    shutdown = ShutdownSignal()
    previous = {number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)}
    try:
        shutdown.install()
        signal.raise_signal(signal.SIGTERM)
        assert shutdown.requested
    finally:
        for number, handler in previous.items():
            signal.signal(number, handler)
```

- Lines 9 to 19: `io.StringIO()` is an in-memory text stream; logging to it lets the test read what would have been written to the terminal. The line must start with `ts=` and contain the level, the logger name, the quoted message (it has a space), the plain `member=consumer-1`, and the list quoted because it contains spaces.
- Lines 22 to 27: a debug record is dropped at level INFO, and the `kafka` logger is at WARNING.
- Lines 30 to 38: inside `except`, `logging.getLogger("demo").exception("failed")` logs with the error attached; the line holds `exc=` and the text `boom`.
- Lines 41 to 46: configuring twice does not duplicate lines.
- Lines 49 to 53: a new `ShutdownSignal` is not set; `request()` sets it.
- Lines 56 to 65: really sends the process `SIGTERM` with `signal.raise_signal(signal.SIGTERM)` after `install()`, and checks the flag. The `try ... finally` puts the original signal handlers back so other tests are not affected.

### `tests/unit/interfaces/test_arguments_and_clis.py`

```python
import argparse

import pytest

from order_events.interfaces import consume_cli, produce_cli
from order_events.interfaces.arguments import non_negative_float, non_negative_int, positive_int


def test_validators_accept_valid_numbers():
    assert positive_int("3") == 3
    assert non_negative_int("0") == 0
    assert non_negative_float("0.5") == 0.5


@pytest.mark.parametrize(
    ("validator", "text"), [(positive_int, "0"), (non_negative_int, "-1"), (non_negative_float, "-0.1")]
)
def test_validators_reject_out_of_range_numbers(validator, text):
    with pytest.raises(argparse.ArgumentTypeError):
        validator(text)


def test_the_producer_defaults_match_the_lessons_run():
    args = produce_cli.build_parser().parse_args([])
    assert (args.orders, args.start, args.prefix, args.no_key, args.window) == (1000, 1, "order", False, 50)


def test_the_producer_refuses_zero_orders():
    with pytest.raises(SystemExit):
        produce_cli.build_parser().parse_args(["--orders", "0"])


def test_the_consumer_runs_until_stopped_by_default():
    args = consume_cli.build_parser().parse_args([])
    assert args.until_idle is None
    assert args.delay == 0.0
    assert args.print_history == 0


def test_a_bad_environment_stops_every_program_with_exit_code_two(monkeypatch, capsys):
    monkeypatch.setenv("BOOTSTRAP_SERVERS", "not-a-server")
    assert produce_cli.main(["--orders", "1"]) == 2
    assert consume_cli.main([]) == 2
    assert "configuration error" in capsys.readouterr().err
```

- Lines 9 to 12: the three checkers accept valid numbers.
- Lines 15 to 20: parametrized with two names at once: `("validator", "text")` each paired item gives one test run; each must raise `ArgumentTypeError`.
- Lines 23 to 25: the producer's defaults are 1000 orders, start 1, prefix `order`, keyed, window 50.
- Lines 28 to 30: `--orders 0` makes `argparse` exit; that is the exception `SystemExit`.
- Lines 33 to 37: the consumer's defaults: no idle timeout, no delay, no history.
- Lines 40 to 44: with a bad address in the environment, `produce_cli.main(...)` and `consume_cli.main([])` both return 2. `capsys` is a built-in fixture that captures what the code prints; `.readouterr().err` is everything written to stderr.

### `tests/integration/test_kafka_adapters.py`

These tests need `make up` first. They use real topics and a real broker.

```python
"""The Kafka adapters against the real broker: make up && make test-integration."""

import random
import threading
import time
import uuid

import pytest
from kafka import KafkaAdminClient

from order_events.application.group_lag import group_lag
from order_events.application.health import check_health
from order_events.application.produce_orders import ProducePlan, produce_orders
from order_events.application.read_orders import ReadPlan, ReadResult, read_orders
from order_events.infrastructure.kafka_health import KafkaTopicProbe
from order_events.infrastructure.kafka_offsets import KafkaOffsetReader
from order_events.infrastructure.kafka_publisher import KafkaEventPublisher
from order_events.infrastructure.kafka_source import KafkaEventSource
from order_events.infrastructure.settings import Settings

from ..conftest import BOOTSTRAP

pytestmark = pytest.mark.integration


def publish(settings: Settings, topic: str, orders: int, *, keyed: bool, start: int = 1) -> int:
    publisher = KafkaEventPublisher(settings, topic)
    try:
        plan = ProducePlan(orders=orders, start=start, keyed=keyed, window=20)
        return produce_orders(publisher, plan, random.Random(5), time.time, time.sleep)
    finally:
        publisher.close()


def read(settings: Settings, topic: str, group_id: str | None = None) -> ReadResult:
    source = KafkaEventSource(settings, "test-reader", group_id or f"check-{uuid.uuid4().hex}", topic)
    try:
        plan = ReadPlan(idle_timeout_seconds=4)
        return read_orders(
            source, plan, stop=lambda: False, clock=time.time, sleep=time.sleep, on_out_of_order=lambda _: None
        )
    finally:
        source.close()

```

- Line 21: `from ..conftest import BOOTSTRAP` imports from the file two levels up with a relative import.
- Line 23: `pytestmark = pytest.mark.integration` puts the mark `integration` on every test in this file. `make check` runs only `tests/unit`, so these are not run there; `make test-integration` runs `pytest tests/integration -m integration` and selects them. (`--strict-markers` in `pyproject.toml` makes an unregistered mark an error.)
- Lines 26 to 32: helper `publish` builds a real publisher, runs the produce use case with real time, and closes the publisher in `finally`.
- Lines 35 to 43: helper `read` reads with a real source in a random group (`uuid.uuid4().hex` makes the group new, so it starts from the earliest record), stopping after four idle seconds.

```python
def test_the_topic_has_six_partitions_and_one_replica(topic: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    description = admin.describe_topics([topic])[0]
    admin.close()
    assert len(description["partitions"]) == 6
    assert all(len(partition["replica_nodes"]) == 1 for partition in description["partitions"])


def test_keyed_events_of_one_order_share_a_partition_and_arrive_in_order(settings: Settings, topic: str) -> None:
    sent = publish(settings, topic, orders=100, keyed=True)
    result = read(settings, topic)
    assert result.events_read == sent
    assert result.report.out_of_order == ()
    assert result.report.orders_split_across_partitions == 0


def test_unkeyed_events_arrive_out_of_order(settings: Settings, topic: str) -> None:
    sent = publish(settings, topic, orders=100, keyed=False)
    result = read(settings, topic)
    assert result.events_read == sent
    assert len(result.report.out_of_order) > 0
    assert result.report.orders_split_across_partitions > 0
```

- Lines 46 to 51: the topic has six partitions and each has exactly one replica (read with `KafkaAdminClient.describe_topics`).
- Lines 54 to 59: 100 keyed orders: every event read, none out of order, no order split over partitions.
- Lines 62 to 67: 100 unkeyed orders: every event read, but some out of order and some orders split.

```python
def test_eight_consumers_on_six_partitions_leave_two_idle(settings: Settings, topic: str) -> None:
    group = f"scale-{uuid.uuid4().hex}"
    sources = [KafkaEventSource(settings, f"member-{number}", group, topic) for number in range(8)]
    stop = threading.Event()
    counts: dict[int, int] = {}

    def member(number: int) -> None:
        while not stop.is_set():
            sources[number].poll(0.3)
            counts[number] = sources[number].assigned_partition_count

    threads = [threading.Thread(target=member, args=(number,)) for number in range(8)]
    for thread in threads:
        thread.start()
        time.sleep(0.5)
    deadline = time.time() + 60
    while time.time() < deadline and sorted(counts.values()) != [0, 0, 1, 1, 1, 1, 1, 1]:
        time.sleep(1)
    stop.set()
    for thread in threads:
        thread.join()
    for source in sources:
        source.close()
    assert sorted(counts.values()) == [0, 0, 1, 1, 1, 1, 1, 1]
```

- The test starts eight consumers in one group, each polling in its own **thread**. A thread is a second line of execution inside one program; the eight consumers can then wait for the broker at the same time. `threading.Event` is a flag shared by the threads; `stop.set()` tells them to finish. Each thread polls and records its `assigned_partition_count` in a shared dictionary.
- The threads are started half a second apart, so each join triggers a rebalance. The test then waits up to 60 seconds until the sorted counts are exactly `[0, 0, 1, 1, 1, 1, 1, 1]`: two consumers idle, six with one partition. Then it stops the threads (`join` waits for each to finish), closes the sources and asserts the same.

```python
def test_lag_grows_while_the_group_is_stopped_and_drains_when_it_reads(settings: Settings, topic: str) -> None:
    group, offsets = f"lag-{uuid.uuid4().hex}", KafkaOffsetReader(settings)
    first = publish(settings, topic, orders=20, keyed=True)
    assert read(settings, topic, group).events_read == first
    assert sum(group_lag(offsets, group, topic).values()) == 0

    backlog = publish(settings, topic, orders=50, keyed=True, start=1000)
    assert sum(group_lag(offsets, group, topic).values()) == backlog

    assert read(settings, topic, group).events_read == backlog
    assert sum(group_lag(offsets, group, topic).values()) == 0


def test_the_health_probe_passes_for_the_designed_topic_and_fails_for_a_missing_one(
    settings: Settings, topic: str
) -> None:
    ok = check_health([KafkaTopicProbe(Settings.from_env({"BOOTSTRAP_SERVERS": BOOTSTRAP, "TOPIC": topic}))])
    assert ok.healthy
    missing = check_health(
        [KafkaTopicProbe(Settings.from_env({"BOOTSTRAP_SERVERS": BOOTSTRAP, "TOPIC": "no-such-topic"}))]
    )
    assert not missing.healthy
    assert "expected 6" in missing.results[0].detail
```

- Lines 96 to 106: lag. A group reads 20 orders (60 events) and the lag is 0. Then 50 more orders (150 events) are published with `start=1000` (different ids): the lag equals the backlog. The group reads again: all 150, and the lag is 0 again. This is the lesson's lag story in miniature.
- Lines 109 to 118: the health probe is healthy for the topic made by the fixture and unhealthy for `no-such-topic`, with the detail containing `expected 6`.

### `tests/e2e/test_lesson_scenario.py`

```python
"""The lesson's scenario through the command-line programs, as the Makefile runs it: make up && make test-e2e."""

import os
import subprocess
import sys
import uuid

import pytest

from ..conftest import BOOTSTRAP

pytestmark = pytest.mark.e2e


def program(module: str, topic: str, *args: str) -> subprocess.CompletedProcess[str]:
    environment = {**os.environ, "BOOTSTRAP_SERVERS": BOOTSTRAP, "TOPIC": topic}
    return subprocess.run(
        [sys.executable, "-m", f"order_events.interfaces.{module}", *args],
        capture_output=True,
        text=True,
        env=environment,
        timeout=180,
        check=False,
    )


def test_a_keyed_run_logs_zero_out_of_order_events_and_an_unkeyed_run_logs_some(topic: str) -> None:
    keyed = program("produce_cli", topic, "--orders", "200", "--prefix", "keyed", "--max-delay", "0")
    assert keyed.returncode == 0
    assert "produced 600 events for 200 orders" in keyed.stdout

    unkeyed = program("produce_cli", topic, "--orders", "200", "--prefix", "unkeyed", "--max-delay", "0", "--no-key")
    assert unkeyed.returncode == 0

    keyed_read = program(
        "consume_cli",
        topic,
        "--until-idle",
        "4",
        "--group",
        f"e2e-{uuid.uuid4().hex}",
        "--prefix",
        "keyed",
        "--print-history",
        "1",
    )
    assert keyed_read.returncode == 0
    assert "out-of-order events logged: 0" in keyed_read.stdout
    assert "OrderPlaced -> OrderPaid -> OrderShipped" in keyed_read.stdout

    unkeyed_read = program(
        "consume_cli", topic, "--until-idle", "4", "--group", f"e2e-{uuid.uuid4().hex}", "--prefix", "unkeyed"
    )
    assert unkeyed_read.returncode == 0
    assert "out-of-order events logged: 0" not in unkeyed_read.stdout
    assert "out of order" in unkeyed_read.stderr


def test_the_health_command_reports_the_designed_topic(topic: str) -> None:
    result = program("health_cli", topic)
    assert result.returncode == 0
    assert f"ok kafka: topic {topic} has 6 partitions" in result.stdout


def test_the_lag_command_prints_a_total(topic: str) -> None:
    program("produce_cli", topic, "--orders", "10", "--max-delay", "0")
    result = program("lag_cli", topic, "--group", f"e2e-lag-{uuid.uuid4().hex}")
    assert result.returncode == 0
    assert "total" in result.stdout
    assert result.stdout.strip().splitlines()[-1].split()[-1] == "30"
```

- Lines 15 to 24: helper `program(...)` starts one of the command-line programs as a separate **process** with `subprocess.run`. `sys.executable` is the path of the Python running the tests. `capture_output=True, text=True` collects stdout and stderr as text. `env={**os.environ, ...}` copies the environment and adds the broker and topic; `**` spreads a dictionary into another. `timeout=180` stops a hung program; `check=False` means a non-zero exit code does not raise, the test examines `returncode` itself.
- Lines 27 to 56: the main scenario. A keyed run of 200 orders prints `produced 600 events for 200 orders`; an unkeyed run with a different prefix follows. Reading only the keyed prefix logs `out-of-order events logged: 0` and prints the history `OrderPlaced -> OrderPaid -> OrderShipped`. Reading only the unkeyed prefix does *not* print zero, and stderr contains `out of order`.
- Lines 59 to 62: `health_cli` exits 0 and prints `ok kafka: topic <name> has 6 partitions`.
- Lines 65 to 70: `lag_cli` prints a table whose last line ends in `30` (10 orders times three events, none consumed by the new group). `.splitlines()[-1].split()[-1]` takes the last word of the last line.

## Try it yourself

```bash
.venv/bin/python -m pytest tests/unit/domain -v
```

Expected: a list of the domain tests, each marked `PASSED`, and a summary line `20 passed`.

Break something on purpose: in `src/order_events/domain/lag.py` change `end - (committed.get(partition) or 0)` to `end + (committed.get(partition) or 0)`, run the same command, and see `test_lag_is_log_end_minus_committed` fail with both sides of the failed `assert` printed. Then undo the change with `git checkout src/order_events/domain/lag.py`.

## Mistakes people make

- A test that depends on another test's leftovers: each integration test gets its own topic and its own group id for that reason.
- Real `sleep` in unit tests: this repository injects fake clocks and sleeps so the 81 unit tests finish in well under a second.
- Forgetting `list(...)` around a generator in a test that expects an error: nothing runs, so nothing is raised.

## Check yourself

1. What makes pytest treat a function as a test? (A name starting with `test_` in a file whose name starts with `test_`.)
2. What does a fixture with `yield` do after the test finishes? (Runs the code after the `yield`: the clean-up.)
3. Why does the integration test use a new group id every time? (A new group has no committed offsets, so it starts from the earliest record.)
4. How does each integration test get a topic of its own? (The `topic` fixture creates a topic with a random name and deletes it after the test.)
5. What does `--strict-markers` do? (It makes a test mark that is not registered in `pyproject.toml` an error, which catches typos.)

## Further reading

- pytest: fixtures: https://docs.pytest.org/en/stable/how-to/fixtures.html
- pytest: parametrize: https://docs.pytest.org/en/stable/how-to/parametrize.html
- pytest: monkeypatch: https://docs.pytest.org/en/stable/how-to/monkeypatch.html
- pytest: capturing output (`capsys`): https://docs.pytest.org/en/stable/how-to/capture-stdout-stderr.html
- pytest: marks: https://docs.pytest.org/en/stable/how-to/mark.html
