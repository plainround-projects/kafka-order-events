# Python

## What it is

Python is a programming language. You write instructions in text files that end in `.py`. A program called the **interpreter** reads the file and carries out the instructions one at a time. This repository needs Python 3.12 or newer (`requires-python = ">=3.12"` in `pyproject.toml`); the continuous-integration job uses 3.12 and the author tested with 3.13.

A **module** is one `.py` file. A **package** is a folder of modules that has a name. This repository's package is `order_events`.

## Why this repository uses it

The lesson asks for a producer and a consumer that talk to Kafka. The lesson's own starting point is Python, and Python has a plain Kafka client library. All the code here is Python, so reading it well means knowing the few language features it relies on. They are listed below, each at the place it is used.

## Where it appears

| Path | What it does with Python |
| --- | --- |
| `src/order_events/domain/` | Plain Python objects and functions: events, the ordering check, the lag sum. No library imports except the standard library. |
| `src/order_events/application/` | Use cases and *ports* (descriptions of what the use cases need from the outside world). |
| `src/order_events/infrastructure/` | Code that talks to Kafka, reads the environment, writes logs, retries, and handles signals. |
| `src/order_events/interfaces/` | The command-line programs. Each has a `main()` function. |
| `tests/` | Python test functions run by pytest. |
| `pyproject.toml` | Says which Python version is needed and where the packages are. |

## How it works underneath

You start a program with `python -m order_events.interfaces.produce_cli`. The `-m` flag means "run this module by its package name". The interpreter finds the package, reads the file, turns it into an internal form (bytecode), and runs it from the top.

Everything in Python is an **object**: numbers, text, functions, classes. A **variable** is a name that points at an object. A **function** is an object you can call. A **class** is a recipe for making objects. When you write `OrderEvent(...)` you ask the class to make one object.

Python does not check types before it runs. You can write **type hints** (`order_id: str`) to say what you expect. The interpreter ignores them while running. A separate tool, mypy (see `tech/mypy.md`), reads them and finds mistakes without running the code.

An **import** loads another module once and gives you names from it. The **standard library** is the set of modules that ship with Python (`logging`, `signal`, `argparse`, `dataclasses`, `random`). Anything else must be installed first (see `tech/pip-venv-and-lock-files.md`).

## Walkthrough of this repository's use

### Postponed type hints

```python
from __future__ import annotations
```

Almost every file starts with this line. It tells Python to keep type hints as text and not to work them out when the file loads. That lets a hint mention a class that is defined further down the file.

### Dataclasses: small objects with named fields

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

`@dataclass` is a **decorator**: a line starting with `@` that wraps the class below it. It writes the boring methods for you (`__init__`, `__eq__`, `__repr__`) from the list of fields. `frozen=True` makes the object read-only after it is built: assigning to `event.sequence` raises an error. That is an **immutable value object**, and it means no other code can change an event by accident. `slots=True` stores the fields in a fixed layout, which saves memory and forbids adding new fields later.

`__post_init__` runs right after the automatic `__init__`. It is the place to refuse bad values. The `f"..."` is an **f-string**: text with `{expression}` holes that Python fills in.

### Enumerations

```python
class EventType(StrEnum):
    """The three events of an order's lifecycle, in the order they happen."""

    PLACED = "OrderPlaced"
    PAID = "OrderPaid"
    SHIPPED = "OrderShipped"


LIFECYCLE: tuple[EventType, ...] = tuple(EventType)
```

`StrEnum` makes a fixed set of named values that are also plain text. `EventType.PLACED` equals the string `"OrderPlaced"`. `tuple(EventType)` lists the three members in the order they were written. `tuple[EventType, ...]` in the hint means "a tuple of any length whose items are `EventType`".

### Generators: functions that hand out values one at a time

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
    if window < 1:
        raise ValueError("window must be at least 1")
    pending = iter(order_ids)
    open_orders: list[tuple[str, list[EventType]]] = []

    def refill() -> None:
        while len(open_orders) < window:
            order_id = next(pending, None)
            if order_id is None:
                return
            open_orders.append((order_id, list(LIFECYCLE)))

    refill()
    while open_orders:
        entry = open_orders[rng.randrange(len(open_orders))]
        order_id, remaining = entry
        sequence = len(LIFECYCLE) - len(remaining) + 1
        yield OrderEvent(order_id, remaining.pop(0), sequence, clock())
        if not remaining:
            open_orders.remove(entry)
            refill()
```

`yield` turns the function into a **generator**. Calling `interleave(...)` does not run the body. It returns an object. Each time the caller asks for the next item, the body runs until the next `yield`, hands the value out, and pauses. This lets the producer take events one by one without building a list of all 3,000.

Inside, `refill` is a **nested function** (a function defined inside a function). It can read the outer variables `pending` and `open_orders`. `rng.randrange(...)` picks a random open order; `rng` is passed in so tests can pass a seeded generator and get the same sequence every time.

### Protocols: describing what you need, not who provides it

```python
class EventPublisher(Protocol):
    """Sends order events to the log."""

    def publish(self, event: OrderEvent, key: str | None) -> None:
        """Send one event. `key` decides the partition; None lets the client choose."""

    def flush(self) -> None:
        """Block until every published event is acknowledged."""


class EventSource(Protocol):
```

A `Protocol` is a list of methods a class must have. Any class with a `publish` and a `flush` method counts as an `EventPublisher`, even if it never mentions the word. The use cases ask for an `EventPublisher`; tests pass a fake, and the real program passes the Kafka one. The body `"""Send one event..."""` is only a docstring, so the method does nothing here.

### Functions as values, default arguments and keyword-only arguments

```python
def produce_orders(
    publisher: EventPublisher,
    plan: ProducePlan,
    rng: random.Random,
    clock: Callable[[], float],
    pause: Callable[[float], None],
    stop: Callable[[], bool] = lambda: False,
) -> int:
```

Functions can be passed around like numbers. `clock: Callable[[], float]` means "a function that takes nothing and returns a float". The program passes `time.time`; a test passes a function that returns fixed numbers. `stop: Callable[[], bool] = lambda: False` gives `stop` a **default value**: a `lambda` is a tiny unnamed function, here one that always answers "no". `Callable`, `list[str]` and `int | None` are hint syntax; `X | None` means "an X or nothing".

### Generic functions (Python 3.12)

```python
def retry[T](
    operation: Callable[[], T],
    *,
    attempts: int,
    base_delay_seconds: float,
    retry_on: tuple[type[BaseException], ...],
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
    max_delay_seconds: float = 10.0,
    on_retry: Callable[[int, BaseException], None] | None = None,
) -> T:
```

`def retry[T](...)` declares a **type variable** `T`. It says: whatever type `operation` returns, `retry` returns the same type. The lone `*,` means every parameter after it must be named in the call, such as `attempts=3`. This syntax needs Python 3.12, which is why that is the minimum version.

### Closures and `nonlocal`

```python
    reported = 0

    def on_out_of_order(delivery: Delivery) -> None:
        nonlocal reported
        reported += 1
        if reported <= REPORT_LIMIT:
            logger.warning(
                "out of order",
                extra=fields(
                    order=delivery.event.order_id,
                    sequence=delivery.event.sequence,
                    partition=delivery.partition,
                    offset=delivery.offset,
                ),
            )
```

`on_out_of_order` is defined inside `main` and remembers `reported` from the outside. `nonlocal reported` says "assign to the outer variable, do not make a new one". A function that remembers its surroundings is a **closure**.

### The program entry point

```python
def main(argv: Sequence[str] | None = None) -> int:
    """Run one consumer-group member; returns the process exit code."""
    args = build_parser().parse_args(argv)
```

```python
if __name__ == "__main__":
    raise SystemExit(main())
```

`main` returns an integer, the **exit code**: `0` means success, anything else means failure. `__name__` is `"__main__"` only when the file is run directly or with `-m`, so importing the module (as the tests do) does not start the program. `raise SystemExit(main())` ends the process with that code.

### Reading the environment, signals, logging and argument parsing

- `os.environ` holds the environment variables (`infrastructure/settings.py`).
- `argparse` reads the command line and prints `--help` (`interfaces/produce_cli.py`).
- `signal.signal(...)` runs a function when the operating system sends SIGINT (Ctrl-C) or SIGTERM (`infrastructure/shutdown.py`). See `tech/bash.md` for how `kill` sends them.
- `logging` writes log lines (`infrastructure/logging.py`, explained in `tech/logfmt.md`).
- `threading.Event` is a flag that one thread sets and another reads (`infrastructure/shutdown.py`).

## Try it yourself

1. See the version in use: `.venv/bin/python --version` prints `Python 3.13.x` (3.12 or newer is fine).
2. Run a module by name and read its help: `.venv/bin/python -m order_events.interfaces.produce_cli --help` prints `usage: produce_cli.py [-h] [--orders ORDERS] ...`.
3. Watch a frozen dataclass refuse a change: `.venv/bin/python -c "from order_events.domain.events import *; e = lifecycle_events('a-1', 1.0)[0]; e.sequence = 9"` ends with `dataclasses.FrozenInstanceError: cannot assign to field 'sequence'`.
4. Watch the generator hand out events one at a time: `.venv/bin/python -c "import random; from order_events.domain.events import interleave; g = interleave(['a','b'], 2, random.Random(1), lambda: 0.0); print(next(g)); print(next(g))"` prints two events of different orders.

## Mistakes people make

- Thinking type hints are checked at run time. They are not; run `make lint` to have mypy check them.
- Changing a list while looping over it. The code here builds a new list or removes an item only after the loop step finishes (`open_orders.remove(entry)` in `interleave`).
- Using a mutable default such as `def f(items=[])`. The shared list survives between calls. This repository uses immutable defaults (`None`, numbers, tuples, `lambda`).
- Forgetting that `import` runs the file. The `if __name__ == "__main__":` guard keeps programs from starting when a test imports them.

## Check yourself

1. What does `frozen=True` do to a dataclass? *It makes instances read-only after they are built.*
2. What is the difference between a function and a generator? *A generator uses `yield` and hands values out one at a time, pausing between them.*
3. Why does `main` return a number? *It becomes the process exit code; 0 means success.*
4. Why are `clock` and `rng` passed in as arguments? *So tests can control time and randomness and get the same result every run.*
5. What does `Callable[[], bool]` mean? *A function that takes no arguments and returns a bool.*

## Further reading

- Python 3.12 documentation: https://docs.python.org/3.12/
- Data classes: https://docs.python.org/3.12/library/dataclasses.html
- Generics syntax (PEP 695, new in 3.12): https://docs.python.org/3.12/reference/compound_stmts.html#generic-functions
- `typing.Protocol`: https://docs.python.org/3.12/library/typing.html#typing.Protocol
