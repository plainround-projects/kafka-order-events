# Python: the infrastructure layer, file by file

## What this note covers

The folder `src/order_events/infrastructure/` holds the **adapters**: the code that touches the outside world. Kafka (through the `kafka-python` library), environment variables, the terminal's log output, the clock's `sleep`, and operating-system signals. Each adapter fits one of the **ports** (the descriptions in `application/ports.py`), so the use cases never know which real thing is behind them.

This is the only layer allowed to `import kafka`. It may use the application and the domain. It is not allowed to be imported by them: `make lint` fails if `domain/` or `application/` imports `infrastructure/`.

Three words used below:

- A **port** is a description of what a use case needs; an **adapter** is a class that really does it.
- A **library** is code written by other people that you install and import. `kafka-python` is the library that speaks Kafka's network protocol.
- The **environment** is a set of named text values (like `TOPIC=order-events`) that the operating system gives every program when it starts. They are called **environment variables**.

## The files

| File | Role |
| --- | --- |
| `infrastructure/__init__.py` | Marks the folder as a package. |
| `codec.py` | Turns an event into JSON bytes and back, rejecting bad input. |
| `settings.py` | Reads and checks the environment variables once at startup. |
| `logging.py` | Writes one `key=value` log line per event. |
| `retry.py` | Retries a failing network call with growing pauses. |
| `shutdown.py` | Turns Ctrl+C and `docker stop` into a flag the loops check. |
| `kafka_publisher.py` | Implements `EventPublisher` with a Kafka producer. |
| `kafka_source.py` | Implements `EventSource` with a Kafka consumer in a group. |
| `kafka_offsets.py` | Implements `OffsetReader`: end offsets and committed offsets. |
| `kafka_health.py` | Implements `HealthProbe`: can I reach the broker, and are there six partitions? |

## `infrastructure/__init__.py`

```python
"""Adapters: Kafka clients, settings, logging, retries, signals. Implements the application's ports."""
```

Only a docstring: it marks the folder as a package and states the layer's job.

## `codec.py`: the JSON wire format

A **codec** is a pair of functions, one that encodes data into bytes and one that decodes bytes back. **JSON** is a text format for data: `{"orderId": "order-00042", "sequence": 1}` is an object (a set of named values) written in curly braces. See `tech/json.md`.

```python
"""The JSON wire format of an order event, with validation on the way in."""

from __future__ import annotations

import json

from ..domain.errors import MalformedEventError
from ..domain.events import EventType, OrderEvent


def encode_event(event: OrderEvent) -> bytes:
    """The JSON bytes of an order event."""
    payload = {"orderId": event.order_id, "type": event.type.value, "sequence": event.sequence, "ts": event.ts}
    return json.dumps(payload).encode()
```

- Line 5: `import json` loads Python's JSON module.
- Lines 7 and 8: imports from the domain. `from ..domain.errors import ...` goes up one package (from `infrastructure` to `order_events`) and into `domain`.
- Line 11: `encode_event(event: OrderEvent) -> bytes`. **Bytes** are raw numbers from 0 to 255; a network carries bytes, not text.
- Line 13: builds a **dictionary** with four keys. Notice the keys are `orderId` (camel case, the name in the lesson's JSON) while the Python field is `order_id` (snake case, the Python habit). The codec is the one place where the two spellings meet. `event.type.value` is the text of the enum member, for example `"OrderPaid"`.
- Line 14: `json.dumps(payload)` turns the dictionary into a JSON **string** (text); `.encode()` turns the text into bytes using UTF-8, the usual way to write text as bytes.

```python
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

- Line 17: `decode_event(raw: bytes) -> OrderEvent`.
- Line 19: `try:` begins a block whose errors can be caught below.
- Line 20: `json.loads(raw)` parses bytes (or text) into Python values. It raises `json.JSONDecodeError`, which is a kind of `ValueError`, for bad JSON.
- Lines 21 to 26: builds an `OrderEvent`. `data["orderId"]` looks up a key; a missing key raises `KeyError`. `EventType(data["type"])` looks up the enum member by its text; an unknown text raises `ValueError`. `str(...)`, `int(...)`, `float(...)` force each value to the right type; `int(None)` raises `TypeError`, and `int("abc")` raises `ValueError`. The domain's own check inside `OrderEvent` may raise `MalformedEventError` (an empty id, a sequence below 1).
- Lines 27 and 28: `except MalformedEventError: raise`. If the domain already raised the named error, let it go on unchanged. A bare `raise` re-throws the error being handled.
- Lines 29 and 30: `except (ValueError, KeyError, TypeError) as error:` catches the other three failures and turns them into one named error: `raise MalformedEventError(f"...") from error`. `from error` records the original as the *cause*, so the traceback still shows what really failed. Callers now need to know only one error type.

Why validate on the way in? Bytes from the network cannot be trusted. Everything past this function works with an `OrderEvent` that is known to be well formed.

## `settings.py`: the environment, checked once

```python
"""Configuration from the environment, validated once at startup (twelve-factor)."""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

_SERVERS = re.compile(r"^[\w.\-]+:\d{1,5}(,[\w.\-]+:\d{1,5})*$")
_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}


class ConfigurationError(ValueError):
    """An environment variable is missing or has an unusable value."""

```

- Line 1: "twelve-factor" is a set of rules for programs; the one used here says *configuration comes from the environment*, so the same code runs on a laptop and in a container without edits.
- Line 5: `import os` gives access to `os.environ`, the environment as a dictionary.
- Line 6: `import re` is Python's **regular expression** module. A regular expression is a short pattern that describes text.
- Line 10: `_SERVERS = re.compile(r"^[\w.\-]+:\d{1,5}(,[\w.\-]+:\d{1,5})*$")`. The `r` before the quote makes a **raw string** (backslashes are kept as they are). Reading the pattern: `^` is the start of the text; `[\w.\-]+` is one or more characters that are letters, digits, underscore, dot or hyphen (a host name); `:` is a colon; `\d{1,5}` is one to five digits (a port); the group `(,host:port)*` allows zero or more extra `,host:port` parts; `$` is the end. `re.compile` builds the pattern once so it can be used many times. The leading underscore on the name means "private to this file".
- Line 11: `_LEVELS` is a **set** (curly braces, unique values) of the allowed log levels.
- Lines 14 and 15: `ConfigurationError`, a named error built on `ValueError`.

```python
def _text(environ: Mapping[str, str], name: str, default: str) -> str:
    value = environ.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _positive_int(environ: Mapping[str, str], name: str, default: int) -> int:
    raw = environ.get(name, str(default)).strip()
    if not raw.isdigit() or int(raw) < 1:
        raise ConfigurationError(f"{name} must be a positive integer, got {raw!r}")
    return int(raw)
```

- Lines 18 to 22: `_text` reads one variable. `environ.get(name, default)` returns the value or the default if the variable is absent; `.strip()` removes spaces at both ends; an empty result raises `ConfigurationError` that names the variable. So `TOPIC=` (set but empty) is an error, not silently the default.
- Lines 25 to 29: `_positive_int` reads the variable as text, `str(default)` for the fallback. `raw.isdigit()` is true only if every character is a digit (so `-5`, `1.5` and `abc` fail); `int(raw) < 1` rejects zero. `{raw!r}` in the message prints the value with quotes so spaces are visible. Then `int(raw)` is returned.
- The `Mapping[str, str]` hint means the environment can be any read-only dictionary, which lets a test pass a plain dictionary and not touch the real environment.

```python
@dataclass(frozen=True, slots=True)
class Settings:
    """Everything the programs read from the environment. See .env.example for each variable."""

    bootstrap_servers: str
    topic: str
    group_id: str
    partitions: int
    log_level: str
    request_timeout_ms: int
    retry_backoff_ms: int

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if environ is None else environ
        servers = _text(env, "BOOTSTRAP_SERVERS", "localhost:9092")
        if not _SERVERS.match(servers):
            raise ConfigurationError(f"BOOTSTRAP_SERVERS must be host:port[,host:port], got {servers!r}")
        level = _text(env, "LOG_LEVEL", "INFO").upper()
        if level not in _LEVELS:
            raise ConfigurationError(f"LOG_LEVEL must be one of {sorted(_LEVELS)}, got {level!r}")
        return cls(
            bootstrap_servers=servers,
            topic=_text(env, "TOPIC", "order-events"),
            group_id=_text(env, "GROUP_ID", "order-history"),
            partitions=_positive_int(env, "PARTITIONS", 6),
            log_level=level,
            request_timeout_ms=_positive_int(env, "KAFKA_REQUEST_TIMEOUT_MS", 30_000),
            retry_backoff_ms=_positive_int(env, "KAFKA_RETRY_BACKOFF_MS", 100),
        )
```

- Lines 32 to 42: `Settings`, a frozen dataclass with seven fields: the Kafka address(es), the topic name, the consumer group id, the expected partition count, the log level, and two client timeouts in milliseconds. The docstring points to `.env.example`, where each variable is documented.
- Line 44: `@classmethod` makes `from_env` a method that belongs to the class, not to one object. Its first parameter `cls` is the class itself, so `cls(...)` builds a `Settings`. You call it as `Settings.from_env()`. The result hint `-> Settings` is allowed without quotes because of `from __future__ import annotations`.
- Line 45: `environ: Mapping[str, str] | None = None`: pass a dictionary in tests, or nothing to use the real environment.
- Line 46: `env = os.environ if environ is None else environ`.
- Lines 47 to 49: read `BOOTSTRAP_SERVERS` (default `localhost:9092`) and check it against the pattern with `.match(...)`; no match means a clear error.
- Lines 50 to 52: read `LOG_LEVEL` (default `INFO`), turn it to upper case so `info` works, and check it is one of the four allowed. `sorted(_LEVELS)` lists them in the message.
- Lines 53 to 61: build the object with names for each field. `30_000` is just `30000`; Python lets you put underscores in long numbers to make them readable. The defaults here are the same as in `.env.example`: topic `order-events`, group `order-history`, `6` partitions, request timeout 30,000 ms, retry backoff 100 ms.

**Fail fast:** every wrong value stops the program at startup with a message naming the variable, instead of crashing later in the middle of a run.

## `logging.py`: one line per event

**Logging** is a program writing a line of text about what it is doing. **Logfmt** is a simple format: `key=value key=value ...` on one line. See `tech/logfmt.md`.

```python
"""Structured logging: one logfmt line per event (`ts=... level=... logger=... msg=... key=value`)."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any, TextIO


def _quote(value: object) -> str:
    text = str(value)
    return json.dumps(text) if not text or any(char in text for char in ' ="\n') else text
```

- Line 5: `json` is used only to quote text safely (below).
- Line 6: `import logging` is Python's own logging module. The file is also *named* `logging.py` inside this package; because the import is absolute (`import logging`, not `from . import logging`) Python loads the standard module, not this file.
- Line 7: `sys` gives access to `sys.stderr`, the stream where programs write error and diagnostic messages. Standard output (stdout) is kept for results.
- Line 8: `datetime` and `UTC` (the time zone with no offset) for the timestamp.
- Line 9: `Any` means "any type"; `TextIO` means "an open text stream".
- Lines 12 to 14: `_quote` turns any value into text. If the text is empty or contains a space, an equals sign, a double quote or a newline, `json.dumps(text)` wraps it in double quotes and escapes special characters. Otherwise it is returned as is. The result: values with spaces still parse as one value.

```python
class LogfmtFormatter(logging.Formatter):
    """Format a log record as one logfmt line, with the record's extra fields as key=value pairs."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds")
        parts = [
            f"ts={timestamp}",
            f"level={record.levelname}",
            f"logger={record.name}",
            f"msg={_quote(record.getMessage())}",
        ]
        parts.extend(f"{key}={_quote(value)}" for key, value in getattr(record, "fields", {}).items())
        if record.exc_info:
            parts.append(f"exc={_quote(self.formatException(record.exc_info))}")
        return " ".join(parts)
```

- Line 17: `LogfmtFormatter(logging.Formatter)` is a class built on the standard `Formatter`, which decides how a log record becomes text. Here the method `format` is replaced.
- Line 21: `record.created` is the time the record was made, in seconds since 1970. `datetime.fromtimestamp(..., tz=UTC)` makes a date-time in UTC. `.isoformat(timespec="milliseconds")` writes it like `2026-10-26T12:34:56.789+00:00`.
- Lines 22 to 27: a list of four `key=value` strings: time, level (`INFO`, `WARNING`...), the logger's name (which module wrote it), and the message with `record.getMessage()`.
- Line 28: `parts.extend(...)` adds more items from a generator expression. `getattr(record, "fields", {})` reads an attribute named `fields` from the record and returns `{}` if it is absent. Each pair becomes `key=value`.
- Lines 29 and 30: if the log call came from inside an error handler, `record.exc_info` is set; then the formatted traceback is added as `exc=...`, quoted so it stays on one line.
- Line 31: `" ".join(parts)` glues the pieces together with single spaces.

```python
def fields(**values: Any) -> dict[str, Any]:
    """Use as `logger.info("message", extra=fields(order_id=...))` to attach key=value pairs."""
    return {"fields": values}


def configure_logging(level: str, stream: TextIO | None = None) -> None:
    """Send every log record to `stream` (stderr by default) as logfmt. Safe to call twice."""
    handler = logging.StreamHandler(stream or sys.stderr)
    handler.setFormatter(LogfmtFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    logging.getLogger("kafka").setLevel(max(logging.getLevelName(level), logging.WARNING))
```

- Lines 34 to 36: `fields(**values)`. The `**values` means "collect every keyword argument into a dictionary named `values`". `fields(order_id="x", count=3)` returns `{"fields": {"order_id": "x", "count": 3}}`. The standard logger accepts an `extra=` dictionary and copies its keys onto the record as attributes, so the record gets an attribute named `fields`, which the formatter reads on line 28.
- Lines 39 to 46: `configure_logging(level, stream=None)`. `logging.StreamHandler(stream or sys.stderr)` is a handler that writes records to a stream; `stream or sys.stderr` uses the given stream or, when `None`, stderr. `handler.setFormatter(...)` attaches the formatter. `root = logging.getLogger()` is the **root logger**, the parent of all loggers. `root.handlers[:] = [handler]` replaces its list of handlers in place, so calling the function twice does not print every line twice ("safe to call twice"). `root.setLevel(level)` drops records below the level.
- Line 46: the library `kafka-python` logs a lot at INFO level. `logging.getLogger("kafka").setLevel(max(logging.getLevelName(level), logging.WARNING))` makes it at least `WARNING`. `getLevelName("INFO")` returns the number 20; `max` keeps the higher of that and `WARNING`'s 30. So the library is quiet unless it has a problem, whatever level the programs use.

## `retry.py`: trying again, politely

```python
"""Retry with exponential backoff and jitter for calls that cross the network."""

from __future__ import annotations

import random
import time
from collections.abc import Callable


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

- Line 5: `random` for jitter. Line 6: `time` for `time.sleep`. Line 7: `Callable`.
- Line 10: `def retry[T](...)`. This is **new syntax in Python 3.12**: the `[T]` after the name declares a **type variable**. It says "whatever type `operation` returns, `retry` returns the same type". It lets `mypy` know that retrying a function that returns `int` gives an `int`.
- Line 11: `operation: Callable[[], T]` is a function that takes nothing and returns a `T`.
- Line 12: the `*` makes all following parameters keyword-only.
- Lines 13 to 19: `attempts` (how many tries), `base_delay_seconds` (first pause), `retry_on` (a tuple of error classes that count as retryable; `type[BaseException]` is "a class that is an error"), `sleep` (default `time.sleep`; a test passes a fake), `rng` (random generator or `None`), `max_delay_seconds` (the cap, default 10), `on_retry` (an optional function called with the attempt number and the error, used for logging).

```python
    """Call `operation` up to `attempts` times, doubling the pause after each failure in `retry_on`.

    The pause is `base * 2**(attempt - 1)` capped at `max_delay_seconds`, then scaled by a random
    factor in [0.5, 1.0] so many clients do not retry in step. The last failure is raised.
    """
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    jitter = rng or random.Random()
    for attempt in range(1, attempts + 1):
        try:
            return operation()
        except retry_on as error:
            if attempt == attempts:
                raise
            if on_retry:
                on_retry(attempt, error)
            sleep(min(max_delay_seconds, base_delay_seconds * 2 ** (attempt - 1)) * jitter.uniform(0.5, 1.0))
    raise AssertionError("unreachable")  # pragma: no cover
```

- Lines 21 to 25: the docstring gives the formula: the pause is `base * 2**(attempt - 1)`, capped, then multiplied by a random number between 0.5 and 1.0. `**` is "to the power of". This is **exponential backoff** (the wait doubles after each failure, so a struggling service gets room) with **jitter** (randomness, so many clients that failed together do not all retry in the same instant).
- Lines 26 and 27: refuse `attempts < 1`.
- Line 28: `jitter = rng or random.Random()` uses the given generator or makes one.
- Line 29: `for attempt in range(1, attempts + 1):` runs `attempts` times, numbering from 1.
- Lines 30 and 31: `try: return operation()`. A success returns at once.
- Line 32: `except retry_on as error:` catches only the listed kinds of error; any other error passes straight through to the caller, because retrying a bug does not help.
- Lines 33 and 34: on the last attempt, a bare `raise` re-throws the error just caught, so the caller sees the real last failure.
- Lines 35 and 36: otherwise tell the `on_retry` function.
- Line 37: sleep the computed time. `min(cap, base * 2 ** (attempt - 1))` is the capped pause; `jitter.uniform(0.5, 1.0)` is the random factor.
- Line 38: this line can never run (the loop either returns or raises), but `mypy` cannot prove that. `raise AssertionError("unreachable")` satisfies it, and the comment `# pragma: no cover` tells the coverage tool not to count the line as untested.

## `shutdown.py`: leaving cleanly

A **signal** is a message the operating system sends to a running program. `SIGINT` is sent when you press Ctrl+C in the terminal. `SIGTERM` is the polite "please stop" that `docker stop` sends. By default Python stops on SIGINT with an error and the program may die in the middle of a batch.

```python
"""Graceful shutdown: SIGINT and SIGTERM set a flag the work loops check between units of work."""

from __future__ import annotations

import signal
import threading
from types import FrameType


class ShutdownSignal:
    """A flag that SIGINT and SIGTERM set, which work loops check between units of work."""

    def __init__(self) -> None:
        self._requested = threading.Event()

    def install(self) -> None:
        """Route SIGINT and SIGTERM to `request`, so in-flight work finishes and connections close."""
        for number in (signal.SIGINT, signal.SIGTERM):
            signal.signal(number, self._handle)

    def _handle(self, _number: int, _frame: FrameType | None) -> None:
        self.request()

    def request(self) -> None:
        self._requested.set()

    @property
    def requested(self) -> bool:
        return self._requested.is_set()
```

- Line 5: `signal` is Python's signal module. Line 6: `threading` provides `Event`. Line 7: `FrameType` is a hint name for the "frame" argument handlers receive (the place in the code where the signal arrived).
- Lines 13 and 14: `__init__` is the method that runs when you build an object. `self._requested = threading.Event()` creates a flag: an `Event` is an object with `set()` (raise the flag), `is_set()` (is it raised?) and `wait()`. Events are safe to use from signal handlers and other threads.
- Lines 16 to 19: `install` loops over the two signal numbers and calls `signal.signal(number, self._handle)`, which tells the operating system "when this signal comes, run this function" instead of the default.
- Lines 21 and 22: `_handle` is the function that runs. It ignores both arguments (the underscore prefix says so) and calls `request()`.
- Lines 24 and 25: `request` raises the flag. The programs also call it themselves when a limit is reached.
- Lines 27 to 29: the `requested` property reports whether the flag is raised.

The work loops (`produce_orders`, `read_orders`) call `stop()` (which here is `lambda: shutdown.requested`) between units of work. So a Ctrl+C lets the current event finish, the producer flush and the consumer commit and leave its group, instead of dying half-way.

## `kafka_publisher.py`: the producer adapter

```python
"""Adapter: publish order events with kafka-python."""

from __future__ import annotations

from typing import Any

from kafka import KafkaProducer

from ..domain.events import OrderEvent
from .codec import encode_event
from .settings import Settings

```

- Line 5: `Any` in a hint switches type checking off for that value. It is used for the client object because `kafka-python` ships no type hints.
- Line 7: `from kafka import KafkaProducer` loads the library's producer class. This is the import the layer rule allows here and nowhere inside `domain/` or `application/`.
- Lines 9 to 11: imports of the domain event, the codec and the settings from this layer.

```python
class KafkaEventPublisher:
    """Implements `EventPublisher`. The client waits for all in-sync replicas (acks=all) and retries."""

    def __init__(self, settings: Settings, topic: str | None = None, flush_timeout_seconds: float = 60.0) -> None:
        self._topic = topic or settings.topic
        self._flush_timeout = flush_timeout_seconds
        self._producer: Any = KafkaProducer(
            bootstrap_servers=settings.bootstrap_servers,
            acks="all",
            request_timeout_ms=settings.request_timeout_ms,
            retry_backoff_ms=settings.retry_backoff_ms,
        )

    def publish(self, event: OrderEvent, key: str | None) -> None:
        self._producer.send(self._topic, key=None if key is None else key.encode(), value=encode_event(event))

    def flush(self) -> None:
        self._producer.flush(timeout=self._flush_timeout)

    def close(self) -> None:
        self._producer.close(timeout=self._flush_timeout)
```

- Line 14: the class has no parent. It does not say it implements `EventPublisher`; it just has `publish` and `flush`, which is enough for the protocol.
- Line 17: `topic: str | None = None` and `flush_timeout_seconds: float = 60.0` are optional.
- Line 18: `topic or settings.topic` uses the given topic or the one from settings.
- Lines 20 to 25: builds the Kafka producer. `bootstrap_servers` is the address(es) the client uses to find the cluster. `acks="all"` means a send counts as done only when **all in-sync replicas** have the record; with one broker that is the one broker. `request_timeout_ms` is how long to wait for an answer. `retry_backoff_ms` is the pause between the client's own internal retries.
- Lines 27 and 28: `publish` calls `send(topic, key=..., value=...)`. The key must be bytes, so `key.encode()` converts the order id; `None` is passed through. `send` does not wait: it puts the record in a buffer and returns at once; a background thread in the library sends batches.
- Lines 30 and 31: `flush` waits up to the timeout for everything in the buffer to be acknowledged.
- Lines 33 and 34: `close` flushes again and shuts the connections.

## `kafka_source.py`: the consumer adapter

```python
"""Adapter: read order events as a member of a consumer group with kafka-python."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from typing import Any

from kafka import ConsumerRebalanceListener, KafkaConsumer

from ..application.ports import Delivery
from ..domain.errors import MalformedEventError
from .codec import decode_event
from .logging import fields
from .settings import Settings

logger = logging.getLogger(__name__)
```

- Line 17: `logger = logging.getLogger(__name__)` gets a logger named after this module (`order_events.infrastructure.kafka_source`), which appears in the `logger=` part of each log line. `__name__` is a special variable holding the module's dotted name.
- Line 9: `ConsumerRebalanceListener` is a class from the library: you give it to a consumer to be told when partitions are taken away or handed out.

```python
class _LogAssignment(ConsumerRebalanceListener):  # type: ignore[misc]
    def __init__(self, member: str) -> None:
        self._member = member

    def on_partitions_revoked(self, revoked: Iterable[Any]) -> None:
        logger.info("partitions revoked", extra=fields(member=self._member, count=len(list(revoked))))

    def on_partitions_assigned(self, assigned: Iterable[Any]) -> None:
        numbers = sorted(partition.partition for partition in assigned)
        logger.info("partitions assigned", extra=fields(member=self._member, partitions=numbers or "none (idle)"))
```

- Line 20: the class builds on `ConsumerRebalanceListener`. The comment `# type: ignore[misc]` tells `mypy` not to complain about inheriting from an untyped class.
- Line 21 and 22: remembers the member's name (for example `consumer-1`).
- Lines 24 and 25: `on_partitions_revoked` is called by the library when the group takes partitions away from this member (a **rebalance**: the group reshuffles who reads what whenever members join or leave). It logs how many.
- Lines 27 to 29: `on_partitions_assigned` is called when partitions are handed out. `sorted(partition.partition for partition in assigned)` lists their numbers; `numbers or "none (idle)"` logs the text when the list is empty. This is the line the `make scale N=8` demo uses to show two idle consumers.

```python
class KafkaEventSource:
    """Implements `EventSource`. Offsets auto-commit every second and once more on `close`."""

    def __init__(self, settings: Settings, member: str, group_id: str | None = None, topic: str | None = None) -> None:
        self._member = member
        self._consumer: Any = KafkaConsumer(
            bootstrap_servers=settings.bootstrap_servers,
            group_id=group_id or settings.group_id,
            client_id=member,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            request_timeout_ms=settings.request_timeout_ms,
            retry_backoff_ms=settings.retry_backoff_ms,
        )
        self._consumer.subscribe([topic or settings.topic], listener=_LogAssignment(member))

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        deliveries: list[Delivery] = []
        for batch in self._consumer.poll(timeout_ms=int(timeout_seconds * 1000)).values():
            for record in batch:
                try:
                    deliveries.append(Delivery(decode_event(record.value), record.partition, record.offset))
                except MalformedEventError as error:
                    logger.warning(
                        "skipping malformed record",
                        extra=fields(
                            member=self._member, partition=record.partition, offset=record.offset, error=error
                        ),
                    )
        return deliveries

    @property
    def assigned_partition_count(self) -> int:
        """How many partitions the group currently gives this member (0 means idle)."""
        return len(self._consumer.assignment())

    def close(self) -> None:
        self._consumer.close()
```

- Lines 35 to 46: building the consumer. `group_id` is the consumer group; `client_id` is the member's name, shown in `kafka-consumer-groups.sh --describe`. `auto_offset_reset="earliest"` means a group with no committed offset starts from the oldest record. `enable_auto_commit=True` with `auto_commit_interval_ms=1000` has the library commit progress every second in the background; `close` commits once more. (The sibling project `kafka-idempotent-consumer` turns this off on purpose and commits by hand.) The two timeouts come from settings.
- Line 47: `subscribe([...], listener=...)` joins the group for the topic and registers the listener. Partitions are not assigned yet; that happens inside the first `poll` calls.
- Lines 49 to 62: `poll`. `self._consumer.poll(timeout_ms=...)` waits up to that many milliseconds and returns a dictionary whose values are lists of records, grouped by partition. `.values()` loops over the lists; the second `for` over each record. Each record has `.value` (bytes), `.partition` and `.offset`. A `Delivery` is made from the decoded event and the record's place. If the bytes are not a valid event, `MalformedEventError` is caught, a warning is logged with the position and error, and the loop goes on: **one bad record must not stop the stream** (the dead-letter topic of the sibling project is the next step up from this).
- Lines 64 to 67: `assigned_partition_count` is used by the scale demo to know when the group has settled.
- Lines 69 and 70: `close` commits and leaves the group.

## `kafka_offsets.py`: reading offsets without joining the group

```python
"""Adapter: committed and log-end offsets, read without joining the group."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from .logging import fields
from .retry import retry
from .settings import Settings

logger = logging.getLogger(__name__)


class KafkaOffsetReader:
    """Implements `OffsetReader`. Every call retries with backoff on Kafka errors."""

    def __init__(self, settings: Settings, attempts: int = 4) -> None:
        self._settings = settings
        self._attempts = attempts

    def _with_retry(self, operation: Any) -> Any:
        return retry(
            operation,
            attempts=self._attempts,
            base_delay_seconds=self._settings.retry_backoff_ms / 1000,
            retry_on=(KafkaError,),
            on_retry=lambda attempt, error: logger.warning(
                "retrying offset read", extra=fields(attempt=attempt, error=error)
            ),
        )
```

- Line 9: `TopicPartition` is the library's name for the pair (topic, partition number). Line 10: `KafkaError` is the base class of the errors the library defines.
- Lines 22 to 24: the constructor stores the settings and the number of attempts (default 4).
- Lines 26 to 35: `_with_retry` wraps any operation in `retry(...)` from the file above, retrying only on `KafkaError`, with the library backoff in seconds (`retry_backoff_ms / 1000`). `on_retry=lambda attempt, error: logger.warning(...)` is a one-line function that logs each retry.
- Lines 37 to 43: `_consumer(group_id)` builds a consumer with auto-commit off. It never calls `subscribe`, so it never joins a group. With `group_id=None` it is not even tied to a group.

```python
    def end_offsets(self, topic: str) -> Mapping[int, int]:
        def read() -> Mapping[int, int]:
            consumer = self._consumer(None)
            try:
                partitions = [
                    TopicPartition(topic, number) for number in sorted(consumer.partitions_for_topic(topic) or ())
                ]
                return {partition.partition: end for partition, end in consumer.end_offsets(partitions).items()}
            finally:
                consumer.close(autocommit=False)

        result: Mapping[int, int] = self._with_retry(read)
        return result

    def committed_offsets(self, group_id: str, topic: str) -> Mapping[int, int | None]:
        def read() -> Mapping[int, int | None]:
            # Same group id, never subscribed: asks the broker for committed offsets without joining.
            consumer = self._consumer(group_id)
            try:
                numbers = sorted(consumer.partitions_for_topic(topic) or ())
                return {number: consumer.committed(TopicPartition(topic, number)) for number in numbers}
            finally:
                consumer.close(autocommit=False)

        result: Mapping[int, int | None] = self._with_retry(read)
        return result
```

- Lines 45 to 57: `end_offsets`. The inner function `read` does one attempt. `consumer.partitions_for_topic(topic) or ()` returns the set of partition numbers (or `()` if the topic is unknown). `consumer.end_offsets(partitions)` asks the broker for the log-end offset of each. The dictionary comprehension turns the `TopicPartition` keys into plain numbers. `try ... finally: consumer.close(autocommit=False)` always closes the connection, even if an error was raised, and without committing anything. `self._with_retry(read)` runs `read` with the retry rules. The line `result: Mapping[int, int] = ...` exists only to tell `mypy` the type of an untyped value.
- Lines 59 to 70: `committed_offsets` is the same, but builds the consumer with the group's id and calls `consumer.committed(TopicPartition(...))` per partition. Asking for committed offsets of a group *without subscribing* reads them from the broker without joining the group, so the demo's measurement does not trigger a rebalance.

## `kafka_health.py`: the health check

```python
"""Adapter: is the broker reachable and does the topic have the partitions the design calls for?"""

from __future__ import annotations

from typing import Any

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from ..application.health import ProbeFailedError
from .retry import retry
from .settings import Settings


class KafkaTopicProbe:
    """Implements `HealthProbe` for the broker and the order-events topic."""

    name = "kafka"

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def check(self) -> str:
        try:
            partitions = retry(
                self._partitions,
                attempts=3,
                base_delay_seconds=self._settings.retry_backoff_ms / 1000,
                retry_on=(KafkaError,),
            )
        except KafkaError as error:
            raise ProbeFailedError(f"cannot reach {self._settings.bootstrap_servers}: {error}") from error
        if len(partitions) != self._settings.partitions:
            raise ProbeFailedError(
                f"topic {self._settings.topic} has {len(partitions)} partitions, expected {self._settings.partitions}"
            )
        return f"topic {self._settings.topic} has {len(partitions)} partitions"

    def _partitions(self) -> set[int]:
        consumer: Any = KafkaConsumer(
            bootstrap_servers=self._settings.bootstrap_servers,
            enable_auto_commit=False,
            request_timeout_ms=self._settings.request_timeout_ms,
        )
        try:
            return set(consumer.partitions_for_topic(self._settings.topic) or ())
        finally:
            consumer.close(autocommit=False)
```

- Line 10: `ProbeFailedError` comes from the application layer. This is an adapter using a name defined one layer inward, which is allowed.
- Line 18: `name = "kafka"` is a class attribute: every probe object has it.
- Lines 23 to 32: `check` calls `retry(self._partitions, attempts=3, ...)` so a broker that is only slow to answer is given three tries. If `KafkaError` still escapes, it is turned into a `ProbeFailedError` with a readable message that includes the address. `from error` keeps the original as the cause.
- Lines 33 to 36: if the number of partitions found differs from `settings.partitions`, fail with a message saying both numbers. This is how `make health` notices that the topic is missing or was created with the wrong design.
- Line 37: on success the detail says how many partitions were found.
- Lines 39 to 48: `_partitions` builds a consumer, asks `partitions_for_topic`, returns the numbers as a set (empty when the topic does not exist), and closes the consumer in the `finally` block.

## Try it yourself

Settings, with no Kafka needed:

```bash
.venv/bin/python -c "
from order_events.infrastructure.settings import Settings, ConfigurationError
print(Settings.from_env({}))
try:
    Settings.from_env({'PARTITIONS': 'six'})
except ConfigurationError as e:
    print('refused:', e)
"
```

Expected: the first line prints the seven default values; the second prints `refused: PARTITIONS must be a positive integer, got 'six'`.

Retry timing:

```bash
.venv/bin/python -c "
import random
from order_events.infrastructure.retry import retry
calls = []
def flaky():
    calls.append(1)
    if len(calls) < 3: raise OSError('down')
    return 'ok'
print(retry(flaky, attempts=5, base_delay_seconds=1, retry_on=(OSError,), sleep=lambda s: print('would sleep', round(s, 2)), rng=random.Random(0)))
"
```

Expected: two `would sleep` lines (the first between 0.5 and 1.0, the second between 1.0 and 2.0) and then `ok`. Nothing really waits, because `sleep` was replaced.

## Mistakes people make

- Trusting bytes from the network: always decode and validate in one place (the codec).
- Retrying every error: only errors that can clear up (a broker that is briefly away) are listed in `retry_on`.
- Forgetting to close a client: the `finally:` blocks in `kafka_offsets.py` and `kafka_health.py` exist for that.
- Assuming `send` has delivered: it only buffers. Delivery is certain only after `flush` returns.

## Check yourself

1. Why does `KafkaOffsetReader` never call `subscribe`? (So it does not join the group and cause a rebalance.)
2. What are `acks="all"`, and what do they mean with one broker? (Wait for all in-sync replicas; here the one broker.)
3. What does `from error` do on a `raise`? (Records the original error as the cause.)
