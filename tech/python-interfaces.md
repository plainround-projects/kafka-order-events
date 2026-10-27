# Python: the interfaces layer, file by file

## What this note covers

The folder `src/order_events/interfaces/` holds the **command-line programs**: the four things you can start from a terminal. Each one reads its arguments, builds the real adapters, hands them to a use case, prints the result, and decides the **exit code**. They are the outermost layer: they may import everything below them, and nothing may import them.

Words you need first:

- A **terminal** is a window where you type commands as text. A **command** is a program name followed by **arguments** (extra words that change what it does). `python -m order_events.interfaces.produce_cli --orders 1000` runs Python (`python`), asks it to run a module by its package name (`-m`), and passes it the argument `--orders` with the value `1000`.
- A **process** is a running program. When it ends it hands the operating system a number called its **exit code** (also called exit status). By convention `0` means success and any other number means a problem. The shell (and `make`, and CI) read that number to decide what to do next. This repository uses `0` for success, `1` for "ran but found a problem" (an unhealthy system), and `2` for "refused to start because the configuration is wrong".
- **Standard output** (stdout) and **standard error** (stderr) are the two text streams every process has. This repository prints results to stdout and log lines to stderr, so you can save the results to a file without the logs mixed in.

## The files

| File | Program it defines |
| --- | --- |
| `interfaces/__init__.py` | Marks the folder as a package. |
| `arguments.py` | Three small checks for command-line numbers. |
| `produce_cli.py` | The producer: publishes the order events. |
| `consume_cli.py` | One consumer-group member that checks the order. |
| `health_cli.py` | The health check: exit 0 when healthy. |
| `lag_cli.py` | Prints the group's lag per partition. |

## `interfaces/__init__.py`

```python
"""Command-line entry points. They wire adapters into use cases and own exit codes."""
```

Only a docstring; the folder becomes a package, and the sentence states the layer's job.

## `arguments.py`: checking numbers typed on the command line

```python
"""Argument validation shared by the command-line programs."""

from __future__ import annotations

import argparse


def positive_int(text: str) -> int:
    """Parse an integer of at least 1, for argparse."""
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def non_negative_int(text: str) -> int:
    """Parse an integer of at least 0, for argparse."""
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must not be negative, got {value}")
    return value


def non_negative_float(text: str) -> float:
    """Parse a float of at least 0, for argparse."""
    value = float(text)
    if value < 0:
        raise argparse.ArgumentTypeError(f"must not be negative, got {value}")
    return value
```

**`argparse`** is Python's standard module for reading command-line arguments. For each argument you declare a `type`: a function that receives the text the user typed and returns the value, or raises an error if it is not acceptable.

- Lines 8 to 13: `positive_int` converts the text to a whole number with `int(text)` (a text such as `abc` makes `int` raise `ValueError`, which `argparse` also turns into an error message), then refuses numbers below 1 by raising `argparse.ArgumentTypeError`. `argparse` catches that error, prints the usage line plus the message, and exits with code 2.
- Lines 16 to 21: `non_negative_int` is the same with 0 allowed (used for how many histories to print).
- Lines 24 to 29: `non_negative_float` parses a number with a decimal part (seconds), at least 0.

Together they mean `--orders -5` is refused with a clear message before any Kafka client is created.

## `produce_cli.py`: the producer program

### The header and the imports

```python
"""Publish OrderPlaced, OrderPaid and OrderShipped for a range of orders.

python -m order_events.interfaces.produce_cli --orders 1000
python -m order_events.interfaces.produce_cli --orders 1000 --no-key --prefix unkeyed
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from collections.abc import Sequence

from ..application.produce_orders import ProducePlan, produce_orders
from ..infrastructure.kafka_publisher import KafkaEventPublisher
from ..infrastructure.logging import configure_logging
from ..infrastructure.settings import ConfigurationError, Settings
from ..infrastructure.shutdown import ShutdownSignal
from .arguments import non_negative_float, positive_int

```

- Lines 1 to 5: this docstring is also the program's help text (line 25 passes `__doc__`, the special variable holding the file's docstring, as the description). It shows two ways to run it.
- Lines 9 to 13: standard-library imports: `argparse` (arguments), `random` (the generator), `sys` (for stderr), `time` (the clock and sleep), `Sequence` (a hint).
- Lines 15 to 20: imports from the layers below. This file is where the real `KafkaEventPublisher`, the logging setup, the settings and the shutdown flag are brought together.

### The parser

```python
def build_parser() -> argparse.ArgumentParser:
    """The argument parser of the producer program."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--orders", type=positive_int, default=1000, help="how many orders (three events each)")
    parser.add_argument("--start", type=positive_int, default=1, help="number of the first order")
    parser.add_argument("--prefix", default="order", help="order id prefix, so runs never share ids")
    parser.add_argument("--no-key", action="store_true", help="send without a key (the failure mode)")
    parser.add_argument("--window", type=positive_int, default=50, help="orders in flight at once")
    parser.add_argument(
        "--max-delay",
        type=non_negative_float,
        default=0.002,
        help="random pause in seconds after each event; the lesson uses up to 0.05",
    )
    parser.add_argument("--seed", type=int, default=None, help="fixed seed for a repeatable run")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    return parser
```

- Line 25: `argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)`. The `RawTextHelpFormatter` keeps the line breaks of the docstring when help is printed.
- Line 26: `add_argument("--orders", type=positive_int, default=1000, help=...)` declares an option named `--orders`. `type` is the checking function, `default` the value when the option is absent, `help` the text shown by `--help`.
- Lines 27 and 28: `--start` (the number of the first order) and `--prefix` (the id prefix; the end-to-end test and the second run in the lesson use different prefixes so ids never collide).
- Line 29: `--no-key` uses `action="store_true"`: it takes no value; if it is present the result is `True`, if absent `False`.
- Line 30: `--window`, the number of orders in flight at once.
- Lines 31 to 36: `--max-delay`, a float with default `0.002`: the largest random pause after each event. The help text says the lesson used up to `0.05`.
- Line 37: `--seed` takes any whole number (`type=int`); `None` means "pick a random seed". Giving a seed makes a run repeatable.
- Line 38: `--topic` defaults to `None`, which means "use `$TOPIC`", the environment variable.
- Line 39: return the parser.

### `main`

```python
def main(argv: Sequence[str] | None = None) -> int:
    """Publish orders; returns the process exit code."""
    args = build_parser().parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    plan = ProducePlan(
        orders=args.orders,
        start=args.start,
        prefix=args.prefix,
        keyed=not args.no_key,
        window=args.window,
        max_pause_seconds=args.max_delay,
    )
    shutdown = ShutdownSignal()
    shutdown.install()
    publisher = KafkaEventPublisher(settings, args.topic)
    started = time.time()
    try:
        sent = produce_orders(
            publisher, plan, random.Random(args.seed), time.time, time.sleep, stop=lambda: shutdown.requested
        )
    finally:
        publisher.close()
    mode = "unkeyed" if args.no_key else "keyed by orderId"
    topic = args.topic or settings.topic
    print(f"produced {sent} events for {args.orders} orders to {topic} ({mode}) in {time.time() - started:.1f}s")
    return 0
```

- Line 42: `main(argv)` takes an optional list of arguments. `None` makes `argparse` read the real command line; a test passes a list. It returns an `int`, the exit code.
- Line 44: `build_parser().parse_args(argv)` reads and checks the arguments. If one is wrong, `argparse` prints a message and exits with code 2 by itself. The result `args` has one attribute per option, with dashes turned into underscores (`--no-key` becomes `args.no_key`).
- Lines 45 to 49: read the settings. On `ConfigurationError`, print `configuration error: ...` to **stderr** (`file=sys.stderr`) and return 2.
- Line 50: set up logging at the configured level.
- Lines 51 to 58: build a `ProducePlan` from the arguments. `keyed=not args.no_key` flips the flag.
- Lines 59 and 60: create the shutdown flag and install the signal handlers.
- Line 61: build the real publisher; `args.topic` is `None` unless given.
- Line 62: remember the start time.
- Lines 63 to 68: run the use case inside `try ... finally`. `random.Random(args.seed)` makes the generator; `time.time` is the clock; `time.sleep` the pause; `stop=lambda: shutdown.requested` links the use case to the signal flag. The `finally` block always closes the publisher, even if an error is raised or a signal arrived.
- Lines 69 to 71: print the one result line to stdout. In `{time.time() - started:.1f}`, `:.1f` means "one digit after the decimal point".
- Line 72: return 0 for success.

### The entry-point lines

```python
if __name__ == "__main__":
    raise SystemExit(main())
```

`__name__` is a special variable: when a file is run directly (as `python -m ...` does) its value is `"__main__"`; when the file is merely imported by another file, it is the module's name. So these two lines run `main()` only when the file is started as a program, never when a test imports it. `SystemExit(code)` is an exception that Python treats as "end the process with this exit code".

## `consume_cli.py`: one member of the consumer group

### Header, imports and constants

```python
"""A member of the consumer group `order-history`: reads events and checks each order's sequence.

python -m order_events.interfaces.consume_cli --until-idle 5 --print-history 3
python -m order_events.interfaces.consume_cli --name consumer-1      # runs until Ctrl-C or SIGTERM
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections.abc import Sequence

from ..application.ports import Delivery
from ..application.read_orders import ReadPlan, read_orders
from ..infrastructure.kafka_source import KafkaEventSource
from ..infrastructure.logging import configure_logging, fields
from ..infrastructure.settings import ConfigurationError, Settings
from ..infrastructure.shutdown import ShutdownSignal
from .arguments import non_negative_float, non_negative_int

logger = logging.getLogger("order_events.consume")
REPORT_LIMIT = 5  # log the first few out-of-order events; the summary counts all of them
```

- Line 23: a logger with an explicit name, `order_events.consume`, so its lines read `logger=order_events.consume`.
- Line 24: `REPORT_LIMIT = 5` is a constant (capital letters): only the first five out-of-order events get their own log line, so an unkeyed run (about 1,700 of them) does not flood the screen. The summary line still counts all of them.

### The parser

```python
def build_parser() -> argparse.ArgumentParser:
    """The argument parser of the consumer program."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--name", default="consumer", help="client id and label in the output")
    parser.add_argument("--group", default=None, help="defaults to $GROUP_ID")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    parser.add_argument(
        "--until-idle",
        type=non_negative_float,
        default=None,
        help="exit after this many seconds without a new event (default: run until stopped)",
    )
    parser.add_argument("--delay", type=non_negative_float, default=0.0, help="artificial seconds of work per event")
    parser.add_argument("--prefix", default="", help="only check orders whose id starts with this")
    parser.add_argument(
        "--print-history",
        type=non_negative_int,
        default=0,
        metavar="N",
        help="at exit, print the history of the first N orders",
    )
    return parser
```

- `--name` is the client id and the label in the output (the scale script starts several with `consumer-1`, `consumer-2`...). `--group` and `--topic` default to the environment values.
- `--until-idle` is a non-negative float, default `None`: exit after that many seconds without a new event. Without it, the program runs until stopped.
- `--delay` is the artificial work per event (the lag demo uses it).
- `--prefix` limits the check to orders with that id prefix.
- `--print-history N` prints the history of the first N orders at exit. `metavar="N"` is the name shown in the help text for the value.

### `main`

```python
def main(argv: Sequence[str] | None = None) -> int:
    """Run one consumer-group member; returns the process exit code."""
    args = build_parser().parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    shutdown = ShutdownSignal()
    shutdown.install()
```

Same start as the producer: parse arguments, read settings (exit 2 on a configuration error), configure logging, install the signal flag.

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

- Line 63: `reported = 0` counts how many out-of-order events were logged.
- Lines 65 to 77: `on_out_of_order` is defined inside `main`, so it can use `reported`. Because it *changes* `reported`, which belongs to the surrounding function, line 66 says `nonlocal reported`; without it Python would treat `reported += 1` as making a new local variable and fail. This is a **closure** with a changeable captured variable. The function logs a warning with the order, sequence, partition and offset, but only for the first five.

```python
    source = KafkaEventSource(settings, args.name, args.group, args.topic)
    plan = ReadPlan(idle_timeout_seconds=args.until_idle, work_delay_seconds=args.delay, order_prefix=args.prefix)
    try:
        result = read_orders(
            source,
            plan,
            stop=lambda: shutdown.requested,
            clock=time.time,
            sleep=time.sleep,
            on_out_of_order=on_out_of_order,
            histories=args.print_history,
        )
    finally:
        source.close()
    print(f"[{args.name}] read {result.events_read} events  {result.report.summary()}")
    for order_id, partitions, history in result.first_histories:
        print(f"  {order_id} (partition {list(partitions)}): {' -> '.join(history)}")
    return 0
```

- Line 79: builds the real consumer (`KafkaEventSource`) with the member's name, group and topic.
- Line 80: builds a `ReadPlan` from `--until-idle`, `--delay` and `--prefix`.
- Lines 81 to 92: runs the use case. `stop=lambda: shutdown.requested`, `clock=time.time`, `sleep=time.sleep` wire real time into it. The `finally` calls `source.close()`, which commits the last offsets and leaves the group even when interrupted.
- Line 93: prints `[consumer-1] read 3000 events  orders: 1000  ...` using the report's own `summary()`.
- Lines 94 and 95: for each of the first histories prints one line. `list(partitions)` shows `[3]`; `' -> '.join(history)` glues the event-type texts with arrows.
- Line 96: return 0.

## `health_cli.py`: the health check

```python
"""Health command: exit 0 when the broker is reachable and the topic has the designed partitions.

python -m order_events.interfaces.health_cli
"""

from __future__ import annotations

import sys
from collections.abc import Sequence

from ..application.health import check_health
from ..infrastructure.kafka_health import KafkaTopicProbe
from ..infrastructure.logging import configure_logging
from ..infrastructure.settings import ConfigurationError, Settings


def main(_argv: Sequence[str] | None = None) -> int:
    """Check the broker and the topic; returns 0 when healthy."""
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    report = check_health([KafkaTopicProbe(settings)])
    for result in report.results:
        print(f"{'ok' if result.ok else 'FAIL'} {result.name}: {result.detail}")
    return 0 if report.healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

- Line 17: the parameter is named `_argv` with a leading underscore because this program takes no arguments; the name tells readers and tools the value is unused.
- Lines 19 to 23: settings, or exit 2.
- Line 25: `check_health([KafkaTopicProbe(settings)])` runs the single probe.
- Lines 26 and 27: prints one line per probe: `ok kafka: topic order-events has 6 partitions` or `FAIL kafka: ...`. The f-string contains a quoted choice inside brackets: `{'ok' if result.ok else 'FAIL'}`.
- Line 28: `return 0 if report.healthy else 1`. This is what `make health` (and the `HEALTHCHECK` of the `Dockerfile`) use: the exit code is the answer.

## `lag_cli.py`: lag per partition

```python
"""Print the lag of the consumer group per partition (committed offset against log-end offset).

python -m order_events.interfaces.lag_cli
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from ..application.group_lag import group_lag
from ..infrastructure.kafka_offsets import KafkaOffsetReader
from ..infrastructure.logging import configure_logging
from ..infrastructure.settings import ConfigurationError, Settings


def main(argv: Sequence[str] | None = None) -> int:
    """Print the group's lag per partition; returns the process exit code."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--group", default=None, help="defaults to $GROUP_ID")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    group, topic = args.group or settings.group_id, args.topic or settings.topic
    lag = group_lag(KafkaOffsetReader(settings), group, topic)
    print(f"{'PARTITION':<10}{'LAG':>10}")
    for partition, value in sorted(lag.items()):
        print(f"{partition:<10}{value:>10}")
    print(f"{'total':<10}{sum(lag.values()):>10}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- Lines 20 to 23: this program builds its small parser inside `main` (two options only).
- Line 30: `group, topic = args.group or settings.group_id, args.topic or settings.topic` assigns two names at once from two values separated by a comma; `x or y` picks the argument if given, else the setting.
- Line 31: calls the use case with a real `KafkaOffsetReader`.
- Lines 32 to 35: prints a table. In `f"{'PARTITION':<10}{'LAG':>10}"` the part after the colon is a format specification: `<10` pads on the right to 10 characters (left-aligned), `>10` pads on the left (right-aligned). `sorted(lag.items())` lists the pairs in partition order. The last line prints the sum of all lags.

## Try it yourself

```bash
.venv/bin/python -m order_events.interfaces.produce_cli --help
.venv/bin/python -m order_events.interfaces.produce_cli --orders -5; echo "exit code: $?"
```

Expected: the first prints the docstring and the list of options. The second prints a usage line and `produce_cli.py: error: argument --orders: must be at least 1, got -5`, and the echo prints `exit code: 2`. No broker is needed for either.

```bash
BOOTSTRAP_SERVERS=nonsense .venv/bin/python -m order_events.interfaces.health_cli; echo "exit code: $?"
```

Expected: `configuration error: BOOTSTRAP_SERVERS must be host:port[,host:port], got 'nonsense'` on stderr and `exit code: 2`. Here `BOOTSTRAP_SERVERS=nonsense` placed before the command sets that variable for that one command only.

## Mistakes people make

- Printing logs to stdout: then `program > result.txt` fills the file with log lines. Logs go to stderr here.
- Calling `sys.exit` or `exit()` deep inside the code: only the `__main__` block here ends the process; everything else returns values or raises named errors.
- Forgetting `nonlocal`: `reported += 1` would raise `UnboundLocalError`.

## Check yourself

1. What are the three exit codes used here, and what does each mean? (0 success, 1 unhealthy, 2 configuration or argument error.)
2. Why is the code in `if __name__ == "__main__":` and not at the top of the file? (A test can import the module without starting the program.)
3. Why is `publisher.close()` in a `finally` block? (It must run on success, failure and signals.)
