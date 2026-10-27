# logfmt (structured log lines)

## What it is

A log line is one line of text a program writes to say what it is doing. A **structured** log line has a fixed shape that a machine can split into named fields. `logfmt` is a very simple shape: a space-separated list of `key=value` pairs, with double quotes around a value that contains spaces. It is a convention, not a library or a standard document.

## Why this repository uses it

The production-grade rule in `TEMPLATE.md` asks for structured logging with levels. `logfmt` stays readable to a person in a terminal and is easy for a tool to search (`grep level=WARNING`).

## Where it appears

| Path | What it does with logfmt |
| --- | --- |
| `src/order_events/infrastructure/logging.py` | Formats every log record as one logfmt line. |
| `src/order_events/infrastructure/kafka_source.py`, `interfaces/consume_cli.py` | Write log records with extra fields. |
| `tests/unit/infrastructure/test_logging_and_shutdown.py` | Checks the format. |

## How it works underneath

Python's `logging` module creates a **record** for each call such as `logger.info("partitions assigned", extra=fields(member="consumer-1"))`. A **handler** decides where the record goes (here: standard error, the second output stream every program has, meant for diagnostics). A **formatter** turns the record into text. This repository supplies its own formatter. The level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) lets you hide noise: a setting of `INFO` drops `DEBUG` records.

## Walkthrough of this repository's use

```python
def _quote(value: object) -> str:
    text = str(value)
    return json.dumps(text) if not text or any(char in text for char in ' ="\n') else text


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

- `_quote` wraps a value in JSON quotes when it is empty or contains a space, `=`, a double quote or a newline; otherwise it leaves it bare. That is the whole logfmt rule.
- `LogfmtFormatter.format` builds a list of parts: a UTC timestamp (`isoformat(timespec="milliseconds")`), the level, the logger name, the message, then every extra field from `getattr(record, "fields", {})`. `" ".join(parts)` makes the line. If an exception is attached (`record.exc_info`), its traceback text is added as `exc=...` on the same line.
- `fields(**values)` returns `{"fields": values}`. Passing it as `extra=` is how callers attach key-value pairs to a record.
- `configure_logging` replaces the root logger's handlers (so calling it twice does not duplicate lines), sets the level, and keeps the noisy `kafka` library logger at WARNING or above.

An output line looks like this:

```text
ts=2026-10-26T20:35:43.075+00:00 level=INFO logger=order_events.infrastructure.kafka_source msg="partitions assigned" member=consumer partitions="none (idle)"
```

## Try it yourself

1. `.venv/bin/python -c "import logging; from order_events.infrastructure.logging import *; configure_logging('INFO'); logging.getLogger('demo').info('hello world', extra=fields(a=1, b='x y'))"` prints one line ending `msg="hello world" a=1 b="x y"` on standard error.
2. Same with `'WARNING'` instead of `'INFO'`: nothing prints.
3. Keep only warnings from a run: `make run 2>&1 >/dev/null | grep level=WARNING`.

## Mistakes people make

- Logging with string concatenation and no fields, which makes searching hard.
- Putting secrets in log fields. This repository has none; the idempotent-consumer repository hides its password from logs.
- Writing results and logs to the same stream. Here results go to standard output and logs to standard error, so each can be redirected alone.

## Check yourself

1. What are the parts of a logfmt line? *Space-separated `key=value` pairs.*
2. When is a value quoted? *When it is empty or contains a space, `=`, a quote or a newline.*
3. What does the level `INFO` hide? *`DEBUG` records.*
4. How do you attach a field to a record? *`extra=fields(name=value)`.*
5. Why call `configure_logging` twice safely? *It replaces the handlers instead of adding another.*

## Further reading

- Python 3.12 logging: https://docs.python.org/3.12/library/logging.html
- The logfmt convention (Brandur Leach's write-up): https://brandur.org/logfmt
