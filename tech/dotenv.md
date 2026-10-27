# `.env` files (environment variables)

## What it is

An **environment variable** is a named setting that every program started by a shell can read, such as `LOG_LEVEL=INFO`. A **`.env` file** is a plain text file of `NAME=value` lines that tools load into the environment so you do not have to type them each time. This repository ships `.env.example` (the template, safe to commit). You may copy it to `.env` (ignored by git) to change a value; nothing in this project is secret, and everything works without a `.env`.

## Why this repository uses it

The template requires configuration to come from the environment. Listing every variable with its default in one file shows a reader all the knobs.

## Where it appears

| Path | What it does |
| --- | --- |
| `.env.example` | Every variable the project reads, with its default. Committed. |
| `.env` | Your optional local copy. Not committed (`.gitignore`). |
| `Makefile` | `-include .env` (skip silently if missing) and `export` load it into every command Make runs. |
| `src/order_events/infrastructure/settings.py` | Reads `os.environ`, checks each value, and refuses wrong ones. |

## How it works underneath

`.env` is just text. Make includes it as if its lines were Make assignments, and `export` passes them on to every program it starts. Docker Compose has its own reader, though this project's compose file uses no placeholders. Python reads the final result through `os.environ`. A value already set in your shell wins over the file in Compose. Lines starting with `#` are comments.

## Walkthrough of this repository's use

First, what an **environment variable** is: a named piece of text that a program receives from the program that started it. Every process has its own set. A shell sets one for a single command like this: `TOPIC=my-topic .venv/bin/python -m order_events.interfaces.lag_cli`.

A `.env` file is a plain text file of `NAME=value` lines, one per line; a `#` starts a comment. It is a convenient place to keep the values for one machine. This repository ships `.env.example`, a copy-me template for the variables the programs read (the docstring of `Settings` points to it), and `.gitignore` and `.dockerignore` both exclude a real `.env` so a personal or secret value is never committed or copied into an image.

### How the values reach the programs

```make
-include .env
export
```

- `-include .env` makes Make read `.env` if the file exists. Make understands `NAME=value` lines as variable assignments, so `TOPIC=something` in `.env` becomes the Make variable `TOPIC`. The `-` means "no error if the file is missing".
- `export` (alone on a line) passes every Make variable to the commands Make starts. So the value in `.env` reaches `python -m order_events...`, which reads its environment.

### What reads them

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

The two helper functions read one variable each. `_text` strips spaces and refuses an empty value. `_positive_int` requires a whole number of at least 1. Their messages begin with the variable's name.

```python
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

`Settings.from_env` is the single place that knows every variable name. The seven variables, their defaults and what each does:

| Variable | Default | Meaning |
| --- | --- | --- |
| `BOOTSTRAP_SERVERS` | `localhost:9092` | The address(es) of Kafka, as `host:port`, or several separated by commas. Checked against a pattern. |
| `TOPIC` | `order-events` | The topic the programs write to and read from. |
| `GROUP_ID` | `order-history` | The consumer group a `consume_cli` joins, and the group `lag_cli` measures. |
| `PARTITIONS` | `6` | How many partitions the health check expects the topic to have. It does not create the topic; `make up` does. |
| `LOG_LEVEL` | `INFO` | One of `DEBUG`, `INFO`, `WARNING`, `ERROR`. Lower-case is accepted. |
| `KAFKA_REQUEST_TIMEOUT_MS` | `30000` | How long a Kafka client waits for the broker's answer before giving up on a request, in milliseconds. |
| `KAFKA_RETRY_BACKOFF_MS` | `100` | The pause between the Kafka client's own retries, in milliseconds. The retry helper uses it as its first pause too. |

Every variable has a default that suits `docker-compose.yml`, so **no `.env` is required**: `make up` and `make run` work on a fresh clone. `.env.example` exists so a reader sees all seven names in one place and can copy it to `.env` to change one. A secret (a password, a key) never belongs in `.env.example`; this project has none.

## Try it yourself

1. `ls -a | grep env` lists `.env.example`.
2. Defaults work: `.venv/bin/python -c "from order_events.infrastructure.settings import Settings; print(Settings.from_env({}).bootstrap_servers)"` prints `localhost:9092`.
3. A wrong value: `BOOTSTRAP_SERVERS=oops .venv/bin/python -m order_events.interfaces.health_cli` prints `configuration error: BOOTSTRAP_SERVERS must be host:port[,host:port], got 'oops'`.
4. `git check-ignore -v .env` shows the `.gitignore` rule that keeps it out of git.

## Mistakes people make

- Committing a `.env` that holds real secrets. Only `.env.example` is committed.
- Quoting values inconsistently: Make, Compose and shells treat quotes differently. This file uses bare values.
- Forgetting that a variable already set in the shell can override the file.
- Letting a program invent defaults for secrets. This project has none; the idempotent-consumer project's database password has no default.

## Check yourself

1. Which file is committed, `.env` or `.env.example`? *`.env.example`.*
2. What does `export` in the Makefile do? *Passes every Make variable to the commands it runs.*
3. Why is `.env` optional here? *Every variable has a working default.*
4. What happens with a wrong value such as `BOOTSTRAP_SERVERS=oops`? *The program prints a configuration error and exits with code 2.*
5. Who reads `.env` in this project? *Make (which exports the values to the programs).*

## Further reading

- Compose environment files: https://docs.docker.com/compose/how-tos/environment-variables/set-environment-variables/
- Twelve-factor app, config: https://12factor.net/config
- GNU Make `include` and `export`: https://www.gnu.org/software/make/manual/make.html#Include
