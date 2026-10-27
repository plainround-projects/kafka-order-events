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

The file lists: the Kafka address (`BOOTSTRAP_SERVERS`), the topic, group and partition count (`TOPIC`, `GROUP_ID`, `PARTITIONS`), the log level and the two Kafka timeouts.

```make
-include .env
```

```make
export
```

```python
def _text(environ: Mapping[str, str], name: str, default: str) -> str:
    value = environ.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value
```

`_text` reads one variable (or its default), strips spaces and refuses an empty value. `Settings.from_env` applies defaults and checks the formats (a server list like `host:port`, a known log level). Every variable has a default that suits the compose setup, so `.env` is optional.

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
