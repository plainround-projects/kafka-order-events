# GNU Make

## What it is

`make` is a command-line tool that runs named recipes from a file called `Makefile`. Each **target** (a name) lists its **prerequisites** (other targets or files it needs first) and a **recipe** (the shell commands that produce it). Make was built to rebuild files only when their inputs changed, but it is also a common way to give a project short, memorable commands.

This repository uses GNU Make 3.81 (the version Apple ships). Every feature below works in it.

## Why this repository uses it

The template says a reader should need only `make up`, `make run`, `make test`, `make lint`, `make down`. The Makefile hides the long commands (virtual environment, Docker, Kafka tools) behind those names.

## Where it appears

| Path | What it does |
| --- | --- |
| `Makefile` | Every command the README mentions. |
| `.github/workflows/ci.yml` | Runs `make check`, `make audit`, `make test-integration`, `make test-e2e`. |
| `README.md` | Lists the targets. |

## How it works underneath

You type `make up`. Make reads `Makefile`, finds the target `up`, first builds each prerequisite that is out of date, then runs the recipe lines one at a time, each in a fresh shell. If any line exits with a failure code, Make stops. A target that is a **file** is rebuilt only if a prerequisite file is newer; a **phony** target (not a real file) always runs.

## Walkthrough of this repository's use

The whole `Makefile` is read below, top to bottom, in the order it appears. Every line is explained.

First, three ideas you need.

- A **shell** is the program that reads a command line and starts the programs named in it (`bash` or `zsh`). Make does not run commands itself. It hands each recipe line to a shell (`/bin/sh`).
- An **environment variable** is a named piece of text that a process receives from the process that started it. For example, if the variable `TOPIC` has the value `order-events`, a Python program can read that with `os.environ["TOPIC"]`.
- A file's **modification time** is the moment it was last changed. Make compares these times to decide whether work is needed.

### The first line: a comment

```make
# make up | run | run-unkeyed | order | scale | lag | health | check | down      (make help lists everything)
```

A line that starts with `#` is a **comment**. Make ignores it. It tells the reader the short list of targets to remember; `make help` lists all of them.

### The variables

```make
PYTHON ?= $(shell command -v python3.13 || command -v python3.12 || command -v python3)
VENV := .venv
PY := $(VENV)/bin/python
STAMP := $(VENV)/.installed
TOPIC ?= order-events
GROUP_ID ?= order-history
KAFKA_BIN := docker compose exec -T kafka /opt/kafka/bin
PRODUCE := $(PY) -m order_events.interfaces.produce_cli
CONSUME := $(PY) -m order_events.interfaces.consume_cli
```

A **variable** gives a name to a piece of text. You write `NAME = text` or `NAME := text` to set it and `$(NAME)` to use it. The three kinds of assignment used here:

- `?=` sets the variable **only if it is not already set**. That lets a person override it on the command line (`make TOPIC=my-topic run`) or from the environment.
- `:=` sets it **once, right now**, and uses the finished text every time.
- `=` (not used here) would re-evaluate every time it is used.

Line by line:

- Line 2: `PYTHON ?= $(shell command -v python3.13 || command -v python3.12 || command -v python3)`. `$(shell ...)` runs a command and uses what it prints. `command -v NAME` prints the path of a program if the shell can find it and prints nothing otherwise. The `||` means "if the previous command failed, try the next one". So the variable becomes the first of Python 3.13, 3.12, or plain `python3` that exists on the machine.
- Line 3: `VENV := .venv` is the folder name of the virtual environment (an isolated folder of installed packages; see `tech/pip-venv-and-lock-files.md`).
- Line 4: `PY := $(VENV)/bin/python` becomes `.venv/bin/python`, the Python inside that folder. Using it directly means no need to "activate" the environment.
- Line 5: `STAMP := $(VENV)/.installed` is the name of an empty file that the install step creates to remember that it finished.
- Lines 6 and 7: the default topic and consumer group. They are overridable with `?=`.
- Line 8: `KAFKA_BIN := docker compose exec -T kafka /opt/kafka/bin` is the start of a long command: run a program **inside the running container** called `kafka`. `docker compose exec` runs a command in an existing container; `-T` means "do not attach a terminal" (needed when the output is piped or there is no keyboard, such as in CI); `/opt/kafka/bin` is the folder inside the Kafka image where its command-line tools live. Recipes below add a script name to it, as in `$(KAFKA_BIN)/kafka-topics.sh`.
- Lines 9 and 10: `PRODUCE` and `CONSUME` store the two Python commands, so the recipes below stay short.

### Reading `.env` and passing variables on

```make
-include .env
export
```

- Line 12: `-include .env` reads another file as part of the Makefile. A file named `.env` (if you create one from `.env.example`) holds lines like `TOPIC=order-events`, which Make understands as variable assignments. The leading `-` means "do not complain if the file is missing". See `tech/dotenv.md`.
- Line 13: `export` on a line by itself tells Make to put **every** variable it knows into the environment of the commands it runs. That is how a value set in `.env` (or on the command line) reaches the Python programs, which read settings from the environment. A side effect: helper variables such as `N` and `ORDER` also reach the programs; they are harmless here because no program reads them.

### The list of phony targets

```make
.PHONY: help install up describe reset-topic run run-unkeyed order scale lag lag-report health \
        lint format typecheck test test-integration test-e2e audit check docker-build down clean
```

A **target** is the thing a rule makes; normally it is a file name. `.PHONY` lists targets that are only **names for actions**, not files. Without the list, if a file called `test` ever appeared in the folder, `make test` would say "nothing to do, the file exists". The backslash at the end of line 15 continues the list on line 16. Every target in this Makefile is listed except the stamp file `.venv/.installed`, which really is a file.

### `help`

```make
help: ## list the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | sort
```

- Line 18: `help: ## list the targets`. The part before the colon is the target name. After the colon go the prerequisites (none here). Then `## list the targets` is a comment by a convention of this repository: every target that should appear in the help list has a `##` description.
- Line 19, the **recipe**: the tab at the start is required (Make rejects spaces here). `@` hides the command itself from the output. `grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST)` searches (`-E` allows the extended pattern) every line that begins with a lowercase name, then a colon, then anything, then `## `. `$(MAKEFILE_LIST)` is a built-in variable holding the names of the Makefiles read, here just `Makefile`. The `|` pipe sends those lines to `sed -E 's/:.*## /\t/'`, which replaces everything from the colon through `## ` with a tab, leaving `name<TAB>description`. The last pipe sends the result to `sort`, which puts the lines in alphabetical order.

### Building the virtual environment (a file target)

```make
$(STAMP): requirements-dev.lock pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q --require-hashes -r requirements-dev.lock
	$(VENV)/bin/pip install -q --no-deps -e .
	touch $@

install: $(STAMP) ## create .venv from the lock file
```

- Line 21: `$(STAMP): requirements-dev.lock pyproject.toml`. This target **is a file**: `.venv/.installed`. Its **prerequisites** are the two files after the colon. Make's rule: if the target does not exist, or if any prerequisite has a newer modification time than the target, run the recipe. So the environment is built once, and rebuilt automatically when someone edits the lock file or `pyproject.toml`.
- Line 22: `$(PYTHON) -m venv $(VENV)` creates the virtual environment folder.
- Line 23: `$(VENV)/bin/pip install -q --require-hashes -r requirements-dev.lock` installs every package listed in the lock file. `-q` is quiet. `--require-hashes` makes pip refuse any package whose file does not match the fingerprint (hash) written in the lock file. `-r FILE` means "read the requirements from this file".
- Line 24: `$(VENV)/bin/pip install -q --no-deps -e .` installs this project itself. `-e` means **editable**: the installed copy points at the `src/` folder, so edits to the code take effect with no reinstall. `--no-deps` means "do not install dependencies", because line 23 already did, with hashes. The `.` means "the project in the current folder".
- Line 25: `touch $@`. `$@` is an **automatic variable** that means "the name of the target being built", here `.venv/.installed`. `touch` creates an empty file or updates its time. Creating it is how the rule remembers it has finished.
- Line 27: `install: $(STAMP) ## create .venv from the lock file`. A phony target whose only prerequisite is the stamp: running `make install` builds the stamp if needed. Most targets below also list `$(STAMP)` so the environment exists before they run.

### Starting Kafka

```make
# Start the broker and create the topic: six partitions, replication factor 1, seven-day retention.
up: $(STAMP) ## start Kafka and create the topic
	docker compose up -d --wait
	$(KAFKA_BIN)/kafka-topics.sh --create --if-not-exists --topic $(TOPIC) --partitions 6 \
	  --replication-factor 1 --config retention.ms=604800000 --bootstrap-server localhost:9092
	$(MAKE) --no-print-directory describe

describe: ## show the topic's partitions and settings
	$(KAFKA_BIN)/kafka-topics.sh --describe --topic $(TOPIC) --bootstrap-server localhost:9092
```

- Line 29: a comment explaining the next target.
- Line 30: `up: $(STAMP) ## start Kafka and create the topic`. It depends on the stamp, so the Python environment is built first.
- Line 31: `docker compose up -d --wait` starts the services in `docker-compose.yml`. `-d` ("detached") runs them in the background. `--wait` makes the command return only when the service's health check reports healthy, so the next line does not hit a broker that is still starting. See `tech/docker-compose.md`.
- Lines 32 and 33: one command split with a trailing `\` into two lines. It runs Kafka's `kafka-topics.sh` inside the container with `--create` (make a topic), `--if-not-exists` (do nothing if it is already there, so `make up` can be repeated), `--topic $(TOPIC)`, `--partitions 6`, `--replication-factor 1` (one copy of each partition), `--config retention.ms=604800000` (keep records for 604,800,000 ms, which is seven days), and `--bootstrap-server localhost:9092` (the address of the broker, as seen from inside the container).
- Line 34: `$(MAKE) --no-print-directory describe` runs Make again for the `describe` target. `$(MAKE)` stands for the Make program currently running, so flags carry over. `--no-print-directory` stops the "entering directory" noise.
- Lines 36 and 37: `describe` runs `kafka-topics.sh --describe` and prints each partition's leader, replicas and in-sync replicas. The lesson's first checklist item reads this output.

### Recreating the topic

```make
# Delete and recreate the topic, so a run never meets order ids left over from an earlier one.
reset-topic:
	-$(KAFKA_BIN)/kafka-topics.sh --delete --topic $(TOPIC) --bootstrap-server localhost:9092
	@sleep 3
	$(KAFKA_BIN)/kafka-topics.sh --create --topic $(TOPIC) --partitions 6 \
	  --replication-factor 1 --config retention.ms=604800000 --bootstrap-server localhost:9092
```

- Line 41: the leading `-` before the command means **ignore a failure**: normally if a recipe line exits with a non-zero code, Make stops. Here the delete fails on the very first run (the topic does not exist yet, or was just made), and that is fine.
- Line 42: `@sleep 3` waits three seconds, because Kafka deletes a topic in the background and an immediate re-create could be refused.
- Lines 43 and 44: create the topic again with the same settings (no `--if-not-exists`: it must be new).
- The target has no `##`, so `make help` hides it: it is a helper for `run`, not meant to be typed.

### The keyed run

```make
# The keyed run: 1,000 orders, then the group reads them and checks every order's sequence.
run: $(STAMP) reset-topic ## keyed run: 1,000 orders produced, then read and checked
	$(PRODUCE) --orders 1000
	$(CONSUME) --until-idle 5 --print-history 3
```

- Line 47: `run: $(STAMP) reset-topic ## ...` has two prerequisites. Make builds them first, left to right, so every `make run` starts with an empty topic.
- Line 48: `$(PRODUCE) --orders 1000` expands to `.venv/bin/python -m order_events.interfaces.produce_cli --orders 1000`.
- Line 49: `$(CONSUME) --until-idle 5 --print-history 3` reads everything, exits after five quiet seconds and prints the history of three orders.

### The unkeyed run

```make
# The same events with no key, in the same topic: the checker logs out-of-order events.
run-unkeyed: $(STAMP) ## the failure mode: the same events with no key
	@prefix=unkeyed-$$(date +%s); \
	$(PRODUCE) --orders 1000 --no-key --prefix $$prefix && \
	$(CONSUME) --until-idle 5 --group unkeyed-check-$$prefix --prefix $$prefix
```

- Line 53: `@prefix=unkeyed-$$(date +%s); \`. Make treats a dollar sign as the start of a Make variable, so a dollar sign meant for the **shell** is written twice: `$$` reaches the shell as `$`. The shell then runs `date +%s` (the current time in seconds since 1970) and stores `unkeyed-` plus that number in a shell variable `prefix`. The `;` ends the command. The backslash continues the *same* recipe line, which matters: Make runs **each recipe line in its own new shell**, so a variable set in one line would be lost in the next. Joining the lines with `\` keeps one shell.
- Line 54: `$(PRODUCE) --orders 1000 --no-key --prefix $$prefix && \` produces 1,000 unkeyed orders with ids that start with that prefix. `&&` means "run the next command only if this one succeeded".
- Line 55: `$(CONSUME) --until-idle 5 --group unkeyed-check-$$prefix --prefix $$prefix` reads in a new group, so it starts from the beginning, and checks only the orders with that prefix.

### Showing one order

```make
# make order ORDER=order-00042: every event of one order with its partition (the console consumer's view).
ORDER ?= order-00042
order: ## show one order's events and partitions (ORDER=order-00042)
	$(KAFKA_BIN)/kafka-console-consumer.sh --topic $(TOPIC) --from-beginning --timeout-ms 8000 \
	  --property print.key=true --property print.partition=true --bootstrap-server localhost:9092 2>/dev/null \
	  | grep "^Partition:[0-9]*[[:space:]]$(ORDER)[[:space:]]"
```

- Line 58: `ORDER ?= order-00042` is the default order to look at; `make order ORDER=order-00001` changes it.
- Lines 60 and 61: runs Kafka's console consumer inside the container: `--from-beginning` starts at the oldest record, `--timeout-ms 8000` ends the program after 8 seconds with no new records, and the two `--property` options print each record's key and partition. `2>/dev/null` throws away standard error (the tool prints a count line there). `/dev/null` is a special file that discards what is written to it.
- Line 62: `| grep "^Partition:[0-9]*[[:space:]]$(ORDER)[[:space:]]"` keeps only the lines that start with `Partition:`, a number, a space or tab, the chosen order id, and another space or tab. `[[:space:]]` matches one whitespace character. The result is the order's three events with their partition number, which is the lesson's check that all three share a partition.

### Scale, lag, health

```make
# make scale N=3 or N=8: consumers in the group, then the group description.
N ?= 3
scale: $(STAMP) ## run N consumers in the group and describe it (N=3 or N=8)
	scripts/scale.sh $(N)

lag: $(STAMP) ## build a 50,000-event backlog and time its drain
	scripts/lag.sh

lag-report: $(STAMP) ## print the group's lag per partition with this project's own reader
	$(PY) -m order_events.interfaces.lag_cli

health: $(STAMP) ## exit 0 when the broker is reachable and the topic has six partitions
	$(PY) -m order_events.interfaces.health_cli
```

- Line 65: `N ?= 3` is the default number of consumers.
- Lines 66 and 67: `scale` runs the script `scripts/scale.sh` with `$(N)` as its argument (see `tech/bash.md`).
- Lines 69 and 70: `lag` runs `scripts/lag.sh`.
- Lines 72 and 73: `lag-report` runs this project's own Python program that prints the lag table.
- Lines 75 and 76: `health` runs the health program. Its exit code becomes Make's: if the program exits non-zero, `make health` fails too.

### Lint and format

```make
lint: $(STAMP) ## ruff check, ruff format check, mypy, import-linter
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .
	$(VENV)/bin/mypy
	$(VENV)/bin/lint-imports

format: $(STAMP) ## apply ruff's formatting and safe fixes
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/ruff format .
```

- Lines 79 to 82: `lint` runs four checkers in order, each in the virtual environment: `ruff check .` (style and bug patterns), `ruff format --check .` (is every file already formatted? fail if not), `mypy` (type hints), and `lint-imports` (the layer rules). If any line fails, Make stops at once, so the target fails. See `tech/ruff.md`, `tech/mypy.md` and `tech/import-linter.md`.
- Lines 84 to 86: `format` fixes what can be fixed automatically: `ruff check --fix .` and `ruff format .`.

### Tests

```make
# Unit tests need no broker. Coverage is reported for the package; the domain and application must stay at 95% or more.
test: $(STAMP) ## unit tests with coverage (no broker needed)
	$(PY) -m pytest tests/unit --cov=order_events --cov-report=term
	$(PY) -m coverage report --include='src/order_events/domain/*,src/order_events/application/*' --fail-under=95

test-integration: $(STAMP) ## adapters against the running broker (make up first)
	$(PY) -m pytest tests/integration -m integration

test-e2e: $(STAMP) ## the lesson's scenario through the command-line programs (make up first)
	$(PY) -m pytest tests/e2e -m e2e
```

- Line 88: a comment saying what the target promises.
- Lines 89 to 91: `test` runs the unit tests with coverage measurement (`--cov=order_events` measures the package, `--cov-report=term` prints the table in the terminal). The second line, `coverage report --include=... --fail-under=95`, prints the coverage of only the domain and application folders and exits non-zero if it is under 95%. The `--include` value is in single quotes so the shell does not expand the `*`.
- Lines 93 and 94: `test-integration` runs `pytest tests/integration -m integration`. `-m integration` selects tests carrying that mark.
- Lines 96 and 97: `test-e2e` is the same for the end-to-end tests.

### Audit, check, build, stop, clean

```make
audit: $(STAMP) ## check the locked dependencies for known vulnerabilities
	$(VENV)/bin/pip-audit --no-deps --disable-pip -r requirements-dev.lock

check: lint test ## everything that needs no broker

docker-build: ## build the container image of the command-line programs
	docker build -t kafka-order-events:dev .

down: ## stop Kafka and delete its data
	docker compose down -v

clean: down
	rm -rf $(VENV) logs .pytest_cache .mypy_cache .ruff_cache .coverage
```

- Lines 99 and 100: `audit` runs `pip-audit` on the lock file: `--no-deps` (the lock file already lists everything), `--disable-pip` (do not build a new environment to check), `-r` (read from this file). It reports packages with known security problems. See `tech/pip-audit.md`.
- Line 102: `check: lint test`. This target has no recipe: it only has prerequisites. Running `make check` therefore runs `lint` and then `test`, which is "everything that needs no broker".
- Lines 104 and 105: `docker-build` runs `docker build -t kafka-order-events:dev .`. `-t` names (tags) the image; the `.` says "use the Dockerfile in this folder".
- Lines 107 and 108: `down` runs `docker compose down -v`: stop and remove the containers and, because of `-v`, the data volumes too. That is why the README says it "deletes its data".
- Lines 110 and 111: `clean: down` first runs `down`, then `rm -rf` deletes the virtual environment, the logs folder, and the caches of pytest, mypy, ruff and coverage. `-r` means "folders too" and `-f` means "no questions, no errors if missing". Be careful: `rm -rf` cannot be undone, and the names here are fixed, so it only removes what the project itself created.

## Try it yourself

1. `make help` lists every target with its description.
2. `make -n up` (dry run) prints the commands `up` would run without running them.
3. `make -n scale N=8` shows `scripts/scale.sh 8`.
4. `make -n test` shows the pytest and coverage commands.
5. `make PYTHON=python3.12 -n install` shows the chosen interpreter in the first recipe line (if the target is already built, run `make -B -n install`).

## Mistakes people make

- Using spaces instead of a tab at the start of a recipe line.
- Forgetting `.PHONY`, so a folder named like a target blocks it.
- Expecting `cd` in one recipe line to affect the next: each line is a fresh shell. Join lines with `&&` and `\` (as `run-unkeyed` does).
- Writing `$` for the shell when Make reads it first. Write `$$`.
- Exporting every variable. This repository's `export` is deliberate, and the demo variables carry names (`N`, `ORDER`) that no program reads.

## Check yourself

1. What is a prerequisite? *A target or file that must be up to date before this target runs.*
2. What does `?=` do? *Sets a variable only if it is not already set.*
3. Why list `.PHONY` targets? *To stop a file with the same name from blocking them.*
4. What does a leading `-` on a recipe line mean? *Ignore a failure of that line.*
5. How does the install stamp avoid reinstalling? *The stamp file is rebuilt only when `requirements-dev.lock` or `pyproject.toml` is newer than it.*

## Further reading

- GNU Make manual (3.81 behavior; later versions only add): https://www.gnu.org/software/make/manual/make.html
