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

### Variables

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

-include .env
export
```

- `PYTHON ?= $(shell command -v python3.13 || command -v python3.12 || command -v python3)`: `?=` sets the variable only if it is not set already, so `make PYTHON=python3.12 install` wins. `$(shell ...)` runs a command and uses its output; `command -v` prints the path of a program if it exists. The `||` tries the next one.
- `VENV := .venv`: `:=` is evaluated once, right now. `$(VENV)/bin/python` later becomes `.venv/bin/python`.
- `KAFKA_BIN := docker compose exec -T kafka /opt/kafka/bin` stores the start of a long command.
- `-include .env`: read the file `.env` if it exists; the leading `-` means "do not fail if it is missing". `export` with no names passes every Make variable to the commands it runs, so values in `.env` reach the Python programs.

### Phony targets and the help trick

```make
.PHONY: help install up describe reset-topic run run-unkeyed order scale lag lag-report health \
        lint format typecheck test test-integration test-e2e audit check docker-build down clean

help: ## list the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | sort
```

`.PHONY:` lists targets that are not files. Without it, a file named `test` in the folder would make `make test` say "nothing to do". The `help` recipe searches the Makefile itself with `grep` for lines like `name: ... ## text`, and `sed` turns the `## ` into a tab. That is why most targets end with `## description`. The `@` in front of a recipe line stops Make printing the command before running it.

### A file target: the install stamp

```make
$(STAMP): requirements-dev.lock pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q --require-hashes -r requirements-dev.lock
	$(VENV)/bin/pip install -q --no-deps -e .
	touch $@

install: $(STAMP) ## create .venv from the lock file
```

`$(STAMP)` is the file `.venv/.installed`. Its prerequisites are `requirements-dev.lock` and `pyproject.toml`. If either file is newer than the stamp (or the stamp is missing), Make runs the recipe: create a virtual environment, install the locked packages, install this project, and `touch $@` to refresh the stamp (`$@` means "the target's name"). Targets that need the environment list `$(STAMP)` as a prerequisite, so the environment is built once and rebuilt only when the dependencies change. See `tech/pip-venv-and-lock-files.md`.

### Recipes that start Kafka and run programs

```make
# Start the broker and create the topic: six partitions, replication factor 1, seven-day retention.
up: $(STAMP) ## start Kafka and create the topic
	docker compose up -d --wait
	$(KAFKA_BIN)/kafka-topics.sh --create --if-not-exists --topic $(TOPIC) --partitions 6 \
	  --replication-factor 1 --config retention.ms=604800000 --bootstrap-server localhost:9092
	$(MAKE) --no-print-directory describe
```

A line starting with a tab is a recipe line (spaces will not work). A trailing `\` continues the command on the next line. `$(MAKE) --no-print-directory describe` calls Make again for another target; `$(MAKE)` (not `make`) keeps flags consistent.

```make
# Delete and recreate the topic, so a run never meets order ids left over from an earlier one.
reset-topic:
	-$(KAFKA_BIN)/kafka-topics.sh --delete --topic $(TOPIC) --bootstrap-server localhost:9092
	@sleep 3
	$(KAFKA_BIN)/kafka-topics.sh --create --topic $(TOPIC) --partitions 6 \
	  --replication-factor 1 --config retention.ms=604800000 --bootstrap-server localhost:9092

# The keyed run: 1,000 orders, then the group reads them and checks every order's sequence.
run: $(STAMP) reset-topic ## keyed run: 1,000 orders produced, then read and checked
	$(PRODUCE) --orders 1000
	$(CONSUME) --until-idle 5 --print-history 3
```

The recipe line `-$(KAFKA_BIN)/kafka-topics.sh --delete ...` in `reset-topic` starts with `-`: ignore a failure (the topic might not exist yet). `run` depends on `reset-topic`, so a run always starts with an empty topic.

### Passing values on the command line

```make
# make scale N=3 or N=8: consumers in the group, then the group description.
N ?= 3
scale: $(STAMP) ## run N consumers in the group and describe it (N=3 or N=8)
	scripts/scale.sh $(N)
```

`N ?= 3` is a default. `make scale N=8` overrides it, and the recipe passes `$(N)` to the script.

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
