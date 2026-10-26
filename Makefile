# make up | run | run-unkeyed | order | scale | lag | health | check | down      (make help lists everything)
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

.PHONY: help install up describe reset-topic run run-unkeyed order scale lag lag-report health \
        lint format typecheck test test-integration test-e2e audit check docker-build down clean

help: ## list the targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/' | sort

$(STAMP): requirements-dev.lock pyproject.toml
	$(PYTHON) -m venv $(VENV)
	$(VENV)/bin/pip install -q --require-hashes -r requirements-dev.lock
	$(VENV)/bin/pip install -q --no-deps -e .
	touch $@

install: $(STAMP) ## create .venv from the lock file

# Start the broker and create the topic: six partitions, replication factor 1, seven-day retention.
up: $(STAMP) ## start Kafka and create the topic
	docker compose up -d --wait
	$(KAFKA_BIN)/kafka-topics.sh --create --if-not-exists --topic $(TOPIC) --partitions 6 \
	  --replication-factor 1 --config retention.ms=604800000 --bootstrap-server localhost:9092
	$(MAKE) --no-print-directory describe

describe: ## show the topic's partitions and settings
	$(KAFKA_BIN)/kafka-topics.sh --describe --topic $(TOPIC) --bootstrap-server localhost:9092

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

# The same events with no key, in the same topic: the checker logs out-of-order events.
run-unkeyed: $(STAMP) ## the failure mode: the same events with no key
	@prefix=unkeyed-$$(date +%s); \
	$(PRODUCE) --orders 1000 --no-key --prefix $$prefix && \
	$(CONSUME) --until-idle 5 --group unkeyed-check-$$prefix --prefix $$prefix

# make order ORDER=order-00042: every event of one order with its partition (the console consumer's view).
ORDER ?= order-00042
order: ## show one order's events and partitions (ORDER=order-00042)
	$(KAFKA_BIN)/kafka-console-consumer.sh --topic $(TOPIC) --from-beginning --timeout-ms 8000 \
	  --property print.key=true --property print.partition=true --bootstrap-server localhost:9092 2>/dev/null \
	  | grep "^Partition:[0-9]*[[:space:]]$(ORDER)[[:space:]]"

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

lint: $(STAMP) ## ruff check, ruff format check, mypy, import-linter
	$(VENV)/bin/ruff check .
	$(VENV)/bin/ruff format --check .
	$(VENV)/bin/mypy
	$(VENV)/bin/lint-imports

format: $(STAMP) ## apply ruff's formatting and safe fixes
	$(VENV)/bin/ruff check --fix .
	$(VENV)/bin/ruff format .

# Unit tests need no broker. Coverage is reported for the package; the domain and application must stay at 95% or more.
test: $(STAMP) ## unit tests with coverage (no broker needed)
	$(PY) -m pytest tests/unit --cov=order_events --cov-report=term
	$(PY) -m coverage report --include='src/order_events/domain/*,src/order_events/application/*' --fail-under=95

test-integration: $(STAMP) ## adapters against the running broker (make up first)
	$(PY) -m pytest tests/integration -m integration

test-e2e: $(STAMP) ## the lesson's scenario through the command-line programs (make up first)
	$(PY) -m pytest tests/e2e -m e2e

audit: $(STAMP) ## check the locked dependencies for known vulnerabilities
	$(VENV)/bin/pip-audit --no-deps --disable-pip -r requirements-dev.lock

check: lint test ## everything that needs no broker

docker-build: ## build the container image of the command-line programs
	docker build -t kafka-order-events:dev .

down: ## stop Kafka and delete its data
	docker compose down -v

clean: down
	rm -rf $(VENV) logs .pytest_cache .mypy_cache .ruff_cache .coverage
