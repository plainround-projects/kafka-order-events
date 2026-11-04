# kafka-order-events

A runnable order-events stream on a single-node Kafka 4 cluster: a producer publishes `OrderPlaced`, `OrderPaid` and `OrderShipped` keyed by order id, and a consumer group prints each order's history, checks per-order ordering, scales across partitions and shows consumer lag.

| | |
| --- | --- |
| **Lesson** | `/prep/kafka/project-order-events`, "Design and run an order-events stream" (`data-structure-explainer/src/content/tracks/kafka/14-project-order-events.md` in the Plainround app) |
| **Planned hours** | 6 |
| **Level** | Beginner |
| **Architecture** | [ARCHITECTURE.md](ARCHITECTURE.md), decisions in [docs/adr](docs/adr) |
| **Technology notes** | [tech/README.md](tech/README.md): one file per technology used here, written for a reader who knows none of them |

## Architecture

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/diagrams/architecture-dark.svg">
  <img alt="The Producer sends events keyed by orderId to the six-partition topic order-events in a single-node Kafka 4 container; Consumers 1 to 3 of the group order-history share the partitions and report to the Group coordinator, and the Lag reader reads the offsets." src="docs/diagrams/architecture.svg">
</picture>

One Producer writes to one six-partition log and one consumer group shares it; the order id as the key is what keeps an order's events in sequence, and the amber paths are the two failures the lesson drills.

## How one run flows

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/diagrams/flow-dark.svg">
  <img alt="The seven steps of make run from topic reset to the summary line, with a warning branch where make run-unkeyed sends key None and events arrive out of order." src="docs/diagrams/flow.svg">
</picture>

`make run` produces 1,000 orders and reads them back with every sequence checked, and the amber rows show what the same run does with no key.

## Prerequisites

| Tool | Version | Used for |
| --- | --- | --- |
| Docker with Compose v2 | 24 or newer (tested with 24.0.6 and Compose v2.23) | the Kafka broker |
| Python | 3.12 or 3.13 (tested with 3.13; the CI uses 3.12) | the programs and tests |
| GNU Make | 3.81 or newer (the one on macOS works) | every command below |
| Kafka image | `apache/kafka:4.0.0`, KRaft mode, no ZooKeeper | pulled by Docker on first `make up` |
| Python packages | `kafka-python` 3.0.12 at run time; pytest, pytest-cov, ruff, mypy, import-linter, pip-audit for development. All pinned with hashes in `requirements.lock` and `requirements-dev.lock`; `make` installs them into `.venv/` | everything |

No cloud account, no secrets. Port 9092 must be free (the broker publishes it and advertises `localhost:9092`).

## Setup, run, test, stop

```bash
make up                 # starts the broker, creates the topic (6 partitions, RF 1, 7-day retention), shows it
make run                # keyed run: 1,000 orders, then the group reads them and checks every order's sequence
make run-unkeyed        # the failure mode: the same events with no key
make scale N=3          # three consumers in the group; N=8 shows two of eight idle
make lag                # a 50,000-event backlog, then one slow consumer drains it (about 11 minutes)
make health             # exit 0 when the broker is reachable and the topic has six partitions
make check              # format check, lint, types, import rules, unit tests with coverage (no broker needed)
make test-integration   # the Kafka adapters against the running broker (needs make up first)
make test-e2e           # the lesson's scenario through the command-line programs (needs make up first)
make audit              # known-vulnerability check of the locked dependencies
make down               # stops the broker and deletes its data
```

`make help` lists every target. `make run` deletes and recreates the topic first, so every run starts from an empty log. `make run-unkeyed`, `make scale` and `make lag` add to the topic that `make run` filled. A quicker `make lag`: `LAG_DELAY=0.002 make lag` (2 ms of work per event instead of 10 ms; 159 seconds measured).

The programs write structured log lines (`ts=... level=... logger=... msg=... key=value`) to standard error and their results to standard output. What `make run` prints on standard output (counts and partitions vary between runs):

```text
produced 3000 events for 1000 orders to order-events (keyed by orderId) in 4.1s
[consumer] read 3000 events  orders: 1000  out-of-order events logged: 0  orders spread over more than one partition: 0
  order-00004 (partition [3]): OrderPlaced -> OrderPaid -> OrderShipped
  order-00024 (partition [3]): OrderPlaced -> OrderPaid -> OrderShipped
  order-00055 (partition [3]): OrderPlaced -> OrderPaid -> OrderShipped
```

## Done when: the lesson's checklist and the proof for each line

| # | Done when (from the lesson) | Command | What proves it |
| --- | --- | --- | --- |
| 1 | The design names one thing that would make you change the key, such as ordering per customer instead of per order. | Read [ADR 0001](docs/adr/0001-key-events-by-order-id-with-six-partitions.md) and ["Decisions and trade-offs"](ARCHITECTURE.md#decisions-and-trade-offs) | The design states the topic, the key `orderId`, six partitions, replication factor 1, seven-day retention and the JSON event, and says what would change the key: a reader that needs one customer's events in order across all their orders. |
| 2 | `kafka-topics.sh --describe` shows six partitions, and you can say what replication factor 1 means for durability. | `make describe` (also printed at the end of `make up`, before any event exists) | `PartitionCount: 6  ReplicationFactor: 1  Configs: retention.ms=604800000`, and six lines `Leader: 1  Replicas: 1  Isr: 1`. Replication factor 1 means each partition has exactly one copy, on broker 1: lose that disk and every event is gone. `Isr: 1` is the copy being in sync with itself, not durability. |
| 3 | The console consumer with `print.key=true` and `print.partition=true` shows every event of one order on the same partition. | `make run`, then `make order ORDER=order-00042` | Three lines, all with the same `Partition:` number, in sequence 1, 2, 3, although the producer interleaved up to 50 orders. |
| 4 | A full run logs zero out-of-order events, and producing without a key logs some, explained in one sentence. | `make run` and `make run-unkeyed` | Keyed: `out-of-order events logged: 0`. Unkeyed: about 1,700 of 3,000 events (1,769 on one run) and `orders spread over more than one partition` close to 1,000. **Why:** ordering is a property of one partition, not of the topic; with no key tying an order's events to one partition, nothing keeps them in sequence. |
| 5 | `kafka-consumer-groups.sh --describe` shows partitions spread across three consumers, and two of the eight consumers sit idle. | `make scale N=3`, then `make scale N=8` | N=3: three members with `#PARTITIONS 2` each. N=8: six members with `#PARTITIONS 1` and two with `#PARTITIONS 0` and an empty assignment. The script waits for the rebalance to settle before it describes. |
| 6 | You can read the lag per partition, watch it fall, and say how long recovery took. | `make lag` (and `make lag-report` for this project's own reader) | Step 3 prints a lag table with `LAG` per partition (about 8,000 each); step 4 prints the total lag about every fifteen seconds, falling to 0, and the last line says how long it took (678 seconds on one run with the default 10 ms per event). |

The same scenarios run as automated tests: [tests/integration](tests/integration) (six partitions and one replica; keyed events share a partition and arrive in order; unkeyed events do not; eight consumers leave two idle; lag grows and drains; the health probe) and [tests/e2e](tests/e2e) (the command-line programs end to end).

## Stretch goals

| Stretch (from the lesson) | Built? |
| --- | --- |
| Run three brokers with replication factor 3, stop one broker, and show producing and consuming carry on. | No. It is the starting point of [kafka-durable-pipeline](../kafka-durable-pipeline), which runs three brokers and kills the leader of a partition while producing. |
| Add an event type with a new optional field and show an old consumer still reads the stream. | No here. Schema evolution with a registry and compatibility checks is built in `kafka-durable-pipeline` (step 6). |
| Try a share group for a "send confirmation email" consumer. | No. Share groups are an early-access feature in Kafka 4.0 and not enabled in this setup; the lesson's point (ordering no longer matters for that consumer) is in the lesson text. |

## What is simplified for teaching

Only what the lesson scopes out, plus where this build differs from the lesson's text:

- One broker that is also the controller, replication factor 1, a plain-text listener on `localhost` (the lesson's single-node cluster).
- Events are bare JSON with no schema registry (the lesson's event format); offsets auto-commit every second.
- The lesson's producer sends one order at a time, so events of different orders never interleave. This producer keeps up to 50 orders open (`--window`) and sends the next event of a random one, which is what the lesson says it is imitating. The random pause after each event defaults to at most 2 ms instead of 50 ms so `make run` takes a few seconds; `--max-delay 0.05` restores the lesson's pause.
- The lesson says "10,000 events for 1,000 orders", but three events per order is 3,000 events; this repository sends 3,000.
- The consumer group is named `order-history`, as in the lesson's diagram (the lesson's step 5 text uses `order-readers`).
- The lesson quotes about 600 records per second with a 10 ms delay per record. One consumer sleeping 10 ms per event cannot exceed 100 per second; measured here: about 75 per second, so 50,000 events take about 11 minutes, not 83 seconds.
- No `Dockerfile` is needed to run the lesson, since the programs run from the host's virtual environment; the `Dockerfile` here packages them (non-root, pinned base image, the health command as its check) and is built by `make docker-build`.

## Project structure

```text
src/order_events/
  domain/            events, ordering checker, lag arithmetic, named errors.
                     Depends on nothing outside the standard library.
  application/       use cases (produce orders, read orders, group lag, health) and the ports
                     they need (EventPublisher, EventSource, OffsetReader, HealthProbe).
                     Depends on domain only.
  infrastructure/    adapters that implement the ports with kafka-python, plus settings
                     (environment, validated), structured logging, retry with backoff, signals.
                     Depends on application and domain.
  interfaces/        command-line programs: produce, consume, health, lag. Wire adapters into use
                     cases and own exit codes. Depends on everything below; nothing depends on it.
tests/
  unit/              domain, application (with in-memory fakes), infrastructure helpers, CLI arguments. No broker.
  integration/       the Kafka adapters against the compose broker.
  e2e/               the lesson's scenario through the command-line programs.
scripts/             scale.sh and lag.sh, the multi-process demos behind make scale and make lag.
docs/adr/            decision records (key and partitions, ports and adapters, the client library).
docker-compose.yml   single-node Kafka 4, KRaft.        Dockerfile  the programs as an image.
pyproject.toml       project metadata and every tool's configuration (ruff, mypy, pytest, import-linter).
requirements*.lock   hash-pinned dependencies (runtime, and runtime plus development tools).
Makefile             the only commands a reader needs (make help).
```

The dependency rule (domain depends on nothing; application on domain; infrastructure and interfaces on both; nothing on interfaces) is enforced by `make lint` through import-linter. A second contract forbids `domain` and `application` from importing `kafka`.

## Troubleshooting

- **Port 9092 is in use:** stop the other Kafka, or change both `9092:9092` and the advertised listener in `docker-compose.yml` and `BOOTSTRAP_SERVERS`.
- **A `--describe` shows a partial assignment after `make scale`:** you described during a rebalance; wait a few seconds and run `kafka-consumer-groups.sh --describe` again.
- **Everything is slow after sleeping the laptop:** `make down && make up`.

## Author and license

Written by Sumit Srivastava. Released under the MIT License, see [LICENSE](LICENSE). Changes are in [CHANGELOG.md](CHANGELOG.md).
