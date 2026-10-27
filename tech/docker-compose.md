# Docker Compose

## What it is

Docker Compose reads a YAML file (`docker-compose.yml`) that describes one or more containers and starts them together with one command. It adds the network between them, the published ports, the environment variables and the health checks. This repository uses Compose v2 (`docker compose`, with a space), tested with v2.23.

## Why this repository uses it

One command, `make up`, must give every reader the same broker. The compose file holds every Kafka setting in one place.

## Where it appears

| Path | What it does |
| --- | --- |
| `docker-compose.yml` | The whole description: one service, `kafka`. |
| `Makefile` | `docker compose up -d --wait`, `exec`, `down -v`. |
| `scripts/*.sh` | `docker compose exec` into the `kafka` service. |

## How it works underneath

`docker compose up` reads the file, makes a project (named by the top-level `name:`), creates a default network, pulls missing images, creates the containers and starts them. `--wait` then polls each container's health check until it is healthy. Service names (here `kafka`) work as host names inside that network.

## Walkthrough of this repository's use

```yaml
# Single-node Kafka 4 in KRaft mode: one process is both broker and controller, no ZooKeeper.
name: kafka-order-events

services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      # Clients on this Mac connect to localhost:9092, so the broker advertises exactly that.
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      # One broker, so the internal topics cannot have more than one replica.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      # Do not wait three seconds for more members before the first rebalance.
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20
      start_period: 10s
```

- `name: kafka-order-events`: the project name, so container names are the same whatever the folder is called.
- `services:` then `kafka:`: one service called `kafka`.
- `image: apache/kafka:4.0.0`: the official Kafka image, version pinned.
- `ports: - "9092:9092"`: publish container port 9092 as port 9092 on your computer (`host:container`).
- `environment:`: the broker's settings. Every key is explained in `tech/apache-kafka.md`.
- `healthcheck:`: how Docker decides the broker is ready. `test: ["CMD-SHELL", "..."]` runs the command in a shell; `kafka-broker-api-versions.sh` succeeds only when the broker answers. `interval: 5s` is how often to try. `timeout: 10s` is how long one try may take. `retries: 20` is how many failures are tolerated before the container is `unhealthy`. `start_period: 10s` is a grace period at the start in which failures do not count.

## Try it yourself

1. `docker compose config` prints the file as Compose understands it (it checks the syntax).
2. `make up`, then `docker compose ps`: `STATUS` shows `Up ... (healthy)`.
3. `docker compose logs --tail=5 kafka` shows the broker's last lines.
4. `docker compose exec -T kafka ls /opt/kafka/bin | head -3` shows tools inside the container.
5. `make down`: both container and network are removed.

## Mistakes people make

- Changing the published port but not `KAFKA_ADVERTISED_LISTENERS`, so clients connect and are then redirected to a port that does not exist (see `tech/apache-kafka.md`).
- Using `depends_on` without a health check and assuming the dependency is ready.
- Using tabs in YAML (see `tech/yaml.md`).
- Typing `docker-compose` (old v1 tool) instead of `docker compose`.

## Check yourself

1. What does `up -d --wait` do? *Starts the services in the background and returns once their health checks pass.*
2. What does `"9092:9092"` mean? *Host port 9092 forwards to container port 9092.*
3. What does `start_period` do? *Failures during that time are not counted against `retries`.*
4. Why is there no volume? *The data should disappear on `down`.*
5. How does `docker compose config` help? *It validates and prints the resolved file.*

## Further reading

- Compose file reference: https://docs.docker.com/reference/compose-file/
- `docker compose up`: https://docs.docker.com/reference/cli/docker/compose/up/
