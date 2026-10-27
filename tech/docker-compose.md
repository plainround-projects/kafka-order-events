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

The whole `docker-compose.yml` is read below. First the ideas:

- A **container** is a running program packaged with everything it needs and isolated from the rest of the machine: it has its own files, its own network address and its own process list. It is lighter than a virtual machine because it shares the host's operating-system kernel.
- An **image** is the read-only package a container starts from. `apache/kafka:4.0.0` means the image named `kafka` published by the `apache` account, at version (tag) `4.0.0`.
- A **port** is a numbered door on a network address; a program listens on a port, and clients connect to it. **Publishing** a port connects a port of the host machine to a port inside the container.
- **Docker Compose** reads a YAML file that describes one or several containers (services) and starts them together with `docker compose up`.

```yaml
# Single-node Kafka 4 in KRaft mode: one process is both broker and controller, no ZooKeeper.
name: kafka-order-events

services:
  kafka:
    image: apache/kafka:4.0.0
    ports:
      - "9092:9092"
```

- Line 1: a comment. KRaft is Kafka's built-in way to agree on cluster metadata, replacing the older ZooKeeper program. "One process is both broker and controller" means no second program is needed.
- Line 2: `name: kafka-order-events` is the **project name**. Compose puts it in the names of the containers and networks it creates, so two projects do not clash.
- Line 4: `services:` begins the list of containers. Line 5: this one is named `kafka`; that name is also what `docker compose exec kafka` uses.
- Line 6: the image to run.
- Lines 7 and 8: `ports: - "9092:9092"`. The format is `host:container`. A request to port 9092 on this computer reaches port 9092 in the container. The quotes protect the colon in YAML.

```yaml
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      # Clients on this Mac connect to localhost:9092, so the broker advertises exactly that.
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
```

`environment:` sets environment variables inside the container. The Kafka image converts each `KAFKA_*` variable into a broker setting.

- Line 10: `KAFKA_NODE_ID: 1`: this server's number (`node.id`). It must be unique in a cluster; here it is the only server.
- Line 11: `KAFKA_PROCESS_ROLES: broker,controller`: this one process plays both roles. A **broker** stores partitions and answers producers and consumers. A **controller** keeps the cluster's records of which broker leads which partition.
- Line 12: `KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093`. A **listener** is a named address the broker listens on, in the form `NAME://host:port`. An empty host means "every network address of the container". So it listens on 9092 for clients and on 9093 for controller traffic.
- Lines 13 and 14: the comment, and `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092`. A client first connects to any address it knows (the **bootstrap** address), and the broker replies with the addresses clients must use from then on. This is the address given out. It is `localhost:9092` because the clients (the Python programs and the Kafka tools run through `docker compose exec`) reach the broker as `localhost:9092`. If it advertised a different name, clients on the host would fail after the first connection.
- Line 15: `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT` says each listener's security: both use `PLAINTEXT`, which means no encryption and no login. That is right for a laptop demo and wrong for anything else.
- Line 16: `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093`: the list of controllers that vote, as `id@host:port`. A **quorum** is the group whose majority decides; here it is one voter, node 1.
- Line 17: `KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER` tells the broker which of the listeners is used for controller traffic.

```yaml
      # One broker, so the internal topics cannot have more than one replica.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      # Do not wait three seconds for more members before the first rebalance.
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

- Lines 18 to 21: comment, then three settings that fix Kafka's internal topics for one broker. `__consumer_offsets` stores each group's committed offsets and the transaction state log stores transactions. Their default replication factor is 3; with one broker Kafka could not create them. `TRANSACTION_STATE_LOG_MIN_ISR: 1` allows writes with one in-sync replica.
- Lines 22 and 23: `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`. By default the broker waits 3 seconds after the first member joins a group, hoping more will join, before it hands out partitions. Zero means start at once, so tests and demos do not wait.

```yaml
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 5s
      timeout: 10s
      retries: 20
      start_period: 10s
```

A **health check** is a command Docker runs repeatedly inside the container to decide if the service works.

- Line 25: `test: ["CMD-SHELL", "..."]`: the first word says "run the next string through the container's shell". The command `kafka-broker-api-versions.sh --bootstrap-server localhost:9092` asks the broker which API versions it supports; it succeeds only when the broker is up. `> /dev/null 2>&1` throws away its output.
- Line 26: `interval: 5s`: run it every 5 seconds. Line 27: `timeout: 10s`: give each run 10 seconds. Line 28: `retries: 20`: after 20 failures in a row the container is marked `unhealthy`. Line 29: `start_period: 10s`: failures during the first 10 seconds do not count.
- `docker compose up -d --wait` (used by `make up`) returns only when this check passes.

### Starting and stopping

`make up` runs `docker compose up -d --wait`. `make down` runs `docker compose down -v`: stop and delete the container and any volumes it used. This file has no `volumes:` section of its own, so nothing is kept between a `make down` and the next `make up`: the topic is created again each time.

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
