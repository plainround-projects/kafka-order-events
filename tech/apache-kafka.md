# Apache Kafka

## What it is

Kafka is a server that stores a stream of messages and lets many programs write to it and read from it. Think of a very long notebook that only grows at the end. Writers add lines at the bottom. Readers each keep their own bookmark and read forward at their own speed. Nobody erases a line when it is read; lines are deleted only when they get old.

Some words you need:

- **Message** (or **record**): one entry. It has a **key** (optional), a **value** (the content) and a time.
- **Topic**: a named notebook, here `order-events`.
- **Partition**: a topic is cut into several separate notebooks called partitions, numbered from 0. Each partition keeps its records in the exact order they arrived. Kafka guarantees order *inside one partition only*.
- **Offset**: the line number of a record inside one partition (0, 1, 2, ...).
- **Broker**: one Kafka server process. A group of brokers is a **cluster**.
- **Producer**: a program that writes records. **Consumer**: a program that reads them.
- **Consumer group**: a team of consumers that share a topic. Kafka gives each partition to exactly one member of the team at a time.
- **Replication factor**: how many copies of each partition exist on different brokers.
- **KRaft**: the way Kafka 4 keeps its own bookkeeping (who leads which partition) without a separate helper program. A broker can also play the *controller* role that does this bookkeeping.

## Why this repository uses it

The lesson is about three questions every Kafka interview asks: what should the key be, how many partitions, and how do you know a consumer is behind. This repository runs one real broker and lets you see each answer happen.

## Where it appears

| Path | What it does with Kafka |
| --- | --- |
| `docker-compose.yml` | Starts one broker (version 4.0.0) that is also its own controller. |
| `Makefile` (`up`, `reset-topic`, `describe`, `order`) | Creates, deletes and describes the topic with Kafka's own command-line tools. |
| `scripts/scale.sh`, `scripts/lag.sh` | Start consumers and ask Kafka who owns which partition and how far behind each is. |
| `src/order_events/infrastructure/kafka_*.py` | Python code that writes and reads records (see `tech/kafka-python.md`). |
| `docs/adr/0001-...` | Why the key is the order id and the topic has six partitions. |

## How it works underneath

**Writing.** The producer picks a partition for each record. With a key, the client computes a hash of the key and takes the remainder after dividing by the number of partitions (a hash is a fixed number computed from text; the same text always gives the same number). So the same key always lands in the same partition, and that is how one order's three events stay in order. Without a key the client spreads records across partitions, and order across partitions is lost. The broker appends the record to the end of that partition's file on disk and gives it the next offset.

**Reading.** A consumer asks the broker "give me records from offset N of partition P". When it has processed some, it **commits** an offset: it tells the broker "my bookmark for this group on this partition is now N". The broker stores that bookmark in an internal topic called `__consumer_offsets`. If the consumer restarts, it continues from the bookmark.

**Groups and rebalancing.** When a consumer joins or leaves a group, one broker (the **group coordinator**) re-deals the partitions among the members. This is a **rebalance**. With six partitions and three consumers each gets two. With eight consumers, six get one each and two get none, because a partition is never split between members of one group.

**Lag.** For each partition: the **log-end offset** (next offset to be written) minus the group's committed offset. It counts the records written but not yet processed. It grows when consumers stop or are slow.

**Retention.** The setting `retention.ms=604800000` means records older than 604,800,000 milliseconds (seven days) may be deleted.

## Walkthrough of this repository's use

### The broker's settings

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
      # One broker, so the internal topics cannot have more than one replica.
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      # Do not wait three seconds for more members before the first rebalance.
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

Kafka's container image turns every variable starting with `KAFKA_` into a line of the broker's `server.properties` file (underscores become dots, so `KAFKA_NODE_ID` is `node.id`).

- `KAFKA_NODE_ID: 1`: this broker's number.
- `KAFKA_PROCESS_ROLES: broker,controller`: it stores data *and* does the bookkeeping. One process, no ZooKeeper (the older helper program).
- `KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093`: the doors the broker listens on. Port 9092 is for clients. Port 9093 is for the controller role. `PLAINTEXT` means no encryption.
- `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092`: the address the broker *tells clients to use* in later requests. A client first connects, then is told "talk to me at localhost:9092". If this said something your computer cannot reach, the client would connect and then fail. This is the most common Kafka setup mistake.
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`, `KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093` and `KAFKA_CONTROLLER_LISTENER_NAMES`: say which listener name means what, and that the only controller voter is node 1 at port 9093.
- `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` and the two `TRANSACTION_STATE_LOG` lines: Kafka keeps internal topics. Their default is three copies, which a one-broker cluster cannot make, so they are set to one.
- `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0`: do not wait three seconds for more members before the first rebalance.

### Creating the topic

```make
up: $(STAMP) ## start Kafka and create the topic
	docker compose up -d --wait
	$(KAFKA_BIN)/kafka-topics.sh --create --if-not-exists --topic $(TOPIC) --partitions 6 \
	  --replication-factor 1 --config retention.ms=604800000 --bootstrap-server localhost:9092
	$(MAKE) --no-print-directory describe
```

`kafka-topics.sh` is Kafka's own tool, run inside the container with `docker compose exec`. `--create --if-not-exists` makes the command safe to run twice. `--partitions 6` cuts the topic into six partitions. `--replication-factor 1` keeps one copy (there is one broker). `--config retention.ms=604800000` sets seven-day retention. `--bootstrap-server localhost:9092` is the first address to ask; it is called "bootstrap" because the broker then tells the client about everything else.

### The key decides the partition

```python
    def publish(self, event: OrderEvent, key: str | None) -> None:
        self._producer.send(self._topic, key=None if key is None else key.encode(), value=encode_event(event))
```

`key=None if key is None else key.encode()` sends the order id as bytes when there is a key (Kafka stores bytes, not text) and no key otherwise. The application passes `event.order_id` as the key for the normal run and `None` for `make run-unkeyed`.

### Reading as a team

```python
        self._consumer: Any = KafkaConsumer(
            bootstrap_servers=settings.bootstrap_servers,
            group_id=group_id or settings.group_id,
            client_id=member,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            request_timeout_ms=settings.request_timeout_ms,
            retry_backoff_ms=settings.retry_backoff_ms,
        )
        self._consumer.subscribe([topic or settings.topic], listener=_LogAssignment(member))
```

`group_id` names the team. `auto_offset_reset="earliest"` means: a group with no bookmark starts from the oldest record. `enable_auto_commit=True` with `auto_commit_interval_ms=1000` commits the bookmark every second in the background. `subscribe` joins the group; `listener=_LogAssignment(member)` is called on every rebalance and logs which partitions this member got.

### Asking Kafka who owns what

```bash
kafka_groups=(docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092)
echo "== $count consumers: members and their partitions"
"${kafka_groups[@]}" --describe --group "$group" --members --verbose
echo
echo "== $count consumers: partitions, owner and lag"
"${kafka_groups[@]}" --describe --group "$group"
```

`kafka-consumer-groups.sh --describe --group G --members --verbose` lists each member and its partitions (a member with `#PARTITIONS 0` is idle). Without `--members` it lists each partition with `CURRENT-OFFSET`, `LOG-END-OFFSET` and `LAG`.

## Try it yourself

(Start Kafka first with `make up`.)

1. `make describe` shows `PartitionCount: 6  ReplicationFactor: 1  Configs: retention.ms=604800000` and six lines each ending `Isr: 1`. `Isr` means *in-sync replicas*: the copies that are up to date. Here the only copy agrees with itself.
2. `make run` then `make order ORDER=order-00042`: three lines, each starting with the same `Partition:` number, with sequence 1, 2, 3.
3. `make run-unkeyed`: the summary line shows about 1,700 `out-of-order events` and close to 1,000 orders `spread over more than one partition`.
4. `make scale N=8`: in the members table, six consumers have `#PARTITIONS 1` and two have `0`.
5. `make lag-report` prints the lag per partition and a total.

## Mistakes people make

- Setting `KAFKA_ADVERTISED_LISTENERS` to an address clients cannot reach. Here it is `localhost:9092`, and the compose file publishes exactly port 9092 so both agree.
- Expecting order across the whole topic. Only one partition is ordered. This is why the key matters.
- Adding consumers beyond the partition count and expecting more speed. Extra members sit idle.
- Lowering the partition count later: Kafka cannot. Raising it changes which partition a key maps to.
- Reading "replication factor 1, `Isr: 1`" as safe. It means one copy exists; losing the disk loses the data.

## Check yourself

1. What does Kafka guarantee about order? *Records in one partition keep the order they were written; nothing is promised across partitions.*
2. Why does a key keep an order's events in order? *The same key always hashes to the same partition, and one partition is ordered.*
3. What is lag? *Log-end offset minus the group's committed offset, per partition.*
4. Eight consumers, six partitions, one group: how many are idle? *Two.*
5. What does `retention.ms` control? *How old a record may get before Kafka may delete it.*

## Further reading

- Kafka 4.0 documentation: https://kafka.apache.org/40/documentation.html
- Design (log, partitions, consumers): https://kafka.apache.org/40/documentation.html#design
- Broker configuration reference: https://kafka.apache.org/40/documentation.html#brokerconfigs
- Image used here: `apache/kafka:4.0.0` (https://hub.docker.com/r/apache/kafka)
