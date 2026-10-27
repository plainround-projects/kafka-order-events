# kafka-python

## What it is

`kafka-python` is a Python **library**: a bundle of ready-made code you import into your own program. It speaks Kafka's network protocol, so Python code can write records to a Kafka broker and read them back without you handling sockets and byte layouts. It is written entirely in Python (no compiled parts), which is why it installs on any Python version. This repository pins version 3.0.12.

## Why this repository uses it

The lesson's starting code uses it. It gives the three things the lesson needs: a producer that can send keyed records, a consumer that joins a group and reports its partitions, and a way to read committed offsets for the lag number.

## Where it appears

| Path | What it does with the library |
| --- | --- |
| `requirements.lock`, `pyproject.toml` | Pin `kafka-python==3.0.12`. |
| `src/order_events/infrastructure/kafka_publisher.py` | `KafkaProducer`: sends events. |
| `src/order_events/infrastructure/kafka_source.py` | `KafkaConsumer` and `ConsumerRebalanceListener`: reads events as a group member. |
| `src/order_events/infrastructure/kafka_offsets.py` | `KafkaConsumer` without joining a group, to read offsets for the lag. |
| `src/order_events/infrastructure/kafka_health.py` | `KafkaConsumer` to ask which partitions a topic has. |
| `tests/conftest.py`, `tests/integration/` | `KafkaAdminClient` to create and delete test topics. |

Only the `infrastructure` folder imports it. The import-linter contract in `pyproject.toml` forbids `domain` and `application` from importing `kafka` (see `tech/import-linter.md`).

## How it works underneath

Your program holds one or more **TCP connections** (network pipes) to brokers. `KafkaProducer.send(...)` does not write to the network at once: it puts the record in a buffer and returns a *future* (a promise of a result). A background thread groups buffered records into batches and sends them. `flush()` waits until every batch is acknowledged.

`acks="all"` means the broker answers only after every in-sync copy of the record is stored. With one broker that is just the leader. The client also retries by itself if a request fails.

`KafkaConsumer.poll(timeout_ms)` asks the brokers for the next records and returns whatever arrived within the timeout, as a dictionary of partition to list of records. A consumer in a group must call `poll` regularly; a background thread sends **heartbeats** (small "I am alive" messages) so the coordinator does not think it died. `close()` leaves the group cleanly and commits offsets when auto-commit is on.

## Walkthrough of this repository's use

### The producer

```python
class KafkaEventPublisher:
    """Implements `EventPublisher`. The client waits for all in-sync replicas (acks=all) and retries."""

    def __init__(self, settings: Settings, topic: str | None = None, flush_timeout_seconds: float = 60.0) -> None:
        self._topic = topic or settings.topic
        self._flush_timeout = flush_timeout_seconds
        self._producer: Any = KafkaProducer(
            bootstrap_servers=settings.bootstrap_servers,
            acks="all",
            request_timeout_ms=settings.request_timeout_ms,
            retry_backoff_ms=settings.retry_backoff_ms,
        )

    def publish(self, event: OrderEvent, key: str | None) -> None:
        self._producer.send(self._topic, key=None if key is None else key.encode(), value=encode_event(event))

    def flush(self) -> None:
        self._producer.flush(timeout=self._flush_timeout)

    def close(self) -> None:
        self._producer.close(timeout=self._flush_timeout)
```

- `KafkaProducer(bootstrap_servers=..., acks="all", request_timeout_ms=..., retry_backoff_ms=...)`: where to connect first, how many copies must confirm, how long to wait for an answer, and how long to pause between retries. The values come from the validated `Settings` object (`tech/python.md`, `tech/toml.md` for how the project is configured).
- `self._producer: Any`: the library has no type information, so the hint says `Any` (mypy then does not check calls on it; see `tech/mypy.md`).
- `publish` calls `send` and does **not** wait. `flush` waits up to `flush_timeout` seconds. `close` flushes and disconnects.

### The consumer and its rebalance listener

```python
class _LogAssignment(ConsumerRebalanceListener):  # type: ignore[misc]
    def __init__(self, member: str) -> None:
        self._member = member

    def on_partitions_revoked(self, revoked: Iterable[Any]) -> None:
        logger.info("partitions revoked", extra=fields(member=self._member, count=len(list(revoked))))

    def on_partitions_assigned(self, assigned: Iterable[Any]) -> None:
        numbers = sorted(partition.partition for partition in assigned)
        logger.info("partitions assigned", extra=fields(member=self._member, partitions=numbers or "none (idle)"))


class KafkaEventSource:
    """Implements `EventSource`. Offsets auto-commit every second and once more on `close`."""

    def __init__(self, settings: Settings, member: str, group_id: str | None = None, topic: str | None = None) -> None:
        self._member = member
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

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        deliveries: list[Delivery] = []
        for batch in self._consumer.poll(timeout_ms=int(timeout_seconds * 1000)).values():
            for record in batch:
                try:
                    deliveries.append(Delivery(decode_event(record.value), record.partition, record.offset))
                except MalformedEventError as error:
                    logger.warning(
                        "skipping malformed record",
                        extra=fields(
                            member=self._member, partition=record.partition, offset=record.offset, error=error
                        ),
                    )
        return deliveries
```

- `_LogAssignment` extends `ConsumerRebalanceListener`. The library calls `on_partitions_revoked` before it takes partitions away and `on_partitions_assigned` after it hands new ones. Here both just log.
- `poll(timeout_ms=int(timeout_seconds * 1000))` returns a dictionary. The two `for` loops walk partition by partition, record by record.
- `decode_event(record.value)` turns the bytes into an `OrderEvent`. A record that is not a valid event raises `MalformedEventError`; it is logged and skipped, so one bad record cannot stop the stream.
- `record.partition` and `record.offset` say where the record was read.

### Reading offsets without joining the group

```python
    def end_offsets(self, topic: str) -> Mapping[int, int]:
        def read() -> Mapping[int, int]:
            consumer = self._consumer(None)
            try:
                partitions = [
                    TopicPartition(topic, number) for number in sorted(consumer.partitions_for_topic(topic) or ())
                ]
                return {partition.partition: end for partition, end in consumer.end_offsets(partitions).items()}
            finally:
                consumer.close(autocommit=False)

        result: Mapping[int, int] = self._with_retry(read)
        return result
```

A consumer created with `group_id=group_id` but never subscribed does not join the group, yet can ask the broker for the group's committed offset: `consumer.committed(TopicPartition(topic, number))`. `end_offsets(partitions)` asks for each partition's log-end offset. `enable_auto_commit=False` stops this helper from writing an offset of its own. `close(autocommit=False)` closes without committing.

## Try it yourself

1. Show the pinned version: `.venv/bin/python -c "import kafka; print(kafka.__version__)"` prints `3.0.12`.
2. List what the library offers: `.venv/bin/python -c "import kafka; print([n for n in dir(kafka) if n.startswith('Kafka')])"` prints names such as `KafkaAdminClient`, `KafkaConsumer`, `KafkaProducer`.
3. With Kafka running (`make up`): `make run`, then read the consumer's first log lines in the terminal: `msg="partitions assigned" member=consumer partitions="[0, 1, 2, 3, 4, 5]"`.
4. `make scale N=3` then look at `logs/consumer-1.log`: it shows `partitions revoked` and `partitions assigned` each time another member joined.

## Mistakes people make

- Assuming `send` has delivered the record. Only `flush` or the future's result says so.
- Never calling `poll` for a long time, so the group evicts the consumer (the idempotent-consumer repository shows this on purpose).
- Using a client library version that does not support the broker. Version 3.0.12 was tested here against Kafka 4.0.0.
- Forgetting `close()`: the group waits for the session to expire before it re-deals the partitions. Both programs close in a `finally` block.

## Check yourself

1. Does `producer.send(...)` wait for the broker? *No. It returns a future; `flush()` waits.*
2. What does `acks="all"` ask for? *Confirmation from all in-sync copies before the broker answers.*
3. What does `poll` return? *The records that arrived within the timeout, grouped by partition.*
4. Why is the offset reader created with `enable_auto_commit=False`? *So reading the lag does not change the group's committed offsets.*
5. Which folder may import `kafka`? *Only `infrastructure`.*

## Further reading

- kafka-python 3.0.12 on PyPI: https://pypi.org/project/kafka-python/3.0.12/
- Project documentation: https://kafka-python.readthedocs.io/
- Producer and consumer concepts in Kafka itself: https://kafka.apache.org/40/documentation.html#producerapi
