# 3. kafka-python 3.0.12 as the client

Status: accepted, 2026-10-26

## Context

The lesson's starting point uses `kafka-python`. The alternative, `confluent-kafka`, wraps the C library librdkafka.

## Decision

Use `kafka-python` 3.0.12, pinned. It is pure Python, so it installs on any interpreter (the author's Mac defaults to Python 3.15, for which `confluent-kafka` had no wheel), and it speaks to Kafka 4.0.

## Alternatives considered

- **confluent-kafka:** faster, but needs a compiled wheel per Python version and platform.
- **Java client:** the reference implementation, but the lesson and the rest of the repository are in Python.

## Consequences

- Producer defaults in 3.x are `acks=all`, idempotence on and unlimited retries; the adapter sets `acks="all"` explicitly so the design is visible in the code.
- Throughput is lower than a C-based client; irrelevant at this project's volume.
