# 2. Ports and adapters around the Kafka client

Status: accepted, 2026-10-26

## Context

The ordering rules, the produce plan and the read loop are the lesson's substance and should be testable without a broker. The Kafka client (`kafka-python`) is infrastructure that may be replaced.

## Decision

Four layers: `domain` (events, ordering, lag arithmetic), `application` (use cases and the ports `EventPublisher`, `EventSource`, `OffsetReader`, `HealthProbe`), `infrastructure` (Kafka adapters, settings, logging, retry, shutdown), `interfaces` (command-line programs). Dependencies point downward only; import-linter enforces it and forbids `domain` and `application` from importing `kafka`. Time, randomness and sleeping are passed in as functions.

## Alternatives considered

- **Scripts calling the client directly** (the lesson's starting point): shorter, but the ordering logic can only be tested against a broker.
- **A framework** (Faust, Spring-style abstractions): hides the consumer group mechanics this project exists to show.

## Consequences

- Domain and application code is covered by fast unit tests with in-memory fakes (100% line and branch coverage is enforced at 95%).
- Adapters are covered by integration tests against the real broker; the programs by an end-to-end test.
- One more indirection than a script; justified by the tests and by being able to swap the client.
