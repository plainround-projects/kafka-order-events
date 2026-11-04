# Changelog

All notable changes to this project are recorded here. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Drawn architecture and flow figures in `docs/diagrams/`, light and dark, shown in the README.

## [0.1.0] - 2026-10-26

### Added

- Single-node Kafka 4 (KRaft) in `docker-compose.yml` and the `order-events` topic: six partitions, replication factor 1, seven-day retention.
- A producer that interleaves open orders and keys every event by `orderId`, or sends unkeyed on request.
- A consumer-group member that checks every order's sequence, prints order histories and logs its partition assignment.
- Lag reading (`make lag-report`, `kafka-consumer-groups.sh`) and demos for scaling the group and draining a backlog.
- Layered package (`domain`, `application`, `infrastructure`, `interfaces`) with the dependency rule enforced by import-linter, unit, integration and end-to-end tests, ruff, mypy, a dependency lock file and a CI workflow.
