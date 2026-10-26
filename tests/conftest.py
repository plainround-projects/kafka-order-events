"""Fixtures for the tests that need the broker from docker-compose.yml."""

import os
import uuid
from collections.abc import Iterator

import pytest
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from order_events.infrastructure.settings import Settings

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "localhost:9092")


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env({"BOOTSTRAP_SERVERS": BOOTSTRAP})


@pytest.fixture
def topic() -> Iterator[str]:
    """A fresh six-partition topic per test, so no test sees another's events."""
    name = f"test-order-events-{uuid.uuid4().hex[:8]}"
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    admin.create_topics([NewTopic(name, num_partitions=6, replication_factor=1)])
    yield name
    admin.delete_topics([name])
    admin.close()
