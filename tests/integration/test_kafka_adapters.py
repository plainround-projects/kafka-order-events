"""The Kafka adapters against the real broker: make up && make test-integration."""

import random
import threading
import time
import uuid

import pytest
from kafka import KafkaAdminClient

from order_events.application.group_lag import group_lag
from order_events.application.health import check_health
from order_events.application.produce_orders import ProducePlan, produce_orders
from order_events.application.read_orders import ReadPlan, ReadResult, read_orders
from order_events.infrastructure.kafka_health import KafkaTopicProbe
from order_events.infrastructure.kafka_offsets import KafkaOffsetReader
from order_events.infrastructure.kafka_publisher import KafkaEventPublisher
from order_events.infrastructure.kafka_source import KafkaEventSource
from order_events.infrastructure.settings import Settings

from ..conftest import BOOTSTRAP

pytestmark = pytest.mark.integration


def publish(settings: Settings, topic: str, orders: int, *, keyed: bool, start: int = 1) -> int:
    publisher = KafkaEventPublisher(settings, topic)
    try:
        plan = ProducePlan(orders=orders, start=start, keyed=keyed, window=20)
        return produce_orders(publisher, plan, random.Random(5), time.time, time.sleep)
    finally:
        publisher.close()


def read(settings: Settings, topic: str, group_id: str | None = None) -> ReadResult:
    source = KafkaEventSource(settings, "test-reader", group_id or f"check-{uuid.uuid4().hex}", topic)
    try:
        plan = ReadPlan(idle_timeout_seconds=4)
        return read_orders(
            source, plan, stop=lambda: False, clock=time.time, sleep=time.sleep, on_out_of_order=lambda _: None
        )
    finally:
        source.close()


def test_the_topic_has_six_partitions_and_one_replica(topic: str) -> None:
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    description = admin.describe_topics([topic])[0]
    admin.close()
    assert len(description["partitions"]) == 6
    assert all(len(partition["replica_nodes"]) == 1 for partition in description["partitions"])


def test_keyed_events_of_one_order_share_a_partition_and_arrive_in_order(settings: Settings, topic: str) -> None:
    sent = publish(settings, topic, orders=100, keyed=True)
    result = read(settings, topic)
    assert result.events_read == sent
    assert result.report.out_of_order == ()
    assert result.report.orders_split_across_partitions == 0


def test_unkeyed_events_arrive_out_of_order(settings: Settings, topic: str) -> None:
    sent = publish(settings, topic, orders=100, keyed=False)
    result = read(settings, topic)
    assert result.events_read == sent
    assert len(result.report.out_of_order) > 0
    assert result.report.orders_split_across_partitions > 0


def test_eight_consumers_on_six_partitions_leave_two_idle(settings: Settings, topic: str) -> None:
    group = f"scale-{uuid.uuid4().hex}"
    sources = [KafkaEventSource(settings, f"member-{number}", group, topic) for number in range(8)]
    stop = threading.Event()
    counts: dict[int, int] = {}

    def member(number: int) -> None:
        while not stop.is_set():
            sources[number].poll(0.3)
            counts[number] = sources[number].assigned_partition_count

    threads = [threading.Thread(target=member, args=(number,)) for number in range(8)]
    for thread in threads:
        thread.start()
        time.sleep(0.5)
    deadline = time.time() + 60
    while time.time() < deadline and sorted(counts.values()) != [0, 0, 1, 1, 1, 1, 1, 1]:
        time.sleep(1)
    stop.set()
    for thread in threads:
        thread.join()
    for source in sources:
        source.close()
    assert sorted(counts.values()) == [0, 0, 1, 1, 1, 1, 1, 1]


def test_lag_grows_while_the_group_is_stopped_and_drains_when_it_reads(settings: Settings, topic: str) -> None:
    group, offsets = f"lag-{uuid.uuid4().hex}", KafkaOffsetReader(settings)
    first = publish(settings, topic, orders=20, keyed=True)
    assert read(settings, topic, group).events_read == first
    assert sum(group_lag(offsets, group, topic).values()) == 0

    backlog = publish(settings, topic, orders=50, keyed=True, start=1000)
    assert sum(group_lag(offsets, group, topic).values()) == backlog

    assert read(settings, topic, group).events_read == backlog
    assert sum(group_lag(offsets, group, topic).values()) == 0


def test_the_health_probe_passes_for_the_designed_topic_and_fails_for_a_missing_one(
    settings: Settings, topic: str
) -> None:
    ok = check_health([KafkaTopicProbe(Settings.from_env({"BOOTSTRAP_SERVERS": BOOTSTRAP, "TOPIC": topic}))])
    assert ok.healthy
    missing = check_health(
        [KafkaTopicProbe(Settings.from_env({"BOOTSTRAP_SERVERS": BOOTSTRAP, "TOPIC": "no-such-topic"}))]
    )
    assert not missing.healthy
    assert "expected 6" in missing.results[0].detail
