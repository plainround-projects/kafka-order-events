"""The lesson's scenario through the command-line programs, as the Makefile runs it: make up && make test-e2e."""

import os
import subprocess
import sys
import uuid

import pytest

from ..conftest import BOOTSTRAP

pytestmark = pytest.mark.e2e


def program(module: str, topic: str, *args: str) -> subprocess.CompletedProcess[str]:
    environment = {**os.environ, "BOOTSTRAP_SERVERS": BOOTSTRAP, "TOPIC": topic}
    return subprocess.run(
        [sys.executable, "-m", f"order_events.interfaces.{module}", *args],
        capture_output=True,
        text=True,
        env=environment,
        timeout=180,
        check=False,
    )


def test_a_keyed_run_logs_zero_out_of_order_events_and_an_unkeyed_run_logs_some(topic: str) -> None:
    keyed = program("produce_cli", topic, "--orders", "200", "--prefix", "keyed", "--max-delay", "0")
    assert keyed.returncode == 0
    assert "produced 600 events for 200 orders" in keyed.stdout

    unkeyed = program("produce_cli", topic, "--orders", "200", "--prefix", "unkeyed", "--max-delay", "0", "--no-key")
    assert unkeyed.returncode == 0

    keyed_read = program(
        "consume_cli",
        topic,
        "--until-idle",
        "4",
        "--group",
        f"e2e-{uuid.uuid4().hex}",
        "--prefix",
        "keyed",
        "--print-history",
        "1",
    )
    assert keyed_read.returncode == 0
    assert "out-of-order events logged: 0" in keyed_read.stdout
    assert "OrderPlaced -> OrderPaid -> OrderShipped" in keyed_read.stdout

    unkeyed_read = program(
        "consume_cli", topic, "--until-idle", "4", "--group", f"e2e-{uuid.uuid4().hex}", "--prefix", "unkeyed"
    )
    assert unkeyed_read.returncode == 0
    assert "out-of-order events logged: 0" not in unkeyed_read.stdout
    assert "out of order" in unkeyed_read.stderr


def test_the_health_command_reports_the_designed_topic(topic: str) -> None:
    result = program("health_cli", topic)
    assert result.returncode == 0
    assert f"ok kafka: topic {topic} has 6 partitions" in result.stdout


def test_the_lag_command_prints_a_total(topic: str) -> None:
    program("produce_cli", topic, "--orders", "10", "--max-delay", "0")
    result = program("lag_cli", topic, "--group", f"e2e-lag-{uuid.uuid4().hex}")
    assert result.returncode == 0
    assert "total" in result.stdout
    assert result.stdout.strip().splitlines()[-1].split()[-1] == "30"
