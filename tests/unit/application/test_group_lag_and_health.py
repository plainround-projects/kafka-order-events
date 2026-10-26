from order_events.application.group_lag import group_lag
from order_events.application.health import ProbeFailedError, check_health
from tests.unit.application.fakes import FixedOffsets


def test_group_lag_is_end_minus_committed_per_partition():
    offsets = FixedOffsets({0: 100, 1: 50}, {0: 90, 1: None})
    assert group_lag(offsets, "g", "t") == {0: 10, 1: 50}


class Healthy:
    name = "kafka"

    def check(self) -> str:
        return "six partitions"


class Broken:
    name = "postgres"

    def check(self) -> str:
        raise ProbeFailedError("connection refused")


def test_a_report_is_healthy_only_when_every_probe_is():
    assert check_health([Healthy()]).healthy
    report = check_health([Healthy(), Broken()])
    assert not report.healthy
    assert [(result.name, result.ok, result.detail) for result in report.results] == [
        ("kafka", True, "six partitions"),
        ("postgres", False, "connection refused"),
    ]


def test_no_probes_is_healthy():
    assert check_health([]).healthy
