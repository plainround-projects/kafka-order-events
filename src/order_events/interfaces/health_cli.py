"""Health command: exit 0 when the broker is reachable and the topic has the designed partitions.

python -m order_events.interfaces.health_cli
"""

from __future__ import annotations

import sys
from collections.abc import Sequence

from ..application.health import check_health
from ..infrastructure.kafka_health import KafkaTopicProbe
from ..infrastructure.logging import configure_logging
from ..infrastructure.settings import ConfigurationError, Settings


def main(_argv: Sequence[str] | None = None) -> int:
    """Check the broker and the topic; returns 0 when healthy."""
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    report = check_health([KafkaTopicProbe(settings)])
    for result in report.results:
        print(f"{'ok' if result.ok else 'FAIL'} {result.name}: {result.detail}")
    return 0 if report.healthy else 1


if __name__ == "__main__":
    raise SystemExit(main())
