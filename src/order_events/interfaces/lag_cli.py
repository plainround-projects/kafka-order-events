"""Print the lag of the consumer group per partition (committed offset against log-end offset).

python -m order_events.interfaces.lag_cli
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from ..application.group_lag import group_lag
from ..infrastructure.kafka_offsets import KafkaOffsetReader
from ..infrastructure.logging import configure_logging
from ..infrastructure.settings import ConfigurationError, Settings


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--group", default=None, help="defaults to $GROUP_ID")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    group, topic = args.group or settings.group_id, args.topic or settings.topic
    lag = group_lag(KafkaOffsetReader(settings), group, topic)
    print(f"{'PARTITION':<10}{'LAG':>10}")
    for partition, value in sorted(lag.items()):
        print(f"{partition:<10}{value:>10}")
    print(f"{'total':<10}{sum(lag.values()):>10}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
