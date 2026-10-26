"""A member of the consumer group `order-history`: reads events and checks each order's sequence.

python -m order_events.interfaces.consume_cli --until-idle 5 --print-history 3
python -m order_events.interfaces.consume_cli --name consumer-1      # runs until Ctrl-C or SIGTERM
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections.abc import Sequence

from ..application.ports import Delivery
from ..application.read_orders import ReadPlan, read_orders
from ..infrastructure.kafka_source import KafkaEventSource
from ..infrastructure.logging import configure_logging, fields
from ..infrastructure.settings import ConfigurationError, Settings
from ..infrastructure.shutdown import ShutdownSignal
from .arguments import non_negative_float, non_negative_int

logger = logging.getLogger("order_events.consume")
REPORT_LIMIT = 5  # log the first few out-of-order events; the summary counts all of them


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--name", default="consumer", help="client id and label in the output")
    parser.add_argument("--group", default=None, help="defaults to $GROUP_ID")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    parser.add_argument(
        "--until-idle",
        type=non_negative_float,
        default=None,
        help="exit after this many seconds without a new event (default: run until stopped)",
    )
    parser.add_argument("--delay", type=non_negative_float, default=0.0, help="artificial seconds of work per event")
    parser.add_argument("--prefix", default="", help="only check orders whose id starts with this")
    parser.add_argument(
        "--print-history",
        type=non_negative_int,
        default=0,
        metavar="N",
        help="at exit, print the history of the first N orders",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    shutdown = ShutdownSignal()
    shutdown.install()

    reported = 0

    def on_out_of_order(delivery: Delivery) -> None:
        nonlocal reported
        reported += 1
        if reported <= REPORT_LIMIT:
            logger.warning(
                "out of order",
                extra=fields(
                    order=delivery.event.order_id,
                    sequence=delivery.event.sequence,
                    partition=delivery.partition,
                    offset=delivery.offset,
                ),
            )

    source = KafkaEventSource(settings, args.name, args.group, args.topic)
    plan = ReadPlan(idle_timeout_seconds=args.until_idle, work_delay_seconds=args.delay, order_prefix=args.prefix)
    try:
        result = read_orders(
            source,
            plan,
            stop=lambda: shutdown.requested,
            clock=time.time,
            sleep=time.sleep,
            on_out_of_order=on_out_of_order,
            histories=args.print_history,
        )
    finally:
        source.close()
    print(f"[{args.name}] read {result.events_read} events  {result.report.summary()}")
    for order_id, partitions, history in result.first_histories:
        print(f"  {order_id} (partition {list(partitions)}): {' -> '.join(history)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
