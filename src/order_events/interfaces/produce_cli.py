"""Publish OrderPlaced, OrderPaid and OrderShipped for a range of orders.

python -m order_events.interfaces.produce_cli --orders 1000
python -m order_events.interfaces.produce_cli --orders 1000 --no-key --prefix unkeyed
"""

from __future__ import annotations

import argparse
import random
import sys
import time
from collections.abc import Sequence

from ..application.produce_orders import ProducePlan, produce_orders
from ..infrastructure.kafka_publisher import KafkaEventPublisher
from ..infrastructure.logging import configure_logging
from ..infrastructure.settings import ConfigurationError, Settings
from .arguments import non_negative_float, positive_int


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--orders", type=positive_int, default=1000, help="how many orders (three events each)")
    parser.add_argument("--start", type=positive_int, default=1, help="number of the first order")
    parser.add_argument("--prefix", default="order", help="order id prefix, so runs never share ids")
    parser.add_argument("--no-key", action="store_true", help="send without a key (the failure mode)")
    parser.add_argument("--window", type=positive_int, default=50, help="orders in flight at once")
    parser.add_argument(
        "--max-delay",
        type=non_negative_float,
        default=0.002,
        help="random pause in seconds after each event; the lesson uses up to 0.05",
    )
    parser.add_argument("--seed", type=int, default=None, help="fixed seed for a repeatable run")
    parser.add_argument("--topic", default=None, help="defaults to $TOPIC")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        settings = Settings.from_env()
    except ConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    configure_logging(settings.log_level)
    plan = ProducePlan(
        orders=args.orders,
        start=args.start,
        prefix=args.prefix,
        keyed=not args.no_key,
        window=args.window,
        max_pause_seconds=args.max_delay,
    )
    publisher = KafkaEventPublisher(settings, args.topic)
    started = time.time()
    try:
        sent = produce_orders(publisher, plan, random.Random(args.seed), time.time, time.sleep)
    finally:
        publisher.close()
    mode = "unkeyed" if args.no_key else "keyed by orderId"
    topic = args.topic or settings.topic
    print(f"produced {sent} events for {args.orders} orders to {topic} ({mode}) in {time.time() - started:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
