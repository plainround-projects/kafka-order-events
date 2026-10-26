"""Consumer lag: the same numbers `kafka-consumer-groups.sh --describe` prints."""

from __future__ import annotations

from collections.abc import Mapping


def compute_lag(end_offsets: Mapping[int, int], committed: Mapping[int, int | None]) -> dict[int, int]:
    """Lag per partition: the log-end offset minus the committed offset.

    A partition the group never committed on counts from offset 0, which is where a group with
    auto.offset.reset=earliest would start.
    """
    return {partition: end - (committed.get(partition) or 0) for partition, end in end_offsets.items()}
