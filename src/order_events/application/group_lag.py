"""Use case: the lag of a consumer group on a topic."""

from __future__ import annotations

from ..domain.lag import compute_lag
from .ports import OffsetReader


def group_lag(reader: OffsetReader, group_id: str, topic: str) -> dict[int, int]:
    return compute_lag(reader.end_offsets(topic), reader.committed_offsets(group_id, topic))
