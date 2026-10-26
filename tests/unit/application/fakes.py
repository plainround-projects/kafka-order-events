"""In-memory stand-ins for the application's ports."""

from collections.abc import Mapping, Sequence

from order_events.application.ports import Delivery
from order_events.domain.events import OrderEvent


class RecordingPublisher:
    def __init__(self) -> None:
        self.published: list[tuple[OrderEvent, str | None]] = []
        self.flushed = 0

    def publish(self, event: OrderEvent, key: str | None) -> None:
        self.published.append((event, key))

    def flush(self) -> None:
        self.flushed += 1


class ScriptedSource:
    """Returns the scripted batches one per poll, then empty polls forever."""

    def __init__(self, batches: Sequence[Sequence[Delivery]]) -> None:
        self._batches = list(batches)
        self.closed = False

    def poll(self, timeout_seconds: float) -> Sequence[Delivery]:
        return self._batches.pop(0) if self._batches else []

    def close(self) -> None:
        self.closed = True


class FakeClock:
    """A clock that moves only when told: `sleep` and every `poll` advance it by a fixed step."""

    def __init__(self, step: float = 1.0) -> None:
        self.now = 0.0
        self.step = step
        self.slept: list[float] = []

    def __call__(self) -> float:
        self.now += self.step
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)


class FixedOffsets:
    def __init__(self, end: Mapping[int, int], committed: Mapping[int, int | None]) -> None:
        self._end, self._committed = end, committed

    def end_offsets(self, topic: str) -> Mapping[int, int]:
        return self._end

    def committed_offsets(self, group_id: str, topic: str) -> Mapping[int, int | None]:
        return self._committed
