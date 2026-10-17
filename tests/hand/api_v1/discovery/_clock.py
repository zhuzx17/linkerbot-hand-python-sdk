"""Logical time for synchronous discovery peers, separate from transport time."""


class DiscoveryClock:
    def __init__(self) -> None:
        # Keep zero available as the explicit expired-budget sentinel.
        self.now = 1_000.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, duration_s: float) -> None:
        self.now += duration_s
