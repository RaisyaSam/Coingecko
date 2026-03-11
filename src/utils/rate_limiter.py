import time
from dataclasses import dataclass

@dataclass
class RateLimiter:
    """Simple token bucket-like limiter: allow `max_per_minute` requests."""
    max_per_minute: int
    _window_start: float = 0.0
    _count: int = 0

    def wait(self) -> None:
        now = time.time()
        if self._window_start == 0.0:
            self._window_start = now
            self._count = 0

        # reset window each 60s
        if now - self._window_start >= 60:
            self._window_start = now
            self._count = 0

        if self._count >= self.max_per_minute:
            sleep_for = 60 - (now - self._window_start)
            if sleep_for > 0:
                time.sleep(sleep_for)
            self._window_start = time.time()
            self._count = 0

        self._count += 1
