"""Retry policy validation and deterministic backoff calculations."""

import math
import random
from dataclasses import dataclass
from typing import Callable, Optional


@dataclass(frozen=True)
class RetryPolicy:
    """Per-task retry limits and delay settings. Attempts count committed leases."""

    max_attempts: int = 1
    initial_delay: float = 0.0
    multiplier: float = 2.0
    max_delay: float = 300.0
    jitter: float = 0.0

    def __post_init__(self) -> None:
        if isinstance(self.max_attempts, bool) or not isinstance(self.max_attempts, int) or self.max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        for name, value, minimum in (("initial_delay", self.initial_delay, 0.0),
                                     ("multiplier", self.multiplier, 1.0),
                                     ("max_delay", self.max_delay, 0.0),
                                     ("jitter", self.jitter, 0.0)):
            try:
                valid = math.isfinite(value) and value >= minimum
            except (TypeError, ValueError):
                valid = False
            if not valid:
                raise ValueError(f"{name} must be finite and >= {minimum}")
        if self.jitter > 1.0:
            raise ValueError("jitter must be between 0 and 1")

    def delay_for(self, attempt_number: int,
                  random_value: Optional[Callable[[], float]] = None) -> float:
        """Return capped exponential delay with configurable full-jitter mixing."""
        if isinstance(attempt_number, bool) or not isinstance(attempt_number, int) or attempt_number < 1:
            raise ValueError("attempt_number must be a positive integer")
        if self.initial_delay == 0.0 or self.max_delay == 0.0:
            ceiling = 0.0
        elif self.multiplier == 1.0 or attempt_number == 1:
            ceiling = min(self.max_delay, self.initial_delay)
        else:
            # Compare logarithms before exponentiation so even extreme valid
            # policies cannot overflow while calculating a capped delay.
            cap_exponent = (math.log(self.max_delay) - math.log(self.initial_delay)) / math.log(self.multiplier)
            if attempt_number - 1 >= cap_exponent:
                ceiling = self.max_delay
            else:
                ceiling = self.initial_delay * self.multiplier ** (attempt_number - 1)
        if self.jitter == 0.0 or ceiling == 0.0:
            return ceiling
        sample = (random_value or random.random)()
        if not math.isfinite(sample) or not 0.0 <= sample <= 1.0:
            raise ValueError("random source must return a value between 0 and 1")
        return ceiling * ((1.0 - self.jitter) + self.jitter * sample)


class PermanentTaskError(Exception):
    """Marks an action failure as ineligible for retry."""
