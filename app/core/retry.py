"""Bounded retry with exponential backoff."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


def backoff_delay(attempt: int, base: float, maximum: float) -> float:
    """Delay before retry number ``attempt`` (1-based): base * 2^(attempt-1), capped."""
    if attempt <= 0 or base <= 0:
        return 0.0
    return min(maximum, base * (2 ** (attempt - 1)))


def retry_call(
    fn: Callable[[], T],
    *,
    attempts: int,
    base_delay: float,
    max_delay: float,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    sleep: Callable[[float], None] = time.sleep,
    on_retry: Callable[[int, BaseException, float], None] | None = None,
) -> T:
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            if attempt == attempts:
                raise
            delay = backoff_delay(attempt, base_delay, max_delay)
            if on_retry is not None:
                on_retry(attempt, exc, delay)
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover
