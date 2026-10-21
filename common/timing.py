"""Shared timing helpers for warm-up + measured runs."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

TIMEOUT_SECONDS = 900.0

T = TypeVar("T")


def timed_call(fn: Callable[[], T], timeout_seconds: float = TIMEOUT_SECONDS) -> tuple[float, T | None, str]:
    """
    Time a single call. If wall time exceeds timeout, return TIMEOUT_900.
    Callers should still stop long-running work themselves when possible;
    this wrapper records status based on elapsed time after completion.
    """
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    if elapsed > timeout_seconds:
        return timeout_seconds, result, "TIMEOUT_900"
    return elapsed, result, "OK"


def warm_up_then_measure(
    fn: Callable[[], T],
    *,
    n_runs: int = 3,
    do_warmup: bool = True,
) -> list[tuple[float, T | None, str]]:
    """One unrecorded warm-up, then n_runs measured runs."""
    if do_warmup:
        fn()
    measured: list[tuple[float, T | None, str]] = []
    for _ in range(n_runs):
        measured.append(timed_call(fn))
        if measured[-1][2] == "TIMEOUT_900":
            break
    return measured
