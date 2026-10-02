"""Tests — :mod:`statflows.core.rate_limiter`.

Frozen behaviour: parameter validation, sliding-window accounting, waiting when
the quota is exhausted (clock and ``sleep`` patched, no real wait), composite
limiters aggregating their members, and configuration-driven construction.
"""

from __future__ import annotations

import pytest

from statflows.core import rate_limiter as rl
from statflows.core.rate_limiter import (
    CompositeRateLimiter,
    RateLimiter,
    build_rate_limiter,
)


class FakeTime:
    """Deterministic clock: ``sleep`` advances ``time`` instead of blocking."""

    def __init__(self) -> None:
        self.now = 1_000.0
        self.slept: list[float] = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch) -> FakeTime:
    fake = FakeTime()
    monkeypatch.setattr(rl.time, "time", fake.time)
    monkeypatch.setattr(rl.time, "sleep", fake.sleep)
    return fake


# ──────────────────────────────────────────────────────────────────────
# Validation
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"max_requests": 0, "time_unit": "seconds"}, "max_requests"),
        ({"max_requests": 1, "time_unit": "seconds", "time_count": 0}, "time_count"),
        ({"max_requests": 1, "time_unit": "months"}, "time_unit"),
    ],
)
def test_invalid_parameters_are_rejected(kwargs, message) -> None:
    with pytest.raises(ValueError, match=message):
        RateLimiter(**kwargs)


def test_window_is_unit_times_count() -> None:
    assert RateLimiter(5, "hours", 2).window_seconds == 7200


def test_repr() -> None:
    assert repr(RateLimiter(3, "minutes", 2)) == (
        "RateLimiter(max_requests=3, time_unit='minutes', time_count=2)"
    )


# ──────────────────────────────────────────────────────────────────────
# RateLimiter
# ──────────────────────────────────────────────────────────────────────


def test_acquire_below_quota_does_not_wait(clock) -> None:
    limiter = RateLimiter(2, "seconds")
    limiter.acquire()
    limiter.acquire()
    assert clock.slept == []
    assert limiter.get_remaining_requests() == 0


def test_acquire_waits_for_oldest_request_to_leave_the_window(clock) -> None:
    limiter = RateLimiter(2, "seconds", 10)
    limiter.acquire()
    clock.now += 3
    limiter.acquire()
    clock.now += 2  # la plus ancienne a 5 s, il en reste 5 avant sortie de fenêtre
    limiter.acquire()
    assert clock.slept == [pytest.approx(5.0)]


def test_remaining_requests_recover_once_window_has_elapsed(clock) -> None:
    limiter = RateLimiter(1, "seconds")
    limiter.acquire()
    assert limiter.get_remaining_requests() == 0
    clock.now += 2
    assert limiter.get_remaining_requests() == 1


def test_stats_track_acquisitions_and_waits(clock) -> None:
    limiter = RateLimiter(1, "seconds", 4)
    limiter.acquire()
    limiter.acquire()  # attend 4 s
    stats = limiter.get_stats()
    assert stats.n_acquisitions == 2
    assert stats.total_wait_seconds == pytest.approx(4.0)
    assert stats.max_wait_seconds == pytest.approx(4.0)
    assert stats.remaining_requests == 0


def test_reset_clears_state_and_counters(clock) -> None:
    limiter = RateLimiter(1, "seconds", 4)
    limiter.acquire()
    limiter.acquire()
    limiter.reset()
    stats = limiter.get_stats()
    assert (stats.n_acquisitions, stats.total_wait_seconds) == (0, 0.0)
    assert stats.remaining_requests == 1


# ──────────────────────────────────────────────────────────────────────
# from_dict / build_rate_limiter
# ──────────────────────────────────────────────────────────────────────


def test_from_dict_defaults_count_to_one() -> None:
    limiter = RateLimiter.from_dict({"requests": 30, "unit": "minutes"})
    assert (limiter.max_requests, limiter.time_unit, limiter.time_count) == (
        30,
        "minutes",
        1,
    )


def test_from_dict_missing_keys() -> None:
    with pytest.raises(ValueError, match="Missing required keys"):
        RateLimiter.from_dict({"requests": 30})


def test_build_from_dict_gives_simple_limiter() -> None:
    assert isinstance(
        build_rate_limiter({"requests": 1, "unit": "seconds"}), RateLimiter
    )


def test_build_from_single_item_list_gives_simple_limiter() -> None:
    limiter = build_rate_limiter([{"requests": 1, "unit": "seconds"}])
    assert isinstance(limiter, RateLimiter)


def test_build_from_list_gives_composite() -> None:
    limiter = build_rate_limiter(
        [{"requests": 1, "unit": "seconds"}, {"requests": 500, "unit": "days"}]
    )
    assert isinstance(limiter, CompositeRateLimiter)
    assert len(limiter.limiters) == 2


def test_build_rejects_empty_list_and_unsupported_type() -> None:
    with pytest.raises(ValueError, match="at least one entry"):
        build_rate_limiter([])
    with pytest.raises(ValueError, match="Unsupported RATE_LIMIT"):
        build_rate_limiter("1/s")  # type: ignore[arg-type]


# ──────────────────────────────────────────────────────────────────────
# CompositeRateLimiter
# ──────────────────────────────────────────────────────────────────────


def test_composite_requires_at_least_one_limiter() -> None:
    with pytest.raises(ValueError, match="at least one limiter"):
        CompositeRateLimiter([])


def test_composite_acquires_every_limiter_and_reports_the_tightest(clock) -> None:
    per_second = RateLimiter(1, "seconds")
    per_day = RateLimiter(5, "days")
    composite = CompositeRateLimiter([per_second, per_day])

    composite.acquire()
    assert composite.get_remaining_requests() == 0  # min(0, 4)
    clock.now += 2
    assert composite.get_remaining_requests() == 1  # min(1, 4)

    composite.acquire()
    stats = composite.get_stats()
    assert stats.n_acquisitions == 2
    assert stats.total_wait_seconds == 0.0


def test_composite_stats_sum_waits_and_take_max_wait(clock) -> None:
    composite = CompositeRateLimiter(
        [RateLimiter(1, "seconds", 2), RateLimiter(1, "seconds", 3)]
    )
    composite.acquire()
    composite.acquire()
    stats = composite.get_stats()
    # Acquisition séquentielle : 2 s d'attente pour la 1re limite, puis les 3 s de
    # la 2nde sont entamées de 2 s d'horloge déjà écoulées → 1 s
    assert stats.total_wait_seconds == pytest.approx(3.0)
    assert stats.max_wait_seconds == pytest.approx(2.0)


def test_composite_reset_and_repr(clock) -> None:
    composite = CompositeRateLimiter([RateLimiter(1, "seconds")])
    composite.acquire()
    composite.reset()
    assert composite.get_remaining_requests() == 1
    assert repr(composite).startswith("CompositeRateLimiter(limiters=[RateLimiter(")
