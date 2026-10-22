"""Tests of the shared last-update check (``FreshnessCheckMixin``).

The fake client below implements only the two extension points
(``_freshness_key``, ``_fetch_last_update``) on top of ``execute_query``: it is
all a new provider needs to get the incremental ``fetch_updates`` and its
per-run cache.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import pytest

import statflows.core.client as client_module
from statflows.core.client import FreshnessCheckMixin
from statflows.core.reports import FreshnessStats

# Date de dernière mise à jour simulée côté fournisseur
LAST_UPDATE = datetime(2026, 3, 1, tzinfo=UTC)
# Dates de téléchargement précédent : après et avant la mise à jour
AFTER = datetime(2026, 4, 1, tzinfo=UTC)
BEFORE = datetime(2026, 2, 1, tzinfo=UTC)


@dataclass(frozen=True)
class _Query:
    """Query of a fake dataflow, split by product batch."""

    dataflow: str
    batch: int = 0


class _FakeClient(FreshnessCheckMixin):
    """Provider implementing only the two extension points (and execute_query)."""

    def __init__(self, last_update: Any = LAST_UPDATE) -> None:
        super().__init__()
        # Réponse du fournisseur : date, None, ou exception à lever
        self.last_update = last_update
        self.lookups: list[_Query] = []
        self.executed: list[_Query] = []

    def _freshness_key(self, query: _Query) -> str:
        return query.dataflow

    def _fetch_last_update(self, query: _Query) -> datetime | None:
        self.lookups.append(query)
        if isinstance(self.last_update, BaseException):
            raise self.last_update
        return self.last_update

    def execute_query(self, query: _Query) -> pd.DataFrame:
        self.executed.append(query)
        return pd.DataFrame({"batch": [query.batch]})


class _Clock:
    """Monotonic clock substituted for the failure expiry."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    fake = _Clock()
    monkeypatch.setattr(client_module, "_monotonic", fake)
    return fake


# ──────────────────────────────────────────────────────────────────────
# Gabarit et cache
# ──────────────────────────────────────────────────────────────────────


def test_two_extension_points_are_enough_for_a_new_client() -> None:
    client = _FakeClient()
    assert client.fetch_updates(_Query("DF"), None)["batch"].tolist() == [0]
    assert client.fetch_updates(_Query("DF"), AFTER).empty
    assert len(client.fetch_updates(_Query("DF"), BEFORE)) == 1


def test_a_client_without_the_extension_points_cannot_be_built() -> None:
    class Incomplete(FreshnessCheckMixin):
        def execute_query(self, query: Any) -> pd.DataFrame:
            return pd.DataFrame()

    with pytest.raises(TypeError, match="_fetch_last_update"):
        Incomplete()  # type: ignore[abstract]


def test_first_download_is_full_without_any_lookup() -> None:
    client = _FakeClient()
    client.fetch_updates(_Query("DF"), None)
    assert client.lookups == []
    assert client.freshness_stats_ == FreshnessStats()


def test_a_thousand_queries_of_one_key_cost_one_lookup() -> None:
    client = _FakeClient()
    results = [
        client.fetch_updates(_Query("DF", batch), AFTER) for batch in range(1000)
    ]

    assert all(result.empty for result in results)
    assert len(client.lookups) == 1
    assert client.executed == []
    assert client.freshness_stats_ == FreshnessStats(n_checks=1, n_cache_hits=999)


def test_two_keys_cost_two_lookups() -> None:
    client = _FakeClient()
    for dataflow in ("A", "B", "A", "B"):
        client.fetch_updates(_Query(dataflow), AFTER)
    assert [query.dataflow for query in client.lookups] == ["A", "B"]
    assert client.freshness_stats_ == FreshnessStats(n_checks=2, n_cache_hits=2)


def test_updated_data_is_fetched_for_every_query() -> None:
    client = _FakeClient()
    queries = [_Query("DF", batch) for batch in range(5)]
    results = [client.fetch_updates(query, BEFORE) for query in queries]

    assert [result["batch"].tolist() for result in results] == [[b] for b in range(5)]
    assert client.executed == queries
    assert len(client.lookups) == 1


def test_a_date_equal_to_since_skips_the_query() -> None:
    client = _FakeClient()
    assert client.fetch_updates(_Query("DF"), LAST_UPDATE).empty
    assert client.fetch_updates(_Query("DF"), LAST_UPDATE - timedelta(microseconds=1))[
        "batch"
    ].tolist() == [0]


def test_naive_dates_are_compared_in_utc() -> None:
    # Date fournisseur naïve (lue comme UTC) et date de téléchargement naïve
    client = _FakeClient(last_update=datetime(2026, 3, 1, 12))
    assert client.fetch_updates(_Query("DF"), datetime(2026, 3, 1, 13)).empty
    paris = datetime(2026, 3, 1, 13, tzinfo=UTC).astimezone(
        timezone(timedelta(hours=2))
    )
    assert client.fetch_updates(_Query("DF"), paris).empty
    assert len(client.fetch_updates(_Query("DF"), datetime(2026, 3, 1, 11))) == 1


def test_cached_dates_are_utc_aware() -> None:
    client = _FakeClient(last_update=datetime(2026, 3, 1, 12))
    assert client._query_last_update(_Query("DF")) == datetime(
        2026, 3, 1, 12, tzinfo=UTC
    )


def test_clear_freshness_cache_forgets_answers_but_keeps_counters() -> None:
    client = _FakeClient()
    client.fetch_updates(_Query("DF"), AFTER)
    client.clear_freshness_cache()
    client.fetch_updates(_Query("DF"), AFTER)

    assert len(client.lookups) == 2
    assert client.freshness_stats_ == FreshnessStats(n_checks=2)


# ──────────────────────────────────────────────────────────────────────
# Échecs de résolution
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("answer", [RuntimeError("down"), None], ids=["raises", "none"])
def test_unresolved_date_refreshes_and_is_remembered_within_the_ttl(
    clock: _Clock, answer: Any
) -> None:
    client = _FakeClient(last_update=answer)
    for batch in range(3):
        clock.now = 100.0 * batch  # < 300 s depuis l'échec
        assert len(client.fetch_updates(_Query("DF", batch), AFTER)) == 1

    assert len(client.lookups) == 1
    assert len(client.executed) == 3
    assert client.freshness_stats_ == FreshnessStats(
        n_checks=1, n_cache_hits=2, n_unresolved=1
    )


def test_unresolved_date_is_retried_once_the_ttl_elapsed(clock: _Clock) -> None:
    client = _FakeClient(last_update=RuntimeError("down"))
    client.fetch_updates(_Query("DF"), AFTER)

    # Fournisseur rétabli, durée de mémorisation écoulée : nouvelle vérification
    client.last_update = LAST_UPDATE
    clock.now = client.FRESHNESS_FAILURE_TTL
    assert client.fetch_updates(_Query("DF"), AFTER).empty
    # Succès : plus d'expiration, servi par le cache ensuite
    clock.now = 10_000.0
    assert client.fetch_updates(_Query("DF"), AFTER).empty

    assert len(client.lookups) == 2
    assert client.freshness_stats_ == FreshnessStats(
        n_checks=2, n_cache_hits=1, n_unresolved=1
    )


def test_unresolved_date_is_never_retried_without_ttl(clock: _Clock) -> None:
    class RunLongFailure(_FakeClient):
        FRESHNESS_FAILURE_TTL = None

    client = RunLongFailure(last_update=RuntimeError("down"))
    client.fetch_updates(_Query("DF"), AFTER)
    clock.now = 1e9
    client.fetch_updates(_Query("DF"), AFTER)
    assert len(client.lookups) == 1


def test_a_stop_request_crosses_the_lookup_without_caching() -> None:
    class Stop(BaseException):
        pass

    client = _FakeClient(last_update=Stop())
    with pytest.raises(Stop):
        client.fetch_updates(_Query("DF"), AFTER)

    assert client._freshness_cache == {}
    assert client.executed == []


# ──────────────────────────────────────────────────────────────────────
# Horizon de connaissance
# ──────────────────────────────────────────────────────────────────────


def test_a_skip_exposes_the_instant_the_answer_was_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    instants = iter([datetime(2026, 5, 1, 8, tzinfo=UTC)])
    monkeypatch.setattr(client_module, "_utc_now", lambda: next(instants))
    client = _FakeClient()

    client.fetch_updates(_Query("DF", 0), AFTER)
    client.fetch_updates(_Query("DF", 1), AFTER)

    # Réponse servie par le cache : horizon = instant de la vérification initiale
    assert client.last_fetch_as_of_ == datetime(2026, 5, 1, 8, tzinfo=UTC)


def test_a_fetch_or_a_first_download_resets_the_horizon() -> None:
    client = _FakeClient()
    client.fetch_updates(_Query("DF"), AFTER)
    assert client.last_fetch_as_of_ is not None

    client.fetch_updates(_Query("DF"), BEFORE)
    assert client.last_fetch_as_of_ is None

    client.fetch_updates(_Query("DF"), AFTER)
    client.fetch_updates(_Query("DF"), None)
    assert client.last_fetch_as_of_ is None
