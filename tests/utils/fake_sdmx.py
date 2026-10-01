"""Fake SDMX client, fake queries and deterministic clock for the orchestrator.

Implements the minimal interface consumed by
:class:`~statflows.core.download.SDMXDownloader` (``fetch_updates``,
``resolve_query_structure``, ``structure_registry``) without any network call.

The data are deterministic: the query of index ``i`` carries the reporter
``R{i:05d}`` and, if it is not empty, returns ``len(PRODUCTS) × len(PERIODS)``
observations whose value only depends on ``(i, product, period)``. Two queries
therefore never overlap on the primary key.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pandas as pd

from statflows.core.structures import (
    DataflowStructure,
    DataflowStructureRegistry,
    DimensionInfo,
)

# Agence et dataflows simulés (deux schémas DuckLake)
AGENCY = "FAKE"
DATAFLOWS = ("DF_ALPHA", "DF_BETA")
# Produits et périodes renvoyés par chaque requête non vide
PRODUCTS = ("P1", "P2")
PERIODS = ("2020", "2021")
# Instant de départ de l'horloge factice
T0 = datetime(2026, 1, 1, tzinfo=UTC)


@dataclass
class FakeQuery:
    """Minimal query: ``identity_key()``, ``to_dict()``, ``agency``, ``dataflow``."""

    index: int
    dataflow: str
    agency: str = AGENCY

    @property
    def reporter(self) -> str:
        return f"R{self.index:05d}"

    def identity_key(self) -> str:
        return f"{self.agency}:{self.dataflow}:{self.reporter}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "dataflow": self.dataflow,
            "dimensions": {"REF_AREA": self.reporter},
        }


def make_queries(n: int) -> list[FakeQuery]:
    """Build ``n`` queries spread in blocks of 7 over the two dataflows.

    Splitting in blocks (rather than a strict alternation) guarantees that the
    non-empty queries (``index % data_every == 0``) fall into both dataflows
    whatever the parity of ``data_every``.
    """
    return [FakeQuery(i, DATAFLOWS[(i // 7) % len(DATAFLOWS)]) for i in range(n)]


def query_frame(query: FakeQuery) -> pd.DataFrame:
    """(Deterministic) data returned for a non-empty query."""
    rows = [
        {
            "REF_AREA": query.reporter,
            "PRODUCT": product,
            "TIME_PERIOD": period,
            "OBS_VALUE": float(query.index * 100 + p * 10 + t),
        }
        for p, product in enumerate(PRODUCTS)
        for t, period in enumerate(PERIODS)
    ]
    return pd.DataFrame(rows)


def structure_for(dataflow: str) -> DataflowStructure:
    """Structure of the simulated dataflow (dimensions = primary key excluding period)."""
    return DataflowStructure(
        agency=AGENCY,
        dataflow=dataflow,
        num_dimensions=2,
        dimensions=[DimensionInfo("REF_AREA", 0), DimensionInfo("PRODUCT", 1)],
    )


class FakeClock:
    """Fake clock advancing one minute on every call to ``fetch_updates``.

    The reference date of a query (captured right before its ``fetch``) is therefore
    ``T0 + i minutes`` for the ``i``-th processed query, whatever the number of
    intermediate calls to the clock: the registry is identical from one write mode to
    the other.
    """

    def __init__(self) -> None:
        self.ticks = 0

    def __call__(self) -> datetime:
        return T0 + timedelta(minutes=self.ticks)


@dataclass
class FakeClient:
    """Minimal client consumed by ``SDMXDownloader``.

    Args:
        clock: Clock advanced on every ``fetch_updates``.
        data_every: One query out of ``data_every`` returns data, the others an
            empty DataFrame.
        on_fetch: Optional callback ``(position, query)`` run at the start of every
            ``fetch_updates`` (simulation of a signal, a failure…).
    """

    clock: FakeClock
    data_every: int = 1
    on_fetch: Callable[[int, FakeQuery], None] | None = None
    structure_registry: DataflowStructureRegistry = field(
        default_factory=DataflowStructureRegistry
    )
    calls: list[str] = field(default_factory=list)

    def fetch_updates(
        self, query: FakeQuery, since: datetime | None, n_observations: int
    ) -> pd.DataFrame:
        position = len(self.calls)
        self.calls.append(query.identity_key())
        if self.on_fetch is not None:
            self.on_fetch(position, query)
        # Avance de l'horloge : la requête suivante a une date de référence distincte
        self.clock.ticks += 1
        if query.index % self.data_every != 0:
            return pd.DataFrame()
        return query_frame(query)

    def resolve_query_structure(self, query: FakeQuery) -> DataflowStructure:
        structure = structure_for(query.dataflow)
        if not self.structure_registry.has(query.agency, query.dataflow):
            self.structure_registry.register(structure)
        return structure


def expected_table(
    queries: list[FakeQuery], dataflow: str, data_every: int
) -> pd.DataFrame:
    """Expected content of a dataflow's fact table, sorted by key."""
    frames = [
        query_frame(q)
        for q in queries
        if q.dataflow == dataflow and q.index % data_every == 0
    ]
    return (
        pd.concat(frames, ignore_index=True)
        .sort_values(["REF_AREA", "PRODUCT", "TIME_PERIOD"])
        .reset_index(drop=True)
    )
