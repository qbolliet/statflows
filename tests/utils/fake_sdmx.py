"""Faux client SDMX, fausses requêtes et horloge déterministe pour l'orchestrateur.

Implémente l'interface minimale consommée par
:class:`~statflows.core.download.SDMXDownloader` (``fetch_updates``,
``resolve_query_structure``, ``structure_registry``) sans aucun appel réseau.

Les données sont déterministes : la requête d'indice ``i`` porte le déclarant
``R{i:05d}`` et renvoie, si elle n'est pas vide, ``len(PRODUCTS) × len(PERIODS)``
observations dont la valeur ne dépend que de ``(i, produit, période)``. Deux
requêtes ne se recouvrent donc jamais sur la clé primaire.
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
    """Requête minimale : ``identity_key()``, ``to_dict()``, ``agency``, ``dataflow``."""

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
    """Construit ``n`` requêtes réparties par blocs de 7 sur les deux dataflows.

    Le découpage par blocs (plutôt qu'une alternance stricte) garantit que les
    requêtes non vides (``index % data_every == 0``) tombent dans les deux
    dataflows quelle que soit la parité de ``data_every``.
    """
    return [FakeQuery(i, DATAFLOWS[(i // 7) % len(DATAFLOWS)]) for i in range(n)]


def query_frame(query: FakeQuery) -> pd.DataFrame:
    """Données (déterministes) renvoyées pour une requête non vide."""
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
    """Structure du dataflow simulé (dimensions = clé primaire hors période)."""
    return DataflowStructure(
        agency=AGENCY,
        dataflow=dataflow,
        num_dimensions=2,
        dimensions=[DimensionInfo("REF_AREA", 0), DimensionInfo("PRODUCT", 1)],
    )


class FakeClock:
    """Horloge factice avançant d'une minute à chaque appel à ``fetch_updates``.

    La date de référence d'une requête (capturée juste avant son ``fetch``) vaut
    donc ``T0 + i minutes`` pour la ``i``-ème requête traitée, quel que soit le
    nombre d'appels intermédiaires à l'horloge : le registre est identique d'un
    mode d'écriture à l'autre.
    """

    def __init__(self) -> None:
        self.ticks = 0

    def __call__(self) -> datetime:
        return T0 + timedelta(minutes=self.ticks)


@dataclass
class FakeClient:
    """Client minimal consommé par ``SDMXDownloader``.

    Args:
        clock: Horloge avancée à chaque ``fetch_updates``.
        data_every: Une requête sur ``data_every`` renvoie des données, les
            autres un DataFrame vide.
        on_fetch: Rappel optionnel ``(position, query)`` exécuté au début de
            chaque ``fetch_updates`` (simulation d'un signal, d'une panne…).
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
    """Contenu attendu de la table de faits d'un dataflow, trié par clé."""
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
