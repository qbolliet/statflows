"""Tests — :class:`statflows.core.queries.SDMXQueryRequest` default keys.

Frozen behaviour of the keys inherited by the provider DTOs that do not
override them: ``get_dataflow_key`` (``agency::dataflow::version``) and
``identity_key`` (``agency::dataflow::version::dims``).
"""

from __future__ import annotations

from dataclasses import dataclass

from statflows.core.queries import SDMXQueryRequest


# Sous-classe concrète minimale, sans surcharge des clés
@dataclass
class _MinimalQuery(SDMXQueryRequest):
    agency: str
    dataflow: str
    version: str = "1.0"
    dimensions: dict | None = None


def test_default_dataflow_key_includes_agency_dataflow_and_version() -> None:
    query = _MinimalQuery("AG", "DF", "2.1")
    assert query.get_dataflow_key() == "AG::DF::2.1"


def test_default_identity_key_without_dimensions_ends_with_empty_segment() -> None:
    assert _MinimalQuery("AG", "DF").identity_key() == "AG::DF::1.0::"


def test_default_identity_key_encodes_sorted_dimensions() -> None:
    query = _MinimalQuery("AG", "DF", dimensions={"GEO": ["FR", "DE"], "FREQ": "Q"})
    assert query.identity_key() == "AG::DF::1.0::FREQ=Q;GEO=DE,FR"


def test_default_identity_key_ignores_dimension_order() -> None:
    first = _MinimalQuery("AG", "DF", dimensions={"A": ["x", "y"], "B": "z"})
    second = _MinimalQuery("AG", "DF", dimensions={"B": "z", "A": ["y", "x"]})
    assert first.identity_key() == second.identity_key()
