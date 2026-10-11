"""Tests — :class:`statflows.sources.eurostat.queries.EurostatQueryRequest`.

Frozen behaviour: the agency is always ``ESTAT`` and left out of the dataflow
key; the identity key (primary key of the download registry) is inherited from
the shared SDMX builder and ignores presentation fields such as ``format``.
"""

from __future__ import annotations

from statflows.sources.eurostat.formats import EurostatResponseFormat
from statflows.sources.eurostat.queries import EurostatQueryRequest


def test_agency_is_always_estat() -> None:
    assert EurostatQueryRequest(dataflow="une_rt_m").agency == "ESTAT"


def test_dataflow_key_leaves_the_agency_out() -> None:
    assert EurostatQueryRequest(dataflow="une_rt_m").get_dataflow_key() == "une_rt_m::*"
    assert (
        EurostatQueryRequest(dataflow="une_rt_m", version="1.0").get_dataflow_key()
        == "une_rt_m::1.0"
    )


def test_identity_key_is_exact_and_ignores_format_and_list_order() -> None:
    query = EurostatQueryRequest(
        dataflow="une_rt_m", dimensions={"geo": ["FR", "DE"], "unit": "PC"}
    )
    assert query.identity_key() == "ESTAT::une_rt_m::*::geo=DE,FR;unit=PC"
    reordered = EurostatQueryRequest(
        dataflow="une_rt_m",
        dimensions={"unit": "PC", "geo": ["DE", "FR"]},
        format=EurostatResponseFormat.JSON,
    )
    assert reordered.identity_key() == query.identity_key()
