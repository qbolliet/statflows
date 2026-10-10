"""Tests — :class:`statflows.sources.comtrade.queries.ComtradeQueryRequest`.

The identity key indexes the download registry: changing its layout would make
every existing query be downloaded again in full. The exact strings below are
therefore literal on purpose. Frozen behaviour: unset (``None``) fields are
omitted, anchored defaults always appear, ``format`` has no effect, and the
order of list elements has no effect (values are sorted by the shared builder).
"""

from __future__ import annotations

from statflows.sources.comtrade.formats import ComtradeResponseFormat
from statflows.sources.comtrade.queries import ComtradeQueryRequest


def test_agency_is_always_comtrade() -> None:
    assert ComtradeQueryRequest(dataflow="C_A_HS").agency == "COMTRADE"


def test_dataflow_key_leaves_the_agency_out() -> None:
    assert ComtradeQueryRequest(dataflow="C_A_HS").get_dataflow_key() == "C_A_HS::*"
    assert (
        ComtradeQueryRequest(dataflow="C_M_HS", version="1").get_dataflow_key()
        == "C_M_HS::1"
    )


def test_identity_key_of_a_minimal_query_keeps_only_the_anchored_defaults() -> None:
    query = ComtradeQueryRequest(dataflow="C_A_HS")
    assert query.identity_key() == (
        "COMTRADE::C_A_HS::*::classification=HS;frequency=annual;"
        "include_desc=True;type_code=C"
    )


def test_identity_key_of_a_monthly_query_with_explicit_periods() -> None:
    query = ComtradeQueryRequest(
        dataflow="C_M_HS",
        flows=["M", "X"],
        reporters=[251, 276],
        partners="0",
        products=["010121", "010129"],
        periods=["202001", "202002"],
        frequency="monthly",
        max_records=500,
        count_only=False,
    )
    assert query.identity_key() == (
        "COMTRADE::C_M_HS::*::classification=HS;count_only=False;flows=M,X;"
        "frequency=monthly;include_desc=True;max_records=500;partners=0;"
        "periods=202001,202002;products=010121,010129;reporters=251,276;"
        "type_code=C"
    )


def test_identity_key_of_a_query_with_period_boundaries() -> None:
    query = ComtradeQueryRequest(
        dataflow="C_M_HS",
        flows="M",
        reporters="FRA",
        period_start="2020-01",
        period_end="2020-12",
        frequency="monthly",
    )
    assert query.identity_key() == (
        "COMTRADE::C_M_HS::*::classification=HS;flows=M;frequency=monthly;"
        "include_desc=True;period_end=2020-12;period_start=2020-01;"
        "reporters=FRA;type_code=C"
    )


def test_unset_fields_are_omitted_from_the_key() -> None:
    key = ComtradeQueryRequest(dataflow="C_A_HS", reporters="FRA").identity_key()
    for name in ("flows", "partners", "products", "periods", "mot", "max_records"):
        assert f"{name}=" not in key
    assert "reporters=FRA" in key


def test_format_has_no_effect_on_the_key() -> None:
    json_query = ComtradeQueryRequest(dataflow="C_A_HS", reporters="FRA")
    csv_query = ComtradeQueryRequest(
        dataflow="C_A_HS", reporters="FRA", format=ComtradeResponseFormat.CSV
    )
    assert csv_query.identity_key() == json_query.identity_key()


def test_list_element_order_has_no_effect_on_the_key() -> None:
    first = ComtradeQueryRequest(
        dataflow="C_A_HS", flows=["M", "X"], reporters=[251, 276]
    )
    second = ComtradeQueryRequest(
        dataflow="C_A_HS", flows=["X", "M"], reporters=[276, 251]
    )
    assert first.identity_key() == second.identity_key()


def test_scalar_and_single_element_list_share_the_same_key() -> None:
    scalar = ComtradeQueryRequest(dataflow="C_A_HS", reporters="FRA")
    listed = ComtradeQueryRequest(dataflow="C_A_HS", reporters=["FRA"])
    assert scalar.identity_key() == listed.identity_key()
