"""Tests — :mod:`statflows.sources.oecd.endpoints`.

Frozen behaviour: Accept headers per format and API version, v1 / v2 URL paths
and query parameters, ``updatedAfter`` conversion, and the v1 (multi-values) vs
v2 (single value or wildcard) positional dimension filters.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

from statflows.core.sdmx import DimensionAtObservation, SDMXVersion
from statflows.sources.oecd.endpoints import (
    OECDEndpointBuilder,
    OECDEndpointBuilderV1,
    OECDEndpointBuilderV2,
)
from statflows.sources.oecd.formats import OECDResponseFormat

V1 = OECDEndpointBuilderV1()
V2 = OECDEndpointBuilderV2()

# ──────────────────────────────────────────────────────────────────────
# Accept headers
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("fmt", "version", "expected"),
    [
        (
            OECDResponseFormat.JSON,
            SDMXVersion.V1,
            "application/vnd.sdmx.data+json; charset=utf-8; version=1.0",
        ),
        (
            OECDResponseFormat.JSON,
            SDMXVersion.V2,
            "application/vnd.sdmx.data+json; charset=utf-8; version=2",
        ),
        (
            OECDResponseFormat.CSV,
            SDMXVersion.V1,
            "application/vnd.sdmx.data+csv; charset=utf-8",
        ),
        (
            OECDResponseFormat.CSV_LABELS,
            SDMXVersion.V2,
            "application/vnd.sdmx.data+csv; charset=utf-8; version=2",
        ),
        (
            OECDResponseFormat.XML,
            SDMXVersion.V1,
            "application/vnd.sdmx.structurespecificdata+xml; charset=utf-8; version=2.1",
        ),
    ],
)
def test_accept_header_per_format_and_version(fmt, version, expected) -> None:
    assert OECDEndpointBuilder.get_accept_header(fmt, version) == expected


def test_accept_header_unknown_format_falls_back_to_json() -> None:
    assert OECDEndpointBuilder.get_accept_header("other", SDMXVersion.V1) == (  # type: ignore[arg-type]
        "application/json"
    )


def test_structure_accept_header_is_always_json_1_0() -> None:
    assert OECDEndpointBuilder.get_structure_accept_header().endswith("version=1.0")


def test_build_headers_defaults_and_language() -> None:
    headers = V2.build_headers()
    assert headers["Accept"].endswith("version=2")  # CSV_LABELS par défaut, v2
    assert headers["Accept-Encoding"] == "gzip, deflate"
    assert "Accept-Language" not in headers

    headers = V1.build_headers(
        accept_encoding="gzip",
        accept_language="fr",
        response_format=OECDResponseFormat.JSON,
    )
    assert headers == {
        "Accept": "application/vnd.sdmx.data+json; charset=utf-8; version=1.0",
        "Accept-Encoding": "gzip",
        "Accept-Language": "fr",
    }


def test_structure_params_are_shared_by_both_versions() -> None:
    expected = {"references": "all", "detail": "referencepartial"}
    assert V1.build_structure_params() == expected
    assert V2.build_structure_params() == expected
    assert V1.build_structure_params(references=None, detail=None) == {}


# ──────────────────────────────────────────────────────────────────────
# v1
# ──────────────────────────────────────────────────────────────────────


def test_v1_endpoints() -> None:
    assert V1.build_data_endpoint("DF", "AG", "1.0") == "data/AG,DF,1.0/all"
    assert V1.build_data_endpoint("DF", "AG", "1.0", key="FRA.M") == (
        "data/AG,DF,1.0/FRA.M"
    )
    assert V1.build_structure_endpoint(None, "DF", "AG", None) == "dataflow/AG/DF/+"
    assert V1.build_structure_endpoint(None, "DF", "AG", "2.0") == "dataflow/AG/DF/2.0"


def test_v1_data_params() -> None:
    assert V1.build_data_params() == {
        "dimensionAtObservation": "AllDimensions",
        "format": "csvfilewithlabels",
    }
    params = V1.build_data_params(
        start_period="2020",
        end_period="2022",
        last_n_observations=3,
        response_format=OECDResponseFormat.JSON,
        dimension_at_observation=DimensionAtObservation.TIME_PERIOD.value,
        updated_after="2024-01-01T00:00:00Z",  # ignoré en v1
    )
    assert params == {
        "dimensionAtObservation": "TIME_PERIOD",
        "format": "jsondata",
        "startPeriod": "2020",
        "endPeriod": "2022",
        "lastNObservations": 3,
    }


def test_v1_dimension_filter() -> None:
    build = OECDEndpointBuilderV1.build_dimension_filter
    assert build({}) == "all"
    assert build({}, 3) == ".."
    assert build({0: ["FRA", "DEU"], 2: ["LI"]}) == "FRA,DEU..LI"
    assert build({1: ["M"]}, 3) == ".M."


# ──────────────────────────────────────────────────────────────────────
# v2
# ──────────────────────────────────────────────────────────────────────


def test_v2_endpoints() -> None:
    assert V2.build_data_endpoint("DF", "AG", "1.0") == "v2/data/dataflow/AG/DF/1.0/*"
    assert V2.build_data_endpoint("DF", "AG", "1.0", key="FRA.M") == (
        "v2/data/dataflow/AG/DF/1.0/FRA.M"
    )
    assert V2.build_structure_endpoint(None, "DF", "AG", None) == (
        "v2/structure/dataflow/AG/DF/+"
    )


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"start_period": "2020", "end_period": "2022"}, "ge:2020+le:2022"),
        ({"start_period": "2020"}, "ge:2020"),
        ({"end_period": "2022"}, "le:2022"),
    ],
)
def test_v2_time_period_filter(kwargs, expected) -> None:
    assert V2.build_data_params(**kwargs)["c[TIME_PERIOD]"] == expected


def test_v2_data_params_attributes_measures_and_last_n() -> None:
    params = V2.build_data_params(
        attributes="dsd", measures="all", last_n_observations=5
    )
    assert params == {
        "dimensionAtObservation": "AllDimensions",
        "format": "csvfilewithlabels",
        "attributes": "dsd",
        "measures": "all",
        "lastNObservations": 5,
    }


@pytest.mark.parametrize(
    ("updated_after", "expected"),
    [
        ("2024-01-01T00:00:00Z", "2024-01-01T00:00:00Z"),
        (datetime(2024, 1, 2, 3, 4, 5), "2024-01-02T03:04:05Z"),
        (datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC), "2024-01-02T03:04:05+00:00"),
        (
            datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone(timedelta(hours=2))),
            "2024-01-02T03:04:05+02:00",
        ),
    ],
)
def test_v2_updated_after_conversion(updated_after, expected) -> None:
    assert V2.build_data_params(updated_after=updated_after)["updatedAfter"] == expected


def test_v2_dimension_filter() -> None:
    build = OECDEndpointBuilderV2.build_dimension_filter
    assert build({}) == "*"
    assert build({}, 3) == "*.*.*"
    assert build({0: ["FRA"], 2: ["LI"]}) == "FRA.*.LI"
    assert build({1: ["M"]}, 3) == "*.M.*"


def test_v2_dimension_filter_rejects_multiple_values() -> None:
    with pytest.raises(ValueError, match="does not support multiple values"):
        OECDEndpointBuilderV2.build_dimension_filter({0: ["FRA", "DEU"]})


def test_base_dimension_filter_is_abstract() -> None:
    with pytest.raises(NotImplementedError):
        OECDEndpointBuilder.build_dimension_filter({})
