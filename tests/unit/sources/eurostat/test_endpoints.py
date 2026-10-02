"""Tests — :mod:`statflows.sources.eurostat.endpoints`.

Frozen behaviour: URL paths and query parameters of the SDMX 3.0 builder
(filters in ``c[DIM]`` parameters) and of the SDMX 2.1 builder (positional key
in the path, wildcard tokens rewritten).
"""

from __future__ import annotations

import pytest

from statflows.core.sdmx import StructureResourceType
from statflows.sources.eurostat.endpoints import (
    EurostatEndpointBuilderV21,
    EurostatEndpointBuilderV30,
)
from statflows.sources.eurostat.formats import EurostatResponseFormat

V30 = EurostatEndpointBuilderV30()
V21 = EurostatEndpointBuilderV21()

# ──────────────────────────────────────────────────────────────────────
# En-têtes
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("builder", "version"), [(V30, "3.0.0"), (V21, "2.1")], ids=["v30", "v21"]
)
def test_headers_accept_and_optional_fields(builder, version) -> None:
    assert builder.build_headers() == {
        "Accept": f"application/vnd.sdmx.structure+xml;version={version}"
    }
    headers = builder.build_headers(accept_encoding="gzip", accept_language="en")
    assert headers["Accept-Encoding"] == "gzip"
    assert headers["Accept-Language"] == "en"


# ──────────────────────────────────────────────────────────────────────
# SDMX 3.0
# ──────────────────────────────────────────────────────────────────────


def test_v30_data_endpoint_with_and_without_key() -> None:
    assert V30.build_data_endpoint("NAMA", "ESTAT", "1.0") == (
        "/sdmx/3.0/data/dataflow/ESTAT/NAMA/1.0"
    )
    assert V30.build_data_endpoint("NAMA", "ESTAT", "*", key="A.FR") == (
        "/sdmx/3.0/data/dataflow/ESTAT/NAMA/*/A.FR"
    )


def test_v30_data_params_default_is_only_compress_false() -> None:
    assert V30.build_data_params() == {"compress": "false"}


def test_v30_data_params_full() -> None:
    params = V30.build_data_params(
        start_period="2020",
        end_period="2022",
        last_n_observations=2,
        first_n_observations=1,
        compress=True,
        dimensions={"geo": ["FR", "DE"], "freq": ["A"]},
        response_format=EurostatResponseFormat.CSV,
        response_format_version="1.0",
        lang="en",
        labels="name",
        attributes="dsd",
        measures="all",
        return_data="all",
    )
    assert params == {
        "c[GEO]": "FR,DE",
        "c[FREQ]": "A",
        "c[TIME_PERIOD]": "ge:2020+le:2022",
        "lastNObservations": "2",
        "firstNObservations": "1",
        "attributes": "dsd",
        "measures": "all",
        "format": "csvdata",
        "formatVersion": "1.0",
        "lang": "en",
        "labels": "name",
        "compress": "true",
        "returnData": "all",
    }


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"start_period": "2020"}, "ge:2020"),
        ({"end_period": "2022"}, "le:2022"),
    ],
)
def test_v30_single_sided_time_filter(kwargs, expected) -> None:
    assert V30.build_data_params(**kwargs)["c[TIME_PERIOD]"] == expected


@pytest.mark.parametrize(
    ("fmt", "value"),
    [
        (EurostatResponseFormat.CSV, "csvdata"),
        (EurostatResponseFormat.TSV, "tsv"),
        (EurostatResponseFormat.JSON, "json"),
        (EurostatResponseFormat.XML, "structurespecificdata"),
    ],
)
def test_v30_format_parameter_mapping(fmt, value) -> None:
    assert V30.build_data_params(response_format=fmt)["format"] == value


def test_v30_structure_endpoint_with_and_without_version() -> None:
    codelist = StructureResourceType.CODELIST
    assert V30.build_structure_endpoint(codelist, "CL", "ESTAT", "+") == (
        "/sdmx/3.0/structure/codelist/ESTAT/CL/+"
    )
    assert V30.build_structure_endpoint(codelist, "CL", "ESTAT", None) == (
        "/sdmx/3.0/structure/codelist/ESTAT/CL"
    )


def test_v30_structure_params_defaults_and_omission() -> None:
    assert V30.build_structure_params() == {
        "references": "none",
        "detail": "full",
        "format": "structure",
        "formatVersion": "3.0",
        "compress": "true",
    }
    assert (
        V30.build_structure_params(
            references=None,
            detail=None,
            format=None,
            format_version=None,
            compress=None,
        )
        == {}
    )


# ──────────────────────────────────────────────────────────────────────
# SDMX 2.1
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("agency", "version", "expected"),
    [
        (None, None, "/sdmx/2.1/data/EXR/all"),
        ("ECB", None, "/sdmx/2.1/data/ECB,EXR/all"),
        ("ECB", "1.0", "/sdmx/2.1/data/ECB,EXR,1.0/all"),
    ],
)
def test_v21_data_endpoint_compound_flow(agency, version, expected) -> None:
    assert V21.build_data_endpoint("EXR", agency, version) == expected


def test_v21_data_endpoint_with_key() -> None:
    assert V21.build_data_endpoint("EXR", "ECB", "1.0", key="A.FR+DE") == (
        "/sdmx/2.1/data/ECB,EXR,1.0/A.FR+DE"
    )


def test_v21_data_params_ignore_3_0_only_arguments() -> None:
    params = V21.build_data_params(
        start_period="2020",
        end_period="2022",
        last_n_observations=2,
        first_n_observations=1,
        compress=True,
        dimension_at_observation="AllDimensions",
        detail="dataonly",
        dimensions={"geo": ["FR"]},
        lang="en",
    )
    assert params == {
        "startPeriod": "2020",
        "endPeriod": "2022",
        "firstNObservations": "1",
        "lastNObservations": "2",
        "dimensionAtObservation": "AllDimensions",
        "detail": "dataonly",
        "compressed": "true",
    }
    assert V21.build_data_params() == {"compressed": "false"}


@pytest.mark.parametrize(
    ("resource_type", "resource_id", "agency", "version", "expected"),
    [
        (
            StructureResourceType.DATACONSTRAINT,
            "NAMA",
            "ESTAT",
            "1.0",
            "/sdmx/2.1/contentconstraint/ESTAT/NAMA/1.0",
        ),
        (
            StructureResourceType.DATAFLOW,
            "*",
            "*",
            "*",
            "/sdmx/2.1/dataflow/all/all/all",
        ),
        (
            StructureResourceType.CODELIST,
            "CL",
            "ESTAT",
            "+",
            "/sdmx/2.1/codelist/ESTAT/CL/latest",
        ),
        (
            StructureResourceType.DATASTRUCTURE,
            "DSD",
            "ESTAT",
            None,
            "/sdmx/2.1/datastructure/ESTAT/DSD/latest",
        ),
    ],
)
def test_v21_structure_endpoint_rewrites_wildcards(
    resource_type, resource_id, agency, version, expected
) -> None:
    assert (
        V21.build_structure_endpoint(resource_type, resource_id, agency, version)
        == expected
    )


def test_v21_structure_params() -> None:
    assert V21.build_structure_params() == {"detail": "full", "references": "none"}
    assert V21.build_structure_params(references=None, detail=None) == {}
