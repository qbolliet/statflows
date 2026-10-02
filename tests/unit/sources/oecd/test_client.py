"""Tests — :class:`statflows.sources.oecd.client.OECDClient`.

Frozen behaviour: dimension / split normalisation, request combinations
(wildcard + post-filter for non-split multi-values), URL / parameter / header
construction through the versioned builder, response parsing per format,
structure retrieval, incremental fetch and dataflow listing. No network: the
HTTP layer (``api_client``) is replaced by a mock.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest import mock

import pytest

from statflows.core.sdmx import DimensionAtObservation, SDMXVersion
from statflows.core.structures import DataflowStructure, DimensionInfo
from statflows.sources.oecd.client import OECDClient
from statflows.sources.oecd.formats import OECDResponseFormat
from statflows.sources.oecd.queries import OECDQueryRequest

AGENCY = "OECD.SDD.STES"
DATAFLOW = "DSD_KEI@DF_KEI"

CSV = "REF_AREA,MEASURE,TIME_PERIOD,OBS_VALUE\nFRA,M1,2020,1\nDEU,M1,2020,2\n"


def _structure() -> DataflowStructure:
    return DataflowStructure(
        AGENCY,
        DATAFLOW,
        3,
        [
            DimensionInfo("REF_AREA", 0),
            DimensionInfo("MEASURE", 1),
            DimensionInfo("FREQ", 2),
        ],
    )


@pytest.fixture
def client() -> OECDClient:
    oecd = OECDClient(auto_load_rate_limit=False)
    oecd.api_client = mock.Mock()
    oecd.api_client.get.return_value = mock.Mock(text=CSV)
    return oecd


@pytest.fixture
def structured_client(client) -> OECDClient:
    client.register_structure(_structure())
    return client


# ──────────────────────────────────────────────────────────────────────
# Construction
# ──────────────────────────────────────────────────────────────────────


def test_default_client_uses_v2_and_loads_its_rate_limiter() -> None:
    oecd = OECDClient()
    assert oecd.sdmx_version is SDMXVersion.V2
    assert oecd.rate_limiter is not None
    oecd.close()


def test_v1_client_selects_v1_builder() -> None:
    oecd = OECDClient(sdmx_version=SDMXVersion.V1, auto_load_rate_limit=False)
    assert oecd.endpoint_builder.sdmx_version is SDMXVersion.V1


def test_close_closes_the_http_client(client) -> None:
    client.close()
    client.api_client.close.assert_called_once()


# ──────────────────────────────────────────────────────────────────────
# Normalisation des dimensions
# ──────────────────────────────────────────────────────────────────────


def test_normalize_dimensions(structured_client) -> None:
    normalize = structured_client._normalize_dimensions
    assert normalize(None, AGENCY, DATAFLOW) == {}
    assert normalize({0: "FRA", 1: ["M1", "M2"]}, AGENCY, DATAFLOW) == {
        0: ["FRA"],
        1: ["M1", "M2"],
    }
    # Clés nominales : résolues via le registre de structures
    assert normalize({"MEASURE": "M1"}, AGENCY, DATAFLOW) == {1: ["M1"]}


def test_normalize_split_dimensions(structured_client) -> None:
    normalize = structured_client._normalize_split_dimensions
    structure = _structure()
    dims = {0: ["FRA", "DEU"], 1: ["M1", "M2"]}

    assert normalize(None, structure, dims) == []
    assert normalize([0, "MEASURE", "REF_AREA"], structure, dims) == [
        "REF_AREA",
        "MEASURE",
    ]


@pytest.mark.parametrize(
    ("split", "message"),
    [
        ([9], "Position 9 not found"),
        ([2], "not in the dimensions filter"),
        (["NOPE"], "Dimension name 'NOPE' not found.*Available dimensions"),
        (["FREQ"], "not in the dimensions filter"),
        ([1.5], "Invalid type for split_dimensions"),
    ],
)
def test_normalize_split_dimensions_errors(structured_client, split, message) -> None:
    with pytest.raises(ValueError, match=message):
        structured_client._normalize_split_dimensions(
            split, _structure(), {0: ["FRA", "DEU"]}
        )


def test_split_dimensions_require_a_structure(client) -> None:
    with pytest.raises(ValueError, match="Structure required"):
        client._normalize_split_dimensions(["REF_AREA"], None, {0: ["FRA", "DEU"]})


# ──────────────────────────────────────────────────────────────────────
# Combinaisons de requêtes
# ──────────────────────────────────────────────────────────────────────


def test_combinations_split_and_postfilter(client) -> None:
    dims = {0: ["FRA", "DEU"], 1: ["M1", "M2"], 2: ["A"]}
    combos = client._generate_request_combinations(dims, ["REF_AREA"], _structure())

    # Split sur REF_AREA ; MEASURE (multi-valeurs non splitté) → joker + post-filtre
    assert combos == [
        ({0: ["FRA"], 1: ["*"], 2: ["A"]}, {"MEASURE": ["M1", "M2"]}),
        ({0: ["DEU"], 1: ["*"], 2: ["A"]}, {"MEASURE": ["M1", "M2"]}),
    ]


def test_combinations_without_structure_cannot_postfilter(client) -> None:
    combos = client._generate_request_combinations({0: ["FRA", "DEU"]}, [], None)
    assert combos == [({0: ["*"]}, {})]


def test_combinations_respect_max_split_combinations(client) -> None:
    with pytest.raises(ValueError, match="exceeding max_split_combinations=1"):
        client._generate_request_combinations(
            {0: ["FRA", "DEU"]}, ["REF_AREA"], _structure(), max_combinations=1
        )


# ──────────────────────────────────────────────────────────────────────
# Requête de données
# ──────────────────────────────────────────────────────────────────────


def test_get_data_requires_a_dataflow(client) -> None:
    with pytest.raises(ValueError, match="dataflow is required"):
        client.get_data(AGENCY, None)  # type: ignore[arg-type]


def test_get_data_builds_url_params_headers_and_parses_csv(structured_client) -> None:
    df = structured_client.get_data(
        AGENCY,
        DATAFLOW,
        dimensions={"REF_AREA": "FRA"},
        start_period="2020",
        end_period="2021",
        last_n_observations=1,
    )

    assert df.to_dict("records") == [
        {"REF_AREA": "FRA", "MEASURE": "M1", "TIME_PERIOD": 2020, "OBS_VALUE": 1},
        {"REF_AREA": "DEU", "MEASURE": "M1", "TIME_PERIOD": 2020, "OBS_VALUE": 2},
    ]
    call = structured_client.api_client.get.call_args
    assert call.args[0] == f"v2/data/dataflow/{AGENCY}/{DATAFLOW}/+/FRA.*.*"
    assert call.kwargs["params"] == {
        "dimensionAtObservation": "AllDimensions",
        "format": "csvfilewithlabels",
        "c[TIME_PERIOD]": "ge:2020+le:2021",
        "lastNObservations": 1,
    }
    assert call.kwargs["headers"]["Accept"].startswith("application/vnd.sdmx.data+csv")


def test_get_data_json_format_is_parsed_as_sdmx_json(structured_client) -> None:
    payload = {
        "structure": {
            "dimensions": {
                "observation": [{"id": "REF_AREA", "values": [{"id": "FRA"}]}]
            }
        },
        "dataSets": [{"observations": {"0": [1.5]}}],
    }
    structured_client.api_client.get.return_value = mock.Mock(
        json=mock.Mock(return_value=payload)
    )
    df = structured_client.get_data(
        AGENCY, DATAFLOW, format=OECDResponseFormat.JSON, on_duplicate="ignore"
    )
    assert df.to_dict("records") == [{"REF_AREA": "FRA", "value": 1.5}]


def test_get_data_unimplemented_format_surfaces_as_failed_request(
    structured_client,
) -> None:
    with pytest.raises(ValueError, match="Format .* not yet implemented"):
        structured_client.get_data(AGENCY, DATAFLOW, format=OECDResponseFormat.XML)


def test_get_data_postfilters_wildcarded_dimensions(structured_client) -> None:
    structured_client.api_client.get.return_value = mock.Mock(
        text="REF_AREA,MEASURE,OBS_VALUE\nFRA,M1,1\nFRA,M3,2\nDEU,M2,3\n"
    )
    df = structured_client.get_data(
        AGENCY,
        DATAFLOW,
        dimensions={"REF_AREA": "FRA", "MEASURE": ["M1", "M2"]},
        default_dimensions=[],
    )
    # M3 (hors filtre) est écarté après coup ; la dimension à valeur unique n'est pas filtrée
    assert df["MEASURE"].tolist() == ["M1", "M2"]
    # Une seule requête, avec joker sur la dimension multi-valeurs
    assert structured_client.api_client.get.call_count == 1
    assert structured_client.api_client.get.call_args.args[0].endswith("/FRA.*.*")


def test_get_data_splits_into_one_request_per_value(structured_client) -> None:
    structured_client.api_client.get.side_effect = [
        mock.Mock(text="REF_AREA,OBS_VALUE\nFRA,1\n"),
        mock.Mock(text="REF_AREA,OBS_VALUE\nDEU,2\n"),
    ]
    df = structured_client.get_data(
        AGENCY,
        DATAFLOW,
        dimensions={"REF_AREA": ["FRA", "DEU"]},
        split_dimensions=["REF_AREA"],
        default_dimensions=[],
    )
    assert df["REF_AREA"].tolist() == ["FRA", "DEU"]
    urls = [c.args[0] for c in structured_client.api_client.get.call_args_list]
    assert [u.rsplit("/", 1)[1] for u in urls] == ["FRA.*.*", "DEU.*.*"]


def test_get_data_acquires_the_rate_limiter(structured_client) -> None:
    structured_client.rate_limiter = mock.Mock()
    structured_client.get_data(AGENCY, DATAFLOW)
    structured_client.rate_limiter.acquire.assert_called_once()


def test_get_data_accepts_enum_or_string_dimension_at_observation(
    structured_client,
) -> None:
    structured_client._execute_single_request(
        {},
        agency=AGENCY,
        dataflow=DATAFLOW,
        dimension_at_observation=DimensionAtObservation.TIME_PERIOD,
    )
    structured_client._execute_single_request(
        {},
        agency=AGENCY,
        dataflow=DATAFLOW,
        dimension_at_observation="TIME_PERIOD",
    )
    sent = [
        c.kwargs["params"]["dimensionAtObservation"]
        for c in structured_client.api_client.get.call_args_list
    ]
    assert sent == ["TIME_PERIOD", "TIME_PERIOD"]


# ──────────────────────────────────────────────────────────────────────
# Structure
# ──────────────────────────────────────────────────────────────────────

STRUCTURE_JSON = {
    "structure": {
        "dimensions": {
            "observation": [
                {"id": "REF_AREA", "position": 0, "name": "Area"},
                {"id": "MEASURE", "position": 1, "name": "Measure"},
            ]
        }
    }
}


def test_get_structure_requests_the_structure_endpoint(client) -> None:
    client.api_client.get.return_value = mock.Mock(
        json=mock.Mock(return_value=STRUCTURE_JSON)
    )
    structure = client.get_structure(AGENCY, DATAFLOW, timeout=5)

    assert [d.name for d in structure.dimensions] == ["REF_AREA", "MEASURE"]
    call = client.api_client.get.call_args
    assert call.args[0] == f"v2/structure/dataflow/{AGENCY}/{DATAFLOW}/+"
    assert call.kwargs["params"] == {"references": "all", "detail": "referencepartial"}
    assert call.kwargs["headers"]["Accept"].endswith("version=1.0")
    assert call.kwargs["timeout"] == 5


def test_get_structure_requires_a_dataflow(client) -> None:
    with pytest.raises(ValueError, match="dataflow is required"):
        client.get_structure(AGENCY, None)  # type: ignore[arg-type]


def test_missing_structure_is_fetched_once_and_cached(client) -> None:
    client.api_client.get.side_effect = [
        mock.Mock(json=mock.Mock(return_value=STRUCTURE_JSON)),
        mock.Mock(text="REF_AREA,TIME_PERIOD,OBS_VALUE\nFRA,2020,1\n"),
        mock.Mock(text="REF_AREA,TIME_PERIOD,OBS_VALUE\nFRA,2020,1\n"),
    ]
    client.get_data(AGENCY, DATAFLOW, dimensions={"REF_AREA": "FRA"})
    client.get_data(AGENCY, DATAFLOW, dimensions={"REF_AREA": "FRA"})
    assert client.n_structures_fetched_ == 1
    assert client.structure_registry.has(AGENCY, DATAFLOW)


# ──────────────────────────────────────────────────────────────────────
# Téléchargement incrémental
# ──────────────────────────────────────────────────────────────────────


def test_fetch_updates_first_download_is_full_and_next_is_incremental(
    structured_client,
) -> None:
    query = OECDQueryRequest(AGENCY, DATAFLOW)
    since = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)

    structured_client.fetch_updates(query, None)
    assert (
        "updatedAfter"
        not in structured_client.api_client.get.call_args.kwargs["params"]
    )

    structured_client.fetch_updates(query, since)
    params = structured_client.api_client.get.call_args.kwargs["params"]
    assert params["updatedAfter"] == "2026-01-02T03:04:05+00:00"


# ──────────────────────────────────────────────────────────────────────
# Catalogue
# ──────────────────────────────────────────────────────────────────────

DATAFLOWS_XML = b"""<?xml version="1.0"?>
<mes:Structure
  xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/message"
  xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/structure"
  xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v2_1/common">
  <mes:Structures><str:Dataflows>
    <str:Dataflow id="DF_A" agencyID="OECD.SDD" version="1.0"><com:Name>Alpha</com:Name></str:Dataflow>
    <str:Dataflow id="DF_B" agencyID="OECD.ENV" version="2.0"/>
  </str:Dataflows></mes:Structures>
</mes:Structure>"""


def test_list_all_dataflows(client) -> None:
    client.rate_limiter = mock.Mock()
    client.api_client.get.return_value = mock.Mock(content=DATAFLOWS_XML)

    df = client.list_all_dataflows()

    client.rate_limiter.acquire.assert_called_once()
    assert client.api_client.get.call_args.args[0] == "dataflow/all"
    assert df.astype(object).where(df.notna(), None).to_dict("records") == [
        {"dataflow": "DF_A", "agency": "OECD.SDD", "version": "1.0", "name": "Alpha"},
        {"dataflow": "DF_B", "agency": "OECD.ENV", "version": "2.0", "name": None},
    ]
