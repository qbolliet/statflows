"""Tests — :class:`statflows.sources.eurostat.client.EurostatClient`.

Frozen behaviour: API version validation, Comext routing (``DS-`` / ``CXT_``),
positional key and combination building for SDMX 3.0 vs 2.1, single-request
execution per response format (gzip handled), structure retrieval and its
fallbacks, codelist and catalogue helpers, and incremental fetch driven by the
dataconstraint update date. No network: the HTTP layer is replaced by mocks.
"""

from __future__ import annotations

import gzip
import json
from datetime import UTC, datetime
from unittest import mock

import pytest

from statflows.core.sdmx import SDMXVersion, StructureResourceType
from statflows.core.structures import DataflowStructure, DimensionInfo
from statflows.sources.eurostat.client import EurostatClient, _to_utc
from statflows.sources.eurostat.formats import EurostatResponseFormat
from statflows.sources.eurostat.queries import EurostatQueryRequestV30

CSV = "freq,geo,TIME_PERIOD,OBS_VALUE\nA,FR,2020,1\nA,DE,2020,2\n"

NS3 = (
    'xmlns:mes="http://www.sdmx.org/resources/sdmxml/schemas/v3_0/message" '
    'xmlns:str="http://www.sdmx.org/resources/sdmxml/schemas/v3_0/structure" '
    'xmlns:com="http://www.sdmx.org/resources/sdmxml/schemas/v3_0/common"'
)

DSD_XML = f"""<mes:Structure {NS3}>
  <str:DataStructure id="DSD">
    <str:DimensionList>
      <str:Dimension id="freq" position="1"/>
      <str:Dimension id="geo" position="2"/>
    </str:DimensionList>
  </str:DataStructure>
</mes:Structure>"""

CONSTRAINT_XML = f"""<mes:Structure {NS3}><com:Annotation>
  <com:AnnotationType>UPDATE_DATA</com:AnnotationType>
  <com:AnnotationTitle>2026-03-01T00:00:00Z</com:AnnotationTitle>
</com:Annotation></mes:Structure>"""


def _response(content: bytes | str) -> mock.Mock:
    raw = content.encode() if isinstance(content, str) else content
    return mock.Mock(content=raw)


def _structure() -> DataflowStructure:
    return DataflowStructure(
        "ESTAT",
        "nama",
        3,
        [DimensionInfo("freq", 0), DimensionInfo("geo", 1), DimensionInfo("unit", 2)],
    )


@pytest.fixture
def client() -> EurostatClient:
    eurostat = EurostatClient(auto_load_rate_limit=False)
    eurostat.api_client = mock.Mock()
    eurostat.api_client.get.return_value = _response(CSV)
    return eurostat


@pytest.fixture
def structured(client) -> EurostatClient:
    client.register_structure(_structure())
    return client


# ──────────────────────────────────────────────────────────────────────
# Construction et routage
# ──────────────────────────────────────────────────────────────────────


def test_unsupported_api_version_is_rejected() -> None:
    with pytest.raises(ValueError, match="Unsupported Eurostat API version"):
        EurostatClient(api_version=SDMXVersion.V1)


def test_v21_client_selects_the_v21_builder() -> None:
    eurostat = EurostatClient(api_version=SDMXVersion.V2_1, auto_load_rate_limit=False)
    assert "2.1" in eurostat.endpoint_builder.ACCEPT_HEADER


@pytest.mark.parametrize(
    ("resource_id", "expected"),
    [
        ("DS-045409", True),
        ("ds-045409", True),
        ("CXT_FREE_ISO", True),
        ("namq_10_gdp", False),
    ],
)
def test_is_comext_dataset(resource_id, expected) -> None:
    assert EurostatClient._is_comext_dataset(resource_id) is expected


def test_comext_resources_use_a_lazily_created_dedicated_client(client) -> None:
    assert client._comext_client is None
    assert client._get_api_client("namq_10_gdp") is client.api_client

    comext = client._get_api_client("DS-045409")
    assert comext is not client.api_client
    assert "comext" in comext.base_url
    assert client._get_api_client("CXT_NC") is comext  # réutilisé


def test_close_closes_both_http_clients(client) -> None:
    comext = mock.Mock()
    client._comext_client = comext
    client.close()
    client.api_client.close.assert_called_once()
    comext.close.assert_called_once()


def test_to_utc_handles_naive_and_aware_values() -> None:
    naive = datetime(2026, 1, 1, 12)
    assert _to_utc(naive) == datetime(2026, 1, 1, 12, tzinfo=UTC)
    aware = datetime(2026, 1, 1, 14, tzinfo=datetime.now().astimezone().tzinfo)
    assert _to_utc(aware).tzinfo == UTC


# ──────────────────────────────────────────────────────────────────────
# Dimensions, clé positionnelle et combinaisons
# ──────────────────────────────────────────────────────────────────────


def test_normalize_dimensions(caplog) -> None:
    normalize = EurostatClient._normalize_dimensions
    assert normalize(None) is None
    assert normalize({}) is None
    assert normalize({"geo": "FR", "unit": ["A", "B"]}) == {
        "geo": ["FR"],
        "unit": ["A", "B"],
    }
    with caplog.at_level("WARNING"):
        assert normalize({"nope": "x"}, _structure()) == {"nope": ["x"]}
    assert "Dimension 'nope' not found" in caplog.text


@pytest.mark.parametrize(
    ("version", "wildcard"), [(SDMXVersion.V3, "*"), (SDMXVersion.V2_1, "all")]
)
def test_build_key_string_orders_by_position_with_version_wildcard(
    version, wildcard
) -> None:
    eurostat = EurostatClient(api_version=version, auto_load_rate_limit=False)
    key = eurostat._build_key_string({"GEO": ["FR", "DE"], "freq": ["A"]}, _structure())
    assert key == f"A.FR+DE.{wildcard}"


def test_generate_combinations_v30_splits_single_values_from_params(client) -> None:
    combos = client._generate_request_combinations(
        {"geo": ["FR"], "unit": ["A", "B"]},
        split_dims=None,
        max_combinations=10,
        is_v21=False,
    )
    assert combos == [({"geo": ["FR"]}, {"unit": ["A", "B"]})]


def test_generate_combinations_v21_puts_everything_in_the_key(client) -> None:
    combos = client._generate_request_combinations(
        {"geo": ["FR"], "unit": ["A", "B"]},
        split_dims=None,
        max_combinations=10,
        is_v21=True,
    )
    assert combos == [({"geo": ["FR"], "unit": ["A", "B"]}, {})]


def test_generate_combinations_split_and_empty_cases(client) -> None:
    generate = client._generate_request_combinations
    assert generate(None, None, 10, False) == [({}, {})]

    combos = generate({"geo": ["FR", "DE"], "unit": ["A", "B"]}, ["geo"], 10, False)
    assert combos == [
        ({"geo": ["FR"]}, {"unit": ["A", "B"]}),
        ({"geo": ["DE"]}, {"unit": ["A", "B"]}),
    ]
    with pytest.raises(ValueError, match="max_split_combinations=1"):
        generate({"geo": ["FR", "DE"]}, ["geo"], 1, False)


# ──────────────────────────────────────────────────────────────────────
# Requête unitaire
# ──────────────────────────────────────────────────────────────────────


def test_get_data_v30_builds_key_params_and_parses_csv(structured) -> None:
    df = structured.get_data(
        "nama",
        dimensions={"geo": "FR", "unit": ["A", "B"]},
        start_period="2020",
        on_duplicate="ignore",
    )

    assert len(df) == 2
    call = structured.api_client.get.call_args
    assert call.args[0] == "/sdmx/3.0/data/dataflow/ESTAT/nama/*/*.FR.*"
    assert call.kwargs["params"]["c[UNIT]"] == "A,B"
    assert call.kwargs["params"]["c[TIME_PERIOD]"] == "ge:2020"
    assert call.kwargs["params"]["format"] == "csvdata"


def test_get_data_without_structure_filters_client_side(client) -> None:
    client.auto_fetch_structure = False
    df = client.get_data("nama", dimensions={"geo": "FR"}, on_duplicate="ignore")
    # Sans structure : filtre de repli côté client sur les dimensions demandées
    assert df["geo"].tolist() == ["FR"]


def test_missing_structure_is_fetched_through_the_datastructure_endpoint(
    client,
) -> None:
    client.api_client.get.side_effect = [
        _response(gzip.compress(DSD_XML.encode())),
        _response(CSV),
    ]
    client.get_data(
        "nama", version="*", dimensions={"geo": "FR"}, on_duplicate="ignore"
    )

    structure_call = client.api_client.get.call_args_list[0]
    # Le joker de version "*" est remplacé par "+" sur l'endpoint de structure
    assert structure_call.args[0] == "/sdmx/3.0/structure/datastructure/ESTAT/nama/+"
    assert structure_call.kwargs["params"]["references"] == "descendants"
    assert client.structure_registry.has("ESTAT", "nama")


def test_resolve_structure_falls_back_to_a_direct_fetch_when_auto_fetch_is_off(
    client,
) -> None:
    client.auto_fetch_structure = False
    client.api_client.get.return_value = _response(DSD_XML)
    structure = client._resolve_structure({"dataflow": "nama"})
    assert structure is not None
    assert client.structure_registry.has("ESTAT", "nama")


def test_resolve_structure_failure_is_not_fatal(client) -> None:
    client.api_client.get.side_effect = RuntimeError("down")
    assert client._resolve_structure({"dataflow": "nama"}) is None


def test_ensure_structure_raises_when_fetch_is_disabled(client) -> None:
    client.auto_fetch_structure = False
    with pytest.raises(ValueError, match="Structure not found for nama::\\*"):
        client._ensure_structure("nama")


def test_v21_without_dimensions_uses_the_all_key(structured) -> None:
    eurostat = EurostatClient(api_version=SDMXVersion.V2_1, auto_load_rate_limit=False)
    eurostat.api_client = mock.Mock()
    eurostat.api_client.get.return_value = _response(CSV)
    eurostat.auto_fetch_structure = False

    eurostat._execute_single_request({}, dataflow="nama", version="1.0")

    assert (
        eurostat.api_client.get.call_args.args[0] == "/sdmx/2.1/data/ESTAT,nama,1.0/all"
    )


def test_comext_dataflow_requests_go_to_the_comext_client(client) -> None:
    comext = mock.Mock()
    comext.get.return_value = _response(CSV)
    client._comext_client = comext
    client._execute_single_request({}, dataflow="DS-045409")
    comext.get.assert_called_once()
    client.api_client.get.assert_not_called()


def test_execute_single_request_parses_tsv_json_and_gzip(client) -> None:
    tsv = "freq,geo\\TIME_PERIOD\t2020\nA,FR\t1\n"
    client.api_client.get.return_value = _response(gzip.compress(tsv.encode()))
    df = client._execute_single_request(
        {}, dataflow="nama", response_format=EurostatResponseFormat.TSV
    )
    assert df["value"].tolist() == [1.0]

    payload = {
        "id": ["geo"],
        "size": [1],
        "dimension": {"geo": {"category": {"index": {"FR": 0}}}},
        "value": {"0": 4.0},
    }
    client.api_client.get.return_value = _response(json.dumps(payload))
    df = client._execute_single_request(
        {}, dataflow="nama", response_format=EurostatResponseFormat.JSON
    )
    assert df.to_dict("records") == [{"geo": "FR", "value": 4.0}]


def test_execute_single_request_unsupported_format(client) -> None:
    with pytest.raises(ValueError, match="Unsupported format"):
        client._execute_single_request(
            {}, dataflow="nama", response_format=EurostatResponseFormat.XML
        )


# ──────────────────────────────────────────────────────────────────────
# Structures et catalogue
# ──────────────────────────────────────────────────────────────────────


def test_get_structure_bulk_harvest_forces_the_wildcard_version(client) -> None:
    client.api_client.get.return_value = _response("<x/>")
    client.get_structure(StructureResourceType.CODELIST, "*", version="+")
    assert client.api_client.get.call_args.args[0] == (
        "/sdmx/3.0/structure/codelist/ESTAT/*/*"
    )


def test_get_structure_routes_comext_codelists_and_decodes_gzip(client) -> None:
    comext = mock.Mock()
    comext.get.return_value = _response(gzip.compress("<é/>".encode()))
    client._comext_client = comext
    assert client.get_structure(StructureResourceType.CODELIST, "CXT_NC") == "<é/>"


def test_get_structure_wraps_http_errors(client) -> None:
    client.api_client.get.side_effect = RuntimeError("boom")
    with pytest.raises(ValueError, match="Failed to fetch dataflow 'nama': boom"):
        client.get_structure(StructureResourceType.DATAFLOW, "nama")


def test_get_structure_forwards_timeout_and_headers(client) -> None:
    client.api_client.get.return_value = _response("<x/>")
    client.get_structure(
        StructureResourceType.DATAFLOW,
        "nama",
        accept_encoding="gzip",
        accept_language="en",
        timeout=7,
    )
    call = client.api_client.get.call_args
    assert call.kwargs["timeout"] == 7
    assert call.kwargs["headers"]["Accept-Encoding"] == "gzip"
    assert call.kwargs["headers"]["Accept-Language"] == "en"


def test_list_all_dataflows_uses_the_bulk_endpoint_per_version(client) -> None:
    xml = (
        f'<mes:Structure {NS3}><str:Dataflow id="A" agencyID="ESTAT" version="1.0">'
        "<com:Name>Alpha</com:Name></str:Dataflow></mes:Structure>"
    )
    client.api_client.get.return_value = _response(xml)

    df = client.list_all_dataflows()

    assert df["id"].tolist() == ["A"]
    call = client.api_client.get.call_args
    assert call.args[0] == "/sdmx/3.0/structure/dataflow/ESTAT/*/*"
    assert call.kwargs["params"]["detail"] == "allstubs"

    v21 = EurostatClient(api_version=SDMXVersion.V2_1, auto_load_rate_limit=False)
    v21.api_client = mock.Mock()
    v21.api_client.get.return_value = _response("<x/>")
    v21.list_all_dataflows(agency="*")
    assert v21.api_client.get.call_args.args[0] == "/sdmx/2.1/dataflow/all/all/latest"


# ──────────────────────────────────────────────────────────────────────
# Téléchargement incrémental
# ──────────────────────────────────────────────────────────────────────

LAST_UPDATE = datetime(2026, 3, 1, tzinfo=UTC)


def test_data_last_update_reads_the_dataconstraint(client) -> None:
    client.api_client.get.return_value = _response(CONSTRAINT_XML)
    assert client.get_data_last_update("nama") == LAST_UPDATE
    assert client.api_client.get.call_args.args[0] == (
        "/sdmx/3.0/structure/dataconstraint/ESTAT/nama/+"
    )


def test_data_last_update_failure_is_not_fatal(client) -> None:
    client.api_client.get.side_effect = RuntimeError("down")
    assert client.get_data_last_update("nama") is None


def test_fetch_updates_first_download_is_full(structured) -> None:
    query = EurostatQueryRequestV30("nama", on_duplicate="ignore")
    structured.fetch_updates(query, None)
    assert (
        "lastNObservations" not in structured.api_client.get.call_args.kwargs["params"]
    )


def test_fetch_updates_skips_the_fetch_when_nothing_changed(structured) -> None:
    structured.get_data_last_update = mock.Mock(return_value=LAST_UPDATE)  # type: ignore[method-assign]
    query = EurostatQueryRequestV30("nama", on_duplicate="ignore")

    # Date de téléchargement naïve : comparée en UTC
    result = structured.fetch_updates(query, datetime(2026, 3, 2))

    assert result.empty
    structured.api_client.get.assert_not_called()


@pytest.mark.parametrize("last_update", [datetime(2026, 3, 1, tzinfo=UTC), None])
def test_fetch_updates_pulls_the_last_n_observations(structured, last_update) -> None:
    structured.get_data_last_update = mock.Mock(return_value=last_update)  # type: ignore[method-assign]
    query = EurostatQueryRequestV30("nama", on_duplicate="ignore")

    structured.fetch_updates(query, datetime(2026, 2, 1, tzinfo=UTC), n_observations=3)

    assert (
        structured.api_client.get.call_args.kwargs["params"]["lastNObservations"] == "3"
    )
