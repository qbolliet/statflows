"""Tests — :class:`statflows.sources.comtrade.client.ComtradeClient`.

Frozen behaviour: authentication (key in a header, public preview otherwise),
JSON retrieval with HTTP status carried by :class:`ComtradeAPIError`, one call per
period, recursive subdivision when the per-call record limit is hit, reference
metadata caching, availability-driven incremental fetch and primary-key
aggregation. No network: the inherited HTTP ``get`` is replaced by a mock.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest import mock

import pandas as pd
import pytest
import requests

from statflows.core.rate_limiter import CompositeRateLimiter
from statflows.sources.comtrade import client as client_module
from statflows.sources.comtrade.client import ComtradeAPIError, ComtradeClient
from statflows.sources.comtrade.queries import ComtradeQueryRequest


def _json_response(payload) -> mock.Mock:
    return mock.Mock(content=json.dumps(payload).encode("utf-8-sig"))


def _http_error(status: int, reason: str = "Err", text: str = "") -> Exception:
    response = mock.Mock(status_code=status, reason=reason, text=text)
    return requests.exceptions.HTTPError(str(status), response=response)


@pytest.fixture
def client() -> ComtradeClient:
    comtrade = ComtradeClient(subscription_key=None, auto_load_rate_limit=False)
    comtrade.get = mock.Mock()  # type: ignore[method-assign]
    return comtrade


@pytest.fixture
def keyed(client) -> ComtradeClient:
    client.subscription_key = "secret"
    return client


def _rows(n: int, **extra) -> dict:
    return {"data": [{"id": i, **extra} for i in range(n)]}


# ──────────────────────────────────────────────────────────────────────
# Construction et configuration
# ──────────────────────────────────────────────────────────────────────


def test_subscription_key_comes_from_argument_or_environment(monkeypatch) -> None:
    monkeypatch.setenv("COMTRADE_SUBSCRIPTION_KEY", "from-env")
    assert ComtradeClient(auto_load_rate_limit=False).subscription_key == "from-env"
    assert (
        ComtradeClient(
            subscription_key="arg", auto_load_rate_limit=False
        ).subscription_key
        == "arg"
    )


def test_proxy_is_applied_to_the_session() -> None:
    comtrade = ComtradeClient(
        subscription_key=None, proxy="host:8080", auto_load_rate_limit=False
    )
    assert comtrade.session.proxies["https"] == "http://host:8080"
    assert ComtradeClient(auto_load_rate_limit=False)._proxy_url is None


def test_rate_limiter_is_composite_by_default_and_overridable() -> None:
    assert isinstance(ComtradeClient().rate_limiter, CompositeRateLimiter)
    explicit = mock.Mock()
    assert ComtradeClient(rate_limiter=explicit).rate_limiter is explicit


def test_load_rate_limiter_without_section_or_with_bad_section(monkeypatch) -> None:
    comtrade = ComtradeClient(auto_load_rate_limit=False)
    monkeypatch.setitem(client_module.PARAMETERS, "RATE_LIMIT", [{"requests": 0}])
    assert comtrade._load_rate_limiter() is None
    monkeypatch.delitem(client_module.PARAMETERS, "RATE_LIMIT")
    assert comtrade._load_rate_limiter() is None


def test_declared_structures_are_preloaded(client) -> None:
    assert client.structure_registry.has("COMTRADE", "C_A_HS")


def test_close_closes_the_session(client) -> None:
    client.session = mock.Mock()
    client.close()
    client.session.close.assert_called_once()


# ──────────────────────────────────────────────────────────────────────
# Authentification et appels JSON
# ──────────────────────────────────────────────────────────────────────


def test_auth_endpoint_and_headers(client, keyed) -> None:
    assert keyed._auth("v1/x") == (
        "/data/v1/x",
        {"Ocp-Apim-Subscription-Key": "secret"},
    )
    keyed.subscription_key = None
    assert keyed._auth("v1/x") == ("/public/v1/x", None)


def test_get_json_drops_none_params_and_decodes_bom(client) -> None:
    client.get.return_value = _json_response({"ok": 1})
    assert client._get_json("/e", params={"a": 1, "b": None}) == {"ok": 1}
    assert client.get.call_args.kwargs["params"] == {"a": 1}


def test_get_json_converts_http_errors_with_status_and_body(client) -> None:
    client.get.side_effect = _http_error(500, "Server Error", " boom ")
    with pytest.raises(
        ComtradeAPIError, match="ctx failed: HTTP 500 Server Error - boom"
    ) as exc:
        client._get_json("/e", context="ctx")
    assert exc.value.status_code == 500


def test_get_json_flags_empty_error_bodies(client) -> None:
    client.get.side_effect = _http_error(500, "Server Error", "")
    with pytest.raises(ComtradeAPIError, match="empty response body"):
        client._get_json("/e")


def test_get_json_converts_network_errors_without_status(client) -> None:
    client.get.side_effect = requests.exceptions.ConnectionError("down")
    with pytest.raises(ComtradeAPIError, match="failed: down") as exc:
        client._get_json("/e")
    assert exc.value.status_code is None


def test_acquire_uses_the_rate_limiter_when_configured(client) -> None:
    client._acquire()  # sans limiteur : sans effet
    client.rate_limiter = mock.Mock()
    client._acquire()
    client.rate_limiter.acquire.assert_called_once()


# ──────────────────────────────────────────────────────────────────────
# Prétraitement
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("codes", "expected"),
    [([1, "02", 3], "1,02,3"), (5, "5"), ("FRA", "FRA"), (None, None)],
)
def test_preprocess_codes(client, codes, expected) -> None:
    assert client._preprocess_codes(codes) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, True), ("A,B", True), ("A", False), (5, False)],
)
def test_validate_subdivision(client, value, expected) -> None:
    assert client._validate_subdivision(value) is expected


@pytest.mark.parametrize(
    ("name", "value", "expected"),
    [
        ("flows", "M,X", True),
        ("flows", "M", False),
        ("reporters", None, True),  # énumérable via les métadonnées
        ("periods", None, False),  # non énumérable
        ("mot", None, False),
    ],
)
def test_can_subdivide(client, name, value, expected) -> None:
    assert client._can_subdivide(name, value) is expected


def test_build_periods_from_boundaries(client) -> None:
    build = client._build_periods_from_boundaries
    assert build("2020-01-01", "annual", "2022-01-01") == ["2020", "2021", "2022"]
    assert build("2020-01-01", "monthly", "2020-03-01") == [
        "202001",
        "202002",
        "202003",
    ]
    assert build(None, "annual", "2022-01-01") == []
    open_ended = build("2020-01-01", "annual")
    assert open_ended[0] == "2020"
    assert open_ended[-1] == str(datetime.today().year)


def test_validate_date(client) -> None:
    assert client._validate_date("2023") == "2023-01-01"
    assert client._validate_date(2023) == "2023-01-01"
    assert client._validate_date("2023/06") == "2023-06-01"
    assert client._validate_date("2023-06-15") == "2023-06-15"
    with pytest.raises(ValueError, match="at least 4 digits"):
        client._validate_date("23")
    with pytest.raises(ValueError, match="Period must be in the format"):
        client._validate_date("2023-6")


def test_get_valid_periods(client) -> None:
    # La période finale (en cours) est toujours écartée
    assert client.get_valid_periods(
        period_start="2020", period_end="2023", frequency="annual"
    ) == ["2020", "2021", "2022"]
    assert client.get_valid_periods(
        periods=["2021", "2099"],
        period_start="2020",
        period_end="2023",
        frequency="annual",
    ) == ["2021"]
    with pytest.raises(ValueError, match="Invalid value for frequency"):
        client.get_valid_periods(frequency="weekly")


# ──────────────────────────────────────────────────────────────────────
# Appel tariffline unitaire
# ──────────────────────────────────────────────────────────────────────


def test_request_tariffline_public_preview(client) -> None:
    client.get.return_value = _json_response(_rows(2, v=1))
    df = client._request_tariffline(
        "C", "A", "HS", reporterCode="251", period="2022", flowCode=None
    )

    assert len(df) == 2
    call = client.get.call_args
    assert call.args[0] == "/public/v1/previewTariffline/C/A/HS"
    assert call.kwargs["headers"] is None
    assert call.kwargs["params"] == {
        "reportercode": "251",
        "period": "2022",
        "format": "JSON",
    }


def test_request_tariffline_with_key_uses_a_header_not_the_url(keyed) -> None:
    keyed.get.return_value = _json_response(_rows(1))
    keyed._request_tariffline("C", "M", "HS", countOnly=None)
    call = keyed.get.call_args
    assert call.args[0] == "/data/v1/getTariffline/C/M/HS"
    assert call.kwargs["headers"] == {"Ocp-Apim-Subscription-Key": "secret"}
    assert "secret" not in str(call.args) + str(call.kwargs["params"])


def test_request_tariffline_count_only_and_format_validation(client) -> None:
    client.get.return_value = _json_response({"count": 42})
    assert client._request_tariffline("C", "A", "HS", countOnly=True).to_dict(
        "records"
    ) == [{"count": 42}]
    with pytest.raises(ValueError, match="Only JSON output is supported"):
        client._request_tariffline("C", "A", "HS", format_output="CSV")


def test_fetch_tariffline_calls_once_per_period_and_counts_calls(client) -> None:
    client.get.side_effect = [_json_response(_rows(1)), _json_response(_rows(2))]
    client.rate_limiter = mock.Mock()

    df, truncated = client._fetch_tariffline(
        {"period": "2021,2022"}, typeCode="C", freqCode="A", clCode="HS"
    )

    assert len(df) == 3
    assert truncated is False
    assert client.api_calls == 2
    assert client.rate_limiter.acquire.call_count == 2
    assert [c.kwargs["params"]["period"] for c in client.get.call_args_list] == [
        "2021",
        "2022",
    ]


def test_fetch_tariffline_without_period_makes_a_single_call(client) -> None:
    client.get.return_value = _json_response(_rows(1))
    client._fetch_tariffline({"period": None}, typeCode="C", freqCode="A", clCode="HS")
    assert client.get.call_count == 1
    assert "period" not in client.get.call_args.kwargs["params"]


def test_fetch_tariffline_counts_failed_calls_too(client) -> None:
    client.get.side_effect = _http_error(500)
    with pytest.raises(ComtradeAPIError):
        client._fetch_tariffline(
            {"period": "2021"}, typeCode="C", freqCode="A", clCode="HS"
        )
    assert client.api_calls == 1


# ──────────────────────────────────────────────────────────────────────
# get_data et subdivision
# ──────────────────────────────────────────────────────────────────────


def test_get_data_rejects_unknown_frequency(client) -> None:
    with pytest.raises(ValueError, match="Invalid value for frequency"):
        client.get_data(frequency="weekly")


def test_get_data_returns_frame_and_resolved_metadata(client) -> None:
    client.get.return_value = _json_response(_rows(2))

    df, metadata = client.get_data(
        flows="M",
        reporters=[251, 276],
        products=1,
        partners=0,
        partners2=0,
        periods=["2021"],
        frequency="monthly",
    )

    assert len(df) == 2
    assert metadata["reporters"] == "251,276"
    assert metadata["periods"] == "2021"
    assert metadata["frequency"] == "monthly"
    assert client.get.call_args.args[0].endswith("/C/M/HS")


def test_get_data_expands_period_boundaries(client) -> None:
    client.get.return_value = _json_response(_rows(1))
    _, metadata = client.get_data(
        flows="M",
        reporters=1,
        products=1,
        partners=1,
        partners2=1,
        period_start="2020-01-01",
        period_end="2021-01-01",
    )
    assert metadata["periods"] == "2020,2021"
    assert client.get.call_count == 2


def test_get_data_subdivides_when_the_record_limit_is_reached(
    client, monkeypatch
) -> None:
    monkeypatch.setitem(client_module.PARAMETERS, "LIMIT", 2)
    client.get.side_effect = [
        _json_response(_rows(2)),  # limite atteinte → subdivision sur les flux
        _json_response(_rows(1)),
        _json_response(_rows(1)),
    ]

    df, metadata = client.get_data(
        flows=["M", "X"],
        reporters=1,
        products=1,
        partners=1,
        partners2=1,
        periods="2022",
    )

    assert len(df) == 2
    assert client.get.call_count == 3
    sent_flows = [c.kwargs["params"].get("flowCode") for c in client.get.call_args_list]
    assert sent_flows == ["M,X", "M", "X"]
    assert metadata["flows"] == "M,X"


def test_get_data_keeps_a_truncated_response_that_cannot_be_split(
    client, monkeypatch, caplog
) -> None:
    monkeypatch.setitem(client_module.PARAMETERS, "LIMIT", 1)
    client.get.return_value = _json_response(_rows(1))

    with caplog.at_level("WARNING"):
        df, _ = client.get_data(
            flows="M",
            reporters=1,
            products=1,
            partners=1,
            partners2=1,
            periods="2022",
        )

    assert len(df) == 1
    assert "Unable to further truncate" in caplog.text


# ──────────────────────────────────────────────────────────────────────
# _divide_request
# ──────────────────────────────────────────────────────────────────────


def test_divide_request_rejects_invalid_or_indivisible_dimensions(client) -> None:
    with pytest.raises(ValueError, match="Invalid subdivision"):
        client._divide_request("nope", {}, {})
    with pytest.raises(ValueError, match="Unable to further truncate"):
        client._divide_request("flows", {"flows": "M"}, {})
    with pytest.raises(ValueError, match="Unable to request the valid values"):
        client._divide_request("periods", {"periods": None}, {})


def test_divide_request_enumerates_codes_when_the_dimension_is_unset(client) -> None:
    client._extract_codes = mock.Mock(return_value=["M", "X", "R"])  # type: ignore[method-assign]
    client.get_data = mock.Mock(  # type: ignore[method-assign]
        side_effect=[
            (pd.DataFrame({"a": [1]}), {"flows": "M", "frequency": "annual"}),
            (pd.DataFrame({"a": [2]}), {"flows": "X,R", "frequency": "annual"}),
        ]
    )

    df, metadata = client._divide_request("flows", {"flows": None}, {"type_code": "C"})

    client._extract_codes.assert_called_once_with(category="flow")
    # Moitié inférieure / supérieure de la liste, autres paramètres propagés
    assert client.get_data.call_args_list[0].kwargs["flows"] == ["M"]
    assert client.get_data.call_args_list[1].kwargs["flows"] == ["X", "R"]
    assert df["a"].tolist() == [1, 2]
    # Seules les dimensions scindées sont accolées
    assert metadata == {"flows": "M,X,R", "frequency": "annual"}


# ──────────────────────────────────────────────────────────────────────
# Métadonnées de référence
# ──────────────────────────────────────────────────────────────────────

REFERENCES = {
    "results": [
        {"category": "flow", "fileuri": "https://files/flow.json"},
        {"category": "reporter", "fileuri": "https://files/reporter.json"},
    ]
}
FLOW = {"results": [{"id": "M", "text": "Import"}, {"id": "X", "text": "Export"}]}
REPORTER = {
    "results": [
        {
            "reporterCode": 251,
            "reporterDesc": "France",
            "entryExpiredDate": None,
        },
        {"reporterCode": 99, "reporterDesc": "Old", "entryExpiredDate": "2000-01-01"},
    ]
}


@pytest.fixture
def references(client):
    files = {
        client.REFERENCES_URL: REFERENCES,
        "https://files/flow.json": FLOW,
        "https://files/reporter.json": REPORTER,
    }
    client.get.side_effect = lambda url, **_: _json_response(files[url])
    return client


def test_get_metadata_registry_and_category_are_cached(references) -> None:
    references.rate_limiter = mock.Mock()

    registry = references.get_metadata()
    assert registry["category"].tolist() == ["flow", "reporter"]
    flows = references.get_metadata("flow")
    assert flows["id"].tolist() == ["M", "X"]
    calls_after_first = references.get.call_count

    flows.loc[0, "id"] = "mutated"  # le cache ne doit pas être altéré
    assert references.get_metadata("flow")["id"].tolist() == ["M", "X"]
    assert references.get.call_count == calls_after_first
    # Aucun créneau de limitation consommé sur un accès au cache
    assert references.rate_limiter.acquire.call_count == 2


def test_get_metadata_refresh_bypasses_the_cache(references) -> None:
    references.get_metadata("flow")
    before = references.get.call_count
    references.get_metadata("flow", refresh=True)
    assert references.get.call_count == before + 2  # registre + fichier


def test_get_metadata_unknown_category(references) -> None:
    with pytest.raises(ValueError, match="Invalid 'category' : nope"):
        references.get_metadata("nope")


def test_extract_codes_drops_expired_entries(references) -> None:
    assert references._extract_codes("flow") == ["M", "X"]
    assert references._extract_codes("reporter") == [251]


def test_get_codelist_has_code_and_label(references) -> None:
    codelist = references.get_codelist("flow")
    assert codelist["code"].tolist() == ["M", "X"]
    assert codelist["label"].tolist() == ["Import", "Export"]


# ──────────────────────────────────────────────────────────────────────
# Disponibilité et téléchargement incrémental
# ──────────────────────────────────────────────────────────────────────

AVAILABILITY = {
    "data": [
        {"period": 2022, "reporterCode": 251, "lastReleased": "2026-02-06T10:18:40.23"},
        {"period": 2022, "reporterCode": 276, "lastReleased": "2025-01-01T00:00:00"},
    ]
}


def test_availability_request_with_key_and_list_arguments(keyed) -> None:
    keyed.get.return_value = _json_response(AVAILABILITY)

    df = keyed.get_tariffline_data_availability(
        reporters=[251, 276], periods=[2021, 2022], frequency="monthly"
    )

    assert len(df) == 2
    call = keyed.get.call_args
    assert call.args[0] == "/data/v1/getDaTariffline/C/M/HS"
    assert call.kwargs["params"] == {"reportercode": "251,276", "period": "2021,2022"}
    assert keyed.api_calls == 1


def test_availability_failure_still_counts_the_call(client) -> None:
    client.get.side_effect = _http_error(500)
    with pytest.raises(ComtradeAPIError):
        client.get_tariffline_data_availability()
    assert client.api_calls == 1


def _query(**overrides) -> ComtradeQueryRequest:
    defaults = {"dataflow": "C_A_HS", "reporters": "251", "periods": "2022"}
    return ComtradeQueryRequest(**{**defaults, **overrides})


def test_period_last_released_takes_the_latest_date_and_is_memoised(client) -> None:
    client.get.return_value = _json_response(AVAILABILITY)

    first = client._period_last_released(_query(products="01"))
    second = client._period_last_released(
        _query(products="02")
    )  # autre lot, même période

    assert first == datetime(2026, 2, 6, 10, 18, 40, 230000, tzinfo=UTC)
    assert second == first
    assert client.get.call_count == 1


def test_period_last_released_failure_is_not_fatal_and_cached(client) -> None:
    client.get.side_effect = _http_error(500)
    assert client._period_last_released(_query()) is None
    assert client._period_last_released(_query()) is None
    assert client.get.call_count == 1


def test_period_last_released_without_dates_is_none(client) -> None:
    client.get.return_value = _json_response({"data": []})
    assert client._period_last_released(_query()) is None


def test_fetch_updates_first_download_is_full(client) -> None:
    client.execute_query = mock.Mock(return_value=pd.DataFrame({"a": [1]}))  # type: ignore[method-assign]
    assert len(client.fetch_updates(_query(), None)) == 1


@pytest.mark.parametrize(
    ("since", "last_released", "refetched"),
    [
        (datetime(2026, 3, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC), False),
        (datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 2, 1, tzinfo=UTC), True),
        (datetime(2026, 3, 1, tzinfo=UTC), None, True),
    ],
    ids=["unchanged", "republished", "unknown-release-date"],
)
def test_fetch_updates_compares_since_with_the_release_date(
    client, since, last_released, refetched
) -> None:
    client._period_last_released = mock.Mock(return_value=last_released)  # type: ignore[method-assign]
    client.execute_query = mock.Mock(return_value=pd.DataFrame({"a": [1]}))  # type: ignore[method-assign]

    result = client.fetch_updates(_query(), since)

    assert client.execute_query.called is refetched
    assert result.empty is not refetched


def test_fetch_updates_skip_logs_periods_given_as_boundaries(client, caplog) -> None:
    client._period_last_released = mock.Mock(  # type: ignore[method-assign]
        return_value=datetime(2020, 1, 1, tzinfo=UTC)
    )
    query = _query(periods=None, period_start="2020-01-01", period_end="2021-01-01")
    with caplog.at_level("INFO"):
        assert client.fetch_updates(query, datetime(2026, 1, 1, tzinfo=UTC)).empty
    assert "2020-2021" in caplog.text


# ──────────────────────────────────────────────────────────────────────
# execute_query et structure
# ──────────────────────────────────────────────────────────────────────


def test_execute_query_aggregates_duplicates_on_the_primary_key(client) -> None:
    client.get_data = mock.Mock(  # type: ignore[method-assign]
        return_value=(
            pd.DataFrame(
                {
                    "period": [2022, 2022, 2022],
                    "reporterCode": [251, 251, 276],
                    "flowCode": ["M", "M", "M"],
                    "primaryValue": [1.0, 2.0, 5.0],
                }
            ),
            {},
        )
    )

    df = client.execute_query(_query())

    assert df.sort_values("reporterCode")["primaryValue"].tolist() == [3.0, 5.0]
    assert client.get_data.call_args.kwargs["format_output"] == "JSON"


def test_execute_query_returns_an_empty_response_as_is(client) -> None:
    client.get_data = mock.Mock(return_value=(pd.DataFrame(), {}))  # type: ignore[method-assign]
    assert client.execute_query(_query()).empty


def test_get_structure_reads_the_registry(client) -> None:
    structure = client.get_structure("C_A_HS")
    assert structure is client.resolve_query_structure(_query())
    assert structure.get_position("reporterISO") is not None


def test_get_structure_rejects_an_undeclared_dataflow(client) -> None:
    with pytest.raises(ValueError, match="No structure declared for COMTRADE::S_A_HS"):
        client.get_structure("S_A_HS")
    # Le message liste les dataflows déclarés et indique où en ajouter
    with pytest.raises(ValueError, match=r"C_A_HS.*C_M_HS.*comtrade\.json"):
        client.get_structure("S_A_HS")
    assert not client.structure_registry.has("COMTRADE", "S_A_HS")
