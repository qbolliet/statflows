"""Tests — :mod:`statflows.core.client` (``APIClient`` and ``AbstractSDMXClient``).

Frozen behaviour: URL joining, header merging, per-request timeout override and
HTTP counters; shared SDMX pipeline — structure cache, split-request loop (empty
and ``NoRecordsFound`` answers tolerated, genuine errors surfaced), post-filtering,
duplicate detection and cartesian split. No network: the session is mocked.
"""

from __future__ import annotations

import warnings
from unittest import mock

import pandas as pd
import pytest
import requests

from statflows.core.client import AbstractSDMXClient, APIClient
from statflows.core.rate_limiter import RateLimiter
from statflows.core.structures import DataflowStructure, DimensionInfo

# ──────────────────────────────────────────────────────────────────────
# APIClient
# ──────────────────────────────────────────────────────────────────────


def _response(status: int = 200, body: bytes = b"ok", text: str = "") -> mock.Mock:
    response = mock.Mock(spec=requests.Response)
    response.status_code = status
    response.content = body
    response.text = text
    if status >= 400:
        response.raise_for_status.side_effect = requests.exceptions.HTTPError(
            f"{status}", response=response
        )
    return response


@pytest.fixture
def api() -> APIClient:
    client = APIClient("https://api.example.com/", timeout=10, headers={"X-Key": "k"})
    client.session = mock.Mock()
    return client


def test_get_joins_url_merges_headers_and_counts_success(api) -> None:
    api.session.get.return_value = _response(body=b"12345")

    api.get("/data", params={"a": 1}, headers={"Accept": "json"})

    api.session.get.assert_called_once_with(
        "https://api.example.com/data",
        params={"a": 1},
        headers={"X-Key": "k", "Accept": "json"},
        timeout=10,
    )
    assert api.stats_.n_requests == 1
    assert api.stats_.total_bytes == 5
    assert api.stats_.status_counts == {"200": 1}


def test_get_per_request_timeout_overrides_default(api) -> None:
    api.session.get.return_value = _response()
    api.get("data", timeout=99)
    assert api.session.get.call_args.kwargs["timeout"] == 99


def test_get_http_error_is_counted_and_reraised(api) -> None:
    api.session.get.return_value = _response(500, text="boom")
    with pytest.raises(requests.exceptions.HTTPError):
        api.get("data")
    assert api.stats_.n_failures == 1
    assert api.stats_.status_counts == {"500": 1}


def test_get_connection_error_is_counted_and_reraised(api) -> None:
    api.session.get.side_effect = requests.exceptions.ConnectionError("down")
    with pytest.raises(requests.exceptions.ConnectionError):
        api.get("data")
    assert api.stats_.n_failures == 1
    assert api.stats_.status_counts == {"error": 1}


def test_session_retries_on_server_errors_and_context_manager_closes() -> None:
    with APIClient("https://api.example.com", max_retries=2) as client:
        adapter = client.session.get_adapter("https://api.example.com")
        assert adapter.max_retries.total == 2
        assert 503 in adapter.max_retries.status_forcelist
        session = client.session
        session.close = mock.Mock()
    session.close.assert_called_once()


# ──────────────────────────────────────────────────────────────────────
# Client SDMX minimal
# ──────────────────────────────────────────────────────────────────────


class _Client(AbstractSDMXClient):
    """Concrete client driven by a scripted list of sub-request outcomes."""

    PROVIDER_CONFIG_NAME = None

    def __init__(self, outcomes=(), **kwargs) -> None:
        super().__init__(**kwargs)
        self.outcomes = list(outcomes)
        self.requested: list[dict] = []
        self.fetched: list[tuple[str, str]] = []
        self.closed = False

    def get_data(self, **kwargs) -> pd.DataFrame:
        return pd.DataFrame({"called_with": [kwargs]})

    def close(self) -> None:
        self.closed = True

    def _fetch_structure(self, agency, dataflow, **kwargs) -> DataflowStructure:
        self.fetched.append((agency, dataflow))
        return DataflowStructure(agency, dataflow, 1, [DimensionInfo("geo", 0)])

    def _execute_single_request(self, dims_for_request, **request_kwargs):
        self.requested.append(dims_for_request)
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def _resolve_structure(self, params):
        return self._ensure_structure("AG", params["dataflow"])

    def _prepare_requests(self, structure, params):
        combos = [({"geo": [g]}, {"geo": [g]}) for g in params["geos"]]
        return combos, {"geo": params["geos"]}, {}

    def fetch_updates(self, query, since, n_observations=10):
        return pd.DataFrame()


def _frame(*geos: str) -> pd.DataFrame:
    return pd.DataFrame({"geo": list(geos), "value": range(len(geos))})


def _no_records() -> requests.exceptions.HTTPError:
    return requests.exceptions.HTTPError(
        "404", response=_response(404, text="NoRecordsFound")
    )


# ──────────────────────────────────────────────────────────────────────
# Rate limiter, structures, execute_query
# ──────────────────────────────────────────────────────────────────────


def test_rate_limiter_resolution() -> None:
    explicit = RateLimiter(1, "seconds")
    assert _Client(rate_limiter=explicit).rate_limiter is explicit
    # Sans nom de configuration, ni limiteur explicite ni chargement automatique
    assert _Client().rate_limiter is None
    assert _Client(auto_load_rate_limit=False).rate_limiter is None


def test_load_rate_limiter_reads_provider_file() -> None:
    class _Eurostat(_Client):
        PROVIDER_CONFIG_NAME = "eurostat"

    assert _Eurostat().rate_limiter is not None


def test_load_rate_limiter_missing_file_or_section_gives_none() -> None:
    class _Missing(_Client):
        PROVIDER_CONFIG_NAME = "does_not_exist"

    assert _Missing().rate_limiter is None


def test_load_rate_limiter_swallows_invalid_configuration() -> None:
    class _Broken(_Client):
        PROVIDER_CONFIG_NAME = "eurostat"

    with mock.patch(
        "statflows.core.client.build_rate_limiter", side_effect=ValueError("bad")
    ):
        assert _Broken().rate_limiter is None


def test_execute_query_delegates_to_get_data() -> None:
    query = mock.Mock()
    query.to_dict.return_value = {"dataflow": "DF"}
    result = _Client().execute_query(query)
    assert result["called_with"].iloc[0] == {"dataflow": "DF"}


def test_ensure_structure_fetches_once_then_uses_cache() -> None:
    client = _Client()
    first = client._ensure_structure("AG", "DF")
    second = client._ensure_structure("AG", "DF")
    assert first is second
    assert client.fetched == [("AG", "DF")]
    assert client.n_structures_fetched_ == 1


def test_ensure_structure_without_auto_fetch_returns_none() -> None:
    client = _Client(auto_fetch_structure=False)
    assert client._ensure_structure("AG", "DF") is None
    assert client.fetched == []


def test_ensure_structure_swallows_fetch_failures() -> None:
    client = _Client()
    with mock.patch.object(client, "_fetch_structure", side_effect=OSError("down")):
        assert client._ensure_structure("AG", "DF") is None


def test_resolve_query_structure_registers_the_structure() -> None:
    client = _Client()
    query = mock.Mock()
    query.to_dict.return_value = {"dataflow": "DF"}
    assert client.resolve_query_structure(query) is not None
    assert client.structure_registry.has("AG", "DF")


def test_context_manager_closes_the_client() -> None:
    with _Client() as client:
        assert not client.closed
    assert client.closed


# ──────────────────────────────────────────────────────────────────────
# Pipeline et boucle de sous-requêtes
# ──────────────────────────────────────────────────────────────────────


def test_pipeline_fills_the_fetch_report() -> None:
    client = _Client(outcomes=[_frame("FR", "DE"), _frame("IT")])
    df = client._execute_query_pipeline({"dataflow": "DF", "geos": ["FR", "IT"]})

    # Post-filtre : seules les lignes demandées sont conservées
    assert df["geo"].tolist() == ["FR", "IT"]
    report = client.last_fetch_report_
    assert report is not None
    assert report.n_requests == 2
    assert report.rows_fetched == 3
    assert report.rows_after_filter == 2
    assert report.n_duplicates == 0
    assert report.structure_from_cache is False

    # Deuxième exécution : structure en cache
    client.outcomes = [_frame("FR")]
    client._execute_query_pipeline({"dataflow": "DF", "geos": ["FR"]})
    assert client.last_fetch_report_.structure_from_cache is True  # type: ignore[union-attr]


def test_pipeline_ignore_skips_duplicate_check() -> None:
    client = _Client(outcomes=[_frame("FR", "FR")])
    client._execute_query_pipeline(
        {"dataflow": "DF", "geos": ["FR"], "on_duplicate": "ignore"}
    )
    assert client.last_fetch_report_.n_duplicates == 0  # type: ignore[union-attr]


def test_split_requests_tolerate_no_records_and_empty_answers() -> None:
    client = _Client(outcomes=[_no_records(), pd.DataFrame(), _frame("IT")])
    report_target = _Client().last_fetch_report_
    assert report_target is None

    from statflows.core.reports import FetchReport

    report = FetchReport()
    combos = [({"geo": [g]}, {"geo": [g]}) for g in ("FR", "DE", "IT")]
    df = client._execute_split_requests(combos, report=report)

    assert df["geo"].tolist() == ["IT"]
    assert (report.n_no_records, report.n_empty_responses) == (1, 1)
    assert report.n_request_errors == 0


def test_split_requests_all_no_records_return_empty_frame() -> None:
    client = _Client(outcomes=[_no_records()])
    assert client._execute_split_requests([({"geo": ["FR"]}, {})]).empty


def test_split_requests_post_filter_emptying_everything_returns_empty_frame() -> None:
    client = _Client(outcomes=[_frame("DE")])
    assert client._execute_split_requests([({"geo": ["FR"]}, {"geo": ["FR"]})]).empty


def test_split_requests_all_failures_raise_with_summary() -> None:
    client = _Client(outcomes=[RuntimeError("boom")])
    with pytest.raises(ValueError, match=r"(?s)All 1 split requests failed.*boom"):
        client._execute_split_requests([({"geo": ["FR"]}, {})])


def test_split_requests_partial_failure_keeps_successful_frames(caplog) -> None:
    client = _Client(outcomes=[RuntimeError("boom"), _frame("DE")])
    with caplog.at_level("WARNING"):
        df = client._execute_split_requests(
            [({"geo": ["FR"]}, {}), ({"geo": ["DE"]}, {})]
        )
    assert df["geo"].tolist() == ["DE"]
    assert "1 out of 2 requests failed" in caplog.text


def test_split_requests_acquire_the_rate_limiter_before_each_request() -> None:
    limiter = mock.Mock()
    client = _Client(outcomes=[_frame("FR"), _frame("DE")], rate_limiter=limiter)
    client._execute_split_requests([({"geo": ["FR"]}, {}), ({"geo": ["DE"]}, {})])
    assert limiter.acquire.call_count == 2


@pytest.mark.parametrize(
    ("exc", "expected"),
    [
        (_no_records(), True),
        (
            requests.exceptions.HTTPError("404", response=_response(404, text="other")),
            False,
        ),
        (
            requests.exceptions.HTTPError(
                "500", response=_response(500, text="NoRecordsFound")
            ),
            False,
        ),
        (requests.exceptions.HTTPError("no response"), False),
        (ValueError("NoRecordsFound"), False),
    ],
)
def test_is_no_records_error(exc, expected) -> None:
    assert AbstractSDMXClient._is_no_records_error(exc) is expected


# ──────────────────────────────────────────────────────────────────────
# Fonctions statiques
# ──────────────────────────────────────────────────────────────────────


def test_cartesian_split() -> None:
    split = AbstractSDMXClient._cartesian_split
    assert split({}, 10) == [{}]
    assert split({"a": ["1", "2"], "b": ["x"]}, 10) == [
        {"a": "1", "b": "x"},
        {"a": "2", "b": "x"},
    ]
    with pytest.raises(ValueError, match="exceeding max_split_combinations=3"):
        split({"a": ["1", "2"], "b": ["x", "y"]}, 3)


def test_filter_dataframe_by_dimensions(caplog) -> None:
    df = pd.DataFrame({"geo": ["FR", "DE", "IT"], "freq": ["A", "A", "M"]})
    filter_ = AbstractSDMXClient._filter_dataframe_by_dimensions

    assert filter_(df, {"geo": ["FR", "IT"], "freq": ["A"]})["geo"].tolist() == ["FR"]
    assert filter_(df, {}) is df
    assert filter_(pd.DataFrame(), {"geo": ["FR"]}).empty
    with caplog.at_level("WARNING"):
        assert len(filter_(df, {"missing": ["x"]})) == 3
    assert "not found in DataFrame" in caplog.text


def _structure() -> DataflowStructure:
    return DataflowStructure(
        "AG", "DF", 2, [DimensionInfo("geo", 0), DimensionInfo("freq", 1)]
    )


def test_check_duplicates_counts_and_warns() -> None:
    df = pd.DataFrame({"geo": ["FR", "FR", "DE"], "value": [1, 2, 3]})
    with pytest.warns(UserWarning, match="Found 2 duplicate rows"):
        count = AbstractSDMXClient._check_duplicates(df, {"geo": ["FR"]}, None, "warn")
    assert count == 2


def test_check_duplicates_raises_when_asked() -> None:
    df = pd.DataFrame({"geo": ["FR", "FR"], "value": [1, 2]})
    with pytest.raises(ValueError, match="duplicate rows"):
        AbstractSDMXClient._check_duplicates(df, {"geo": ["FR"]}, None, "raise")


def test_check_duplicates_resolves_positions_through_the_structure() -> None:
    df = pd.DataFrame({"geo": ["FR", "FR"], "freq": ["A", "M"], "value": [1, 2]})
    # Position 0 → "geo" : doublon sur geo seul
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assert (
            AbstractSDMXClient._check_duplicates(df, {0: ["FR"]}, _structure(), "warn")
            == 2
        )


def test_check_duplicates_falls_back_to_all_columns_but_the_value() -> None:
    df = pd.DataFrame({"geo": ["FR", "FR"], "OBS_VALUE": [1, 2]})
    # Sans dimension exploitable : clé = toutes les colonnes hors valeur observée
    with pytest.warns(UserWarning, match=r"columns \['geo'\]"):
        assert AbstractSDMXClient._check_duplicates(df, {}, None, "warn") == 2


def test_check_duplicates_does_not_mutate_default_dimensions() -> None:
    df = pd.DataFrame({"geo": ["FR", "FR"], "freq": ["A", "A"], "value": [1, 2]})
    given = ["freq"]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        AbstractSDMXClient._check_duplicates(df, {"geo": ["FR"]}, None, "warn", given)
        AbstractSDMXClient._check_duplicates(df, {"geo": ["FR"]}, None, "warn")
    # Ni la liste de l'appelant ni le défaut partagé ne doivent avoir bougé
    assert given == ["freq"]
    assert AbstractSDMXClient._check_duplicates.__defaults__ == ([],)


def test_check_duplicates_empty_frame_has_no_duplicates() -> None:
    assert AbstractSDMXClient._check_duplicates(pd.DataFrame(), {}, None, "raise") == 0


def test_parse_csv_response() -> None:
    df = AbstractSDMXClient._parse_csv_response("a,b\n1,2\n")
    assert df.to_dict("records") == [{"a": 1, "b": 2}]
    with pytest.raises(ValueError, match="Failed to parse CSV response"):
        AbstractSDMXClient._parse_csv_response("")
