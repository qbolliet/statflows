"""Tests — :mod:`statflows.core.factory` (clients, queries, code filtering).

Frozen behaviour: client factories resolved from a provider alias or a dotted
path and validated at construction (callable target, bound arguments, enum
coercion, hidden credentials, pickling), ``build_client`` restricted to the
provider aliases with an explicit error for unknown providers, JSON
specifications turned into query objects (unknown keys ignored, textual format
coerced), and the four-step codelist filter.
"""

from __future__ import annotations

import pickle

import pytest

from statflows import ComtradeClient, EurostatClient, SDMXVersion, UNSDClient
from statflows.core.factory import (
    CLIENT_PATHS,
    ClientFactory,
    build_client,
    build_queries,
    filter_codes,
)
from statflows.sources.eurostat.queries import EurostatQueryRequestV30
from statflows.sources.oecd.client import OECDClient
from statflows.sources.oecd.formats import OECDResponseFormat
from statflows.sources.oecd.queries import OECDQueryRequest
from tests.utils.clients import Mode, RecordingClient, UnresolvableClient

RECORDING = "tests.utils.clients.RecordingClient"

PROVIDERS = [
    ("eurostat", EurostatClient),
    ("oecd", OECDClient),
    ("comtrade", ComtradeClient),
    ("unsd", UNSDClient),
]

# ──────────────────────────────────────────────────────────────────────
# ClientFactory — résolution de la cible
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("alias", "client_cls"), PROVIDERS)
def test_factory_alias_builds_provider_client(alias, client_cls) -> None:
    factory = ClientFactory(alias)
    assert factory.target == alias
    assert factory.path == CLIENT_PATHS[alias]
    with factory() as client:
        assert type(client) is client_cls


@pytest.mark.parametrize(
    "target",
    [
        "tests.utils.clients.RecordingClient",
        "tests.utils.clients:RecordingClient",
        "tests.utils.clients.make_recording_client",
    ],
)
def test_factory_dotted_path_targets(target) -> None:
    factory = ClientFactory(target, options={"timeout": 5})
    assert factory.path == target
    client = factory()
    assert isinstance(client, RecordingClient)
    assert client.kwargs["timeout"] == 5


def test_factory_unknown_alias_lists_known_aliases() -> None:
    with pytest.raises(ValueError, match="Unknown client target 'nope'.*eurostat"):
        ClientFactory("nope")


@pytest.mark.parametrize(
    "target",
    ["tests.utils.no_such_module.Client", "tests.utils.no_such_module:Client"],
)
def test_factory_missing_module_raises_import_error(target) -> None:
    with pytest.raises(ImportError, match="no_such_module"):
        ClientFactory(target)


@pytest.mark.parametrize(
    "target", ["tests.utils.clients.Missing", "tests.utils.clients:Missing"]
)
def test_factory_missing_attribute_raises_import_error(target) -> None:
    with pytest.raises(ImportError, match="has no attribute 'Missing'"):
        ClientFactory(target)


@pytest.mark.parametrize("target", ["a.b:", ":Name", "a:b:C", "a..B", "a."])
def test_factory_malformed_path_raises_value_error(target) -> None:
    with pytest.raises(ValueError, match="Malformed dotted path"):
        ClientFactory(target)


def test_factory_non_callable_target() -> None:
    with pytest.raises(TypeError, match="is not callable"):
        ClientFactory("tests.utils.clients.NOT_CALLABLE")


def test_client_paths_is_read_only() -> None:
    assert set(CLIENT_PATHS) == {"eurostat", "oecd", "comtrade", "unsd"}
    with pytest.raises(TypeError):
        CLIENT_PATHS["other"] = "pkg.mod.Client"  # type: ignore[index]


# ──────────────────────────────────────────────────────────────────────
# ClientFactory — validation des arguments
# ──────────────────────────────────────────────────────────────────────


def test_factory_construction_never_builds_a_client() -> None:
    before = RecordingClient.instances
    ClientFactory(RECORDING, options={"mode": "slow", "timeout": 1})
    assert RecordingClient.instances == before


def test_factory_unknown_option_fails_at_construction() -> None:
    before = RecordingClient.instances
    with pytest.raises(TypeError, match="unexpected keyword argument 'timeot'"):
        ClientFactory(RECORDING, options={"timeot": 5})
    assert RecordingClient.instances == before


def test_factory_missing_required_argument_fails_at_construction() -> None:
    with pytest.raises(TypeError, match="Invalid arguments.*missing"):
        ClientFactory("collections.namedtuple")


def test_factory_unknown_credential_fails_at_construction() -> None:
    with pytest.raises(TypeError, match="Invalid arguments"):
        ClientFactory("unsd", credentials={"api_key": "secret"})


def test_factory_var_keyword_target_defers_validation_to_the_call() -> None:
    factory = ClientFactory(
        "tests.utils.clients.make_recording_client", options={"timeot": 5}
    )
    with pytest.raises(TypeError, match="timeot"):
        factory()


# ──────────────────────────────────────────────────────────────────────
# ClientFactory — coercition des enums
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("slow-mode", Mode.SLOW),  # valeur
        ("SLOW", Mode.SLOW),  # nom
        ("slow", Mode.SLOW),  # nom, casse ignorée
        ("Fast", Mode.FAST),
        (Mode.SLOW, Mode.SLOW),  # membre conservé
    ],
)
def test_factory_coerces_enum_options(raw, expected) -> None:
    factory = ClientFactory(RECORDING, options={"mode": raw, "fallback": raw})
    assert factory.options == {"mode": expected, "fallback": expected}
    assert factory().kwargs["fallback"] is expected


def test_factory_keeps_none_for_optional_enum() -> None:
    factory = ClientFactory(RECORDING, options={"fallback": None})
    assert factory.options["fallback"] is None


def test_factory_does_not_coerce_wider_unions() -> None:
    # str | Mode | None : une chaîne est une valeur légitime
    factory = ClientFactory(RECORDING, options={"label": "slow"})
    assert factory.options["label"] == "slow"


def test_factory_invalid_enum_value_lists_admitted_values() -> None:
    with pytest.raises(ValueError) as excinfo:
        ClientFactory(RECORDING, options={"mode": "turbo"})
    message = str(excinfo.value)
    assert "'turbo'" in message and "'mode'" in message
    assert "fast-mode" in message and "SLOW" in message


def test_factory_unresolvable_annotations_skip_coercion() -> None:
    factory = ClientFactory(
        "tests.utils.clients.UnresolvableClient", options={"mode": "slow"}
    )
    assert factory.options["mode"] == "slow"
    assert isinstance(factory(), UnresolvableClient)


@pytest.mark.parametrize(
    ("alias", "option", "raw", "expected"),
    [
        ("eurostat", "api_version", "v2_1", SDMXVersion.V2_1),
        ("eurostat", "api_version", "V3", SDMXVersion.V3),
        ("oecd", "sdmx_version", "v1", SDMXVersion.V1),
    ],
)
def test_factory_coerces_sdmx_version_of_real_clients(
    alias, option, raw, expected
) -> None:
    factory = ClientFactory(alias, options={option: raw})
    with factory() as client:
        assert getattr(client, option) is expected


# ──────────────────────────────────────────────────────────────────────
# ClientFactory — credentials
# ──────────────────────────────────────────────────────────────────────


def test_factory_passes_credentials_and_hides_them() -> None:
    factory = ClientFactory(
        RECORDING, options={"timeout": 3}, credentials={"token": "s3cr3t"}
    )
    assert factory().kwargs["token"] == "s3cr3t"
    text = repr(factory)
    assert "s3cr3t" not in text and "token" not in text
    assert text == (
        "ClientFactory('tests.utils.clients.RecordingClient', "
        "options={'timeout': 3}, credentials=<1 hidden>)"
    )


def test_factory_repr_without_credentials() -> None:
    assert repr(ClientFactory("unsd", options={"timeout": 30})) == (
        "ClientFactory('unsd', options={'timeout': 30})"
    )


def test_factory_comtrade_subscription_key_as_credential() -> None:
    factory = ClientFactory("comtrade", credentials={"subscription_key": "k"})
    with factory() as client:
        assert client.subscription_key == "k"


def test_factory_conflicting_keys_raise_without_values() -> None:
    with pytest.raises(ValueError, match=r"\['token'\]") as excinfo:
        ClientFactory(
            RECORDING, options={"token": "visible"}, credentials={"token": "s3cr3t"}
        )
    assert "s3cr3t" not in str(excinfo.value)


# ──────────────────────────────────────────────────────────────────────
# ClientFactory — appel, égalité, pickle
# ──────────────────────────────────────────────────────────────────────


def test_factory_builds_a_new_client_on_every_call() -> None:
    factory = ClientFactory(RECORDING)
    first, second = factory(), factory()
    assert first is not second


def test_factory_does_not_alias_caller_dicts() -> None:
    options = {"timeout": 3}
    factory = ClientFactory(RECORDING, options=options)
    options["timeout"] = 99
    assert factory.options == {"timeout": 3}


def test_factory_pickle_round_trip_then_call() -> None:
    factory = ClientFactory(
        RECORDING, options={"mode": "slow"}, credentials={"token": "s3cr3t"}
    )
    restored = pickle.loads(pickle.dumps(factory))
    assert restored == factory
    assert restored.options == {"mode": Mode.SLOW}
    assert restored().kwargs["token"] == "s3cr3t"


def test_factory_pickle_round_trip_of_provider_alias() -> None:
    factory = ClientFactory("eurostat", options={"api_version": "v2_1"})
    restored = pickle.loads(pickle.dumps(factory))
    assert restored == factory
    assert restored.target == "eurostat"


def test_factory_with_unpicklable_option_is_unpicklable() -> None:
    factory = ClientFactory(RECORDING, options={"label": lambda: None})
    with pytest.raises((pickle.PicklingError, AttributeError)):
        pickle.dumps(factory)


def test_factory_equality() -> None:
    alias = ClientFactory("eurostat", options={"timeout": 5})
    assert alias == ClientFactory(
        "statflows.sources.eurostat.client:EurostatClient", options={"timeout": 5}
    )
    assert alias != ClientFactory("eurostat", options={"timeout": 6})
    assert alias != ClientFactory("oecd", options={"timeout": 5})
    assert ClientFactory(RECORDING, options={"mode": "slow"}) == ClientFactory(
        RECORDING, options={"mode": Mode.SLOW}
    )
    assert ClientFactory(RECORDING, credentials={"token": "a"}) != ClientFactory(
        RECORDING, credentials={"token": "b"}
    )
    assert alias != "eurostat"


def test_factory_is_not_hashable() -> None:
    with pytest.raises(TypeError):
        hash(ClientFactory("unsd"))


# ──────────────────────────────────────────────────────────────────────
# build_client
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(("provider", "client_cls"), PROVIDERS)
def test_build_client_dispatches_on_provider(provider, client_cls) -> None:
    assert isinstance(build_client(provider), client_cls)


def test_build_client_keyword_provider_still_accepted() -> None:
    assert isinstance(build_client(provider="oecd"), OECDClient)


def test_build_client_forwards_options() -> None:
    client = build_client("eurostat", api_version="v2_1", auto_load_rate_limit=False)
    assert client.api_version is SDMXVersion.V2_1
    assert client.rate_limiter is None


def test_build_client_unknown_option() -> None:
    with pytest.raises(TypeError, match="unexpected keyword argument 'timeot'"):
        build_client("unsd", timeot=30)


def test_build_client_unknown_provider() -> None:
    with pytest.raises(
        ValueError,
        match="Unknown provider 'nope'; known providers: comtrade, eurostat, oecd, unsd",
    ):
        build_client("nope")


def test_build_client_rejects_dotted_paths() -> None:
    with pytest.raises(ValueError, match="Unknown provider"):
        build_client(RECORDING)


# ──────────────────────────────────────────────────────────────────────
# build_queries
# ──────────────────────────────────────────────────────────────────────


def test_build_queries_ignores_unknown_keys_and_coerces_format() -> None:
    queries = build_queries(
        "oecd",
        [
            {
                "agency": "OECD.SDD.STES",
                "dataflow": "DSD_KEI@DF_KEI",
                "dimensions": {"REF_AREA": ["FRA"]},
                "format": "csv_labels",
                "description": "ignored",
            }
        ],
    )
    assert len(queries) == 1
    query = queries[0]
    assert isinstance(query, OECDQueryRequest)
    assert query.format is OECDResponseFormat.CSV_LABELS
    assert "description" not in query.to_dict()


def test_build_queries_eurostat_uses_v30_request() -> None:
    queries = build_queries("eurostat", [{"dataflow": "NAMA_10_GDP"}])
    assert isinstance(queries[0], EurostatQueryRequestV30)


def test_build_queries_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unknown provider 'nope'"):
        build_queries("nope", [])


# ──────────────────────────────────────────────────────────────────────
# filter_codes
# ──────────────────────────────────────────────────────────────────────

CODES = ["00", "27", "2710", "TOTAL", "2710XX", "271012", "27"]


def test_filter_codes_without_criteria_dedups_and_sorts() -> None:
    assert filter_codes(CODES) == ["00", "27", "2710", "271012", "2710XX", "TOTAL"]


def test_filter_codes_include_list_warns_about_absent_codes(caplog) -> None:
    with caplog.at_level("WARNING"):
        assert filter_codes(CODES, include=["27", "99"]) == ["27"]
    assert "absent from the codelist" in caplog.text


def test_filter_codes_regex_and_exclusions_apply_in_order() -> None:
    result = filter_codes(
        CODES,
        include_regex=r"\d+",
        exclude=["00"],
        exclude_regex=r"\d{6}",
    )
    assert result == ["27", "2710"]


def test_filter_codes_regex_must_match_fully() -> None:
    assert filter_codes(["27", "2710"], include_regex=r"\d{2}") == ["27"]
