"""Tests — :mod:`statflows.core.factory` (clients, queries, code filtering).

Frozen behaviour: lazy provider dispatch with an explicit error for unknown
providers, JSON specifications turned into query objects (unknown keys ignored,
textual format coerced), and the four-step codelist filter.
"""

from __future__ import annotations

import pytest

from statflows import EurostatClient
from statflows.core.factory import build_client, build_queries, filter_codes
from statflows.sources.eurostat.queries import EurostatQueryRequestV30
from statflows.sources.oecd.client import OECDClient
from statflows.sources.oecd.formats import OECDResponseFormat
from statflows.sources.oecd.queries import OECDQueryRequest

# ──────────────────────────────────────────────────────────────────────
# build_client
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("provider", "client_cls"), [("eurostat", EurostatClient), ("oecd", OECDClient)]
)
def test_build_client_dispatches_on_provider(provider, client_cls) -> None:
    assert isinstance(build_client(provider), client_cls)


def test_build_client_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unknown provider 'nope'"):
        build_client("nope")


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
