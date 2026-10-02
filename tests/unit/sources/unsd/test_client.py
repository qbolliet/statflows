"""Tests — :class:`statflows.sources.unsd.client.UNSDClient` and its query DTO.

Frozen behaviour: catalogue lookups (only published downward pairs), percent-encoded
URLs, download of the raw workbook with its extension, correspondence retrieval
with conversion validation, and the query request's identity. No network and no
Excel engine: the HTTP call and the workbook parsing are replaced.
"""

from __future__ import annotations

from unittest import mock

import pandas as pd
import pytest

from statflows.sources.unsd import client as client_module
from statflows.sources.unsd.client import UNSDClient
from statflows.sources.unsd.formats import UNSDResponseFormat
from statflows.sources.unsd.queries import UNSDCorrespondenceRequest

PAIR = ("HS2022", "HS2017")
FILENAME = "HS2022toHS2017ConversionAndCorrelationTables.xlsx"


@pytest.fixture
def client() -> UNSDClient:
    return UNSDClient()


def _table(*sources: str) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "source_classification": "HS2022",
            "source_code": list(sources),
            "target_classification": "HS2017",
            "target_code": list(sources),
            "relationship": pd.NA,
        }
    )


# ──────────────────────────────────────────────────────────────────────
# Catalogue
# ──────────────────────────────────────────────────────────────────────


def test_table_filename_and_url(client) -> None:
    assert client._table_filename(*PAIR) == FILENAME
    assert client._table_url("HS2012", "HS2007") == (
        f"{client.base_url}/HS%202012%20to%20HS%202007%20Correlation%20and%20conversion%20tables.xls"
    )


def test_unpublished_pair_fails_early_and_lists_available_pairs(client) -> None:
    with pytest.raises(KeyError, match="No correspondence table published"):
        client._table_filename("HS2017", "HS2022")  # montante : non publiée


def test_list_available_tables_shape_and_order(client) -> None:
    tables = client.list_available_tables()

    assert list(tables.columns) == [
        "source_classification",
        "target_classification",
        "filename",
        "extension",
        "url",
    ]
    assert tables["source_classification"].is_monotonic_decreasing
    row = tables[tables["filename"] == FILENAME].iloc[0]
    assert row["extension"] == ".xlsx"
    assert row["url"] == f"{client.base_url}/{FILENAME}"
    assert set(tables["extension"]) <= {".xls", ".xlsx"}


# ──────────────────────────────────────────────────────────────────────
# Téléchargement
# ──────────────────────────────────────────────────────────────────────


def test_download_raw_returns_bytes_and_extension(client) -> None:
    client.get = mock.Mock(return_value=mock.Mock(content=b"PK"))  # type: ignore[method-assign]
    assert client.download_raw(*PAIR) == (b"PK", ".xlsx")
    client.get.assert_called_once_with(FILENAME)


def test_download_raw_percent_encodes_the_file_name(client) -> None:
    client.get = mock.Mock(return_value=mock.Mock(content=b""))  # type: ignore[method-assign]
    _, extension = client.download_raw("HS2012", "HS2007")
    assert extension == ".xls"
    assert "%20" in client.get.call_args.args[0]


# ──────────────────────────────────────────────────────────────────────
# get_correspondence
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def parsed(client, monkeypatch):
    client.download_raw = mock.Mock(return_value=(b"bytes", ".xlsx"))  # type: ignore[method-assign]
    parse = mock.Mock(return_value=_table("010121", "010129"))
    validate = mock.Mock()
    monkeypatch.setattr(client_module.parsing, "parse_correspondence", parse)
    monkeypatch.setattr(client_module.parsing, "validate_conversion", validate)
    return parse, validate


def test_conversion_is_parsed_then_validated(client, parsed) -> None:
    parse, validate = parsed
    df = client.get_correspondence(*PAIR)

    assert len(df) == 2
    parse.assert_called_once_with(
        content=b"bytes",
        extension=".xlsx",
        source="HS2022",
        target="HS2017",
        kind="conversion",
        filename=FILENAME,
    )
    validate.assert_called_once()


def test_correlation_is_not_validated_as_a_function(client, parsed) -> None:
    parse, validate = parsed
    client.get_correspondence(*PAIR, kind="correlation")
    assert parse.call_args.kwargs["kind"] == "correlation"
    validate.assert_not_called()


def test_unknown_kind_is_rejected_before_any_download(client, parsed) -> None:
    with pytest.raises(ValueError, match="Unsupported correspondence kind 'other'"):
        client.get_correspondence(*PAIR, kind="other")
    client.download_raw.assert_not_called()


def test_execute_query_delegates_to_get_correspondence(client, parsed) -> None:
    parse, _ = parsed
    query = UNSDCorrespondenceRequest("HS2022", "HS2017", kind="correlation")
    client.execute_query(query)
    assert parse.call_args.kwargs["kind"] == "correlation"


# ──────────────────────────────────────────────────────────────────────
# UNSDCorrespondenceRequest
# ──────────────────────────────────────────────────────────────────────


def test_request_derived_properties() -> None:
    query = UNSDCorrespondenceRequest("HS2022", "HS2017")
    assert query.agency == "UNSD"
    assert query.dataflow == "HS2022-HS2017"
    assert query.get_dataflow_key() == "HS2022-HS2017::*"


def test_request_identity_key_ignores_presentation_fields() -> None:
    query = UNSDCorrespondenceRequest("HS2022", "HS2017")
    assert query.identity_key() == (
        "UNSD::HS2022-HS2017::*::kind=conversion;source=HS2022;target=HS2017"
    )
    other_format = UNSDCorrespondenceRequest(
        "HS2022", "HS2017", format=UNSDResponseFormat.XLS
    )
    assert other_format.identity_key() == query.identity_key()
    assert UNSDCorrespondenceRequest(
        "HS2022", "HS2017", kind="correlation"
    ).identity_key() != (query.identity_key())


def test_request_from_dict_coerces_format_and_ignores_unknown_keys() -> None:
    query = UNSDCorrespondenceRequest.from_dict(
        {"source": "HS2022", "target": "HS2017", "format": "xls", "description": "x"}
    )
    assert query.format is UNSDResponseFormat.XLS
