"""Tests — :mod:`statflows.sources.unsd.parsing` and ``formats`` helpers.

Frozen behaviour: sheet selection on a case-insensitive radical, header-row
detection, marker-column rejection, vintage-based column pairing, code and
relationship normalisation, and conversion-table validation.
"""

from __future__ import annotations

import pandas as pd
import pytest

from statflows.sources.unsd import formats
from statflows.sources.unsd.formats import (
    engine_for,
    ensure_engine_available,
    table_key,
    vintage_year,
)
from statflows.sources.unsd.parsing import (
    detect_header_row,
    drop_marker_columns,
    normalise_codes,
    normalise_relationships,
    parse_correspondence,
    read_sheet,
    resolve_columns,
    select_sheet,
    validate_conversion,
)

# ──────────────────────────────────────────────────────────────────────
# formats
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("extension", "engine"),
    [(".xls", "xlrd"), ("xlsx", "openpyxl"), (".XLSX", "openpyxl")],
)
def test_engine_for(extension, engine) -> None:
    assert engine_for(extension) == engine


def test_engine_for_unsupported_extension() -> None:
    with pytest.raises(ValueError, match="Unsupported workbook extension"):
        engine_for(".ods")


def test_ensure_engine_available_names_the_extra_when_missing(monkeypatch) -> None:
    monkeypatch.setattr(formats.importlib.util, "find_spec", lambda name: None)
    with pytest.raises(ImportError, match=r"'openpyxl'.*statflows\[unsd\]"):
        ensure_engine_available(".xlsx")
    with pytest.raises(ImportError, match=r"'xlrd'.*statflows\[unsd\]"):
        ensure_engine_available("xls")


def test_ensure_engine_available_rejects_unsupported_extension() -> None:
    with pytest.raises(ValueError, match="Unsupported workbook extension"):
        ensure_engine_available(".ods")


def test_workbook_readers_fail_early_when_the_engine_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(formats.importlib.util, "find_spec", lambda name: None)
    with pytest.raises(ImportError, match=r"statflows\[unsd\]"):
        read_sheet(b"", ".xlsx", "Sheet")
    with pytest.raises(ImportError, match=r"statflows\[unsd\]"):
        parse_correspondence(b"", ".xls", "HS2012", "HS2007", "conversion", "f")


def test_table_key() -> None:
    assert table_key("HS2022", "HS2017") == "HS2022-HS2017"


@pytest.mark.parametrize(
    ("classification", "year"), [("HS2022", "2022"), ("From HS 2017", "2017")]
)
def test_vintage_year(classification, year) -> None:
    assert vintage_year(classification) == year


def test_vintage_year_without_vintage() -> None:
    with pytest.raises(ValueError, match="Could not read an HS vintage"):
        vintage_year("CPC21")


# ──────────────────────────────────────────────────────────────────────
# select_sheet
# ──────────────────────────────────────────────────────────────────────


def test_select_sheet_is_case_insensitive() -> None:
    sheets = ["Correlation HS2012-HS2007", "CONVERSION HS12-HS07"]
    assert select_sheet(sheets, "conversion", "f.xls") == "CONVERSION HS12-HS07"


def test_select_sheet_unknown_kind() -> None:
    with pytest.raises(ValueError, match="Unsupported correspondence kind"):
        select_sheet(["a"], "other", "f.xls")


def test_select_sheet_no_match() -> None:
    with pytest.raises(ValueError, match="No 'conversion' sheet found"):
        select_sheet(["Correlation"], "conversion", "f.xls")


def test_select_sheet_ambiguous() -> None:
    with pytest.raises(ValueError, match="Ambiguous 'conversion' sheet"):
        select_sheet(["Conversion A", "Conversion B"], "conversion", "f.xls")


# ──────────────────────────────────────────────────────────────────────
# detect_header_row
# ──────────────────────────────────────────────────────────────────────


def test_detect_header_row_skips_title_block() -> None:
    df_raw = pd.DataFrame(
        [
            ["Conversion table HS 2012 to HS 2002", None],
            [None, None],
            ["HS 2012", "HS 2002"],
        ]
    )
    assert detect_header_row(df_raw, "f.xls") == 2


def test_detect_header_row_not_found() -> None:
    with pytest.raises(ValueError, match="No header row found"):
        detect_header_row(pd.DataFrame([["a", "b"], [1, 2]]), "f.xls")


# ──────────────────────────────────────────────────────────────────────
# drop_marker_columns / resolve_columns
# ──────────────────────────────────────────────────────────────────────


def test_drop_marker_empty_and_partial_columns() -> None:
    df_body = pd.DataFrame(
        {
            0: ["010121", "010129"],
            1: ["ex", "ex."],
            2: [None, None],
            3: ["x", "y"],
            4: ["010110", "010111"],
        }
    )
    headers = ["HS 2012", "", "", "Partial", "HS 2007"]
    body, kept = drop_marker_columns(df_body, headers)
    assert kept == ["HS 2012", "HS 2007"]
    assert body.shape == (2, 2)


def test_resolve_columns_with_and_without_relationship() -> None:
    assert resolve_columns(["HS 2012", "HS 2007"], "HS2012", "HS2007", "f") == (
        0,
        1,
        None,
    )
    assert resolve_columns(
        ["From HS 2017", "Relationship", "HS2022"], "HS2022", "HS2017", "f"
    ) == (2, 0, 1)


def test_resolve_columns_missing_vintage() -> None:
    with pytest.raises(ValueError, match="No column found"):
        resolve_columns(["HS 2012", "HS 2007"], "HS2022", "HS2007", "f")


def test_resolve_columns_ambiguous_vintage() -> None:
    with pytest.raises(ValueError, match="Ambiguous columns"):
        resolve_columns(["HS 2012", "HS2012", "HS 2007"], "HS2012", "HS2007", "f")


# ──────────────────────────────────────────────────────────────────────
# Normalisation
# ──────────────────────────────────────────────────────────────────────


def test_normalise_codes_pads_and_strips() -> None:
    assert normalise_codes(pd.Series([" 10121 ", "010129"])).tolist() == [
        "010121",
        "010129",
    ]


def test_normalise_relationships() -> None:
    result = normalise_relationships(pd.Series(["1:1", "'n:n", "n to 1", " N:1 "]))
    assert result.tolist() == ["1:1", "n:n", "n:1", "n:1"]


# ──────────────────────────────────────────────────────────────────────
# parse_correspondence (classeur xlsx synthétique)
# ──────────────────────────────────────────────────────────────────────


def _workbook(tmp_path, sheets: dict[str, list[list]]) -> bytes:
    pytest.importorskip("openpyxl")
    path = tmp_path / "wb.xlsx"
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, rows in sheets.items():
            pd.DataFrame(rows).to_excel(
                writer, sheet_name=name, header=False, index=False
            )
    return path.read_bytes()


def test_parse_correspondence_correlation_sheet(tmp_path) -> None:
    content = _workbook(
        tmp_path,
        {
            "Conversions": [["HS2022", "HS2017"], ["010121", "010121"]],
            "Correlations": [
                ["HS2022", "HS2017", "Relationship"],
                ["10121", "010121", "'1:1"],
                ["010129", "010130", "n to 1"],
                ["010140", None, None],
            ],
        },
    )
    df = parse_correspondence(content, ".xlsx", "HS2022", "HS2017", "correlation", "f")
    assert df["source_code"].tolist() == ["010121", "010129"]
    assert df["target_code"].tolist() == ["010121", "010130"]
    assert df["relationship"].tolist() == ["1:1", "n:1"]
    assert set(df["source_classification"]) == {"HS2022"}


def test_parse_correspondence_conversion_sheet_has_no_relationship(tmp_path) -> None:
    content = _workbook(
        tmp_path,
        {
            "Conversions": [["HS2022", "HS2017"], ["010121", "010122"]],
            "Correlations": [["HS2022", "HS2017"], ["010121", "010122"]],
        },
    )
    df = parse_correspondence(content, ".xlsx", "HS2022", "HS2017", "conversion", "f")
    assert df["relationship"].isna().all()
    assert len(df) == 1


# ──────────────────────────────────────────────────────────────────────
# validate_conversion
# ──────────────────────────────────────────────────────────────────────


def _table(sources, targets) -> pd.DataFrame:
    return pd.DataFrame({"source_code": sources, "target_code": targets})


def test_validate_conversion_accepts_a_function() -> None:
    validate_conversion(_table(["010121", "010129"], ["010121", "010121"]), "f")


def test_validate_conversion_rejects_duplicated_sources() -> None:
    with pytest.raises(ValueError, match="is not a function"):
        validate_conversion(_table(["010121", "010121"], ["010121", "010122"]), "f")


@pytest.mark.parametrize("column", ["source_code", "target_code"])
def test_validate_conversion_rejects_bad_code_length(column) -> None:
    table = _table(["010121", "010129"], ["010121", "010122"])
    table.loc[1, column] = "0101"
    with pytest.raises(ValueError, match=f"'{column}' values"):
        validate_conversion(table, "f")
