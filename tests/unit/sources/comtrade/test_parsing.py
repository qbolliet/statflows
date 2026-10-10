"""Tests — :mod:`statflows.sources.comtrade.parsing`.

Frozen behaviour of ``parse_availability_last_released``: one row per
(reporter, period) as input, one date per period as output (the most recent
across all reporters), missing or invalid dates tolerated, empty or incomplete
input → empty dictionary. Frozen behaviour of ``build_codelist``: rows without a
code are dropped, the root ``parent`` is a missing value, the label column falls
back on ``text`` (or is empty) when the category-specific one is absent.
``extract_codes`` and ``build_structure_from_parameters`` reject unknown input.
"""

from __future__ import annotations

import pandas as pd
import pytest

from statflows.sources.comtrade.parsing import (
    build_codelist,
    build_structure_from_parameters,
    extract_codes,
    parse_availability_last_released,
)

# ──────────────────────────────────────────────────────────────────────
# Entrées vides ou incomplètes
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "availability",
    [
        None,
        pd.DataFrame(),
        pd.DataFrame({"period": [2022]}),
        pd.DataFrame({"lastReleased": ["2026-02-06T10:18:40.23"]}),
    ],
)
def test_empty_or_incomplete_input_returns_empty_mapping(availability) -> None:
    assert parse_availability_last_released(availability) == {}


# ──────────────────────────────────────────────────────────────────────
# Agrégation sur les reporters
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("order", [[0, 1, 2], [2, 1, 0], [1, 2, 0]])
def test_keeps_most_recent_date_across_reporters(order) -> None:
    # Plusieurs reporters pour la même période, quel que soit l'ordre des lignes
    rows = pd.DataFrame(
        {
            "period": [2022, 2022, 2022],
            "reporterCode": [251, 276, 380],
            "lastReleased": [
                "2025-08-14T08:57:07.4733333",
                "2026-02-06T10:18:40.23",
                "2024-01-01T00:00:00",
            ],
        }
    ).iloc[order]
    assert parse_availability_last_released(rows) == {"2022": "2026-02-06T10:18:40.23"}


def test_compares_parsed_dates_not_raw_strings() -> None:
    # Précisions différentes : la comparaison porte sur les instants
    rows = pd.DataFrame(
        {
            "period": ["2022", "2022"],
            "lastReleased": ["2026-02-06T10:18:40.5", "2026-02-06T10:18:40.4999999"],
        }
    )
    assert parse_availability_last_released(rows) == {"2022": "2026-02-06T10:18:40.5"}


def test_periods_are_kept_separate() -> None:
    rows = pd.DataFrame(
        {
            "period": [2021, 2022, 2021],
            "lastReleased": [
                "2022-11-16T20:57:52.18",
                "2026-02-06T10:18:40.23",
                "2026-06-23T23:45:52.11",
            ],
        }
    )
    assert parse_availability_last_released(rows) == {
        "2021": "2026-06-23T23:45:52.11",
        "2022": "2026-02-06T10:18:40.23",
    }


# ──────────────────────────────────────────────────────────────────────
# Dates absentes ou invalides
# ──────────────────────────────────────────────────────────────────────


def test_missing_or_invalid_dates_are_ignored() -> None:
    rows = pd.DataFrame(
        {
            "period": [2022, 2022, 2022, 2021, 2020],
            "lastReleased": [
                None,
                "2026-02-06T10:18:40.23",
                float("nan"),
                None,
                "not a date",
            ],
        }
    )
    assert parse_availability_last_released(rows) == {
        "2022": "2026-02-06T10:18:40.23",
        "2021": None,
        "2020": None,
    }


# ──────────────────────────────────────────────────────────────────────
# build_codelist
# ──────────────────────────────────────────────────────────────────────


def test_build_codelist_drops_rows_without_code() -> None:
    # Un code manquant est écarté : il ne devient pas le code littéral "None"/"nan"
    meta = pd.DataFrame({"id": ["01", None, "0101"], "text": ["A", "B", "C"]})
    codelist = build_codelist(meta, "cmd:HS")
    assert codelist["code"].tolist() == ["01", "0101"]
    assert codelist["label"].tolist() == ["A", "C"]


def test_build_codelist_parent_is_missing_at_the_root() -> None:
    # Colonne texte par défaut : la racine est un manquant (NaN), pas None
    meta = pd.DataFrame(
        {"id": ["TOTAL", "01"], "text": ["All", "Animals"], "parent": [None, "TOTAL"]}
    )
    parent = build_codelist(meta, "cmd:HS")["parent"]
    assert parent.dtype == "str"
    assert pd.isna(parent.iloc[0])
    assert parent.iloc[1] == "TOTAL"


def test_build_codelist_label_falls_back_on_text_column() -> None:
    # Pas de « reporterDesc » : le libellé est lu dans la colonne « text »
    meta = pd.DataFrame(
        {
            "reporterCode": [251, 276],
            "text": ["France", "Germany"],
            "entryExpiredDate": [None, None],
        }
    )
    assert build_codelist(meta, "reporter")["label"].tolist() == ["France", "Germany"]


def test_build_codelist_label_is_empty_without_any_label_column() -> None:
    meta = pd.DataFrame({"reporterCode": [251], "entryExpiredDate": [None]})
    codelist = build_codelist(meta, "reporter")
    assert codelist["code"].tolist() == ["251"]
    assert codelist["label"].isna().all()


# ──────────────────────────────────────────────────────────────────────
# extract_codes et build_structure_from_parameters
# ──────────────────────────────────────────────────────────────────────


def test_extract_codes_partner_drops_expired_entries() -> None:
    meta = pd.DataFrame(
        {
            "PartnerCode": [0, 251, 999],
            "entryExpiredDate": [None, None, "2010-01-01"],
        }
    )
    assert extract_codes(meta, "partner") == [0, 251]


def test_extract_codes_rejects_unknown_category() -> None:
    with pytest.raises(ValueError, match="Unsupported category 'unknown'"):
        extract_codes(pd.DataFrame({"id": ["M"]}), "unknown")


def test_build_structure_from_parameters_rejects_unknown_dataflow() -> None:
    with pytest.raises(ValueError, match="No structure declared for COMTRADE::NOPE"):
        build_structure_from_parameters("COMTRADE", "NOPE")
