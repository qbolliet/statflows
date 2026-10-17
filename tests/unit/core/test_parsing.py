"""Tests — :mod:`statflows.core.parsing` and the CSV response parser.

Frozen behaviour: dimension codes survive parsing verbatim (``"NA"`` for
Namibia is never a missing value, product codes keep their leading zeros),
only an empty field or ``":"`` is missing, measures stay numeric, annual
periods keep their numeric type, and a null key column raises an explicit
``ValueError`` while null measures are allowed.
"""

from __future__ import annotations

import pandas as pd
import pytest

from statflows.core.client import AbstractSDMXClient
from statflows.core.parsing import (
    check_no_null_keys,
    read_codes_csv,
    resolve_key_columns,
)
from statflows.core.structures import DataflowStructure, DimensionInfo

# Réponse SDMX-CSV de type Comext : Namibie (NA), Norvège (NO), Pays-Bas (NL)
COMEXT_CSV = (
    "DATAFLOW,LAST UPDATE,freq,reporter,partner,product,flow,indicators,"
    "TIME_PERIOD,OBS_VALUE,OBS_FLAG\n"
    "ESTAT:DS-045409(1.0),01/10/26,A,BE,NA,01012100,1,VALUE_IN_EUROS,2020,12.5,\n"
    "ESTAT:DS-045409(1.0),01/10/26,A,BE,NO,01012100,1,VALUE_IN_EUROS,2020,:,\n"
    "ESTAT:DS-045409(1.0),01/10/26,A,BE,NL,01012100,2,VALUE_IN_EUROS,2020,,c\n"
)
DIMENSIONS = ("freq", "reporter", "partner", "product", "flow", "indicators")


def _comext_structure() -> DataflowStructure:
    return DataflowStructure(
        "ESTAT",
        "DS-045409",
        len(DIMENSIONS),
        [DimensionInfo(name, i) for i, name in enumerate(DIMENSIONS)],
    )


# ──────────────────────────────────────────────────────────────────────
# Lecture des codes
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "parse", [read_codes_csv, AbstractSDMXClient._parse_csv_response]
)
def test_namibia_code_is_not_a_missing_value(parse) -> None:
    df = parse(COMEXT_CSV)
    assert df["partner"].tolist() == ["NA", "NO", "NL"]
    assert not df["partner"].isna().any()


@pytest.mark.parametrize(
    "token", ["NA", "N/A", "NULL", "nan", "NaN", "None", "null", "#N/A"]
)
def test_default_pandas_missing_markers_are_kept_as_codes(token) -> None:
    df = read_codes_csv(f"partner,OBS_VALUE\n{token},1\n")
    assert df.loc[0, "partner"] == token


def test_dimension_columns_are_text_and_keep_leading_zeros() -> None:
    df = AbstractSDMXClient._parse_csv_response(COMEXT_CSV)
    assert df["product"].tolist() == ["01012100"] * 3
    # Flux lus comme codes, pas comme entiers
    assert df["flow"].tolist() == ["1", "1", "2"]
    for column in DIMENSIONS:
        assert pd.api.types.is_string_dtype(df[column]), column


def test_missing_observations_stay_missing_and_values_numeric() -> None:
    df = AbstractSDMXClient._parse_csv_response(COMEXT_CSV)
    assert df["OBS_VALUE"].dtype == "float64"
    assert df.loc[0, "OBS_VALUE"] == 12.5
    # « : » et champ vide : observations manquantes
    assert df["OBS_VALUE"].isna().tolist() == [False, True, True]
    # Attribut vide : manquant, attribut renseigné : conservé
    assert df["OBS_FLAG"].isna().tolist() == [True, True, False]
    assert df.loc[2, "OBS_FLAG"] == "c"


def test_integer_measures_and_annual_periods_keep_their_numeric_type() -> None:
    df = read_codes_csv("geo,TIME_PERIOD,OBS_VALUE\nFR,2020,1\nDE,2021,2\n")
    assert df["OBS_VALUE"].tolist() == [1, 2]
    assert pd.api.types.is_integer_dtype(df["OBS_VALUE"])
    # Périodes annuelles numériques, comme avec l'inférence par défaut de pandas
    assert df["TIME_PERIOD"].tolist() == [2020, 2021]


def test_sub_annual_periods_stay_text() -> None:
    df = read_codes_csv("geo,TIME_PERIOD,OBS_VALUE\nFR,2020-01,1\nFR,2020-Q2,2\n")
    assert df["TIME_PERIOD"].tolist() == ["2020-01", "2020-Q2"]


def test_unparseable_measure_is_coerced_to_nan() -> None:
    df = read_codes_csv("geo,OBS_VALUE\nFR,12.3 p\nDE,4\n")
    assert df["OBS_VALUE"].isna().tolist() == [True, False]


def test_empty_response_still_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Failed to parse CSV response"):
        AbstractSDMXClient._parse_csv_response("")


# ──────────────────────────────────────────────────────────────────────
# Colonnes de clé et garde-fou
# ──────────────────────────────────────────────────────────────────────


def test_resolve_key_columns_from_structure_excludes_labels() -> None:
    structure = DataflowStructure(
        "ESTAT",
        "DF",
        3,
        [
            DimensionInfo("GEO", 0),
            DimensionInfo("geo_label", 1, attribute_of="GEO"),
            DimensionInfo("freq", 2),
        ],
    )
    columns = ["freq", "geo", "geo_label", "TIME_PERIOD", "OBS_VALUE"]
    assert resolve_key_columns(structure, columns) == ["geo", "freq", "TIME_PERIOD"]


def test_guard_accepts_namibia_and_null_measures() -> None:
    df = AbstractSDMXClient._parse_csv_response(COMEXT_CSV)
    keys = resolve_key_columns(_comext_structure(), list(df.columns))
    assert keys == [*DIMENSIONS, "TIME_PERIOD"]
    # Aucune erreur : NA est un code, et OBS_VALUE nul n'est pas une clé
    check_no_null_keys(df, keys)


def test_guard_names_the_null_key_column() -> None:
    text = COMEXT_CSV.replace(",BE,NO,", ",BE,,")
    df = AbstractSDMXClient._parse_csv_response(text)
    keys = resolve_key_columns(_comext_structure(), list(df.columns))
    with pytest.raises(
        ValueError, match=r"DS-045409: Key column 'partner' has 1 null value\(s\)"
    ):
        check_no_null_keys(df, keys, label="DS-045409")


def test_guard_ignores_columns_absent_from_the_frame() -> None:
    check_no_null_keys(pd.DataFrame({"geo": ["FR"]}), ["geo", "missing"])
