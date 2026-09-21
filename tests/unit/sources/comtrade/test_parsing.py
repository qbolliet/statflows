"""Tests — :func:`statflows.sources.comtrade.parsing.parse_availability_last_released`.

Comportement figé : une ligne par (reporter, période) en entrée, une date par
période en sortie (la plus récente tous reporters confondus), dates absentes
ou invalides tolérées, entrée vide ou incomplète → dictionnaire vide.
"""

from __future__ import annotations

import pandas as pd
import pytest

from statflows.sources.comtrade.parsing import parse_availability_last_released


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
            "lastReleased": [None, "2026-02-06T10:18:40.23", float("nan"), None, "not a date"],
        }
    )
    assert parse_availability_last_released(rows) == {
        "2022": "2026-02-06T10:18:40.23",
        "2021": None,
        "2020": None,
    }
