"""Tests — :mod:`statflows.sources.oecd.parsing`.

Frozen behaviour: SDMX-JSON observations flattened to one row per observation,
empty responses → empty frame, structure responses parsed in both SDMX v1 and
v2 layouts, unusable payloads → ``ValueError``.
"""

from __future__ import annotations

import pytest

from statflows.sources.oecd.parsing import (
    create_structure_from_api_response,
    parse_json_response,
)

# ──────────────────────────────────────────────────────────────────────
# parse_json_response
# ──────────────────────────────────────────────────────────────────────


def _sdmx_json(observations: dict) -> dict:
    return {
        "structure": {
            "dimensions": {
                "observation": [
                    {"id": "REF_AREA", "values": [{"id": "FRA"}, {"id": "DEU"}]},
                    {"id": "TIME_PERIOD", "values": [{"id": "2020"}, {"id": "2021"}]},
                ]
            }
        },
        "dataSets": [{"observations": observations}],
    }


def test_parse_json_flattens_observations() -> None:
    df = parse_json_response(_sdmx_json({"0:0": [1.5], "1:1": [2.5, "flag"]}))
    assert df.to_dict("records") == [
        {"REF_AREA": "FRA", "TIME_PERIOD": "2020", "value": 1.5},
        {"REF_AREA": "DEU", "TIME_PERIOD": "2021", "value": 2.5},
    ]


def test_parse_json_scalar_observation_is_kept_as_is() -> None:
    df = parse_json_response(_sdmx_json({"0:1": 7}))
    assert df["value"].tolist() == [7]


def test_parse_json_empty_list_observation_keeps_the_list() -> None:
    df = parse_json_response(_sdmx_json({"0:0": []}))
    assert df["value"].tolist() == [[]]


def test_parse_json_reads_payload_nested_under_data() -> None:
    payload = {"data": _sdmx_json({"1:0": [3.0]})}
    df = parse_json_response(payload)
    assert df.to_dict("records") == [
        {"REF_AREA": "DEU", "TIME_PERIOD": "2020", "value": 3.0}
    ]


def test_parse_json_short_key_leaves_trailing_dimensions_out() -> None:
    df = parse_json_response(_sdmx_json({"1": [3.0]}))
    assert df.to_dict("records") == [{"REF_AREA": "DEU", "value": 3.0}]


@pytest.mark.parametrize("payload", [{}, {"dataSets": []}, {"data": {"dataSets": []}}])
def test_parse_json_without_datasets_returns_empty_frame(payload) -> None:
    assert parse_json_response(payload).empty


def test_parse_json_malformed_payload_is_reraised() -> None:
    payload = _sdmx_json({"9:0": [1.0]})  # indice hors bornes
    with pytest.raises(IndexError):
        parse_json_response(payload)


# ──────────────────────────────────────────────────────────────────────
# create_structure_from_api_response
# ──────────────────────────────────────────────────────────────────────


def test_structure_v1_inline_names_sorted_by_position() -> None:
    response = {
        "structure": {
            "dimensions": {
                "observation": [
                    {"id": "TIME_PERIOD", "position": 1, "name": "Time"},
                    {"id": "REF_AREA", "position": 0, "name": "Reference area"},
                ]
            }
        }
    }
    structure = create_structure_from_api_response("OECD", "DF_X", response)
    assert (structure.agency, structure.dataflow, structure.num_dimensions) == (
        "OECD",
        "DF_X",
        2,
    )
    assert [(d.name, d.position, d.description) for d in structure.dimensions] == [
        ("REF_AREA", 0, "Reference area"),
        ("TIME_PERIOD", 1, "Time"),
    ]


def test_structure_dimensions_given_as_list() -> None:
    response = {
        "structure": {
            "dimensions": [{"id": "A", "name": "A"}, {"id": "B", "name": "b"}]
        }
    }
    structure = create_structure_from_api_response("OECD", "DF_X", response)
    # Même nom que l'identifiant → pas de description ; position = rang
    assert [(d.name, d.position, d.description) for d in structure.dimensions] == [
        ("A", 0, None),
        ("B", 1, "b"),
    ]


def test_structure_v2_resolves_names_through_concept_schemes() -> None:
    response = {
        "data": {
            "conceptSchemes": [
                {
                    "concepts": [
                        {"id": "REF_AREA", "names": {"en": "Reference area"}},
                        {"id": "FREQ", "name": "Frequency"},
                        {"id": "NO_NAME"},
                    ]
                }
            ],
            "dataStructures": [
                {
                    "dataStructureComponents": {
                        "dimensionList": {
                            "dimensions": [
                                {
                                    "id": "FREQ",
                                    "position": 1,
                                    "conceptIdentity": "urn:...CS_STES(4.0).FREQ",
                                },
                                {
                                    "id": "REF_AREA",
                                    "keyPosition": 0,
                                    "conceptIdentity": "urn:...CS_STES(4.0).REF_AREA",
                                },
                                {"id": "NO_NAME", "position": 2},
                            ]
                        }
                    }
                }
            ],
        }
    }
    structure = create_structure_from_api_response("OECD", "DF_X", response)
    assert [(d.name, d.position, d.description) for d in structure.dimensions] == [
        ("REF_AREA", 0, "Reference area"),
        ("FREQ", 1, "Frequency"),
        ("NO_NAME", 2, None),
    ]


def test_structure_without_any_dimension_is_empty() -> None:
    structure = create_structure_from_api_response("OECD", "DF_X", {})
    assert structure.num_dimensions == 0


def test_structure_unparsable_response_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Unable to parse structure"):
        create_structure_from_api_response("OECD", "DF_X", {"data": "not a dict"})
