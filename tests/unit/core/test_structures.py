"""Tests — :mod:`statflows.core.structures`.

Frozen behaviour: name ↔ position lookups (first dimension at position 0 stays
reachable, lookup falls back to lowercase), dict / JSON round trips, and
resolution of dimension names to positions with explicit error messages.
"""

from __future__ import annotations

import json

import pytest

from statflows.core.structures import (
    DataflowStructure,
    DataflowStructureRegistry,
    DimensionInfo,
)


def _structure(agency: str = "AG", dataflow: str = "DF") -> DataflowStructure:
    return DataflowStructure(
        agency=agency,
        dataflow=dataflow,
        num_dimensions=3,
        dimensions=[
            DimensionInfo("REF_AREA", 0, "Reference area", "CL_AREA"),
            DimensionInfo("freq", 1),
            DimensionInfo("PRODUCT", 2, "Product"),
        ],
        description="demo",
    )


# ──────────────────────────────────────────────────────────────────────
# DimensionInfo / DataflowStructure
# ──────────────────────────────────────────────────────────────────────


def test_dimension_info_to_dict_omits_empty_optional_fields() -> None:
    assert DimensionInfo("A", 0).to_dict() == {"name": "A", "position": 0}
    assert DimensionInfo("A", 0, "desc", "CL").to_dict() == {
        "name": "A",
        "position": 0,
        "description": "desc",
        "codelist": "CL",
    }


def test_dimension_info_round_trip() -> None:
    dim = DimensionInfo("A", 4, "desc", "CL")
    restored = DimensionInfo.from_dict(dim.to_dict())
    assert restored.to_dict() == dim.to_dict()


def test_get_position_keeps_position_zero_and_falls_back_to_lowercase() -> None:
    structure = _structure()
    assert structure.get_position("REF_AREA") == 0
    assert structure.get_position("FREQ") == 1  # repli sur la casse minuscule
    assert structure.get_position("unknown") is None


def test_get_name_and_key() -> None:
    structure = _structure()
    assert structure.get_name(2) == "PRODUCT"
    assert structure.get_name(9) is None
    assert structure.get_key() == "AG::DF"


def test_structure_round_trip() -> None:
    structure = _structure()
    restored = DataflowStructure.from_dict(structure.to_dict())
    assert restored.to_dict() == structure.to_dict()
    assert restored.get_position("PRODUCT") == 2


# ──────────────────────────────────────────────────────────────────────
# DataflowStructureRegistry
# ──────────────────────────────────────────────────────────────────────


def test_registry_register_get_has_and_list() -> None:
    registry = DataflowStructureRegistry()
    registry.register(_structure())
    assert registry.has("AG", "DF")
    assert not registry.has("AG", "OTHER")
    assert registry.get("AG", "DF") is not None
    assert registry.get("AG", "OTHER") is None
    assert registry.list_structures() == ["AG::DF"]


def test_registry_loads_from_dict() -> None:
    payload = {"STRUCTURES": [_structure().to_dict()]}
    registry = DataflowStructureRegistry(structures_dict=payload)
    assert registry.to_dict() == payload


def test_registry_file_round_trip_and_file_overrides_dict(tmp_path) -> None:
    path = tmp_path / "structures.json"
    registry = DataflowStructureRegistry()
    registry.register(_structure())
    registry.save_to_file(path)

    other = _structure().to_dict()
    other["description"] = "from dict"
    reloaded = DataflowStructureRegistry(
        structures_dict={"STRUCTURES": [other]}, config_path=path
    )
    structure = reloaded.get("AG", "DF")
    assert structure is not None
    assert structure.description == "demo"


def test_registry_load_from_file_accepts_lowercase_key(tmp_path) -> None:
    path = tmp_path / "structures.json"
    path.write_text(json.dumps({"structures": [_structure().to_dict()]}))
    registry = DataflowStructureRegistry()
    registry.load_from_file(path)
    assert registry.has("AG", "DF")


def test_registry_load_from_file_missing_file(tmp_path) -> None:
    with pytest.raises(FileNotFoundError):
        DataflowStructureRegistry(config_path=tmp_path / "absent.json")


def test_get_num_dimensions() -> None:
    registry = DataflowStructureRegistry()
    registry.register(_structure())
    assert registry.get_num_dimensions("AG", "DF") == 3
    assert registry.get_num_dimensions("AG", "OTHER") is None


# ──────────────────────────────────────────────────────────────────────
# resolve_dimensions
# ──────────────────────────────────────────────────────────────────────


def test_resolve_dimensions_mixes_names_and_positions() -> None:
    registry = DataflowStructureRegistry()
    registry.register(_structure())
    resolved = registry.resolve_dimensions(
        "AG", "DF", {"REF_AREA": "FRA", 2: ["P1", "P2"], "FREQ": ("M",)}
    )
    assert resolved == {0: ["FRA"], 2: ["P1", "P2"], 1: ["M"]}


def test_resolve_dimensions_positions_do_not_need_a_structure() -> None:
    registry = DataflowStructureRegistry()
    assert registry.resolve_dimensions("AG", "DF", {0: "FRA"}) == {0: ["FRA"]}


def test_resolve_dimensions_name_without_structure() -> None:
    registry = DataflowStructureRegistry()
    with pytest.raises(ValueError, match="Structure not found for AG::DF"):
        registry.resolve_dimensions("AG", "DF", {"REF_AREA": "FRA"})


def test_resolve_dimensions_unknown_name_lists_available_ones() -> None:
    registry = DataflowStructureRegistry()
    registry.register(_structure())
    with pytest.raises(ValueError, match="Available dimensions: .*REF_AREA"):
        registry.resolve_dimensions("AG", "DF", {"nope": "x"})
