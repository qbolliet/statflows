"""Unit tests — :mod:`statflows.core.registry` (dates registry, local storage).

Frozen behaviour: identical reading of a single-file registry and of a sharded
one, "most recent date" merge of both formats, migration to shards, rewriting of
the modified shards only. The module depends on no extra (importable without
duckdb).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from statflows import RegistryEntry, iter_registry_entries
from statflows.core.registry import (
    DEFAULT_SHARD,
    DownloadRegistry,
    fragment_dir,
    sanitize_shard,
    shard_path,
)
from statflows.storage.json import Loader, Saver

UTC = UTC


def _raw(dataflow: str, day: int, reporter: str = "FR") -> dict[str, Any]:
    """Raw registry entry dated the ``day`` of January 2026."""
    return {
        "agency": "ESTAT",
        "dataflow": dataflow,
        "params": {"dataflow": dataflow, "dimensions": {"geo": reporter}},
        "last_download": datetime(2026, 1, day, tzinfo=UTC).isoformat(),
    }


ENTRIES = {
    "ESTAT:A:FR": _raw("A", 1, "FR"),
    "ESTAT:A:DE": _raw("A", 2, "DE"),
    "ESTAT:B:FR": _raw("B", 3, "FR"),
}


def _save(path: Path, entries: dict[str, Any]) -> None:
    Saver().save(path, {"DOWNLOADS": entries}, indent=2)


def _registry(path: Path, *, sharded: bool) -> DownloadRegistry:
    saver = Saver()
    registry = DownloadRegistry(path, loader=Loader(), saver=saver, sharded=sharded)
    registry.load()
    return registry


def _spy(registry: DownloadRegistry) -> list[str]:
    """Spy on the registry writes; return the names of the written files."""
    written: list[str] = []
    original = registry._saver.save

    def spy(filepath, *args, **kwargs):
        written.append(Path(filepath).name)
        return original(filepath, *args, **kwargs)

    registry._saver.save = spy  # type: ignore[method-assign]
    return written


# ──────────────────────────────────────────────────────────────────────
# Helpers de chemin et entrée typée
# ──────────────────────────────────────────────────────────────────────


def test_fragment_paths() -> None:
    assert fragment_dir("reg/last.json") == Path("reg/last")
    assert shard_path("reg/last.json", "A") == Path("reg/last/A.json")


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("DSD_KEI@DF_KEI", "DSD_KEI_DF_KEI"),
        ("a/b c", "a_b_c"),
        (".hidden", "hidden"),
        ("", DEFAULT_SHARD),
    ],
)
def test_sanitize_shard(name: str, expected: str) -> None:
    assert sanitize_shard(name) == expected


def test_registry_entry_roundtrip_and_hash() -> None:
    entry = RegistryEntry.from_raw("ESTAT:A:FR", ENTRIES["ESTAT:A:FR"])

    assert entry.last_download == datetime(2026, 1, 1, tzinfo=UTC)
    assert entry.to_raw() == ENTRIES["ESTAT:A:FR"]
    # Hachable malgré ``params`` (exclu du hachage) ; gelée
    assert hash(entry) == hash(
        RegistryEntry.from_raw("ESTAT:A:FR", ENTRIES["ESTAT:A:FR"])
    )
    with pytest.raises(AttributeError):
        entry.agency = "X"  # type: ignore[misc]


def test_registry_entry_without_valid_date_is_none() -> None:
    assert RegistryEntry.from_raw("k", {"last_download": "not-a-date"}) is None
    assert RegistryEntry.from_raw("k", {}) is None


# ──────────────────────────────────────────────────────────────────────
# Lecture : fichier unique, fragments, fusion
# ──────────────────────────────────────────────────────────────────────


def test_iter_entries_identical_from_single_file_and_fragments(tmp_path: Path) -> None:
    single = tmp_path / "single" / "last.json"
    _save(single, ENTRIES)
    sharded = tmp_path / "sharded" / "last.json"
    _save(
        shard_path(sharded, "A"),
        {k: v for k, v in ENTRIES.items() if v["dataflow"] == "A"},
    )
    _save(
        shard_path(sharded, "B"),
        {k: v for k, v in ENTRIES.items() if v["dataflow"] == "B"},
    )

    from_single = list(iter_registry_entries(single))
    from_fragments = list(iter_registry_entries(sharded))

    assert from_single == from_fragments
    assert [e.identity_key for e in from_single] == sorted(ENTRIES)


def test_iter_entries_merge_keeps_most_recent_date(tmp_path: Path) -> None:
    path = tmp_path / "last.json"
    # Fichier historique plus récent pour FR, fragment plus récent pour DE
    _save(path, {"ESTAT:A:FR": _raw("A", 9, "FR"), "ESTAT:A:DE": _raw("A", 2, "DE")})
    _save(
        shard_path(path, "A"),
        {"ESTAT:A:FR": _raw("A", 1, "FR"), "ESTAT:A:DE": _raw("A", 5, "DE")},
    )

    dates = {e.identity_key: e.last_download.day for e in iter_registry_entries(path)}

    assert dates == {"ESTAT:A:FR": 9, "ESTAT:A:DE": 5}


def test_iter_entries_skips_unreadable_fragment_and_invalid_dates(
    tmp_path: Path,
) -> None:
    path = tmp_path / "last.json"
    _save(
        shard_path(path, "A"),
        {"ESTAT:A:FR": _raw("A", 1), "bad": {"last_download": "?"}},
    )
    shard_path(path, "B").write_text("{not json", encoding="utf-8")
    # Temporaire d'une écriture atomique interrompue : jamais lu
    (fragment_dir(path) / ".tmp-123.json").write_text("{}", encoding="utf-8")

    assert [e.identity_key for e in iter_registry_entries(path)] == ["ESTAT:A:FR"]


def test_iter_entries_missing_registry_is_empty(tmp_path: Path) -> None:
    assert list(iter_registry_entries(tmp_path / "absent.json")) == []


# ──────────────────────────────────────────────────────────────────────
# Écriture : fichier unique, fragments, migration
# ──────────────────────────────────────────────────────────────────────


def test_single_file_flush_rewrites_whole_file(tmp_path: Path) -> None:
    path = tmp_path / "last.json"
    registry = _registry(path, sharded=False)
    registry.commit("ESTAT:A:FR", ENTRIES["ESTAT:A:FR"])

    assert registry.flush() == 1
    assert Loader().load(path) == {"DOWNLOADS": {"ESTAT:A:FR": ENTRIES["ESTAT:A:FR"]}}
    assert not fragment_dir(path).exists()


def test_sharded_flush_rewrites_only_dirty_fragments(tmp_path: Path) -> None:
    path = tmp_path / "last.json"
    registry = _registry(path, sharded=True)
    for key, raw in ENTRIES.items():
        registry.commit(key, raw, raw["dataflow"])
    assert registry.flush() == 2

    written = _spy(registry)
    registry.commit("ESTAT:B:FR", _raw("B", 7), "B")
    assert registry.flush() == 1
    assert written == ["B.json"]
    # Rien de modifié : aucune écriture
    assert registry.flush() == 0
    assert {
        e.identity_key: e.last_download.day for e in iter_registry_entries(path)
    } == {
        "ESTAT:A:FR": 1,
        "ESTAT:A:DE": 2,
        "ESTAT:B:FR": 7,
    }


def test_moving_an_entry_rewrites_both_fragments(tmp_path: Path) -> None:
    path = tmp_path / "last.json"
    registry = _registry(path, sharded=True)
    registry.commit("k", _raw("A", 1), "A")
    registry.flush()

    written = _spy(registry)
    registry.commit("k", _raw("A", 2), "Z")
    registry.flush()

    assert sorted(written) == ["A.json", "Z.json"]
    assert Loader().load(shard_path(path, "A")) == {"DOWNLOADS": {}}


def test_migration_from_single_file_to_fragments(tmp_path: Path) -> None:
    path = tmp_path / "last.json"
    _save(path, ENTRIES)
    registry = _registry(path, sharded=True)

    # Seules les requêtes du run ont un fragment connu ; les autres → _default
    registry.assign_shards({"ESTAT:A:FR": "A", "ESTAT:A:DE": "A"})
    written = _spy(registry)
    registry.flush()

    assert sorted(written) == ["A.json", f"{DEFAULT_SHARD}.json"]
    assert Loader().list_json(fragment_dir(path)) == [
        str(shard_path(path, "A")),
        str(shard_path(path, DEFAULT_SHARD)),
    ]
    assert set(Loader().load(shard_path(path, DEFAULT_SHARD))["DOWNLOADS"]) == {
        "ESTAT:B:FR"
    }
    # Fichier historique intact ; lecture inchangée
    assert Loader().load(path) == {"DOWNLOADS": ENTRIES}
    assert {e.identity_key: e.to_raw() for e in iter_registry_entries(path)} == ENTRIES

    # Rechargement : les entrées sont désormais rangées, plus rien à migrer
    reloaded = _registry(path, sharded=True)
    assert reloaded.flush() == 0


def test_migration_without_assignment_uses_default_shard(tmp_path: Path) -> None:
    path = tmp_path / "last.json"
    _save(path, ENTRIES)
    registry = _registry(path, sharded=True)

    assert registry.flush() == 1
    assert set(Loader().load(shard_path(path, DEFAULT_SHARD))["DOWNLOADS"]) == set(
        ENTRIES
    )
