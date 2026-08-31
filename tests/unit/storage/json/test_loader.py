"""Tests de caractérisation — :class:`statflows.storage.json.Loader` (mode local).

Comportement figé : extension non ``.json`` → ``ValueError``, aller-retour local,
acceptation d'un ``Path``, lecture tolérante (``missing_ok``). Le mode S3 est
couvert par ``tests/integration/storage/json``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from statflows.storage.json import Loader, Saver


# ──────────────────────────────────────────────────────────────────────
# Extension non supportée
# ──────────────────────────────────────────────────────────────────────


def test_rejects_non_json_extension(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"only '\.json' files are supported\."):
        Loader().load(str(tmp_path / "data.txt"))


# ──────────────────────────────────────────────────────────────────────
# Aller-retour
# ──────────────────────────────────────────────────────────────────────


def test_roundtrip(tmp_path: Path) -> None:
    path = str(tmp_path / "data.json")
    obj = {"k": "v", "nums": [1, 2, 3], "nested": {"x": True}}

    Saver().save(path, obj)

    assert Loader().load(path) == obj


def test_accepts_path_object(tmp_path: Path) -> None:
    path = tmp_path / "data.json"
    obj = {"a": 1, "b": [1, 2, 3]}

    Saver().save(path, obj)

    assert Loader().load(path) == obj


# ──────────────────────────────────────────────────────────────────────
# Lecture tolérante (``missing_ok``)
# ──────────────────────────────────────────────────────────────────────


def test_missing_file_returns_none_with_missing_ok(tmp_path: Path) -> None:
    # Fichier absent → None (pas d'exception) : un premier run n'est pas un cas spécial
    assert Loader().load(tmp_path / "absent.json", missing_ok=True) is None


def test_missing_file_raises_without_missing_ok(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        Loader().load(tmp_path / "absent.json")
