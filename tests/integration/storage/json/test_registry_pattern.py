"""Tests d'intégration — convention « racine nommée + fusion » des scripts.

Les scripts du pipeline composent ``Loader`` / ``Saver`` selon un motif fixe
(cf. :mod:`tests.utils.registries`) : lecture des entrées sous une racine nommée,
fusion incrémentale des seules clés fournies, forme persistée ``{root: {...}}``.
Ce contrat est vérifié en local et sur S3 (moto).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from statflows.storage.json import Loader
from tests.utils.registries import merge_named_registry, read_named_registry

ROOT = "DOWNLOADS"


# ──────────────────────────────────────────────────────────────────────
# Local
# ──────────────────────────────────────────────────────────────────────


def test_read_missing_file_returns_empty_dict(tmp_path: Path) -> None:
    assert read_named_registry(tmp_path / "absent.json", root=ROOT) == {}


def test_read_missing_root_returns_empty_dict(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    merge_named_registry(path, {}, root="AUTRE")  # crée le fichier sous une autre racine

    assert read_named_registry(path, root=ROOT) == {}


def test_merge_persists_shape_and_merge_semantics(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"
    merge_named_registry(path, {"k1": {"v": 1}, "k2": {"v": 2}}, root=ROOT)

    # Seule k2 est fournie : k1 préservée, k2 écrasée
    merge_named_registry(path, {"k2": {"v": 99}}, root=ROOT)

    raw = Loader().load(path)
    assert set(raw) == {ROOT}
    assert raw[ROOT] == {"k1": {"v": 1}, "k2": {"v": 99}}


def test_merge_on_absent_file_creates_it(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    merge_named_registry(path, {"k1": {"v": 1}}, root=ROOT)

    assert Loader().load(path) == {ROOT: {"k1": {"v": 1}}}


# ──────────────────────────────────────────────────────────────────────
# S3 (moto)
# ──────────────────────────────────────────────────────────────────────


def test_read_and_merge_s3(s3_bucket: str) -> None:
    key = Path("reg/state.json")

    assert read_named_registry(key, s3_bucket, root=ROOT) == {}

    merge_named_registry(key, {"k1": {"v": 1}}, s3_bucket, root=ROOT)
    merge_named_registry(key, {"k2": {"v": 2}}, s3_bucket, root=ROOT)

    assert read_named_registry(key, s3_bucket, root=ROOT) == {
        "k1": {"v": 1},
        "k2": {"v": 2},
    }
