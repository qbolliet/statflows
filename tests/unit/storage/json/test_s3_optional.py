"""L'extra « s3 » est optionnel pour :mod:`statflows.storage.json`.

``Loader`` / ``Saver`` s'importent et fonctionnent en local sans ``boto3`` /
``s3fs`` ; seul un appel avec ``bucket=...`` exige l'extra et, à défaut, lève une
``ImportError`` explicite. L'absence des paquets est simulée en plaçant ``None``
dans ``sys.modules`` (la machinerie d'import de CPython lève alors ``ImportError``).
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest


@pytest.fixture
def s3_deps_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Rend ``boto3`` et ``s3fs`` non importables pour la durée du test."""
    for pkg in ("boto3", "s3fs"):
        monkeypatch.setitem(sys.modules, pkg, None)
        for name in [k for k in list(sys.modules) if k.startswith(f"{pkg}.")]:
            monkeypatch.delitem(sys.modules, name)
    # Ré-import à froid du sous-paquet pour que l'absence soit prise en compte
    for name in [
        k for k in list(sys.modules) if k.startswith("statflows.storage")
    ]:
        monkeypatch.delitem(sys.modules, name)


def test_import_and_local_roundtrip_without_s3_deps(
    s3_deps_absent: None, tmp_path: Path
) -> None:
    storage_json = importlib.import_module("statflows.storage.json")
    Loader, Saver = storage_json.Loader, storage_json.Saver

    path = tmp_path / "data.json"
    Saver().save(path, {"a": 1})
    assert Loader().load(path) == {"a": 1}


def test_load_with_bucket_raises_explicit_import_error(
    s3_deps_absent: None,
) -> None:
    from statflows.storage.json import Loader

    with pytest.raises(ImportError, match=r"statflows\[s3\]"):
        Loader().load("registry.json", bucket="my-bucket")


def test_save_with_bucket_raises_explicit_import_error(
    s3_deps_absent: None,
) -> None:
    from statflows.storage.json import Saver

    with pytest.raises(ImportError, match=r"statflows\[s3\]"):
        Saver().save("registry.json", {"a": 1}, bucket="my-bucket")
