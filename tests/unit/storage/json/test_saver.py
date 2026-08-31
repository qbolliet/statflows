"""Tests de caractérisation — :class:`statflows.storage.json.Saver` (mode local).

Comportement figé : extension non ``.json`` → ``ValueError``, transmission de
``indent`` / ``ensure_ascii`` jusqu'à ``json.dump``, création du dossier parent,
écriture atomique (``atomic``) sans temporaire résiduel. Le mode S3 est couvert
par ``tests/integration/storage/json``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from statflows.storage.json import Loader, Saver

_BAD_EXT_MSG = "Unsupported extension '.txt': only '.json' files are supported."


# ──────────────────────────────────────────────────────────────────────
# Extension non supportée
# ──────────────────────────────────────────────────────────────────────


def test_rejects_non_json_extension(tmp_path: Path) -> None:
    with pytest.raises(ValueError) as excinfo:
        Saver().save(str(tmp_path / "data.txt"), {"a": 1})
    assert str(excinfo.value) == _BAD_EXT_MSG


# ──────────────────────────────────────────────────────────────────────
# Format d'écriture
# ──────────────────────────────────────────────────────────────────────


def test_honours_indent_and_non_ascii(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    Saver().save(path, {"pays": "Suède", "note": "éàü"}, indent=2, ensure_ascii=False)

    content = path.read_text(encoding="utf-8")
    # indent=2 transmis jusqu'à json.dump malgré le passage par le temporaire
    assert '\n  "pays"' in content
    # ensure_ascii=False : les accents ne sont pas échappés
    assert "Suède" in content
    assert "\\u" not in content


def test_creates_missing_parent_dir(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "deeper" / "reg.json"

    Saver().save(path, {"ok": True})

    assert path.exists()
    assert Loader().load(path) == {"ok": True}


# ──────────────────────────────────────────────────────────────────────
# Atomicité
# ──────────────────────────────────────────────────────────────────────


def test_atomic_write_leaves_no_tempfile(tmp_path: Path) -> None:
    path = tmp_path / "reg.json"

    Saver().save(path, {"v": 1})
    # Ré-écriture au-dessus d'un fichier existant
    Saver().save(path, {"v": 2})

    assert Loader().load(path) == {"v": 2}
    # Aucun temporaire (tmp*.json) résiduel dans le dossier de destination
    assert [p.name for p in tmp_path.iterdir()] == ["reg.json"]


def test_non_atomic_write_same_content(tmp_path: Path) -> None:
    atomic_path = tmp_path / "atomic.json"
    direct_path = tmp_path / "direct.json"
    payload = {"pays": "Suède", "n": 3}

    Saver().save(atomic_path, payload, indent=2, ensure_ascii=False)
    Saver().save(direct_path, payload, atomic=False, indent=2, ensure_ascii=False)

    assert direct_path.read_text(encoding="utf-8") == atomic_path.read_text(
        encoding="utf-8"
    )
