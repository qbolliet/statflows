"""Tests d'intégration — :mod:`statflows.storage.json` contre S3 (moto).

Comportement figé : validation d'extension côté S3, aller-retour, lecture
tolérante (``missing_ok`` avale l'absence d'objet mais pas l'erreur de format),
conversion d'un ``Path`` en clé POSIX, transmission de ``indent`` / ``ensure_ascii``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from statflows.storage.json import Loader, Saver

_BAD_EXT_MSG = "Unsupported extension '.txt': only '.json' files are supported."


# ──────────────────────────────────────────────────────────────────────
# Extension non supportée
# ──────────────────────────────────────────────────────────────────────


def test_loader_rejects_non_json_extension(s3_bucket: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        Loader().load("data.txt", bucket=s3_bucket)
    assert str(excinfo.value) == _BAD_EXT_MSG


def test_saver_rejects_non_json_extension(s3_bucket: str) -> None:
    with pytest.raises(ValueError) as excinfo:
        Saver().save("data.txt", {"a": 1}, bucket=s3_bucket)
    assert str(excinfo.value) == _BAD_EXT_MSG


def test_extension_error_raised_even_with_missing_ok(s3_bucket: str) -> None:
    # ``missing_ok`` n'avale que l'absence d'objet, pas la validation de format
    with pytest.raises(ValueError) as excinfo:
        Loader().load("data.txt", bucket=s3_bucket, missing_ok=True)
    assert str(excinfo.value) == _BAD_EXT_MSG


# ──────────────────────────────────────────────────────────────────────
# Aller-retour
# ──────────────────────────────────────────────────────────────────────


def test_roundtrip(s3_bucket: str) -> None:
    obj = {"k": "v", "n": 42}

    Saver().save("dir/data.json", obj, bucket=s3_bucket)

    assert Loader().load("dir/data.json", bucket=s3_bucket) == obj


def test_path_converted_to_posix_key(s3_bucket: str, s3_client) -> None:
    # Un ``Path`` (séparateurs Windows) devient une clé S3 POSIX
    key = Path("reg") / "state.json"

    Saver().save(key, {"v": 1}, bucket=s3_bucket)

    body = s3_client.get_object(Bucket=s3_bucket, Key="reg/state.json")["Body"].read()
    assert body.decode("utf-8") == '{"v": 1}'
    assert Loader().load(key, bucket=s3_bucket) == {"v": 1}


# ──────────────────────────────────────────────────────────────────────
# Lecture tolérante
# ──────────────────────────────────────────────────────────────────────


def test_missing_key_returns_none_with_missing_ok(s3_bucket: str) -> None:
    assert Loader().load(Path("reg/state.json"), bucket=s3_bucket, missing_ok=True) is None


def test_missing_key_raises_without_missing_ok(s3_bucket: str) -> None:
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError):
        Loader().load("reg/state.json", bucket=s3_bucket)


# ──────────────────────────────────────────────────────────────────────
# Format d'écriture
# ──────────────────────────────────────────────────────────────────────


def test_honours_indent_and_non_ascii(s3_bucket: str, s3_client) -> None:
    key = Path("reg/state.json")
    payload = {"pays": "Suède", "n": 3}

    Saver().save(key, payload, bucket=s3_bucket, indent=2, ensure_ascii=False)

    body = s3_client.get_object(Bucket=s3_bucket, Key="reg/state.json")["Body"].read()
    text = body.decode("utf-8")
    assert '\n  "pays"' in text
    assert "Suède" in text

    assert Loader().load(key, bucket=s3_bucket) == payload
