"""Characterisation tests — S3 mode of :class:`Loader` / :class:`Saver` with mocked
clients (no ``moto``, no credential).

Frozen behaviour: key without ``.json`` → ``ValueError`` before any connection;
connection opened lazily at the first call and reused; ``boto3`` reads through
``get_object`` and writes through ``put_object``, ``s3fs`` through ``open``;
absent object → ``ClientError`` unless ``missing_ok`` (``None``); invalid JSON is
never swallowed by ``missing_ok``; ``list_json`` filters hidden and non-JSON keys
over every paginator page (``boto3``) or ``glob`` (``s3fs``). Real S3 round trips
are covered by ``tests/integration/storage/json``.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from statflows.storage.json import Loader, Saver
from statflows.storage.json.s3.loader import S3Loader
from statflows.storage.json.s3.saver import S3Saver

_BAD_EXT_MSG = "Unsupported extension '.csv': only '.json' files are supported."


def _client_error() -> Exception:
    """Build the ``ClientError`` raised by ``boto3`` for an absent object."""
    exceptions = pytest.importorskip("botocore.exceptions")
    return exceptions.ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")


def _body(payload: str) -> dict:
    """Return a ``get_object`` response whose ``Body`` is a readable stream."""
    return {"Body": io.BytesIO(payload.encode())}


@pytest.fixture(autouse=True)
def _aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set fake ``AWS_*`` values so the implicit connection reads them."""
    monkeypatch.setenv("AWS_S3_ENDPOINT", "minio.test")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")


# ──────────────────────────────────────────────────────────────────────
# Extension non supportée (aucune connexion ouverte)
# ──────────────────────────────────────────────────────────────────────


def test_s3_loader_rejects_key_without_json_extension(boto3_client: MagicMock) -> None:
    with pytest.raises(ValueError) as excinfo:
        S3Loader().load(bucket="b", key="dir/data.csv")
    assert str(excinfo.value) == _BAD_EXT_MSG
    boto3_client.assert_not_called()


def test_s3_loader_rejects_key_without_dot(boto3_client: MagicMock) -> None:
    with pytest.raises(ValueError, match="Unsupported extension '.registry'"):
        S3Loader().load(bucket="b", key="registry")
    boto3_client.assert_not_called()


def test_s3_saver_rejects_key_without_json_extension(boto3_client: MagicMock) -> None:
    with pytest.raises(ValueError) as excinfo:
        S3Saver().save(bucket="b", key="dir/data.csv", obj={"a": 1})
    assert str(excinfo.value) == _BAD_EXT_MSG
    boto3_client.assert_not_called()


def test_s3_extension_check_is_case_insensitive(boto3_client: MagicMock) -> None:
    loader = S3Loader()
    loader.s3 = boto3_client.return_value
    boto3_client.return_value.get_object.return_value = _body('{"a": 1}')

    assert loader.load(bucket="b", key="DATA.JSON") == {"a": 1}


# ──────────────────────────────────────────────────────────────────────
# Connexion paresseuse
# ──────────────────────────────────────────────────────────────────────


def test_connection_is_lazy_and_reused(boto3_client: MagicMock) -> None:
    boto3_client.return_value.get_object.side_effect = lambda **_: _body("[1]")
    loader = Loader()

    # Construction : aucune connexion
    boto3_client.assert_not_called()
    assert not hasattr(loader, "s3")

    assert loader.load("a.json", bucket="b") == [1]
    assert loader.load("b.json", bucket="b") == [1]

    # Un seul client créé pour les deux lectures
    boto3_client.assert_called_once()


def test_loader_forwards_s3_kwargs_to_the_first_connection(
    boto3_client: MagicMock,
) -> None:
    boto3_client.return_value.get_object.return_value = _body("{}")

    Loader().load(
        Path("registries") / "reg.json",
        bucket="b",
        endpoint_url="https://s3.example.test",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        aws_session_token="token",
        verify=True,
    )

    kwargs = boto3_client.call_args.kwargs
    assert kwargs["endpoint_url"] == "https://s3.example.test"
    assert kwargs["aws_access_key_id"] == "id"
    assert kwargs["verify"] is True
    # La clé est normalisée en POSIX
    boto3_client.return_value.get_object.assert_called_once_with(
        Bucket="b", Key="registries/reg.json"
    )


def test_saver_forwards_s3_kwargs_to_the_first_connection(
    boto3_client: MagicMock,
) -> None:
    Saver().save(
        Path("registries") / "reg.json",
        {"a": 1},
        bucket="b",
        endpoint_url="https://s3.example.test",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        aws_session_token="token",
        indent=2,
    )

    assert boto3_client.call_args.kwargs["endpoint_url"] == "https://s3.example.test"
    # indent est transmis à json.dumps, pas au client
    boto3_client.return_value.put_object.assert_called_once_with(
        Bucket="b", Key="registries/reg.json", Body=json.dumps({"a": 1}, indent=2)
    )


def test_existing_connection_is_not_replaced(boto3_client: MagicMock) -> None:
    saver = Saver()
    saver.s3 = MagicMock()

    saver.save("reg.json", {"a": 1}, bucket="b")

    boto3_client.assert_not_called()
    saver.s3.put_object.assert_called_once()


def test_s3_classes_connect_themselves_on_first_call(
    boto3_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("AWS_S3_ENDPOINT")
    boto3_client.return_value.get_object.return_value = _body("[]")
    loader = S3Loader()

    # Sans connect() explicite ni variables d'environnement : la connexion
    # implicite lève le KeyError actuel, avant tout appel au client
    with pytest.raises(KeyError, match="AWS_S3_ENDPOINT"):
        loader.load(bucket="b", key="a.json")
    boto3_client.assert_not_called()

    loader.connect(
        endpoint_url="https://s3.example.test",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        aws_session_token="token",
    )
    assert loader.load(bucket="b", key="a.json") == []


def test_s3_saver_connects_itself_on_first_save(boto3_client: MagicMock) -> None:
    saver = S3Saver()

    # Variables d'environnement posées par la fixture autouse : connexion implicite
    saver.save(bucket="b", key="a.json", obj=[1])

    boto3_client.assert_called_once()
    boto3_client.return_value.put_object.assert_called_once_with(
        Bucket="b", Key="a.json", Body="[1]"
    )


# ──────────────────────────────────────────────────────────────────────
# Chargement / sauvegarde — boto3
# ──────────────────────────────────────────────────────────────────────


def test_boto3_load_forwards_kwargs_to_json_load(boto3_client: MagicMock) -> None:
    boto3_client.return_value.get_object.return_value = _body('{"n": 1.5}')

    result = Loader().load(
        "reg.json", bucket="b", parse_float=str, endpoint_url="https://x.test"
    )

    assert result == {"n": "1.5"}


def test_boto3_save_serialises_and_puts(boto3_client: MagicMock) -> None:
    saver = S3Saver()
    saver.s3 = boto3_client.return_value

    saver.save(bucket="b", key="dir/reg.json", obj={"pays": "Suède"}, indent=2)

    boto3_client.return_value.put_object.assert_called_once_with(
        Bucket="b", Key="dir/reg.json", Body=json.dumps({"pays": "Suède"}, indent=2)
    )


def test_boto3_save_none_writes_json_null(boto3_client: MagicMock) -> None:
    Saver().save("reg.json", bucket="b", endpoint_url="https://x.test")

    assert boto3_client.return_value.put_object.call_args.kwargs["Body"] == "null"


def test_boto3_save_unserialisable_object_raises_type_error(
    boto3_client: MagicMock,
) -> None:
    with pytest.raises(TypeError):
        Saver().save(
            "reg.json", {"a": object()}, bucket="b", endpoint_url="https://x.test"
        )

    boto3_client.return_value.put_object.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# Objet absent / JSON invalide
# ──────────────────────────────────────────────────────────────────────


def test_missing_object_raises_without_missing_ok(boto3_client: MagicMock) -> None:
    error = _client_error()
    boto3_client.return_value.get_object.side_effect = error

    with pytest.raises(type(error)):
        Loader().load("absent.json", bucket="b", endpoint_url="https://x.test")


def test_missing_object_returns_none_with_missing_ok(boto3_client: MagicMock) -> None:
    boto3_client.return_value.get_object.side_effect = _client_error()

    assert (
        Loader().load(
            "absent.json", bucket="b", missing_ok=True, endpoint_url="https://x.test"
        )
        is None
    )


def test_invalid_json_raises_even_with_missing_ok(boto3_client: MagicMock) -> None:
    boto3_client.return_value.get_object.return_value = _body("{not json")

    # missing_ok ne masque que ClientError : un contenu corrompu reste visible
    with pytest.raises(json.JSONDecodeError):
        Loader().load(
            "reg.json", bucket="b", missing_ok=True, endpoint_url="https://x.test"
        )


def test_invalid_json_raises_without_missing_ok(boto3_client: MagicMock) -> None:
    boto3_client.return_value.get_object.return_value = _body("")
    loader = S3Loader()
    loader.s3 = boto3_client.return_value

    with pytest.raises(json.JSONDecodeError):
        loader.load(bucket="b", key="reg.json")


# ──────────────────────────────────────────────────────────────────────
# Chargement / sauvegarde — s3fs
# ──────────────────────────────────────────────────────────────────────


def test_s3fs_load_reads_through_open(s3fs_class: MagicMock) -> None:
    fs = s3fs_class.return_value
    fs.open.return_value.__enter__.return_value = io.StringIO('{"a": 1}')

    result = Loader(s3_package="s3fs").load(
        Path("dir") / "reg.json", bucket="b", endpoint_url="https://x.test"
    )

    assert result == {"a": 1}
    fs.open.assert_called_once_with("b/dir/reg.json", "r")


def test_s3fs_save_writes_through_open(s3fs_class: MagicMock) -> None:
    fs = s3fs_class.return_value
    handle = fs.open.return_value.__enter__.return_value

    Saver(s3_package="s3fs").save(
        "dir/reg.json", {"a": 1}, bucket="b", endpoint_url="https://x.test"
    )

    fs.open.assert_called_once_with("b/dir/reg.json", "w")
    handle.write.assert_called_once_with(json.dumps({"a": 1}))


# ──────────────────────────────────────────────────────────────────────
# Loader.list_json en S3
# ──────────────────────────────────────────────────────────────────────


def test_list_json_boto3_walks_every_page_and_filters(boto3_client: MagicMock) -> None:
    paginator = boto3_client.return_value.get_paginator.return_value
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "reg/b.json"}, {"Key": "reg/.tmp-x.json"}]},
        # Page sans « Contents »
        {},
        {"Contents": [{"Key": "reg/a.json"}, {"Key": "reg/notes.txt"}]},
        {"Contents": [{"Key": "reg/.hidden.json"}, {"Key": "reg/c.json"}]},
    ]

    keys = Loader().list_json(Path("reg"), bucket="b", endpoint_url="https://x.test")

    # Trié, sans fichier masqué ni non-JSON
    assert keys == ["reg/a.json", "reg/b.json", "reg/c.json"]
    boto3_client.return_value.get_paginator.assert_called_once_with("list_objects_v2")
    paginator.paginate.assert_called_once_with(Bucket="b", Prefix="reg/", Delimiter="/")


def test_list_json_boto3_empty_prefix(boto3_client: MagicMock) -> None:
    boto3_client.return_value.get_paginator.return_value.paginate.return_value = [{}]

    assert Loader().list_json("reg/", bucket="b", endpoint_url="https://x.test") == []


def test_list_json_boto3_connects_once(boto3_client: MagicMock) -> None:
    boto3_client.return_value.get_paginator.return_value.paginate.return_value = []
    loader = Loader()

    loader.list_json("reg", bucket="b", endpoint_url="https://x.test")
    loader.list_json("reg", bucket="b")

    boto3_client.assert_called_once()


def test_list_json_s3fs_uses_glob(s3fs_class: MagicMock) -> None:
    fs = s3fs_class.return_value
    fs.glob.return_value = [
        "b/reg/z.json",
        "b/reg/.tmp-1.json",
        "b/reg/a.json",
        "b/reg/readme.json.bak",
    ]

    keys = Loader(s3_package="s3fs").list_json(
        "reg", bucket="b", endpoint_url="https://x.test"
    )

    # Le nom du bucket est retiré des chemins renvoyés par glob
    assert keys == ["reg/a.json", "reg/z.json"]
    fs.glob.assert_called_once_with("b/reg/*.json")
