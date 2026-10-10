"""Characterisation tests — :class:`statflows.storage._connection.S3Connection` and
the lazy ``statflows.storage.__getattr__``.

Frozen behaviour: invalid ``s3_package`` → ``ValueError``; ``boto3`` client built
from explicit arguments, else from the ``AWS_*`` environment (a missing variable
raises ``KeyError``); ``s3fs`` file system built from the endpoint only; missing
optional package → ``ImportError`` carrying the extra's install hint;
``statflows.storage.S3Connection`` resolved lazily, any other name →
``AttributeError``. No real client is ever created.
"""

from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

import statflows.storage as storage
from statflows.storage._connection import S3Connection

# ──────────────────────────────────────────────────────────────────────
# Paquet invalide
# ──────────────────────────────────────────────────────────────────────


def test_rejects_invalid_package() -> None:
    with pytest.raises(ValueError, match="'s3_package' must be in"):
        S3Connection(s3_package="requests")  # type: ignore[arg-type]


def test_default_package_is_boto3() -> None:
    assert S3Connection().s3_package == "boto3"


# ──────────────────────────────────────────────────────────────────────
# Branche boto3
# ──────────────────────────────────────────────────────────────────────


def test_boto3_uses_explicit_arguments(boto3_client: MagicMock) -> None:
    conn = S3Connection()

    result = conn._connect(
        endpoint_url="https://s3.example.test",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        aws_session_token="token",
        verify=True,
        region_name="eu-west-1",
    )

    # _connect renvoie l'instance et expose le client créé
    assert result is conn
    assert conn.s3 is boto3_client.return_value
    boto3_client.assert_called_once_with(
        "s3",
        endpoint_url="https://s3.example.test",
        aws_access_key_id="id",
        aws_secret_access_key="secret",
        aws_session_token="token",
        verify=True,
        region_name="eu-west-1",
    )


def test_boto3_falls_back_on_environment(
    boto3_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_S3_ENDPOINT", "minio.test:9000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "env-token")

    S3Connection()._connect()

    # Le schéma https:// est préfixé à l'endpoint lu dans l'environnement ;
    # verify vaut False par défaut
    boto3_client.assert_called_once_with(
        "s3",
        endpoint_url="https://minio.test:9000",
        aws_access_key_id="env-id",
        aws_secret_access_key="env-secret",
        aws_session_token="env-token",
        verify=False,
    )


def test_boto3_mixes_explicit_and_environment(
    boto3_client: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "env-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "env-secret")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "env-token")

    S3Connection()._connect(endpoint_url="https://explicit.test")

    kwargs = boto3_client.call_args.kwargs
    assert kwargs["endpoint_url"] == "https://explicit.test"
    assert kwargs["aws_access_key_id"] == "env-id"


@pytest.mark.parametrize(
    "missing",
    [
        "AWS_S3_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
    ],
)
def test_boto3_missing_environment_variable_raises_key_error(
    boto3_client: MagicMock, monkeypatch: pytest.MonkeyPatch, missing: str
) -> None:
    env = {
        "AWS_S3_ENDPOINT": "minio.test",
        "AWS_ACCESS_KEY_ID": "id",
        "AWS_SECRET_ACCESS_KEY": "secret",
        "AWS_SESSION_TOKEN": "token",
    }
    del env[missing]
    for name, value in env.items():
        monkeypatch.setenv(name, value)

    # Comportement actuel : KeyError brut, nommant la variable manquante
    with pytest.raises(KeyError, match=missing):
        S3Connection()._connect()

    boto3_client.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# Branche s3fs
# ──────────────────────────────────────────────────────────────────────


def test_s3fs_explicit_endpoint(s3fs_class: MagicMock) -> None:
    conn = S3Connection(s3_package="s3fs")

    result = conn._connect(endpoint_url="https://s3.example.test", anon=False)

    assert result is conn
    assert conn.s3 is s3fs_class.return_value
    s3fs_class.assert_called_once_with(
        client_kwargs={"endpoint_url": "https://s3.example.test"}, anon=False
    )


def test_s3fs_endpoint_from_environment(
    s3fs_class: MagicMock, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AWS_S3_ENDPOINT", "minio.test:9000")

    S3Connection(s3_package="s3fs")._connect()

    s3fs_class.assert_called_once_with(
        client_kwargs={"endpoint_url": "https://minio.test:9000"}
    )


def test_s3fs_missing_endpoint_raises_key_error(s3fs_class: MagicMock) -> None:
    with pytest.raises(KeyError, match="AWS_S3_ENDPOINT"):
        S3Connection(s3_package="s3fs")._connect()

    s3fs_class.assert_not_called()


def test_package_changed_after_construction_is_rejected(s3fs_class: MagicMock) -> None:
    # Attribut modifié après construction : la garde de _connect reste effective
    conn = S3Connection(s3_package="s3fs")
    conn.s3_package = "other"  # type: ignore[assignment]

    with pytest.raises(ValueError, match="'s3_package' must be in"):
        conn._connect(endpoint_url="https://s3.example.test")

    s3fs_class.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# Extra « s3 » absent
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("package", ["boto3", "s3fs"])
def test_missing_package_raises_import_error_with_extra_hint(
    monkeypatch: pytest.MonkeyPatch, package: str
) -> None:
    # None dans sys.modules : l'import lève ImportError, comme un paquet absent
    monkeypatch.setitem(sys.modules, package, None)

    with pytest.raises(ImportError, match=r"statflows\[s3\]") as excinfo:
        S3Connection(s3_package=package)._connect(  # type: ignore[arg-type]
            endpoint_url="https://s3.example.test"
        )

    # La cause d'origine est conservée
    assert isinstance(excinfo.value.__cause__, ImportError)


# ──────────────────────────────────────────────────────────────────────
# statflows.storage.__getattr__
# ──────────────────────────────────────────────────────────────────────


def test_storage_getattr_resolves_s3_connection_lazily() -> None:
    assert storage.S3Connection is S3Connection


def test_storage_getattr_unknown_name_raises_attribute_error() -> None:
    with pytest.raises(AttributeError) as excinfo:
        storage.does_not_exist  # noqa: B018
    assert str(excinfo.value) == (
        "module 'statflows.storage' has no attribute 'does_not_exist'"
    )
