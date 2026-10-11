"""Shared doubles for the S3 unit tests of :mod:`statflows.storage`.

``boto3.client`` / ``s3fs.S3FileSystem`` are replaced by mocks at the place
:meth:`S3Connection._connect` imports them from (its function body), so no
credential, network or ``moto`` is involved. Each fixture skips the test when the
matching optional extra is not installed (``bare-imports`` CI job).
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def boto3_client(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace ``boto3.client`` by a mock and return it."""
    boto3 = pytest.importorskip("boto3", reason="requires the 's3' extra")
    mock = MagicMock(name="boto3.client")
    monkeypatch.setattr(boto3, "client", mock)
    return mock


@pytest.fixture
def s3fs_class(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    """Replace ``s3fs.S3FileSystem`` by a mock and return it."""
    s3fs = pytest.importorskip("s3fs", reason="requires the 's3' extra")
    mock = MagicMock(name="s3fs.S3FileSystem")
    monkeypatch.setattr(s3fs, "S3FileSystem", mock)
    return mock
