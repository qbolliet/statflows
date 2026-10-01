"""Integration test fixtures: simulated S3 (moto) and file-based DuckLake catalog.

Every test collected under ``tests/integration/`` is automatically marked
``integration`` (see :func:`pytest_collection_modifyitems`).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.utils.ducklake import file_backed_catalog

# Nom du bucket de test
BUCKET = "test-bucket"

# Racine du paquet des tests d'intégration
_INTEGRATION_DIR = Path(__file__).parent


# Marquage automatique des seuls tests de ce sous-arbre
def pytest_collection_modifyitems(config, items) -> None:
    """Add the ``integration`` marker to the tests collected under ``tests/integration``.

    The hook receives the full list of items whichever conftest defines it: filtering
    on the path is therefore indispensable.
    """
    for item in items:
        if _INTEGRATION_DIR in Path(str(item.fspath)).parents:
            item.add_marker(pytest.mark.integration)


# ──────────────────────────────────────────────────────────────────────
# S3 simulé (moto)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set the AWS environment variables expected by ``S3Connection``.

    ``S3Connection._connect`` reads ``os.environ[...]`` (and raises ``KeyError`` when
    missing) as soon as an S3 argument is ``None`` — which is the case through the
    default ``Loader``/``Saver``. Dummy values are therefore set.
    """
    monkeypatch.setenv("AWS_S3_ENDPOINT", "s3.amazonaws.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_bucket(aws_env: None):
    """Enable moto's S3 mock and create an empty bucket.

    Yields:
        The name of the created bucket (``"test-bucket"``).
    """
    moto = pytest.importorskip("moto")

    with moto.mock_aws():
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield BUCKET


@pytest.fixture
def s3_client(s3_bucket: str):
    """Raw boto3 client on the moto bucket (to prepare / check objects)."""
    import boto3

    return boto3.client("s3", region_name="us-east-1")


# ──────────────────────────────────────────────────────────────────────
# Catalogue DuckLake sur fichier temporaire
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def ducklake_conn(tmp_path: Path):
    """DuckDB connection + file-based ``.ducklake`` DuckLake catalog in a temp dir.

    ``pytest.skip`` if the "ducklake" extra or the ``ducklake`` DuckDB extension is
    not available.

    Yields:
        Tuple ``(conn, catalog_alias)``; connection on ``db.main``, schema ``s1``
        already created.
    """
    pytest.importorskip("dt_ducklake_manager")
    with file_backed_catalog(tmp_path) as (conn, alias):
        yield conn, alias
