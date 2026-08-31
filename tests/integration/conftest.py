"""Fixtures des tests d'intégration : S3 simulé (moto) et catalogue DuckLake fichier.

Tout test collecté sous ``tests/integration/`` est automatiquement marqué
``integration`` (cf. :func:`pytest_collection_modifyitems`).
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
    """Ajoute le marqueur ``integration`` aux tests collectés sous ``tests/integration``.

    Le hook reçoit la liste complète des items quel que soit le conftest qui le
    définit : le filtrage sur le chemin est donc indispensable.
    """
    for item in items:
        if _INTEGRATION_DIR in Path(str(item.fspath)).parents:
            item.add_marker(pytest.mark.integration)


# ──────────────────────────────────────────────────────────────────────
# S3 simulé (moto)
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def aws_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Renseigne les variables d'environnement AWS attendues par ``S3Connection``.

    ``S3Connection._connect`` lit ``os.environ[...]`` (et lève ``KeyError`` en
    l'absence) dès qu'un argument S3 vaut ``None`` — ce qui est le cas via les
    ``Loader``/``Saver`` par défaut. On pose donc des valeurs factices.
    """
    monkeypatch.setenv("AWS_S3_ENDPOINT", "s3.amazonaws.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def s3_bucket(aws_env: None):
    """Active le mock S3 de moto et crée un bucket vide.

    Yields:
        Le nom du bucket créé (``"test-bucket"``).
    """
    moto = pytest.importorskip("moto")

    with moto.mock_aws():
        import boto3

        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield BUCKET


@pytest.fixture
def s3_client(s3_bucket: str):
    """Client boto3 brut sur le bucket moto (pour préparer / vérifier des objets)."""
    import boto3

    return boto3.client("s3", region_name="us-east-1")


# ──────────────────────────────────────────────────────────────────────
# Catalogue DuckLake sur fichier temporaire
# ──────────────────────────────────────────────────────────────────────


@pytest.fixture
def ducklake_conn(tmp_path: Path):
    """Connexion DuckDB + catalogue DuckLake ``.ducklake`` en fichier temp.

    ``pytest.skip`` si l'extra « ducklake » ou l'extension DuckDB ``ducklake``
    n'est pas disponible.

    Yields:
        Tuple ``(conn, catalog_alias)`` ; connexion sur ``db.main``, schéma
        ``s1`` déjà créé.
    """
    pytest.importorskip("dt_ducklake_manager")
    with file_backed_catalog(tmp_path) as (conn, alias):
        yield conn, alias
