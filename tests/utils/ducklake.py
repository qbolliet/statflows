"""Montage d'un catalogue DuckLake sur fichier temporaire, sans connecteur externe.

Reproduit le montage du pipeline (``duckdb.connect(":memory:")`` +
``INSTALL/LOAD ducklake`` + ``ATTACH 'ducklake:...'``) pour exercer les helpers
de :mod:`statflows.storage.ducklake.tables` sur un vrai catalogue.
"""

from __future__ import annotations

import contextlib
from pathlib import Path
from typing import Iterator, Tuple

import pytest


@contextlib.contextmanager
def file_backed_catalog(
    tmp_path: Path, *, alias: str = "db", schema: str = "s1"
) -> Iterator[Tuple["object", str]]:
    """Ouvre un catalogue DuckLake ``.ducklake`` en fichier temp.

    ``pytest.skip`` si ``duckdb`` n'est pas installé (extra « ducklake ») ou si
    l'extension DuckDB ``ducklake`` n'est pas récupérable (hors-ligne).

    Args:
        tmp_path: Dossier temporaire du test.
        alias: Alias sous lequel attacher le catalogue.
        schema: Schéma créé d'emblée dans le catalogue.

    Yields:
        Tuple ``(conn, alias)``, connexion positionnée sur ``{alias}.main``.
    """
    duckdb = pytest.importorskip("duckdb")

    catalog_path = tmp_path / "catalog.ducklake"
    data_path = tmp_path / "data"
    data_path.mkdir(exist_ok=True)

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("INSTALL ducklake")
        conn.execute("LOAD ducklake")
    except duckdb.Error as exc:  # extension indisponible hors-ligne
        conn.close()
        pytest.skip(f"extension duckdb 'ducklake' indisponible : {exc}")

    conn.execute(
        f"ATTACH 'ducklake:{catalog_path.as_posix()}' AS {alias} "
        f"(DATA_PATH '{data_path.as_posix()}')"
    )
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {alias}.{schema}")
    conn.execute(f"USE {alias}.main")

    try:
        yield conn, alias
    finally:
        with contextlib.suppress(Exception):
            conn.close()
