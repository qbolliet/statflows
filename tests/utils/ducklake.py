"""Mounting of a DuckLake catalog on a temporary file, without an external connector.

Reproduces the pipeline's mounting (``duckdb.connect(":memory:")`` +
``INSTALL/LOAD ducklake`` + ``ATTACH 'ducklake:...'``) to exercise the helpers of
:mod:`statflows.storage.ducklake.tables` on a real catalog.
"""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from pathlib import Path

import pytest


@contextlib.contextmanager
def file_backed_catalog(
    tmp_path: Path, *, alias: str = "db", schema: str = "s1"
) -> Iterator[tuple[object, str]]:
    """Open a file-based ``.ducklake`` DuckLake catalog in a temp dir.

    ``pytest.skip`` if ``duckdb`` is not installed ("ducklake" extra) or if the
    ``ducklake`` DuckDB extension cannot be fetched (offline).

    Args:
        tmp_path: Temporary folder of the test.
        alias: Alias under which to attach the catalog.
        schema: Schema created up front in the catalog.

    Yields:
        Tuple ``(conn, alias)``, connection positioned on ``{alias}.main``.
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
