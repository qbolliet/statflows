"""Characterisation tests — :func:`statflows.storage.ducklake.tables.fact_table_exists`.

Frozen behaviour: existence detection on an in-memory DuckDB, keyword-only
``table`` argument. Writing (``write_dataframe``) on a real catalog is covered by
``tests/integration/storage/ducklake``.

``fact_table_exists`` only uses ``duckdb`` ("ducklake" extra): the module is
skipped at collection if ``duckdb`` is missing.
"""

from __future__ import annotations

import pytest

pytest.importorskip("duckdb", reason="requiert l'extra « ducklake » (duckdb absent)")

from statflows.storage.ducklake.tables import (  # noqa: E402
    FACT_TABLE,
    fact_table_exists,
)


def test_fact_table_exists_in_memory() -> None:
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        # Table absente → False
        assert fact_table_exists(conn, "memory", "main", table="t") is False
        # Défaut keyword-only : cherche "fact_table"
        assert FACT_TABLE == "fact_table"
        assert fact_table_exists(conn, "memory", "main") is False

        conn.execute("CREATE TABLE t (a INTEGER)")

        # Table présente → True
        assert fact_table_exists(conn, "memory", "main", table="t") is True
        # Mauvais catalogue / schéma → False
        assert fact_table_exists(conn, "autre", "main", table="t") is False
        assert fact_table_exists(conn, "memory", "autre", table="t") is False
    finally:
        conn.close()


def test_table_argument_is_keyword_only() -> None:
    import duckdb

    conn = duckdb.connect(":memory:")
    try:
        with pytest.raises(TypeError):
            fact_table_exists(conn, "memory", "main", "t")  # type: ignore[misc]
    finally:
        conn.close()
