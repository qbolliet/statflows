"""Tests de caractérisation — :func:`statflows.storage.ducklake.tables.fact_table_exists`.

Comportement figé : détection d'existence sur un DuckDB en mémoire, argument
``table`` keyword-only. L'écriture (``write_dataframe``) sur un vrai catalogue est
couverte par ``tests/integration/storage/ducklake``.

``fact_table_exists`` n'utilise que ``duckdb`` (extra « ducklake ») : le module
est ignoré à la collecte si ``duckdb`` est absent.
"""

from __future__ import annotations

import pytest

pytest.importorskip(
    "duckdb", reason="requiert l'extra « ducklake » (duckdb absent)"
)

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
