"""Unit tests — :func:`statflows.storage.ducklake.tables.write_dataframe`.

The ``dt_ducklake_manager`` classes are replaced by ``autospec`` mocks: calls are
validated against the **real signatures** of the installed version. A parameter
renamed or removed by the library (e.g. ``ducklake_catalog_alias`` →
``catalog_alias``) therefore makes these tests fail without any DuckLake catalog
being needed.

Requires the "ducklake" extra: the module is skipped at collection otherwise.
"""

from __future__ import annotations

import inspect
from unittest import mock

import pandas as pd
import pytest

pytest.importorskip("duckdb", reason="requiert l'extra « ducklake » (duckdb absent)")
pytest.importorskip(
    "dt_ducklake_manager",
    reason="requiert l'extra « ducklake » (dt_ducklake_manager absent)",
)

import dt_ducklake_manager  # noqa: E402
import duckdb  # noqa: E402

from statflows.storage.ducklake.tables import write_dataframe  # noqa: E402

ALIAS = "lake"
SCHEMA = "s1"


@pytest.fixture
def data() -> pd.DataFrame:
    return pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})


@pytest.fixture
def conn():
    """In-memory connection, with or without a fact table depending on the test."""
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


def _make_fact_table_visible(conn, alias: str, schema: str) -> None:
    """Make ``{alias}.{schema}.fact_table`` exist for ``fact_table_exists``."""
    conn.execute(f"ATTACH ':memory:' AS {alias}")
    conn.execute(f"CREATE SCHEMA {alias}.{schema}")
    conn.execute(f"CREATE TABLE {alias}.{schema}.fact_table (id INTEGER)")


@pytest.fixture
def updater_cls():
    """``DatabaseUpdater`` mocked with the real signature; successful update."""
    with mock.patch.object(
        dt_ducklake_manager, "DatabaseUpdater", autospec=True
    ) as cls:
        cls.return_value.update_database.return_value = True
        yield cls


@pytest.fixture
def builder_cls():
    """``DuckLakeTablesBuilder`` mocked with the real signature."""
    with mock.patch.object(
        dt_ducklake_manager, "DuckLakeTablesBuilder", autospec=True
    ) as cls:
        yield cls


# ──────────────────────────────────────────────────────────────────────
# Chemin d'upsert (la table de faits existe)
# ──────────────────────────────────────────────────────────────────────


def test_upsert_passes_catalog_alias_and_schema_to_updater(
    conn, data, updater_cls
) -> None:
    _make_fact_table_visible(conn, ALIAS, SCHEMA)

    created = write_dataframe(
        conn,
        data,
        ["id"],
        catalog_alias=ALIAS,
        schema=SCHEMA,
        categorical_threshold=7,
    )

    assert created is False
    updater_cls.assert_called_once_with(
        connection=conn,
        categorical_threshold=7,
        catalog_alias=ALIAS,
        schema=SCHEMA,
    )
    updater_cls.return_value.update_database.assert_called_once_with(
        data, use_transaction=True
    )


def test_upsert_failure_raises_value_error(conn, data, updater_cls) -> None:
    _make_fact_table_visible(conn, ALIAS, SCHEMA)
    updater_cls.return_value.update_database.return_value = False

    with pytest.raises(ValueError, match=rf"run-42: .*'{SCHEMA}'"):
        write_dataframe(
            conn,
            data,
            ["id"],
            catalog_alias=ALIAS,
            schema=SCHEMA,
            label="run-42",
        )


def test_upsert_does_not_build_schema(conn, data, updater_cls, builder_cls) -> None:
    _make_fact_table_visible(conn, ALIAS, SCHEMA)

    write_dataframe(conn, data, ["id"], catalog_alias=ALIAS, schema=SCHEMA)

    builder_cls.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# Chemin de création (la table de faits n'existe pas)
# ──────────────────────────────────────────────────────────────────────


def test_create_passes_catalog_alias_and_schema_to_builder(
    conn, data, builder_cls
) -> None:
    created = write_dataframe(
        conn,
        data,
        ("id",),
        catalog_alias=ALIAS,
        schema=SCHEMA,
        categorical_threshold=7,
    )

    assert created is True
    # `primary_keys` est converti en liste ; l'alias doit être propagé, faute de
    # quoi le builder retombe silencieusement sur son défaut ("db")
    builder_cls.assert_called_once_with(
        data,
        categorical_threshold=7,
        primary_keys=["id"],
        connection=conn,
        schema=SCHEMA,
        catalog_alias=ALIAS,
    )
    builder_cls.return_value.build_schema.assert_called_once_with()


def test_create_does_not_update(conn, data, updater_cls, builder_cls) -> None:
    write_dataframe(conn, data, ["id"], catalog_alias=ALIAS, schema=SCHEMA)

    updater_cls.assert_not_called()


# ──────────────────────────────────────────────────────────────────────
# Contrat avec la librairie et isolation de la session
# ──────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("cls_name", ["DatabaseUpdater", "DuckLakeTablesBuilder"])
def test_library_classes_accept_catalog_alias(cls_name: str) -> None:
    """Both classes expose ``catalog_alias`` and ``schema`` (not the old name)."""
    params = inspect.signature(getattr(dt_ducklake_manager, cls_name)).parameters

    assert "catalog_alias" in params
    assert "schema" in params
    assert "ducklake_catalog_alias" not in params


def test_write_leaves_session_position_untouched(conn, data, builder_cls) -> None:
    """``write_dataframe`` no longer moves the connection (no more ``USE``)."""
    conn.execute("ATTACH ':memory:' AS other")
    conn.execute("USE other")

    write_dataframe(conn, data, ["id"], catalog_alias=ALIAS, schema=SCHEMA)

    assert conn.execute("SELECT current_database()").fetchone() == ("other",)


def test_missing_dependency_raises_import_error(conn, data) -> None:
    """Without ``dt_ducklake_manager``, the error is explicit and precedes any effect."""
    with mock.patch.dict("sys.modules", {"dt_ducklake_manager": None}):
        with pytest.raises(ImportError, match=r"statflows\[ducklake\]"):
            write_dataframe(conn, data, ["id"], catalog_alias=ALIAS, schema=SCHEMA)


# ──────────────────────────────────────────────────────────────────────
# Options transmises à la librairie (compaction, colonnes, commit)
# ──────────────────────────────────────────────────────────────────────


def test_upsert_forwards_explicit_options(conn, data, updater_cls) -> None:
    _make_fact_table_visible(conn, ALIAS, SCHEMA)

    write_dataframe(
        conn,
        data,
        ["id"],
        catalog_alias=ALIAS,
        schema=SCHEMA,
        update_options={"allow_new_columns": True, "compact_after_update": True},
        run_id="run-1",
        commit_message="msg",
    )

    updater_cls.return_value.update_database.assert_called_once_with(
        data,
        use_transaction=True,
        compact_after_update=True,
        allow_new_columns=True,
        run_id="run-1",
        commit_message="msg",
    )


def test_create_forwards_commit_options_to_build_schema(
    conn, data, builder_cls
) -> None:
    write_dataframe(
        conn, data, ["id"], catalog_alias=ALIAS, schema=SCHEMA, run_id="run-1"
    )

    builder_cls.return_value.build_schema.assert_called_once_with(run_id="run-1")


def test_create_forwards_build_options(conn, data, builder_cls) -> None:
    write_dataframe(
        conn,
        data,
        ["id"],
        catalog_alias=ALIAS,
        schema=SCHEMA,
        build_options={"partition_by": ["id"], "run_id": "ignored"},
        run_id="run-1",
    )

    # Les options de commit explicites priment sur celles du dictionnaire
    builder_cls.return_value.build_schema.assert_called_once_with(
        partition_by=["id"], run_id="run-1"
    )


@pytest.mark.parametrize(
    "param", ["compact_after_update", "allow_new_columns", "run_id", "commit_message"]
)
def test_library_update_accepts_forwarded_options(param: str) -> None:
    """The installed version's ``update_database`` accepts the forwarded options."""
    params = inspect.signature(
        dt_ducklake_manager.DatabaseUpdater.update_database
    ).parameters

    assert param in params
