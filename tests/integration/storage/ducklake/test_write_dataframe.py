"""Test d'intégration — ``write_dataframe`` sur un catalogue DuckLake fichier.

Comportement figé : création de la table de faits au premier appel (→ ``True``),
upsert par clé primaire au second (→ ``False``), lignes non fournies préservées.

Requiert l'extra « ducklake » : la fixture ``ducklake_conn`` skippe sinon.
"""

from __future__ import annotations

import pandas as pd

from tests.utils.ducklake import file_backed_catalog
from statflows.storage.ducklake.tables import (
    FACT_TABLE,
    fact_table_exists,
    write_dataframe,
)


def test_create_then_upsert(ducklake_conn) -> None:
    conn, catalog_alias = ducklake_conn

    batch1 = pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]})
    batch2 = pd.DataFrame({"id": [2, 4], "val": [999, 40]})

    # Premier appel : la table de faits n'existe pas → création → True
    created = write_dataframe(
        conn, batch1, ["id"], catalog_alias=catalog_alias, schema="s1"
    )
    assert created is True
    assert fact_table_exists(conn, catalog_alias, "s1") is True

    # Second appel : la table existe → upsert → False
    upserted = write_dataframe(
        conn, batch2, ["id"], catalog_alias=catalog_alias, schema="s1"
    )
    assert upserted is False

    # Lignes préservées : id 1 et 3 intacts, id 2 mis à jour, id 4 inséré
    rows = conn.execute(
        f"SELECT id, val FROM {catalog_alias}.s1.{FACT_TABLE} ORDER BY id"
    ).fetchall()
    assert rows == [(1, 10), (2, 999), (3, 30), (4, 40)]


def test_non_default_alias_with_connection_on_another_catalog(tmp_path) -> None:
    """Alias ≠ ``"db"`` et connexion positionnée hors du catalogue cible.

    Garde-fou contre deux régressions de ``dt_ducklake_manager`` : un alias
    transmis sous un mauvais nom de paramètre, et un alias non propagé au
    constructeur (retombée silencieuse sur ``"db"`` ou sur le catalogue courant).
    """
    with file_backed_catalog(tmp_path, alias="lake") as (conn, alias):
        conn.execute("USE memory")

        batch1 = pd.DataFrame({"id": [1, 2], "val": [10, 20]})
        batch2 = pd.DataFrame({"id": [2, 3], "val": [200, 30]})

        assert (
            write_dataframe(conn, batch1, ["id"], catalog_alias=alias, schema="s1")
            is True
        )
        assert fact_table_exists(conn, alias, "s1") is True
        assert (
            write_dataframe(conn, batch2, ["id"], catalog_alias=alias, schema="s1")
            is False
        )

        # Données écrites dans le catalogue cible, upsert appliqué
        rows = conn.execute(
            f"SELECT id, val FROM {alias}.s1.{FACT_TABLE} ORDER BY id"
        ).fetchall()
        assert rows == [(1, 10), (2, 200), (3, 30)]

        # Rien n'a fui dans le catalogue courant, position de la session intacte
        assert fact_table_exists(conn, "memory", "s1") is False
        assert conn.execute("SELECT current_database()").fetchone() == ("memory",)
