"""Test d'intégration — ``write_dataframe`` sur un catalogue DuckLake fichier.

Comportement figé : création de la table de faits au premier appel (→ ``True``),
upsert par clé primaire au second (→ ``False``), lignes non fournies préservées.

Requiert l'extra « ducklake » : la fixture ``ducklake_conn`` skippe sinon.
"""

from __future__ import annotations

import pandas as pd

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
