"""Shared DuckLake table helpers.

Gathers the create-then-upsert logic that every result-producing step of a
consuming pipeline used to duplicate: the download orchestrator
(:mod:`statflows.core.download`) and the downstream metric-computation steps.

Both helpers take an **already-open** DuckDB connection.

Only :func:`write_dataframe` needs ``dt_ducklake_manager``, which it imports
lazily: this module — and :func:`fact_table_exists` — stays importable without
the optional ``ducklake`` extra.
"""
# Importation des modules
from __future__ import annotations
# Modules de base
import logging
from typing import TYPE_CHECKING, Any, Optional, Sequence

# Module de manipulation de la base de données : usage purement annotatif, donc
# importé au seul typage (annotations différées par `from __future__`)
if TYPE_CHECKING:
    import duckdb

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de la table de faits DuckLake (convention dt_ducklake_manager)
FACT_TABLE = "fact_table"


# Fonction de détection de l'existence de la table de faits d'un schéma
def fact_table_exists(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    schema: str,
    *,
    table: str = FACT_TABLE,
) -> bool:
    """Return whether ``{schema}.{table}`` exists in the attached catalog.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias under which the catalog is attached.
        schema: Target schema.
        table: Table name to look for. Defaults to the DuckLake fact table.

    Returns:
        ``True`` if the table already exists in that schema.

    Examples:
        >>> import duckdb
        >>> conn = duckdb.connect(":memory:")
        >>> fact_table_exists(conn, "db", "vulnerabilities")
        False
        >>> conn.close()
    """
    # Introspection des tables du catalogue attaché
    row = conn.execute(
        "SELECT count(*) FROM duckdb_tables() "
        "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
        [catalog_alias, schema, table],
    ).fetchone()
    return bool(row and row[0] > 0)


# Fonction d'écriture d'un jeu de données dans un schéma DuckLake (création ou upsert)
def write_dataframe(
    conn: duckdb.DuckDBPyConnection,
    data: Any,
    primary_keys: Sequence[str],
    *,
    catalog_alias: str,
    schema: str,
    categorical_threshold: Optional[int] = None,
    label: Optional[str] = None,
) -> bool:
    """Create the schema on first encounter, upsert by primary key afterwards.

    The distinction is made on the sole existence of the fact table, so the same
    call initialises a brand-new schema and incrementally updates an existing
    one — no caller ever has to branch on it.

    Args:
        conn: Open DuckLake connection, owned by the caller.
        data: Dataset to persist. Any ``IntoDataFrame`` accepted by
            ``dt_ducklake_manager`` (pandas, polars or narwhals frame), passed
            through without conversion.
        primary_keys: Primary-key columns, used both to build the schema and to
            upsert onto it.
        catalog_alias: Alias under which the catalog is attached.
        schema: Target schema in the catalog.
        categorical_threshold: Maximum cardinality for a column to be turned
            into a dimension table. ``None`` disables dimension tables.
        label: Optional prefix identifying the run in the logs (dataflow,
            vintage…).

    Returns:
        ``True`` if the schema was created, ``False`` if it was upserted.

    Raises:
        ImportError: If the optional ``dt-ducklake-manager`` dependency is not
            installed.
        ValueError: If the update operation reports failure.
    """
    # Dépendance optionnelle : seule l'écriture DuckLake la requiert. Importation
    # en tête de corps, avant tout effet de bord sur la connexion.
    try:
        from dt_ducklake_manager import DatabaseUpdater, DuckLakeTablesBuilder
    except ImportError as exc:
        raise ImportError(
            "write_dataframe requires the optional 'dt-ducklake-manager' "
            "dependency. Install it with: pip install 'statflows[ducklake]'"
        ) from exc

    # Préfixe de journalisation : identifie le jeu de données écrit
    prefix = f"{label}: " if label else ""

    # Mise à jour de la table si elle existe déjà
    if fact_table_exists(conn, catalog_alias, schema):
        # Mise à jour incrémentale (upsert par clé primaire) : ne touche que
        # les lignes fournies, le reste de la table est préservé.
        updater = DatabaseUpdater(
            connection=conn,
            categorical_threshold=categorical_threshold,
            catalog_alias=catalog_alias,
            schema=schema,
        )
        success = updater.update_database(
            data,
            use_transaction=True,
            compact_after_update=True,
        )

        # Vérification de la bonne réalisation de la mise à jour
        if not success:
            raise ValueError(
                f"{prefix}DatabaseUpdater reported failure for schema '{schema}'"
            )

        # Logging
        logger.info(f"{prefix}Upserted {len(data)} rows into '{schema}'")
        return False

    # Première construction : métadonnées, dimensions et table de faits
    builder = DuckLakeTablesBuilder(
        data,
        categorical_threshold=categorical_threshold,
        primary_keys=list(primary_keys),
        connection=conn,
        schema=schema,
        catalog_alias=catalog_alias,
    )
    builder.build_schema()

    # Logging
    logger.info(
        f"{prefix}Created schema '{schema}' with {len(data)} rows "
        f"(primary keys: {list(primary_keys)})"
    )
    return True
