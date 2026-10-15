"""Shared DuckLake table helpers.

Gathers the create-then-upsert logic that every result-producing step of a
consuming pipeline used to duplicate: the download orchestrator
(:mod:`statflows.core.download`) and the downstream metric-computation steps,
plus :func:`changes_since`, which reads DuckLake's change feed so that a
downstream step recomputes only what changed since its previous run.

Every helper takes an **already-open** DuckDB connection.

Only :func:`write_dataframe` / :func:`write_dataframe_with_report` need
``dt_ducklake_manager``, which they import lazily: this module — and
:func:`fact_table_exists` / :func:`changes_since` — stays importable without
the optional ``ducklake`` extra.
"""

# Importation des modules
from __future__ import annotations

# Modules de base
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

# Modules de manipulation de la base de données : usage purement annotatif, donc
# importés au seul typage (annotations différées par `from __future__`)
if TYPE_CHECKING:
    import duckdb
    from dt_ducklake_manager.reporting import OperationReport

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de la table de faits DuckLake (convention dt_ducklake_manager)
FACT_TABLE = "fact_table"
# Nom de la dimension temporelle SDMX
TIME_COLUMN = "TIME_PERIOD"
# Types de changement DuckLake décrivant l'état écrit d'une ligne
_WRITTEN_CHANGES = ("insert", "update_postimage")


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


# Fonction auxiliaire : options de commit DuckLake renseignées
def _commit_options(run_id: str | None, commit_message: str | None) -> dict[str, str]:
    """Keep the commit options that were actually provided.

    Args:
        run_id: Run identifier, or ``None``.
        commit_message: Commit message, or ``None``.

    Returns:
        Keyword arguments for ``update_database`` / ``build_schema``.
    """
    # Instanciation des options
    options = {"run_id": run_id, "commit_message": commit_message}
    return {name: value for name, value in options.items() if value is not None}


# Fonction d'écriture d'un jeu de données dans un schéma DuckLake (création ou upsert)
def write_dataframe(
    conn: duckdb.DuckDBPyConnection,
    data: Any,
    primary_keys: Sequence[str],
    *,
    catalog_alias: str,
    schema: str,
    categorical_threshold: int | None = None,
    label: str | None = None,
    update_options: Mapping[str, Any] | None = None,
    build_options: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    commit_message: str | None = None,
) -> bool:
    """Create the schema on first encounter, upsert by primary key afterwards.

    The distinction is made on the sole existence of the fact table, so the same
    call initialises a brand-new schema and incrementally updates an existing
    one — no caller ever has to branch on it.

    Each call is one DuckLake transaction, hence at least one snapshot and one
    Parquet file: batching rows into fewer, larger calls is what keeps the
    catalog small (see ``SDMXDownloader(write_batch_rows=..., ...)``).

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
        update_options: Extra keyword arguments forwarded to
            ``DatabaseUpdater.update_database`` (upsert path only), e.g.
            ``{"allow_new_columns": True}``. The post-commit compaction is left to
            the library default (``compact_after_update``); pass
            ``{"compact_after_update": False}`` to skip it, e.g. when writes are
            batched and compaction is better left to a single end-of-run or
            planned maintenance pass.
        build_options: Extra keyword arguments forwarded to
            ``DuckLakeTablesBuilder.build_schema`` (creation path only), e.g.
            ``{"partition_by": ["reporter"]}``.
        run_id: Run identifier recorded on the resulting DuckLake snapshot
            (``ducklake_set_commit_message``), on both paths. Takes precedence
            over the same key in ``update_options`` / ``build_options``.
        commit_message: Commit message recorded alongside ``run_id``; same
            precedence.

    Returns:
        ``True`` if the schema was created, ``False`` if it was upserted.

    Raises:
        ImportError: If the optional ``dt-ducklake-manager`` dependency is not
            installed.
        ValueError: If the update operation reports failure.
    """
    # Écriture déléguée, rapport d'opération ignoré
    return write_dataframe_with_report(
        conn,
        data,
        primary_keys,
        catalog_alias=catalog_alias,
        schema=schema,
        categorical_threshold=categorical_threshold,
        label=label,
        update_options=update_options,
        build_options=build_options,
        run_id=run_id,
        commit_message=commit_message,
    ).created


# Résultat d'une écriture DuckLake
@dataclass(frozen=True)
class WriteResult:
    """Outcome of :func:`write_dataframe_with_report`.

    Attributes:
        created: ``True`` if the schema was created, ``False`` if it was
            upserted.
        report: ``OperationReport`` of ``dt_ducklake_manager`` describing what
            the write actually did (``rows_inserted``, ``rows_updated``,
            ``snapshot_before``, ``snapshot_after``…), or ``None`` when the
            library did not provide one.
    """

    created: bool
    report: OperationReport | None

    # Nombre de lignes réellement insérées ou modifiées
    @property
    def rows_changed(self) -> int | None:
        """Rows inserted or updated by the write, ``None`` if unknown."""
        if self.report is None:
            return None
        return int(self.report.rows_inserted) + int(self.report.rows_updated)


# Fonction d'écriture d'un jeu de données, avec le rapport de l'opération
def write_dataframe_with_report(
    conn: duckdb.DuckDBPyConnection,
    data: Any,
    primary_keys: Sequence[str],
    *,
    catalog_alias: str,
    schema: str,
    categorical_threshold: int | None = None,
    label: str | None = None,
    update_options: Mapping[str, Any] | None = None,
    build_options: Mapping[str, Any] | None = None,
    run_id: str | None = None,
    commit_message: str | None = None,
) -> WriteResult:
    """Same as :func:`write_dataframe`, also returning the operation report.

    An upsert only rewrites the rows whose values actually change, so the
    report tells an effective write (``rows_inserted + rows_updated > 0``) from
    a no-op one, even though both commit a DuckLake snapshot.

    Args:
        conn: Open DuckLake connection, owned by the caller.
        data: Dataset to persist (see :func:`write_dataframe`).
        primary_keys: Primary-key columns.
        catalog_alias: Alias under which the catalog is attached.
        schema: Target schema in the catalog.
        categorical_threshold: See :func:`write_dataframe`.
        label: See :func:`write_dataframe`.
        update_options: See :func:`write_dataframe`.
        build_options: See :func:`write_dataframe`.
        run_id: See :func:`write_dataframe`.
        commit_message: See :func:`write_dataframe`.

    Returns:
        A :class:`WriteResult`.

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
        # Options transmises surchargeables ; les options de commit priment
        options: dict[str, Any] = dict(update_options or {})
        options.update(_commit_options(run_id, commit_message))
        success = updater.update_database(data, use_transaction=True, **options)

        # Vérification de la bonne réalisation de la mise à jour
        if not success:
            raise ValueError(
                f"{prefix}DatabaseUpdater reported failure for schema '{schema}'"
            )

        # Logging
        logger.info(f"{prefix}Upserted {len(data)} rows into '{schema}'")
        return WriteResult(created=False, report=getattr(updater, "last_report", None))

    # Première construction : métadonnées, dimensions et table de faits
    builder = DuckLakeTablesBuilder(
        data,
        categorical_threshold=categorical_threshold,
        primary_keys=list(primary_keys),
        connection=conn,
        schema=schema,
        catalog_alias=catalog_alias,
    )
    build_report = builder.build_schema(
        **{**(build_options or {}), **_commit_options(run_id, commit_message)}
    )

    # Logging
    logger.info(
        f"{prefix}Created schema '{schema}' with {len(data)} rows "
        f"(primary keys: {list(primary_keys)})"
    )
    return WriteResult(created=True, report=build_report)


# ──────────────────────────────────────────────────────────────────────
# Flux de changements DuckLake (conditionnement des étapes aval)
# ──────────────────────────────────────────────────────────────────────


# Fonction auxiliaire : littéral SQL d'une chaîne
def _sql_string(value: str) -> str:
    """Quote ``value`` as a SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


# Fonction auxiliaire : identifiant SQL quoté
def _sql_ident(name: str) -> str:
    """Quote ``name`` as a SQL identifier."""
    return '"' + name.replace('"', '""') + '"'


# Changements d'une table entre deux snapshots
@dataclass(frozen=True)
class TableChanges:
    """Row changes of a DuckLake table between two snapshots.

    Attributes:
        since_snapshot: Snapshot the changes are counted from (exclusive), as
            passed to :func:`changes_since`; ``None`` for "since the
            beginning".
        current_snapshot: Snapshot the changes are counted up to (inclusive):
            the ``until_snapshot`` passed to :func:`changes_since`, else the
            latest snapshot of the catalog when the changes were read. Store it
            as the downstream step's new watermark once its recomputation
            succeeds.
        rows_inserted: Rows inserted.
        rows_updated: Rows updated (post-images).
        rows_deleted: Rows deleted.
        min_time_period: Smallest value of the time column among the inserted,
            updated (before and after the update) and deleted rows — the
            earliest period a downstream step has to recompute from. ``None``
            when nothing changed or the table has no time column.
        key_columns: Columns whose distinct values were collected (the
            ``key_columns`` argument, in the table's spelling); empty when
            none was requested.
        changed_keys: Distinct tuples of ``key_columns`` values among the
            inserted, updated (before and after the update) and deleted rows;
            ``None`` when no key column was requested. Lets a downstream step
            recompute only the slices (vintage x period, reporter x
            product…) actually touched.
    """

    since_snapshot: int | None
    current_snapshot: int | None
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    min_time_period: Any = None
    key_columns: tuple[str, ...] = ()
    changed_keys: frozenset[tuple[Any, ...]] | None = None

    # Indicateur de modification effective
    @property
    def modified(self) -> bool:
        """Whether at least one row was inserted, updated or deleted."""
        return (self.rows_inserted + self.rows_updated + self.rows_deleted) > 0


# Fonction de lecture du snapshot courant d'un catalogue
def current_snapshot(conn: duckdb.DuckDBPyConnection, catalog_alias: str) -> int | None:
    """Return the latest snapshot id of an attached DuckLake catalog.

    Read it once at the start of a downstream step to pin every read of the
    step at that snapshot (``... AT (VERSION => snapshot)``) and pass it as
    ``until_snapshot`` to :func:`changes_since`: the step then computes on,
    and records as its watermark, exactly the same state, even while another
    process keeps writing.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias under which the catalog is attached.

    Returns:
        The snapshot id, or ``None`` when the catalog has none.

    Examples:
        >>> pinned = current_snapshot(conn, "db")  # doctest: +SKIP
        >>> changes = changes_since(conn, "db", "s1", watermark,
        ...                         until_snapshot=pinned)  # doctest: +SKIP
    """
    # Requête SQL
    row = conn.execute(
        f"SELECT * FROM ducklake_current_snapshot({_sql_string(catalog_alias)})"
    ).fetchone()
    return int(row[0]) if row is not None and row[0] is not None else None


# Fonction de résolution insensible à la casse de colonnes demandées
def _resolve_columns(
    columns: Sequence[str], requested: Sequence[str], qualified: str
) -> tuple[str, ...]:
    """Map requested column names to the table's spelling (case-insensitive).

    Args:
        columns: Columns of the table.
        requested: Requested column names.
        qualified: Qualified table name, for the error message.

    Returns:
        The table's spelling of each requested column, in request order.

    Raises:
        ValueError: If a requested column is absent from the table.
    """
    # Unicisation en retirant les majuscules
    by_lower = {column.lower(): column for column in columns}
    # Identification des colonnes manquantes
    missing = [name for name in requested if name.lower() not in by_lower]
    if missing:
        raise ValueError(f"Key column(s) {missing} not found in {qualified}")
    return tuple(by_lower[name.lower()] for name in requested)


# Fonction de lecture des changements d'une table depuis un snapshot
def changes_since(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    schema: str,
    since_snapshot: int | None,
    *,
    table: str = FACT_TABLE,
    time_column: str = TIME_COLUMN,
    key_columns: Sequence[str] | None = None,
    until_snapshot: int | None = None,
) -> TableChanges:
    """Summarise the row changes of a DuckLake table since a snapshot.

    Meant for the downstream steps of a pipeline: each step keeps its own
    watermark (the ``current_snapshot`` returned by its previous successful
    call), skips its computation when nothing changed, and otherwise
    recomputes from ``min_time_period`` only — or, with ``key_columns``, only
    the slices listed in ``changed_keys``. Unlike a date in the download
    registry, the watermark belongs to the consumer: a skipped or failed run
    loses nothing, the next call covers every snapshot since.

    Changes are read from ``ducklake_table_changes``, so a snapshot that only
    stamped metadata (an upsert whose rows were all unchanged) counts as no
    change.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias under which the catalog is attached.
        schema: Schema of the table (one per dataflow for the downloads).
        since_snapshot: Watermark: changes committed *after* this snapshot are
            counted. ``None`` counts every row of the table (as of the upper
            bound) as inserted (first run).
        table: Table to inspect. Defaults to the DuckLake fact table.
        time_column: Column whose minimum is reported, matched
            case-insensitively. Defaults to ``TIME_PERIOD``.
        key_columns: Columns whose distinct value tuples among the changed
            rows are returned in ``changed_keys``, matched case-insensitively.
            ``None`` (default) skips that query. With ``since_snapshot=None``
            every distinct tuple of the table is returned: choose coarse
            columns (a vintage, a period), not the primary key of a large
            table.
        until_snapshot: Upper bound of the changes (inclusive), typically the
            snapshot the step pinned its reads at (:func:`current_snapshot`).
            ``None`` uses the latest snapshot of the catalog.

    Returns:
        A :class:`TableChanges`. A missing table yields no change (and an
        empty ``changed_keys`` when key columns were requested).

    Raises:
        ValueError: If ``since_snapshot`` is ahead of the upper bound, if
            ``until_snapshot`` is ahead of the catalog, if a key column is
            missing, or if the snapshots following ``since_snapshot`` have
            been expired (``ducklake_expire_snapshots``): the changes can no
            longer be known and the caller must recompute everything (then
            restart from ``changes_since(..., None)``).

    Examples:
        >>> changes = changes_since(conn, "db", "namq_10_gdp", watermark)  # doctest: +SKIP
        >>> if changes.modified:  # doctest: +SKIP
        ...     recompute(start_period=changes.min_time_period)
        ...     save_watermark(changes.current_snapshot)
        >>> changes = changes_since(
        ...     conn, "db", "indicators", watermark,
        ...     key_columns=["classification", "TIME_PERIOD"],
        ... )  # doctest: +SKIP
        >>> sorted(changes.changed_keys)  # doctest: +SKIP
        [('HS2022', '2023'), ('HS2022', '2024')]
    """
    # Import paresseux
    import duckdb

    # Borne supérieure : snapshot épinglé par l'appelant, sinon snapshot courant
    latest = current_snapshot(conn, catalog_alias)
    if until_snapshot is not None and (latest is None or until_snapshot > latest):
        raise ValueError(
            f"until_snapshot {until_snapshot} is ahead of the current snapshot "
            f"{latest} of catalog '{catalog_alias}'"
        )
    current = until_snapshot if until_snapshot is not None else latest
    requested_keys = tuple(key_columns) if key_columns is not None else None
    # Ensemble vide de clés si des clés sont demandées, None sinon
    no_keys: frozenset[tuple[Any, ...]] | None = (
        frozenset() if requested_keys is not None else None
    )

    # Table absente : aucun changement observable
    if current is None or not fact_table_exists(
        conn, catalog_alias, schema, table=table
    ):
        return TableChanges(
            since_snapshot=since_snapshot,
            current_snapshot=current,
            key_columns=requested_keys or (),
            changed_keys=no_keys,
        )

    # Résolution insensible à la casse de la colonne temporelle et des clés
    qualified = ".".join(_sql_ident(p) for p in (catalog_alias, schema, table))
    columns = [
        name
        for (name,) in conn.execute(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
            [catalog_alias, schema, table],
        ).fetchall()
    ]
    time_actual = next((c for c in columns if c.lower() == time_column.lower()), None)
    min_expr = f"min({_sql_ident(time_actual)})" if time_actual else "NULL"
    keys = (
        _resolve_columns(columns, requested_keys, qualified)
        if requested_keys is not None
        else ()
    )
    keys_sql = ", ".join(_sql_ident(k) for k in keys)

    # Premier passage : toutes les lignes présentes à la borne comptent comme insérées
    if since_snapshot is None:
        source = f"{qualified} AT (VERSION => {current})"
        count, minimum = conn.execute(
            f"SELECT count(*), {min_expr} FROM {source}"
        ).fetchone() or (0, None)
        changed_keys = (
            frozenset(
                conn.execute(f"SELECT DISTINCT {keys_sql} FROM {source}").fetchall()
            )
            if keys
            else no_keys
        )
        return TableChanges(
            since_snapshot=None,
            current_snapshot=current,
            rows_inserted=int(count),
            min_time_period=minimum if count else None,
            key_columns=keys,
            changed_keys=changed_keys,
        )

    # Point de reprise à jour (ou en avance, signe d'une incohérence)
    if since_snapshot > current:
        raise ValueError(
            f"since_snapshot {since_snapshot} is ahead of the current snapshot "
            f"{current} of catalog '{catalog_alias}'"
        )
    if since_snapshot == current:
        return TableChanges(
            since_snapshot=since_snapshot,
            current_snapshot=current,
            key_columns=keys,
            changed_keys=no_keys,
        )

    # Lecture du flux de changements : comptages par type et clés distinctes
    feed = (
        f"ducklake_table_changes({_sql_string(catalog_alias)}, "
        f"{_sql_string(schema)}, {_sql_string(table)}, "
        f"{since_snapshot + 1}, {current})"
    )
    try:
        rows = conn.execute(
            f"SELECT change_type, count(*), {min_expr} FROM {feed} GROUP BY change_type"
        ).fetchall()
        changed_keys = (
            frozenset(
                conn.execute(f"SELECT DISTINCT {keys_sql} FROM {feed}").fetchall()
            )
            if keys
            else no_keys
        )
    except duckdb.Error as exc:
        raise ValueError(
            f"Changes of '{schema}.{table}' since snapshot {since_snapshot} are "
            f"no longer available (expired snapshots?): recompute everything "
            f"and restart from changes_since(..., None). Cause: {exc}"
        ) from exc

    # Comptages par type et minimum de la période sur toutes les lignes touchées
    counts = {change_type: int(count) for change_type, count, _ in rows}
    minima = [minimum for _, _, minimum in rows if minimum is not None]
    return TableChanges(
        since_snapshot=since_snapshot,
        current_snapshot=current,
        rows_inserted=counts.get("insert", 0),
        rows_updated=counts.get("update_postimage", 0),
        rows_deleted=counts.get("delete", 0),
        min_time_period=min(minima) if minima else None,
        key_columns=keys,
        changed_keys=changed_keys,
    )


# Fonction d'attribution des lignes écrites entre deux snapshots à des lots de clés
def _written_key_groups(
    conn: duckdb.DuckDBPyConnection,
    catalog_alias: str,
    schema: str,
    start_snapshot: int,
    end_snapshot: int,
    keys: Any,
    primary_keys: Sequence[str],
    group_column: str,
    *,
    table: str = FACT_TABLE,
) -> set[Any]:
    """Return the groups of ``keys`` whose rows were written in a snapshot range.

    Joins the inserted and updated rows of ``ducklake_table_changes`` between
    ``start_snapshot`` and ``end_snapshot`` (both inclusive) with ``keys`` on
    the primary keys.

    Args:
        conn: Open DuckLake connection.
        catalog_alias: Alias under which the catalog is attached.
        schema: Schema of the table.
        start_snapshot: First snapshot of the range (inclusive).
        end_snapshot: Last snapshot of the range (inclusive).
        keys: pandas DataFrame holding ``primary_keys`` and ``group_column``.
        primary_keys: Primary-key columns.
        group_column: Column of ``keys`` identifying each group.
        table: Table to inspect. Defaults to the DuckLake fact table.

    Returns:
        Distinct values of ``group_column`` having at least one written row.

    Raises:
        duckdb.Error: If the change feed cannot be read or joined.
    """
    # Enregistrement des clés comme vue temporaire
    view_name = "_statflows_written_keys"
    join_condition = " AND ".join(
        f"c.{_sql_ident(k)} = k.{_sql_ident(k)}" for k in primary_keys
    )
    change_types = ", ".join(_sql_string(t) for t in _WRITTEN_CHANGES)
    conn.register(view_name, keys)
    try:
        rows = conn.execute(
            f"SELECT DISTINCT k.{_sql_ident(group_column)} "
            f"FROM ducklake_table_changes({_sql_string(catalog_alias)}, "
            f"{_sql_string(schema)}, {_sql_string(table)}, "
            f"{start_snapshot}, {end_snapshot}) c "
            f"JOIN {view_name} k ON {join_condition} "
            f"WHERE c.change_type IN ({change_types})"
        ).fetchall()
    finally:
        # Suppression de la vue, succès comme échec
        conn.unregister(view_name)
    return {value for (value,) in rows}
