"""Incremental SDMX → DuckLake download orchestrator.

Production entry point that, for a list of provider queries:

1. Initialises (or reuses) a per-provider DuckLake catalog of downloads,
2. Performs a first **full** download of missing series and an **incremental**
   download of series already present (only newly published observations),
3. Persists a JSON registry of the last-download date per query (single file
   or fragments, see :mod:`statflows.core.registry`),
4. Exports the provider structure registry when a new structure was fetched,
5. Stops gracefully after a configurable runtime (e.g. 23 h) or on ``SIGTERM``,
6. Closes the catalog connection cleanly.

Both the registry persistence and the DuckLake writes can be buffered (see
:class:`SDMXDownloader`), under one invariant: a registry entry never moves
forward before the data of its query has been written successfully.

The orchestration is provider-agnostic: *how* to fetch the incremental data is
delegated to each client through
:meth:`~statflows.core.client.AbstractSDMXClient.fetch_updates`
"""

# Importation des modules
from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    cast,
)

import duckdb
import pandas as pd

# Importation du connecteur à la base de données
from dt_ducklake_manager import DuckLakeConnector

# Helper DuckLake partagé (création puis upsert de la table de faits)
from ..storage.ducklake.tables import write_dataframe

# Importation des modules de connexion
from ..storage.json import Loader, Saver
from .client import AbstractSDMXClient

# Registre des dates de dernier téléchargement (lecture publique réexportée ici)
from .registry import (
    REGISTRY_ROOT,
    DownloadRegistry,
    RegistryEntry,  # noqa: F401
    _parse_iso,
    iter_registry_entries,  # noqa: F401
    sanitize_shard,
)
from .reports import DownloadReport, HttpStats, QueryReport, RateLimitStats
from .structures import DataflowStructure, DataflowStructureRegistry

# Initialisation du logger
logger = logging.getLogger(__name__)

# Nom de la dimension temporelle SDMX (incluse dans la clé primaire)
_TIME_COLUMN = "TIME_PERIOD"
# Clé racine du registre JSON des dates de dernier téléchargement
_REGISTRY_ROOT = REGISTRY_ROOT
# Nom de l'option DuckLake d'ATTACH contrôlant l'inlining des petites écritures
_INLINING_OPTION = "data_inlining_row_limit"


# ──────────────────────────────────────────────────────────────────────
# Fonctions utilitaires
# ──────────────────────────────────────────────────────────────────────


# Fonction de récupération de l'instant courant en UTC
def _now() -> datetime:
    """Return the current instant as a UTC-aware datetime."""
    return datetime.now(UTC)


# Fonction de normalisation d'un nom de dataflow en identifiant de schéma SQL
def _schema_name(dataflow: str) -> str:
    """Sanitise a dataflow identifier into a safe DuckLake schema name.

    Non-alphanumeric characters (e.g. ``@``, ``-``, ``.`` in
    ``"DSD_KEI@DF_KEI"``) are replaced by underscores, and a leading digit is
    prefixed so the result is a valid unquoted SQL identifier.

    Args:
        dataflow: Dataflow identifier.

    Returns:
        Sanitised schema name.

    Examples:
        >>> _schema_name("DSD_KEI@DF_KEI")
        'DSD_KEI_DF_KEI'
        >>> _schema_name("namq_10_gdp")
        'namq_10_gdp'
    """
    # Remplacement de tout caractère non alphanumérique par un underscore
    sanitized = "".join(c if c.isalnum() or c == "_" else "_" for c in dataflow)
    # Préfixe si l'identifiant commence par un chiffre (interdit en SQL non quoté)
    if sanitized and sanitized[0].isdigit():
        sanitized = f"df_{sanitized}"
    return sanitized


# Fonction de calcul des clés primaires d'une table à partir de sa structure
def _primary_keys(
    structure: DataflowStructure | None,
    df_columns: list[str],
) -> list[str]:
    """Derive the primary-key columns of the DuckLake table.

    The primary key is the set of dataflow dimensions (which uniquely identify
    an observation) intersected with the columns actually present in the
    DataFrame, matched case-insensitively. The time dimension
    (``TIME_PERIOD``) is always appended when present, guaranteeing
    observation uniqueness even if it is not listed among the structure
    dimensions.

    Args:
        structure: Resolved dataflow structure (may be ``None``).
        df_columns: Columns of the retrieved DataFrame.

    Returns:
        Ordered list of primary-key column names (using the DataFrame's
        original casing).
    """
    # Index insensible à la casse : nom en minuscules → nom réel de la colonne
    cols_lower = {c.lower(): c for c in df_columns}
    primary_keys: list[str] = []

    # Dimensions de la structure présentes dans le DataFrame
    if structure is not None:
        for dim in structure.dimensions:
            actual = cols_lower.get(dim.name.lower())
            if actual is not None and actual not in primary_keys:
                primary_keys.append(actual)

    # Ajout systématique de la dimension temporelle si présente
    time_actual = cols_lower.get(_TIME_COLUMN.lower())
    if time_actual is not None and time_actual not in primary_keys:
        primary_keys.append(time_actual)

    return primary_keys


# Fonction de conversion d'une valeur en représentation JSON-sérialisable
def _json_safe(value: Any) -> Any:
    """Recursively convert enums and datetimes into JSON-serialisable values.

    Args:
        value: Arbitrary value (possibly nested) from a query ``to_dict()``.

    Returns:
        JSON-serialisable equivalent.
    """
    # Enum → valeur sous-jacente ; datetime → ISO ; conteneurs → récursion
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


# ──────────────────────────────────────────────────────────────────────
# Diagnostics d'exécution
# ──────────────────────────────────────────────────────────────────────
#
# `DownloadReport` et `QueryReport` vivent dans `reports.py` : ils constituent
# le pendant exploitable des journaux, réutilisable par un projet consommateur
# sans dépendre de l'orchestrateur. Ils sont réexportés ici par compatibilité.


# Fonction auxiliaire : sources de compteurs HTTP portées par un client
def _http_stats_sources(client: Any) -> list[HttpStats]:
    """Collect every HTTP counter object a client carries.

    Providers either *are* an ``APIClient`` (Comtrade, UNSD) or *hold* one or
    more (Eurostat, OECD, plus Eurostat's dedicated Comext client). Discovering
    them by attribute keeps the orchestrator provider-agnostic.

    Args:
        client: Provider client.

    Returns:
        The :class:`HttpStats` instances found, possibly empty.
    """
    # Compteurs portés par le client lui-même (héritage d'APIClient)
    sources: list[HttpStats] = []
    own = getattr(client, "stats_", None)
    if isinstance(own, HttpStats):
        sources.append(own)
    # Compteurs des clients HTTP détenus par composition
    for attribute in vars(client).values():
        stats = getattr(attribute, "stats_", None)
        if isinstance(stats, HttpStats):
            sources.append(stats)
    return sources


# Fonction auxiliaire : totalisation de compteurs HTTP
def _total_http_stats(sources: Iterable[HttpStats]) -> HttpStats:
    """Sum several HTTP counter objects into one.

    Args:
        sources: Counter objects to total.

    Returns:
        A detached :class:`HttpStats` holding the sums.
    """
    # Totalisation champ à champ, histogramme de statuts compris
    total = HttpStats()
    for stats in sources:
        total.n_requests += stats.n_requests
        total.n_failures += stats.n_failures
        total.total_seconds += stats.total_seconds
        total.total_bytes += stats.total_bytes
        for status, count in stats.status_counts.items():
            total.status_counts[status] = total.status_counts.get(status, 0) + count
    return total


# Fonction auxiliaire : consommation HTTP d'une requête (différence de compteurs)
def _http_delta(before: HttpStats, after: HttpStats) -> HttpStats:
    """Difference two HTTP snapshots to isolate one query's cost.

    Args:
        before: Snapshot taken before the query.
        after: Snapshot taken after it.

    Returns:
        A :class:`HttpStats` holding what the query alone consumed.
    """
    # Différence champ à champ ; les statuts absents avant valent zéro
    return HttpStats(
        n_requests=after.n_requests - before.n_requests,
        n_failures=after.n_failures - before.n_failures,
        total_seconds=after.total_seconds - before.total_seconds,
        total_bytes=after.total_bytes - before.total_bytes,
        status_counts={
            status: count - before.status_counts.get(status, 0)
            for status, count in after.status_counts.items()
            if count - before.status_counts.get(status, 0) > 0
        },
    )


# Fonction auxiliaire : consommation du limiteur de débit d'une requête
def _rate_limit_delta(before: RateLimitStats, after: RateLimitStats) -> RateLimitStats:
    """Difference two rate-limit snapshots to isolate one query's waits.

    Args:
        before: Snapshot taken before the query.
        after: Snapshot taken after it.

    Returns:
        A :class:`RateLimitStats` holding what the query alone incurred.
    """
    # Attentes et acquisitions imputables à la requête
    return RateLimitStats(
        n_acquisitions=after.n_acquisitions - before.n_acquisitions,
        total_wait_seconds=after.total_wait_seconds - before.total_wait_seconds,
        max_wait_seconds=after.max_wait_seconds,
        remaining_requests=after.remaining_requests,
    )


# Fonction auxiliaire : instantané des compteurs d'un client
def _client_snapshot(client: Any) -> tuple[HttpStats, RateLimitStats]:
    """Snapshot the HTTP and rate-limit counters of a client.

    Args:
        client: Provider client.

    Returns:
        Tuple ``(http, rate_limit)`` of detached counter objects.
    """
    # Compteurs HTTP totalisés sur toutes les sessions du client
    http = _total_http_stats(_http_stats_sources(client))
    # Compteurs du limiteur de débit, absent chez certains providers
    limiter = getattr(client, "rate_limiter", None)
    rate_limit = limiter.get_stats() if limiter is not None else RateLimitStats()
    return http, rate_limit


# ──────────────────────────────────────────────────────────────────────
# Tampons internes
# ──────────────────────────────────────────────────────────────────────


# Exception d'interruption levée par le gestionnaire de SIGTERM
class _GracefulStop(BaseException):
    """Abort the fetch in progress after a ``SIGTERM``.

    Derives from :class:`BaseException` so the per-query ``except Exception``
    isolation never swallows it. Only ever raised while the orchestrator is
    waiting on the provider (never during a DuckLake write or a registry
    flush), so the interrupted query simply stays unrecorded.
    """


# Requête dont les données attendent l'écriture de leur lot
@dataclass
class _PendingWrite:
    """A fetched, non-empty query waiting for its write batch.

    Attributes:
        query_report: Diagnostics, published once the batch is committed or
            has failed.
        key: Identity key of the query.
        entry: Registry entry to commit once the batch is written.
        shard: Registry fragment of the entry (sharded mode), else ``None``.
        df: Fetched data.
    """

    query_report: QueryReport
    key: str
    entry: dict[str, Any]
    shard: str | None
    df: pd.DataFrame


# Lot en attente d'un schéma DuckLake
@dataclass
class _SchemaBuffer:
    """Write batch accumulated for one DuckLake schema (one dataflow).

    Attributes:
        dataflow: Dataflow identifier (logging, primary keys).
        structure: Dataflow structure used to resolve the primary keys.
        items: Pending queries, in processing order.
        rows: Total rows buffered.
    """

    dataflow: str
    structure: DataflowStructure | None
    items: list[_PendingWrite] = field(default_factory=list)
    rows: int = 0


# Marqueur : aucun gestionnaire de signal installé par run()
_NO_HANDLER = object()


# ──────────────────────────────────────────────────────────────────────
# Orchestrateur
# ──────────────────────────────────────────────────────────────────────


# Classe orchestrant le téléchargement incrémental vers DuckLake
class SDMXDownloader:
    """Incremental SDMX → DuckLake download orchestrator.

    Drives the full run for a single provider: prioritises the queries,
    fetches each one (full or incremental via
    :meth:`AbstractSDMXClient.fetch_updates`), writes the result into the
    provider's DuckLake catalog (one schema per dataflow), and maintains the
    JSON registry of last-download dates.

    The orchestrator owns the DuckLake connection (opened from the supplied
    connector and closed in a ``finally`` block) but **not** the SDMX client,
    whose lifetime is managed by the caller.

    **Buffering.** By default every query is written to DuckLake on its own and
    the whole registry is rewritten after each query — safe, but quadratic on
    10⁴-10⁵ queries. ``registry_flush_every`` / ``registry_flush_seconds``
    space out the registry writes; ``write_batch_rows`` /
    ``write_batch_queries`` accumulate the DataFrames per schema and write them
    in one transaction. Whatever the settings, a registry entry never moves
    forward before its data has been written: the entries of a buffered batch
    are committed only after the batch write succeeds, and a failed batch
    leaves all of them untouched (each of its queries is counted as an error;
    the run goes on). Pending batches and the registry are flushed at the end of
    the run, on the deadline, on ``SIGTERM`` and on any exception.

    **SIGTERM.** While :meth:`run` executes in the main thread, a ``SIGTERM``
    handler is installed (the previous one is restored on exit): a fetch in
    progress is aborted (that query is retried next run), pending batches and
    the registry are flushed, and :meth:`run` returns its report with
    ``stopped_early=True``. Outside the main thread no handler can be
    installed; a warning is logged.

    Args:
        client: Provider SDMX client (e.g. ``EurostatClient``, ``OECDClient``).
        connector: DuckLake connector for the provider catalog. Opened once and
            closed by :meth:`run`; the schema is selected per dataflow.
        structures_path: Path to the JSON structure registry of the provider.
        last_download_path: Path to the JSON registry of last-download dates.
        n_observations: Number of most-recent observations to retrieve in
            incremental mode for providers without a per-observation filter
            (Eurostat).
        fresh_registry: When ``True``, start from an empty structure registry
            instead of loading ``structures_path``.
        max_runtime: Graceful-shutdown delay. The loop stops between two
            queries once this much wall-clock time has elapsed. ``None``
            disables the deadline.
        categorical_threshold: ``categorical_threshold`` forwarded to
            dt_ducklake_manager. ``None`` (default) disables dimension tables.
        bucket: Optional S3 bucket name. When provided, the structure and
            last-download JSON registries are read from and written to S3
            (the registry paths are used as object keys); when ``None``
            (default) they live on the local filesystem.
        storage_options: Optional keyword arguments forwarded to the
            ``Loader``/``Saver`` ``connect()`` method (e.g. ``endpoint_url``,
            ``aws_access_key_id``, ``aws_secret_access_key``, ``verify``). Only
            used when ``bucket`` is set; if omitted, the connection is
            established lazily from the standard AWS environment variables.
        on_query_complete: Callback invoked with the :class:`QueryReport` of
            every processed query, successful or not. The seam through which a
            consuming project streams per-query metrics to its experiment
            tracker without this package ever knowing about it. A failing
            callback warns and never interrupts the download. With write
            batching, a buffered query is reported when its batch completes.
        registry_flush_every: Persist the registry once this many entries
            have been committed since the previous flush. ``1`` (default)
            persists after every query, as historically.
        registry_flush_seconds: Also persist the registry when this many
            seconds have elapsed since the previous flush (checked at each
            commit). ``None`` (default) disables the time threshold.
        registry_shard_key: Optional callable ``query → str`` naming the
            registry fragment of a query (e.g. ``lambda q: q.dataflow``). When
            set, the registry is stored as
            ``<last_download_path without extension>/<fragment>.json`` and only
            the fragments touched since the previous flush are rewritten. An
            existing single-file registry is migrated at the first flush
            (entries of queries absent from the run go to ``_default.json``);
            the historical file is left in place and is harmless, since readers
            keep the most recent date per entry. Read the registry with
            :func:`iter_registry_entries`, whatever its layout.
        write_batch_rows: Write a schema's pending batch once it holds at
            least this many rows. ``None`` disables this threshold.
        write_batch_queries: Write a schema's pending batch once it holds at
            least this many non-empty queries. ``None`` disables this
            threshold. With both thresholds ``None`` (default), every
            non-empty query is written immediately, as historically. A batch of
            several queries is the concatenation of their DataFrames,
            deduplicated on the primary key (last query wins) before writing.
        update_options: Forwarded to
            :func:`~statflows.storage.ducklake.tables.write_dataframe`, then to
            ``DatabaseUpdater.update_database`` (e.g. ``allow_new_columns``).
            Post-commit compaction follows the library default; pass
            ``{"compact_after_update": False}`` to disable it per batch.
        build_options: Forwarded to ``write_dataframe``, then to
            ``DuckLakeTablesBuilder.build_schema`` (e.g. ``partition_by``).
        ducklake_options: Optional DuckLake options applied for the duration
            of :meth:`run`'s ``connect()``, without modifying the connector
            durably. The ``data_inlining_row_limit`` key becomes the
            ``DATA_INLINING_ROW_LIMIT`` ATTACH option (small writes kept in the
            catalog instead of a Parquet file); any other key is merged into
            the connector's post-ATTACH ``ducklake_options`` (``set_option``).
        run_id: Optional run identifier recorded on every DuckLake snapshot
            written by the run, with a ``"statflows <dataflow>"`` commit
            message.

    Raises:
        ValueError: If a buffering threshold is not strictly positive.
    """

    # Initialisation
    def __init__(
        self,
        client: AbstractSDMXClient,
        connector: DuckLakeConnector,
        structures_path: str | Path,
        last_download_path: str | Path,
        *,
        n_observations: int = 10,
        fresh_registry: bool = False,
        max_runtime: timedelta | None = timedelta(hours=23),
        categorical_threshold: int | None = None,
        bucket: str | None = None,
        storage_options: dict[str, Any] | None = None,
        on_query_complete: Callable[[QueryReport], None] | None = None,
        registry_flush_every: int = 1,
        registry_flush_seconds: float | None = None,
        registry_shard_key: Callable[[Any], str] | None = None,
        write_batch_rows: int | None = None,
        write_batch_queries: int | None = None,
        update_options: Mapping[str, Any] | None = None,
        build_options: Mapping[str, Any] | None = None,
        ducklake_options: dict[str, Any] | None = None,
        run_id: str | None = None,
    ) -> None:
        # Validation des seuils de tamponnage
        if registry_flush_every < 1:
            raise ValueError("registry_flush_every must be >= 1")
        for name, value in (
            ("registry_flush_seconds", registry_flush_seconds),
            ("write_batch_rows", write_batch_rows),
            ("write_batch_queries", write_batch_queries),
        ):
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be > 0 or None")

        # Dépendances injectées
        self._client = client
        self._connector = connector
        self._structures_path = Path(structures_path)
        self._last_download_path = Path(last_download_path)

        # Paramètres d'exécution
        self._n_observations = n_observations
        self._max_runtime = max_runtime
        self._categorical_threshold = categorical_threshold
        self._update_options = update_options
        self._build_options = build_options
        self._ducklake_options = ducklake_options
        self._run_id = run_id

        # Paramètres de tamponnage du registre et des écritures
        self._registry_flush_every = registry_flush_every
        self._registry_flush_seconds = registry_flush_seconds
        self._shard_key = registry_shard_key
        self._write_batch_rows = write_batch_rows
        self._write_batch_queries = write_batch_queries
        self._batching = write_batch_rows is not None or write_batch_queries is not None

        # Accès au stockage des registres JSON (local ou S3 selon ``bucket``).
        # Instances réutilisables : la connexion S3 paresseuse est ainsi établie
        # une seule fois et partagée par toutes les lectures/écritures.
        self._bucket = bucket
        self._loader = Loader()
        self._saver = Saver()
        # Connexion explicite uniquement si des options sont fournies ; sinon la
        # connexion reste paresseuse (variables d'environnement AWS au 1er accès).
        if bucket is not None and storage_options:
            self._loader.connect(**storage_options)
            self._saver.connect(**storage_options)

        # Alias du catalogue DuckLake (pour les requêtes d'introspection)
        self._catalog_alias = connector.catalog_alias

        # Registre des dates de dernier téléchargement (deux formats lus)
        self._registry_store = DownloadRegistry(
            self._last_download_path,
            loader=self._loader,
            saver=self._saver,
            bucket=self._bucket,
            sharded=registry_shard_key is not None,
        )
        self._load_registry()

        # Registre des structures, injecté dans le client pour mutualiser le cache
        self._structure_registry = DataflowStructureRegistry()
        if not fresh_registry:
            structures_data = self._loader.load(
                self._structures_path, bucket=self._bucket, missing_ok=True
            )
            if structures_data:
                self._structure_registry.load_from_dict(structures_data)
        self._client.structure_registry = self._structure_registry

        # Rappel de fin de requête (seam d'observabilité côté appelant)
        self._on_query_complete = on_query_complete

        # Instant de démarrage (renseigné dans run())
        self._t0: datetime | None = None

        # État d'exécution (réinitialisé par run())
        self._buffers: dict[str, _SchemaBuffer] = {}
        self._shard_by_key: dict[str, str] = {}
        self._commits_since_flush = 0
        self._last_flush = time.monotonic()
        self._stop_requested = False
        self._interruptible = False

    # Registre validé (identity_key → entrée) : ne contient que des entrées
    # dont les données ont été écrites
    @property
    def _registry(self) -> dict[str, dict[str, Any]]:
        """Committed registry entries, keyed by identity key."""
        return self._registry_store.entries

    # Méthode principale d'exécution du téléchargement
    def run(
        self,
        queries: Any | Iterable[Any],
    ) -> DownloadReport:
        """Run the download for the provided queries.

        Args:
            queries: A single provider query or an iterable/iterator of
                queries. Each query must expose ``identity_key()``,
                ``to_dict()``, ``dataflow`` and ``agency``.

        Returns:
            A :class:`DownloadReport` summarising the run.
        """
        # Normalisation en liste (un objet requête isolé est accepté)
        query_list = self._as_query_list(queries)

        # Priorisation : jamais téléchargées d'abord, puis les plus anciennes
        query_list = self._prioritize(query_list)

        # Fragments du registre : calculés une fois pour toutes les requêtes du
        # run, ce qui range aussi les entrées héritées du fichier unique
        if self._shard_key is not None:
            self._shard_by_key = {
                query.identity_key(): sanitize_shard(self._shard_key(query))
                for query in query_list
            }
            self._registry_store.assign_shards(self._shard_by_key)

        # Initialisation du chronomètre de graceful shutdown
        self._t0 = _now()

        # Initialisation du rapport de téléchargement
        report = DownloadReport(n_queries_planned=len(query_list))

        # Extraction des structures connues à l'initialisation : tout ajout
        # (par fetch_updates ou par la résolution explicite) déclenchera
        # l'export du registre en fin de run
        initial_structure_keys = set(self._structure_registry.list_structures())

        # Réinitialisation de l'état de tamponnage et d'arrêt
        self._buffers = {}
        self._commits_since_flush = 0
        self._last_flush = time.monotonic()
        self._stop_requested = False
        self._interruptible = False

        # Gestionnaire de SIGTERM, restauré quoi qu'il arrive
        previous_handler = self._install_signal_handler()
        try:
            # Ouverture de la connexion au catalogue DuckLake
            conn = self._connect()
            try:
                self._run_queries(conn, query_list, report)
            finally:
                try:
                    # Écriture des lots en attente (fin, deadline, SIGTERM, erreur)
                    self._flush_all_buffers(conn, report)
                finally:
                    # Lignes restées en tampon (0 attendu)
                    report.rows_pending_at_stop = sum(
                        buffer.rows for buffer in self._buffers.values()
                    )
                    self._finalize_run(conn, report, initial_structure_keys)
        finally:
            self._restore_signal_handler(previous_handler)

        return report

    # Méthode de parcours des requêtes
    def _run_queries(
        self,
        conn: duckdb.DuckDBPyConnection,
        query_list: list[Any],
        report: DownloadReport,
    ) -> None:
        """Process the queries in order until done, deadline or SIGTERM.

        Args:
            conn: Open DuckLake connection.
            query_list: Prioritised queries.
            report: Run report to update in place.
        """
        # Parcours des requêtes
        for index, query in enumerate(query_list):
            # Arrêt anticipé : signal reçu ou délai maximal atteint
            if self._stop_requested or self._deadline_reached():
                reason = (
                    "SIGTERM received"
                    if self._stop_requested
                    else "max runtime reached"
                )
                self._stop_early(report, len(query_list) - index, reason)
                break

            # Diagnostic de la requête, renseigné qu'elle aboutisse ou non
            query_report = QueryReport(
                identity_key=query.identity_key(),
                agency=getattr(query, "agency", ""),
                dataflow=getattr(query, "dataflow", ""),
            )
            # Traitement d'une requête (les erreurs sont isolées par requête)
            deferred = False
            try:
                deferred = self._process_query(conn, query, report, query_report)
            # Récupération interrompue par SIGTERM : requête non enregistrée
            except _GracefulStop:
                self._stop_early(report, len(query_list) - index, "SIGTERM received")
                break
            # Si échec : la date n'est pas mise à jour → la requête sera retentée
            except Exception as e:
                # Logging
                logger.exception(f"Query {query.identity_key()} failed: {e}")
                self._record_failure(report, query_report, e)
            # Publication immédiate, sauf si le lot d'écriture s'en charge
            if not deferred:
                self._publish(report, query_report)

    # Méthode de consignation d'un arrêt anticipé
    @staticmethod
    def _stop_early(report: DownloadReport, remaining: int, reason: str) -> None:
        """Record a graceful early stop in the report.

        Args:
            report: Run report to update in place.
            remaining: Queries left unprocessed.
            reason: Human-readable cause, for the logs.
        """
        # Requêtes laissées de côté
        report.n_queries_remaining = remaining
        report.stopped_early = True
        # Logging
        logger.warning(
            f"Graceful shutdown: {reason}, stopping after {report.processed} "
            f"queries, {remaining} left"
        )

    # Méthode de clôture du run
    def _finalize_run(
        self,
        conn: duckdb.DuckDBPyConnection,
        report: DownloadReport,
        initial_structure_keys: set,
    ) -> None:
        """Export structures, persist the registry, close the connection.

        Args:
            conn: Open DuckLake connection (closed here).
            report: Run report to update in place.
            initial_structure_keys: Structures known when the run started.
        """
        try:
            # Structures nouvellement téléchargées pendant le run
            report.n_structures_fetched = len(
                set(self._structure_registry.list_structures()) - initial_structure_keys
            )
            # Durée totale du run
            report.duration_seconds = (
                _now() - cast(datetime, self._t0)
            ).total_seconds()
            # Export du registre des structures si une structure a été ajoutée
            if (
                set(self._structure_registry.list_structures())
                != initial_structure_keys
            ):
                # Export (routé vers le local ou S3 selon ``bucket``)
                self._saver.save(
                    self._structures_path,
                    self._structure_registry.to_dict(),
                    bucket=self._bucket,
                    indent=2,
                    ensure_ascii=False,
                )
                # Logging
                logger.info(f"Structure registry exported to {self._structures_path}")
            # Persistance finale du registre des dates : entrées non encore
            # persistées, ou aucune persistance pendant le run (le fichier est
            # alors créé ou migré, comme historiquement)
            if self._commits_since_flush > 0 or report.n_registry_flushes == 0:
                self._flush_registry(report)
        finally:
            # Fermeture propre de la connexion au catalogue
            conn.close()
            # Logging
            logger.info("DuckLake connection closed")

    # ──────────────────────────────────────────────────────────────────
    # Traitement d'une requête
    # ──────────────────────────────────────────────────────────────────

    # Méthode de traitement d'une requête unique
    def _process_query(
        self,
        conn: duckdb.DuckDBPyConnection,
        query: Any,
        report: DownloadReport,
        query_report: QueryReport,
    ) -> bool:
        """Process a single query: fetch, buffer or write, update registry.

        The DuckLake write is committed **before** the registry entry is
        updated, so the registry never references data absent from the
        database. The last-download date is updated whether or not new data
        was returned — immediately when the query returned nothing, after its
        batch is written otherwise.

        Args:
            conn: Open DuckLake connection.
            query: Provider query object.
            report: Run report to update in place.
            query_report: Per-query diagnostics to fill in place — volumetry,
                sub-request outcomes, HTTP and rate-limit cost, duration.

        Returns:
            ``True`` if the query went into a write batch, whose completion
            publishes ``query_report``; ``False`` if the caller must publish it.

        Raises:
            _GracefulStop: If ``SIGTERM`` interrupted the fetch.
        """
        # Clé identifiante de la requête
        key = query.identity_key()
        # Recherche de l'entrée associée dans le registre
        entry = self._registry.get(key)
        # Date du dernier téléchargement (None si jamais téléchargée)
        since = _parse_iso(entry.get("last_download")) if entry else None
        # Mode de téléchargement : complet ou incrémental
        query_report.incremental = since is not None

        # Instant capturé avant la requête : sert de nouvelle date de référence
        # (évite de manquer des observations publiées pendant le fetch)
        req_started = _now()
        # Compteurs du client avant la requête : la différence isole son coût
        http_before, rate_before = _client_snapshot(self._client)

        try:
            # Seule fenêtre interruptible par SIGTERM : l'attente du fournisseur
            self._interruptible = True
            if self._stop_requested:
                raise _GracefulStop()
            # Récupération des données (complète ou incrémentale selon ``since``)
            df = self._client.fetch_updates(query, since, self._n_observations)
        finally:
            self._interruptible = False
            # Coût de la requête, imputé même si la récupération a échoué
            http_after, rate_after = _client_snapshot(self._client)
            query_report.http = _http_delta(http_before, http_after)
            query_report.rate_limit = _rate_limit_delta(rate_before, rate_after)
            # Diagnostics de récupération tenus par le client
            fetch_report = getattr(self._client, "last_fetch_report_", None)
            if fetch_report is not None:
                query_report.fetch = fetch_report
            query_report.duration_seconds = (_now() - req_started).total_seconds()

        # Incrément des requêtes exécutées
        report.processed += 1
        # Extraction du nom du schéma
        schema = _schema_name(query.dataflow)
        query_report.schema = schema

        # Nouvelle entrée du registre, validée une fois les données écrites
        new_entry = {
            "agency": query.agency,
            "dataflow": query.dataflow,
            "params": _json_safe(query.to_dict()),
            "last_download": req_started.isoformat(),
        }
        shard = self._shard_for(query)

        # Aucune donnée : rien à écrire, l'entrée avance immédiatement
        if df is None or df.empty:
            # Incrément des requêtes vides
            report.empty += 1
            query_report.empty = True
            # Logging
            logger.info(f"{query.dataflow}: no new data")
            self._commit_entries(report, [(key, new_entry, shard)])
            return False

        # Résolution de la structure (pour les clés primaires) ; l'appel
        # enregistre aussi la structure dans le registre partagé, dont
        # l'ajout est détecté en fin de run pour déclencher l'export.
        structure = self._client.resolve_query_structure(query)

        # Mise en attente dans le lot du schéma (lot d'une requête sans tamponnage)
        buffer = self._buffers.setdefault(
            schema, _SchemaBuffer(dataflow=query.dataflow, structure=structure)
        )
        buffer.structure = structure
        buffer.items.append(_PendingWrite(query_report, key, new_entry, shard, df))
        buffer.rows += len(df)

        # Écriture immédiate sans tamponnage, ou dès qu'un seuil est atteint
        if not self._batching or self._buffer_full(buffer):
            self._flush_buffer(conn, schema, report)
        return True

    # Méthode de test des seuils d'un lot
    def _buffer_full(self, buffer: _SchemaBuffer) -> bool:
        """Tell whether a schema batch reached a write threshold."""
        if self._write_batch_rows is not None and buffer.rows >= self._write_batch_rows:
            return True
        return (
            self._write_batch_queries is not None
            and len(buffer.items) >= self._write_batch_queries
        )

    # Méthode d'écriture de tous les lots en attente
    def _flush_all_buffers(
        self, conn: duckdb.DuckDBPyConnection, report: DownloadReport
    ) -> None:
        """Write every pending batch (end of run, deadline, SIGTERM).

        Args:
            conn: Open DuckLake connection.
            report: Run report to update in place.
        """
        for schema in list(self._buffers):
            self._flush_buffer(conn, schema, report)

    # Méthode d'écriture du lot en attente d'un schéma
    def _flush_buffer(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str,
        report: DownloadReport,
    ) -> None:
        """Write a schema's pending batch, then commit or fail its queries.

        On success the registry entries of every query of the batch are
        committed and their reports published. On failure no entry moves, the
        error is recorded on every query of the batch, and the run goes on.

        Args:
            conn: Open DuckLake connection.
            schema: Target DuckLake schema.
            report: Run report to update in place.
        """
        # Lot du schéma (vidé dans tous les cas : écrit ou abandonné)
        buffer = self._buffers.get(schema)
        if buffer is None or not buffer.items:
            self._buffers.pop(schema, None)
            return

        try:
            # Lot d'une requête : DataFrame tel quel (comportement historique)
            if len(buffer.items) == 1:
                data = buffer.items[0].df
            else:
                data = self._concat_batch(buffer)
            created = self._write_dataframe(
                conn, data, buffer.structure, schema, buffer.dataflow
            )
        except Exception as e:
            # Échec du lot : aucune entrée n'avance, erreur imputée à chaque requête
            self._buffers.pop(schema, None)
            # Logging
            logger.exception(
                f"Write of {len(buffer.items)} query(ies) into '{schema}' failed: {e}"
            )
            # Parcours des items du buffer
            for item in buffer.items:
                self._record_failure(report, item.query_report, e)
                self._publish(report, item.query_report)
            return

        # Lot écrit : retrait du tampon, diagnostics, validation des entrées
        self._buffers.pop(schema, None)
        report.n_write_batches += 1
        report.n_tables_created += int(created)
        for position, item in enumerate(buffer.items):
            item.query_report.rows_written = len(item.df)
            item.query_report.table_created = created and position == 0
            report.rows_written += len(item.df)
        # ORDRE CRITIQUE : la base est écrite avant la mise à jour du registre
        self._commit_entries(
            report, [(item.key, item.entry, item.shard) for item in buffer.items]
        )
        for item in buffer.items:
            self._publish(report, item.query_report)

    # Méthode de concaténation d'un lot de plusieurs requêtes
    @staticmethod
    def _concat_batch(buffer: _SchemaBuffer) -> pd.DataFrame:
        """Concatenate a batch and deduplicate it on the primary key.

        Two queries of a batch are not expected to overlap, but the library
        would drop *every* occurrence of a duplicated key (``keep="none"``):
        duplicates are resolved here instead, the last query winning.

        Args:
            buffer: Batch of at least two queries.

        Returns:
            Concatenated, primary-key-unique DataFrame.
        """
        # Concaténation dans l'ordre de traitement
        data = pd.concat([item.df for item in buffer.items], ignore_index=True)
        primary_keys = _primary_keys(buffer.structure, list(data.columns))
        if not primary_keys:
            return data
        # Dédoublonnage par clé primaire, dernier gagnant
        n_rows = len(data)
        data = data.drop_duplicates(subset=primary_keys, keep="last")
        if len(data) < n_rows:
            # Logging
            logger.warning(
                f"{buffer.dataflow}: {n_rows - len(data)} duplicated primary "
                "key(s) dropped from the write batch (last query wins)"
            )
        return data

    # Méthode d'écriture d'un DataFrame dans le catalogue DuckLake
    def _write_dataframe(
        self,
        conn: duckdb.DuckDBPyConnection,
        df: pd.DataFrame,
        structure: DataflowStructure | None,
        schema: str,
        dataflow: str,
    ) -> bool:
        """Create or update the dataflow's DuckLake table with a DataFrame.

        Resolves the primary keys from the dataflow structure, then delegates the
        create-then-upsert logic to the shared
        :func:`statflows.storage.ducklake.tables.write_dataframe`.

        Args:
            conn: Open DuckLake connection.
            df: Non-empty DataFrame to persist.
            structure: Resolved dataflow structure (for primary keys).
            schema: Target DuckLake schema (one per dataflow).
            dataflow: Dataflow identifier (for logging).

        Returns:
            ``True`` if the schema was created, ``False`` if it was upserted —
            an outcome that used to be readable only in the logs.

        Raises:
            ValueError: If no primary-key column can be resolved, or if the
                update operation reports failure.
        """
        # Calcul des clés primaires (dimensions + TIME_PERIOD)
        primary_keys = _primary_keys(structure, list(df.columns))
        if not primary_keys:
            raise ValueError(
                f"No primary-key column resolved for '{dataflow}'. "
                f"Columns: {list(df.columns)}"
            )

        # Création du schéma à la première rencontre, upsert par clé primaire ensuite
        return write_dataframe(
            conn,
            df,
            primary_keys,
            catalog_alias=self._catalog_alias,
            schema=schema,
            categorical_threshold=self._categorical_threshold,
            label=dataflow,
            update_options=self._update_options,
            build_options=self._build_options,
            run_id=self._run_id,
            commit_message=f"statflows {dataflow}" if self._run_id else None,
        )

    # Méthode de consignation de l'échec d'une requête
    @staticmethod
    def _record_failure(
        report: DownloadReport, query_report: QueryReport, error: Exception
    ) -> None:
        """Record a query failure as data in the reports.

        Args:
            report: Run report to update in place.
            query_report: Diagnostics of the failed query.
            error: The exception raised.
        """
        # L'échec devient une donnée du rapport, pas seulement un log
        query_report.error_type = type(error).__name__
        query_report.error_message = str(error)[:500]
        # Incrément des erreurs
        report.errors += 1

    # Méthode de publication du rapport final d'une requête
    def _publish(self, report: DownloadReport, query_report: QueryReport) -> None:
        """Append a final per-query report to the run and notify the caller.

        Args:
            report: Run report to update in place.
            query_report: Diagnostics of the completed (or failed) query.
        """
        report.queries.append(query_report)
        self._notify(query_report)

    # Méthode de publication du diagnostic d'une requête
    def _notify(self, query_report: QueryReport) -> None:
        """Hand a per-query report to the caller's callback.

        No observability failure may interrupt a download: a raising callback
        is warned about and swallowed, exactly as a tracking failure is.

        Args:
            query_report: Diagnostics of the query just processed.
        """
        # Aucun rappel configuré : rien à faire
        if self._on_query_complete is None:
            return
        try:
            self._on_query_complete(query_report)
        except Exception as exc:
            # Logging
            logger.warning(f"on_query_complete callback failed: {exc}")

    # ──────────────────────────────────────────────────────────────────
    # Connexion et signal
    # ──────────────────────────────────────────────────────────────────

    # Méthode d'ouverture de la connexion avec options DuckLake temporaires
    def _connect(self) -> duckdb.DuckDBPyConnection:
        """Open the DuckLake connection, applying ``ducklake_options`` if any.

        The connector reads its ``data_inlining_row_limit`` and
        ``ducklake_options`` attributes when ``connect()`` is called: they are
        overridden for that call only, then restored.

        Returns:
            Open DuckLake connection.
        """
        # Aucune option : connexion telle que configurée par l'appelant
        if not self._ducklake_options:
            return self._connector.connect()

        # Séparation de l'option d'ATTACH (insensible à la casse) et des autres
        options = {
            name: value
            for name, value in self._ducklake_options.items()
            if name.lower() != _INLINING_OPTION
        }
        inlining = [
            value
            for name, value in self._ducklake_options.items()
            if name.lower() == _INLINING_OPTION
        ]

        # Sauvegarde de la configuration du connecteur
        saved_inlining = getattr(self._connector, _INLINING_OPTION, None)
        saved_options = getattr(self._connector, "ducklake_options", None)
        try:
            if inlining:
                setattr(self._connector, _INLINING_OPTION, inlining[-1])
            if options:
                # Résolution du raccourci « recommended » du connecteur
                base = saved_options
                if base == "recommended":
                    from dt_ducklake_manager.connection import (
                        RECOMMENDED_DUCKLAKE_OPTIONS,
                    )

                    base = RECOMMENDED_DUCKLAKE_OPTIONS
                self._connector.ducklake_options = {**(base or {}), **options}
            return self._connector.connect()
        finally:
            # Restauration : le connecteur de l'appelant n'est pas modifié durablement
            setattr(self._connector, _INLINING_OPTION, saved_inlining)
            self._connector.ducklake_options = saved_options

    # Gestionnaire de SIGTERM
    def _handle_sigterm(self, signum: int, frame: Any) -> None:
        """Request a graceful stop; abort the fetch in progress, if any.

        Never interrupts a DuckLake write or a registry flush: outside the
        fetch window it only sets the stop flag, checked between queries.

        Args:
            signum: Signal number.
            frame: Current stack frame (unused).

        Raises:
            _GracefulStop: If a fetch is in progress.
        """
        self._stop_requested = True
        # Logging
        logger.warning(
            f"Signal {signum} received: stopping after flushing pending writes"
        )
        if self._interruptible:
            raise _GracefulStop()

    # Méthode d'installation du gestionnaire de SIGTERM
    def _install_signal_handler(self) -> Any:
        """Install the SIGTERM handler for the duration of :meth:`run`.

        Returns:
            The previous handler, or :data:`_NO_HANDLER` when none could be
            installed (not in the main thread).
        """
        # Les gestionnaires de signaux ne s'installent que dans le thread principal
        if threading.current_thread() is not threading.main_thread():
            # Logging
            logger.warning(
                "run() is not executing in the main thread: no SIGTERM handler "
                "installed, pending writes are only flushed at the end of the run"
            )
            return _NO_HANDLER
        return signal.signal(signal.SIGTERM, self._handle_sigterm)

    # Méthode de restauration du gestionnaire de SIGTERM précédent
    @staticmethod
    def _restore_signal_handler(previous: Any) -> None:
        """Restore the SIGTERM handler that was active before :meth:`run`.

        Args:
            previous: Value returned by :meth:`_install_signal_handler`.
        """
        if previous is _NO_HANDLER:
            return
        # Gestionnaire non installé depuis Python : retour au comportement par défaut
        signal.signal(
            signal.SIGTERM, previous if previous is not None else signal.SIG_DFL
        )

    # ──────────────────────────────────────────────────────────────────
    # Priorisation, deadline et registre des dates
    # ──────────────────────────────────────────────────────────────────

    # Méthode de normalisation des requêtes en liste
    @staticmethod
    def _as_query_list(queries: Any | Iterable[Any]) -> list[Any]:
        """Normalise the queries argument into a list.

        Args:
            queries: A single query object or an iterable of queries.

        Returns:
            List of query objects.
        """
        # Objet requête isolé (expose identity_key) → liste singleton
        if hasattr(queries, "identity_key"):
            return [queries]
        return list(queries)

    # Méthode de priorisation des requêtes
    def _prioritize(self, queries: list[Any]) -> list[Any]:
        """Order queries: never-downloaded first, then oldest first.

        Args:
            queries: Query objects.

        Returns:
            Sorted list of queries.
        """
        # Date « minimale » UTC pour départager les requêtes jamais téléchargées
        epoch = datetime.min.replace(tzinfo=UTC)

        # Fonction de tri
        def sort_key(query: Any) -> tuple[int, datetime]:
            entry = self._registry.get(query.identity_key())
            last = _parse_iso(entry.get("last_download")) if entry else None
            # Jamais téléchargée (rang 0) avant déjà téléchargée (rang 1, plus ancienne d'abord)
            if last is None:
                return (0, epoch)
            return (1, last)

        return sorted(queries, key=sort_key)

    # Méthode de vérification du dépassement du délai maximal
    def _deadline_reached(self) -> bool:
        """Return whether the graceful-shutdown deadline has been reached."""
        # Pas de délai configuré → jamais d'arrêt anticipé
        if self._max_runtime is None or self._t0 is None:
            return False
        return (_now() - self._t0) >= self._max_runtime

    # Méthode de calcul du fragment de registre d'une requête
    def _shard_for(self, query: Any) -> str | None:
        """Return the registry fragment of a query (``None`` if not sharded)."""
        if self._shard_key is None:
            return None
        key = query.identity_key()
        if key not in self._shard_by_key:
            self._shard_by_key[key] = sanitize_shard(self._shard_key(query))
        return self._shard_by_key[key]

    # Méthode de validation d'entrées du registre
    def _commit_entries(
        self,
        report: DownloadReport,
        entries: list[tuple[str, dict[str, Any], str | None]],
    ) -> None:
        """Commit registry entries whose data is written, then maybe flush.

        A failed periodic flush is logged and retried at the next one: the
        entries stay committed in memory and the data they reference is
        already in DuckLake.

        Args:
            report: Run report to update in place.
            entries: ``(identity_key, entry, shard)`` triples.
        """
        for key, entry, shard in entries:
            self._registry_store.commit(key, entry, shard)
        self._commits_since_flush += len(entries)

        # Seuils de persistance : nombre d'entrées ou durée écoulée
        due = self._commits_since_flush >= self._registry_flush_every or (
            self._registry_flush_seconds is not None
            and time.monotonic() - self._last_flush >= self._registry_flush_seconds
        )
        if not due:
            return
        try:
            self._flush_registry(report)
        except Exception as e:
            # Logging
            logger.exception(f"Registry flush failed, retried at the next one: {e}")

    # Méthode de chargement du registre des dates de dernier téléchargement
    def _load_registry(self) -> None:
        """Load the last-download registry (single file and/or fragments)."""
        # Lecture des deux formats (registre vide si absent)
        self._registry_store.load()
        # Logging
        logger.info(
            f"Loaded {len(self._registry_store)} download records from "
            f"{self._last_download_path}"
        )

    # Méthode de persistance du registre des dates
    def _flush_registry(self, report: DownloadReport) -> None:
        """Persist the last-download registry through the configured storage.

        Args:
            report: Run report whose flush counter is incremented.
        """
        written = self._registry_store.flush()
        self._commits_since_flush = 0
        self._last_flush = time.monotonic()
        if written:
            report.n_registry_flushes += 1


# ──────────────────────────────────────────────────────────────────────
# Fonction de convenance
# ──────────────────────────────────────────────────────────────────────


# Fonction utilitaire enveloppant l'orchestrateur
def download_updates(
    client: AbstractSDMXClient,
    queries: Any | Iterable[Any],
    connector: DuckLakeConnector,
    structures_path: str | Path,
    last_download_path: str | Path,
    *,
    n_observations: int = 10,
    fresh_registry: bool = False,
    max_runtime: timedelta | None = timedelta(hours=23),
    categorical_threshold: int | None = None,
    bucket: str | None = None,
    storage_options: dict[str, Any] | None = None,
    on_query_complete: Callable[[QueryReport], None] | None = None,
    registry_flush_every: int = 1,
    registry_flush_seconds: float | None = None,
    registry_shard_key: Callable[[Any], str] | None = None,
    write_batch_rows: int | None = None,
    write_batch_queries: int | None = None,
    update_options: Mapping[str, Any] | None = None,
    build_options: Mapping[str, Any] | None = None,
    ducklake_options: dict[str, Any] | None = None,
    run_id: str | None = None,
) -> DownloadReport:
    """Run an incremental SDMX → DuckLake download.

    Thin wrapper around :class:`SDMXDownloader` for one-call usage (e.g. from a
    Kedro node). See :class:`SDMXDownloader` for the argument semantics; every
    argument defaults to the historical, unbuffered behaviour.

    Returns:
        A :class:`DownloadReport` summarising the run.
    """
    # Construction de l'orchestrateur et exécution
    downloader = SDMXDownloader(
        client,
        connector,
        structures_path,
        last_download_path,
        n_observations=n_observations,
        fresh_registry=fresh_registry,
        max_runtime=max_runtime,
        categorical_threshold=categorical_threshold,
        bucket=bucket,
        storage_options=storage_options,
        on_query_complete=on_query_complete,
        registry_flush_every=registry_flush_every,
        registry_flush_seconds=registry_flush_seconds,
        registry_shard_key=registry_shard_key,
        write_batch_rows=write_batch_rows,
        write_batch_queries=write_batch_queries,
        update_options=update_options,
        build_options=build_options,
        ducklake_options=ducklake_options,
        run_id=run_id,
    )
    return downloader.run(queries)
