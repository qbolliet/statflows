"""Generic API client, shared last-update check and abstract SDMX client base.

This module provides:
- ``APIClient``: generic HTTP client with retry logic.
- ``FreshnessCheckMixin``: template of the incremental ``fetch_updates`` for
  providers that must ask "has the data changed since?" before fetching,
  with a per-run cache of the provider's answer.
- ``AbstractSDMXClient``: abstract base class mutualising logic shared by all
  SDMX provider clients (structure registry, duplicate checking, split-request
  execution, CSV parsing, context manager, etc.).
"""

# Importation des modules
import itertools
import json
import logging
import operator
import time
import warnings
from abc import ABC, abstractmethod
from collections.abc import Hashable
from datetime import UTC, datetime
from functools import reduce
from pathlib import Path

# Imports internes — éviter les imports circulaires en utilisant TYPE_CHECKING
from typing import TYPE_CHECKING, Any, ClassVar, Optional
from urllib.parse import urljoin

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .parsing import check_no_null_keys, read_codes_csv, resolve_key_columns
from .rate_limiter import CompositeRateLimiter, RateLimiter, build_rate_limiter

# Import runtime du rate limiter (rate_limiter.py n'a pas de dépendance interne
# au package, aucun risque de circularité)
from .reports import FetchReport, FreshnessStats, HttpStats

if TYPE_CHECKING:
    from .sdmx import DuplicateHandling
    from .structures import DataflowStructure, DataflowStructureRegistry

# Initialisation du logger
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Client HTTP générique
# ──────────────────────────────────────────────────────────────────────


# Classe permettant d'effectuer des requêtes API avec 'requests'
class APIClient:
    """Generic HTTP client with retry logic and error handling.

    This class handles HTTP requests with automatic retry on failures,
    connection pooling, and timeout management.

    Args:
        base_url: Base URL for API requests.
        timeout: Request timeout in seconds (default: 30).
        max_retries: Maximum number of retry attempts (default: 3).
        backoff_factor: Backoff factor for retries (default: 0.5).
        headers: Additional headers to include in requests.

    Example:
        >>> client = APIClient("https://api.example.com")
        >>> response = client.get("/data", params={"key": "value"})
    """

    # Initialisation
    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        headers: dict[str, str] | None = None,
    ) -> None:
        # Initialisation des attributs
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = self._create_session(max_retries, backoff_factor)
        self.default_headers = {}
        if headers:
            self.default_headers.update(headers)

        # Compteurs HTTP : statuts, durées et volumes
        self.stats_ = HttpStats()

    # Méthode auxiliaire de création de la session 'request'
    def _create_session(
        self, max_retries: int, backoff_factor: float
    ) -> requests.Session:
        """Create a session with retry configuration.

        Args:
            max_retries: Maximum number of retry attempts.
            backoff_factor: Backoff factor between retries.

        Returns:
            Configured requests Session object.
        """
        # Initialisation d'une session requests
        session = requests.Session()
        # Initialisation d'une stratégie de retry
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            # Retries épuisés : renvoi de la dernière réponse plutôt qu'un RetryError,
            # afin que raise_for_status() lève un HTTPError portant le statut HTTP
            raise_on_status=False,
        )
        # Ajout de la stratégie à la session
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    # Méthode de requête "GET"
    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int | None = None,
    ) -> requests.Response:
        """Make a GET request.

        Args:
            endpoint: API endpoint (relative to base_url).
            params: Query parameters.
            headers: Additional headers for this request.
            timeout: Request timeout in seconds. Overrides the instance-level
                timeout when provided.

        Returns:
            Response object.

        Raises:
            requests.exceptions.RequestException: On request failure.
        """
        # Création de l'URL de requête
        url = urljoin(self.base_url + "/", endpoint.lstrip("/"))
        # Création des headers
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)

        # Résolution du timeout : surcharge par requête ou valeur par défaut
        effective_timeout = timeout if timeout is not None else self.timeout

        # Logging
        logger.debug(f"GET request to {url} with params: {params}")
        # Instant de départ : la durée alimente les compteurs, quelle que soit l'issue
        started = time.monotonic()
        try:
            # Excution de la requête
            response = self.session.get(
                url,
                params=params,
                headers=request_headers,
                timeout=effective_timeout,
            )
            # Statut de la requête
            response.raise_for_status()
            # Comptabilisation de la réponse (statut, durée, volume)
            self.stats_.record(
                response.status_code,
                time.monotonic() - started,
                len(response.content or b""),
            )
            return response
        except requests.exceptions.HTTPError as e:
            # Comptabilisation de l'échec, statut compris
            self.stats_.record_failure(
                e.response.status_code if e.response is not None else None,
                time.monotonic() - started,
            )
            # Logging
            logger.error(f"HTTP error: {e}")
            logger.error(f"Response content: {e.response.text[:500]}")
            raise
        except requests.exceptions.RequestException as e:
            # Comptabilisation de l'échec sans réponse (erreur de connexion)
            self.stats_.record_failure(None, time.monotonic() - started)
            # Logging
            logger.error(f"Request failed: {e}")
            raise

    # Méthode de fermeture de la session
    def close(self) -> None:
        """Close the session and clean up resources."""
        self.session.close()

    # Constructeur d'entrée comme context manager
    def __enter__(self) -> "APIClient":
        """Context manager entry."""
        return self

    # Constructeur de sortie comme contexte manager
    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        """Context manager exit."""
        self.close()


# ──────────────────────────────────────────────────────────────────────
# Vérification de fraîcheur des données — approche mutualisée
# ──────────────────────────────────────────────────────────────────────


# Fonction de normalisation d'un datetime en UTC
def _to_utc(value: datetime) -> datetime:
    """Return a UTC-aware copy of a datetime (naive values assumed UTC).

    Ensures the last-update / last-download comparison never raises on mixed
    aware/naive datetimes.

    Args:
        value: Datetime to normalise.

    Returns:
        UTC-aware ``datetime``.

    Examples:
        >>> _to_utc(datetime(2026, 1, 1, 12))
        datetime.datetime(2026, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
    """
    # Datetime naïf → interprété comme UTC ; sinon conversion vers UTC
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


# Horloge murale UTC de la vérification (isolée pour les tests)
def _utc_now() -> datetime:
    """Return the current instant as a UTC-aware datetime."""
    return datetime.now(UTC)


# Horloge monotone de l'expiration des échecs (isolée pour les tests)
def _monotonic() -> float:
    """Return a monotonic clock reading, in seconds."""
    return time.monotonic()


# Mixin de récupération incrémentale conditionnée à la date de mise à jour
class FreshnessCheckMixin(ABC):
    """Template of ``fetch_updates`` for providers gated by a last-update date.

    Some providers offer no server-side "updated after" filter: before pulling
    data, the client must ask when the data was last updated and compare that
    instant with the previous download. The question is usually the same for
    many queries (every product batch of a dataflow, every product batch of a
    Comtrade period), so its answer is cached for the run.

    The mixin goes first in the bases of a client exposing
    ``execute_query(query)`` (``class MyClient(FreshnessCheckMixin,
    AbstractSDMXClient)``); its concrete :meth:`fetch_updates` then satisfies
    the abstract one of :class:`AbstractSDMXClient`. A provider implements:

    - :meth:`_freshness_key` (required): what identifies "the same question",
      independently of how the queries are split (e.g. ``(dataflow, version)``).
    - :meth:`_fetch_last_update` (required): ask the provider when the data
      behind a query was last updated. May raise.
    - :meth:`_fetch_incremental` (optional): fetch a query whose data changed;
      defaults to ``execute_query(query)``.
    - :meth:`_log_no_update` (optional): log a skipped query.

    Behaviour of :meth:`fetch_updates`:

    - ``since is None`` (first download) → full ``execute_query``.
    - Otherwise the last-update date is resolved (cached per key). When it is
      known and not after ``since`` (both compared in UTC) the query is skipped
      and an empty DataFrame returned; otherwise :meth:`_fetch_incremental`.

    Cache policy:

    - **Lifetime**: the client instance, never persisted.
      :meth:`clear_freshness_cache` empties it, and the download orchestrator
      calls it at the start of every run, so an answer never outlives its run.
    - **Unresolved date** (the lookup raised or returned ``None``): the data is
      refreshed as a precaution, and the failure is remembered for
      :attr:`FRESHNESS_FAILURE_TTL` seconds before one new lookup is allowed.
      Retrying on every query would add thousands of doomed calls (each with
      its own HTTP retries) on top of the data fetches while a provider is
      down; remembering the failure for the whole run would turn a few seconds
      of outage into a full refresh of every query. ``None`` remembers the
      failure for the whole run (used by quota-limited providers).
    - **Update published during the run**: a cached "unchanged" answer only
      covers what the provider knew when it was asked. When a query is skipped,
      :attr:`last_fetch_as_of_` holds the instant the answer was requested, and
      the orchestrator records it as the query's download date (instead of the
      later start of the query), so an update published in between is still
      detected at the next run. A date after ``since`` always triggers a fetch.
    - **Threads**: no lock. The download orchestrator calls the client from a
      single thread; a client must not be shared across threads.

    Counters are accumulated in :attr:`freshness_stats_` and read by the
    orchestrator next to the HTTP counters.

    Attributes:
        FRESHNESS_FAILURE_TTL: Seconds an unresolved date is remembered before
            a new lookup (``None``: for the life of the cache).
        freshness_stats_: Lookups sent, answers served by the cache, and
            unresolved lookups.
        last_fetch_as_of_: For the last :meth:`fetch_updates` call that skipped
            its query, the instant the "unchanged" answer was requested;
            ``None`` when the call fetched data (its result is then as recent
            as the call itself).

    Examples:
        >>> class TinyClient(FreshnessCheckMixin):
        ...     def _freshness_key(self, query):
        ...         return query["dataflow"]
        ...     def _fetch_last_update(self, query):
        ...         return datetime(2026, 3, 1, tzinfo=UTC)
        ...     def execute_query(self, query):
        ...         return pd.DataFrame({"value": [1.0]})
        >>> client = TinyClient()
        >>> since = datetime(2026, 4, 1, tzinfo=UTC)
        >>> [len(client.fetch_updates({"dataflow": "DF"}, since)) for _ in range(3)]
        [0, 0, 0]
        >>> client.freshness_stats_
        FreshnessStats(n_checks=1, n_cache_hits=2, n_unresolved=0)
        >>> len(client.fetch_updates({"dataflow": "DF"}, datetime(2026, 2, 1)))
        1
    """

    # Durée de mémorisation d'un échec de résolution avant nouvelle tentative
    FRESHNESS_FAILURE_TTL: ClassVar[float | None] = 300.0

    # Initialisation coopérative : les arguments vont aux bases suivantes
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Cache des dates de dernière mise à jour par clé de fraîcheur
        self._freshness_cache: dict[Hashable, datetime | None] = {}
        # Instant (horloge monotone) du dernier échec de résolution par clé
        self._freshness_failed_at: dict[Hashable, float] = {}
        # Instant (UTC) de la demande ayant produit chaque réponse en cache
        self._freshness_checked_at: dict[Hashable, datetime] = {}
        # Compteurs de vérification (lus par l'orchestrateur)
        self.freshness_stats_ = FreshnessStats()
        # Horizon de connaissance de la dernière requête sautée
        self.last_fetch_as_of_: datetime | None = None

    # Signature attendue du client concret (définie par la base suivante)
    if TYPE_CHECKING:

        def execute_query(self, query: Any) -> pd.DataFrame: ...

    # Point d'extension : clé de fraîcheur
    @abstractmethod
    def _freshness_key(self, query: Any) -> Hashable:
        """Return what identifies the last-update question of a query.

        Two queries sharing a key share the provider's answer: the key must
        therefore ignore everything that does not change that answer (product
        batch, dimension filters, …) and include everything that does
        (dataflow, version, period, …).

        Args:
            query: Provider query request.

        Returns:
            Hashable key.
        """

    # Point d'extension : date de dernière mise à jour côté fournisseur
    @abstractmethod
    def _fetch_last_update(self, query: Any) -> datetime | None:
        """Ask the provider when the data behind a query was last updated.

        Called at most once per :meth:`_freshness_key` and per run (plus the
        retries of an expired failure). No caching is needed here.

        Args:
            query: Provider query request.

        Returns:
            The last data-update instant (naive values are read as UTC), or
            ``None`` when the provider does not tell.

        Raises:
            Exception: Any failure; it is logged and treated like ``None``.
        """

    # Point d'extension : récupération d'une requête dont les données ont changé
    def _fetch_incremental(self, query: Any, n_observations: int) -> pd.DataFrame:
        """Fetch a query whose data changed since the previous download.

        Defaults to a full ``execute_query``; providers able to restrict the
        fetch (e.g. to the last observations) override it.

        Args:
            query: Provider query request.
            n_observations: Number of most-recent observations requested by
                the orchestrator (ignored by the default implementation).

        Returns:
            DataFrame with the retrieved data.
        """
        return self.execute_query(query)

    # Point d'extension : journalisation d'une requête sautée
    def _log_no_update(
        self, query: Any, since: datetime, last_update: datetime
    ) -> None:
        """Log that a query is skipped because nothing changed.

        Args:
            query: Skipped query.
            since: Previous download instant of the query.
            last_update: Last data-update instant of the provider.
        """
        # Logging
        logger.info(
            f"{getattr(query, 'dataflow', query)}: no update since {since} "
            f"(last update {last_update}); skipping data fetch"
        )

    # Méthode patron de la récupération incrémentale
    def fetch_updates(
        self,
        query: Any,
        since: datetime | None,
        n_observations: int = 10,
    ) -> pd.DataFrame:
        """Fetch the data for a query, incrementally when possible.

        Provider seam used by the download orchestrator
        (:class:`~statflows.core.download.SDMXDownloader`); see the class
        docstring for the decision rules and the cache policy.

        Args:
            query: Provider query request.
            since: Instant of the previous successful download for this query,
                or ``None`` if it was never downloaded.
            n_observations: Number of most-recent observations to retrieve in
                incremental mode, forwarded to :meth:`_fetch_incremental`.

        Returns:
            DataFrame with the retrieved data; empty when nothing was published
            since ``since``.
        """
        # Horizon réinitialisé : seul un saut le renseigne
        self.last_fetch_as_of_ = None

        # Premier téléchargement : récupération complète
        if since is None:
            return self.execute_query(query)

        # Date de dernière mise à jour (cache par run)
        last_update = self._query_last_update(query)

        # Aucune publication depuis le dernier téléchargement → requête sautée
        if last_update is not None and _to_utc(last_update) <= _to_utc(since):
            # Horizon : instant de la demande dont la réponse a servi, pour que
            # le registre n'avance pas au-delà de ce que le fournisseur savait
            self.last_fetch_as_of_ = self._freshness_checked_at.get(
                self._freshness_key(query)
            )
            self._log_no_update(query, since, last_update)
            return pd.DataFrame()

        # Données modifiées, ou date inconnue (rafraîchissement par précaution)
        return self._fetch_incremental(query, n_observations)

    # Méthode de résolution mise en cache de la date de dernière mise à jour
    def _query_last_update(self, query: Any) -> datetime | None:
        """Return the last-update date of a query, cached per freshness key.

        Args:
            query: Provider query request.

        Returns:
            UTC-aware last data-update instant, or ``None`` when it could not
            be resolved.
        """
        # Extraction de la clé
        key = self._freshness_key(query)

        # Réponse en cache, sauf échec dont la durée de mémorisation est écoulée
        if key in self._freshness_cache and not self._freshness_failure_expired(key):
            self.freshness_stats_.n_cache_hits += 1
            return self._freshness_cache[key]

        # Instant capturé avant l'appel : borne de ce que la réponse couvre
        checked_at = _utc_now()
        self.freshness_stats_.n_checks += 1
        try:
            last_update = self._fetch_last_update(query)
        # Seules les erreurs ordinaires sont absorbées : un arrêt demandé
        # (SIGTERM) traverse la méthode sans rien mettre en cache
        except Exception as e:
            # Logging
            logger.warning(f"Could not resolve the last update for {key!r}: {e}")
            last_update = None

        # Échec mémorisé avec son instant ; succès normalisé en UTC
        if last_update is None:
            self.freshness_stats_.n_unresolved += 1
            self._freshness_failed_at[key] = _monotonic()
        else:
            last_update = _to_utc(last_update)
            self._freshness_failed_at.pop(key, None)
        self._freshness_cache[key] = last_update
        self._freshness_checked_at[key] = checked_at
        return last_update

    # Méthode de test d'expiration d'un échec mémorisé
    def _freshness_failure_expired(self, key: Hashable) -> bool:
        """Tell whether a remembered failure may be retried.

        Args:
            key: Freshness key.

        Returns:
            ``True`` when the key holds a failure older than
            :attr:`FRESHNESS_FAILURE_TTL`; ``False`` otherwise (success, recent
            failure, or failures remembered for the life of the cache).
        """
        failed_at = self._freshness_failed_at.get(key)
        ttl = self.FRESHNESS_FAILURE_TTL
        return (
            failed_at is not None
            and ttl is not None
            and _monotonic() - failed_at >= ttl
        )

    # Méthode de vidage du cache de fraîcheur
    def clear_freshness_cache(self) -> None:
        """Forget every cached last-update answer (counters are kept).

        Called by the download orchestrator at the start of each run, so that
        a client reused across runs never serves an answer from a previous one.
        """
        # Vidage sur place : les références détenues (alias) restent valides
        self._freshness_cache.clear()
        self._freshness_failed_at.clear()
        self._freshness_checked_at.clear()
        self.last_fetch_as_of_ = None


# ──────────────────────────────────────────────────────────────────────
# Client SDMX abstrait — logique mutualisée entre tous les providers
# ──────────────────────────────────────────────────────────────────────


class AbstractSDMXClient(ABC):
    """Abstract base class for SDMX provider clients.

    Provides the shared logic common to all SDMX provider clients :

    - Structure registry management (``register_structure``, ``_ensure_structure``)
    - Rate-limiter loading from ``parameters/{PROVIDER_CONFIG_NAME}.json``
      (``_load_rate_limiter``)
    - Query-object execution (``execute_query``)
    - Cartesian product of split dimensions (``_cartesian_split``)
    - Data-retrieval pipeline orchestration (``_execute_query_pipeline``)
    - Split-request execution loop (``_execute_split_requests``)
    - Post-request DataFrame filtering (``_filter_dataframe_by_dimensions``)
    - Duplicate detection (``_check_duplicates``)
    - CSV response parsing (``_parse_csv_response``)
    - Context manager protocol

    Subclasses must implement:
    - ``get_data``: provider-specific data retrieval entry point (typically a
      thin wrapper packing its arguments and delegating to
      ``_execute_query_pipeline``).
    - ``close``: release provider-specific resources.
    - ``_fetch_structure``: fetch a ``DataflowStructure`` from the provider API.
    - ``_execute_single_request``: execute one API request for a given
      dimension combination and return a parsed DataFrame.
    - ``_resolve_structure``: resolve/cache the structure for a query.
    - ``_prepare_requests``: build the split-request combinations and the
      execution keyword arguments.

    Subclasses must set :attr:`PROVIDER_CONFIG_NAME` to enable automatic
    rate-limiter loading, and may override ``_postprocess_dataframe`` to apply
    a provider-specific post-filter.

    Args:
        structure_registry: Pre-populated registry of dataflow structures.
            A new empty registry is created if ``None``.
        auto_fetch_structure: If ``True``, automatically query the provider
            API to retrieve dimension metadata when a dataflow structure is
            not yet in the registry.
        rate_limiter: Rate limiter instance. If ``None`` and
            ``auto_load_rate_limit`` is ``True``, the provider's JSON
            configuration file is read via ``_load_rate_limiter``.
        auto_load_rate_limit: Whether to attempt loading the rate limiter
            automatically from the provider config file.

    Example:
        >>> class MyClient(AbstractSDMXClient):
        ...     PROVIDER_CONFIG_NAME = "myprovider"
        ...     def get_data(self, ...): ...
        ...     def close(self): ...
        ...     def _fetch_structure(self, agency, dataflow, **kwargs): ...
        ...     def _execute_single_request(self, dims, **kwargs): ...
        ...     def _resolve_structure(self, params): ...
        ...     def _prepare_requests(self, structure, params): ...
    """

    # Nom du fichier de configuration du provider (sans extension) utilisé pour
    # le chargement automatique du rate limiter depuis parameters/{nom}.json.
    # Laissé à None dans la base ; surchargé par chaque client concret.
    PROVIDER_CONFIG_NAME: ClassVar[str | None] = None

    # Initialisation
    def __init__(
        self,
        structure_registry: Optional["DataflowStructureRegistry"] = None,
        auto_fetch_structure: bool = True,
        rate_limiter: Optional["RateLimiter"] = None,
        auto_load_rate_limit: bool = True,
    ) -> None:
        # Import local pour éviter la circularité au niveau module
        from .structures import DataflowStructureRegistry as _Registry

        # Initialisation des attributs
        # Registre des structures de dataflows
        self.structure_registry: DataflowStructureRegistry = (
            structure_registry if structure_registry is not None else _Registry()
        )
        self.auto_fetch_structure = auto_fetch_structure

        # Chargement automatique du rate limiter si demandé
        resolved_limiter: RateLimiter | CompositeRateLimiter | None = rate_limiter
        if auto_load_rate_limit and rate_limiter is None:
            resolved_limiter = self._load_rate_limiter()
        self.rate_limiter: RateLimiter | CompositeRateLimiter | None = resolved_limiter

        # Diagnostics de la dernière récupération (convention sklearn du dépôt :
        # attribut suffixé d'un underscore, renseigné à l'exécution)
        self.last_fetch_report_: FetchReport | None = None
        # Nombre de structures effectivement téléchargées (défauts de cache)
        self.n_structures_fetched_: int = 0

    # Méthodes abstraites
    # Méthode abstraite de requête des données
    @abstractmethod
    def get_data(self, *args, **kwargs) -> pd.DataFrame:
        """Retrieve data from the provider API.

        Provider-specific signature. Implementations should handle dimension
        normalisation, structure auto-fetch, rate limiting, split requests,
        and duplicate checking.

        Returns:
            DataFrame with the retrieved data.
        """

    # Méthode abstraite de fermeture de la connexion
    @abstractmethod
    def close(self) -> None:
        """Release provider-specific resources (HTTP sessions, etc.)."""

    # Méthode de chargement du rate-limiter depuis le fichier de configuration
    def _load_rate_limiter(
        self,
    ) -> RateLimiter | CompositeRateLimiter | None:
        """Load the rate limiter from ``parameters/{PROVIDER_CONFIG_NAME}.json``.

        Reads the ``RATE_LIMIT`` section of the provider configuration file and
        builds a limiter via :func:`build_rate_limiter`. The section may be a
        single ``{requests, unit, count}`` dict (→ :class:`RateLimiter`) or a
        list of such dicts (→ :class:`CompositeRateLimiter`). Subclasses only
        need to set :attr:`PROVIDER_CONFIG_NAME`; the lookup is skipped (and
        ``None`` returned) when it is left unset.

        Returns:
            ``RateLimiter`` / ``CompositeRateLimiter`` instance, or ``None`` if
            no configuration is found or loading fails.
        """
        # Aucun fichier de configuration déclaré → pas de rate limiting
        if not self.PROVIDER_CONFIG_NAME:
            return None
        try:
            # Construction du chemin vers parameters/{provider}.json (données du package)
            params_path = (
                Path(__file__).parents[1]
                / "parameters"
                / f"{self.PROVIDER_CONFIG_NAME}.json"
            )
            # Lecture et parsing du fichier de configuration si présent
            if params_path.exists():
                with open(params_path, encoding="utf-8") as f:
                    config = json.load(f)
                # Extraction de la configuration du rate limiter
                if "RATE_LIMIT" in config:
                    # Logging
                    logger.info(
                        f"Loading rate limiter from "
                        f"parameters/{self.PROVIDER_CONFIG_NAME}.json"
                    )
                    return build_rate_limiter(config["RATE_LIMIT"])
            # Logging si aucune configuration de rate limit trouvée
            logger.debug("No RATE_LIMIT configuration found")
            return None
        # Échec non bloquant de chargement
        except Exception as e:
            # Logging
            logger.warning(f"Could not load rate limiter: {e}")
            return None

    # Méthode abstraire de requête de la structure d'un dataflow
    @abstractmethod
    def _fetch_structure(
        self,
        agency: str,
        dataflow: str,
        **kwargs,
    ) -> "DataflowStructure":
        """Fetch a dataflow structure from the provider API (no cache).

        Args:
            agency: Agency identifier.
            dataflow: Dataflow identifier.
            **kwargs: Provider-specific keyword arguments (e.g. ``version``
                for Eurostat).

        Returns:
            Parsed ``DataflowStructure``.

        Raises:
            Exception: On HTTP or parsing errors.
        """

    # Méthode abstraite d'exécution d'une requête
    @abstractmethod
    def _execute_single_request(
        self,
        dims_for_request: dict,
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute one API request for a given dimension combination.

        Called by ``_execute_split_requests`` for each entry in the
        Cartesian product of split dimensions.

        Args:
            dims_for_request: Dimension values for this specific request
                (format is provider-specific: ``Dict[str, List[str]]`` for
                Eurostat, ``Dict[int, List[str]]`` for OECD).
            **request_kwargs: Additional provider-specific parameters
                forwarded from ``_execute_split_requests``.

        Returns:
            Parsed DataFrame (before post-filtering).
        """

    # Méthode abstraite de résolution de la structure d'une requête
    @abstractmethod
    def _resolve_structure(
        self, params: dict[str, Any]
    ) -> Optional["DataflowStructure"]:
        """Resolve (and cache) the dataflow structure for a query.

        Called first by :meth:`_execute_query_pipeline`. Implementations read
        the relevant identifiers from ``params`` (e.g. ``dataflow``,
        ``agency``, ``version``) and return the structure, fetching it on
        demand when necessary.

        Args:
            params: Parameter dict assembled by the provider ``get_data``
                method (keys match its signature).

        Returns:
            ``DataflowStructure`` if available, ``None`` otherwise.
        """

    # Méthode abstraite de préparation des requêtes splitées
    @abstractmethod
    def _prepare_requests(
        self,
        structure: Optional["DataflowStructure"],
        params: dict[str, Any],
    ) -> tuple[list[tuple[dict, dict]], dict, dict[str, Any]]:
        """Build the split-request combinations and execution arguments.

        Called by :meth:`_execute_query_pipeline` after structure resolution.
        Implementations normalise dimensions, generate the request
        combinations and assemble the keyword arguments forwarded to every
        ``_execute_single_request`` call.

        Args:
            structure: Resolved dataflow structure (may be ``None``).
            params: Parameter dict assembled by the provider ``get_data``
                method.

        Returns:
            Tuple ``(request_combinations, normalized_dimensions,
            execute_kwargs)``:

            - ``request_combinations``: list of ``(dims_for_request,
              dims_for_postfilter)`` tuples consumed by
              ``_execute_split_requests``.
            - ``normalized_dimensions``: dimensions used for duplicate
              checking.
            - ``execute_kwargs``: keyword arguments forwarded to every
              ``_execute_single_request`` call.
        """

    # Méthode abstraite de récupération incrémentale d'une requête
    @abstractmethod
    def fetch_updates(
        self,
        query: Any,
        since: datetime | None,
        n_observations: int = 10,
    ) -> pd.DataFrame:
        """Fetch the data for a query, incrementally when possible.

        Provider seam used by the download orchestrator
        (:class:`~statflows.core.download.SDMXDownloader`). This
        is where each provider encodes *how* to retrieve only the newly
        published observations:

        - First download (``since`` is ``None``) → retrieve the **full**
          series.
        - Subsequent download (``since`` set) → retrieve only what changed
          since that instant. OECD relies on the ``updated_after`` query
          parameter; Eurostat issues a ``dataconstraint`` structure request
          first and, when the data was updated after ``since``, pulls the last
          ``n_observations`` observations (to avoid leaving gaps).

        Args:
            query: Provider-specific query request exposing ``to_dict()``
                (e.g. ``OECDQueryRequest``, ``EurostatQueryRequest``).
            since: Instant of the previous successful download for this query,
                or ``None`` if it was never downloaded.
            n_observations: Number of most-recent observations to retrieve in
                incremental mode (providers without a per-observation update
                filter, e.g. Eurostat).

        Returns:
            DataFrame with the retrieved data. May be empty when nothing was
            published since ``since``.
        """

    # ──────────────────────────────────────────────────────────────────
    # Pipeline de récupération mutualisé
    # ──────────────────────────────────────────────────────────────────

    # Méthode d'exécution d'un objet requête provider
    def execute_query(self, query: Any) -> pd.DataFrame:
        """Execute a provider query object.

        Generic helper relying on the provider query dataclass exposing a
        ``to_dict()`` method whose keys match the provider ``get_data``
        parameters.

        Args:
            query: Provider-specific query request exposing ``to_dict()``
                (e.g. ``OECDQueryRequest``, ``EurostatQueryRequest``).

        Returns:
            DataFrame with the retrieved data.
        """
        # Délégation à get_data avec les paramètres de la requête
        return self.get_data(**query.to_dict())

    # Méthode de résolution de la structure associée à un objet requête
    def resolve_query_structure(self, query: Any) -> Optional["DataflowStructure"]:
        """Resolve the dataflow structure backing a provider query object.

        Thin adapter delegating to the provider :meth:`_resolve_structure`
        hook with the query parameters. Used by the download orchestrator to
        derive the primary keys (the dataflow dimensions) of the target
        DuckLake table. Resolving the structure also registers it in
        :attr:`structure_registry` (side effect of ``_resolve_structure``),
        so the orchestrator can detect newly fetched structures.

        Args:
            query: Provider-specific query request exposing ``to_dict()``.

        Returns:
            Resolved ``DataflowStructure`` or ``None`` when unavailable.
        """
        # Délégation au hook provider avec les paramètres de la requête
        return self._resolve_structure(query.to_dict())

    # Méthode patron orchestrant la récupération des données
    def _execute_query_pipeline(self, params: dict[str, Any]) -> pd.DataFrame:
        """Run the shared data-retrieval pipeline (template method).

        Orchestrates the steps common to every provider: structure
        resolution, request preparation, split-request execution, duplicate
        checking and optional post-processing. Providers customise the
        behaviour through :meth:`_resolve_structure`, :meth:`_prepare_requests`
        and :meth:`_postprocess_dataframe`.

        Every step feeds :attr:`last_fetch_report_`, so that a caller — the
        download orchestrator, or a consuming project — can read what the run
        did instead of parsing its logs.

        Args:
            params: Parameter dict assembled by the provider ``get_data``
                method (keys match its signature).

        Returns:
            Retrieved DataFrame.
        """
        # Rapport de la récupération, renseigné étape par étape
        report = FetchReport()
        self.last_fetch_report_ = report

        # Structures déjà téléchargées avant résolution : un compteur inchangé
        # signe un accès au cache
        n_fetched_before = self.n_structures_fetched_
        # Résolution de la structure du dataflow (spécifique au provider)
        structure = self._resolve_structure(params)
        if structure is not None:
            report.structure_from_cache = self.n_structures_fetched_ == n_fetched_before

        # Préparation des requêtes : combinaisons, dims normalisées, kwargs d'exécution
        request_combinations, normalized_dims, execute_kwargs = self._prepare_requests(
            structure, params
        )

        # Exécution mutualisée des sous-requêtes (rate limiting, concaténation)
        df = self._execute_split_requests(
            request_combinations, report=report, **execute_kwargs
        )

        # Garde-fou : un code de dimension nul rendrait l'observation inidentifiable
        # et ferait rejeter tout le lot d'écriture plus loin, sans diagnostic
        check_no_null_keys(
            df,
            resolve_key_columns(structure, list(df.columns)),
            label=params.get("dataflow"),
        )

        # Vérification des doublons sauf si explicitement désactivée
        on_duplicate = params.get("on_duplicate", "warn")
        default_dimensions = params.get("default_dimensions", [])
        if on_duplicate != "ignore":
            report.n_duplicates = self._check_duplicates(
                df, normalized_dims, structure, on_duplicate, default_dimensions
            )

        # Post-traitement optionnel (post-filtre de repli côté provider)
        df = self._postprocess_dataframe(df, structure, normalized_dims, params)
        # Volumétrie finale, après l'éventuel post-filtre du provider
        report.rows_after_filter = len(df)
        return df

    # Hook de post-traitement du DataFrame (no-op par défaut)
    def _postprocess_dataframe(
        self,
        df: pd.DataFrame,
        structure: Optional["DataflowStructure"],
        normalized_dims: dict,
        params: dict[str, Any],
    ) -> pd.DataFrame:
        """Post-process the retrieved DataFrame (hook, no-op by default).

        Overridden by providers needing a client-side fallback filter (e.g.
        Eurostat when no structure is available).

        Args:
            df: DataFrame returned by the split requests.
            structure: Resolved dataflow structure (may be ``None``).
            normalized_dims: Normalised dimensions from
                :meth:`_prepare_requests`.
            params: Parameter dict from the provider ``get_data`` method.

        Returns:
            Possibly filtered DataFrame. The default implementation returns
            ``df`` unchanged.
        """
        return df

    # Méthode statique de génération du produit cartésien des dimensions à splitter
    @staticmethod
    def _cartesian_split(
        split_values: dict[Any, list[str]],
        max_combinations: int,
    ) -> list[dict[Any, str]]:
        """Build the cartesian product of split-dimension values.

        Args:
            split_values: Mapping of dimension key (name or position) to the
                list of values to split into separate requests.
            max_combinations: Maximum number of combinations allowed.

        Returns:
            List of ``{key: single_value}`` dicts — one per combination.
            Returns ``[{}]`` (a single empty combination) when
            ``split_values`` is empty.

        Raises:
            ValueError: If the cartesian product exceeds ``max_combinations``.
        """
        # Aucune dimension à splitter → une seule combinaison vide
        keys = list(split_values.keys())
        if not keys:
            return [{}]

        # Listes de valeurs dans l'ordre des clés
        value_lists = [split_values[k] for k in keys]

        # Contrôle du nombre de combinaisons avant génération
        num_combinations = reduce(operator.mul, (len(v) for v in value_lists), 1)
        if num_combinations > max_combinations:
            raise ValueError(
                f"Cartesian product would generate {num_combinations} requests, "
                f"exceeding max_split_combinations={max_combinations}. "
                f"Consider splitting fewer dimensions or filtering values."
            )

        # Construction des combinaisons {clé: valeur unique}
        return [dict(zip(keys, combo)) for combo in itertools.product(*value_lists)]

    # Structure registry
    # Méthode d'enregistrement d'une structure dans le registre
    def register_structure(self, structure: "DataflowStructure") -> None:
        """Register a dataflow structure for dimension name resolution.

        Args:
            structure: ``DataflowStructure`` to store in the registry.
        """
        # Enregistrement de la structure dans le registre
        self.structure_registry.register(structure)
        # Comptage des structures téléchargées : un accès au cache ne passe
        # jamais par ici, ce qui en fait le point de mesure des défauts de cache
        self.n_structures_fetched_ += 1
        # Logging
        logger.info(f"Registered structure for {structure.dataflow}")

    # Méthode d'extraction d'une structure d'un registre si elle existe et de téléchargement sinon
    def _ensure_structure(
        self,
        agency: str,
        dataflow: str,
        **kwargs,
    ) -> Optional["DataflowStructure"]:
        """Return the structure for a dataflow, fetching it if necessary.

        Checks the registry first. If not found and ``auto_fetch_structure``
        is ``True``, calls ``_fetch_structure`` and caches the result.

        Args:
            agency: Agency identifier used as the registry key.
            dataflow: Dataflow identifier used as the registry key.
            **kwargs: Forwarded to ``_fetch_structure`` (e.g. ``version``).

        Returns:
            ``DataflowStructure`` if available, ``None`` otherwise.
        """
        # Vérification du cache
        if self.structure_registry.has(agency, dataflow):
            return self.structure_registry.get(agency, dataflow)

        # Récupération automatique si activée
        if self.auto_fetch_structure:
            try:
                # Logging
                logger.info(f"Fetching structure for {agency}::{dataflow}")
                # Requête de la structure
                structure = self._fetch_structure(agency, dataflow, **kwargs)
                # Enregistrement
                self.register_structure(structure)
                return structure
            except Exception as e:
                # Logging
                logger.warning(
                    f"Failed to fetch structure for {agency}::{dataflow}: {e}"
                )

        return None

    # Méthode auxiliaire de détection d'une réponse SDMX « aucun enregistrement »
    @staticmethod
    def _is_no_records_error(exc: Exception) -> bool:
        """Return True if exc is an SDMX '404 NoRecordsFound' response.

        SDMX providers answer a valid query that matches no data with an HTTP
        404 whose body contains ``NoRecordsFound``. This is an empty result,
        not a genuine failure, and should be treated as such.

        Args:
            exc: Exception raised while executing a sub-request.

        Returns:
            ``True`` if ``exc`` is an HTTP 404 ``NoRecordsFound`` error,
            ``False`` otherwise.
        """
        # Filtre sur les seules erreurs HTTP porteuses d'une réponse
        if not isinstance(exc, requests.exceptions.HTTPError):
            return False
        response = exc.response
        # Statut 404 et corps signalant explicitement l'absence de données
        return (
            response is not None
            and response.status_code == 404
            and "NoRecordsFound" in (response.text or "")
        )

    #  Méthode auxiliaire d'exécution de requêtes multiples
    def _execute_split_requests(
        self,
        request_combinations: list[tuple[dict, dict]],
        *,
        report: FetchReport | None = None,
        **request_kwargs,
    ) -> pd.DataFrame:
        """Execute multiple API requests and concatenate the results.

        Iterates over the Cartesian product of split dimensions. For each
        combination, acquires the rate limiter, delegates the HTTP call to
        ``_execute_single_request``, applies post-request dimension filtering,
        and collects the resulting DataFrames.

        Counters are written into ``report`` as the loop goes — the same
        in-place style as ``SDMXDownloader._process_query`` — so that the
        sub-request outcomes survive the call instead of being only logged.

        Args:
            report: Fetch report to fill in place. A fresh one is used when
                ``None``, keeping the method usable on its own.
            request_combinations: List of ``(dims_for_request,
                dims_for_postfilter)`` tuples produced by
                ``_generate_request_combinations``.
                - ``dims_for_request``: passed verbatim to
                  ``_execute_single_request``.
                - ``dims_for_postfilter``: ``Dict[str, List[str]]``
                  (dimension name → allowed values) applied after retrieval.
            **request_kwargs: Additional keyword arguments forwarded to
                ``_execute_single_request`` for every sub-request.

        Returns:
            Concatenated DataFrame from all successful sub-requests.

        Raises:
            ValueError: If every sub-request failed or returned an empty
                DataFrame.
        """
        # Initialisation de la liste des jeux de données requêtés
        all_dataframes: list[pd.DataFrame] = []
        # Initialisation de la liste des erreurs
        errors: list[str] = []
        # Calcul du nombre de combinaisons
        n = len(request_combinations)

        # Rapport de récupération : rempli en place tout au long de la boucle
        report = report if report is not None else FetchReport()
        report.n_requests = n

        # Logging
        logger.info(f"Executing {n} split API requests")

        # Parcours des requêtes
        for i, (dims_for_request, dims_for_postfilter) in enumerate(
            request_combinations
        ):
            # Application du rate limiter avant chaque sous-requête
            if self.rate_limiter:
                self.rate_limiter.acquire()

            # Logging de progression tous les 10 requêtes
            if (i + 1) % 10 == 0 or i == 0 or i == n - 1:
                logger.info(f"Processing request {i + 1}/{n}")

            try:
                # Exécution de la requête
                df = self._execute_single_request(dims_for_request, **request_kwargs)

                # Volumétrie brute, avant tout post-filtrage
                report.rows_fetched += len(df)

                # Post-filtrage par dimensions si nécessaire
                if not df.empty and dims_for_postfilter:
                    df = self._filter_dataframe_by_dimensions(df, dims_for_postfilter)

                # Ajout du DataFrame à la liste des jeux de données requêtés si non vide
                if not df.empty:
                    all_dataframes.append(df)
                else:
                    # Sous-requête sans donnée exploitable après filtrage
                    report.n_empty_responses += 1
                    # Logging
                    logger.debug(f"Request {i + 1} returned empty after filtering")

            except Exception as e:
                # 404 NoRecordsFound : requête valide sans donnée → résultat vide
                # (et non une erreur), on poursuit sans alimenter la liste d'erreurs
                if self._is_no_records_error(e):
                    # Requête valide sans donnée : comptée à part des erreurs
                    report.n_no_records += 1
                    # Logging
                    logger.info(
                        f"Request {i + 1}/{n} returned no records (empty result)"
                    )
                    continue
                # Erreur véritable
                report.n_request_errors += 1
                # Construction du message d'erreur
                error_msg = f"Request {i + 1}/{n} failed: {e}"
                # Logging
                logger.error(error_msg)
                # Ajout de l'erreur à la liste
                errors.append(error_msg)

        # Aucun DataFrame collecté : distinction entre échec réel et résultat vide
        if not all_dataframes:
            # Vraies erreurs présentes → levée d'exception
            if errors:
                error_summary = "\n".join(errors)
                raise ValueError(
                    f"All {n} split requests failed or returned empty results.\n"
                    f"Errors:\n{error_summary}"
                )
            # Aucune erreur réelle : toutes les requêtes ont réussi mais ne renvoient
            # aucune donnée (no-records et/ou vide après post-filtrage) → DataFrame vide
            logger.info(
                "All requests returned empty results; returning empty DataFrame"
            )
            return pd.DataFrame()

        # Logging si les requêtes ont partiellement échoué
        if errors:
            # Logging
            logger.warning(
                f"{len(errors)} out of {n} requests failed. "
                f"Successfully retrieved {len(all_dataframes)} DataFrames."
            )

        # Logging
        logger.info(f"Concatenating {len(all_dataframes)} DataFrames")
        # Concaténation des jeux de données
        result = pd.concat(all_dataframes, ignore_index=True)
        # Logging
        logger.info(f"Split requests result: {len(result)} rows")

        return result

    # Méthode de filtrage post-requête
    @staticmethod
    def _filter_dataframe_by_dimensions(
        df: pd.DataFrame,
        dimension_filters: dict[str, list[str]],
    ) -> pd.DataFrame:
        """Filter a DataFrame to retain only allowed dimension values.

        Applied after retrieval when wildcard dimensions would return more
        data than requested (e.g. OECD ``*`` wildcard, or multi-value
        non-split dimensions).

        Args:
            df: DataFrame to filter.
            dimension_filters: Mapping of dimension column name to allowed
                values (``{dim_name: [value, ...]})``).

        Returns:
            Filtered DataFrame (same columns, subset of rows).
        """
        # Vérification que le jeu de données est non vide et que des dimensions de filtre sont fournies
        if df.empty or not dimension_filters:
            return df

        # Masque cumulatif : toutes les conditions doivent être vraies
        mask = pd.Series([True] * len(df), index=df.index)

        # Parcours des filtres de dimension
        for dim_name, allowed_values in dimension_filters.items():
            # Vérification que la dimension de filtre est comprise dans le jeu de données
            if dim_name not in df.columns:
                # Logging
                logger.warning(
                    f"Dimension column '{dim_name}' not found in DataFrame. "
                    f"Available columns: {list(df.columns)}. Skipping filter."
                )
                continue
            # Mise à jour du masque
            mask &= df[dim_name].isin(allowed_values)

        # Filtre du jeu de données
        filtered_df = df[mask]

        # Logging des lignes filtrées
        if len(filtered_df) < len(df):
            logger.info(
                f"Filtered {len(df) - len(filtered_df)} rows by dimensions "
                f"{list(dimension_filters.keys())}"
            )

        return filtered_df

    # Méthode auxiliaire de détection des doublons
    @staticmethod
    def _check_duplicates(
        df: pd.DataFrame,
        dimensions: dict[int, Any] | dict[str, Any],
        structure: Optional["DataflowStructure"],
        on_duplicate: "DuplicateHandling",
        default_dimensions: list[str] = [],
    ) -> int:
        """Detect and handle duplicate rows in the result DataFrame.

        Identifies the relevant key columns (filtered dimension columns plus
        ``TIME_PERIOD``) and checks for duplicate combinations. Duplicate
        rows usually indicate that wildcard dimensions returned unexpected
        extra dimension values that were not post-filtered.

        Args:
            df: DataFrame to check.
            dimensions: Dimension filter dict used in the query. Keys may be
                integer positions (OECD) or string names (Eurostat); the
                method handles both automatically.
            structure: Dataflow structure used to resolve int positions to
                column names. May be ``None`` if unavailable.
            on_duplicate: Strategy — ``"ignore"`` (no check), ``"warn"``
                (log a warning), or ``"raise"`` (raise ``ValueError``).
            default_dimensions: Default dimensions to check duplicates on.

        Returns:
            Number of duplicate rows found — the count was previously computed
            and thrown away, leaving it visible only in a warning.

        Raises:
            ValueError: If ``on_duplicate="raise"`` and duplicates are found.
        """
        # Vérification que le jeu de données est non vide
        if df.empty:
            return 0

        # Détermination des colonnes de vérification
        # Copie : la liste reçue (y compris le défaut `[]`) ne doit pas être mutée
        check_columns: list[str] = list(default_dimensions)

        # Parcours des dimensions de filtre
        for key in dimensions.keys():
            if isinstance(key, int):
                # Clé positionnelle (OECD) → résolution en nom via structure
                if structure:
                    dim_name = structure.get_name(key)
                    if dim_name and dim_name in df.columns:
                        check_columns.append(dim_name)
            else:
                # Clé nominale (Eurostat)
                if key in df.columns:
                    check_columns.append(str(key))

        # Fallback : toutes les colonnes sauf la valeur observée
        if not check_columns:
            check_columns = [
                col
                for col in df.columns
                if col.lower() not in ("value", "obs_value", "obsvalue")
            ]

        # Détection des duplicats
        duplicates = df.duplicated(subset=check_columns, keep=False)
        # Comptage des duplicats
        num_duplicates = int(duplicates.sum())

        # Affichage des duplicats
        if num_duplicates > 0:
            # Extraction des 10 premières lignes
            dup_df = df[duplicates].sort_values(check_columns).head(10)
            # Construction du message
            message = (
                f"Found {num_duplicates} duplicate rows for columns "
                f"{check_columns}. This may indicate that undesired values "
                f"are included via wildcards (*). Examples:\n{dup_df.to_string()}"
            )
            # Affichage d'un message d'erreur
            if on_duplicate == "raise":
                raise ValueError(message)
            else:
                # Warning
                warnings.warn(message, UserWarning)
                # Logging
                logger.warning(message)

        return num_duplicates

    # Méthode de parsing du CSV de réponse
    @staticmethod
    def _parse_csv_response(text: str) -> pd.DataFrame:
        """Parse a CSV-formatted API response into a DataFrame.

        Dimension codes are kept verbatim as text (``"NA"`` for Namibia is
        not a missing value, ``"01012100"`` keeps its leading zero); only an
        empty field or ``":"`` is missing, and ``OBS_VALUE`` is numeric.

        Args:
            text: Raw CSV text from the API response.

        Returns:
            DataFrame with the parsed data.

        Raises:
            ValueError: If the CSV cannot be parsed.
        """
        try:
            # Lecture du CSV sans conversion des codes de dimension
            df = read_codes_csv(text)
            # Logging
            logger.info(f"Parsed {len(df)} rows from CSV")
            return df
        except Exception as e:
            # Logging
            logger.error(f"Failed to parse CSV response: {e}")
            # Erreur
            raise ValueError(f"Failed to parse CSV response: {e}") from e

    # Constructeur d'entrée dans le context manager
    def __enter__(self) -> "AbstractSDMXClient":
        """Context manager entry."""
        return self

    # Constructeur de sortie du contexte manager
    def __exit__(
        self,
        exc_type: type | None,
        exc_val: BaseException | None,
        exc_tb: Any | None,
    ) -> None:
        """Context manager exit."""
        self.close()
