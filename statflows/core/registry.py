"""Registry of last-download dates, in its single-file or sharded layout.

The download orchestrator (:mod:`statflows.core.download`) records, for every
query, the instant of its last successful download. Downstream steps read the
same registry to decide what to recompute. Two physical layouts coexist:

- **single file** (historical): ``<last_download_path>`` holds
  ``{"DOWNLOADS": {identity_key: entry, ...}}``;
- **sharded**: ``<last_download_path without extension>/<shard>.json``, each
  fragment holding ``{"DOWNLOADS": {...}}`` for a subset of the entries, so a
  flush only rewrites the fragments that changed.

Readers should go through :func:`iter_registry_entries`, which hides the
layout. When both layouts are present (migration in progress, or interrupted),
entries are merged per identity key and the most recent ``last_download`` wins:
an entry only ever moves forward, so the merge is always safe.

The module depends on nothing heavier than :mod:`statflows.storage.json`: it is
importable without the optional ``ducklake`` extra.
"""

# Importation des modules
from __future__ import annotations

import logging
import re
from collections.abc import Iterator, Mapping

# Modules de base
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import (
    Any,
    cast,
)

# Modules de stockage des registres JSON
from ..storage.json import Loader, Saver

# Initialisation du logger
logger = logging.getLogger(__name__)

# Clé racine du registre JSON des dates de dernier téléchargement
REGISTRY_ROOT = "DOWNLOADS"
# Fragment recevant les entrées héritées dont la requête est absente du run
DEFAULT_SHARD = "_default"
# Caractères interdits dans un nom de fragment (remplacés par « _ »)
_SHARD_FORBIDDEN = re.compile(r"[^A-Za-z0-9_.\-]")

# Type d'un chemin de registre (local ou clé S3)
RegistryPath = str | Path


# ──────────────────────────────────────────────────────────────────────
# Fonctions utilitaires
# ──────────────────────────────────────────────────────────────────────


# Fonction de parsing d'une chaîne ISO en datetime UTC
def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 string into a UTC-aware datetime.

    Args:
        value: ISO-8601 string (``Z`` suffix accepted) or ``None``.

    Returns:
        UTC-aware ``datetime`` or ``None`` when ``value`` is falsy/unparseable.
    """
    # Court-circuit si la valeur est absente
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        logger.warning(f"Could not parse stored date '{value}'")
        return None
    # Normalisation en UTC (les datetimes naïfs sont interprétés comme UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


# Fonction de calcul du répertoire des fragments
def fragment_dir(last_download_path: RegistryPath) -> Path:
    """Return the directory holding the fragments of a sharded registry.

    Args:
        last_download_path: Path (or S3 key) of the historical single file.

    Returns:
        The same path without its extension.

    Examples:
        >>> fragment_dir("registries/last_download.json").as_posix()
        'registries/last_download'
    """
    return Path(last_download_path).with_suffix("")


# Fonction de normalisation d'un nom de fragment
def sanitize_shard(name: Any) -> str:
    """Turn a shard key into a safe file name (without extension).

    Characters other than letters, digits, ``_``, ``.`` and ``-`` become
    ``_``; leading dots are stripped (dot-files are ignored when listing); an
    empty result falls back to :data:`DEFAULT_SHARD`.

    Args:
        name: Value returned by the shard-key callable.

    Returns:
        Sanitised shard name.

    Examples:
        >>> sanitize_shard("DSD_KEI@DF_KEI")
        'DSD_KEI_DF_KEI'
        >>> sanitize_shard("")
        '_default'
    """
    sanitized = _SHARD_FORBIDDEN.sub("_", str(name)).lstrip(".")
    return sanitized or DEFAULT_SHARD


# Fonction de calcul du chemin d'un fragment
def shard_path(last_download_path: RegistryPath, shard: str) -> Path:
    """Return the path (or S3 key) of one fragment of a sharded registry.

    Args:
        last_download_path: Path of the historical single file.
        shard: Sanitised shard name.

    Returns:
        ``<last_download_path without extension>/<shard>.json``.
    """
    return fragment_dir(last_download_path) / f"{shard}.json"


# Fonction auxiliaire : une entrée est-elle au moins aussi récente qu'une autre
def _not_older(candidate: Mapping[str, Any], current: Mapping[str, Any]) -> bool:
    """Tell whether ``candidate`` is at least as recent as ``current``.

    An unparseable date counts as the oldest possible one.

    Args:
        candidate: Raw entry read last.
        current: Raw entry already retained.

    Returns:
        ``True`` if ``candidate`` should replace ``current``.
    """
    oldest = datetime.min.replace(tzinfo=UTC)
    new = _parse_iso(candidate.get("last_download")) or oldest
    old = _parse_iso(current.get("last_download")) or oldest
    return new >= old


# ──────────────────────────────────────────────────────────────────────
# Lecture
# ──────────────────────────────────────────────────────────────────────


# Entrée du registre, indépendante du format physique
@dataclass(frozen=True)
class RegistryEntry:
    """One entry of the last-download registry.

    Attributes:
        identity_key: Identity key of the query (``query.identity_key()``).
        agency: Provider agency.
        dataflow: Dataflow identifier.
        params: JSON-safe parameters of the query (``query.to_dict()``).
            Excluded from hashing (a mapping is unhashable).
        last_download: UTC instant of the last successful download — the
            instant captured just *before* the request was sent.
    """

    identity_key: str
    agency: str
    dataflow: str
    params: dict[str, Any] = field(hash=False)
    last_download: datetime

    # Construction depuis une entrée JSON brute
    @classmethod
    def from_raw(
        cls, identity_key: str, raw: Mapping[str, Any]
    ) -> RegistryEntry | None:
        """Build an entry from its JSON representation.

        Args:
            identity_key: Key of the entry in the registry.
            raw: ``{"agency", "dataflow", "params", "last_download"}`` mapping.

        Returns:
            The entry, or ``None`` when ``last_download`` is missing or
            unparseable.
        """
        last_download = _parse_iso(raw.get("last_download"))
        if last_download is None:
            return None
        return cls(
            identity_key=identity_key,
            agency=raw.get("agency", ""),
            dataflow=raw.get("dataflow", ""),
            params=dict(raw.get("params") or {}),
            last_download=last_download,
        )

    # Conversion en entrée JSON brute
    def to_raw(self) -> dict[str, Any]:
        """Return the JSON representation stored in the registry."""
        return {
            "agency": self.agency,
            "dataflow": self.dataflow,
            "params": dict(self.params),
            "last_download": self.last_download.isoformat(),
        }


# Fonction de lecture des entrées brutes d'un registre, tous formats confondus
def load_registry_records(
    last_download_path: RegistryPath,
    loader: Loader,
    bucket: str | None = None,
) -> dict[str, tuple[dict[str, Any], str | None]]:
    """Read the raw entries of a registry, whatever its physical layout.

    Reads the historical single file, then every fragment, and keeps per
    identity key the most recent ``last_download``. An unreadable fragment is
    skipped with a warning: its entries then look older (or absent), which only
    ever causes a query to be downloaded again, never data to be skipped.

    Args:
        last_download_path: Path (or S3 key) of the historical single file.
        loader: Loader to read with (its S3 connection is reused).
        bucket: S3 bucket, or ``None`` for the local filesystem.

    Returns:
        Mapping ``identity_key → (raw entry, shard)``, where ``shard`` is the
        name of the fragment the entry came from, or ``None`` when it came from
        the single file.
    """
    # Fichier unique historique (absent → aucune entrée)
    single = loader.load(last_download_path, bucket=bucket, missing_ok=True) or {}
    records: dict[str, tuple[dict[str, Any], str | None]] = {
        key: (raw, None) for key, raw in single.get(REGISTRY_ROOT, {}).items()
    }

    # Fragments : à date égale, le fragment l'emporte (format cible)
    for fragment in loader.list_json(fragment_dir(last_download_path), bucket=bucket):
        shard = Path(fragment).stem
        try:
            data = loader.load(fragment, bucket=bucket) or {}
        except Exception as exc:
            # Logging
            logger.warning(f"Skipping unreadable registry fragment {fragment}: {exc}")
            continue
        for key, raw in data.get(REGISTRY_ROOT, {}).items():
            if key not in records or _not_older(raw, records[key][0]):
                records[key] = (raw, shard)
    return records


# Fonction publique d'itération sur les entrées du registre
def iter_registry_entries(
    last_download_path: RegistryPath,
    bucket: str | None = None,
    storage_options: dict[str, Any] | None = None,
) -> Iterator[RegistryEntry]:
    """Iterate over the entries of a last-download registry.

    Reads the historical single file and/or the fragments of a sharded
    registry transparently (see the module docstring for the merge rule), so
    consumers never depend on the physical layout.

    Args:
        last_download_path: Path (or S3 key) of the registry, as passed to
            :class:`~statflows.core.download.SDMXDownloader`.
        bucket: S3 bucket, or ``None`` for the local filesystem.
        storage_options: Keyword arguments forwarded to the loader's
            ``connect()`` (e.g. ``endpoint_url``); only used with ``bucket``.

    Yields:
        :class:`RegistryEntry` objects, sorted by identity key. Entries whose
        date is missing or unparseable are skipped with a warning.

    Examples:
        >>> stale = [
        ...     e.identity_key
        ...     for e in iter_registry_entries("registries/eurostat_last_download.json")
        ...     if e.last_download < cutoff
        ... ]  # doctest: +SKIP
    """
    # Lecteur (connexion S3 explicite si des options sont fournies)
    loader = Loader()
    if bucket is not None and storage_options:
        loader.connect(**storage_options)

    # Lecture des deux formats puis conversion en entrées typées
    records = load_registry_records(last_download_path, loader, bucket=bucket)
    for key in sorted(records):
        entry = RegistryEntry.from_raw(key, records[key][0])
        if entry is None:
            # Logging
            logger.warning(f"Skipping registry entry '{key}': no valid last_download")
            continue
        yield entry


# ──────────────────────────────────────────────────────────────────────
# Écriture (usage interne de l'orchestrateur)
# ──────────────────────────────────────────────────────────────────────


# Registre en mémoire et sa persistance
class DownloadRegistry:
    """In-memory last-download registry with layout-aware persistence.

    Internal to the download orchestrator: entries are committed in memory and
    persisted by :meth:`flush`, whose cadence is decided by the caller.

    - In single-file mode, :meth:`flush` rewrites the whole file (the
      historical behaviour).
    - In sharded mode, it rewrites only the fragments touched since the
      previous flush. Entries read from the historical single file are
      migrated: they get a shard through :meth:`assign_shards` (or
      :data:`DEFAULT_SHARD`) and are all written at the first flush.

    Args:
        path: Path (or S3 key) of the historical single file.
        loader: Loader used by :meth:`load`.
        saver: Saver used by :meth:`flush`.
        bucket: S3 bucket, or ``None`` for the local filesystem.
        sharded: Whether to persist as fragments.
    """

    # Initialisation
    def __init__(
        self,
        path: RegistryPath,
        *,
        loader: Loader,
        saver: Saver,
        bucket: str | None = None,
        sharded: bool = False,
    ) -> None:
        # Instanciation des attributs
        self._path = path
        self._loader = loader
        self._saver = saver
        self._bucket = bucket
        self.sharded = sharded
        # Entrées brutes (identity_key → entrée JSON)
        self.entries: dict[str, dict[str, Any]] = {}
        # Fragment de chaque entrée et membres de chaque fragment
        self._shard_of: dict[str, str | None] = {}
        self._members: dict[str, set[str]] = {}
        # Fragments à réécrire au prochain flush
        self._dirty: set[str] = set()

    # Méthode de chargement (les deux formats)
    def load(self) -> None:
        """Load the registry from storage, whatever its current layout."""
        # Chargement du registre
        records = load_registry_records(self._path, self._loader, bucket=self._bucket)
        # Instanciation des entrées chargées
        self.entries = {key: raw for key, (raw, _) in records.items()}
        # Initialisation des paramètres d'écriture
        self._shard_of = {}
        self._members = {}
        self._dirty = set()
        # Entrées issues du fichier unique : fragment ``None``, migrées au flush
        for key, (_, shard) in records.items():
            self._place(key, shard)

    # Méthode de lecture d'une entrée
    def get(self, key: str) -> dict[str, Any] | None:
        """Return the raw entry of ``key``, or ``None``."""
        return self.entries.get(key)

    # Nombre d'entrées
    def __len__(self) -> int:
        return len(self.entries)

    # Méthode d'affectation des fragments des entrées héritées
    def assign_shards(self, shard_of: Mapping[str, str]) -> None:
        """Give a shard to the entries read from the historical single file.

        Entries whose query is unknown to ``shard_of`` go to
        :data:`DEFAULT_SHARD`. No-op in single-file mode.

        Args:
            shard_of: Mapping ``identity_key → sanitised shard`` computed from
                the queries of the run.
        """
        # Cas où l'ensemble est vide
        if not self.sharded:
            return

        # Assignation
        for key in list(self._members.get(cast(Any, None), ())):
            self._move(key, shard_of.get(key, DEFAULT_SHARD))

    # Méthode de validation d'une entrée
    def commit(self, key: str, raw: dict[str, Any], shard: str | None = None) -> None:
        """Record an entry in memory (persisted at the next :meth:`flush`).

        Args:
            key: Identity key.
            raw: JSON entry.
            shard: Sanitised shard (sharded mode only; ``None`` keeps the
                entry's current shard, or :data:`DEFAULT_SHARD` for a new one).
        """
        # Remplissage de l'entrée
        self.entries[key] = raw

        # Cas où l'ensemble est vide
        if not self.sharded:
            return

        target = shard or self._shard_of.get(key) or DEFAULT_SHARD
        self._move(key, target)
        self._dirty.add(target)

    # Méthode de persistance
    def flush(self) -> int:
        """Persist the registry.

        Returns:
            Number of files written: always 1 in single-file mode, the number of
            touched fragments (possibly 0) in sharded mode.
        """
        # Fichier unique : réécriture complète
        if not self.sharded:
            self._save(self._path, self.entries)
            return 1

        # Fragments : entrées héritées encore sans fragment → fragment par défaut
        if None in self._members:
            self.assign_shards({})
        # Réécriture des seuls fragments modifiés
        dirty = sorted(s for s in self._dirty if s is not None)
        for shard in dirty:
            members = self._members.get(shard, set())
            self._save(
                shard_path(self._path, shard),
                {key: self.entries[key] for key in sorted(members)},
            )
        self._dirty.clear()
        return len(dirty)

    # Méthode auxiliaire d'écriture d'un fichier de registre
    def _save(self, path: RegistryPath, entries: Mapping[str, Any]) -> None:
        """Write ``{"DOWNLOADS": entries}`` through the saver."""
        self._saver.save(
            path,
            {REGISTRY_ROOT: dict(entries)},
            bucket=self._bucket,
            indent=2,
            ensure_ascii=False,
        )

    # Méthode auxiliaire de placement initial d'une entrée
    def _place(self, key: str, shard: str | None) -> None:
        """Index ``key`` in ``shard`` (no dirtiness)."""
        self._shard_of[key] = shard
        self._members.setdefault(shard, set()).add(key)  # type: ignore[arg-type]

    # Méthode auxiliaire de déplacement d'une entrée vers un fragment
    def _move(self, key: str, target: str) -> None:
        """Move ``key`` to ``target``, marking both fragments dirty."""
        current = self._shard_of.get(key)
        if key in self._shard_of and current == target:
            return
        if key in self._shard_of:
            members = self._members.get(current, set())  # type: ignore[arg-type]
            members.discard(key)
            if not members:
                self._members.pop(current, None)  # type: ignore[arg-type]
            # L'ancien fragment doit être réécrit sans l'entrée
            if current is not None:
                self._dirty.add(current)
        self._place(key, target)
        self._dirty.add(target)


# Liste des noms exportés
__all__ = [
    "REGISTRY_ROOT",
    "DEFAULT_SHARD",
    "RegistryEntry",
    "iter_registry_entries",
    "load_registry_records",
    "fragment_dir",
    "shard_path",
    "sanitize_shard",
    "DownloadRegistry",
]
