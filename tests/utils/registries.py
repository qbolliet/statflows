"""Helpers reproducing the "named root + merge" convention of the scripts.

The pipeline scripts (``trade-analysis``) do not call a registry function: they
inline, on top of :class:`~statflows.storage.json.Loader` and
:class:`~statflows.storage.json.Saver`, the following pattern —

    read  = (loader.load(path, bucket=bucket, missing_ok=True) or {}).get(root, {})
    merge = read, registry.update(entries),
            saver.save(path, {root: registry}, bucket=bucket, indent=2, ensure_ascii=False)

These two functions replay that pattern so that the tests freeze the behaviour
expected by the callers, without introducing an abstraction that the production
code does not have.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from statflows.storage.json import Loader, Saver

# Type d'un chemin de registre (local ou clé S3)
RegistryPath = str | Path


def read_named_registry(
    path: RegistryPath,
    bucket: str | None = None,
    *,
    root: str,
    loader: Loader | None = None,
) -> dict:
    """Read the entries under the ``root`` root (registry or root missing → ``{}``).

    Args:
        path: Local path of the registry or S3 key.
        bucket: S3 bucket name; ``None`` for local storage.
        root: Root key under which the entries live (e.g. ``"DOWNLOADS"``).
        loader: Instance to reuse; a new one is created by default.

    Returns:
        The dictionary of entries under ``root``, or ``{}``.
    """
    loader = loader or Loader()
    return (loader.load(path, bucket=bucket, missing_ok=True) or {}).get(root, {})


def merge_named_registry(
    path: RegistryPath,
    entries: dict[str, Any],
    bucket: str | None = None,
    *,
    root: str,
    loader: Loader | None = None,
    saver: Saver | None = None,
) -> None:
    """Merge the supplied ``entries`` into the registry (the other keys do not move).

    Args:
        path: Local path of the registry or S3 key.
        entries: Entries to insert / overwrite under ``root``.
        bucket: S3 bucket name; ``None`` for local storage.
        root: Root key under which to write.
        loader: Instance to reuse for the prior read.
        saver: Instance to reuse for the write.
    """
    loader = loader or Loader()
    saver = saver or Saver()
    registry = read_named_registry(path, bucket, root=root, loader=loader)
    registry.update(entries)
    saver.save(path, {root: registry}, bucket=bucket, indent=2, ensure_ascii=False)
