"""Helpers reproduisant la convention « racine nommée + fusion » des scripts.

Les scripts du pipeline (``trade-analysis``) n'appellent pas de fonction de
registre : ils inlinent, au-dessus de :class:`~statflows.storage.json.Loader` et
:class:`~statflows.storage.json.Saver`, le motif suivant —

    lecture = (loader.load(path, bucket=bucket, missing_ok=True) or {}).get(root, {})
    fusion  = lecture, registry.update(entries),
              saver.save(path, {root: registry}, bucket=bucket, indent=2, ensure_ascii=False)

Ces deux fonctions rejouent ce motif pour que les tests figent le comportement
attendu par les appelants, sans introduire d'abstraction que le code de
production n'a pas.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Optional, Union

from statflows.storage.json import Loader, Saver

# Type d'un chemin de registre (local ou clé S3)
RegistryPath = Union[str, Path]


def read_named_registry(
    path: RegistryPath,
    bucket: Optional[str] = None,
    *,
    root: str,
    loader: Optional[Loader] = None,
) -> dict:
    """Lecture des entrées sous la racine ``root`` (registre ou racine absente → ``{}``).

    Args:
        path: Chemin local du registre ou clé S3.
        bucket: Nom du bucket S3 ; ``None`` pour le stockage local.
        root: Clé racine sous laquelle vivent les entrées (p. ex. ``"DOWNLOADS"``).
        loader: Instance à réutiliser ; une neuve est créée par défaut.

    Returns:
        Le dictionnaire des entrées sous ``root``, ou ``{}``.
    """
    loader = loader or Loader()
    return (loader.load(path, bucket=bucket, missing_ok=True) or {}).get(root, {})


def merge_named_registry(
    path: RegistryPath,
    entries: dict[str, Any],
    bucket: Optional[str] = None,
    *,
    root: str,
    loader: Optional[Loader] = None,
    saver: Optional[Saver] = None,
) -> None:
    """Fusion des ``entries`` fournies dans le registre (les autres clés bougent pas).

    Args:
        path: Chemin local du registre ou clé S3.
        entries: Entrées à insérer / écraser sous ``root``.
        bucket: Nom du bucket S3 ; ``None`` pour le stockage local.
        root: Clé racine sous laquelle écrire.
        loader: Instance à réutiliser pour la lecture préalable.
        saver: Instance à réutiliser pour l'écriture.
    """
    loader = loader or Loader()
    saver = saver or Saver()
    registry = read_named_registry(path, bucket, root=root, loader=loader)
    registry.update(entries)
    saver.save(
        path, {root: registry}, bucket=bucket, indent=2, ensure_ascii=False
    )
