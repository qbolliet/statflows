"""Backends de persistance de statflows.

``S3Connection`` est partagée par les registres JSON (:mod:`statflows.storage.json`)
et, côté projet consommateur, par les chargeurs tabulaires qui en héritent. Son
import est **différé** : la classe dépend de ``boto3`` (extra ``s3``), or
:mod:`statflows.storage.ducklake` doit rester importable sans cet extra.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

__all__ = ["S3Connection"]

if TYPE_CHECKING:
    from ._connection import S3Connection


def __getattr__(name: str):
    # Import différé : `S3Connection` tire `boto3` (extra « s3 »)
    if name == "S3Connection":
        from ._connection import S3Connection

        return S3Connection
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
