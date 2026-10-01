"""Persistence backends of statflows.

``S3Connection`` is shared by the JSON registries (:mod:`statflows.storage.json`)
and, on the consuming project side, by the tabular loaders that inherit from it.
Its import is **deferred**: the class depends on ``boto3`` (``s3`` extra), whereas
:mod:`statflows.storage.ducklake` must remain importable without that extra.
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
