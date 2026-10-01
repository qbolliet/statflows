# Importation des éléments d'intérêt du module
# Client
from .client import AbstractSDMXClient, APIClient

# Queries
from .queries import SDMXQueryRequest

# Rate limiter
from .rate_limiter import CompositeRateLimiter, RateLimiter, build_rate_limiter

# Registre des dates de dernier téléchargement (lecture, tous formats)
from .registry import RegistryEntry, iter_registry_entries

# Rapports structurés (diagnostics exploitables du téléchargement)
from .reports import (
    DownloadReport,
    FetchReport,
    HttpStats,
    QueryReport,
    RateLimitStats,
    flatten_metrics,
)

# SDMX
from .sdmx import (
    DimensionAtObservation,
    DuplicateHandling,
    SDMXEndpointBuilder,
    SDMXResponseFormat,
    SDMXVersion,
    StructureResourceType,
)

# Structures
from .structures import DataflowStructure, DataflowStructureRegistry, DimensionInfo

# Réexport des éléments d'intérêt du module
__all__ = [
    "APIClient",
    "AbstractSDMXClient",
    "DownloadReport",
    "QueryReport",
    "FetchReport",
    "HttpStats",
    "RateLimitStats",
    "flatten_metrics",
    "RateLimiter",
    "CompositeRateLimiter",
    "build_rate_limiter",
    "SDMXVersion",
    "DimensionAtObservation",
    "StructureResourceType",
    "DuplicateHandling",
    "SDMXResponseFormat",
    "SDMXEndpointBuilder",
    "SDMXQueryRequest",
    "DimensionInfo",
    "DataflowStructure",
    "DataflowStructureRegistry",
    "RegistryEntry",
    "iter_registry_entries",
]
