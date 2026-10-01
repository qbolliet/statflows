"""Eurostat SDMX client package.

Re-exports the public Eurostat API so that
``from ..sources.eurostat import EurostatClient`` (and the higher-level
``statflows`` re-exports) keep working after the split into
submodules.
"""

# Importation des éléments d'intérêt du sous-package
from .client import EurostatClient
from .endpoints import (
    EurostatEndpointBuilderV21,
    EurostatEndpointBuilderV30,
)
from .formats import EurostatResponseFormat
from .queries import (
    EurostatQueryRequest,
    EurostatQueryRequestV21,
    EurostatQueryRequestV30,
)

# Réexport des éléments d'intérêt du sous-package
__all__ = [
    "EurostatResponseFormat",
    "EurostatEndpointBuilderV30",
    "EurostatEndpointBuilderV21",
    "EurostatQueryRequest",
    "EurostatQueryRequestV30",
    "EurostatQueryRequestV21",
    "EurostatClient",
]
