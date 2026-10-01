"""OECD SDMX client package.

Re-exports the public OECD API so that ``from ..sources.oecd import OECDClient``
(and the higher-level ``statflows`` re-exports) keep working
after the split into submodules.
"""

# Importation des éléments d'intérêt du sous-package
from .client import OECDClient
from .endpoints import (
    OECDEndpointBuilder,
    OECDEndpointBuilderV1,
    OECDEndpointBuilderV2,
)
from .formats import OECDResponseFormat
from .queries import OECDQueryRequest

# Réexport des éléments d'intérêt du sous-package
__all__ = [
    "OECDResponseFormat",
    "OECDEndpointBuilder",
    "OECDEndpointBuilderV1",
    "OECDEndpointBuilderV2",
    "OECDQueryRequest",
    "OECDClient",
]
