"""UNSD classification correspondence client package.

Re-exports the public UNSD API so that ``from ..sources.unsd import
UNSDClient`` (and the higher-level ``statflows`` re-exports) work
consistently with the SDMX provider packages.
"""

# Importation des éléments d'intérêt du sous-package
from .client import UNSDClient
from .formats import UNSDResponseFormat
from .queries import UNSDCorrespondenceRequest

# Réexport des éléments d'intérêt du sous-package
__all__ = [
    "UNSDResponseFormat",
    "UNSDCorrespondenceRequest",
    "UNSDClient",
]
