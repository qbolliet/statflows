"""OECD query request dataclass.

Public DTO encapsulating all parameters of an :meth:`OECDClient.get_data`
call, enabling type-safe construction and batching of query requests.
"""

# Importation des modules
# Modules de base
from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

# Modules du package
from ...core.queries import SDMXQueryRequest
from ...core.sdmx import (
    DimensionAtObservation,
    DuplicateHandling,
    SDMXResponseFormat,
)
from .formats import OECDResponseFormat


# Classe représentant une requête de données
@dataclass
class OECDQueryRequest(SDMXQueryRequest):
    """Represents an OECD data query request.

    This class encapsulates all parameters needed for a get_data() call,
    providing type safety and easier manipulation of query batches.

    Attributes:
        agency: Agency identifier (e.g., "OECD.SDD.STES")
        dataflow: Dataflow identifier (e.g., "DSD_KEI@DF_KEI")
        version: Dataflow version (default: "+")
        dimensions: Dimension filters
        start_period: Start period
        end_period: End period
        last_n_observations: Number of recent observations
        format: Response format
        dimension_at_observation: How to group observations
        attributes: Attributes to include
        measures: Measures to include
        on_duplicate: Duplicate handling strategy
        split_dimensions: Dimensions to split into separate requests
        max_split_combinations: Max allowed split combinations
        updated_after: Incremental-sync threshold (SDMX-CSV v2 only); string
            or datetime restricting the response to observations changed since

    Example:
        >>> query = OECDQueryRequest(
        ...     agency="OECD.SDD.STES",
        ...     dataflow="DSD_KEI@DF_KEI",
        ...     dimensions={"REF_AREA": ["FRA", "DEU"], "FREQ": "M"},
        ... )
        >>> df = client.execute_query(query)
    """

    agency: str
    dataflow: str
    version: str = "+"
    dimensions: dict[int | str, str | list[str]] | None = None
    start_period: str | None = None
    end_period: str | None = None
    last_n_observations: int | None = None
    format: OECDResponseFormat = OECDResponseFormat.CSV_LABELS
    dimension_at_observation: DimensionAtObservation = (
        DimensionAtObservation.ALL_DIMENSIONS
    )
    attributes: str | None = None
    measures: str | None = None
    on_duplicate: DuplicateHandling = "warn"
    split_dimensions: list[int | str] | None = None
    max_split_combinations: int = 100
    updated_after: str | datetime | None = None

    # Enum de format du provider (utilisé par SDMXQueryRequest.from_dict)
    _FORMAT_ENUM: ClassVar[type[SDMXResponseFormat] | None] = OECDResponseFormat
