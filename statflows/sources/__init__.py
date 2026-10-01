# Importation des éléments d'intérêt du module
# OECD
# Comtrade
from .comtrade import (
    ComtradeClient,
    ComtradeQueryRequest,
    ComtradeResponseFormat,
)

# Eurostat
from .eurostat import (
    EurostatClient,
    EurostatEndpointBuilderV21,
    EurostatEndpointBuilderV30,
    EurostatQueryRequest,
    EurostatQueryRequestV21,
    EurostatQueryRequestV30,
    EurostatResponseFormat,
)
from .oecd import (
    OECDClient,
    OECDEndpointBuilder,
    OECDEndpointBuilderV1,
    OECDEndpointBuilderV2,
    OECDQueryRequest,
    OECDResponseFormat,
)

# UNSD
from .unsd import (
    UNSDClient,
    UNSDCorrespondenceRequest,
    UNSDResponseFormat,
)

# Réexport des éléments d'intérêt du module
__all__ = [
    # OECD
    "OECDResponseFormat",
    "OECDEndpointBuilder",
    "OECDEndpointBuilderV1",
    "OECDEndpointBuilderV2",
    "OECDQueryRequest",
    "OECDClient",
    # Eurostat
    "EurostatResponseFormat",
    "EurostatEndpointBuilderV30",
    "EurostatEndpointBuilderV21",
    "EurostatQueryRequest",
    "EurostatQueryRequestV30",
    "EurostatQueryRequestV21",
    "EurostatClient",
    # Comtrade
    "ComtradeResponseFormat",
    "ComtradeQueryRequest",
    "ComtradeClient",
    # UNSD
    "UNSDResponseFormat",
    "UNSDCorrespondenceRequest",
    "UNSDClient",
]
