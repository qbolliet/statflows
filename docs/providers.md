# Data providers

`statflows.sources` provides one client per provider. All of them inherit from
[`APIClient`][statflows.core.client.APIClient] (HTTP session with retries,
`close()`, context-manager support) and load their rate limit from
`statflows/parameters/<provider>.json`.

| Client | Provider | Protocol | Main entry points |
|--------|----------|----------|-------------------|
| [`EurostatClient`][statflows.sources.eurostat.client.EurostatClient] | Eurostat | SDMX 3.0 (default) / 2.1 | `get_data`, `get_structure`, `get_codelist`, `list_all_dataflows` |
| [`OECDClient`][statflows.sources.oecd.client.OECDClient] | OECD | SDMX 2.1 / 3.0 | `get_data`, `get_structure`, `list_all_dataflows` |
| [`ComtradeClient`][statflows.sources.comtrade.client.ComtradeClient] | UN Comtrade | REST (non-SDMX) | `get_data`, `get_metadata`, `get_codelist`, `get_valid_periods` |
| [`UNSDClient`][statflows.sources.unsd.client.UNSDClient] | UN Statistics Division | static workbooks | `list_available_tables`, `get_correspondence` |

## SDMX providers (Eurostat, OECD)

Both clients derive from
[`AbstractSDMXClient`][statflows.core.client.AbstractSDMXClient]. A request
goes through the same pipeline:

1. the **endpoint builder** ([`SDMXEndpointBuilder`][statflows.core.sdmx.SDMXEndpointBuilder])
   builds the URL, headers and parameters for the chosen API version;
2. the **rate limiter** blocks until the provider's quota allows the call;
3. the response (CSV / SDMX-JSON / SDMX-ML) is **parsed** into a `pandas.DataFrame`;
4. the *dataflow structure* ([`DataflowStructure`][statflows.core.structures.DataflowStructure])
   is cached in a [`DataflowStructureRegistry`][statflows.core.structures.DataflowStructureRegistry],
   which lets dimensions be given by **name or position**.

```python
from statflows import OECDClient

client = OECDClient()
df = client.get_data(
    agency="OECD.SDD.STES",
    dataflow="DSD_KEI@DF_KEI",
    dimensions={"REF_AREA": ["FRA", "DEU"]},
    start_period="2020",
)
```

## UN Comtrade

Comtrade does not follow SDMX. `ComtradeClient.get_data` issues one tariffline
request per period and, when a response hits the per-call record limit,
recursively subdivides it over the flow-defining dimensions until every chunk
fits. It returns a tuple `(DataFrame, request_metadata)`.

## UNSD correspondences

`UNSDClient.get_correspondence(source, target, kind="conversion")` fetches one
classification correspondence table (e.g. `"HS2022"` → `"HS2017"`) and
normalises it on the canonical schema `source_classification`, `source_code`,
`target_classification`, `target_code`, `relationship`.

## Rate limiting

Every client applies the limits declared in `parameters/<provider>.json` through
a [`RateLimiter`][statflows.core.rate_limiter.RateLimiter] (sliding window), or a
[`CompositeRateLimiter`][statflows.core.rate_limiter.CompositeRateLimiter] when
several limits apply at once. Waiting time and request counts are reported in
the [`HttpStats`][statflows.core.reports.HttpStats] and
[`RateLimitStats`][statflows.core.reports.RateLimitStats] of each report.

## Factories

[`build_client`][statflows.core.factory.build_client],
[`build_queries`][statflows.core.factory.build_queries],
[`filter_codes`][statflows.core.factory.filter_codes] and
[`codelist_frame`][statflows.core.factory.codelist_frame] turn declarative
specifications (plain dictionaries) into clients and query objects, so that
download jobs can be described in configuration files.
