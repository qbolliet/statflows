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

## Incremental downloads of a new provider

A provider whose API offers a server-side "updated after" filter implements
`fetch_updates` directly (as OECD does). Otherwise, mix
[`FreshnessCheckMixin`][statflows.core.client.FreshnessCheckMixin] in first and
implement two methods — the template, the per-run cache and its counters come
with it:

```python
from statflows.core.client import AbstractSDMXClient, FreshnessCheckMixin


class MyClient(FreshnessCheckMixin, AbstractSDMXClient):
    def _freshness_key(self, query):
        # Same answer for every query of a dataflow, whatever its filters
        return (query.dataflow, query.version)

    def _fetch_last_update(self, query):
        # One provider call per key and per run; may raise
        return self.get_last_update_from_the_api(query.dataflow)
```

`_fetch_incremental(query, n_observations)` (full `execute_query` by default)
and `_log_no_update` can be overridden as well. See
[Incremental downloads](downloads.md#last-update-check-and-per-run-cache) for
the cache rules.

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

[`ClientFactory`][statflows.core.factory.ClientFactory],
[`build_client`][statflows.core.factory.build_client],
[`build_queries`][statflows.core.factory.build_queries],
[`filter_codes`][statflows.core.factory.filter_codes] and
[`codelist_frame`][statflows.core.factory.codelist_frame] turn declarative
specifications (plain dictionaries) into clients and query objects, so that
download jobs can be described in configuration files.

`ClientFactory` is a small picklable recipe — a target and its options — that
builds a **new** client each time it is called. It is the object to hand over
when the client is created elsewhere: in another process, in a pipeline node,
or from a YAML/JSON configuration.

- The target is a provider alias (`"eurostat"`, `"oecd"`, `"comtrade"`,
  `"unsd"`) or the dotted path of any callable (`"package.module.Name"` or
  `"package.module:Name"`, e.g. a test double).
- Everything is checked when the factory is created, not at the first call:
  the target is imported, an unknown or missing option raises `TypeError`, and
  a string is coerced into the enum a parameter expects (`"v3"` or `"V3"` →
  `SDMXVersion.V3`).
- `credentials` (API keys…) are passed to the client but never shown: the
  `repr` only displays their count.

```yaml
# conf/eurostat.yml
client:
  target: eurostat
  options:
    api_version: v3
    timeout: 120
```

```python
import yaml

from statflows import ClientFactory
from statflows.core.download import download_updates

with open("conf/eurostat.yml", encoding="utf-8") as f:
    config = yaml.safe_load(f)

factory = ClientFactory(**config["client"])
# ClientFactory('eurostat', options={'api_version': <SDMXVersion.V3: 'v3'>, 'timeout': 120})

with factory() as client:
    report = download_updates(
        client=client,
        queries=queries,
        connector=connector,
        structures_path="registries/eurostat_structures.json",
        last_download_path="registries/eurostat_last_download.json",
    )
```

For Comtrade, pass the subscription key as a credential, e.g.
`ClientFactory("comtrade", credentials={"subscription_key": key})`. When the
client is needed right away, `build_client(provider, **options)` is the
one-shot equivalent for the provider aliases: `build_client("comtrade")` is
typed as `ComtradeClient`.
