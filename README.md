# statflows

[![CI](https://github.com/qbolliet/statflows/actions/workflows/ci.yml/badge.svg)](https://github.com/qbolliet/statflows/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/qbolliet/statflows/branch/main/graph/badge.svg)](https://codecov.io/gh/qbolliet/statflows)
[![Docs](https://img.shields.io/badge/docs-mkdocs-blue.svg)](https://qbolliet.github.io/statflows/)
[![Python](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![uv](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/uv/main/assets/badge/v0.json)](https://github.com/astral-sh/uv)

<!-- --8<-- [start:intro] -->
This package provides a set of statistical API clients (Eurostat, OECD, COMTRADE, UNSD) and an incremental download orchestrator that makes it possible to industrialise the download and update tracking of databases. For data providers that use SDMX, a common software structure is shared; it is adapted for the other providers.
<!-- --8<-- [end:intro] -->

Full documentation: <https://qbolliet.github.io/statflows/>.

## What the package does

<!-- --8<-- [start:overview] -->
- **`statflows.sources`** — one client per provider: `EurostatClient`,
  `OECDClient` (SDMX 2.1 / 3.0), `ComtradeClient` and `UNSDClient`. Each client
  builds its URLs from the structure of the *dataflow*, applies the provider's
  own *rate limiting*, parses the response (CSV / SDMX-JSON / SDMX-ML) and
  returns a `pandas.DataFrame`.
- **`statflows.core`** — the shared foundation: HTTP client (`APIClient`),
  *rate limiters*, a registry of *dataflow* structures, structured execution
  reports (`DownloadReport`, `QueryReport`), the `build_client` /
  `build_queries` / `filter_codes` factories, and `codelist_frame` (codelists
  with labels, Eurostat and Comtrade).
- **`statflows.core.download`** — the `download_updates` / `SDMXDownloader`
  orchestrator: for a list of queries, a first **full** download of the missing
  series, then an **incremental** download of the series already present,
  writing to a DuckLake catalog (one schema per *dataflow*), and maintaining a
  JSON registry of last-download dates — as a single file or sharded, readable
  through `iter_registry_entries` — with optional buffering of the registry and
  of the writes (see "Performance and volume").
- **`statflows.storage`** — `S3Connection` (shared `boto3` / `s3fs` session) and
  `statflows.storage.json` (`Loader` / `Saver` for JSON registries, local or on
  S3). `statflows.storage.ducklake.tables` provides the `write_dataframe` helper
  (schema creation on the first call, upsert by primary key afterwards).
<!-- --8<-- [end:overview] -->

## Installation

<!-- --8<-- [start:installation] -->
The package is installed from git (not yet published on PyPI):

```bash
# Core only: clients + parsing + one-off queries
pip install "git+https://github.com/qbolliet/statflows"

# With S3 storage for the JSON registries
pip install "statflows[s3] @ git+https://github.com/qbolliet/statflows"

# With DuckLake writing (pulls in dt-ducklake-manager + duckdb)
pip install "statflows[ducklake] @ git+https://github.com/qbolliet/statflows"

# Everything
pip install "statflows[all] @ git+https://github.com/qbolliet/statflows"
```

| Extra        | Content                     | Required for                                                             |
|--------------|-----------------------------|-------------------------------------------------------------------------|
| *(base)*     | `requests`, `pandas`, `pyarrow`, `pyyaml` | clients, one-off queries, parsing, JSON registries **locally** |
| `s3`         | `boto3`, `s3fs`             | reading/writing JSON registries **on a bucket** (`Loader`/`Saver` with `bucket=...`, `download_updates` with `bucket=...`) |
| `ducklake`   | `duckdb`, `dt-ducklake-manager` | `statflows.storage.ducklake.tables` (writing), `statflows.core.download` |
| `all`        | union                       | —                                                                       |

The core (`import statflows`, `statflows.core`, `statflows.sources`,
`statflows.storage`) can be imported without any extra. `statflows.storage.json`
(`Loader` / `Saver`) can be imported and works **locally** without the `s3`
extra; a call with `bucket=...` without `boto3` installed raises an explicit
`ImportError`. `statflows.core.download` and
`statflows.storage.ducklake.tables` are only loaded through an explicit import.
<!-- --8<-- [end:installation] -->

## Examples

<!-- --8<-- [start:examples] -->
### 1. A one-off Eurostat query (no DuckLake)

```python
from statflows import EurostatClient

client = EurostatClient()  # SDMX 3.0 by default

df = client.get_data(
    "namq_10_gdp",
    dimensions={"geo": ["FR", "DE"], "na_item": "B1GQ", "unit": "CP_MEUR"},
    start_period="2020-Q1",
    last_n_observations=8,
)
print(df.head())
```

### 2. A JSON `Loader` / `Saver` round trip on S3

```python
from statflows.storage.json import Loader, Saver

# Credentials read from the AWS_* environment variables (or passed as
# kwargs: aws_access_key_id=..., aws_secret_access_key=..., endpoint_url=...).
registry = {
    "DOWNLOADS": {
        "eurostat:namq_10_gdp": {"last_download": "2026-08-31T00:00:00Z"},
    }
}

Saver().save(
    "registries/last_download.json",
    registry,
    bucket="my-bucket",
    indent=2,
)

# missing_ok=True: a first run (missing object) returns None instead of raising.
loaded = (
    Loader().load(
        "registries/last_download.json",
        bucket="my-bucket",
        missing_ok=True,
    )
    or {}
)

assert loaded == registry
```

### 3. A complete `download_updates` into a local DuckLake catalog

```python
from datetime import timedelta

from dt_ducklake_manager import DuckLakeConnector

from statflows import EurostatClient
from statflows.core.factory import build_queries
from statflows.core.download import download_updates

# Local DuckLake catalog: metadata in catalog.ducklake, Parquet data under
# data/ — no PostgreSQL server required.
connector = DuckLakeConnector("catalog.ducklake", "data/")

queries = build_queries(
    "eurostat",
    [
        {
            "dataflow": "namq_10_gdp",
            "dimensions": {"geo": ["FR", "DE"], "na_item": "B1GQ"},
        },
        {
            "dataflow": "une_rt_q",
            "dimensions": {"geo": ["FR", "DE"], "sex": "T", "age": "TOTAL"},
        },
    ],
)

report = download_updates(
    client=EurostatClient(),
    queries=queries,
    connector=connector,
    structures_path="registries/eurostat_structures.json",
    last_download_path="registries/eurostat_last_download.json",
    n_observations=10,
    max_runtime=timedelta(hours=1),
)

print(report)  # DownloadReport: processed queries, rows written, errors, HTTP
```

The second call with the same queries only downloads the newly published
observations (incremental mode), based on the dates recorded in
`eurostat_last_download.json`.
<!-- --8<-- [end:examples] -->

## Performance and volume

<!-- --8<-- [start:performance] -->
### Cost of the default mode

By default, `download_updates` / `SDMXDownloader` reproduce the historical
behaviour: **every** non-empty query gets its own DuckLake transaction (upsert,
audit, post-write compaction) and **the whole registry** is rewritten after each
query. This is safe but expensive on a full catch-up (10⁴ to 10⁵ queries): the
registry weighs several tens of MB and rewriting it repeatedly has a quadratic
cost, and each query creates snapshots and Parquet files.

Measured on the test bench (`tests/integration/core/test_download_buffering.py`:
2,000 queries of which 100 non-empty, 2 *dataflows*, registry on simulated S3):

| Mode                                                   | Registry writes | DuckLake batches | Snapshots |
|--------------------------------------------------------|----------------:|-----------------:|----------:|
| default                                                | 2,000           | 100              | 200       |
| `registry_flush_every=500`, `write_batch_queries=500`  | 4               | 2                | 4         |

(A `dt-ducklake-manager` upsert creates two snapshots: the write of the fact
table and the `dataset_metadata.updated_at` timestamp.)

### Parameters

| Parameter                | Default | Effect |
|--------------------------|---------|--------|
| `registry_flush_every`   | `1`     | Persists the registry after this many validated entries. |
| `registry_flush_seconds` | `None`  | Also persists the registry after this delay (checked on every validation). |
| `registry_shard_key`     | `None`  | `query → str`: sharded registry, only the modified shards are rewritten. |
| `write_batch_rows`       | `None`  | Writes a schema's batch as soon as it reaches this number of rows. |
| `write_batch_queries`    | `None`  | Writes a schema's batch as soon as it reaches this number of non-empty queries. |
| `update_options`         | `None`  | Options passed to `DatabaseUpdater.update_database` (e.g. `allow_new_columns`). |
| `build_options`          | `None`  | Options passed to `DuckLakeTablesBuilder.build_schema` (e.g. `partition_by`). |
| `ducklake_options`       | `None`  | DuckLake options applied for the duration of the connection (including `data_inlining_row_limit`). |
| `run_id`                 | `None`  | Run identifier recorded on every snapshot written. |

**Invariant.** A registry entry **never** advances before the data of its query
has been successfully written. Batches are built **per schema** (one per
*dataflow*), concatenated then deduplicated by primary key (last one wins); the
entries of their queries are only validated after the batch has been written. If
a batch write fails, none of its entries advance, the error is counted for each
of its queries in the `DownloadReport` (they will be downloaded again on the
next run) and the run continues. An empty query validates its entry
immediately.

**Stops.** Pending batches and the registry are persisted at the end of the run,
when `max_runtime` expires, on exception and on `SIGTERM` (shutdown of a
Kubernetes pod): during `run()`, a signal handler is installed (in the main
thread only; elsewhere, a warning is logged) and then restored. A `SIGTERM`
interrupts the fetch in progress — never a DuckLake write nor a registry
persistence — and `run()` returns its report with `stopped_early=True`. The pod
grace period (`terminationGracePeriodSeconds`) must cover the write of one
batch.

**Diagnostics.** `DownloadReport` exposes `n_registry_flushes`,
`n_write_batches` and `rows_pending_at_stop` (always expected to be `0`), also
included in `to_metrics()`. With buffering, the `QueryReport` of a non-empty
query is published (and `on_query_complete` called) when its batch is written
or has failed.

**Memory.** The DataFrames of a batch stay in memory until it is written: the
footprint is bounded by `write_batch_rows` × the number of active *dataflows*.

### Sharded registry

With `registry_shard_key`, the registry `registries/x_last_download.json`
becomes a directory `registries/x_last_download/<shard>.json`. An existing
single-file registry is migrated on the first persistence (entries of queries
absent from the run go to `_default.json`); the historical file is left in
place: on read, both formats are merged and the most recent date wins. A write
only costs the shards touched since the previous persistence: choose a key
**correlated with the processing order** (for example the reporter, if the
query list iterates over it in the outer loop), otherwise every persistence
touches all the shards.

Downstream steps read the registry without depending on its format:

```python
from statflows import iter_registry_entries

for entry in iter_registry_entries(
    "registries/eurostat_last_download.json", bucket="my-bucket"
):
    print(
        entry.identity_key, entry.dataflow, entry.last_download
    )  # frozen RegistryEntry
```

### Compaction and inlining

`write_dataframe` does not force `compact_after_update`: the post-write
compaction follows the default of `update_database` in `dt-ducklake-manager`.
Compacting after every batch can be expensive and is better placed at the end of
the run or in a scheduled maintenance:
`update_options={"compact_after_update": False}` disables it. When active, it
runs after the commit of each upsert (hence once per batch) and corresponds to
the light compaction of `dt-ducklake-manager` on `<schema>.fact_table`:
`ducklake_merge_adjacent_files` then `ducklake_rewrite_data_files`. It does not
expire any snapshot, does not delete any file and does not flush inlined data;
its failures are logged without interrupting the write.

`DuckLakeConnector` accepts the `DATA_INLINING_ROW_LIMIT` ATTACH option
(`data_inlining_row_limit` argument):
`ducklake_options={"data_inlining_row_limit": N}` sets it for the duration of the
run's connection, without permanently modifying the connector (the other keys
are merged into its `set_option` options). Writes of fewer than `N` rows are then
kept in the catalog rather than in Parquet files; they stay there until an
explicit flush (`ducklake_flush_inlined_data`), which the post-write compaction
does not perform. On the test bench, the DuckLake extension shipped with
DuckDB 1.5 already inlines small writes by default; with large batches, inlining
only concerns the last partial batches.

### Recommended configuration for 10⁵ queries

```python
from datetime import timedelta

report = download_updates(
    client=EurostatClient(),
    queries=queries,  # ~100,000 queries, reporter in the outer loop
    connector=connector,
    structures_path="registries/comext_structures.json",
    last_download_path="registries/comext_last_download.json",
    bucket="my-bucket",
    max_runtime=timedelta(hours=23),
    # Registry: at most one write per 1,000 entries or per 5 minutes,
    # sharded by dataflow and reporter
    registry_flush_every=1_000,
    registry_flush_seconds=300,
    registry_shard_key=lambda q: f"{q.dataflow}_{q.dimensions.get('reporter')}",
    # DuckLake: one transaction per batch of 500 queries or 500,000 rows
    write_batch_queries=500,
    write_batch_rows=500_000,
    run_id="comext-2026-10-01",
)
assert report.rows_pending_at_stop == 0
```

On 10⁵ queries, the registry goes from 10⁵ full rewrites to about a hundred
persistences limited to the touched shards, and the catalog goes from one
transaction per non-empty query to one per slice of 500 (plus one partial batch
per *dataflow* at the end of the run).

### Codelists with labels

`statflows.core.factory.codelist_frame(client, dimension, structure=None)`
returns a DataFrame with `code`, `label` (and `parent` for hierarchical
classifications) for `EurostatClient` and `ComtradeClient`. Codelists are cached
per client: those already downloaded to build the queries cost no second network
call.

```python
from statflows.core.factory import codelist_frame

structure = client.get_dataflow_structure("DS-045409")
products = codelist_frame(client, "product", structure)  # Eurostat Comext
countries = codelist_frame(
    comtrade_client, "reporter", keep_metadata=True
)  # ISO, isGroup…
```
<!-- --8<-- [end:performance] -->

## Development

<!-- --8<-- [start:development] -->
```bash
uv sync --all-extras
uv run pytest                        # the whole suite
uv run pytest tests/unit             # unit tests only (run on a bare install)
uv run pytest -m "not integration"   # excludes tests against simulated services
uv run pytest --cov                  # with the coverage report (terminal)
uv run pytest --cov --cov-report=html   # HTML report in htmlcov/
```

Test layout:

- `tests/unit/` — unit tests, mirroring the package (`core/`, `sources/`,
  `storage/json/`, `storage/ducklake/`): pure logic and local I/O, no external
  service.
- `tests/integration/` — end-to-end against `moto` (S3) and a file-based
  DuckLake catalog; every test there is marked `integration`.
- `tests/utils/` — shared test functions and classes.

DuckLake tests are skipped at collection time when `duckdb` /
`dt_ducklake_manager` are missing; S3 tests when `moto` is missing.
<!-- --8<-- [end:development] -->

## Documentation

The documentation site is built with [MkDocs](https://www.mkdocs.org/) and
[Material](https://squidfunnel.github.io/mkdocs-material/); the API reference is
generated from the docstrings.

```bash
uv sync --group docs
uv run mkdocs serve          # live preview on http://127.0.0.1:8000
uv run mkdocs build --strict # static build in site/
```
