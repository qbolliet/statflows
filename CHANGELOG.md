# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-10-01

Buffering of the orchestrator's registry and DuckLake writes (PS-27, finding
C-08). All default values reproduce the previous behaviour.

### Added

- Initial version: Eurostat, OECD, Comtrade and UNSD clients; incremental
  orchestrator into DuckLake; local or S3 JSON registries.
- `SDMXDownloader` / `download_updates`: `registry_flush_every`,
  `registry_flush_seconds` (buffered registry), `registry_shard_key` (sharded
  registry, transparent migration of the single file), `write_batch_rows`,
  `write_batch_queries` (DuckLake writes in batches, per schema, deduplicated
  on the primary key), `update_options` / `build_options` (options forwarded to
  `DatabaseUpdater.update_database` / `DuckLakeTablesBuilder.build_schema`),
  `ducklake_options` (including `data_inlining_row_limit`) and `run_id`.
  Invariant: a registry entry never advances
  before the data of its query has been written successfully.
- Clean shutdown on `SIGTERM` during `run()` (handler installed in the main
  thread, restored on exit): pending batches and registry are persisted,
  `stopped_early=True`.
- `DownloadReport`: `n_registry_flushes`, `n_write_batches`,
  `rows_pending_at_stop` (included in `to_metrics()`).
- `statflows.core.registry`: `RegistryEntry` (frozen dataclass) and
  `iter_registry_entries(last_download_path, bucket=None, storage_options=None)`,
  format-independent reading (single file or shards), exported from
  `statflows`, `statflows.core` and `statflows.core.download`.
- `write_dataframe(..., update_options=None, build_options=None, run_id=None,
  commit_message=None)`. The compaction of `update_database` follows the library
  default (`update_options={"compact_after_update": False}` to disable it).
- `Loader.list_json(directory, bucket=None)`: JSON files of a local directory
  or an S3 prefix.
- Codelists with labels: `statflows.core.factory.codelist_frame`,
  `EurostatClient.get_codelist`, `ComtradeClient.get_codelist` (columns
  `code`, `label`, and `parent` when available), `parse_codelist_response(...,
  with_parent=False)` and `comtrade.parsing.build_codelist`.

### Changed

- `ComtradeClient.get_metadata` caches the references registry and each
  category (`refresh=True` parameter to force a reload): query building and
  codelists share the same calls.
- `Saver`: the temporary file of the local atomic write is prefixed `.tmp-`
  (ignored by `Loader.list_json`).
- A failed periodic registry persistence is logged and retried at the next one,
  instead of failing the current query (the data had already been written).
- `DownloadReport.queries` follows the completion order of the queries
  (identical to the processing order when writes are not buffered).
- Dependency `dt-ducklake-manager>=0.4.0` (`ducklake` and `all` extras).
