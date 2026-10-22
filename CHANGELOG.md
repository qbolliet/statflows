# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0](https://github.com/qbolliet/statflows/compare/v0.3.1...v0.4.0) (2026-10-22)


### Features

* caching rule for providers ([f02447d](https://github.com/qbolliet/statflows/commit/f02447db5d9c9750d98a936da4594451773a39f3))

## [0.3.1](https://github.com/qbolliet/statflows/compare/v0.3.0...v0.3.1) (2026-10-17)


### Bug Fixes

* **parsing:** keep dimension codes verbatim (NA = Namibia) ([515c558](https://github.com/qbolliet/statflows/commit/515c558cb46e98e5c9e1a500d5513c5a5d14b48b))

## [0.3.0](https://github.com/qbolliet/statflows/compare/v0.2.2...v0.3.0) (2026-10-15)


### Features

* **ducklake:** touched keys and pinned upper bound in changes_since. ([eb7dbed](https://github.com/qbolliet/statflows/commit/eb7dbed3751c603f44ce1398b9cdf7897bd3b573))
* **ducklake:** touched keys and pinned upper bound in changes_since. ([cd5a432](https://github.com/qbolliet/statflows/commit/cd5a4328f4a6cf2f4136c671332e1699a055c02f))


### Documentation

* fix mkdocs build ([88af233](https://github.com/qbolliet/statflows/commit/88af2339b3e4aec959e86408a43c5605faaa55f0))

## [0.2.2](https://github.com/qbolliet/statflows/compare/v0.2.1...v0.2.2) (2026-10-14)


### Bug Fixes

* track data modifications to condition downstream steps ([8d65a17](https://github.com/qbolliet/statflows/commit/8d65a17b57bf16eacf65ac1a99ac0e4a7fcc0c70))
* track data modifications to condition downstream steps ([129f69b](https://github.com/qbolliet/statflows/commit/129f69bc438bada890f3659585ccd093cf59b0e8))


### Documentation

* download documentation ([f980bcb](https://github.com/qbolliet/statflows/commit/f980bcb23ccd736c94f28a6cb263f11ea36fc342))

## [0.2.1](https://github.com/qbolliet/statflows/compare/v0.2.0...v0.2.1) (2026-10-12)


### Bug Fixes

* comtrade primary_keys with label descriptions ([957688d](https://github.com/qbolliet/statflows/commit/957688dfe34b7fa3d45b649ac27d95ab804800eb))

## [0.2.0](https://github.com/qbolliet/statflows/compare/v0.1.1...v0.2.0) (2026-10-11)


### ⚠ BREAKING CHANGES

* **deps:** require pandas >= 3.0

### Features

* **core:** add ClientFactory and extend build_client to all providers ([f3957a3](https://github.com/qbolliet/statflows/commit/f3957a3ed6b918d5053c0e7aef1bfe17ab7cbf41))


### Bug Fixes

* **comtrade:** aggregate duplicate tariffline rows per column kind ([173d0da](https://github.com/qbolliet/statflows/commit/173d0da170752baae71f958246eddbc08a1a867b))
* **core:** count only written rows in rows_written of write batches ([6cfdf86](https://github.com/qbolliet/statflows/commit/6cfdf86737904f50275c38d576c84faabdf03a40))
* minor aggregation + parsing bugs ([02c3c2f](https://github.com/qbolliet/statflows/commit/02c3c2f1448450c367ccdf9d5a706d77f1bcec70))


### Documentation

* implementation specification ([801b8be](https://github.com/qbolliet/statflows/commit/801b8be7a83c4d63d57ff03068bdc93b16332d15))


### Miscellaneous Chores

* **deps:** require pandas &gt;= 3.0 ([df71dc5](https://github.com/qbolliet/statflows/commit/df71dc56f74962cf6dd80f8633ff89211a1b611b))

## [0.1.1](https://github.com/qbolliet/statflows/compare/v0.1.0...v0.1.1) (2026-10-02)


### Documentation

* strcuture + miscellanous ([fd51d2f](https://github.com/qbolliet/statflows/commit/fd51d2f7a562077b7b2290146b2eef9849bb44bc))
* translate docstrings to english + init mkdocs ([6c91c6b](https://github.com/qbolliet/statflows/commit/6c91c6b8795e42eb50de3138a5c2c1b38f97784c))

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
