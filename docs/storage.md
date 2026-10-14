# Storage

## JSON registries

`statflows.storage.json` provides [`Loader`][statflows.storage.json.loader.Loader]
and [`Saver`][statflows.storage.json.saver.Saver], which read and write JSON
documents either on the local filesystem or, when `bucket=...` is given, on S3.

- the target path must end with `.json`;
- local writes are **atomic** (temporary file then rename);
- `Loader.load(..., missing_ok=True)` returns `None` for a missing object instead
  of raising — convenient for a first run — but still raises on a malformed file;
- `Saver.save(..., indent=..., ensure_ascii=...)` forwards its options to `json.dump`.

S3 access requires the `s3` extra. Credentials are read from the standard
`AWS_*` environment variables (`AWS_S3_ENDPOINT`, `AWS_ACCESS_KEY_ID`,
`AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`) or passed as keyword arguments;
see [`S3Connection`][statflows.storage._connection.S3Connection].

## DuckLake helpers

With the `ducklake` extra, `statflows.storage.ducklake.tables` provides
[`write_dataframe`][statflows.storage.ducklake.tables.write_dataframe]: it creates
the schema on the first call (returns `True`) and upserts by primary key on the
following ones (returns `False`), leaving rows that are not supplied untouched.
[`fact_table_exists`][statflows.storage.ducklake.tables.fact_table_exists] tells
whether a schema's fact table already exists.
[`write_dataframe_with_report`][statflows.storage.ducklake.tables.write_dataframe_with_report]
does the same write and also returns the `dt-ducklake-manager` operation report,
whose `rows_changed` tells an effective write from an upsert of unchanged values.

[`changes_since`][statflows.storage.ducklake.tables.changes_since] summarises the
rows inserted, updated and deleted in a table since a snapshot, with the
smallest `TIME_PERIOD` touched: see
[Conditioning downstream steps](downloads.md#conditioning-downstream-steps).

The schema layout (fact table, `metadata`, `dataset_metadata`) is the one of
[dt-ducklake-manager](https://qbolliet.github.io/dt-ducklake-manager/).
