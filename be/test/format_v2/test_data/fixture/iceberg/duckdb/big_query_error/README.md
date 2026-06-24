This fixture is copied from `duckdb/duckdb-iceberg:data/persistent/big_query_error`.

Upstream references:

- https://github.com/duckdb/duckdb-iceberg/tree/v1.5-variegata/data/persistent/big_query_error
- https://github.com/duckdb/duckdb-iceberg/blob/v1.5-variegata/data/persistent/big_query_error/rewrite_manifests.test

The metadata files are kept with the Parquet data file to preserve the original Iceberg table
context. Doris BE fixture tests derive table schema and field ids from the metadata JSON, then read
the Parquet data file through a FE-like split descriptor because Iceberg metadata planning is
performed before the BE table-reader layer.
