# Iceberg REST view binding regression

`test_iceberg_view_query_p0` runs when `enableIcebergTest=true`. It reads the
persistent Spark views prepared by the Iceberg Docker fixture's `run14.sql`
through the REST catalog and preserves those shared views for repeated runs.
It uses the public Docker fixture credentials by default; the optional
`iceberg_view_access_key` and `iceberg_view_secret_key` settings override them.

`test_iceberg_rest_view_binding` requires the Iceberg Docker environment with
a view-capable REST catalog and a Spark Iceberg connection. It creates its own
namespaces, views, base tables, and local CTAS tables, and keeps them for debugging.

Enable it in a local regression configuration:

```groovy
enableIcebergViewTest = "true"
iceberg_view_access_key = System.getenv("ICEBERG_VIEW_ACCESS_KEY")
iceberg_view_secret_key = System.getenv("ICEBERG_VIEW_SECRET_KEY")
```

The usual `externalEnvIp`, `iceberg_rest_uri_port`, `iceberg_minio_port`, and Spark
connection settings must point to the same Docker environment. Credentials are
read from configuration; do not commit local configuration or credential values.
The optional `iceberg_view_catalog`, `iceberg_view_source_namespace`, and
`iceberg_view_store_namespace` settings isolate concurrent runs.

Run with the repository regression runner:

```bash
./run-regression-test.sh -d external_table_p0/iceberg -s test_iceberg_rest_view_binding
```

Coverage includes declared column names, repeated outputs, CTAS, nested views,
saved and empty namespaces, unused default catalogs, metadata refresh, and
predicates over partitioned and unpartitioned tables.
