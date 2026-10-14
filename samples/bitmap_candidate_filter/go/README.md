# Portable Roaring64 candidate filter benchmark

This Go client creates a posting list of `resource_id` values outside Doris, sends it through the MySQL query interface as Base64, and queries `bitmap_contains(bitmap_from_roaring_base64(?), resource_id)`. The list can represent any candidate set, including a facet or an access list. It does not use a Doris table to build the bitmap.

The benchmark expects the unchanged 20-million-row `org_id = 101` fixture from [`bitmap_allowlist_scan.groovy`](../../../regression-test/suites/performance_p2/bitmap_allowlist_scan.groovy) or [`bitmap_allowlist_scan_iceberg.groovy`](../../../regression-test/suites/external_table_p2/iceberg/bitmap_allowlist_scan_iceberg.groovy). Those suites also create 10 million rows for `org_id = 202` with overlapping resource IDs. Run the relevant suite once to prepare the table, then start Doris and run this client:

```sh
cd samples/bitmap_candidate_filter/go
go run . -table regression_test_performance_p2.bitmap_allowlist_fact -sizes 250000,1000000,5000000,10000000
go run . -table bitmap_allowlist_benchmark.bitmap_allowlist_benchmark.bitmap_allowlist_fact -iceberg -sizes 250000,1000000,5000000,10000000
```

The default connection is `root@tcp(127.0.0.1:9030)/`. Set `DORIS_DSN` or `-dsn` for another host or credentials. The client enables `interpolateParams` so the encoded bitmap travels in the SQL request; it does not print the bitmap or DSN. The first query for each size and text shape warms the path, followed by five measured queries by default. SQL and query caches are disabled.

CSV output includes the portable bitmap and Base64 byte counts, client generation and encoding time, query round-trip mean/min/max, and their summed mean. Each result's count and sum are checked against the deterministic fixture. The `needle` shape matches about 1% of org 101; `other` matches about 99%. The query round trip includes client interpolation, transfer, Doris parsing and execution, and result transfer. It does not include opening the connection or obtaining a candidate set from another service.

The pinned Go Roaring library's `roaring64.Bitmap.WriteTo` emits its portable 64-bit format. Use `bitmap_from_roaring_base64` for these bytes; `bitmap_from_base64` accepts Doris's distinct `BitmapValue` serialization.
