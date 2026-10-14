# Bitmap allowlist scan benchmark: 2026-10-14

These are local, warm-cache measurements from one macOS arm64 Doris FE and BE, built in Release mode. The native table used local storage; the Iceberg table used a local REST catalog and MinIO. They are useful for comparing query shapes on this machine, not for predicting production latency or comparing directly with OpenSearch.

## Setup

- Fact data: 20 million rows for `org_id = 101` and 10 million for `org_id = 202`. Both orgs share the same `BIGINT` resource IDs, starting above `2^32`. A list partition on `org_id` allows the query to prune the other org.
- Allowlist sizes: 250,000; 1 million; 5 million; and 10 million IDs, sampled across the 20 million target-org IDs.
- Query: `COUNT(*)` and `SUM(resource_id)` with `org_id = 101` and `payload LIKE '%needle%'`. The allowlist is expressed either as `bitmap_contains(bitmap_from_base64(...), resource_id)` or as `resource_id IN (SELECT resource_id FROM bitmap_allowlist_ids WHERE first_size <= ...)`.
- The bitmap is built by `bitmap_to_base64(bitmap_agg(resource_id))` before timing. Each query shape is warmed once, then timed five times in the order bitmap, `IN`, `IN`, bitmap. Each cell below averages the two five-run means.
- SQL and query caches were disabled. The suites assert matching counts and resource-ID sums, and check that `org_id` selects one of two partitions.

## Results

Average query latency in milliseconds (lower is better):

| Allowlist IDs | Native bitmap | Native `IN` | Iceberg bitmap | Iceberg `IN` |
| ---: | ---: | ---: | ---: | ---: |
| 250,000 | 45.4 | 25.9 | 119.1 | 106.7 |
| 1,000,000 | 123.5 | 37.2 | 217.5 | 148.1 |
| 5,000,000 | 106.2 | 92.1 | 196.1 | 204.8 |
| 10,000,000 | 107.9 | 137.9 | 194.1 | 244.7 |

In this benchmark run, at 10 million IDs the bitmap query was about 22% faster than the `IN` query on native storage and 21% faster on Iceberg. At 250,000 and 1 million IDs, `IN` was faster. The non-monotonic bitmap timings mean these measurements should not be extrapolated into a general crossover point.

Both suites passed with zero failures or skips. The target-org text predicate matched 206,186 rows before the allowlist; the combined query returned 2,578 rows at 250,000 IDs and 103,093 at 10 million IDs. The bitmap and `IN` queries produced the same count and sum at every size.

## Reproduce

From the repo root with a local Doris FE/BE running:

```sh
./run-regression-test.sh --run -d performance_p2 -s bitmap_allowlist_scan
```

With the Iceberg test stack running and `enableIcebergTest=true` in the regression configuration:

```sh
./run-regression-test.sh --run -d external_table_p2/iceberg -s bitmap_allowlist_scan_iceberg
```

The source suites are [`bitmap_allowlist_scan.groovy`](bitmap_allowlist_scan.groovy) and [`bitmap_allowlist_scan_iceberg.groovy`](../external_table_p2/iceberg/bitmap_allowlist_scan_iceberg.groovy). The local run logs were written under `output/regression-test/log/` at 2026-10-14 06:36:50 and 06:37:48. Bitmap literals in those logs are large, so the logs are not checked in.

## Scan profile

I enabled `enable_profile=true` and `profile_level=2`, then ran the same bitmap and `IN` query shapes at 250,000 and 10 million IDs on both storage paths. The table lists **fact scan** counters, excluding the extra allowlist scan in the `IN` query. Every fact scan read all 20 million rows in the selected org's partition.

| Storage | IDs | Filter | Fact scan rows | Fact scan output rows | Fact scan bytes |
| --- | ---: | --- | ---: | ---: | ---: |
| Native | 250,000 | Bitmap | 20,000,000 | 2,578 | 1.67 GB |
| Native | 250,000 | `IN` | 20,000,000 | 2,584 | 1.67 GB |
| Native | 10,000,000 | Bitmap | 20,000,000 | 103,093 | 1.67 GB |
| Native | 10,000,000 | `IN` | 20,000,000 | 206,186 | 1.67 GB |
| Iceberg | 250,000 | Bitmap | 20,000,000 | 2,578 | 44.78 MB |
| Iceberg | 250,000 | `IN` | 20,000,000 | 2,584 | 45.08 MB |
| Iceberg | 10,000,000 | Bitmap | 20,000,000 | 103,093 | 44.78 MB |
| Iceberg | 10,000,000 | `IN` | 20,000,000 | 206,186 | 44.78 MB |

The `IN` scan can output a few rows that the later semi-join rejects; both final query results still match. The Iceberg plan reports `partition=1/2`, and its connector pushdown contains only `org_id = 101`. Bitmap membership runs as part of the Doris file-scan conjunct, after file rows are read. Its `RowsFilteredByConjunct` counter combines membership with the text predicate, so it cannot attribute rows to either predicate separately.

On native storage, the bitmap scan reported `RowsExprPredInput = 20,000,000`, `RowsExprPredFiltered = 19,997,422` at 250,000 IDs and `19,896,907` at 10 million IDs, and `LazyReadTime = 0ns` in both profiles. It read about 1.67 GB regardless of allowlist size. The 250,000-ID `IN` plan built runtime filters from the allowlist and reported `RowsShortCircuitPredFiltered = 19,749,488` and `LazyReadTime = 35.358ms` on the fact scan. Thus the current bitmap expression filters rows *within* the scan, but this profile does not show the desired early resource-ID pruning before `payload` is read. A dedicated scan predicate or runtime filter that can use the bitmap before payload materialization remains the performance work to investigate.

The profile capture was a separate, single execution per shape, not the timing sample above. Its wall times differed substantially from the benchmark means, especially for the 10-million-ID native `IN` path, so the latency advantage needs repeated measurement before relying on it. Raw profiles are local under `/private/tmp/doris_<storage>_<size>_<filter>.profile.txt`; they are not checked in because the Base64 SQL literal makes each bitmap profile large.

## Compressed bitmap subquery runtime filter: subsequent run

After building the opt-in compressed bitmap runtime filter, both the native and Iceberg suites passed with zero failures. This run adds `resource_id IN (SELECT bitmap_agg(resource_id) ...)` to the query shapes above. The default plan uses a compressed bitmap semi join; `enable_bitmap_subquery_runtime_filter=true` also sends that bitmap to the fact scan. Both plans produce the same count and resource-ID sum as the ordinary `IN (SELECT resource_id ...)` control at every allowlist size. The suites warm each shape and average five timed executions. All figures below are milliseconds from one local warm-data run; the native run overlapped a BE unit-test build and should not be compared directly with the earlier tables.

| Allowlist IDs | Native default | Native compressed filter | Native expanded hash join | Iceberg default | Iceberg compressed filter | Iceberg Base64 bitmap | Iceberg ordinary `IN` |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 250,000 | 114.0 | 61.4 | 34.6 | 189.2 | 166.2 | 126.0 | 116.6 |
| 1,000,000 | 127.8 | 86.6 | 57.6 | 184.8 | 206.2 | 219.9 | 145.6 |
| 5,000,000 | 155.0 | 99.4 | 177.4 | 210.0 | 190.4 | 198.8 | 197.1 |
| 10,000,000 | 170.4 | 109.6 | 343.6 | 203.4 | 196.4 | 195.8 | 247.3 |

The native compressed filter beat the native default compressed join at all four sizes in this run. On Iceberg it beat the default at 250,000, 5 million, and 10 million IDs, but was slower at 1 million. The Base64 and ordinary `IN` columns are separate query shapes; bitmap construction is outside their timed queries, while subquery bitmap construction is inside the default and compressed-filter timings. Their values are included as context, not as a controlled comparison of filter delivery alone. These short, single-host samples do not establish a stable latency crossover.

Separate Iceberg profiles at 250,000 and 10 million IDs both report `RawRowsRead = 20,000,000` for the selected-org fact scan with and without the compressed runtime filter. At 250,000 IDs, `RowsFilteredByConjunct` increases from 19,793,814 in the default plan to 19,997,422 with the filter, and the scan waits about 6.4 ms for it. At 10 million IDs, that counter increases to 19,896,907 and the wait is about 36.5 ms. The default scan's conjunct counter covers the text predicate; the compressed-filter counter combines text and bitmap membership. The filter therefore reaches the Iceberg file scan, but this profile shows no reduction in rows read from the files. It does not demonstrate early Iceberg file or row-group pruning.
