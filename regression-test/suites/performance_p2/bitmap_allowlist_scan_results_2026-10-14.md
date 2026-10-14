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

## Iceberg row-filter selectivity follow-up

The Iceberg suite now also compares a broad `payload LIKE '%other%'` predicate, which matches 19,793,814 of the 20 million target-org rows, against the existing `'%needle%'` predicate, which matches 206,186. Both use the same `resource_id IN (SELECT bitmap_agg(resource_id) ...)` query, include bitmap aggregation in the timed SQL, and check count plus resource-ID sum against ordinary `IN (SELECT resource_id ...)`. Each shape was warmed and timed five times in one local run. The values below are mean milliseconds, with the compressed filter disabled/enabled in separate runs:

| Allowlist IDs | Selective text: default | Selective text: scan filter | Broad text: default | Broad text: scan filter |
| ---: | ---: | ---: | ---: | ---: |
| 250,000 | 182.0 | 162.8 | 11,241.4 | 221.4 |
| 1,000,000 | 186.2 | 189.4 | 11,439.2 | 703.2 |
| 5,000,000 | 207.0 | 183.4 | 11,349.0 | 2,991.2 |
| 10,000,000 | 195.0 | 183.4 | 11,324.8 | 5,841.2 |

The broad-text result shows why scan delivery is valuable even without Iceberg file pruning: it keeps millions of rejected rows out of the default compressed bitmap join. The advantage narrows as the bitmap admits more rows. With selective text, the unfiltered scan already sends only about 206,000 rows to that join, so the filter has much less downstream work to save. The 1-million-ID selective case was effectively even in this run, after being slower with the filter in the previous run.

At 250,000, 1 million, and 10 million IDs, both variants read all 20 million target-org rows and all 18 Parquet row groups; `FileReadBytes` stayed at 44.87 MB for each profile. The default selective-text plan reported 19,793,814 `FilteredRowsByLazyRead`, while the compressed-filter plan reported zero and `ReaderReadRows = 40,000,000`: both the ID and text-payload predicate columns were read for all rows. The same zero lazy-read filtering appeared with broad text. Thus the current scan filter avoids downstream join work, but this run does **not** show earlier payload-column I/O or decode pruning. The Parquet scheduler's per-column lazy path requires selected-row-safe predicates; `LIKE` is not currently in that safe set, so evaluating this ordering is the next BE investigation. Filter-build, transfer, and memory costs across hosts remain unmeasured. A Base64 bitmap supplied from an external authorization service was not part of this local subquery benchmark.

## Iceberg selectivity rerun

The same suite passed again on 2026-10-14 without code changes. The first launch attempt could not connect to FE because its detached process had exited; after starting FE and BE in live sessions, the complete suite passed. Each query shape again had one warm-up and five timed executions. Mean times in milliseconds:

| Allowlist IDs | Selective text: default | Selective text: scan filter | Broad text: default | Broad text: scan filter |
| ---: | ---: | ---: | ---: | ---: |
| 250,000 | 182.2 | 156.0 | 11,121.4 | 220.2 |
| 1,000,000 | 179.8 | 191.4 | 11,225.0 | 674.4 |
| 5,000,000 | 203.2 | 186.6 | 11,242.6 | 2,967.0 |
| 10,000,000 | 196.2 | 189.8 | 11,276.6 | 5,855.4 |

The broad-text advantage persists across all four sizes. At 1 million IDs, the selective-text scan filter was 11.6 ms slower than the default join in this run (6.5%), while the broad-text scan filter was 16.6 times faster. This reinforces the need for a plan choice that accounts for other predicate selectivity instead of enabling the filter universally. The 1-million-ID profiles again read 20 million target-org rows and all 18 row groups with either plan; `FileReadBytes` was 44.82 MB in both. The filter plan reported zero `FilteredRowsByLazyRead` and 40 million `ReaderReadRows`, so the rerun does not change the earlier Parquet I/O conclusion.
