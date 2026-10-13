# Bitmap allowlist index benchmark and scan plan: 2026-10-14

## Benchmark

[`bitmap_allowlist_index_scan.groovy`](bitmap_allowlist_index_scan.groovy) compares two native Doris tables with the same 30 million rows and layout. Both use `DUPLICATE KEY(org_id)`, list partitions for `org_id = 101` and `202`, and eight `resource_id` hash buckets. Only one table adds `INDEX idx_resource_id (resource_id) USING INVERTED`. Keeping `resource_id` out of the key isolates the inverted index from short-key pruning. The target org has 20 million rows; the other org has 10 million. Both share `BIGINT` resource IDs above `2^32`.

For each allowlist size, the suite checks that the bitmap and `IN (subquery)` queries return the same count and sum, both with `payload LIKE '%needle%'`. It asserts one of two org partitions is selected and verifies through a profile counter that an indexed point lookup prunes rows. Base64 construction is outside the timed queries. SQL and query caches are disabled; each shape is warmed and measured in two balanced sets of five executions.

Average query latency in milliseconds on one local macOS arm64 Release FE/BE, with warm data:

| Allowlist IDs | Plain bitmap | Plain `IN` | Indexed `IN` | Indexed bitmap |
| ---: | ---: | ---: | ---: | ---: |
| 250,000 | 80.5 | 25.0 | 25.9 | 82.5 |
| 1,000,000 | 143.6 | 39.2 | 39.4 | 145.2 |
| 5,000,000 | 105.9 | 91.0 | 93.9 | 107.0 |
| 10,000,000 | 108.0 | 131.0 | 134.7 | 108.7 |

The indexed point lookup used the inverted index, but bitmap membership and `IN` did not. Separate profiles of the indexed table at 250,000 and 10 million IDs showed `RowsInvertedIndexFiltered = 0` for both query shapes. Bitmap queries scanned all 20 million selected-org rows, read about 1.67 GB, and reported `LazyReadTime = 0ns`. The `IN` query used a runtime filter at 250,000 IDs but also scanned all 20 million fact rows. These profiles were separate single executions, so their wall times are not part of the averages above. The index alone therefore gives no measurable improvement to these allowlist queries in the current path.

Reproduce with a running local FE/BE:

```sh
./run-regression-test.sh --run -d performance_p2 -s bitmap_allowlist_index_scan
```

The 2026-10-14 run passed: one suite, zero failures and zero skips. Its log is under `output/regression-test/log/`; raw profiles were kept outside the repository because the embedded Base64 literals make them large. These numbers compare query shapes on this machine, not production latency.

## Early native scan result

The BE now recognizes a constant `bitmap_contains(bitmap, BIGINT resource_id)` at segment initialization. It reads candidate resource IDs after partition and ordinary index pruning, tests bitmap membership, and narrows the segment row bitmap before later scan expressions. A full 4,096-row sample switches back to the normal vectorized scan when more than 2% match. Empty and null bitmaps eliminate the segment immediately. The original expression still runs on surviving rows.

The same suite passed after this change: one suite, zero failures and zero skips. Average query latency in milliseconds on the same local macOS arm64 Release FE/BE, with warm data:

| Allowlist IDs | Plain bitmap | Plain `IN` | Indexed `IN` | Indexed bitmap |
| ---: | ---: | ---: | ---: | ---: |
| 250,000 | 57.8 | 23.2 | 23.5 | 58.6 |
| 1,000,000 | 129.8 | 35.6 | 34.6 | 131.7 |
| 5,000,000 | 94.8 | 90.4 | 90.7 | 93.1 |
| 10,000,000 | 94.6 | 128.7 | 130.5 | 95.6 |

At 250,000 IDs, the indexed bitmap query is about 29% faster than the earlier 82.5 ms baseline. Its profile reports `RowsBitmapAllowlistFiltered = 19,750,000` and only 250,000 rows entering later expression evaluation. It still reports `RowsInvertedIndexFiltered = 0` and reads 1.82 GB of uncompressed data, compared with 1.67 GB in the original bitmap scan: uniformly scattered matching IDs touch most column pages, and the prepass reads the ID column again. At 10 million IDs the sample abandons the prepass: `RowsBitmapAllowlistFiltered = 0`, `RowsExprPredInput = 20,000,000`, and 1.67 GB is read. Timings vary with cache state, so the table supports a sparse-list latency improvement in this setup, not a general performance guarantee. The `IN` path remains much faster for 250,000 and 1 million IDs; it uses a different runtime-filter plan.

## Indexed BKD prototype: no-go

An experimental reader traversed the native BIGINT BKD index once, checked each indexed value against the roaring allowlist, and intersected the resulting physical row IDs with the segment candidates. The same regression suite passed with the prototype, including an assertion that the indexed bitmap query filtered rows through the index. At 250,000 IDs, the warm-data averages were 57.0 ms for the plain bitmap query, **100.8 ms for the indexed bitmap query**, and 22.3 ms for indexed `IN (subquery)`. The other sizes used the dense-list fallback and stayed close to the prior run. This prototype was removed from the query path after measurement.

The 250,000-ID indexed profile reported `RowsInvertedIndexFiltered = 19,750,000`, 91.95 MB of index reads, 1.67 GB of scan reads, and 1.04 seconds of summed bitmap/index traversal time across scanners. The column prepass profile had about 289 ms of summed filter time; the index saved an ID-column reread but did not avoid scattered payload pages. This is evidence against a full BKD traversal for these uniformly distributed IDs, not against every possible index design. A different postings layout or resource-ID clustering needs its own benchmark before it is considered for the default path.

The full-index traversal remains removed. A small constant bitmap now uses BKD point lookups when an inverted index is available, with a limit of 16 values; larger bitmaps retain the column prepass. The focused regression suite passes and checks that a two-ID bitmap query reports inverted-index filtering. This path is intended for selective point lists, not the 250,000 to 10 million ID benchmark cases.

Subquery-produced bitmaps normally become a semi-join with `bitmap_contains`; its nested-loop plan does not push a runtime filter into the fact scan. An opt-in `enable_bitmap_subquery_hash_join` rewrite expands the bitmap into IDs and uses a hash semi join, which produces the existing scan runtime filter. The regression suite checks the default and opt-in plans, runtime-filter presence, matching rows, empty bitmaps, nullable resource IDs, and IDs above `2^32`. This is a functional baseline for scan delivery, not a measured win for large bitmaps: expanding millions of IDs can consume substantial memory and should remain opt-in until benchmarked. The exact hash join remains the final membership check.

`bitmap_count` or the bitmap object's cardinality can guide a **runtime** choice after the subquery has produced the bitmap. The FE cannot know that cardinality while planning a data-dependent subquery. An adaptive path would keep the bitmap in compressed form, select point lookups only for very small cardinality, use a scan bitmap filter when selective, and leave dense lists to the normal join/scan path. That requires a distributed runtime-filter representation and memory limits; it is not implemented by the opt-in rewrite.

## Implementation order

1. **Filter before later expressions.** Implemented for constant BIGINT bitmap predicates in native `SegmentIterator`, with a dense-list sample fallback. The 250,000-ID case evaluates later expressions on fewer rows, but scattered matches do not reduce bytes read. The bitmap is decoded by the existing constant expression machinery per segment; sharing it across segments is a possible further improvement.
2. **Prototype indexed candidate generation.** A full BIGINT BKD traversal was tested and removed after it lost to the column prepass at 250,000 IDs. BKD equality point probes are implemented for constant bitmaps with at most 16 IDs. Any larger-list index design should avoid scanning every indexed value, demonstrate a lower index traversal cost, and retain the column-only fallback. Do not assume the bitmap's resource IDs are segment-local row positions.
3. **Deliver subquery-produced bitmaps to scans.** `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/rewrite/InApplyToJoin.java` now has an opt-in expansion to a hash semi join, using existing runtime filters. It does not deliver a compressed bitmap to the scan. A future adaptive design must build the subquery bitmap before dependent fact scans, inspect its runtime cardinality, and preserve distributed execution and memory limits. The old `TRuntimeFilterType.BITMAP` thrift value is marked deprecated; any protocol change needs the cross-branch compatibility review required by `gensrc/AGENTS.md`.

The column prepass remains the default sparse-bitmap path above 16 IDs. A future compressed runtime filter, postings-based index, or clustered data layout must beat both that path and the `IN (subquery)` control before becoming the default.

## Acceptance checks

- Keep the four allowlist sizes and compare count plus sum across indexed and plain bitmap and `IN` paths. Add empty, singleton, duplicate, absent-ID, and `BIGINT` IDs above `2^32` cases to the kernel tests.
- Test the selective indexed point lookup as a positive control, and test bitmap behavior with and without the index. Keep org partition pruning visible in `EXPLAIN`; the bitmap does not replace tenant isolation.
- Add BE unit tests for bitmap predicate creation, constant decoding, row membership, and fallback. Add regression coverage for subquery-produced bitmaps when that delivery path exists. Do not assert timing thresholds in regression tests.
- Record fact scan rows, payload bytes, lazy-read counts, index lookups, index candidate rows, and final result rows for each size. Require an indexed path to reduce bytes or time against both current bitmap and `IN` baselines before enabling that path by default.
- Test Iceberg separately. A Doris native inverted index cannot prune Iceberg files; the relevant first step there is early `resource_id` filtering in the Parquet scan, followed by any metadata-based file or row-group skipping that the layout supports.
