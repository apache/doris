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

## Implementation order

1. **Filter before payload reads.** Recognize constant `bitmap_contains(bitmap_from_base64(...), resource_id)` as a scan predicate, decode its bitmap once per query or scanner, and evaluate it using the `BIGINT` column after `org_id` partition pruning and before materializing `payload`. Keep the ordinary expression as the fallback when a safe pushdown is unavailable. The first code path to inspect is scan-conjunct normalization in `be/src/exec/operator/scan_operator.cpp`, native scan predicate setup in `be/src/exec/operator/olap_scan_operator.cpp`, and `SegmentIterator` predicate and lazy-read phases in `be/src/storage/segment/segment_iterator.cpp`. A profile must demonstrate fewer payload bytes or lazy reads, not merely fewer rows emitted from the scan.
2. **Prototype indexed candidate generation.** For a native segment with an inverted index on `resource_id`, obtain physical row candidates whose indexed values belong to the allowlist, then intersect those candidates with the segment's existing row bitmap and visibility/delete filtering before payload reads. Extend the current index-predicate path in `SegmentIterator::_apply_inverted_index_on_column_predicate` and the numeric index reader only after measuring the cost of multiple term lookups or dictionary traversal. The allowlist's 250,000 to 10 million IDs can make per-ID index probes more expensive than a sequential column scan. Choose between index candidates and the column-only membership path using observed or estimated cost; keep a fallback when the index is absent or unsupported. Do not assume the bitmap's resource IDs are segment-local row positions.
3. **Deliver subquery-produced bitmaps to scans.** `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/rewrite/InApplyToJoin.java` currently expresses bitmap subqueries as semi-joins with `bitmap_contains`. Once constant scan pushdown works, add a way to build the subquery bitmap before dependent fact scans and pass it to them as a completed runtime filter, preserving distributed execution and memory limits. The old `TRuntimeFilterType.BITMAP` thrift value is marked deprecated; any protocol change needs the cross-branch compatibility review required by `gensrc/AGENTS.md`.

The index path should be conditional. In this run, plain `IN` was roughly three times faster than bitmap at 250,000 IDs; at 10 million IDs the bitmap was faster, but the index did not help. Start with early column membership, then prototype index candidate generation and retain it only where measured scan-byte and latency gains justify its lookup cost.

## Acceptance checks

- Keep the four allowlist sizes and compare count plus sum across indexed and plain bitmap and `IN` paths. Add empty, singleton, duplicate, absent-ID, and `BIGINT` IDs above `2^32` cases to the kernel tests.
- Test the selective indexed point lookup as a positive control, and test bitmap behavior with and without the index. Keep org partition pruning visible in `EXPLAIN`; the bitmap does not replace tenant isolation.
- Add BE unit tests for bitmap predicate creation, constant decoding, row membership, and fallback. Add regression coverage for subquery-produced bitmaps when that delivery path exists. Do not assert timing thresholds in regression tests.
- Record fact scan rows, payload bytes, lazy-read counts, index lookups, index candidate rows, and final result rows for each size. Require the new path to reduce bytes or time against both current bitmap and `IN` baselines before enabling it by default.
- Test Iceberg separately. A Doris native inverted index cannot prune Iceberg files; the relevant first step there is early `resource_id` filtering in the Parquet scan, followed by any metadata-based file or row-group skipping that the layout supports.
