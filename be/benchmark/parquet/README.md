# Parquet reader microbenchmarks

These benchmarks separate native page decoding from the complete local-file reader path. They use
deterministic data and verify the physical encoding recorded in each generated Parquet footer before
running a measurement.

Agents and maintainers must read [AGENTS.md](AGENTS.md) for the exact timing boundaries, synthetic
data rules, result interpretation, current coverage limitations, and prioritized follow-up work.

## Build

Build the Release benchmark binary from the repository root:

```shell
./build.sh --benchmark -j128
```

List only the Parquet cases:

```shell
be/output/lib/benchmark_test --benchmark_list_tests | grep '^Parquet'
```

List the split-local runtime-filter expression lifecycle cases:

```shell
be/output/lib/benchmark_test --benchmark_list_tests | grep '^FileScannerExpr/'
```

## Decoder cases

`ParquetDecoder` measures the native decoder with data generation and encoder setup outside the
timed region. It covers PLAIN, dictionary, byte-stream-split, and DELTA encodings across their
supported fixed-width and binary physical types. Sparse selections are provided as both one
clustered range and many alternating ranges.

The decoder selection axis includes 0%, 1%, 10%, 50%, 90%, and 100% so boundary and
high-selectivity behavior are visible.

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetDecoder/plain/int64/sel_10/alternating$' \
  --benchmark_min_time=0.1s
```

## SIMD kernel cases

`ParquetKernel` isolates seven decode and selection stages from reader setup and virtual consumer
overhead: byte-stream-split transpose, delta prefix sum, numeric dictionary gather, nullable
expansion, nullable selection planning, raw predicate evaluation, and repeated-level sparse
selection. It covers the applicable
4-byte and 8-byte integer and floating-point physical types, raw-predicate selectivities from 0%
through 100%, and nullable rates from 0% through 90% with clustered and alternating placement.
Nested selection covers 1%, 10%, and 50% surviving parent rows with both placement patterns.
Each nested-selection scenario registers both `impl_legacy` and `impl_fused`; both paths use the
same source levels and are checked against an independent oracle before timing.
Nullable selection planning registers legacy and fused pairs across five selectivities, five null
rates, and independent clustered or alternating selection/null placement. Both implementations are
checked for identical physical ranges and null maps before timing. The full matrix also acts as a
negative control: production fusion is limited to batches with at least 1,024 rows, at least 10%
NULLs, and fragmented definition-level runs; no-NULL, low-NULL, and clustered pages retain the
legacy planner.
Dictionary gather uses 32-, 4,096-, and 262,144-entry working sets to separate cache-resident and
cache-miss-dominated behavior.

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetKernel/(dictionary_gather|nullable_expand)/' \
  --benchmark_min_time=0.1s
```

For a reproducible nested-selection comparison, build once and run the two implementations from
that same binary in ABBA order. Pin every command to the same otherwise-idle CPU:

```shell
taskset -c 8 be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetKernel/nested_selection/.*/impl_legacy$' \
  --benchmark_min_time=1s --benchmark_repetitions=10 \
  --benchmark_report_aggregates_only=true \
  --benchmark_out=nested-legacy-a1.json --benchmark_out_format=json

taskset -c 8 be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetKernel/nested_selection/.*/impl_fused$' \
  --benchmark_min_time=1s --benchmark_repetitions=10 \
  --benchmark_report_aggregates_only=true \
  --benchmark_out=nested-fused-b1.json --benchmark_out_format=json

# Repeat fused as B2, then legacy as A2, changing only --benchmark_out.
```

## Selection compaction cases

`ParquetSelection` isolates the selection-vector paths used after raw and expression predicate
evaluation. It covers implicit identity initialization, a filter indexed by source row, and a
second compact filter applied after an earlier predicate has already made the selection sparse.

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetSelection/(resize_identity|row_filter|cascade_filter)/' \
  --benchmark_min_time=1s \
  --benchmark_repetitions=10 \
  --benchmark_report_aggregates_only=true
```

## Local reader cases

`ParquetReader` measures local open-to-first-block, full scan, predicate scan, complex residual
scan, paired multi-column OR and DNF execution, and LIMIT-shaped reads. The matrix covers:

- PLAIN, dictionary, byte-stream-split, and DELTA binary-packed files;
- NULL ratios of 0%, 1%, 10%, 50%, and 90%, with clustered and alternating placement;
- predicate selectivities of 0%, 1%, 10%, 50%, 90%, and 100%;
- predicate-only and predicate-plus-lazy-projected reads;
- ordered complex residuals whose later columns are reachable only after an earlier residual;
- schemas with 4, 32, 128, and 512 columns, with the predicate first or last.

The DECIMAL(10,2) multi-column OR cases compare the legacy residual expression with raw
disjunction filtering for PLAIN and dictionary files. They sweep selectivity, NULL density, and
whether the first OR column is projected:

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetReader/multi_column_or_scan/' \
  --benchmark_min_time=1s \
  --benchmark_repetitions=10 \
  --benchmark_report_aggregates_only=true
```

The INT32 multi-column DNF cases model three `(category AND bound)` branches over two columns.
They pair the legacy residual expression with exact decoder-produced branch masks for PLAIN and
dictionary files. A projected-predicate pair verifies the conservative fallback:

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetReader/multi_column_dnf_scan/' \
  --benchmark_min_time=1s \
  --benchmark_repetitions=10 \
  --benchmark_report_aggregates_only=true
```

Fixtures are created lazily under the system temporary directory in
`doris_parquet_reader_benchmark`. Generation, footer validation, and reader setup are excluded from
steady-state scan timings. `open_to_first_block` intentionally includes reader initialization,
footer loading, open, and the first `get_block` call.

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetReader/predicate_scan/plain/null_50/alternating/sel_10/' \
  --benchmark_min_time=0.1s \
  --benchmark_out=parquet-reader.json \
  --benchmark_out_format=json
```

The complex-residual case uses a production compound `AND` tree. Its first child,
`c0 < selectivity_percent`, preserves the requested selectivity; its second child, `c2 = c3`,
references two new columns and accepts every row that reaches it:

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetReader/complex_residual_scan/plain/null_10/alternating/sel_10/' \
  --benchmark_min_time=1s
```

Every result reports throughput plus `raw_rows`, `selected_rows`, `fixture_bytes`, `ns/raw_row`,
and (when at least one row survives) `ns/selected_row`. Keep CPU frequency, build type, compiler,
machine placement, and benchmark filters fixed when comparing two commits.

## Runtime-filter expression lifecycle cases

`FileScannerExpr` measures only the repeated deep-clone, prepare, and open work for an already
prepared direct-IN runtime filter. Four cardinalities sweep 128 through 65,536 set values, with
shared-state and forced-rematerialization implementations registered in the same binary. Set
construction and the original fragment-level prepare/open are outside the timed region.

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^FileScannerExpr/direct_in_clone_prepare_open/' \
  --benchmark_min_time=1s \
  --benchmark_repetitions=10 \
  --benchmark_report_aggregates_only=true
```

These cases do not execute `FileScannerV2`, schedule splits, or read Parquet files. They isolate the
expression lifecycle visible in scanner profiles so it can be compared without I/O noise.
