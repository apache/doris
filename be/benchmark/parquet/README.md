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

`ParquetKernel` isolates the five SIMD-sensitive stages from reader setup and virtual consumer
overhead: byte-stream-split transpose, delta prefix sum, numeric dictionary gather, nullable
expansion, and raw predicate evaluation. It covers the applicable 4-byte and 8-byte integer and
floating-point physical types, raw-predicate selectivities from 0% through 100%, and nullable
rates from 0% through 90% with clustered and alternating placement. Dictionary gather uses 32-,
4,096-, and 262,144-entry working sets to separate cache-resident and cache-miss-dominated
behavior.

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetKernel/(dictionary_gather|nullable_expand)/' \
  --benchmark_min_time=0.1s
```

## Local reader cases

`ParquetReader` measures local open-to-first-block, full scan, predicate scan, complex residual
scan, and LIMIT-shaped reads. The matrix covers:

- PLAIN, dictionary, byte-stream-split, and DELTA binary-packed files;
- NULL ratios of 0%, 1%, 10%, 50%, and 90%, with clustered and alternating placement;
- predicate selectivities of 0%, 1%, 10%, 50%, 90%, and 100%;
- predicate-only and predicate-plus-lazy-projected reads;
- ordered complex residuals whose later columns are reachable only after an earlier residual;
- schemas with 4, 32, 128, and 512 columns, with the predicate first or last.

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
