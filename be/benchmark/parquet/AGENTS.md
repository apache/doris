# Parquet microbenchmark guide for agents

This file applies to `be/benchmark/parquet/`. Read it before changing, running, or interpreting
these benchmarks. The current suite is a local benchmark foundation, not the complete Parquet
benchmark system described in the design document.

## What exists today

The benchmark binary registers three groups:

- `ParquetDecoder`: native page decoder benchmarks using in-memory encoded pages.
- `ParquetKernel`: isolated SIMD-sensitive decode and predicate kernels.
- `ParquetReader`: local-file benchmarks that call the format V2 Parquet reader directly.

The relevant files are:

- `benchmark_parquet_decoder.hpp`: deterministic page construction and decoder registration.
- `benchmark_parquet_reader.hpp`: deterministic local Parquet fixtures and reader registration.
- `parquet_benchmark_scenarios.h`: scenario definitions and the selected matrix.
- `README.md`: short human-oriented build and invocation examples.
- `be/test/format_v2/parquet/parquet_benchmark_scenarios_test.cpp`: matrix invariants.

Do not describe this suite as end-to-end SQL, `FileScannerV2`, remote I/O, V1/V2 comparison, or a
cross-engine benchmark. The reader benchmark starts at `format::parquet::ParquetReader` and does
not include FE planning, scanner scheduling, `TableReader`, client latency, or Runtime Profile
collection.

## Build and list cases

Performance results must come from a Release build:

```shell
./build.sh --benchmark -j128
```

List all Parquet cases and verify the expected registration counts:

```shell
be/output/lib/benchmark_test --benchmark_list_tests \
  | grep -E '^Parquet(Decoder|Kernel|Reader)/'

be/output/lib/benchmark_test --benchmark_list_tests \
  | grep -c '^ParquetDecoder/'  # currently 228

be/output/lib/benchmark_test --benchmark_list_tests \
  | grep -c '^ParquetKernel/'   # currently 80

be/output/lib/benchmark_test --benchmark_list_tests \
  | grep -c '^ParquetReader/'   # currently 152
```

When running the binary directly from `be/build_RELEASE/bin`, make sure the JVM and third-party
libraries are discoverable. Prefer the installed `be/output/lib/benchmark_test` produced by
`build.sh` because the normal Doris environment already configures its dependencies.

## Run smoke verification

A smoke run proves that every registered case initializes and completes. It does not produce a
stable performance baseline:

```shell
be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetDecoder/' \
  --benchmark_min_time=0.001s \
  --benchmark_out=parquet-decoder-smoke.json \
  --benchmark_out_format=json

be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetKernel/' \
  --benchmark_min_time=0.001s \
  --benchmark_out=parquet-kernel-smoke.json \
  --benchmark_out_format=json

be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetReader/' \
  --benchmark_min_time=0.001s \
  --benchmark_out=parquet-reader-smoke.json \
  --benchmark_out_format=json
```

Reject a smoke run if the process is non-zero, the expected number of JSON results is absent, or
any result contains `error_occurred`. Also run the scenario matrix unit test after changing the
matrix. Never use the 1 ms smoke measurements to claim a speedup or regression.

## Run a performance comparison

Compare the same named cases on the same host, compiler, build flags, CPU set, NUMA node, and power
policy. Use the same fixture directory and cache state for both revisions. A suitable local command
is:

```shell
taskset -c 8 be/output/lib/benchmark_test \
  --benchmark_filter='^ParquetReader/predicate_scan/plain/null_50/alternating/sel_10/' \
  --benchmark_min_time=1s \
  --benchmark_repetitions=10 \
  --benchmark_report_aggregates_only=true \
  --benchmark_out=parquet-reader.json \
  --benchmark_out_format=json
```

Use at least three untimed warmups before collecting local warm-cache results. Run revisions in an
ABBA order when comparing two commits. Do not compare results from different CPUs, cache
topologies, compiler versions, or build types.

The suite does not currently control the OS page cache. Repeated reader cases are normally
warm-cache measurements. Do not label them cold-NVMe results. Do not clear a shared machine's page
cache to manufacture a cold run.

## Current scenario matrix

`ParquetDecoder` contains 19 encoding/type pairs. Each pair is run at 0%, 1%, 10%, 50%, 90%, and
100% selection with clustered and alternating selection ranges, for 228 registered cases.

| Encoding | Physical types |
|---|---|
| PLAIN | INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY |
| Dictionary | INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY |
| BYTE_STREAM_SPLIT | FLOAT, DOUBLE, FIXED_LEN_BYTE_ARRAY |
| DELTA_BINARY_PACKED | INT32, INT64 |
| DELTA_LENGTH_BYTE_ARRAY | BYTE_ARRAY |
| DELTA_BYTE_ARRAY | BYTE_ARRAY |

`ParquetKernel` contains 80 cases across five SIMD-sensitive stages: BYTE_STREAM_SPLIT,
DELTA_PREFIX_SUM, DICTIONARY_GATHER, NULLABLE_EXPAND, and RAW_PREDICATE. It covers the applicable
four- and eight-byte types, three dictionary working-set sizes, 0% through 90% null rates with both
placement patterns, and 0% through 100% raw-predicate selectivities.

`ParquetReader` deliberately uses a single-variable matrix rather than a Cartesian product. After
deduplication it contains 152 cases covering:

- operations: open-to-first-block, full scan, predicate scan, complex residual scan, limit 1, and
  limit 1000;
- file encodings: PLAIN, dictionary, BYTE_STREAM_SPLIT, and DELTA_BINARY_PACKED;
- null ratios: 0%, 1%, 10%, 50%, and 90%;
- null shapes: clustered and alternating;
- predicate selectivities: 0%, 1%, 10%, 50%, 90%, and 100%;
- projection shapes: predicate-only and predicate plus one lazy payload column;
- schema widths: 4, 32, 128, and 512 columns;
- predicate position: first or last column.

Except for the axis being varied, reader cases inherit the baseline: nullable INT32, PLAIN,
alternating 10% nulls, 10% selectivity, 32 columns, predicate at column zero, and predicate plus
payload projection.

## How decoder data is generated

Decoder pages are constructed in memory before the timed loop. There is no Parquet file, Python
generator, random seed, or manifest involved.

- Every page contains 65,536 logical values.
- Integer values use `(row * 17) % 1000003`.
- Floating-point values use `(row % 1009) * 0.25 - 100.0`.
- Binary values are fixed at 16 bytes. Their first bytes contain `row % 1009` and the remaining
  bytes are deterministic filler.
- PLAIN fixed-width values are copied in physical byte order. PLAIN BYTE_ARRAY values use a
  four-byte length followed by payload bytes.
- BYTE_STREAM_SPLIT pages transpose the byte lanes of the fixed-width PLAIN representation.
- DELTA pages are produced with the Arrow Parquet DELTA encoders.
- Dictionary pages contain 256 deterministic entries. IDs repeat from 0 through 255 and are
  encoded with an eight-bit RLE/bit-packed stream.
- Clustered selection is one continuous range. Alternating selection spreads selected rows evenly
  and intentionally produces many one-row physical ranges.

Page generation, selection construction, decoder creation, dictionary setup, and `set_data` are
outside the timed decode call. Before timing, every decoder case verifies the consumed value count
and a checksum of all selected values against the deterministic source generator. The timed sinks
then consume decoder callbacks and prevent compiler removal, but they do not build a Doris
`Column`. Consequently these cases isolate decoder traversal and selection cost; they do not
measure definition-level decoding, nullable reconstruction, type conversion, or full column
materialization.

## How reader Parquet files are generated

Reader fixtures are generated lazily by C++ code using Arrow builders and the Arrow Parquet writer.
They are stored under:

```text
${TMPDIR:-/tmp}/doris_parquet_reader_benchmark/
```

Fixture contents and writer settings are:

- 16,384 rows and 4 row groups of 4,096 rows each;
- all columns are nullable INT32 and reuse the same deterministic Arrow array;
- every non-null value is `row % 100`;
- the predicate is `value < selectivity_percent`, so the threshold maps directly to the intended
  non-null selectivity;
- the complex residual scan evaluates a production expression tree whose first child is
  `c0 < selectivity_percent` and whose always-true second child is `c2 = c3`, exposing whether
  later-only columns are decoded eagerly;
- alternating nulls use a 101-row period and `(row * 37) % 101`, avoiding direct correlation with
  the 100-value predicate period;
- clustered nulls use contiguous null prefixes inside each 1,024-row cluster;
- Parquet format version 2.6, DataPage V2, no compression, and statistics disabled;
- dictionary is explicitly enabled only for dictionary fixtures; other fixtures disable dictionary
  and request their named encoding;
- after writing, every column chunk in every row group is checked through footer metadata to ensure
  that the writer did not silently choose another encoding.

The fixture name depends only on axes that change file contents. Therefore predicate thresholds,
projection mode, and operation can reuse the same file. Existing fixtures are footer-validated
before reuse. Delete only the dedicated `doris_parquet_reader_benchmark` temporary directory when
fixture rules change; never remove a broad temporary or workspace directory.

Fixture creation and footer verification happen before benchmark iterations. Reader initialization
and `close()` are excluded from steady-state full/predicate/LIMIT timing. The
`open_to_first_block` case intentionally includes reader construction, metadata loading, `open()`,
and the first `get_block()` call.

## Interpret the result correctly

Google Benchmark JSON contains `real_time`, `cpu_time`, repetitions, and custom counters. Use:

- `cpu_time` for decoder CPU efficiency;
- both `real_time` and `cpu_time` for reader cases, because latency and CPU cost answer different
  questions;
- median across repetitions as the primary result;
- p95 and a confidence interval for automated gates; never select the fastest repetition.

Custom counters mean:

- `raw_rows`: logical source rows considered by the case;
- `selected_rows`: rows returned after the predicate or selection;
- `ns/raw_row`: normalized source-row cost and the primary comparison across selectivities;
- `ns/selected_row`: cost per surviving row; it naturally rises as selectivity falls;
- `selection_ranges`: decoder physical ranges; compare alternating with clustered at the same
  selectivity to expose per-range overhead;
- `fixture_bytes` or `encoded_bytes`: fixture/page size, not peak memory;
- `items_per_second`: selected rows per second, so it is not directly comparable across different
  selectivities;
- `bytes_per_second`: a logical benchmark counter. In reader cases it is estimated from projected
  INT32 values, not measured storage-device bytes or compressed bytes.

Useful comparisons are:

- clustered versus alternating at equal selectivity: sparse-range/cursor overhead;
- null 0% versus 1/10/50/90% at equal selection: definition-level and nullable reconstruction
  overhead in the reader;
- predicate-only versus predicate-projected: lazy materialization benefit;
- width 4/32/128/512 with predicate first/last: schema and metadata traversal overhead;
- PLAIN versus dictionary/BYTE_STREAM_SPLIT/DELTA at the same reader baseline: encoding path cost;
- open-to-first-block and LIMIT cases separately from steady-state full scans.

Before attributing a regression, confirm that output row counts are identical, the benchmark name
and fixture encoding match, and repetition variability is acceptable. A coefficient of variation
above 3% is inconclusive and should be rerun. Correlate CPU regressions with `perf stat` counters
such as cycles, instructions, branch misses, and cache misses when possible.

## Coverage audit and required follow-up

The current matrix is not comprehensive. Preserve this distinction in PR descriptions and reports.

### P0: complete the local benchmark phase

1. Add the design's `matrix.yaml`, deterministic corpus generator, `manifest.json`, checksum, and
   standalone corpus verifier. The current runtime-generated files cannot be shared unchanged with
   V1, StarRocks, or DuckDB.
2. Add full reader correctness oracles. Decoder cases validate consumed counts and selected-value
   checksums outside the timed region, and kernel cases compare representative output. Reader cases
   still need value checksums in addition to their output-row counters.
3. Add reader-level INT64, FLOAT, DOUBLE, BYTE_ARRAY/string, FIXED_LEN_BYTE_ARRAY, DATE,
   TIMESTAMP, and DECIMAL cases. Today only nullable INT32 reaches the complete reader path.
4. Extend decoder coverage with definition levels/null reconstruction, dictionary conversion, and
   real Doris `Column` materialization. The current decoder sink does not cover those costs or
   report decoded bytes per second.
5. Add the representative nullable sparse corpus requested by the design: 32 INT64 columns, 128
   row groups, and enough rows to exercise many pages. The current 16K-row/four-row-group fixture
   is a smoke-sized workload.
6. Add dictionary footprint sweeps based on actual bytes relative to L1/L2 cache, value-width
   sweeps, cardinality sweeps, and dictionary-to-PLAIN fallback/mixed-page files. The current
   dictionary is small and fixed.
7. Add a Doris V1/V2 SQL runner over the same verified corpus, correctness comparison, Runtime
   Profile collection, normalized result JSON, comparison report, and the first stable local
   baseline. The current 1 ms run only proves executability.

### P1: broaden Parquet format and execution behavior

1. Vary compression codecs: uncompressed, Snappy, ZSTD, LZ4, Brotli, and Gzip where supported.
2. Cover DataPage V1 and V2, page size, row-group size/count, page boundaries, small files, and
   large files that exceed cache capacity.
3. Add required/non-nullable columns, correlated and anti-correlated null/predicate distributions,
   random deterministic runs, long null runs, and selections that cross page boundaries.
4. Add Boolean, INT96 compatibility, logical annotations, timestamp units/time zones, decimal
   physical representations, short/long strings, skew, and empty values.
5. Add nested ARRAY/MAP/STRUCT and repeated definition/repetition-level cases.
6. Add row-group statistics, page index, Bloom filter, dictionary filtering, all-filtered batches,
   missing-column/default-value handling, schema mapping, and schema-evolution cases. Keep pruning
   benchmarks separate from decode benchmarks so skipped work is not misattributed to a faster
   decoder.
7. Add predicate LIMIT 1/1000 cases. Current LIMIT cases only vary reader batch size without a
   predicate.
8. Add peak tracked memory, allocation counts, and an explicit warm/cold-cache methodology.

### Later phases

Remote S3/FileCache/RTT workloads, prefetch metrics, cross-engine adapters, nightly/weekly jobs, and
release regression gates belong to later phases. They require isolated infrastructure and must not
be simulated by silently changing the local reader benchmark.

## Current validation record

The current expected registration counts are 228 decoder, 80 kernel, and 151 reader cases. A smoke
run is an execution record only, not a reviewed performance baseline, because repetitions, host
isolation, warmups, cache control, `perf` data, variance, and before/after comparison are not
collected.

## Rules for extending the suite

- Keep deterministic data construction and validation outside timed regions.
- Verify actual footer encodings; never trust writer configuration alone.
- Change one primary axis at a time and add only meaningful second-order interactions.
- Add or update matrix invariant tests before registering new dimensions.
- Keep correctness/error-injection tests separate from throughput benchmarks.
- Do not commit large generated corpora. Commit generation rules and manifests; keep only small
  correctness fixtures in the repository when necessary.
- Record the exact commit, compiler, build type, host topology, command, repetitions, cache state,
  and raw JSON for any claimed performance result.
- Never claim a performance improvement from a smoke run or from incomparable machines.
