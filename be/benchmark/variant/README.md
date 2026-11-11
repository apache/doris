# Variant benchmarks

Two Google Benchmark suites live here:

- `BM_VariantCumulativeCompaction`: a source-version-neutral, high-level cumulative compaction
  workload. It writes 10 overlapping rowsets of synthetic Variant rows, runs a real
  `Compaction`/`VerticalSegmentWriter` merge over them, and validates the merged output's semantics
  and physical layout (materialized/sparse/doc-bucket subcolumn counts) before reporting timing.
  It has no Variant-version selector: the same workload should be comparable commit-to-commit as
  the underlying storage format evolves.
- `BM_VariantSparseImport`: an import workload for the segment writer's Variant shredder, which the
  compaction workload does not reach because it only writes flat BIGINT objects. Each row carries a
  Poisson(20) subset (clipped to [5, 60]) of 2,000 possible keys, with a key's value kind fixed by
  its index (50% BIGINT, 25% short strings, 10% doubles, 5% booleans, 5% BIGINT arrays of 1-4
  elements, 5% `{"x": BIGINT, "y": string}` objects) — matching the dataset used for end-to-end JSON
  and Parquet VARIANT import measurements. Input generation and `parse_to_variant` are untimed; the
  timed region is `VerticalSegmentWriter` init/append/finalize, which shreds every value through the
  Variant path builders.

The files are:

- `benchmark_variant_segment.hpp`: both workloads' scenario definitions, synthetic data generation,
  correctness checks, and Google Benchmark registration.
- `run_variant_segment_benchmark.sh`: wraps a benchmark run with an `environment.txt` snapshot
  (git commit, dirty diff, thirdparty fingerprints, host/CPU/memory info) so results can be tied to
  the exact code and machine that produced them, plus JSON output for later parsing.
- `README.md`: this file.
- `BASELINE.md`: a recorded baseline run with reproduction steps, for commit-to-commit comparison.

## Build

Performance results must come from a Release build:

```shell
./build.sh --be --benchmark
```

This installs `be/output/lib/benchmark_test`.

## List and run cases

```shell
be/output/lib/benchmark_test --benchmark_list_tests | grep '^BM_Variant'
```

`BM_VariantCumulativeCompaction` registers 7 scenarios x 5 samples = 35 cases:

| Scenario | Layout | Notes |
|---|---|---|
| `Sparse16/FlatPhysicalColumns` | 16-bucket sparse | subcolumns compacted, flat workload |
| `Sparse16/MixedPhysicalPlacement` | 16-bucket sparse | subcolumns compacted, mixed physical placement workload |
| `Sparse16/WholeVariant` | 16-bucket sparse | whole-Variant column compacted (no subcolumn compaction) |
| `Full/WholeVariant` | fully materialized | whole-Variant column compacted |
| `Doc16/DocBuckets` | 16-bucket Doc Mode | doc-bucket subcolumns compacted |
| `Doc16/DocBucketsMaterialized` | 16-bucket Doc Mode | doc buckets compacted and materialized |
| `Doc16/WholeVariant` | 16-bucket Doc Mode | whole-Variant column compacted |

`BM_VariantSparseImport` registers 4 scenarios x 5 samples = 20 cases: `MixedTypes` vs. `NoArrays`
(arrays replaced with BIGINT, isolating per-array type inference) crossed with `Writers1` vs.
`Writers8` (1 vs. 8 concurrent `VerticalSegmentWriter`s, matching parallel memtable flushes of
several tablets — this contends process-wide shared state such as static `DataTypePtr` reference
counts the way a real import does).

Run the full suite and capture an environment-tagged, reproducible result:

```shell
source custom_env.sh   # sets JAVA_HOME
bash be/benchmark/variant/run_variant_segment_benchmark.sh \
    be/output/lib/benchmark_test <result_dir> '^BM_Variant'
```

`<result_dir>/raw.json` has the Google Benchmark JSON; `<result_dir>/environment.txt` has the
commit, working-tree diff, thirdparty fingerprints, and host info needed to interpret it. Both
suites read `DORIS_VARIANT_BENCHMARK_ROWS` (default 1,000,000) for the row count and
`DORIS_VARIANT_BENCHMARK_ROOT` (default `/tmp`) for scratch space; `DORIS_BENCHMARK_CPU` pins the
run with `taskset` when set.

To run only one suite, pass its own filter as the third argument, e.g.
`'^BM_VariantSparseImport/'`.

## Interpret the result

- `BM_VariantCumulativeCompaction` cases are `->UseManualTime()` with a single iteration per
  registered case (`sample1`..`sample5` are separate named cases, not Google Benchmark
  repetitions); use `real_time` in milliseconds. Lower is faster.
- `BM_VariantSparseImport` reports `cpu_s_per_1m_rows` (also `append_ns_per_row`,
  `finalize_ns_per_row`, `segment_bytes_per_row`, `input_json_bytes_per_row`) alongside
  `real_time`/`cpu_time`. `cpu_s_per_1m_rows` is thread CPU time summed across writers, so
  `Writers8` is expected to cost more than `Writers1` even with no regression; compare `Writers8` to
  `Writers8` and `Writers1` to `Writers1` across commits, not to each other.
- Both suites validate output before returning: a compaction case checks the merged segment's
  subcolumn layout and rewritten values, and an import case checks each writer's row/segment count.
  A failing case aborts benchmark execution (`error_occurred` in the JSON) rather than silently
  reporting bad numbers.
- This is a workload-level benchmark against real `Compaction`/`VerticalSegmentWriter` code, not a
  microbenchmark of an isolated function — treat regressions as signals to investigate with a
  profiler, not as a precise attribution of which code path changed.
- On a shared/busy host, run-to-run noise can be large (see `BASELINE.md` for a measured example).
  Prefer the median of several samples, and re-run before concluding a change regressed or improved
  performance.

## Large JSON reproduction

`BM_VariantLargeJsonImport` and `BM_VariantLargeJsonCompaction` add the following
payloads to the existing flat-object workload:

| Case | Payload |
|---|---|
| `LongIntegers` | 8,192 integer array elements |
| `LongMixedNumbers` | 8,192 alternating integer/fractional array elements |
| `String256KiB` | One 256 KiB string |
| `Wide2048` | An object with 2,048 integer fields |

Import times the real segment writer, excluding JSON generation, parsing and output
validation. Compaction merges ten overlapping input rowsets through the whole-VARIANT
route. `Wide2048Physical` additionally compacts all materialized subcolumns in separate
vertical groups. Every case validates all output values against a checksum computed
from generated JSON; compaction also validates versions, physical rewrite and layout.
These are synthetic mechanism reproductions, not measurements of a production cluster.

Build with `BUILD_TYPE=RELEASE ./build.sh --be --benchmark`, then run:

```shell
DORIS_VARIANT_BENCHMARK_ROWS=2000 DORIS_BENCHMARK_CPU=24 \
  bash be/benchmark/variant/run_variant_segment_benchmark.sh \
    be/output/lib/benchmark_test /tmp/variant-large-json-results '^BM_VariantLargeJson'
```

Each case has five samples. Rows must be divisible by ten and at least 2,000 for
compaction's existing path-coverage checks. Import reports writer CPU separately from
wall time; compaction's `main_thread_cpu_ms` excludes CPU consumed by helper threads.
Retain both binaries and use identical workload sources, row counts and CPU affinity
when comparing commits. Do not include profiled samples in timing results.

Recorded analysis, timing and validation: [LARGE_JSON.md](LARGE_JSON.md).
