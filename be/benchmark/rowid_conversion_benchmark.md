# Row-id conversion block benchmark

`benchmark_rowid_conversion.hpp` measures the actual `CompressedRowIdMap` implementation. Each Google Benchmark iteration encodes or decodes one **4096-source-row block**. Correctness and whole-map allocation checks remain in `be/test/storage/compressed_rowid_map_test.cpp`.

## Build and run

Use the repository build entry point with a Release configuration:

```sh
DORIS_BENCHMARK_HEADER=benchmark_rowid_conversion.hpp bash build.sh --benchmark -j100
```

The local `custom_env.sh` must select `BUILD_TYPE=Release` (or `RELEASE`); an unconditional assignment there overrides a shell environment assignment. `BUILD_TYPE_UT` controls `run-be-ut.sh` and does not control this benchmark. On Linux the Release benchmark build defaults to `USE_JEMALLOC=ON`. Verify the actual CMake cache and compile commands when reporting results.

The selected header keeps unrelated benchmark registration out of the binary. It is also included in `benchmark_main.cpp` for a default benchmark build without `DORIS_BENCHMARK_HEADER`.

After building, load the usual environment and run the binary, optionally pinning it to an allowed CPU:

```sh
export DORIS_HOME="$PWD"
source ./env.sh
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH:-}"
taskset -c <allowed-cpu> be/output/lib/benchmark_test \
  --benchmark_filter=BM_RowIdConversion \
  --benchmark_min_time=0.05s \
  --benchmark_repetitions=5 \
  --benchmark_out=/tmp/rowid-block-benchmark.json \
  --benchmark_out_format=json
```

## Inputs and validation

Each workload pre-generates 64 different blocks using seed 89 and approximately 10% missing source rows:

| Workload | Live target ordinals |
|---|---|
| `linear_holes` | Consecutive targets; the validity bitmap preserves missing source positions. |
| `interleave31_holes` | One source from 31 round-robin streams sharing an increasing output ordinal. |
| `wide_gaps_holes` | Uniform target steps in `[1, 2^20]`. |

These are synthetic distributions, not production traces. Repeating a pattern with an overall target offset preserves monotonicity without changing its bitmap or within-block gaps. Setup builds a reference map and verifies all 64 × 4096 physical positions before timing, including missing values. Encoding checks the emitted payload size against the reference outside timing. Decoding checks the returned value and verifies that every lookup actually decoded another block.

## Timing boundaries

`BM_RowIdConversionEncode` fills a builder outside timing, then times the append crossing into the next block. It includes bitmap/varint encoding, the optional Zstd attempt, payload allocation/copy, directory update, builder reset and writing the next first row. Each map reuses one encoder, warmed on an unmeasured block. The map is replaced outside timing after 1024 measured blocks so calibration cannot grow storage indefinitely. Generation, registration, other append calls, destruction and assertions are excluded.

`BM_RowIdConversionDecode` times a cache-miss `CompressedRowIdMap::get()`. It cycles through 64 blocks with one reused Cursor and DecodeWorkspace, including directory lookup, Zstd decompression, bitmap/delta reconstruction and returning one value. It excludes first-use workspace allocation, correctness assertions and the enclosing `RowIdConversion` destination-prefix lookup. The CPU caches are not flushed between operations.

Both registrations use `UseManualTime()` and report **microseconds per block** in the `Time` column / JSON `real_time`. This interval is measured directly around the production operation. Google Benchmark's `CPU` column also includes some framework and clock bookkeeping and is not the same interval. Use the median of the five repetition averages for the main summary; the aggregate median is not a per-operation latency P50.

Counters report `rows_per_block`, `patterns`, average `plain_bytes` and `payload_bytes`, the fraction of stored Zstd frames, and throughput based on physical source rows / their raw uint64 bytes. Payload size excludes the 40-byte directory and workspaces.

This is a Release codec microbenchmark, not a complete compaction. The benchmark target uses the repository's `BE_TEST` / `BE_BENCHMARK` markers and its main disables memory tracking. Report the actual allocator, compiler flags, CPU and repeat variation; do not treat either ASAN timing or this benchmark as a measured production compaction duration. No timing threshold determines correctness.
