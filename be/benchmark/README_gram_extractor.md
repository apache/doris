# Gram extraction benchmark

`GramExtraction` measures the real `GramExtractor::extract` implementation, including
ASCII folding, boundary selection, per-row deduplication and allocations. It is not a
benchmark of complete token streams, index construction, or SQL queries.

## Build and run

Set `BUILD_TYPE=RELEASE` in the local `custom_env.sh` for this performance build, then
use the standard build entry point:

```bash
./build.sh --benchmark -j "$(nproc)"
./output/be/lib/benchmark_test \
  --benchmark_filter='GramExtraction/' \
  --benchmark_min_time=0.3s \
  --benchmark_repetitions=7 \
  --benchmark_out=gram-extraction.json \
  --benchmark_out_format=json
```

On Linux, make the build's `libjvm.so` discoverable through `LD_LIBRARY_PATH` if
needed. Keep correctness tests in the normal ASAN build; do not compare an ASAN
timing with a RELEASE timing.

## Matrix

| Argument | Values |
| --- | --- |
| `bytes` | 128, 4096, 65536, 1048576 |
| `sparse` | 0: dense; 1: sparse |
| `lower_case` | 0: preserve ASCII case; 1: fold ASCII case |
| `corpus` | 0: deterministic diverse ASCII; 1: repeated ASCII log; 2: mixed UTF-8 log |
| `fresh` | 1: construct/destroy extractor and output vector per row; 0: reuse both |

Other parameters use `GramScheme` defaults: minimum length 3, maximum length 16,
density 250/1000, hash version 1. All inputs are generated before timing and reused
as hot input. The `fresh=1` lifetime matches per-value gram tokenization in the
writer; `fresh=0` isolates extraction after constructor and buffer reuse costs.

`grams_per_row` and `digest` record the initial extraction outside timing. Compare
them before interpreting timing differences between implementations. Correctness
of repeated calls is covered by the gram unit tests, not by a timing threshold.

For comparisons, use identical compiler flags, dependencies, input and CPU affinity.
Do not compile or run regression workloads concurrently with timing. Run both orders
(for example fixed/old/old/fixed), retain the per-repetition CPU `ns/op`, and inspect
dispersion as well as medians. Extend `benchmark_min_time` for cases with few
iterations or high variability. CPU frequency scaling, SMT and unrelated host load
can still affect measurements.

Report `fresh` and `reused` separately, as well as corpus and size: constructor
savings on short rows can conceal an extraction-only cost on long rows. A throughput
ratio from this single-threaded, hot-input microbenchmark is not an end-to-end
database speedup or a statement about all tokenizer parameter combinations.
