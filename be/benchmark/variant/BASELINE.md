# Recorded baseline: 2026-09-17

A full run of both suites (`^BM_Variant`, every registered case, default 1,000,000 rows) was
executed three times back to back on a shared build host to record a baseline and to measure
run-to-run noise from other jobs on the same machine.

## How it was run

```shell
source custom_env.sh
DORIS_BENCHMARK_CPU=16 bash be/benchmark/variant/run_variant_segment_benchmark.sh \
    be/output/lib/benchmark_test <result_dir> '^BM_Variant'
```

- Commit: `adfb3451eaeafb1c3fbb4e437cf2931cdb74a57b` (this PR branch, merged with master
  2026-09-15; working tree otherwise clean — `build.sh` differs only because of local worktree
  tooling, unrelated to this code)
- Build: `RELEASE`, `./build.sh --be --benchmark`
- Host: shared 192-thread build machine, 2x Intel(R) Xeon(R) Platinum 8457C (48 cores/socket, 2
  threads/core), pinned to **one** logical CPU (`DORIS_BENCHMARK_CPU=16` → `taskset -c 16`). All
  `Writers8` threads inherit that single-CPU mask, so they interleave rather than run
  simultaneously; this run cannot demonstrate cross-core contention (see the note under
  `BM_VariantSparseImport` results below)
- Rows: default (1,000,000)
- Each round runs all 35 `BM_VariantCumulativeCompaction` cases (7 scenarios x 5 samples) and all
  20 `BM_VariantSparseImport` cases (4 scenarios x 5 samples) — 55 cases/round, 165 total, 0
  `error_occurred`.

## Machine-load impact

This is a **shared, heavily loaded** host, not an isolated benchmark machine. `/proc/loadavg`
around each round (192 logical CPUs):

| Round | load before | load after |
|---|---|---|
| 1 | 129.6 | 85.3 |
| 2 | 97.3 | 122.2 (peaked at 349 during the run, per round 3's "before") |
| 3 | 122.2 | 55.9 |

Round 2 ran while background load spiked well above the other two rounds, and it measured
20-100%+ slower across almost every scenario, e.g.:

- `Doc16/WholeVariant`: ~16-18s (rounds 1, 3) vs. ~37s (round 2)
- `MixedTypes/Writers1`: ~10.8 CPU-s/1M rows (rounds 1, 3) vs. ~24.6 (round 2)

Rounds 1 and 3 agree with each other within roughly 20%. **Treat single-sample numbers on a shared
host as unreliable**; compare medians across several rounds, and prefer an idle/pinned host when
the comparison matters (e.g. deciding whether a change is a regression).

## Results: `BM_VariantCumulativeCompaction` (`real_time`, ms; lower is faster)

| Scenario | Round 1 median | Round 2 median | Round 3 median | All 3 rounds median (n=15) | Rounds 1+3 median (n=10, lower load) |
|---|---:|---:|---:|---:|---:|
| Sparse16/FlatPhysicalColumns | 7,830 | 9,668 | 6,257 | 8,177 | 7,305 |
| Sparse16/MixedPhysicalPlacement | 7,577 | 10,364 | 6,824 | 7,577 | 7,216 |
| Sparse16/WholeVariant | 16,076 | 16,648 | 15,877 | 16,123 | 15,977 |
| Full/WholeVariant | 16,762 | 18,868 | 16,042 | 16,762 | 16,154 |
| Doc16/DocBuckets | 18,847 | 32,732 | 19,069 | 26,066 | 18,958 |
| Doc16/DocBucketsMaterialized | 30,530 | 43,819 | 22,102 | 30,530 | 22,930 |
| Doc16/WholeVariant | 17,591 | 37,482 | 16,179 | 19,411 | 17,200 |

## Results: `BM_VariantSparseImport` (`cpu_s_per_1m_rows`; lower is faster)

| Scenario | Round 1 median | Round 2 median | Round 3 median | All 3 rounds median (n=15) | Rounds 1+3 median (n=10, lower load) |
|---|---:|---:|---:|---:|---:|
| MixedTypes/Writers1 | 10.831 | 24.594 | 10.880 | 11.151 | 10.855 |
| NoArrays/Writers1 | 9.006 | 18.820 | 11.637 | 11.637 | 9.094 |
| MixedTypes/Writers8 | 15.543 | 24.367 | 13.791 | 15.543 | 14.378 |
| NoArrays/Writers8 | 10.810 | 13.407 | 10.617 | 10.816 | 10.671 |

`MixedTypes/Writers8` costing more than `MixedTypes/Writers1` per-writer CPU time, and both
`NoArrays` variants sitting below their `MixedTypes` counterparts, is a valid same-scenario
comparison: `cpu_s_per_1m_rows` sums each writer's own thread-CPU time regardless of how they were
scheduled, so it stands on its own. **This run cannot, however, be used as evidence of cross-core
contention.** This run pinned all 8 `Writers8` threads to a single logical CPU (`taskset -c 16`),
so they were interleaved, never running simultaneously — the kind of shared-reference/cache-line
contention this benchmark is meant to expose only shows up across distinct cores. Attributing the
Writers1-vs-Writers8 gap to that contention would require rerunning with the threads bound to at
least 8 distinct physical CPUs (and recording the topology), which this baseline does not do.

## Caveats

- These absolute numbers are specific to this shared host, this build, and this moment's
  contention; do not port them to a different machine or compare across a build-type change.
- The "rounds 1+3" column is a best-effort lower-noise reference (excludes the visibly load-spiked
  round 2), not a clean isolated-host measurement. Even rounds 1 and 3 individually show real
  variance (e.g. `Doc16/DocBucketsMaterialized` min/max across all 15 samples was 21.4s/97.6s).
- Full `raw.json` / `environment.txt` / `stdout.txt` / `stderr.txt` / `source.diff` for each round
  are not committed (regenerate with the command above); this file records the derived medians
  only.
