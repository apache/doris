<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# BE compile benchmark (`build.sh --compile-bench`)

A timing framework for analyzing and optimizing the **BE C++ build speed**.
It answers two questions:

1. **Which build phase** is slow (gensrc / submodules / cmake configure / compile)?
2. **Which translation unit / header / template** is slow, and why?

## Usage

```bash
# One cold, cache-free benchmark build of the BE, then print + save a report
./build.sh --compile-bench

# Additionally collect clang -ftime-trace data (per-header / per-template costs)
COMPILE_BENCH_TRACE=ON ./build.sh --compile-bench

# The usual knobs still apply
BUILD_TYPE=Release ./build.sh --compile-bench -j 16
```

Results are stored per run in `be/compile-bench-results/<UTC timestamp>/`:

| file               | content                                                        |
|--------------------|----------------------------------------------------------------|
| `report.txt`       | the human-readable report (also printed at the end of the run) |
| `summary.json`     | machine-readable summary, used by `report.py compare`          |
| `meta.tsv`         | commit, toolchain, `-j`, PCH on/off, ... of the run            |
| `phases.tsv`       | raw phase timestamps                                           |
| `compile_log.jsonl`| one record per compiler/linker invocation (wall/user/sys/rss)  |
| `ninja_log.txt`    | copy of the build dir's `.ninja_log`                           |

Compare two runs (e.g. before/after an optimization attempt):

```bash
python3 build-support/compile-bench/report.py compare \
    be/compile-bench-results/<old_run> be/compile-bench-results/<new_run>
```

Re-generate a report from raw data (e.g. after an interrupted run, using the
still-existing build dir):

```bash
python3 build-support/compile-bench/report.py be/compile-bench-results/<run> \
    --build-dir be/build_Release_compile_bench --build-status interrupted
```

## How caches are kept out of the measurement

Benchmark numbers are only comparable when every run does the same cold work:

- **ccache is fully disabled**: the timing wrapper replaces ccache as
  `CMAKE_<LANG>_COMPILER_LAUNCHER`, and `CCACHE_DISABLE=1` is exported as a
  belt-and-braces measure for anything else that might call ccache.
- **A dedicated build dir** `be/build_<Type>_compile_bench` is deleted and
  recreated on every run: no incremental objects, no CMake cache, no stale
  ninja state. Your normal `be/build_<Type>` dir is left untouched, so day-to-day
  incremental builds are not harmed by benchmarking.
- **BE-only scope**: FE, cloud, java extensions, cdc client, UI and output
  packaging are forcibly skipped; the run ends right after the C++ build and
  the report, before `install`.
- PCH (`ENABLE_PCH`) stays at its normal default on purpose — the PCH is
  generated inside the fresh build dir each run, so it is cold work, and it is
  part of the real build being optimized. Override with `ENABLE_PCH=OFF` to
  compare with/without PCH.

## What the report contains

- **Phases**: gensrc, contrib submodules, datasketches install, cmake
  configure, build — with durations and share of total wall time.
- **Build summary**: TU count, sum of per-TU wall/cpu time, effective
  parallelism, slowest TU, most memory-hungry TU, link times with peak RSS.
- **Top N slowest translation units** with wall/user/sys/maxrss per file.
- **Wall time by directory** (top-level and second-level under `be/src`) —
  tells you which module to attack first.
- **Last finishers** from `.ninja_log` — the critical-path tail the whole
  build waits on (typically the monster TUs and the final link).
- With `COMPILE_BENCH_TRACE=ON` (clang only): **top headers by inclusive parse
  time**, **top template instantiations**, and a **frontend vs backend split**
  for the slowest TUs — this is what tells you *why* a file is slow
  (header/include cost vs template instantiation vs codegen).

## Implementation notes

- `cc-timing-wrapper.py` sits in the compiler-launcher slot, `fork/exec`s the
  real compiler with inherited stdio, and records wall time plus
  `rusage` (user/sys/maxrss) per invocation into `compile_log.jsonl`.
  It is a strict pass-through: exit codes and diagnostics are unchanged, and
  without `DORIS_COMPILE_BENCH_DIR` set it `exec`s straight through.
- Linker launchers (`CMAKE_<LANG>_LINKER_LAUNCHER`) need CMake >= 3.21; with an
  older CMake, link times still show up via the ninja log, just without RSS.
- `-ftime-trace` JSON files land next to the object files in the (transient)
  bench build dir and are aggregated immediately at the end of the run; expect
  roughly 1-2 GB of temporary JSON for a full BE build. Header/template times
  are *inclusive* (nested includes count into their parents), so they rank
  hotspots but do not add up to wall time.
- Timings from a `make` (non-ninja) build lose only the "last finishers"
  section; per-TU data comes from the wrapper and is generator-independent.
