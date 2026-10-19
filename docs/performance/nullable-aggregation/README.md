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

# Nullable aggregation migration: reproducible validation

## Results

All 550 recorded queries (150 reference V1, 185 reference V2, 215 migrated) agree across 21 cases.
Each final kernel has five consecutive repetitions with CV below 5% (maximum 4.71%); all earlier repetitions remain included.
Query latency on this shared host is noisier; raw ranges and CV are reported rather than discarded.

| SQL case | Reference V2 mean ms | Migrated mean ms | Change |
| --- | ---: | ---: | ---: |
| numeric_high_null1 | 162.80 | 153.39 | -5.8% |
| numeric_high_null50 | 179.45 | 150.68 | -16.0% |
| numeric_high_null100 | 153.76 | 112.83 | -26.6% |
| variadic_null50 | 83.21 | 63.36 | -23.9% |
| stored_bulk_merge | 39.98 | 41.54 | +3.9% |
| stored_bulk_union_finalize | 25.94 | 26.08 | +0.6% |
| tpcds_sf1_q78 | 125.34 | 118.26 | -5.7% |

For the 4,096-row/1%-NULL streaming kernel, SUM drops from 7.162 to 3.258 µs and AVG from 18.108 to 9.269 µs versus existing V2. SUM requests 7→5 allocations and 139,392→73,856 bytes per operation. In grouped native SUM/AVG merge, the old retained scratch first allocation disappears: zero requests/bytes in the migrated counter run. These kernel improvements are separate from SQL wall time.

An initial untyped persistent codec regressed bulk MERGE by about 23%; keeping the concrete V2 function type removes per-state virtual dispatch and reduces the final difference to +3.9%, within the observed old/new timing ranges. Fusing native validity updates also avoids a second state pass. Earlier measurements are preserved in the diagnostic archive.

Wide string/set and topn wall-time differences settle at +1.8% and +1.1% after the old binary recheck; both ranges overlap. The unchanged old binary itself shifts across rounds, so those small differences are not a demonstrated engine regression. Q78 is an observed SF1 timing, not a general speedup claim.

Query peak memory does not uniformly decrease. Weighted AVG/50%-NULL mean is 254.1→262.3 MiB; its maxima are 256.7→277.6 MiB. Wide string/set mean is 2565.2→2577.6 MiB; maxima are 2603.1→2776.6 MiB. The corresponding within-migrated ranges are 250.1–277.6 and 2478.6–2776.6 MiB. Aggregate-owned peak counters remain 7.64 MB for weighted AVG and about 2.00 GB for wide string/set (only about 0.25 MB total state-counter difference). Thus the larger query maxima do not demonstrate batch scratch/state amplification. Separate nullable columns, spare capacity and concurrently buffered blocks are possible contributors outside those state counters; that attribution is an inference, not a measured byte-by-byte breakdown. The disposition is to retain the unified NULL contract and existing buffer limits, disclose all maxima and ranges, and avoid claiming a universal memory reduction.

Complete [query results](query-summary.md), [kernel results](kernel-summary.md), [operator avg/min/max](operator-summary.md), [binary/environment manifest](manifest.json), and [validation record](validation.json) accompany the raw archives.

## Builds and dataset

The reference is `e0bf4874070ba0b6837ad93f695ea63532b77a47`. Its production
aggregation implementation is unchanged; the benchmark harness from this change
is copied into that checkout, with `DORIS_NULLABLE_AGGREGATION_REFERENCE` selecting
the original V1/V2 attribute. The migrated calculation implementation is
`1a96ff98396`; subsequent test/report commits do not change it. The manifest records
binary hashes and the precise build metadata. These are new measurements,
independent of the timings reported by PR #60228.

One shared machine: Intel Xeon Platinum 8457C, 2 sockets, 96 physical cores /
192 logical CPUs, approximately 1.48 TiB RAM, no swap. Frequency scaling remains
enabled and other users share the host; differences within the observed range
are not reliable gains. Both builds use Clang 20, RELEASE, `-O3`, AVX2, PCH off,
and JDK 17. The services use identical 16 GiB BE / 8 GiB FE limits, one BE,
`parallel_pipeline_task_num=4`, `agg_phase=2`, SQL cache disabled and profiling
level 2. All services use worktree-local output directories and isolated ports.
Builds, tests and profiler recording are stopped during timing runs.

`prepare.sql` creates four million deterministic nullable-typed source rows, a
small state table with two old rowsets, and an empty version-19 bitmap state
table. `prepare_bulk.sql` creates 100,000 state groups from 400,000 rows in two
inserts. Both scripts are run on the original version-19 build first. The old
FE metadata and BE storage are copied to independent paths while stopped before
starting the new binaries. Timed state tables are unchanged throughout all three
cohorts. Upgrade writes/compaction occur only after timing completes.

TPC-DS uses SF1 data generated with `tools/tpcds-tools/bin/gen-tpcds-data.sh`
and the repository SF1 create/load scripts (24 tables), followed by ANALYZE.
Q78 is exactly `tools/tpcds-tools/queries/sf1/query78.sql`; the driver records
its full SQL. The stock loader also attempts DBGEN_VERSION, which has no query
table and reports that missing metadata table; the 24 query tables load successfully.
This is a small single-BE reproduction, not the unspecified hardware,
scale or SQL used in the historical PR's operator timing table.

## Timing and counters

`run_queries.py` records 21 cases: BIGINT/DECIMAL SUM+AVG at 64 versus one million
groups and 0/1/50/100% NULL; two-argument weighted AVG at the same NULL ratios;
wide string MIN/collect_set and topn; persisted MERGE and UNION/FINALIZE, including
bulk/complex states; and Q78. Every case processes multiple input batches.
The NULL patterns deliberately differ between weighted AVG arguments. SQL/DDL,
settings, query IDs, result values and repetition numbers are recorded.

Before each final cohort, run the complete matrix with five unrecorded warmups
per case, then run five additional warmups followed by at least five recorded
repetitions. Cases with wall-time CV above 5% receive another five warmups and
five recorded repetitions; **all** recorded repetitions are retained. A whole
matrix startup round and earlier implementations are separate diagnostic artifacts,
not silently mixed into the final cohort. Three final cohorts are old V1, old V2,
and migrated V2. Results compare exact integer/decimal/string/NULL values and
float values at `rel_tol=1e-9, abs_tol=1e-9`. A later reference-V2 check repeats
four cases on the same old data with exactly one live BE to assess shared-host
variation; those repetitions are appended rather than replacing the original
measurements. Registered but stopped secondary BEs do not execute these timings.

`collect_metrics.py` joins raw profiles to FE audit CPU milliseconds, query peak
memory, rows and bytes scanned. It preserves each STREAMING_AGGREGATION and
AGGREGATION_SINK ExecTime **avg/min/max** separately from wall time. These operator
counters are parallel task times and cannot be summed into query latency.
`query-summary.json` records mean, median, min/max, CV, CPU, reported peak and
throughput; `operator-summary.json` keeps per-operator repetition statistics.

The kernel harness covers 4,096-row streaming SUM/AVG (BIGINT/DECIMAL128) and native
merge with 64/4,096 groups at all four NULL rates. Run uninstrumented binaries
pinned to logical CPU 12, 0.1 s warmup, minimum 0.3 s measured CPU time, five
repetitions. Four noisy migrated kernels receive another five repetitions; two
receive a further five with minimum 5 s/warmup 0.5 s. All initial values and repeat
files remain available. The final five repetitions' CV is recorded separately
from the dispersion across all rounds. No build, SQL measurement or profiler
runs concurrently. The old
operator's retained vector scratch allocation is modeled, including its first
lazy growth; the migrated reader uses its actual capability. The allocation
binary is separate from the timing binary. Its 16-operation counters include
successful ordinary new/malloc/calloc/realloc requests during output/reader work,
exclude setup, and are **not** complete process allocation or live-memory peaks.
Aligned allocations, mmap and allocator internals are outside this counter.
Google Benchmark's unsupported peak/tombstone field is not a memory measurement.

CPU sampling is separate: kernel `perf record -e cycles:u -g` and 8 s repeated
`numeric_high_null1` with profiling disabled. Query sampling uses existing warmed
BE threads, per-thread buffers, no inheritance, 199 Hz. The BE is restarted after
packaging to keep ELF mappings resolvable; final recording logs report no lost
samples. Symbol reports and compressed raw perf data are retained.

## Compatibility and correctness

406 focused BE ASAN tests pass, including empty/NULL-only states, all NULL ratios,
selected and skipped readers, alignment, exception cleanup, mixed variadic inputs,
Block/PB round trips, converter boundaries, stable ordering and legacy readers.
The complete baseline UT target has unrelated Variant fixture/API build errors;
the standard runner's optional source filter leaves its default full target
unchanged. The exact focused gtest filter is in `gtest-filter.txt`.
The AGG_STATE directory's 19 SQL suites pass; added expected output was generated
and compared with the standard regression runner. RELEASE BE, FE build, C++16
format, build hygiene and changed-line clang-tidy checks pass. Existing whole-file
tidy warnings outside changed lines are retained in validation logs. Whole-header
clang-tidy exits nonzero on eight pre-existing diagnostics; the diff-line comparison
finds zero on changed lines (`tidy-diagnostics.json`). New codec and UT file checks
pass without warnings.

Real old version-19 rowsets are read by new BE, including String, fixed COUNT,
Bitmap, Array and Map codecs. The compatibility records cover old FE/old BE,
old FE/new BE, new FE pinned 19/old BE, new FE pinned 19/new BE, and new FE20/new BE,
with both published session values. Two additional real two-BE tests force scans
and writes across old/new replicas under **both** old FE19 and new FE pinned19;
raw profiles identify both BE heartbeat endpoints. Ordered DISTINCT also passes
through the old wire codec. These prove format interoperability for the tested
baseline; they do not claim every historical released version has been executed.

Version 20's nullable bitmap no-input marker differs from a valid empty bitmap.
`verify_upgrade.py` writes it into the table whose schema was created at version19,
forces full compaction (three rowsets become one) and checks that subsequent valid `{1,2}` merges survive while an
actual empty bitmap still empties the intersection. A first cumulative trigger
on two tiny rowsets returned an empty candidate set under the default size policy;
that is not counted as successful coverage. The final full-compaction probe resumes
those committed inserts, records before/after rowsets, and the SQL regression now
also requires a decreasing rowset count. Mixed old/new rowsets in the
other historical table preserve existing valid states. A separate full compaction
of its two real historical rowsets plus the new insert reduces four rowsets to
one; all eight numeric/bitmap/collection/Array/Map results match before and after
(`mixed-old-new-rowsets-compaction.json`). Historical NULL-only
bitmap values already collapsed to empty by old serialization are irrecoverable;
`legacy-bitmap-ambiguity.json` records that limitation.

Keep FE execution version 19 until all executing/reading/compacting BEs support20.
Only then enable20/create new state types. Type metadata controls persistent
codec version; setting the current query back to19 does not downgrade a version20
type or remove markers already written into version19 schemas. A downgrade after
those writes requires compatible metadata **and** data. There is no automatic
backend feature negotiation in this change. The old session name stays accepted
as a deprecated no-op; its Thrift id is retained. See the migration report for
the complete entry matrix and format contract.

The sparse bitmap NULL-only partial case motivated by the reported issue is
covered across aggregation phases and parallelism. The original issue's private
reproduction/status could not be fetched; this report does not claim to have
verified that external issue's exact SQL or resolution status.

## Reproduction

1. Build the reference and migrated checkouts using `build.sh --be --fe` with
   identical RELEASE/O3 environment; use isolated ports and storage. On the
   reference only, build the copied harness with
   `-DDORIS_NULLABLE_AGGREGATION_REFERENCE`.
2. Load `prepare.sql`, `prepare_bulk.sql` and TPC-DS SF1 on the reference; take
   stopped metadata/storage snapshots for the migrated cluster. Analyze tables.
3. Prewarm with `run_queries.py <label> --repetitions 0`; then run the same driver
   normally, old labels with `--v2 false/true`. Use `--append --cases ...` for
   noisy cases, retaining all measurements. Collect metrics from that FE's audit
   log with `collect_metrics.py <label> <audit-file>`.
4. Build the timing harness with
   `DORIS_BE_BENCHMARK=nullable_aggregation ./build.sh --benchmark -j32` and save it.
   Build a separate counter binary adding `DORIS_BE_BENCHMARK_ALLOCATIONS=1`.
   Run the timing filter `nullable_.*/4096/(0|1|50|100)/1($|/)` (reference both
   mode arguments), warmup0.1/min_time0.3s/repetitions5; use the counter build only
   for allocation collection. Set `LD_LIBRARY_PATH` for the JDK server library.
5. Run `capture_query_hotspots.py` and the kernel perf command separately.
   Run `summarize.py`, `summarize_kernels.py`, `summarize_operators.py`.
   Upgrade/compaction checks mutate fixtures and must run last.
6. Extract `raw-records.tar.gz` into this directory before rerunning summaries or compatibility scripts. Raw profiles are archived to keep the PR file list manageable. Extract
   `raw-profiles.tar.gz` into this directory before re-running metric collection.
   The perf and diagnostic archives are independent of the final summaries.
   Diagnostic archives contain all earlier timing/metric records; their full raw
   profiles remain in the task workspace. Final raw profiles are all included.


## Artifact contents

* `raw-records.tar.gz`: original SQL/settings/results/query IDs, audit counters, kernel repetitions and allocation JSONL, CPU symbol reports, compatibility-stage records, and hardware descriptions. Final two-BE proofs use `mixed-final-*`; earlier stages retain their actual binary versions.
* `raw-profiles.tar.gz`: all final timing-query profiles and compatibility profiles, in the same file names expected by the collectors.
* `raw-perf.tar.gz`: original perf.data files and recording logs for old V1/V2 and migrated kernels/queries, plus the old/new wide-string workload.
* `validation-logs.tar.gz`: standard build, UT, regression, formatting, hygiene and tidy logs, including the unrelated full-UT build blocker.
* `diagnostic-runs.tar.gz`: cold/earlier implementation measurements and an early registration-only mixed-cluster probe; those records are excluded from the final cohorts. The registration-only probe does not prove both BEs executed a query.

Archive SHA256 and measured binary SHA256 are recorded in `manifest.json`. No binaries or generated TPC-DS dataset are committed.
