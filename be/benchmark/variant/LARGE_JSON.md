# Large JSON VARIANT optimization: local validation

Baseline: `1f241cf9b451730d06fc2edbba89bef27ba4efda` (master snapshot from 2026-11-11).

## Evidence and mechanisms

The supplied [capture](https://gavin-tmp-sh.oss-cn-shanghai.aliyuncs.com/jrj/yhty/2026111121/capture-20261111222838-G7i2E9.tar.gz) contains a 10-second, system-wide CPU-clock recording at 49 Hz on 32 CPUs, plus an SVG. Its source revision, input JSON and DDL are unavailable. The SVG contains 8,432 displayed samples; the perf log reports 15,680 raw samples. The following percentages use the displayed samples and deduplicate recursive frames. Inclusive rows overlap and must not be added.

| Captured path | Samples | Displayed samples |
|---|---:|---:|
| Memtable flush | 2,608 | 30.93% |
| VariantPathBuilder::append under flush | 2,442 | 28.96% |
| infer_type under flush | 1,533 | 18.18% |
| path_least_common_type under flush | 497 | 5.89% |
| TabletSchema::copy_from under compaction | 1,096 | 13.00% |

The local wide-object baseline exposed another cost: repeated external-metadata bulk loading. In a separate profiled whole benchmark invocation, VariantExternalMetaReader::load_all_once accounted for 52.22% of samples and TabletSchema::copy_from for 1.99%. The corresponding fixed profile shows 2.69% and 0.07%. Both invocations include fixture preparation and validation, and their totals differ, so these fractions supply attribution rather than a speedup measurement. Profiled runs are excluded from the timing table.

The implementation changes three mechanisms:

- Array traversal borrows an ArrayView, following ObjectView, to validate the container layout once per traversal. Child offsets, sizes and adjacent boundaries retain their existing checks. Array inference stops recomputing common scalar types once JSONB absorbs further scalars, or DOUBLE already covers FLOAT/DOUBLE. Later object/array children still trigger whole-subtree JSONB fallback.
- Vertical compaction copies its reader schema once. Each column group retains its own projected ReadSchema, and the shared reader snapshot remains separate from the writer schema.
- Flat-leaf compaction uses the existing indexed exact-path metadata lookup for ordinary physical leaves. Missing paths retain full-tree/sparse-statistics planning. Root, sparse/doc storage paths and enabled NestedGroup providers retain their existing route. No new persistent cache is introduced.

## Reproduction and timing

Each case has 2,000 rows. LongIntegers and LongMixedNumbers contain 8,192 array elements; String256KiB contains a 256 KiB string; Wide2048 contains 2,048 payload fields in addition to the original flat fields. Wide2048Physical materializes 2,248 paths. Compaction merges ten overlapping key ranges, with input rowset versions 0 through 9.

Both binaries use RELEASE/O3, clang 20, coverage OFF and the same CPU affinity (CPU 24). The benchmark-only local build disables the repository glibc compatibility library because this toolchain already supplies it; no product configuration or compatibility code changes. Five fresh-input rounds alternate baseline/fixed order. The host is shared; per-run environment and load snapshots are retained.

Import measures real VerticalSegmentWriter initialization, append and finalization. JSON generation, parsing and full output validation are excluded. Import CPU is summed over writer threads. Compaction uses the actual cumulative/vertical merge and publication path; its CPU counter covers the calling thread. Network, FE planning, load-transaction latency and Cloud object storage are outside this benchmark.

**CPU milliseconds for 2,000 rows, median of five runs. Positive reduction means faster.** Use the custom CPU counters, not Google Benchmark cpu_time, which also includes untimed fixture/validation work.

| Case | Phase | Baseline median | Fixed median | CPU reduction | Baseline range | Fixed range |
|---|---|---:|---:|---:|---:|---:|
| LongIntegers | Import | 2368.3 | 2016.4 | 14.9% | 2338.7–2387.4 | 2007.3–2087.2 |
| LongIntegers | Compaction | 3746.7 | 3456.2 | 7.8% | 3724.7–3810.0 | 3418.4–3484.0 |
| LongMixedNumbers | Import | 8280.1 | 3597.8 | 56.5% | 8202.8–8445.4 | 3567.7–3719.5 |
| LongMixedNumbers | Compaction | 9666.5 | 5049.7 | 47.8% | 9605.5–10192.0 | 5034.4–5220.2 |
| String256KiB | Import | 609.6 | 607.5 | 0.4% | 602.8–628.8 | 598.1–613.6 |
| String256KiB | Compaction | 1959.8 | 1949.6 | 0.5% | 1928.7–1968.8 | 1937.0–1978.8 |
| Wide2048 | Import | 801.2 | 812.5 | -1.4% | 790.8–817.4 | 803.6–818.1 |
| Wide2048 | Compaction | 2934.7 | 2903.6 | 1.1% | 2857.4–3021.2 | 2878.5–2945.5 |
| Wide2048Physical | Compaction | 37795.5 | 9200.9 | 75.7% | 37621.5–38530.2 | 9153.0–9613.0 |

**Wall-clock milliseconds for the same timed regions:**

| Case | Phase | Baseline median | Fixed median | Reduction |
|---|---|---:|---:|---:|
| LongIntegers | Import | 2393.0 | 2038.9 | 14.8% |
| LongIntegers | Compaction | 3778.8 | 3487.4 | 7.7% |
| LongMixedNumbers | Import | 8345.5 | 3625.2 | 56.6% |
| LongMixedNumbers | Compaction | 9738.9 | 5095.4 | 47.7% |
| String256KiB | Import | 614.6 | 612.7 | 0.3% |
| String256KiB | Compaction | 1973.0 | 1963.2 | 0.5% |
| Wide2048 | Import | 825.2 | 820.5 | 0.6% |
| Wide2048 | Compaction | 2954.3 | 2923.4 | 1.0% |
| Wide2048Physical | Compaction | 38087.9 | 9264.4 | 75.7% |

The gains apply to array inference/traversal and wide physical-subcolumn compaction. Pure string payloads and whole-VARIANT wide-object rewriting show little change. Wide-object import has no demonstrated gain; its small signed difference is retained above. A separate five-sample Wide2048-import-only control gives 832.6 ms versus 820.6 ms CPU medians (1.4% lower), with overlapping ranges of 822.3–861.2 ms and 793.7–888.8 ms. The opposite small signed differences do not establish a consistent import improvement or regression. These measurements do not establish production or parallel-writer throughput improvements.

## Correctness and checks

- Baseline: 33 focused ASAN tests passed before changing product code.
- Final source: 147 ASAN tests selected; 143 passed, zero failures, four skipped because NestedGroup support is unavailable. This includes writer/reader compatibility, decimal/float promotion, sparse/doc layouts, vertical DUP/UNIQUE/AGG merges, deletes, key ranges and cancellation.
- Added tests validate 8,192-element FLOAT/DOUBLE arrays and null maps, container fallback after heterogeneous scalar inference, ArrayView offset widths/truncation/child boundaries, and 128-field VARIANT compaction across groups followed by rowset reload and independent per-field expectations.
- All 90 timed benchmark cases validate persisted complete values against independently generated JSON checksums. Compaction additionally verifies output versions/row counts/layout, replacement output files and absence of ordered link-file compaction. The benchmark checksum canonicalizes exact small numeric values; native type preservation is additionally covered by unit tests.
- RELEASE benchmark build through build.sh, clang-format 16 checks, build hygiene, git diff --check and clang-tidy on changed lines passed. No regression goldens were edited or generated; this task uses BE unit/storage benchmarks rather than a SQL cluster.

## Review checkpoints

| Checkpoint | Conclusion |
|---|---|
| Goal and scope | Large-array and wide-schema costs reproduced before optimization; the complete matrix retains unaffected and no-gain cases. |
| Minimality and reuse | ArrayView follows ObjectView; metadata lookup and ColumnReaderCache are existing APIs; no global cache or new runtime setting. |
| Concurrency and locks | Column groups run sequentially. The snapshot is read-only for their readers; each group owns its ReadSchema. Exact lookup runs before acquiring the metadata-tree shared lock and uses existing cache/reader locking. No new lock order or thread. |
| Ownership and memory | Views borrow immutable input bytes only while the input is alive. The schema snapshot has shared ownership through group readers. No untracked owning buffer or cross-TU static initialization is added. |
| Compatibility/configuration | No storage format, FE/BE protocol, thrift/protobuf field or configuration change. V2/inline metadata and missing paths retain existing fallback; selected I/O errors propagate. |
| Parallel paths | Cloud and local compaction share Merger. Dynamic tests are local; enabled NestedGroup planning is explicitly bypassed by the new optimization and remains unvalidated here. Horizontal compaction is unchanged. |
| Transactions/persistence | Writer schema stays isolated; row selection, delete bitmaps, version publication and rowset replacement code are unchanged. Reload and physical-output checks passed. Full cluster restart was not tested. |
| Observability | Existing read routes/counters remain; scoped benchmark CPU counters and separate perf captures supply attribution without adding hot-path logs. |

## Commands and retained artifacts

Build with the repository entry points; omit -j to retain default parallelism:

```shell
BUILD_TYPE=RELEASE ./build.sh --be --benchmark
DORIS_VARIANT_BENCHMARK_ROWS=2000 DORIS_BENCHMARK_CPU=24 \
  bash be/benchmark/variant/run_variant_segment_benchmark.sh \
    be/output/lib/benchmark_test /tmp/variant-large-json-results \
    '^BM_VariantLargeJson'

BUILD_TYPE=ASAN ./run-be-ut.sh --run --filter='VariantPathBuilderTest.*:VariantShredderTest.*:VariantValueTest.*:VerticalCompactionTest.*:IndexStorageVariantCompactionReadTest.*:IndexStorageVariantCompactionSchemaTest.*:IndexStorageVariantCompactionUtilTest.*:VariantColumnWriterReaderTest.*:*VariantWriterCompatibilityTest*'
```

Local evidence is retained under `/tmp/variant-large-json-20261112/`: `ab_manifest.json`, `ab_summary.json`, `ab-{baseline,fixed}-{1..5}/raw.json`, per-run environment/source snapshots, both exact binaries, baseline-tests.patch, fixed-source.patch, validated-source-sha256.json, ASAN XML/logs, formatting/hygiene/lint logs and separate perf recordings. The supplied capture is retained alongside its parsed flame-analysis JSON. Profiled/smoke/discarded trials are excluded from the timing table.

The fixed binary was built from the baseline revision plus the retained product/test patch. Its delivered C++ file hashes are checked against the validated-source manifest; Git build metadata can still name the pre-commit baseline. No CI run or production Cloud validation is claimed.
