# Row-id conversion unit tests and benchmark

`enable_compressed_rowid_conversion` defaults to `true`. Eligible compactions use 4096-source-row blocks when their estimated dense row-pair array exceeds `compressed_rowid_conversion_threshold_bytes` (default **67,108,864 bytes / 64 MiB**). The estimate is eight bytes per physical row of the actual data input rowsets, including deleted rows but excluding whole rowsets already removed by changelog TTL. Selection happens once before source registration. Exactly the threshold stays DENSE; zero enables compression for every eligible nonempty mapping, and negative thresholds are rejected. Both settings are dynamic and affect subsequent selections, without changing a mapping already under construction.

Consumers include MoW delete bitmap conversion and SNII postings compaction on both MoW and Duplicate Key tables. CLucene postings compaction, cluster-key tables, sequence-map tables, and the legacy index-compaction debug dump retain DENSE. Tasks needing neither bitmap conversion nor postings compaction allocate no mapping. The implementation does not depend on a maximum segment row count and does not spill to disk. The compaction profile's `RowIdConversion` information field records the selected mode.

`RowIdConversion` composes one concrete DENSE, LAZY_CHUNKED or COMPRESSED storage object using a private `std::variant`. Source identity and visibility tracking stay in the facade; layout-specific allocation, append, lookup and finalization stay in each storage implementation. A single visit surrounds each `add()` batch, so its row loop has no mode checks or virtual calls. Dense source views still expose direct spans for posting access and CLucene's existing matrix API is preserved.

The generic encoding is delta-varint64 followed by one Zstd level-1 attempt. EMPTY and LINEAR omit redundant values; RAW64 bounds the final payload size. The map uses a shared 1 MiB paged arena, one builder per opened source, and one encoder workspace. `finalize()` releases builders/encoder before consumers start and derives destination positions from actual output segment row counts.

## Run

From the Doris checkout:

```sh
bash run-be-ut.sh --run \
  --filter='CompressedRowIdMapTest.*:CompressedRowIdMapBenchmark.*:SniiRowIdConversionTest.*:SniiPostingCursorTest.*:SniiIndexCompactionTest.*:*TestRowIdConversion*' \
  -j100
```

To run only the informational benchmark:

```sh
bash run-be-ut.sh --run --filter='CompressedRowIdMapBenchmark.*' -j100
```

The script builds the BE unit-test binaries before applying the runtime filter. For optimized timings, set `BUILD_TYPE_UT=RELEASE` in your local build environment, including `custom_env.sh` if that file sets this variable, and use the same script. ASAN/O0 timings are diagnostic observations, not production throughput numbers. No duration or compression-ratio threshold determines whether this benchmark passes.

## Workloads and output

`ReportsWholeMapMemoryAndLookupCosts` constructs the compressed kernel directly, independently of the automatic compaction threshold (these small samples would normally select DENSE). It builds all blocks for 262,144 source rows, with 1 or 16 sources and about 10% randomly missing rows. The gap parameter chooses either unit steps or uniform steps in `[1, 32]`; steps are multiplied by the source count. Construction and sequential reads alternate sources. Input generation is outside timed regions. These are synthetic monotone source streams, not a measured compaction trace or a joint complete-output model.

Each `ROWID_BENCH` line reports bytes and milliseconds:

| Field | Meaning |
|---|---|
| `dense_bytes`, `oracle_bytes` | 8 bytes per source row; the dense timing copy and input oracle are outside compressed-map accounting |
| `directory`, `payload` | Complete directory capacity and used encoded payload for all sources/blocks |
| `arena_capacity` | Allocated pages, including page-tail gaps and the final unused tail |
| `metadata` | Map implementation object, source-container capacity and page-pointer capacity |
| `retained` | Owned bytes after finish: directory + arena capacity + metadata |
| `owned_peak` | Maximum owned sum observed after allocation; excludes transient vector reallocation overlap |
| `builders_before_finish`, `encoder_before_finish` | Build workspaces before their release; encoder includes bounded CCtx, plain bytes and compression candidate |
| `cursor_bytes`, `workspace` | All source cursor blocks and the single shared decode workspace |
| `tracked_build_peak`, `tracked_retained`, `tracked_total_peak` | Consumer-tracker observations; each allocation/free is recorded without the limiter tracker's small-allocation batching |
| `build_ms` | Directory registration, interleaved append, encoding/compression and finish |
| `cold_decode_ms` | One lookup per block with cold cursor state, including full block decode |
| `sequential_ms`, `sequential_decodes` | All rows through the lookup API, alternating sources; correctness assertions are included in this UT timing |
| `sparse_4096_ms` | 64 ascending groups of 64 lookups, allowing rewind between groups |
| `random_4096_ms` | 4096 random lookups; unlike ordered streams, this can repeatedly decode blocks |
| `dense_build_ms`, `dense_sequential_ms` | Copy and direct-scan baseline for the same pre-generated input; not an end-to-end DENSE compaction |

These counters measure the compressed storage kernel and its reader workspaces. They exclude the benchmark's ordinary STL containers, allocator bookkeeping/RSS, the enclosing `RowIdConversion` source index/destination prefix, and other compaction/index/bitmap allocations. The enclosing map's index and prefix use tracked Doris allocators as well, but are exercised separately in the integration unit tests. A 1 MiB arena tail and codec workspaces can dominate small maps, so encoded payload alone must not be reported as total or peak memory.

## Correctness coverage and remaining validation

`CompressedRowIdMapTest` covers all four representations, deletion holes, skipped blocks, tails, wide ordinals/gaps, arena page transitions, independent source caches, backward seeks, malformed metadata/payload, allocation failure and balanced memory accounting. A separate billion-row EMPTY-source case checks the complete block directory without allocating or iterating a billion dense entries; it is not a billion-row compression benchmark.

Selection tests cover threshold boundaries, multiple input rowsets, zero/disabled settings, overflow-safe large row counts, Duplicate Key SNII and the dense-only restrictions. Merge-reader unit tests cover compressed mappings for Duplicate Key and MoW with horizontal/vertical readers, overlapping/nonoverlapping segments and delete predicates. Storage lifecycle tests verify balanced tracking when selecting and destroying all three alternatives. Source-view tests compare joint interleaved mappings with DENSE and retain SNII's full coverage validation. Posting tests compare inline/slim/windowed results with and without deletions. An in-memory SNII merge checks nulls/postings against the rebuilt index bytes. Bitmap tests cover physical segment IDs and distinguish conversion errors from missing or visibility-filtered rows. SNII read contexts reserve one cursor block per prepared source before writing output and reuse it across terms, with one decode workspace per executing plan.

Actual local/cloud base compaction, concurrent loading/pending transactions, lock-hold time, cluster-wide memory peaks and optimized production timing remain separate validation work. The default threshold is a size-based policy, not a production-measured break-even point.
