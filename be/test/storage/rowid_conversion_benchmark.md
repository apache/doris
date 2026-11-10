# Row-id conversion unit tests and benchmark

## When compression is selected

`Compaction::prepare_rowid_conversion()` first checks whether this task needs a mapping. At least one of these must hold: the tablet is UNIQUE_KEYS with merge-on-write enabled; `ctx.snii_indexes_to_do_compaction` is nonempty; or `ctx.columns_to_do_index_compaction` is nonempty. Without a consumer, it does not register a row-id mapping or allocate source-row arrays.

For a task that needs the mapping, `Compaction::should_compress_rowid_conversion()` selects COMPRESSED only when **all** of the following hold:

| Check in the helper | Required value | Reason |
|---|---|---|
| `config::enable_compressed_rowid_conversion` | `true` (default) | Enables automatic selection. |
| `ctx.columns_to_do_index_compaction.empty()` | `true` | Any planned CLucene postings compaction requires the dense matrix. |
| `config::debug_inverted_index_compaction` | `false` | The legacy debug dump requires the dense matrix. |
| `_cur_tablet_schema->cluster_key_uids().empty()` | `true` | Cluster-key readers can revisit source rows. |
| `_cur_tablet_schema->has_seq_map()` | `false` | Sequence-map readers can revisit source rows; this is not a check for any sequence column. |
| Dense array estimate | Strictly greater than `compressed_rowid_conversion_threshold_bytes` | Avoids codec overhead for small mappings. |

The threshold defaults to **104,857,600 bytes (100 MiB)**. The estimate is `sum(_data_input_rowsets.num_rows()) * sizeof(RowIdConversion::RowIdPair)`, or eight bytes per physical input row, including rows later deleted, merged or filtered. It excludes whole rowsets already removed by changelog TTL, and is unrelated to segment file size, output rows or compressed payload size. The code compares a remaining row budget instead of multiplying/summing large byte counts, avoiding overflow. At the default, **13,107,200 rows stay DENSE; 13,107,201 rows select COMPRESSED** if the other checks pass.

This is a check of the current compaction plan: merely having a CLucene index does not fail it if no CLucene postings compaction is planned. Eligible SNII postings use the separate `snii_indexes_to_do_compaction` field, so Duplicate Key SNII can use compression without MoW. An SNII index rebuilt from raw columns alone does not populate that field and does not itself require a mapping. The helper adds no base-compaction-only, horizontal/vertical or segment-size restriction.

If any helper check fails, a task that needs the mapping keeps DENSE. Selection runs once after output writer/index-plan preparation and before source registration. Exactly the threshold stays DENSE; zero enables compression for every eligible nonempty mapping, and negative thresholds are rejected. Both configuration settings are dynamic and affect subsequent selections, without converting an existing map. The compaction profile's `RowIdConversion` field reports COMPRESSED or DENSE when a mapping is used. The 100 MiB default is a size policy, not a measured production break-even point.

## Representation

`RowIdConversion` composes one concrete DENSE, LAZY_CHUNKED or COMPRESSED storage object using a private `std::variant`. Source identity and visibility tracking stay in the facade; layout-specific allocation, append, lookup and finalization stay in each storage implementation. A single visit surrounds each `add()` batch, so its row loop has no mode checks or virtual calls. Dense source views still expose direct spans for posting access and CLucene's existing matrix API is preserved.

The generic encoding is delta-varint64 followed by one Zstd level-1 attempt. EMPTY and LINEAR omit redundant values; RAW64 bounds the final payload size. Each nonempty block owns a separately allocated payload of exactly its encoded length; EMPTY and full LINEAR allocate none. Doris allocator tracking and jemalloc handle these allocations without an application-level arena. Each 40-byte directory entry includes the owning pointer and allocation length. The map also has one builder per opened source and one shared encoder workspace. `finalize()` releases builders/encoder before consumers start and derives destination positions from actual output segment row counts.

## Production compaction storage size

The compaction RuntimeProfile reports `RowIdConversionStorage` after merging rows and, for COMPRESSED, after `finalize()` releases its builders and encoder. It includes a readable byte size and the exact byte count from `RowIdConversion::memory_usage()`.

This is retained storage: the concrete storage object and allocated backing arrays; COMPRESSED includes all block directories, payloads, storage metadata and destination prefixes. Source-identity indexes, visibility sets, consumer cursor/workspace memory and allocator rounding/fragmentation are outside this count. It is separate from the whole compaction tracker peak and BE RSS. A DENSE row-pair-array estimate is physical input rows times eight bytes, including deleted/filtered source positions.

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

`ReportsLargeJointMappingMemoryAndLookupCosts` additionally constructs 16,777,216 physical rows across 16 sources (128 MiB dense equivalent). About 10% are missing. Sources alternate and share one increasing output ordinal, so live mappings jointly cover the output exactly. Replaying the seed verifies every source position without retaining a dense oracle. This still directly tests the kernel, not an actual compaction or the automatic-selection path.

Each `ROWID_BENCH` / `ROWID_LARGE_BENCH` line reports bytes and milliseconds:

| Field | Meaning |
|---|---|
| `dense_bytes`, `oracle_bytes` | 8 bytes per source row; the dense timing copy and input oracle are outside compressed-map accounting |
| `directory`, `payload` | Complete directory capacity and used encoded payload for all sources/blocks |
| `metadata` | Map implementation object and source-container capacity |
| `retained` | Requested owned bytes after finish: directory + separately allocated payloads + metadata |
| `owned_peak` | Maximum owned sum observed after allocation; excludes transient vector reallocation overlap |
| `builders_before_finish`, `encoder_before_finish` | Build workspaces before their release; encoder includes bounded CCtx, plain bytes and compression candidate |
| `cursor_bytes`, `workspace` | All source cursor blocks and the single shared decode workspace |
| `tracked_build_peak`, `tracked_retained`, `tracked_read_peak`, `tracked_total_peak` | Consumer-tracker observations; each allocation/free is recorded without limiter batching. Read peak is retained map bytes plus a nested reader tracker's peak; builders/encoder have already been freed |
| `build_ms` | Directory registration, interleaved append, encoding/compression and finish |
| `cold_decode_ms` | One lookup per block with cold cursor state, including full block decode |
| `sequential_ms`, `sequential_decodes` | All rows through the lookup API, alternating sources; correctness assertions are included in this UT timing |
| `sparse_4096_ms` | 64 ascending groups of 64 lookups, allowing rewind between groups |
| `random_4096_ms` | 4096 random lookups; unlike ordered streams, this can repeatedly decode blocks |
| `dense_build_ms`, `dense_sequential_ms` | Copy and direct-scan baseline for the same pre-generated input; not an end-to-end DENSE compaction |

These counters measure the compressed storage kernel and its reader workspaces. They exclude the benchmark's ordinary STL containers, allocator bookkeeping/RSS, the enclosing `RowIdConversion` source index/destination prefix, and other compaction/index/bitmap allocations. The enclosing map's index and prefix use tracked Doris allocators as well, but are exercised separately in the integration unit tests. Payload allocation requests now equal encoded bytes; allocator size-class rounding, fragmentation and bookkeeping remain outside these counters. Directory entries and codec/builder/cursor workspaces still count towards total and peak memory.

## Single-block performance

Steady-state 4096-row encoding and decoding costs are measured with Google Benchmark under `be/benchmark`, separate from the correctness and whole-map accounting UTs above. See [the benchmark guide](../../benchmark/rowid_conversion_benchmark.md) for Release build commands, distributions and timing boundaries.

## Correctness coverage and remaining validation

`CompressedRowIdMapTest` covers all four representations, deletion holes, skipped blocks, tails, wide ordinals/gaps, independently owned payloads across source-container growth, independent source caches, backward seeks, malformed metadata/payload, allocation failure and balanced memory accounting. A separate billion-row EMPTY-source case checks the complete block directory without allocating or iterating a billion dense entries; it is not a billion-row compression benchmark.

Selection tests cover threshold boundaries, multiple input rowsets, zero/disabled settings, overflow-safe large row counts, Duplicate Key SNII and the dense-only restrictions. Merge-reader unit tests cover compressed mappings for Duplicate Key and MoW with horizontal/vertical readers, overlapping/nonoverlapping segments and delete predicates. Storage lifecycle tests verify balanced tracking when selecting and destroying all three alternatives. Source-view tests compare joint interleaved mappings with DENSE and retain SNII's full coverage validation. Posting tests compare inline/slim/windowed results with and without deletions. An in-memory SNII merge checks nulls/postings against the rebuilt index bytes. Bitmap tests cover physical segment IDs and distinguish conversion errors from missing or visibility-filtered rows. SNII read contexts reserve one cursor block per prepared source before writing output and reuse it across terms, with one decode workspace per executing plan.

Actual local/cloud base compaction, concurrent loading/pending transactions, lock-hold time, cluster-wide memory peaks and optimized production timing remain separate validation work. The default threshold is a size-based policy, not a production-measured break-even point.
