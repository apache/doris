# Format V2 — Review Guide

Use this guide when reviewing changes under `be/src/format_v2/`. Apply the repository-level
instructions as well; this file adds format-v2-specific review expectations.

## Review Objective

- Report actionable correctness, data-corruption, crash, resource-lifetime, and performance
  regressions. Do not report style-only issues already enforced by the repository tooling.
- Trace the complete affected path instead of reviewing a changed function in isolation. The usual
  path crosses `TableReader`, `TableColumnMapper`, schema projection/materialization, and a concrete
  file or table reader.
- Verify claims against callers, implementations, and tests. Do not report a hypothetical failure
  unless a reachable input or state demonstrates it.

## Architecture and Interface Contracts

- Use the [FileScannerV2 design document](../../../docs/file-scanner-v2-design.md) as the
  architectural reference. Preserve the one-way responsibility chain: Scanner manages query
  integration and Split progression, `TableReader` manages table semantics, and `FileReader`
  interprets physical files. Layer boundaries take priority over incidental code reuse.
- `TableReader` owns table-level projection and column order, partition/default/virtual columns,
  table predicates and delete semantics, per-Split state, reader orchestration, and final table-block
  materialization. It may consume file schema and file-local blocks through stable contracts, but it
  must not depend on a concrete format reader's metadata structures, decoding implementation, or
  physical nested layout.
- `FileReader` owns physical schema discovery, file metadata, encoding and decoding, physical
  pruning, lazy reads, and production of file-local blocks. It must not know query-global column
  positions, table output order, partition/default/virtual-column construction, table-format
  semantics, Scanner scheduling, or Split-source policy.
- `TableColumnMapper` is the only semantic bridge between table/global and file/local column
  domains. It translates table projection and predicates plus file schema into `FileScanRequest`,
  mapping/finalize metadata, constants, and localized expressions. It must not open or read files,
  advance Splits, own reader lifecycle, or depend on concrete `TableReader`/`FileReader`
  implementations.
- Coupling between these layers is allowed only through stable, format-neutral contracts such as
  `ColumnDefinition`, `FileScanRequest`, mapper results, capability/status objects, and file-local
  blocks. Flag new concrete-class includes, downcasts, reverse callbacks, shared mutable state, or
  direct inspection of another layer's implementation details.
- Do not bypass `TableColumnMapper`: `TableReader` must not independently reproduce file-local
  column matching or position logic, and `FileReader` must not independently resolve table schema,
  defaults, partitions, virtual columns, or final table types. There must be one authoritative
  mapping for projection, predicate localization, and final materialization.
- Keep identity namespaces explicit at every boundary. Query expressions and table output use
  global identities; file requests and file blocks use local identities. A file-local ordinal,
  field ID, physical child position, or format wrapper node must never leak upward as a table/global
  identity.
- Localized predicates and delete information may be executed by a file reader only after the
  mapper/table layer has converted them into a file-local contract. The file reader may optimize
  execution but must not reinterpret or invent the table-level semantics.
- Format-specific capability or metadata needed by an upper layer should be exposed as the smallest
  neutral capability/result contract. Do not add Parquet/ORC/JNI-specific conditionals to generic
  table semantics when the decision belongs in a reader, factory, or capability interface.
- When reviewing an interface change, identify the owner layer, document input/output and lifecycle
  invariants, inspect every caller and implementation, and verify that adding another file format or
  table format would not require changes in unrelated layers. Require boundary-focused tests that
  exercise mapping and materialization independently from physical decoding where possible.

## Reader Lifecycle and Contracts

- Preserve the reader lifecycle and state transitions across initialization, schema discovery,
  opening, block production, EOF, split advancement, and close.
- Check that empty blocks, EOF, cancellation, early returns, and errors cannot skip required cleanup
  or leave stale per-file/per-split state for the next reader.
- Keep `current_rows`, block row counts, selection vectors, row positions, and `eos` consistent on
  every path, including fully filtered blocks and aggregate-pushdown paths.
- Check ownership and lifetime of file readers, column readers, blocks, columns, expression
  contexts, callbacks, and objects referenced through raw pointers or views.

## Schema Mapping and Materialization

- Keep table/global identities and positions distinct from file/local identities and positions.
  Review uses of `GlobalIndex`, `LocalColumnId`, `LocalIndex`, `ConstantIndex`, and nested child IDs
  for accidental namespace or ordinal mixing.
- Verify mapping by field ID, name, and position against the intended table format. Missing columns,
  partition columns, defaults, and virtual columns must be materialized with the correct type,
  nullability, and row count.
- For schema evolution, check field additions, removals, renames, reordering, type changes, and
  nullable/non-nullable transitions.
- For `STRUCT`, `ARRAY`, and `MAP`, verify recursive projection and reconstruction, child ordering,
  file-local IDs, offsets, null maps, and empty collections. Remember that semantic Doris trees and
  physical file-format trees may have different shapes.
- Check that casts and defaults preserve Doris semantics for overflow, precision/scale, timezone,
  decimal, date/time, string, and nullable values.

## Filtering, Deletes, and Pushdown

- Predicate columns and lazily materialized non-predicate columns must refer to exactly the same
  rows after filtering. Review selection-vector reuse, skipped row groups/pages, and row-position
  accounting together.
- A pushed-down predicate, statistic, dictionary filter, bloom filter, or aggregate must be
  semantically equivalent to evaluating it after materialization. Unsupported or unsafe cases must
  follow the designed fallback or return an explicit error; they must not silently change results.
- Review equality deletes, position deletes, table-format predicates, and generated row-location
  columns for ordering, null semantics, type conversion, file identity, and absolute row position.
- For Iceberg, Hive, Hudi, Paimon, Remote Doris, and JNI-backed readers, verify that the table-level
  wrapper preserves the underlying file reader's schema, filtering, split, and EOF contracts.

## Format-Specific Boundaries

- Confirm file-format dispatch and capability checks match the actual implementation. New behavior
  must not accidentally route unsupported formats or table modes into a reader that cannot handle
  them.
- For Parquet and ORC, review physical-to-semantic schema conversion, nested levels/offsets,
  statistics validity, page or stripe pruning, and corrupt/truncated input handling.
- For CSV, text, and JSON, review record boundaries, escaping/quoting, malformed rows, encoding,
  column count, and partial-buffer behavior across reads.
- For JNI readers, review local/global reference lifetime, exception propagation, type conversion,
  thread attachment assumptions, and cleanup on partial initialization.

### Parquet Native Decode Kernel

- Keep new production integration under `be/src/format_v2/parquet/`. Doris v1 is the behavior and
  performance baseline, but v2 owns an independent page/encoding reader and must not call the v1
  `ParquetColumnReader`. Do not modify `be/src/format/parquet/` for a v2 decoder change. Reimplement
  the required behavior under the v2 tree and keep v1 unchanged so differential correctness and
  performance results remain meaningful.
- Keep the native decode boundary independent of both Arrow descriptors/builders and table-schema
  objects. A Column Chunk schema contract should contain only immutable physical type, fixed width,
  and Dremel-level thresholds. Review constructor arguments and stored references for ownership and
  lifetime; metadata owned by a temporary schema adapter must not escape into a persistent reader.
- Treat selection positions as logical Row Group rows, including null rows. Selection indices must
  be sorted, unique, and bounded by the batch's logical row count. Dense identity, empty selection,
  and fragmented selection must have explicit representations and tests; do not silently mix row
  ordinals with non-null value ordinals or dictionary IDs.
- Verify the three decode counts separately: logical rows consumed, encoded non-null payload values
  consumed, and output values materialized. Null and filtered-null runs consume no payload;
  selected and filtered non-null runs both consume payload. Every page transition, skip, error, and
  end-of-batch path must leave all three cursors aligned for the next call.
- A flat scalar fast path may run only when `max_repetition_level == 0`. Repeated leaves require a
  definition/repetition-level plan that identifies parent-row boundaries, empty and null
  collections, null ancestors, and rows spanning pages. Choose one physical leaf as the parent
  shape owner; sibling leaf streams advance over the same parent-row range and validate their
  payload counts instead of independently redefining the parent shape.
- The old Arrow value-reader hierarchy (`ParquetLeafBatch`, scalar/list/map/struct readers, and the
  nested load/build/consume protocol) has been removed. Do not reintroduce an intermediate decoded
  batch or a stateful load-before-build phase. Ordinary predicate/output scans construct
  `NativeColumnReader`, consume compressed page data through the native decoder, and append directly
  into the final Doris column.
- Keep logical schema changes distinct from physical decoding. Identical types, integer changes,
  FLOAT-to-DOUBLE widening, decimal precision/scale changes, and string-family changes should
  materialize through the target SerDe directly. A less common logical cast may use the generic
  `ColumnTypeConverter` with a reusable source Doris column, but must not revive
  `PhysicalToLogicalConverter` or expose a decoder-owned value batch.
- `CountColumnReader` uses the v2 native `LevelReader`; it selects one representative leaf (the key
  for MAP) and advances only definition/repetition levels. It exposes no value API and must not be
  reused as a scan reader or expanded into a fallback path.
- Build ARRAY/MAP/STRUCT parent boundaries, offsets, nulls, and child payload spans in one traversal
  of the owning leaf's levels. For example, `[[1, 2], NULL, []]` must yield entry counts
  `[2, 0, 0]` and parent nulls `[0, 1, 0]` without rescanning that leaf. MAP key levels own entry
  existence; value levels validate against that shape. STRUCT siblings validate parent-row
  alignment. If every projected STRUCT child is missing, consume only a retained physical leaf's
  levels and never materialize its payload into a temporary Doris column.
- Do not size level or selection scratch from a 16-bit batch-row assumption. A repeated parent row
  can contain more level entries than the requested parent-row batch. Split large runs without
  changing alternation or row-boundary semantics, and check overflow before narrowing counts.
- Decoder dispatch must reject an incompatible physical type, encoding, or type length explicitly.
  Review PLAIN, dictionary/RLE, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY,
  DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT, BOOLEAN RLE, and level RLE/bit-packed paths for identical
  selection and malformed-input behavior. Page V1 and V2 must feed the same decoder contract after
  their different level/decompression layouts are parsed.
- Keep every index coordinate domain explicit: table-local column ID, physical leaf-column ID,
  Row Group ID, data-page ordinal, OffsetIndex row ordinal, logical batch-row ordinal, non-null
  payload ordinal, and dictionary-entry ID are different types of identity. Data-page ordinals must
  exclude dictionary pages consistently. Dictionary-entry bitmaps are local to one Column Chunk
  dictionary and cannot be reused after a Row Group, dictionary, or encoding transition.
- Review index composition, not only each index in isolation. Row Group statistics, dictionary,
  Bloom, ColumnIndex/OffsetIndex, page cache registration, page skip plans, SelectionVector, and
  lazy column cursors must describe the same surviving logical rows. Missing or unusable optional
  indexes retain candidates; structurally inconsistent indexes or out-of-range IDs return an
  explicit corruption error. A mixed dictionary/plain Column Chunk must leave dictionary-ID
  filtering before any cursor is consumed.
- Decoder owns encoded-stream parsing and cursor movement only. `DataTypeSerDe` owns Parquet
  physical/logical interpretation and writes directly into Doris columns. Dictionary pages are
  materialized once through the same SerDe and data pages expose only validated dictionary indices.
  Decimal and FIXED_LEN_BYTE_ARRAY paths must validate byte width, endianness, sign extension,
  precision, and scale. Date/time and INT96 conversion must preserve timezone and overflow semantics.
- Do not add an Arrow runtime fallback. Once an ordinary v2 scan selects its native Parquet reader,
  unsupported physical types, encodings, page layouts, or malformed inputs return an explicit
  status. Arrow may be used by metadata planning and as a test oracle; no Arrow array, builder,
  RecordReader, or metadata lifetime belongs in data-page value or level materialization.
- Reuse decoder, SerDe, null-map, selection-range, binary-value, level, and builder scratch across
  batches. String-like decoders should gather selected `StringRef` values and append once per batch,
  rather than allocate or grow the destination once per run. Scratch capacity may grow to a bounded
  high-water mark but must be reset logically between pages, Row Groups, files, and errors.
- Adaptive batch sizing must measure completed Doris output rows/bytes and must not recreate native
  readers, builders, or scratch when only the row cap changes. Compare the v1 and v2 lifecycle:
  probe batches must not turn persistent setup into a per-batch cost or amplify highly fragmented
  selection work.
- Footer and metadata caching must key on stable file identity and cache the serialized footer plus
  parsed native metadata at the same lifecycle as v1. Never reuse metadata when path, file size,
  modification/version identity, encryption state, or schema-affecting options differ. Cache misses
  and uncacheable identities must remain correct without a fallback to stale entries.
- Page-cache behavior must match v1 for cache key, stable file identity, registered byte range,
  compressed/decompressed entry kind, checksum/decompression ownership, subrange coverage,
  invalidation, admission, and fallback I/O. Any intentional difference needs benchmark and memory
  evidence showing it is no worse for v1 workloads. Cache lookup must never alter page ordinal,
  decoder, level, or dictionary cursor state.
- Apply v1's MergeRange decision to the native data-page reader, after metadata/dictionary probes
  finish. Predicate and lazy readers for one Row Group must share one ordered-range wrapper, and
  native per-column prefetch must be disabled while that wrapper is active. Never allocate one
  MergeRange buffer per leaf; wide complex projections would multiply its bounded scratch memory.
- Preserve observability inside aggregate counters. `TotalBatches` must be decomposable into probe,
  dense, selected, empty, page-crossing, and nested/fragmented work where relevant; decode, level,
  selection, conversion, allocation, and materialization time must remain attributable without
  adding per-row timer overhead.
- Profile index attempts, successes, conservative fallbacks, and corrupt rejections separately for
  statistics, dictionary, Bloom, ColumnIndex/OffsetIndex, and page skipping. Footer/page/file/
  condition-cache counters must expose requests, hits, misses, writes/admissions, bytes, wait/I/O
  time, and bypass reasons with semantics aligned to v1. A lower total timer without its internal
  work counters is not sufficient observability.

## Detailed FileReader Review Guides

- Before reviewing any FileReader implementation, index, predicate path, cache, or virtual column,
  read and apply the common checklist in
  [FileScannerV2 Code Review Guide](../../../docs/file-scanner-v2-code-review-guide.md).
- For Parquet changes, also apply the guide's Parquet checklist and read
  [FileScannerV2 Parquet Scan Design](../../../docs/file-scanner-v2-parquet-scan-design.md).
- For ORC changes, also apply the guide's ORC SARG and index checklist.
- These detailed guides are mandatory review instructions for their scope, not optional background
  reading. Report any conflict between an implementation and the documented layer contract.

## External Compatibility

- Treat the external table-format specification and the behavior of supported external writers as
  compatibility inputs. Do not assume Doris-generated fixtures or an existing Doris implementation
  are authoritative when they conflict with the external contract.
- Do not require the external representation to behave like Doris internal storage. Verify the
  complete translation from external semantics, through the format-v2 adapter, to the observable
  Doris query result. Any intentional semantic difference must be documented and tested.
- Identify the compatibility matrix affected by a change: lake format and version, physical file
  format and version, producing engine/writer, feature flags, encoding, compression codec, and
  metadata version. Avoid fixes that only work for one writer's representation.
- Preserve backward compatibility with files and metadata produced by supported older versions.
  For newer or unknown versions and features, follow the external specification's compatibility
  rules; do not guess or silently reinterpret metadata.
- Review snapshot selection, time travel, manifest and partition evolution, schema and field IDs,
  name and case matching, file identity, path normalization, and partition value decoding according
  to the relevant lake-format semantics.
- Review writer-dependent physical representations, including Parquet logical annotations and
  legacy encodings, ORC type attributes, timestamps and timezones, decimals, signedness, CHAR
  padding, nested LIST/MAP layouts, null counts, NaN values, statistics, page/stripe indexes, and
  optional or missing metadata.
- Capability detection and dispatch must happen before relying on a feature. Unsupported table
  modes, metadata features, encodings, or semantic conversions must use the explicitly designed
  fallback or return a clear error; they must never produce plausible but incorrect rows.
- Predicate, delete, statistics, and aggregate pushdown must return the same observable result as
  reading and evaluating the external data without that optimization, including NULL, NaN,
  timezone, collation/case, overflow, and precision edge cases.
- Check that a compatibility fix for one combination does not change existing behavior for other
  lake formats, file formats, writers, or versions sharing the same abstraction.
- Require interoperability coverage using artifacts produced by representative external systems
  such as Spark, Hive, Flink, or Trino when applicable. Prefer differential tests against a
  non-pushdown path or the source system's expected result; do not rely only on files synthesized by
  Doris test code.
- Each compatibility finding should state the affected external system or specification, versions
  or writer variants, reachable input, Doris result, and expected result.

## Performance and Observability

- Treat per-row allocation, expression cloning, virtual dispatch, repeated schema work, unnecessary
  column copies, and loss of lazy reads or pruning in hot paths as potential regressions.
- Check I/O ranges, caching, decompression, and batch sizing for accidental read amplification or
  unbounded memory growth.
- Preserve profile counters and timers when control flow changes so filtered rows, bytes, reader
  creation, and pushdown behavior remain diagnosable.

## Tests

- Require focused BE unit tests under `be/test/format_v2/`, following the source subdirectory when
  possible. Add regression coverage when correctness depends on the FE-to-BE request or external
  table integration.
- Include the relevant edge cases: empty input, all rows filtered, multiple blocks/splits/files,
  EOF with and without output rows, nulls, missing/default columns, reordered or nested fields, and
  malformed input.
- For bug fixes, require a test that fails for the original reachable path and validates the result,
  row count, or explicit error after the fix.
- For native Parquet work, require a matrix across physical/logical types, every supported encoding,
  Page V1/V2, required/optional/repeated levels, dictionary fallback, page and batch boundaries,
  dense/empty/fragmented selections, null placement, malformed/truncated input, and files from
  representative external writers. Include focused cursor-invariant tests and end-to-end nested
  reconstruction tests; a scalar happy-path benchmark is not sufficient coverage.
- Performance-sensitive decoder changes need a reproducible comparison against v1 and a relevant
  external baseline such as DuckDB. Report data shape, encoding, selectivity, null/cardinality
  distribution, compression, storage path, batch policy, warm/cold cache state, CPU time, rows/s,
  bytes/s, allocation behavior, and Profile counter deltas.

## Review Output

- List findings first, ordered by severity. Each finding must identify the file and line, the
  reachable execution path, and the concrete incorrect outcome.
- Distinguish verified defects from open questions. If no actionable defect is found, say so and
  mention any important coverage or testing gap that remains.
