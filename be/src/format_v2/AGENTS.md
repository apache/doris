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

## Parquet Multi-Level Filtering

- Use the [Parquet scan design](../../../docs/file-scanner-v2-parquet-scan-design.md) as the detailed
  reference. Trace each affected predicate through localization, Row Group planning, Page ranges,
  row-level residual evaluation, and final selected-column materialization.
- Apply the correctness rule at every pruning layer: discard data only when the available metadata
  proves it cannot match. Missing, malformed, truncated, unsupported, or unsafe metadata must keep
  the candidate range and fall back to a more precise layer.
- At Row Group level, check Split ownership and file-global row offsets, then verify Statistics,
  Dictionary, and Bloom pruning independently. Statistics must respect logical type conversion,
  null counts, NaN, truncated bounds, sort order, and writer validity. Dictionary pruning requires
  complete compatible encoding. Bloom may prove absence only; a hit is never a matching row.
- Preserve the cost order from cheap to expensive. Footer Statistics should reduce candidates before
  Dictionary/Bloom I/O, and ColumnIndex/OffsetIndex should be read only for surviving Row Groups.
  Flag index work whose cost is paid for ranges already known to be irrelevant.
- At Page level, require compatible ColumnIndex and OffsetIndex semantics. Check page-to-row mapping,
  first/last row boundaries, empty or all-null pages, multi-column range intersection, and conversion
  from logical `selected_ranges` to each leaf reader's physical `page_skip_plan`.
- Page skipping must keep every column reader aligned. Skipping values or pages must advance value,
  definition, and repetition state consistently, especially for nested/repeated columns whose Page
  boundaries do not align across leaves. Missing or inconsistent indexes must retain the affected
  pages instead of guessing their row coverage.
- At Row/Batch level, keep SelectionVector positions aligned with the original Row Group rows across
  dictionary-ID filters, incremental single-column predicates, residual multi-column expressions,
  delete conjuncts, and output materialization. Physical row positions used by deletes or virtual
  columns must not be renumbered after Row Group/Page pruning.
- Only split or reorder predicates when equivalence, error behavior, and evaluation state are
  preserved. OR expressions, stateful expressions, exception-sensitive operations, and predicates
  not exactly covered by an index must remain in the residual VExpr path.
- Verify lazy materialization actually avoids reading and decoding non-predicate columns for rejected
  rows while still advancing all readers correctly. Predicate columns should be read/prefetched
  first; output-column prefetch should wait for surviving rows when filtering is active.
- Review I/O and cache behavior together with pruning: register Parquet Page Cache ranges only for
  surviving projected Column Chunks, require a stable file-version key, and check FileCache,
  MergeRange, prefetch, request count, and read amplification rather than cache hit rate alone.
- Require Profile counters/timers that make each layer observable: candidates and pruned units by
  Statistics/Dictionary/Bloom, Page Index selected ranges and skipped rows/pages, raw and filtered
  rows, dictionary-row filtering, lazy-read savings, cache sources, and remote I/O/request counts.
- Require differential correctness tests against the same scan with the relevant optimization
  disabled. Cover absent/invalid statistics, missing or partial Page Index, mixed dictionary/plain
  encoding, Bloom false positives, NULL/NaN/type-conversion edges, cross-Page batches, nested and
  repeated columns, multiple Row Groups/Splits, all rows filtered, and no rows filtered.
- For performance claims, verify both pruning effectiveness and its cost with representative file
  layout, selectivity, writer, remote storage, and warm/cold cache states. A low pruning ratio on
  unsorted data is not itself a Reader regression.

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

## Review Output

- List findings first, ordered by severity. Each finding must identify the file and line, the
  reachable execution path, and the concrete incorrect outcome.
- Distinguish verified defects from open questions. If no actionable defect is found, say so and
  mention any important coverage or testing gap that remains.
