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
