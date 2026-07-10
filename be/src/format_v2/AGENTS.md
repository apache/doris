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
