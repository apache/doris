# Doris New Parquet Reader Design And Status

This document describes the design and current implementation status of the new
Parquet reader under `be/src/format/new_parquet/`.

The goal of this PR is to build a file-local Parquet reader based on Arrow C++
Parquet core APIs while keeping Doris-owned `Block` and `Column` as the scan
output. It does not replace the old `vparquet` path yet.

## Design Goals

- Use Arrow C++ Parquet core APIs for Parquet file metadata, row group and column
  decoding.
- Keep `doris::parquet::ParquetReader` as a file-local reader.
- Do not use `parquet::arrow::FileReader`, `arrow::RecordBatch`,
  `arrow::Table` or `arrow::Array` as the scan output path.
- Do not put Iceberg table schema, schema evolution, default columns, generated
  columns or partition columns into `ParquetReader`.
- Keep table schema mapping and filter localization in the reader/table layer,
  especially `TableColumnMapper`.
- Let the new implementation live in `be/src/format/new_parquet/` so it can
  evolve independently from the old `be/src/format/parquet/` implementation.

## Layering

```text
TableReader / IcebergTableReader
    -> TableColumnMapper
    -> reader::FileScanRequest
    -> doris::parquet::ParquetReader
    -> DorisRandomAccessFile
    -> parquet::ParquetFileReader
    -> parquet::RowGroupReader
    -> parquet::ColumnReader / parquet::internal::RecordReader
    -> Doris Block / Column
```

`ParquetReader` only consumes file-local information:

- file-local schema fields;
- file-local projection columns;
- file-local predicate columns;
- file-local `ColumnPredicate` and `VExprContext` filters.

Any table-level cast, default value, generated column, partition value, Iceberg
field id mapping or schema evolution rule must be handled before or after the
file reader layer.

## Code Layout

```text
be/src/format/new_parquet/parquet_reader.h
be/src/format/new_parquet/parquet_reader.cpp
be/src/format/new_parquet/column_reader.h
be/src/format/new_parquet/column_reader.cpp
be/src/format/new_parquet/parquet_statistics.h
be/src/format/new_parquet/parquet_statistics.cpp
```

`parquet_reader.*` owns file open, schema export, scan state, row group
scheduling, predicate-first reading and output block assembly.

`column_reader.*` owns Doris column assembly for one projected Parquet field. It
wraps Arrow Parquet column-level APIs and converts decoded values into Doris
columns.

`parquet_statistics.*` owns row group statistics pruning. Future page index,
bloom filter and dictionary pruning should also live there rather than being
mixed into the main scan loop.

## Main Components

### DorisRandomAccessFile

`DorisRandomAccessFile` adapts Doris `io::FileReader` to
`arrow::io::RandomAccessFile`.

It only handles random IO and file size lookup. It does not parse Parquet schema,
does not evaluate filters, and does not carry table-level semantics.

### ParquetReaderScanState

`ParquetReaderScanState` is an internal scan state stored in
`parquet_reader.cpp`. It tracks:

- Arrow random access file;
- Arrow Parquet file reader and metadata;
- Parquet schema descriptor;
- selected row groups;
- current row group reader;
- current row group row offset;
- projected file columns;
- predicate columns and non-predicate output columns;
- current row group column readers.

This state is intentionally private to the Parquet reader implementation.

### ParquetColumnReader

`ParquetColumnReader` is Doris's file-local column reader abstraction. It is not
the same as Arrow's `parquet::ColumnReader`.

Current implementations:

- `PrimitiveColumnReader`
- `StructColumnReader`

`PrimitiveColumnReader` supports both the existing Arrow
`parquet::TypedColumnReader` path and the new
`parquet::internal::RecordReader` path for selected primitive reads.

`StructColumnReader` currently supports basic required struct assembly by
recursively reading child readers. Complex nested selective materialization is
not complete.

### ParquetColumnReaderFactory

`ParquetColumnReaderFactory` creates Doris column readers from the current row
group's Arrow Parquet readers and the file-local `ParquetColumnSchema`.

The factory centralizes reader construction so later work can add reader
options, Dremel assemblers, selected-read policies and cache state without
passing those details through free functions.

### ParquetStatisticsUtils

`ParquetStatisticsUtils` compiles file-local predicates into Parquet column
predicate plans and evaluates row group metadata conservatively.

It only understands Parquet file-local schema and Doris `ColumnPredicate`. It
does not know Iceberg schema, slot descriptors or table schema mapping.

## Scan Request Semantics

The new reader consumes `reader::FileScanRequest`.

Important fields:

- `predicate_columns`: file-local columns that must be read first to evaluate
  filters.
- `non_predicate_columns`: file-local projection columns that are only read
  after selection is known.
- `projected_columns`: file-local columns that should appear in the output block.
- `local_filters`: file-local filters produced by the table layer.
- `reader_expression_map`: fallback expressions for filters that cannot be
  represented as direct file-local predicates.

The output block is still file-local. It is not a table/global schema block.

## Predicate Pushdown

Doris new reader uses two existing filter representations:

- `ColumnPredicate`: structured single-column predicates, used for row group
  statistics pruning and decoded value filtering.
- `VExprContext`: expression filters, used for fallback expression evaluation
  and residual filters.

Current implementation status:

- row group min/max pruning is wired through `parquet_statistics.*`;
- supported stats types include boolean, int32, int64, float, double and
  string/binary;
- unsupported stats, missing stats or unsafe cases keep the row group;
- `IS NULL` and `IS NOT NULL` pruning use null count when available;
- page index, bloom filter and dictionary pruning are not implemented yet.

Correctness rule: pruning must be conservative. If the reader cannot prove that
a row group cannot match, it must keep the row group.

## Lazy Materialization

The scan loop follows a predicate-first model:

1. Read predicate columns.
2. Evaluate `ColumnPredicate` and build a selection vector.
3. If a predicate column is also projected, reuse the decoded predicate column.
4. Read non-predicate output columns using the selection.
5. Assemble the file-local output block in projected column order.

The current selected-read implementation uses Arrow Parquet
`parquet::internal::RecordReader` for supported primitive columns.

Why this is needed:

- `parquet::TypedColumnReader::Skip` skips physical values, not SQL rows.
- For nullable columns, row count and physical value count differ.
- For repeated/nested columns, a row can contain multiple physical values.

`RecordReader::SkipRecords` and `RecordReader::ReadRecords` provide row-level
movement. Doris compresses the selection vector into row ranges and alternates
skip/read operations.

Current support:

- selected read for primitive boolean, int32, int64, float and double when the
  RecordReader path is available;
- fallback path reads the whole batch and filters it when selected read is not
  supported;
- output columns are skipped when the selection is empty;
- predicate columns are reused when they are also projected.

Limitations:

- `parquet::internal::RecordReader` is an Arrow internal/experimental API, so it
  must remain hidden behind Doris `ParquetColumnReader`;
- string, decimal and timestamp selected reads still need broader validation;
- nested selected materialization needs a dedicated Dremel assembler.

## Type Coverage

Currently implemented:

- flat required and nullable boolean;
- flat required and nullable int32 / int64;
- flat required and nullable float / double;
- BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY string/binary with Doris-owned memory;
- decimal with precision up to 38 for int32, int64, byte array and fixed-length
  byte array physical encodings;
- INT64 timestamp millis and micros into Doris `DateTimeV2`;
- basic required struct assembly.

Not implemented or incomplete:

- INT96 timestamp;
- nanosecond timestamp;
- TIMESTAMPTZ semantics;
- DECIMAL256;
- nullable struct;
- list and map;
- complex column pruning;
- complex column lazy materialization.

## Current Implementation Status

Implemented in this PR:

- new `new_parquet` module;
- Arrow-backed Parquet file open and metadata read;
- file-local schema export;
- row group scheduling;
- projected leaf reader creation;
- primitive column decoding into Doris columns;
- string, decimal and INT64 timestamp decoding;
- basic struct reader;
- row group statistics pruning skeleton and initial implementation;
- predicate-first scan flow;
- primitive RecordReader-backed selected materialization;
- Debug BE build fixes.

Validated:

- `git diff --check`;
- `BUILD_TYPE=DEBUG ./build.sh --be` on
  `fedora:/home/socrates/code/doris`.

## Future Work

Near term:

- add unit tests for primitive required/nullable selected reads;
- validate selection edge cases: empty selection, full selection, sparse
  selection and highly fragmented ranges;
- add a selection-rate policy so dense selections can fall back to whole-batch
  read plus filter;
- stabilize string, decimal and timestamp selected reads;
- keep Arrow internal API usage isolated in `column_reader.*`.

Mid term:

- implement page index pruning in `parquet_statistics.*`;
- implement bloom filter pruning for equality predicates;
- add dictionary-aware filtering where Arrow exposes enough metadata safely;
- expand complex type assembly for nullable struct, list and map;
- add tests for row group pruning correctness and unsupported-type fallback.

Long term:

- support nested column pruning;
- support nested lazy materialization;
- support page-level row range selection;
- integrate the new file-local reader with table readers after the API boundary
  is stable;
- keep old `vparquet` compatibility until the new path is functionally complete.

## Key Rule

`ParquetReader` must remain a file-local reader. If a feature requires table
schema, Iceberg schema evolution, partition values, default/generated columns or
final table output semantics, it belongs in `TableColumnMapper` or
`TableReader`, not in `be/src/format/new_parquet/`.
