# Parquet Metadata Table-Valued Function

## Background & Motivation
DuckDB exposes multiple Parquet metadata table functions, including `parquet_metadata` (row-group and column statistics) and `parquet_schema` (logical schema). Doris exposes metadata TVFs for Iceberg, Hudi, etc., but has no built-in way to inspect ad-hoc Parquet files without creating external tables. Users resort to external tools when debugging loads or verifying statistics.

The goal is to offer a single Doris TVF, `parquet_metadata`, controlled by a `mode` parameter that emulates either DuckDB function. This keeps the SQL surface small, allows FE to plan the query via existing metadata scan plumbing, and lets BE reuse its Parquet reader to parse only file footers.

## Goals
- Implement a single TVF, `parquet_metadata`, that can emulate DuckDB's `parquet_metadata` and `parquet_schema` via a `mode` argument.
- Support all storage backends already handled by `file`, `s3`, `hdfs`, and related TVFs (local path, S3-compatible, HDFS-compatible, HTTP, etc.).
- Avoid data page reads: only touch Parquet footers and metadata blocks.
- Expose column statistics (min, max, null count, etc.) when available and keep behavior deterministic when statistics are missing.
- Integrate cleanly with the existing `MetadataScanNode` → `MetaScanner` execution path so that metadata can be fetched lazily during query execution.

## Non-goals
- Reading Parquet data rows or returning sample data (use `file` TVF or external table scans instead).
- Inline schema inference for non-Parquet files; the TVF should fail fast if supplied paths are not Parquet files.
- Automatic rewriting of column predicates into row-group filters (Block filtering happens implicitly when Doris filters the metadata rows).

## User Interface
### Syntax
```
SELECT *
FROM parquet_metadata(
    "path" = "s3://bucket/path/*.parquet",
    "mode" = "parquet_metadata"
    -- plus the same auth/storage properties accepted by file/s3/hdfs/http table functions
);
```

### Core Parameters
| Key | Required | Description |
| --- | --- | --- |
| `path` | ✅ | File path, comma-separated list, or glob matching Parquet files. Same semantics as existing file-based TVFs. |
| `mode` | ❌ (default `parquet_metadata`) | Chooses which DuckDB-style function to emulate: `parquet_metadata` (row-group/column stats) or `parquet_schema` (logical schema only). |
| Storage/auth props | ❌ | Properties already supported by `ExternalFileTableValuedFunction` (e.g. `aws.s3.access_key`, `hdfs.nameservices`, `broker`, `user`, `password`, etc.). |

### Returned Columns By View
| Mode (`mode`) | Columns (order) |
| --- | --- |
| `parquet_schema` | `file_name`, `column_name`, `column_path`, `physical_type`, `logical_type`, `repetition_level`, `definition_level`, `type_length`, `precision`, `scale`, `is_nullable` |
| `parquet_metadata` | `file_name`, `row_group_id`, `column_id`, `column_name`, `column_path`, `physical_type`, `logical_type`, `type_length`, `converted_type`, `num_values`, `null_count`, `distinct_count`, `encodings`, `compression`, `data_page_offset`, `index_page_offset`, `dictionary_page_offset`, `total_compressed_size`, `total_uncompressed_size`, `statistics_min`, `statistics_max` |

`statistics_min` and `statistics_max` are serialized to strings to avoid type explosion. When Parquet files do not carry statistics, the stat columns are `NULL`.

### Example Queries
```sql
-- Inspect logical schema
SELECT DISTINCT column_name, logical_type, is_nullable
FROM parquet_metadata(
  "path" = "/tmp/demo.parquet",
  "mode" = "parquet_schema"
);

-- Peek at row-group/column stats
SELECT file_name, row_group_id, column_name,
       statistics_min, statistics_max
FROM parquet_metadata(
  "path" = "s3://lake/orders/date=2024-01-01/*.parquet",
  "mode" = "parquet_metadata"
);
```

## Architecture Overview
The flow reuses the metadata scan pipeline already in place for Iceberg/Hudi metadata TVFs.

1. **SQL parsing**: FE resolves `parquet_metadata(...)` via `TableValuedFunctionIf.getTableFunction`. The new `ParquetMetadataTableValuedFunction` parses/validates parameters, normalizes storage properties, and enumerates files using `ExternalFileTableValuedFunction` helpers.
2. **Planning**: The TVF exposes the output schema (columns from the table above) and implements `getMetaScanRange`, encoding the storage config plus the selected `mode` inside a new `TParquetMetadataParams` field of `TMetaScanRange`.
3. **Execution**: `MetadataScanNode` distributes `TMetaScanRange` instances to BEs. `MetaScanner` recognizes `metadata_type == PARQUET` and instantiates a BE-side `ParquetMetadataReader` instead of calling FE RPCs.
4. **Reading metadata**: `ParquetMetadataReader` iterates over the file list, opens each file via `FileFactory`, reads only the footer (leveraging `vectorized::ParquetReader` and `FileMetaCache`), extracts either schema or metadata rows based on `mode`, and fills `vectorized::Block` columns. Statistics are built lazily depending on slot projection.

This approach keeps FE lightweight (no need to deserialize Parquet footers on the FE) and uses the BE code paths optimized for Parquet.

## Thrift & Planning Changes
- **`gensrc/thrift/Types.thrift`**: Add `PARQUET` to the `TMetadataType` enum.
- **`gensrc/thrift/PlanNodes.thrift`**: Introduce `struct TParquetMetadataParams` with fields such as `1: optional list<TFileRangeDesc> file_ranges`, `2: optional string mode`, `3: optional TFileType file_type`, `4: optional map<string,string> storage_properties`. Extend `TMetaScanRange` with `optional TParquetMetadataParams parquet_params`.
- Regenerate Thrift artifacts for FE/BE once the IDL changes land.

## Frontend Implementation
- **New TVF class**: `fe/fe-core/src/main/java/org/apache/doris/tablefunction/ParquetMetadataTableValuedFunction.java` extending `MetadataTableValuedFunction`.
  - Validate parameters, check user privileges on referenced resources (reuse logic from file/s3/hdfs TVFs).
  - Enumerate files and create `TFileRangeDesc` + `BrokerDesc`/storage props similar to `ExternalFileTableValuedFunction`.
  - Provide the output columns that match the selected `mode`. Columns should be defined using existing `PrimitiveType` enums; string columns can be `VARCHAR(1024)`.
  - Implement `getMetadataType()` returning `TMetadataType.PARQUET` and `getMetaScanRange()` that populates `TMetaScanRange.parquet_params`.
- **Registration**: Update `TableValuedFunctionIf.getTableFunction` to return the new TVF for `parquet_metadata` (and optionally register an alias like `parquet_meta`).
- **Column pruning helper**: If FE-side predicate push-down requires column-to-index mapping, update `MetadataTableValuedFunction.getColumnIndexFromColumnName` to cover the new metadata type.
- **MetadataGenerator guard**: Add a default branch that gracefully errors if FE ever receives `TMetadataType.PARQUET` through the legacy RPC path (should not happen once the BE path is wired, but protects older coordinators).

## Backend Implementation
### MetaScanner
- Update `MetaScanner::open` to detect `TMetadataType::PARQUET`. Instead of calling `_fetch_metadata`, instantiate a `ParquetMetadataReader` similar to the Iceberg/Paimon readers, passing tuple slots and the new parquet params.
- Ensure `_reader` is reused to fill blocks, so `_fill_block_with_remote_data` stays untouched for RPC-based metadata types.

### ParquetMetadataReader
Create `be/src/vec/exec/format/table/parquet_metadata_reader.{h,cpp}` implementing `GenericReader`:
- Constructor accepts tuple slots, runtime state/profile, and `TParquetMetadataParams`.
- Iterate over provided file ranges; for each file:
  - Acquire an `io::FileReader` via `FileFactory`, with the correct FS options derived from FE params.
  - Initialize a lightweight `vectorized::ParquetReader` to parse footers only (call `get_meta_data()` without data reads).
  - Depending on `view`, populate a vector of metadata rows:
    - `parquet_schema`: walk `tparquet::SchemaElement`s.
    - `parquet_metadata`: iterate row groups and nested columns, emitting statistics when available.
- When statistics are missing, NULL out the stat columns.
- Emit rows directly into the `Block` columns. Since the schema is fixed per view, index lookup is straightforward.
- Set `_meta_eos` once all files are processed.

### Supporting Types
- Consider adding helpers to serialize Parquet logical/converted types to strings (maybe under `vec/exec/format/parquet/parquet_utils.h`).
- Reuse `FileMetaCache` to avoid repeated footer reads when multiple fragments touch the same file.

## Error Handling & Limits
- If a supplied file path does not exist or cannot be opened, raise a user-visible error that clearly calls out the offending path.
- Non-Parquet files should trigger `Status::InternalError("<path> is not a Parquet file")`.
- For encrypted Parquet files where footer metadata cannot be decrypted, fail fast with the Parquet library's message.
- When no statistics exist, results should still return one row per column with `statistics_min`/`statistics_max` set to `NULL`.
- Since there is no column or row-group filtering parameter, every file contributes full metadata rows; FE/BE predicate pruning occurs via normal slot projection.

## Testing Strategy
1. **Thrift unit tests**: Ensure the generated classes include the new fields and default values behave as expected.
2. **FE unit tests**:
   - Parameter validation, including missing `path` or invalid `mode`.
   - Storage property parsing for S3/HDFS/local URIs.
   - Schema generation per view.
3. **BE unit tests**:
   - `ParquetMetadataReader` on single-file in-memory readers (use local temp files with known metadata).
   - Cover files with/without statistics, multiple row groups, nested columns.
4. **Integration tests**:
   - Run the TVF against local files and S3/HDFS mocks; verify outputs match expectations.
   - Negative tests: non-existent file, CSV file, lack of permissions.
5. **Regression tests**:
   - Add SQL tests under `regression-test/suites` comparing outputs to golden files.

## Rollout Notes
- The feature depends on both FE and BE changes plus Thrift regeneration; upgrading mixed-version clusters requires care. Metadata scans should fall back to “not supported” if BE/FE versions mismatch.
- Document the new TVF in the official Doris docs/examples once merged.

## Future Enhancements
1. Add optional filters (column lists, row-group ids, statistics toggles) once the base functionality stabilizes.
2. Expand `mode` options beyond `parquet_schema`/`parquet_metadata` if users need file-level summaries.
3. Push down projections more aggressively from the BE, skipping expensive string allocations for slots not requested.
4. Surface column statistics as typed values (e.g., JSON) once Doris supports richer metadata types in schema tables.
5. Support additional formats (ORC, IPC) via the same TVF pattern.
