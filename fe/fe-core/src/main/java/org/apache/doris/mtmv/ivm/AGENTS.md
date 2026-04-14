# AGENTS.md — IVM (Incremental View Maintenance)

## Recommended Test Suite

It is strongly recommended to run the following tests before committing IVM changes.

### Regression Tests

Run all IVM regression suites under `regression-test/suites/mtmv_p0/ivm/`:

```bash
./run-regression-test.sh --run -d mtmv_p0/ivm
```

This runs every suite in the `ivm` subdirectory. New IVM suites are added over time — the `-d mtmv_p0/ivm` flag automatically picks them up.

### FE Unit Tests

Run all IVM-related FE unit tests. The relevant test files are located in:

- `fe/fe-core/src/test/java/org/apache/doris/mtmv/ivm/` — IVM core tests
- `fe/fe-core/src/test/java/org/apache/doris/nereids/rules/rewrite/IvmNormalizeMtmvTest.java`
- `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/CreateMTMVCommandTest.java`
- `fe/fe-core/src/test/java/org/apache/doris/catalog/ShowCreateMTMVTest.java`

New test classes are added over time — always check the directories above for the current set.

## Important: Binlog/Stream Not Ready

The binlog/stream-based incremental data capture is **not yet implemented**. During IVM incremental refresh, the base table delta is **mocked** — it reads the full base table data as if it were the incremental change. This means:

- `IvmStreamRef` and `StreamType` are placeholder structures
- The delta executor currently treats the entire base table scan as the delta input
- Real incremental behavior (capturing only inserted/deleted rows since last refresh) will require binlog/stream integration in the future
- All current regression and unit tests operate under this mock-full-scan assumption

## DML Factor from binlog_op

The `dml_factor` column (+1 for inserts, −1 for deletes) drives all delta computations. It is derived in `IvmSimpleScanDeltaStrategy.visitLogicalOlapScan()`:

- **binlog_op present**: If the base table has a column named `binlog_op` (`Column.BINLOG_OPERATION_COL`), its value follows the delete-sign convention (TinyInt: 0 = insert, 1 = delete). The dml_factor expression is `IF(binlog_op = 0, 1, -1)`.
- **binlog_op absent (fallback)**: When the column does not exist (e.g., normal tables without binlog), dml_factor falls back to the literal `1` (insert-only assumption).

Since `IvmAggDeltaStrategy` inherits `visitLogicalOlapScan()` without overriding, both simple-scan and aggregate MVs automatically use binlog_op-based dml_factor when available.

See `IvmSimpleScanDeltaStrategy.buildDmlFactorExpr()` for the implementation.

## Row ID Generation

The `__DORIS_IVM_ROW_ID_COL__` column uniquely identifies each row/group in the MV. It is used as the join key when merging deltas into the MV's current state.

- **Simple (non-aggregate) MV**: row_id is injected during normalize (`IvmNormalizeMtmv`) as a hidden column. It is typically a monotonically increasing sequence or hash depending on the scan source.
- **Aggregate MV (grouped)**: row_id is a **null-safe** hash: `CAST(murmur_hash3_64(ifnull(cast(k1 AS VARCHAR),''), cast(isnull(k1) AS VARCHAR), ...) AS LARGEINT)`. Each group key produces two hash arguments — the coalesced value and an isnull flag — so that NULL keys never propagate NULL into the hash and groups differing only in NULL positions (e.g. `(NULL,'x')` vs `('x',NULL)`) produce distinct row_ids. VARCHAR keys skip the inner `CAST`.
- **Aggregate MV (scalar, no GROUP BY)**: row_id is the literal `0` (only one output row).

The same `IvmUtil.buildRowIdHash()` function is used by both the normalize phase and the delta rewrite phase to ensure identical row_id derivation.

## Backward Compatibility

IVM is not publicly available until July 2026. Before that date, there is no need to maintain backward compatibility with existing IVM materialized views. Breaking changes to IVM metadata, DDL format, or internal storage layout are acceptable without migration support.
