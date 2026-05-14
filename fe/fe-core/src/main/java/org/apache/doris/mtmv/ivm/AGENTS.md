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

## Explain IVM Refresh Plans

Use `EXPLAIN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL` to inspect IVM refresh dry-run plans without changing MV/IVM persisted state. The overview form prints the normalized MV plan and every delta rewriter plan, including no-op delta plans for streams whose consumed TSO is already up to date.

Use `EXPLAIN LOGICAL PLAN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL FOR DELTA k` to inspect one specific delta rewriter plan. The typed `EXPLAIN ... PLAN` forms require `FOR DELTA k`; the untyped overview command is the form that prints all IVM plans together.

## DML Factor from binlog_op

The `dml_factor` column (+1 for inserts, −1 for deletes) drives all delta computations. It is derived in `IvmLinearDeltaStrategy.visitLogicalOlapScan()`:

- **binlog_op present**: If the base table has a column named `binlog_op` (`Column.BINLOG_OPERATION_COL`), its value follows the delete-sign convention (TinyInt: 0 = insert, 1 = delete). The dml_factor expression is `IF(binlog_op = 0, 1, -1)`.
- **binlog_op absent (fallback)**: When the column does not exist (e.g., normal tables without binlog), dml_factor falls back to the literal `1` (insert-only assumption).

Since `IvmAggDeltaStrategy` inherits `visitLogicalOlapScan()` without overriding, both simple-scan and aggregate MVs automatically use binlog_op-based dml_factor when available.

See `IvmLinearDeltaStrategy.buildDmlFactorExpr()` for the implementation.

## Row ID Generation

The `__DORIS_IVM_ROW_ID_COL__` column uniquely identifies each row/group in the MV. It is used as the join key when merging deltas into the MV's current state.

- **Simple (non-aggregate) MV**: row_id is injected during normalize (`IvmNormalizeMtmv`) as a hidden column. It is typically a monotonically increasing sequence or hash depending on the scan source.
- **Aggregate MV (grouped)**: row_id is a **null-safe** hash: `CAST(murmur_hash3_64(ifnull(cast(k1 AS VARCHAR),''), cast(isnull(k1) AS VARCHAR), ...) AS LARGEINT)`. Each group key produces two hash arguments — the coalesced value and an isnull flag — so that NULL keys never propagate NULL into the hash and groups differing only in NULL positions (e.g. `(NULL,'x')` vs `('x',NULL)`) produce distinct row_ids. VARCHAR keys skip the inner `CAST`.
- **Aggregate MV (scalar, no GROUP BY)**: row_id is the literal `0` (only one output row).

The same `IvmUtil.buildRowIdHash()` function is used by both the normalize phase and the delta rewrite phase to ensure identical row_id derivation.

## Backward Compatibility

IVM is not publicly available until July 2026. Before that date, there is no need to maintain backward compatibility with existing IVM materialized views. Breaking changes to IVM metadata, DDL format, or internal storage layout are acceptable without migration support.

## Regression Test Guide: Mocking binlog_op for Delete Operations

When writing IVM regression tests that need to simulate **delete** operations on base tables,
the base table **must** use a MOW (Merge-on-Write unique key) table. This is because in Doris's
binlog implementation, only MOW tables can produce `delete` operations in the binlog stream;
duplicate/detail tables (i.e., `DUPLICATE KEY` tables) only produce `insert` operations.

### How it works

Add a `binlog_op TINYINT` column as the **last column** in the base table schema:

```sql
CREATE TABLE base_table (
    k1 INT,
    v1 INT,
    binlog_op TINYINT
)
UNIQUE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 2
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"    -- MOW is required
);
```

### Insert vs Delete

- **Insert a row**: set `binlog_op = 0`
  ```sql
  INSERT INTO base_table VALUES (1, 10, 0);
  ```
- **Delete a row**: set `binlog_op = 1` (re-insert same key with op=1)
  ```sql
  INSERT INTO base_table VALUES (1, 10, 1);
  ```

The IVM delta strategy reads `binlog_op` to derive `dml_factor`:
`IF(binlog_op = 0, 1, -1)`, which controls whether a row contributes positively or negatively
to aggregated state.

### Important notes

1. The `binlog_op` column is part of the **base table schema**, not the MV definition.
   MV queries should reference only the business columns (exclude `binlog_op`).
2. After a delete, a "dirty" insert into the same partition is often needed to trigger
   the INCREMENTAL refresh (the partition must have new data for the refresh to run).
3. When all rows of a group are deleted, the group-level count
   (`__DORIS_IVM_AGG_COUNT_COL__`) drops to 0, causing `DELETE_SIGN=1` in the MV
   (effectively removing the group row).
4. The mock delta reads the **entire base table** on each INCREMENTAL refresh. To test
   group deletion correctly, always do a COMPLETE refresh immediately before the deletion
   scenario to reset group counts to the true physical count. Otherwise, counts may be
   inflated by prior INCREMENTALs, preventing deletion from driving the count to zero.
