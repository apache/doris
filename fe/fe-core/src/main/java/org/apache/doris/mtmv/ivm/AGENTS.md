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
- `fe/fe-core/src/test/java/org/apache/doris/catalog/DropMaterializedViewTest.java`

When running IVM FE unit tests, always include
`fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/CreateMTMVCommandTest.java`;
it covers MTMV creation paths that can be affected by IVM normalize/metadata changes.

New test classes are added over time — always check the directories above for the current set.

## Important: Binlog/Stream Status

Stream-based binlog capture is implemented for IVM incremental refresh:

- Delta scans use `LogicalOlapTableStreamScan` with `isIncrementalScan=true`.
- `IvmDeltaRewriteHelper.isIncrementalDeltaScan()` identifies delta scans.
- Consumption positions are managed by `OlapTableStream` per-partition offsets (`partitionOffset`).
- On `CREATE MTMV ... REFRESH INCREMENTAL`, an internal stream
  `__doris_ivm_{mvId}_{baseTableName}` is auto-created for each base table.
- `IvmDeltaRewriter.replaceWithDelta()` constructs `LogicalOlapTableStreamScan`
  with `OlapTableStreamWrapper`, flowing through stream → `RowBinlogTableWrapper` → binlog scan.
- `dml_factor` is derived from `__DORIS_BINLOG_OP__`:
  `ROW_BINLOG_APPEND(0)/UPDATE(1) → +1, DELETE(2) → -1`.

**TSO snapshot reads** (`scan.withTso(tso)`) are mocked — BE does not support TSO-based
snapshot reads yet.

## Explain Refresh Plans

Use `EXPLAIN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL` to inspect the incremental IVM refresh
plan without changing MV/IVM persisted state. The overview form prints the normalized MV plan and
the rewritten incremental refresh plan.

Use `EXPLAIN REFRESH MATERIALIZED VIEW mv_name COMPLETE` to inspect the complete MTMV refresh plan
through the same dry-run path used by normal complete refresh planning.

Typed explain forms continue to work on both refresh modes, for example:

```sql
EXPLAIN ANALYZED PLAN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL;
EXPLAIN LOGICAL PLAN PROCESS REFRESH MATERIALIZED VIEW mv_name COMPLETE;
```

`EXPLAIN REFRESH` currently supports only `INCREMENTAL` and `COMPLETE`.

## DML Factor from `__DORIS_BINLOG_OP__`

The `dml_factor` column (+1 for inserts/updates, −1 for deletes) drives all delta computations.
It is derived in `IvmLinearDeltaHandler.buildDmlFactorExpr()`:

- Reads the real `__DORIS_BINLOG_OP__` binlog column (type `BIGINT`):
  - `ROW_BINLOG_APPEND(0)` or `ROW_BINLOG_UPDATE(1)` → `dml_factor = +1`
  - `ROW_BINLOG_DELETE(2)` → `dml_factor = -1`
- Expression: `IF(__DORIS_BINLOG_OP__ < 2, 1, -1)`
- The `__DORIS_BINLOG_OP__` column is output by `LogicalOlapTableStreamScan.computeOutput()`
  when `isIncrementalScan=true`, populated by the BE binlog scan via `RowBinlogTableWrapper`.

## Row ID Generation

The `__DORIS_IVM_ROW_ID_COL__` column uniquely identifies each row/group in the MV. It is used as the join key when merging deltas into the MV's current state.

- **Simple (non-aggregate) MV**: row_id is injected during normalize (`IvmNormalizeMtmv`) as a hidden column. It is typically a monotonically increasing sequence or hash depending on the scan source.
- **Aggregate MV (grouped)**: row_id is a **null-safe** hash: `CAST(murmur_hash3_64(ifnull(cast(k1 AS VARCHAR),''), cast(isnull(k1) AS VARCHAR), ...) AS LARGEINT)`. Each group key produces two hash arguments — the coalesced value and an isnull flag — so that NULL keys never propagate NULL into the hash and groups differing only in NULL positions (e.g. `(NULL,'x')` vs `('x',NULL)`) produce distinct row_ids. VARCHAR keys skip the inner `CAST`.
- **Aggregate MV (scalar, no GROUP BY)**: row_id is the literal `0` (only one output row).

The same `IvmUtil.buildRowIdHash()` function is used by both the normalize phase and the delta rewrite phase to ensure identical row_id derivation.

## Backward Compatibility

IVM is not publicly available until October 2026. Before that date, there is no need to maintain backward compatibility with existing IVM materialized views. Breaking changes to IVM metadata, DDL format, or internal storage layout are acceptable without migration support.

## Regression Test Guide: Binlog Operations

When writing IVM regression tests, base tables should use MOW (Merge-on-Write unique key) tables
for delete testing. The real binlog stream captures the `__DORIS_BINLOG_OP__` column automatically
— no mock column is needed in the base table schema.

### Base Table Setup

Base tables with MOW primary key can generate `INSERT`, `UPDATE`, and `DELETE` operations
in the binlog stream. The `CREATE MTMV ... REFRESH INCREMENTAL` command auto-creates an
internal stream for each base table.

### Important Notes

1. MV queries should reference only the business columns.
2. After a delete, a "dirty" insert into the same partition is often needed to trigger
   the INCREMENTAL refresh (the partition must have new data for the refresh to run).
3. When all rows of a group are deleted, the group-level count
   (`__DORIS_IVM_AGG_COUNT_COL__`) drops to 0, causing `DELETE_SIGN=1` in the MV.
4. TSO snapshot reads are not yet supported by BE; snapshot scans are still mock
   (read the full table). This means snapshot TSO bindings (`withTso(tso)`) are
   placeholders and will produce correct results once BE support is added.
