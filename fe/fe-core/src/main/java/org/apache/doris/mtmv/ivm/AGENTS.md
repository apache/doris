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
- `fe/fe-core/src/test/java/org/apache/doris/nereids/rules/analysis/IvmNormalizeMTMVTest.java`
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
- `IvmDeltaRewriter` recursively builds one merged delta relation. `IvmDeltaRewriteState`
  owns `Map<OlapTable, OlapTableStream>` and creates `LogicalOlapTableStreamScan` only when
  a leaf scan contributes delta rows.
- `dml_factor` is derived from `__DORIS_STREAM_CHANGE_TYPE__`:
  `APPEND/UPDATE_AFTER → +1, DELETE/UPDATE_BEFORE → -1`.

Snapshot rewrite now uses two explicit logical forms:

- pre snapshot: `LogicalOlapTableStreamScan` in snapshot mode (`stream@snapshot`)
- post snapshot: ordinary `LogicalOlapScan`

IVM no longer uses mock `scan.withTso(tso)` bindings in incremental refresh rewrite.

## Recursive Delta Rewrite

- `IvmNormalizeMTMV` rejects an input `LogicalOlapTableStreamScan`; normalized MV queries start
  from ordinary `LogicalOlapScan` nodes.
- `IvmDeltaRewriteVisitor` returns `Optional<IvmDeltaRewriteResult>`. An empty result means the
  subtree has no pending delta.
- For an inner join, the left delta joins the right pre snapshot and the right delta joins the
  left post snapshot. Non-empty child contributions are combined with `UNION ALL`.
- `preSnapshot(plan)` and `postSnapshot(plan)` rewrite scans on demand and then fresh-copy the plan.
- Delta rows carry `dml_factor` and `sequence` together. `sequence` is allocated at delta scan
  leaves and propagated upward; `maxSeqSuffix` is metadata used by outer-join padding rows.
- Aggregate IVM output uses the refresh-version-encoded `maxSeqSuffix` from its child delta. Aggregate refresh
  does not require row event ordering, but it must retain the normal sequence-column contract.

## Explain Refresh Plans

Use `EXPLAIN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL` to inspect the incremental IVM refresh
plan without changing MV/IVM persisted state. By default, the rewritten plan matches the current
refresh execution scope and only contains streams with pending data; it may be an empty relation
when all streams are up to date.

Use `EXPLAIN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL WITH ALL STREAMS` to inspect the complete
incremental rewrite shape, including streams that are currently up to date. This is a structural
Explain mode and does not change the persisted MV/IVM state. The Explain output format is unchanged;
stream offsets and lag are not included in the plan output and should be inspected with the stream
metadata commands instead.

Use `EXPLAIN REFRESH MATERIALIZED VIEW mv_name COMPLETE` to inspect the complete MTMV refresh plan
through the same dry-run path used by normal complete refresh planning.

Typed explain forms continue to work on both refresh modes, for example:

```sql
EXPLAIN ANALYZED PLAN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL;
EXPLAIN LOGICAL PLAN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL WITH ALL STREAMS;
EXPLAIN LOGICAL PLAN PROCESS REFRESH MATERIALIZED VIEW mv_name COMPLETE;
```

`EXPLAIN REFRESH` supports only `INCREMENTAL` and `COMPLETE`; partition-scoped refresh syntax is
not supported by Explain Refresh. Partition-scoped refresh remains available through ordinary
`REFRESH MATERIALIZED VIEW ... PARTITION(S)` commands.

## DML Factor from `__DORIS_STREAM_CHANGE_TYPE__`

The `dml_factor` column (+1 for inserts/updates, −1 for deletes) drives all delta computations.
It is derived in `IvmLinearDeltaHandler.buildDmlFactorExpr()`:

- Reads `__DORIS_STREAM_CHANGE_TYPE__` from the incremental stream scan:
  - `APPEND` or `UPDATE_AFTER` → `dml_factor = +1`
  - `DELETE` or `UPDATE_BEFORE` → `dml_factor = -1`
- Expression: `IF(change_type IN ('APPEND', 'UPDATE_AFTER'), 1, -1)`
- The `__DORIS_STREAM_CHANGE_TYPE__` virtual column is output by
  `LogicalOlapTableStreamScan.computeOutput()` when `isIncrementalScan=true`.

## Row ID Generation

The `__DORIS_IVM_ROW_ID_COL__` column uniquely identifies each row/group in the MV. It is used as the join key when merging deltas into the MV's current state.

- **Simple (non-aggregate) MV**: row_id is injected during normalize (`IvmNormalizeMTMV`) as a hidden column. It is typically a monotonically increasing sequence or hash depending on the scan source.
- **Aggregate MV (grouped)**: row_id is a **null-safe** hash: `CAST(murmur_hash3_64(ifnull(cast(k1 AS VARCHAR),''), cast(isnull(k1) AS VARCHAR), ...) AS LARGEINT)`. Each group key produces two hash arguments — the coalesced value and an isnull flag — so that NULL keys never propagate NULL into the hash and groups differing only in NULL positions (e.g. `(NULL,'x')` vs `('x',NULL)`) produce distinct row_ids. VARCHAR keys skip the inner `CAST`.
- **Aggregate MV (scalar, no GROUP BY)**: row_id is the literal `0` (only one output row).

The same `IvmUtil.buildRowIdHash()` function is used by both the normalize phase and the delta rewrite phase to ensure identical row_id derivation.

## Backward Compatibility

IVM is not publicly available until October 2026. Before that date, there is no need to maintain backward compatibility with existing IVM materialized views. Breaking changes to IVM metadata, DDL format, or internal storage layout are acceptable without migration support.

## Regression Test Guide: Binlog Operations

When writing IVM regression tests, base tables should use MOW (Merge-on-Write unique key) tables
for delete testing. The real stream scan exposes the change-type metadata automatically
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
4. Snapshot sides in incremental refresh are rewritten explicitly:
   pre snapshot uses `stream@snapshot`, and post snapshot uses ordinary olap scan.
   Regression cases should validate this logical shape instead of any table-level
   TSO binding.
