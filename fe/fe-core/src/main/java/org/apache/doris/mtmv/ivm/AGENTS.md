# AGENTS.md — IVM (Incremental View Maintenance)

## Two Table Models

An IVM MV is built as one of two models, settled once while it is created and read back from the MV's own
key model on every later statement: `IvmRewriteResult#keysType` while the statement is being built,
`MTMV#isIvmAppendOnlyDupTable()` once the MV exists.

Which model a query settles on follows from the query and its base tables, and a user can override that one
way: `PROPERTIES ('ivm_force_mow' = 'true')` builds an MV of the unique-key model where the query would
qualify for the detail one. It is read while the MV is created (`IvmNormalizeMTMV#judgeKeysType`), and, like
`ivm_use_full_keys`, it is fixed there -- `ALTER MTMV ... SET` reports it instead of applying it, because an
MV's key model cannot be altered. Both properties are read back from the MV's own properties, never cached
in `IvmInfo`.

- **Unique key + merge-on-write** (the default): the MV is maintained through a hidden row-id, its rows are
  updated and retracted through a delete sign and a sequence column, and the layout its refresh reads is
  pinned down by a plan signature. `IvmNormalizeUniqKeysMTMV` lays it out; `IvmDeltaRewriter` and the delta
  handlers maintain it.
- **Append-only detail** (a plain DUP table): every base table is an append-only detail table -- a DUP table
  whose row binlog is on, so its rows are only ever appended -- and the query holds no aggregate and no outer
  join. Such an MV holds the query's own rows: no IVM hidden column, no merge-on-write property, no sequence
  column, no plan signature, and a refresh that only appends. `IvmNormalizeDupKeysMTMV` checks the query, and
  `IvmDupDeltaRewriter` builds its delta, which is the query rewritten to read each base table's stream.
  This is the cheaper model, which is why it is worth judging for: a refresh appends rows instead of
  upserting them (no merge-on-write deduplication or delete-bitmap probing, and no per-row row-id to compute),
  the table carries neither the IVM hidden columns nor the merge-on-write ones, and with no layout for a
  signature to pin down, a refresh cannot fail with `PLAN_SIGNATURE_MISMATCH` because another optimizer rule,
  or a newer version's rules, reshaped the MV's query plan.

What follows from the detail model, including where a user can see the difference:

- Its refresh cannot retract a row by construction: the delta plan has no place to carry a sign. The premise
  is therefore checked on the normalize path (`IvmNormalizeDupKeysMTMV#visitLogicalOlapScan`), which CREATE
  and every refresh mode reach. A base table that stops being an append-only detail table -- replacing it
  with a merge-on-write one, say -- fails the refresh as an `AnalysisException` rather than being turned into
  a fallback: the complete refresh a fallback would run normalizes the same way and fails the same way, so a
  retry would only misdescribe a broken premise.
- `REFRESH ... INCREMENTAL WITH DRY RUN` returns the MV's own columns only. There is no sequence column and no
  delete sign for the unique-key model's shape to add.
- A chained MV over a detail MV needs that upstream MV to carry row binlog (`binlog.enable=true` on the MV
  itself), which keeps the chain append-only at both levels. The MV then carries its own binlog's hidden
  columns, which the storage layer fills in as it writes each row.
- `SHOW CREATE` prints DUPLICATE KEY, and what it prints rebuilds an MV of the same model.

## Recommended Test Suite

It is strongly recommended to run the following tests before committing IVM changes.

### Regression Tests

Run all IVM regression suites under `regression-test/suites/mtmv_p0/ivm/`:

```bash
./run-regression-test.sh --run -d mtmv_p0/ivm
```

This runs every suite in the `ivm` subdirectory. New IVM suites are added over time — the `-d mtmv_p0/ivm` flag automatically picks them up.

### Disable Transparent Rewrite

A newly added IVM suite must turn both transparent-rewrite paths off before it runs any statement:

```groovy
sql "SET enable_materialized_view_rewrite = false"
sql "SET enable_dml_materialized_view_rewrite = false"
```

Both default to true. The first is what makes an MV-verifying suite meaningful: such a suite checks the MV by
re-running the MV's own definition SQL over the base tables, and with the rewrite on that query is answered
from the MV itself, so both sides of the comparison move together and the suite can no longer fail. The second
stops a base-table `INSERT`/`UPDATE`/`DELETE` from being redirected into the MV, which would leave the
incremental path the suite exists to exercise unused.

The one exception is a suite whose subject *is* the rewrite: it keeps whatever setting it needs to assert that
an MV is hit, and says so where it does.

### FE Unit Tests

Run all IVM-related FE unit tests. The relevant test files are located in:

- `fe/fe-core/src/test/java/org/apache/doris/mtmv/ivm/` — IVM core tests
- `fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/CreateMTMVCommandTest.java`
- `fe/fe-core/src/test/java/org/apache/doris/catalog/ShowCreateMTMVTest.java`
- `fe/fe-core/src/test/java/org/apache/doris/catalog/DropMaterializedViewTest.java`

When running IVM FE unit tests, always include
`fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/CreateMTMVCommandTest.java`;
it covers MTMV creation paths that can be affected by IVM normalize/metadata changes.

New test classes are added over time — always check the directories above for the current set.

Fixture builders in `IvmDeltaTestBase` reuse base table names across tests, and an IVM stream is named
after the MV and its base table (`IvmUtil.streamName(mvId, qualifiers)`). Two tests that register a
stream for the same table name under the same MV id therefore share one db entry, and the second one
can pick up the first one's stream, whose base table is gone: `getIvmStream` then fails with
"IVM stream is unavailable or references a different base table". Give each test an MV id of its own
(`mtmv.setId(...)`), as `IvmDeltaRewriterTest` and `IvmDupDeltaRewriterTest` do.

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

## Outer Join Null-Side Complexity

Nested outer joins on an outer join's null-producing side are supported during IVM
normalization and incremental rewrite. The null-side delta path computes both pre-refresh and
post-refresh snapshots, so a complex subtree can significantly increase plan size, planning time,
and refresh resource consumption. This is a performance consideration for users, not a session
variable-controlled syntax restriction. Keep the null-producing side simple when practical, or
materialize a complex subtree separately if the generated refresh plan is too expensive.

This does not remove the separate row-id correctness requirement for outer joins: the retained
side row-id must remain deterministic because null-row repair branches use it across the pre/post
snapshot boundary. A DUP_KEYS scan takes its row-id from `__DORIS_ROW_LSN_COL__`
(`IvmNormalizeUniqKeysMTMV#computeScanRowIdAndKeys`), which is deterministic, so it is allowed on either
side. Only the legacy fallback for a DUP_KEYS table without row binlog -- the non-deterministic
`uuid_numeric()` row-id, which `validateBinlogEnabled` rejects unless the table is an excluded
trigger table -- is still rejected on the retained side. FULL OUTER JOIN requires deterministic
row-ids on both sides.

## Shared Acceptance Gates

Both normalize paths and the append-only detail delta rewriter accept the same query shapes, and the
gates that reject the rest live in one place, `IvmNormalizeCommonChecker` (a DISTINCT project, a
UNION DISTINCT or constant-arm UNION ALL, a reserved output column name, an unsupported node type).
Add a new acceptance gate there rather than to a single class, or the paths drift apart on what they
accept -- which is how the join gate below went unshared.

The join gate carries a per-model accepted set (the detail model maintains only inner and cross joins,
the unique-key model also the outer joins) but is otherwise shared, and it tests the mark-join mark as
well as the join type. The mark is orthogonal to the type -- an apply rewrite puts one on a LEFT_SEMI,
a LEFT_ANTI, or a CROSS join -- so a type-only test accepts a mark join whose type happens to be an
accepted one, and a mark join can stop matching a row the MV already holds.

## Recursive Delta Rewrite

- `IvmNormalizeUniqKeysMTMV` rejects an input `LogicalOlapTableStreamScan`; normalized MV queries start
  from ordinary `LogicalOlapScan` nodes.
- `IvmDeltaRewriteVisitor` returns `Optional<IvmDeltaRewriteResult>`. An empty result means the
  subtree has no pending delta.
- For an inner join, the left delta joins the right pre snapshot and the right delta joins the
  left post snapshot. Non-empty child contributions are combined with `UNION ALL`.
- `preSnapshot(plan)` and `postSnapshot(plan)` rewrite scans on demand and then fresh-copy the plan.
- Delta rows carry `dml_factor` and `sequence` together. `sequence` is allocated at delta scan
  leaves and propagated upward; `maxDeltaIndex` is metadata used by outer-join padding rows.
- Aggregate IVM output uses the sequence encoded from its child delta's `maxDeltaIndex`. Aggregate refresh
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

Typed explain forms work the same way here as they do for `EXPLAIN <SELECT>`: `ANALYZED`, `LOGICAL`
and `PHYSICAL` decide how much of the plan is shown, and the `PROCESS` variants print the rules that
ran, one line each. On both refresh modes, for example:

```sql
EXPLAIN ANALYZED PLAN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL;
EXPLAIN LOGICAL PLAN REFRESH MATERIALIZED VIEW mv_name INCREMENTAL WITH ALL STREAMS;
EXPLAIN PHYSICAL PLAN REFRESH MATERIALIZED VIEW mv_name COMPLETE;
EXPLAIN ANALYZED PLAN PROCESS REFRESH MATERIALIZED VIEW mv_name INCREMENTAL;
```

`EXPLAIN ANALYZED PLAN PROCESS REFRESH ...` is the form to reach for when a refresh plan is not what
you expected: its rule timeline is where the IVM rules show themselves -- an incremental refresh
lists `IVM_INCR_REFRESH_MTMV`, the rule that builds the delta plan -- so the log line a rule prints
can be read next to the rule that produced it. Which IVM rules appear follows from the statement and
its refresh mode; the timeline covers the statement's own planning, not every rule of the query the
MV was created from.

Like the plain forms, these go through the dry-run path: no stream offset is consumed and nothing is
written to the MV, so a refresh that follows one still sees all of its pending delta. (`PHYSICAL PLAN`
and `PROCESS` are not combinable — `EXPLAIN PHYSICAL PLAN PROCESS` prints nothing.)

`EXPLAIN REFRESH` supports only `INCREMENTAL` and `COMPLETE`; partition-scoped refresh syntax is
not supported by Explain Refresh. Partition-scoped refresh remains available through ordinary
`REFRESH MATERIALIZED VIEW ... PARTITION(S)` commands.

## Incremental Refresh Dry Run

`REFRESH MATERIALIZED VIEW mv INCREMENTAL WITH DRY RUN` computes the exact delta query of the next
incremental refresh and returns the delta rows to the client **without writing anything** to the MV.
It is a debugging tool for inspecting what an incremental refresh will write before executing it.

### Syntax

```sql
REFRESH MATERIALIZED VIEW mv_name INCREMENTAL WITH DRY RUN
REFRESH MATERIALIZED VIEW mv_name INCREMENTAL WITH DRY RUN LIMIT n
REFRESH MATERIALIZED VIEW mv_name INCREMENTAL WITH DRY RUN LIMIT n OFFSET m
REFRESH MATERIALIZED VIEW mv_name INCREMENTAL WITH DRY RUN LIMIT m, n
```

- `LIMIT` is optional and reuses ordinary `SELECT`-style offset/count syntax.
- Only `INCREMENTAL` is supported; `COMPLETE` dry run is a parse error.
- Only IVM materialized views are supported; non-IVM MV reports an analysis error.
- `EXPLAIN REFRESH ... INCREMENTAL WITH DRY RUN` is a parse error — use `EXPLAIN REFRESH ... INCREMENTAL` instead.

### Result columns

The result set contains the **exact rows that the next incremental refresh would insert** into the
MV. The column order matches the sink output shape produced by `makeDeleteSignProject`:

```
mtmv.getInsertedColumnNames()  +  __DORIS_SEQUENCE_COL__  +  __DORIS_DELETE_SIGN__
```

where `getInsertedColumnNames()` includes business columns plus all IVM internal columns:

- `__DORIS_IVM_ROW_ID_COL__` — deterministic row-id (murmur hash for grouped agg, literal for scalar)
- `__DORIS_IVM_DML_FACTOR_COL__` — +1 (append/update) or −1 (delete)
- `__DORIS_IVM_AGG_COUNT_COL__` — group-level aggregation count
- Aggregation state columns (`__DORIS_IVM_AGG_{ordinal}_{stateType}_COL__`)

The trailing sequence column and delete sign are the **unique-key model's**, which is the model that
retracts and updates rows through them: `getInsertedColumnNames()` yields no IVM internal column for
an append-only detail MV, and `makeDeleteSignProject` adds the two trailing columns to that model's
sink output alone. A detail MV's dry run therefore returns the MV's own columns and nothing else —
its delta plan has no place to carry a sign, which is what `IvmDupDeltaRewriter` builds.

For an aggregate MV defined as `SELECT k1, COUNT(*) AS cnt, SUM(v1) FROM t GROUP BY k1`,
a dry-run result row looks like:

```
__DORIS_IVM_ROW_ID_COL__ | k1 | cnt | sum_v1 | __DORIS_IVM_DML_FACTOR_COL__ | __DORIS_IVM_AGG_COUNT_COL__ | __DORIS_SEQUENCE_COL__ | __DORIS_DELETE_SIGN__
   hash_of_group_key     |  1 |  1  |   15   |             1               |             1              |        4097          |          0
```

### Semantics

- **Read-only.** No insert transaction is built. Stream offsets, sequence prefix, and MV metadata
  are never modified.
- **Idempotent.** Repeating the same dry run returns identical rows as long as the base table data
  has not changed.
- **Snapshot semantics.** Reads base table streams at the same snapshot position as a real
  incremental refresh would use.
- **No master forwarding.** Dry run runs on the FE that received the statement (`NO_FORWARD`);
  results stream directly from the local coordinator to the client, avoiding a full-materializing
  cross-FE RPC.
- **Pending streams only.** Only streams with unconsumed data contribute delta rows
  (same as a real `INCREMENTAL` refresh).

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

- **Simple (non-aggregate) MV**: row_id is injected during normalize (`IvmNormalizeUniqKeysMTMV`) as a hidden column. It is typically a monotonically increasing sequence or hash depending on the scan source.
- **Aggregate MV (grouped)**: row_id is a **null-safe** hash: `CAST(murmur_hash3_64(ifnull(cast(k1 AS VARCHAR),''), cast(isnull(k1) AS VARCHAR), ...) AS LARGEINT)`. Each group key produces two hash arguments — the coalesced value and an isnull flag — so that NULL keys never propagate NULL into the hash and groups differing only in NULL positions (e.g. `(NULL,'x')` vs `('x',NULL)`) produce distinct row_ids. VARCHAR keys skip the inner `CAST`.
- **Aggregate MV (scalar, no GROUP BY)**: row_id is the literal `0` (only one output row).

The same `IvmUtil.buildRowIdHash()` function is used by both the normalize phase and the delta rewrite phase to ensure identical row_id derivation.

## Backward Compatibility

IVM is not publicly available until January 2027. Before that date, there is no need to maintain backward compatibility with existing IVM materialized views, nor with the binlog/stream data they consume. Breaking changes to IVM metadata, DDL format, binlog format, or the internal storage layout are acceptable without migration support: a view created by an earlier build may be dropped and recreated instead of migrated, and a change that only affects such a view is not a correctness defect to be fixed here.

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
