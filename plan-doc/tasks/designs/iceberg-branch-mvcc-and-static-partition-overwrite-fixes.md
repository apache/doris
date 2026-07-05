# Two fe-core fixes for iceberg branch tests (complex_queries + partition_operations)

Context: after the worker-pool TCCL fix, `external_table_p0/iceberg/branch_tag` is 7/10. Remaining real failures
(`tag_retention` is env: spark container not up):
- `iceberg_branch_complex_queries` — scalar subquery branch read returns main data.
- `iceberg_branch_partition_operations` — INSERT OVERWRITE static partition on a branch reads back empty.

Both are confirmed PRODUCT bugs (test expectations are correct). Both fixes are in fe-core. User chose: fix the
general fe-core MVCC issue (per-reference snapshot) for #1, and fix the write path for #2.

---

## Bug ① complex_queries — MVCC snapshot pin collapses same-table main+@branch into one snapshot

### Root cause (confirmed in code)
`StatementContext.snapshots` is `Map<MvccTableInfo, MvccSnapshot>` (StatementContext.java:272), filled by
`loadSnapshots(...)` (996-1005) with first-write-wins (`containsKey`+`put`). `MvccTableInfo`
(datasource/mvcc/MvccTableInfo.java) keys equals/hashCode ONLY on (ctlName, dbName, tableName) — NO branch/version.
So in one statement `select ...(select max(value) from T@branch(b1)).. from T`, the outer main ref `T` and the
subquery `T@branch(b1)` collide on the same key; the first (main) wins, the @branch ref reuses main's snapshot →
reads main (`max=50`, expected `80`). Standalone `@branch` query has one ref, no collision, so `qt_agg_max=80` passes.
General mechanism (paimon + iceberg both go through it). The scan applies the pinned snapshot at
`PluginDrivenScanNode.pinMvccSnapshot()` (703-717) → `getSnapshotFromContext(getTargetTable())` (version-blind).
The scan node DOES carry the per-ref version independently (`FileQueryScanNode.getQueryTableSnapshot()` /
`getScanParams()`), and `resolveSysTableSnapshotPin()` (814-828) already re-derives from them for sys tables.

### Fix design (version-key the map; keep ~35 version-blind readers working)
Touch 4 files. `MvccTableInfo` blast radius: also used by MTMV's own map (MTMVTask:167,492) — MTMV always builds it
latest-only, so adding an optional `version` defaulting to "" leaves MTMV untouched.

1. **MvccTableInfo.java** — add `private final String version` (default ""); single-arg ctor delegates with "";
   add `MvccTableInfo(TableIf, String version)`; include `version` in equals/hashCode/toString; add
   `boolean isSameTable(MvccTableInfo)` comparing (ctl,db,table) ignoring version.
2. **StatementContext.java**
   - `loadSnapshots`: key = `new MvccTableInfo(table, versionKeyOf(ts, sp))` (no more collision; different versions →
     different keys).
   - `getSnapshot(TableIf)` (version-blind, ~35 callers): return `(table,"")` entry if present; else if EXACTLY ONE
     entry exists for that table (ignoring version) return it (preserves a lone `@branch` read's schema accessor);
     else empty. Keeps existing behavior for the common cases; only stops returning an arbitrary version for a
     version-blind read when multiple versions are pinned.
   - NEW `getSnapshot(TableIf, Optional<TableSnapshot>, Optional<TableScanParams>)` (version-aware):
     `snapshots.get(new MvccTableInfo(table, versionKeyOf(ts, sp)))`.
   - private static `versionKeyOf(ts, sp)`: ts present → `"v:" + ts.getType() + ":" + ts.getValue()`; sp present →
     `"p:" + sp.getParamType() + ":" + sp.getMapParams() + ":" + sp.getListParams()`; else `""`.
     (Do NOT use TableSnapshot.toDigest — it redacts the value to '?'.)
3. **MvccUtil.java** — add `getSnapshotFromContext(TableIf, Optional<TableSnapshot>, Optional<TableScanParams>)`
   forwarding to the version-aware `StatementContext.getSnapshot`.
4. **PluginDrivenScanNode.pinMvccSnapshot()** (line 705) — replace
   `MvccUtil.getSnapshotFromContext(getTargetTable())` with
   `MvccUtil.getSnapshotFromContext(getTargetTable(), Optional.ofNullable(getQueryTableSnapshot()),
   Optional.ofNullable(getScanParams()))`. Normal (no-version) tables → versionKey "" → same default entry as today;
   `@branch` ref → its own snapshot. Sys-table path unchanged (getTargetTable not MvccTable → empty → existing
   resolveSysTableSnapshotPin fallback).

Test: a StatementContext-level unit test — load two snapshots for the same table with empty vs `@branch` scanParams,
assert they are stored separately and the version-aware getSnapshot returns each, while the version-blind getSnapshot
returns the default. (APIs verified: TableScanParams getParamType/getMapParams/getListParams/isBranch/isTag;
TableSnapshot getType/getValue.)

Risk: shared with paimon + MTMV — wants the adversarial/clean-room review per repo convention before merge.

---

## Bug ② partition_operations — static-partition INSERT OVERWRITE writes par=NULL into the data column

### Root cause (confirmed end-to-end, FE)
Post-cutover iceberg is a `PluginDrivenExternalCatalog`, so `UnboundTableSinkCreator` builds an
`UnboundConnectorTableSink` and the sink binds through the GENERIC `BindSink.bindConnectorTableSink`
(BindSink.java:862-914), NOT the legacy `bindIcebergTableSink` (708-805). `bindConnectorTableSink` EXCLUDES the
static-partition column from bound columns (selectConnectorSinkBindColumns) and, because iceberg declares
`SINK_REQUIRE_FULL_SCHEMA_ORDER`, takes the full-schema branch where `getColumnToOutput` (367-486) fills the
unmentioned nullable `par` with a `NullLiteral` (line 462). Unlike `bindIcebergTableSink` (lines 783-795), it NEVER
re-projects the static literal `'a'` into `par`. Iceberg keeps the partition column in the data file (its BE writer's
`_non_write_columns_indices` is never populated — viceberg_table_writer.h:154 — vs Hive/MC writers that strip it), so
the file is physically written with `par=NULL`. The manifest partition TUPLE is correctly `'a'` (from BE
`static_partition_values`), so it is NOT partition-pruned; but the all-NULL `par` column → iceberg
`InclusiveMetricsEvaluator` → ROWS_CANNOT_MATCH → metrics-pruned on `WHERE par='a'` → resultDataFiles=0 → 0 rows.
(Regular `insert ... values (.,'d')` works because the child output already carries `par='d'`.)

### Fix design
In `BindSink.bindConnectorTableSink`, mirror the static-partition projection that `bindIcebergTableSink` already has
(BindSink.java:783-795): for each static-partition column, put `new Alias(castIfNotSameType(literalExpr, colType),
colName)` into `columnToOutput` BEFORE `getOutputProjectByCoercion`, so the literal `'a'` is materialized into the
`par` data column. GATE it to connectors that keep partition columns in the data file (iceberg) — MaxCompute strips
partition columns and refills them from `static_partition_values`, so it must keep NULL-filling. The exact gate
(e.g. a connector capability, or reuse of the requiresFullSchemaWriteOrder / partition-column-retention signal)
needs to be picked while reading the surrounding code — do NOT regress MaxCompute. Confirm the static-partition spec
is reachable in bindConnectorTableSink the same way bindIcebergTableSink obtains `staticPartitions`.

Test: regression — the existing `iceberg_branch_partition_operations` (qt_b1_overwrite_partition expects 11,12) is the
acceptance test. Add/keep an FE plan-shape unit test if practical.

---

## Status / handoff — BOTH IMPLEMENTED & COMMITTED (2026-07-01)

- **Bug ① MVCC version-keying = `de1af7a594e`** (4 src + 1 UT). `MvccTableInfo` +version; `StatementContext`
  version-keyed `loadSnapshots` + version-aware `getSnapshot(TableIf,ts,sp)` + version-blind fallback
  (default→lone→empty) + `versionKeyOf`; `MvccUtil` overload; `PluginDrivenScanNode.pinMvccSnapshot` →
  version-aware. UT `StatementContextMvccSnapshotTest` 5/5 + mutation 2/2 KILLED + checkstyle clean.
  **Clean-room 3-agent adversarial review (shared core): NO blocker** — key stability proven (same immutable
  selector object threaded load→scan), no reader/MTMV/paimon regression, 7 break-it scenarios all correct.
- **Bug ② static-partition materialization = `98e00a14c37`** (4 src + 1 UT-assert). New neutral capability
  `SINK_MATERIALIZE_STATIC_PARTITION_VALUES` (iceberg declares, MaxCompute does NOT); `BindSink.bindConnectorTableSink`
  full-schema branch re-projects the static literal (mirrors legacy `bindIcebergTableSink:783-795`), gated by the
  capability. `IcebergConnectorTest.declaresStaticPartitionMaterializationCapability` + mutation KILLED + checkstyle.
  Verbatim mirror arm → focused verification (not multi-agent).
- **e2e flip-gated, NOT run** (no live cluster/iceberg-docker this session): rerun `iceberg_branch_complex_queries`
  + `iceberg_branch_partition_operations` after redeploy to confirm green. `tag_retention` = env (spark container).

### Follow-ups registered (NOT introduced by these fixes)
- [FU-mvcc-mixed-schema] same statement referencing one table as both main and `@branch`/`@tag` with DIVERGENT
  schema (e.g. iceberg type promotion) → version-blind base schema resolves to main (Doris single-schema-per-table
  limitation; data scan is correct). SPI partition pruning lists LATEST partitions for all references regardless
  of snapshot (`getNameToPartitionItems` ignores it) — pre-existing.
- [FU-connector-staticpart-validate] generic connector sink path lacks legacy `bindIcebergTableSink`'s
  static-partition validation (identity-transform / literal checks). Should live connector-side (not fe-core, to
  honor the no-`if(iceberg)` rule), so an invalid static partition fails loud rather than mis-writing.
