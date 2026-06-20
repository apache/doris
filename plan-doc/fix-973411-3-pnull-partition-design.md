# FIX-3 — test_paimon_mtmv: "Duplicated named partition: p_NULL"

## Problem
CI 973411 `mtmv_p0/test_paimon_mtmv.groovy:247`: `CREATE MATERIALIZED VIEW ... partition by(region) AS SELECT
* FROM <catalog>.test_paimon_spark.null_partition` fails: `Duplicated named partition: p_NULL`.

## Root Cause
`null_partition` (docker run01.sql:63-67) region values: `bj`, genuine NULL, string `'null'`, string `'NULL'`.
MTMV names one partition per distinct base PartitionKeyDesc via `MTMVPartitionUtil.generatePartitionName`:
`"p_" + desc.toSql()` with `[^a-zA-Z0-9,]` stripped. Two partitions collapse to `p_NULL`:
- genuine NULL: connector normalizes paimon's `__DEFAULT_PARTITION__` → `__HIVE_DEFAULT_PARTITION__`
  (PaimonConnectorMetadata:1001-1008), bridge marks it `isNull=true` (PluginDrivenMvccExternalTable:193-194)
  → PartitionKey holds a `NullLiteral`. `PartitionInfo.toPartitionValue` maps a `NullLiteral` →
  `new PartitionValue("NULL", true)` (PartitionInfo.java:401-402) → `desc.toSql()` = `('NULL')` → `p_NULL`.
- string `'NULL'`: `isNull=false`, getStringValue()="NULL" → `('NULL')` → `p_NULL`. IDENTICAL.

Master (`PaimonUtil.toListPartitionItem:214`) hardcoded `isNull=false`, so genuine-NULL was a *StringLiteral* of
the sentinel → a DISTINCT name → no collision (test passed; .out / comment line 265 "Will lose null data").
The branch's IS-NULL-prune fix (isNull=true) introduced the collision. Classification: **SPI regression**.

## Design
Keep `isNull=true` (so `region IS NULL` prune from the prune fix still works) but stop the MTMV name from
collapsing a genuine-null to the bare `NULL` that collides with a literal string `'NULL'`. The bridge already
preserves a distinct display string in `PartitionKey.originHiveKeys` ("__HIVE_DEFAULT_PARTITION__", because it
builds the key with `createListPartitionKeyWithTypes(..., isHive=true)`). In
`ListPartitionItem.toPartitionKeyDesc`, for a null partition value whose key carries a sized `originHiveKeys`,
use that origin string as the DISPLAY value (`new PartitionValue(originKey, true)`); `isNull` stays true so
`getValue(type)` is still a `NullLiteral` (pruning unaffected) but `PartitionKeyDesc.toSql()` renders the
distinct sentinel via `getStringValue()`. Result: genuine-NULL → `p_HIVEDEFAULTPARTITION` (distinct, not
asserted), `'NULL'`→`p_NULL`, `'null'`→`p_null`, `'bj'`→`p_bj`. No duplicate. Also closes the same latent
collision for Hive (TablePartitionValues marks `__HIVE_DEFAULT_PARTITION__` isNull=true identically).

Connector-agnostic (no source-specific code); fix is at the generic catalog layer. Blast radius = MTMV only:
the only `toPartitionKeyDesc` callers are MTMV generators; MTMV's own OLAP partitions have empty
`originHiveKeys` so the guard is a no-op for them.

## Implementation Plan
`fe/fe-core/.../catalog/ListPartitionItem.java`: rewrite `toPartitionKeyDesc(int pos)` and the no-arg overload
to substitute the origin-hive-key display string for a null partition value (a private helper
`nullDisplayValue(PartitionKey, List<PartitionValue>, int)`), keeping the existing `pos`-bounds AnalysisException.

## Risk Analysis
Low. `getOriginHiveKeys()` is a plain getter (empty for OLAP → guard skips). `isNull` unchanged → no prune
regression. `PartitionInfo.toPartitionValue` (shared with Range/SHOW-CREATE/internal-OLAP) is NOT touched.

## Test Plan
### Unit Tests
New FE UT (e.g. `ListPartitionItemTest`): build a null PartitionKey via
`createListPartitionKeyWithTypes([PartitionValue("__HIVE_DEFAULT_PARTITION__", true)], [VARCHAR], true)` and a
string PartitionKey for "NULL"; assert `generatePartitionName(toPartitionKeyDesc(0))` differs between them and
that the null key's `getValue(STRING)` is still a NullLiteral. RED before / GREEN after.

### E2E Tests
Existing `test_paimon_mtmv.groovy` under docker enablePaimonTest=true (currently RED, expected GREEN). Also
re-verify `test_paimon_runtime_filter_partition_pruning` (IS NULL prune) stays GREEN.
