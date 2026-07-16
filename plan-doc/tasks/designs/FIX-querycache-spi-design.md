# FIX — Migrate Nereids SQL result cache to the generic SPI plugin table/scan-node

> Restore SQL result-cache eligibility for hms-cutover tables (plain-hive today; iceberg/paimon/hudi
> when they qualify), by replacing the four source-name branches in the cache path with a
> connector-agnostic capability (`MTMVRelatedTableIf`) + a stable, data-tied invalidation token
> (`getNewestUpdateVersionOrTime()`), obeying the fe-core iron rule (no `instanceof HMSExternalTable`
> / `HiveScanNode` / `HMS_EXTERNAL_TABLE` branching).
>
> **Scope decision (user-signed, Option A, 2026-07-12):** the generic mechanism admits **any lakehouse
> plugin table that supplies a valid stable token** (hive + iceberg + paimon + hudi), not a hive-only
> connector opt-in. The user-facing switch stays `enable_hive_sql_cache` (default **false**), so *nothing
> changes by default*; the difference from a hive-only design only manifests once a user turns the switch
> on — then every qualifying lakehouse table caches, not just hive.

## 1. Problem

Before the hms cutover, a plain-hive table was `HMSExternalTable`; its query results were eligible for
the Nereids SQL result cache. After the cutover it is a `PluginDrivenMvccExternalTable`
(`extends PluginDrivenExternalTable extends ExternalTable`) and its scan node is `PluginDrivenScanNode`
(not `HiveScanNode`). The cache path gates on the *old class names / old table-type enum*, so a flipped
hive table silently falls into the "unsupported → no cache" arm. The clean-room re-review flagged this
as a by-design fail-safe minor at cutover time; this task restores the capability properly.

## 2. The four loci (HEAD-verified — trust these, not the stale line numbers in HANDOFF)

The Nereids SQL result cache has **two store tiers** (FE result-set cache for one-row/empty relations via
`StmtExecutor.tryAddFeSqlCache`; BE sql-cache for general scans via `tryAddBeCache` → `CacheProxy`) and a
**lookup** path. `StmtExecutor`'s store dispatch is already table-type-agnostic — **no change there.** The
four source-name gates are:

| # | File:method | HEAD line | Current source-name branch | What breaks post-cutover |
|---|---|---|---|---|
| L1 | `BindRelation.bindWithMetaData` finally | `885-892` | `else if (table instanceof HMSExternalTable && enableHiveSqlCache)` → `addUsedTable`; else `setHasUnsupportedTables(true)` | flipped hive → else → cache disabled |
| L2 | `SqlCacheContext.addUsedTable` | `194-199` | `else if (tableIf instanceof HMSExternalTable) version = getUpdateTime()` | flipped hive → version stays 0 |
| L3 | `NereidsSqlCacheManager.tablesOrDataChanged` | `441-443` type gate + `475-479` re-check + `506-508` partition loop | type gate allows only OLAP/MV/HMS_EXTERNAL_TABLE; re-check + partition loop both `instanceof HMSExternalTable` | `PLUGIN_EXTERNAL_TABLE` type → always `CHANGED_AND_INVALIDATE` (never a hit); scan-table loop also rejects it |
| L4 | `CacheAnalyzer.buildCacheTableList` `305-332` + `buildCacheTableForHiveScanNode` `472-484` | scan-node gate `instanceof OlapScanNode / HiveScanNode`; `latestPartitionTime = table.getUpdateTime()` | flipped hive's node unrecognized → not "all-olap or all-hive" → `emptyList` → `CacheMode.None` → BE tier never fills. **Fixed by target-table capability, NOT node class (see L4 below): `HudiScanNode extends HiveScanNode` and `jdbc_query` TVFs both emit `PluginDrivenScanNode`.** |

`CacheAnalyzer` is confirmed **on the Nereids result-cache hot path** (only reached through
`innerCheckCacheModeForNereids`; partition cache is force-disabled, so of `{NoNeed,None,Sql}` a fresh hive
query used to get `Sql`). It computes `CacheTable.latestPartitionTime`, applies the
"table stable for ≥ `cache_last_version_interval_second`" gate, and via `StmtExecutor.tryAddBeCache` copies
`latestPartition{Id,Version,Time}` + `CacheProxy` into the `SqlCacheContext` that the FE-level
`NereidsSqlCacheManager` later validates. So **all four must change** for end-to-end parity; fixing any
subset leaves the cache broken.

## 3. The invalidation token — the crux

**Legacy (data-tied, correct):** `HMSExternalTable.initSchemaAndUpdateTime` (`:700-709`) *overrides* the
`ExternalTable` default and stamps `updateTime = transient_lastDdlTime * 1000` (wall-clock only as a view
fallback). `getUpdateTime()` therefore changed iff the table's DDL/data time changed → cache invalidated
exactly on change.

**Default (unstable, WRONG for a token):** `PluginDrivenMvccExternalTable` overrides *neither*
`initSchemaAndUpdateTime` nor `getUpdateTime`, so it inherits `ExternalTable.initSchemaAndUpdateTime`
(`:371-372`) = `setUpdateTime(System.currentTimeMillis())` at every FE schema (re)load. `getUpdateTime()`
is then wall-clock-at-last-schema-load, **orthogonal to data**. Concrete failure if used as the token:

```
10:00  SELECT * FROM t   -> FE loads schema, token = 10:00, result stored in cache
10:05  someone INSERTs new rows into hive table t
10:06  SELECT * FROM t   -> schema cache not yet expired, token STILL 10:00 == stored 10:00
                         -> "unchanged" -> returns the stale 10:00 result (missing the new rows)
```

**Stable token (the fix):** `MTMVRelatedTableIf.getNewestUpdateVersionOrTime()` — the same value the MV /
dictionary auto-refresh machinery already trusts.
- Declared on the **capability interface** `MTMVRelatedTableIf:117` (source-agnostic), implemented by
  `OlapTable`, `HMSExternalTable`, and `PluginDrivenMvccExternalTable:707-736`.
- For a **last-modified** connector (hive): returns the cache-backed
  `getTableFreshness().getTimestampMillis()` — unpartitioned = table `transient_lastDdlTime`; partitioned =
  **max `transient_lastDdlTime` over partitions** (`HiveConnectorMetadata:1204-1217`). This is data-tied
  *and* strictly more correct than legacy (legacy's table-level DDL time missed partition-only inserts).
- For a **snapshot-id** connector (iceberg/paimon): returns the monotonic newest snapshot version.
- Plain (non-MVCC) `PluginDrivenExternalTable` does **not** implement `MTMVRelatedTableIf` → automatically
  excluded (no data-change signal).

Equality-compare (`stored != current → invalidate`) is satisfied by both token kinds: the value changes iff
data changes. We store the raw `long` (millis or version); the MTMV "carry the name" nuance
(`MTMVMaxTimestampSnapshot`) is only needed for MV snapshot bookkeeping, not for cache equality.

### 3.1 Token edge cases (must handle)
- **`token <= 0`** (`getNewestUpdateVersionOrTime()` returns `0` for an empty partition set / dropped
  table; no real hive `transient_lastDdlTime`): treat as **not cacheable** — `addUsedTable` records the
  table as unsupported (`setHasUnsupportedTables(true)`) instead of pinning a bogus `0`. Fail-safe; mirrors
  legacy hive-view behaviour (unstable `currentTimeMillis` fallback → never a stable hit within the
  interval). Real snapshot ids / epoch-ms tokens are never `≤ 0`, so this only rejects the "no info" state.
- **snapshot-id token vs the CacheAnalyzer stability interval:** for iceberg/paimon the token is a small
  monotonic version, while the interval gate computes `now(epoch-ms) - latestPartitionTime(version)` — a
  huge positive → always "stable enough". Benign: equality-compare in `tablesOrDataChanged` is the real
  correctness mechanism; the interval gate is only a perf heuristic ("don't cache a table that changed in
  the last N seconds"). Hive keeps exact legacy semantics because its token *is* epoch-ms.
- **meta-cache staleness bound:** the freshness token reflects the `CachingHmsClient` meta cache
  (TTL-bounded), not the live metastore. This is **the same bound legacy operated under** (legacy read
  `transient_lastDdlTime` from the schema-cache-loaded table) and the same bound the scan itself uses, so
  the cache is never *more* stale than a fresh (uncached) query would be. Acceptable, unchanged contract.
- **time-travel (`FOR VERSION/TIME AS OF`):** cache key = SQL text (includes the snapshot clause) → keys
  never collide; token = *latest* version, so a pinned-snapshot result may be *over*-invalidated when new
  data lands (harmless perf loss, never wrong data — the pinned snapshot is immutable). No special-casing.

### 3.2 Token cost on the lookup hot path (red-team L1, honest accounting)
`getNewestUpdateVersionOrTime()` is **not** the cheap "cached-field read" legacy `getUpdateTime()` was.
For a flipped **partitioned hive** table it costs, per call: `materializeLatest()` (a `listLatestPartitions`
→ builds a `PartitionItem` per partition — the result is then *discarded* on the last-modified branch) **plus**
`queryTableFreshness()` → `getTableFreshness()` (`collectPartitionNames` + a `get_partitions_by_names` to
read each partition's `transient_lastDdlTime` and take the max). Both HMS calls are **meta-cache-backed**
(`CachingHmsClient`), so a warm cache makes them in-memory reads; the residual cost is CPU (building the
partition items + the max scan). Unpartitioned hive is cheap (freshness from the handle, no round-trip);
snapshot-id connectors (iceberg/paimon) read a monotonic version. The token is also computed **twice** per
query (store: L2 `addUsedTable` + L4 `buildCacheTableForExternalScanNode`).

**Why this is acceptable to ship, not a blocker:** (a) opt-in, `enable_hive_sql_cache` default **false** — zero
production cost by default; (b) a cache HIT pays ≈ the *metadata* portion of planning (which a cache MISS pays
anyway) and **skips the entire BE data scan** — so a hit is always cheaper than a miss, the feature is always
net-positive; (c) the meta-cache amortises repeated lookups; (d) it matches the already-accepted cost of the
MV/dictionary poll that calls the same method.

**Registered follow-up optimisation (separate task, NOT this change):** short-circuit
`getNewestUpdateVersionOrTime()` to skip `materializeLatest()` on the last-modified branch (read the freshness
kind from `beginQuerySnapshot` alone, then `queryTableFreshness()`), collapsing the double handle-resolution
and dropping the discarded partition-item enumeration. It is a *pure, output-preserving* refactor of a shared,
sensitive MVCC method (`plugindriven-mvcc-table-is-live-not-dormant`), so it needs its **own** cost-neutrality
red-team for iceberg/paimon and is deliberately kept out of the fe-core cache-wiring change here.

## 4. Design — connector-agnostic edits

Keep the `OlapTable` branch untouched everywhere (surgical; internal-table cache behaviour must stay
byte-identical). Insert a generic external-MVCC branch **after** it, keyed on `instanceof MTMVRelatedTableIf`.
`OlapTable` also implements `MTMVRelatedTableIf`, but is always caught by the earlier `instanceof OlapTable`
arm, so its path is unchanged.

### L1 — `BindRelation` (`:885-892`) admission
```java
} else if (table instanceof OlapTable) {
    sqlCacheContext.addUsedTable(table);
} else if (table instanceof ExternalTable && table instanceof MTMVRelatedTableIf
        && cascadesContext.getConnectContext().getSessionVariable().enableHiveSqlCache) {
    sqlCacheContext.addUsedTable(table);          // token validity enforced inside addUsedTable
} else {
    sqlCacheContext.setHasUnsupportedTables(true);
}
```
- `instanceof ExternalTable` keeps this to external catalogs (defensive; every `MTMVRelatedTableIf` that
  isn't `OlapTable` today is an external table, but the belt-and-suspenders keeps a future internal
  `MTMVRelatedTableIf` out of this arm).
- `enableHiveSqlCache` (default false) reused as the external opt-in switch — see §5 naming note.
- Import: add `MTMVRelatedTableIf`; **keep** the existing `HMSExternalTable` import (still used by the
  view/hudi cutover arm at `:754-795`).

### L2 — `SqlCacheContext.addUsedTable` (`:192-213`) token capture
```java
long version = 0;
try {
    if (tableIf instanceof OlapTable) {
        version = ((OlapTable) tableIf).getVisibleVersion();
    } else if (tableIf instanceof MTMVRelatedTableIf) {
        version = ((MTMVRelatedTableIf) tableIf).getNewestUpdateVersionOrTime();
        if (version <= 0) {                       // §3.1 fail-safe: no reliable data-change signal
            setHasUnsupportedTables(true);
            return;
        }
    }
} catch (Throwable e) {
    setHasUnsupportedTables(true);
    LOG.warn("table {}, can not get version", tableIf.getName(), e);
}
```
- Swap import `HMSExternalTable` → `MTMVRelatedTableIf`.
- `TableVersion.type` continues to be `tableIf.getType()` (= `PLUGIN_EXTERNAL_TABLE` for flipped tables).

### L3 — `NereidsSqlCacheManager.tablesOrDataChanged` (`:441-482`)
Type gate — admit the plugin type:
```java
if (tableVersion.type != TableType.OLAP && tableVersion.type != TableType.MATERIALIZED_VIEW
        && tableVersion.type != TableType.HMS_EXTERNAL_TABLE
        && tableVersion.type != TableType.PLUGIN_EXTERNAL_TABLE) {
    return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
}
```
Re-check (used-tables loop) — replace the HMS branch (the `else` catch-all at `:480-481` stays for
non-capability tables):
```java
} else if (tableIf instanceof MTMVRelatedTableIf) {
    if (tableVersion.version != ((MTMVRelatedTableIf) tableIf).getNewestUpdateVersionOrTime()) {
        return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
    }
} else {
    return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
}
```
Partition-existence loop (`:506-508`) — **the branch I initially missed; found by the recon sweep.**
`CacheAnalyzer` adds external tables to `getScanTables()` too, so this loop runs for flipped hive. Legacy
skipped `HMSExternalTable` here (per-partition existence is an Olap-only concern; external freshness is
fully covered by the token compare in the used-tables loop above). Generalize the skip to the capability:
```java
} else if (!(tableIf instanceof MTMVRelatedTableIf)) {   // was: !(tableIf instanceof HMSExternalTable)
    return IsChanged.CHANGED_AND_INVALIDATE_CACHE;
}
```
- `OlapTable` is matched by the earlier `instanceof OlapTable` arm (`:453`/`:489`); `MTMV` too (it *is* an
  `OlapTable`). So the new arms only catch external MVCC tables.
- `HMSExternalTable:125 implements MTMVRelatedTableIf` — so both swaps are a strict *superset* of the old
  HMS behaviour: every legacy HMS table still takes the same path, plus flipped plugin tables now qualify.
- `HMS_EXTERNAL_TABLE` left in the type allow-list for safety (no live producer post-cutover; harmless).
- Swap import `HMSExternalTable` → `MTMVRelatedTableIf`. **NereidsSqlCacheManager has 3 branches to fix**
  (type gate `:441`, used-tables re-check `:475`, partition loop `:506`).

### L4 — `CacheAnalyzer`  (recognize by TARGET-TABLE capability, **not** scan-node class — red-team fix)
**Do NOT swap `instanceof HiveScanNode` → `instanceof PluginDrivenScanNode`.** The scan-node class is the
wrong discriminator on two counts the red-team proved:
- `HudiScanNode extends HiveScanNode` (**not** `PluginDrivenScanNode`) — a class-based swap would *drop*
  hudi from the gate that `HiveScanNode` used to cover (a silent regression).
- `jdbc_query(...)` TVFs *also* emit a `PluginDrivenScanNode` (`JdbcQueryTableValueFunction.java:62`) whose
  backing table is a `FunctionGenTable` (`extends Table`, **not** `MTMVRelatedTableIf`) with no data-version
  token — a class-based `instanceof PluginDrivenScanNode` would wrongly admit it.

Instead, recognize an eligible external scan node by its **target table's capability** — the same
`MTMVRelatedTableIf` gate used everywhere else. This is *more* iron-rule-clean (keys on a capability, not a
node class) and robust to the scan-node hierarchy. `buildCacheTableList` (`:305-332`):
```java
long olapScanNodeSize = 0;
long externalCacheableSize = 0;
for (ScanNode scanNode : scanNodes) {
    if (scanNode instanceof OlapScanNode) {
        olapScanNodeSize++;
    } else if (scanNode.getTupleDesc() != null
            && scanNode.getTupleDesc().getTable() instanceof MTMVRelatedTableIf) {   // capability, not class
        externalCacheableSize++;
    }
}
...
if (!(olapScanNodeSize == scanNodes.size() || externalCacheableSize == scanNodes.size())) {
    return Collections.emptyList();               // no federated / mixed-source / TVF caching (legacy parity)
}
...
CacheTable cTable = node instanceof OlapScanNode
        ? buildCacheTableForOlapScanNode((OlapScanNode) node)
        : buildCacheTableForExternalScanNode(node);   // takes the base ScanNode
```
New `buildCacheTableForExternalScanNode` is connector-agnostic; `getTupleDesc()`/`getTable()` are public on
the `ScanNode`/`TupleDescriptor` base (`PluginDrivenScanNode.getTargetTable()` is `protected`+`throws`, so
we deliberately route through the tuple descriptor):
```java
private CacheTable buildCacheTableForExternalScanNode(ScanNode node) {
    CacheTable cacheTable = new CacheTable();
    TableIf tableIf = node.getTupleDesc().getTable();     // guaranteed MTMVRelatedTableIf by the gate above
    cacheTable.table = tableIf;
    cacheTable.partitionNum = node.getSelectedPartitionNum();   // ScanNode base, public, no throw
    cacheTable.latestPartitionTime = ((MTMVRelatedTableIf) tableIf).getNewestUpdateVersionOrTime();
    DatabaseIf database = tableIf.getDatabase();
    CatalogIf catalog = database.getCatalog();
    ScanTable scanTable = new ScanTable(
            new FullTableName(catalog.getName(), database.getFullName(), tableIf.getName()));
    scanTables.add(Pair.of(scanTable, tableIf));
    return cacheTable;
}
```
- `MetricRepo.COUNTER_QUERY_HIVE_TABLE` bump is dropped (source-specific metric; the generic gate covers all
  sources). Keep `COUNTER_QUERY_TABLE`/`COUNTER_QUERY_OLAP_TABLE`.
- **Remove** the `HiveScanNode` import; add `MTMVRelatedTableIf`. No `PluginDrivenScanNode` import needed —
  the gate never names it (pure capability check), which is the cleanest iron-rule form.

## 5. Iron-rule compliance & naming

- All four gates now key on the **capability interface** `MTMVRelatedTableIf` and the **generic node**
  `PluginDrivenScanNode` — no `instanceof HMSExternalTable`, no `HiveScanNode`, no `switch(dlaType)`, no
  engine-name test. Matches the project rule (memory `catalog-spi-plugindriven-no-source-specific-code`)
  and Trino's "ask the connector for the capability, don't inspect its class" model.
- **Session-var naming:** `enable_hive_sql_cache` is reused as the external opt-in switch (default false,
  so no default behaviour change). Under Option A its scope is now "all lakehouse plugin tables", so the
  `hive` in the name is legacy. Renaming is deliberately **out of scope** (surgical; avoids breaking
  existing user configs). Flagged for a possible future generic alias — NOT this change.

## 6. Blast radius

- **OlapTable / internal:** untouched (earlier `instanceof OlapTable` arm everywhere). Zero behaviour change.
- **Default (switch off):** every table hits the same "unsupported" arm as today. Zero behaviour change.
- **Switch on:** flipped hive restored to (better-than-)legacy caching; iceberg/paimon/hudi *newly* eligible
  when they expose a valid `getNewestUpdateVersionOrTime()` (Option A, user-signed). Their tokens are already
  the monotonic values the MV/dictionary path trusts, so correctness rides on an existing contract — but the
  cache-fill path for them is newly exercised → their e2e is in scope (§8).
- **Plain non-MVCC `PluginDrivenExternalTable` / system tables (`$partitions`) / TVFs:** excluded because
  their table is **not** `MTMVRelatedTableIf` — enforced by **two** capability gates: L1 admission
  (`setHasUnsupportedTables` → `supportSqlCache()` false) and L4 scan-node recognition (target table not
  `MTMVRelatedTableIf` → `externalCacheableSize` short of total → `emptyList` → `CacheMode.None`). It is
  **NOT** enforced by `latestPartitionTime = 0` (a `0` token would pass the CacheAnalyzer interval gate — the
  earlier design draft's claim there was mechanically wrong). The `token <= 0` fail-safe in L2 is a *second*
  line for a real MVCC table that momentarily has no freshness (empty partitions), not the sys-table guard.
- **`HMSExternalTable` DLA types (legacy, dead post-cutover):** `HMSExternalTable implements MTMVRelatedTableIf`
  and `getNewestUpdateVersionOrTime()` returns a non-zero `transient_lastDdlTime`-derived value for a real
  table, so the `token <= 0` fail-safe cannot silently disable a live legacy table (moot anyway — no live
  producer after the cutover).
- **BE side:** unchanged. The BE sql-cache was already fed for external (hive) tables pre-cutover via
  `tryAddBeCache`; restoring `CacheMode.Sql` for the plugin node reuses that exact path.

## 7. Unit tests — AS IMPLEMENTED (3 classes, all RED on the pre-cutover HEAD)
- **`qe/cache/PluginTableCacheAnalyzerTest`** (L4): drives the two new private members via `Deencapsulation`
  (avoids the `MetricRepo`/analyze bootstrap that keeps `HmsQueryCacheTest` disabled for the plugin path).
  `isExternalCacheableScanNode` → true for a `PluginDrivenMvccExternalTable`-backed node, false for a
  `FunctionGenTable` (jdbc TVF) node, false for a null tuple desc; `buildCacheTableForExternalScanNode` →
  `latestPartitionTime == getNewestUpdateVersionOrTime()`, `partitionNum` from the scan node.
- **`nereids/SqlCacheContextPluginTableTest`** (L2): `addUsedTable` records the connector token for a plugin
  table (`TableVersion.version == token`, type `PLUGIN_EXTERNAL_TABLE`); `token <= 0` → `hasUnsupportedTables`
  and no entry (fail-safe).
- **`common/cache/NereidsSqlCacheManagerPluginTableTest`** (L3, the correctness core): `tablesOrDataChanged`
  via `Deencapsulation.invoke` with a mock `Env` chain → unchanged token = NOT_CHANGED (cache served),
  advanced token = CHANGED_AND_INVALIDATE_CACHE (data change evicts).
- Gate: `mvn -pl fe-core -am test` for the 3 classes green, 0 checkstyle. e2e (§8) covers BE fill + the
  `:506` partition-loop skip end-to-end.

### 7.1 Original test-design sketch (superseded by 7)
`tablesOrDataChanged` and `findTableIf` are private → drive them via `Deencapsulation.invoke(manager, ...)`
on a manager instance, passing a mock `Env` whose `getCatalogMgr()`/catalog/db chain resolves the used table
to a mock `MTMVRelatedTableIf` external table (Mockito `mock(PluginDrivenMvccExternalTable.class,
CALLS_REAL_METHODS)` or an interface stub) with a settable `getNewestUpdateVersionOrTime()`.
- **`NereidsSqlCacheManager.tablesOrDataChanged`** (the crux):
  - used-tables loop (`:475`): `PLUGIN_EXTERNAL_TABLE` used-table, token unchanged → `NOT_CHANGED`; token
    changed → `CHANGED_AND_INVALIDATE_CACHE`. **RED on HEAD** (type gate `:441` returns invalidate for
    `PLUGIN_EXTERNAL_TABLE`).
  - **partition-existence loop (`:506`)**: a plugin table in `getScanTables()` with an *unchanged* token →
    `NOT_CHANGED`. **RED if the `:506` skip is reverted** to `!(instanceof HMSExternalTable)` — this is the
    easily-missed branch, so it gets its own assertion.
- **`SqlCacheContext.addUsedTable`**: records `getNewestUpdateVersionOrTime()` for an `MTMVRelatedTableIf`
  table; `token ≤ 0` → `hasUnsupportedTables()==true` and no entry; throw → unsupported. Keep `OlapTable`
  (`getVisibleVersion`) and any legacy HMS expectation byte-unchanged.
- **`CacheAnalyzer.buildCacheTableList`**: build with a Mockito `PluginDrivenScanNode` whose `getTupleDesc()`
  returns a `TupleDescriptor` bound to a mock `MTMVRelatedTableIf` table → all-external plan is recognized
  (non-empty list, `latestPartitionTime == getNewestUpdateVersionOrTime()`); a plan mixing that node with an
  `OlapScanNode` → `emptyList` (no federated caching); a `PluginDrivenScanNode` whose target table is a
  `FunctionGenTable` (jdbc TVF) → `emptyList` (capability gate excludes it). **RED on HEAD** (`instanceof
  HiveScanNode` never matches the plugin node).
- **Parity note (corrected):** the (currently `@Disabled`) `HmsQueryCacheTest` only pins the **L4**
  CacheAnalyzer decision (`buildCacheTableList` + stability-interval → `CacheMode`/`latestTime`) over a legacy
  `HMSExternalTable`; it does **not** exercise L1 admission or the invalidation re-check, and is not directly
  portable (it drives a real `HMSExternalCatalog`). It is *not* the invalidation oracle — the dedicated
  `NereidsSqlCacheManager` tests above are.
- Gate: `mvn -pl fe-core -am test-compile` + targeted `mvn test` green, 0 checkstyle, import gate clean.

## 8. e2e (user to run — BE cache-fill is only observable end-to-end)
- **hive PARTITIONED:** query-cache suite with `set enable_sql_cache=true` + `set enable_hive_sql_cache=true`:
  same query twice → 2nd is a cache hit (profile / `SqlCacheCounter`); then mutate (INSERT / add partition) →
  next query MISSES and returns fresh rows (proves token invalidation on a partition-only change — the exact
  stale-cache class this fix targets). Assert result equality vs a no-cache run.
- **hive UNPARTITIONED:** same hit-then-invalidate cycle — this exercises the `queryTableFreshness()`
  handle-only token path (distinct from the partition-max path) and confirms the `token ≤ 0` fail-safe does
  not spuriously disable a real unpartitioned table (assert a non-zero token / an actual cache hit).
- **iceberg / paimon / hudi (Option A newly-eligible):** the same hit-then-invalidate cycle on one table each,
  to validate the snapshot-version token path and the BE fill for non-hive lakehouse tables (memory
  `hms-iceberg-delegation-needs-e2e`). **hudi** specifically confirms the target-table-capability gate (not
  the dead `HudiScanNode extends HiveScanNode` path).
- **negative:** a `jdbc_query(...)` TVF and a `$partitions` sys-table query must NOT be cached (no crash, no
  stale) with the switch on.
```
