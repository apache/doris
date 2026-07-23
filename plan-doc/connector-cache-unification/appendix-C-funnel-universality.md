All evidence is grounded. Here is the report.

---

# Verdict on the CLAIM

**CLAIM:** "The per-statement `ConnectorMetadata` funnel (Layer 2) already routes EVERY plugin connector, so all connectors already share one metadata per statement — the only per-connector gap is whether each connector's own `ConnectorMetadata` memoizes its heavy loads."

**Verdict: CONFIRMED on the routing half; DIRECTIONALLY CORRECT but imprecise on the "memoizes its heavy loads" half.**

- **Half 1 (routing) — TRUE.** The funnel `PluginDrivenMetadata.get(ConnectorSession, Connector)` is fully connector-agnostic: it keys the memo on `catalogId` only and has **no connector-type branch** (`PluginDrivenMetadata.java:53-70`). Every fe-core seam — read, scan, DDL, MVCC, write/insert — acquires metadata through it, for all 8 SPI types (`jdbc, es, trino-connector, max_compute, paimon, iceberg, hms`, + hudi as an HMS-gateway sibling). Enforced build-wide by `tools/check-fecore-metadata-funnel.sh` (gate passes, `exit=0`; **0** remaining exempt markers).

- **Half 2 (the gap) — needs correction.** It is true that the open per-connector question is memoization. But the CLAIM's phrase "each connector's *own* `ConnectorMetadata` memoizes" is **misleading: no connector stores the loaded table as a `ConnectorMetadata` instance field.** The load-collapsing lives either **one level up** (iceberg → the per-statement scope; hive/hudi/paimon → connector-owned cross-query caches) or **one level down** (paimon/maxcompute/trino → fat handle). Consequently, "one metadata per statement" translates into "one remote load per statement" **only for Iceberg** — the one connector that binds the memo to `session.getStatementScope()`. For every other connector the collapse happens at a *different granularity* (per-handle, or cross-query TTL), which does **not** guarantee one-load-per-statement.

---

## 1. The funnel is connector-general and covers all seams

`PluginDrivenMetadata.get` (`fe/fe-core/.../datasource/plugin/PluginDrivenMetadata.java:53-70`) memoizes `connector.getMetadata(session)` on the statement scope keyed by `"metadata:" + catalogId`; the only conditional is a fail-loud **identity** guard (`getUser` mismatch), not a connector-type branch.

| Seam | Callsites through the funnel (file:line) |
|---|---|
| Read path | `PluginDrivenExternalTable.java:133,159,189,449,565,583,606,716,820,881,923,954,1025,1060,1126,1277` |
| Scan planning | `PluginDrivenScanNode.java:206,222` |
| DDL (catalog/table) | `PluginDrivenExternalCatalog.java:296,312,333,423,502,551,599,671,715,808…1019,1107,1113`; `ConnectorExecuteAction.java:129`; `CallExecuteStmtFunc.java:102` |
| MVCC | `PluginDrivenMvccExternalTable.java:148,363,782` |
| Write / insert | `PluginDrivenInsertExecutor.java:219`; `BindSink.java:674,713`; `PhysicalExternalRowLevelMergeSink.java:289`; `IcebergRowLevelDmlTransform.java:124` |
| Translation / TVF / SHOW | `PhysicalPlanTranslator.java:613,661`; `MetadataGenerator.java:1266`; `JdbcQueryTableValueFunction.java:53`; `ShowPartitionsCommand.java:286` |

**Connector-type branch inside the funnel: NONE.** The only `switch(catalogType)` in the plugin classes (`PluginDrivenExternalTable.java:1211-1258`) is purely cosmetic — user-visible engine names for `SHOW TABLE STATUS`/`information_schema` — not metadata routing. The one heterogeneous case is the **HMS gateway**, which keys its *sibling* memo by owner label inside `fe-connector-hive` (`HiveConnectorMetadata.memoizedSiblingMetadata:302`), not in fe-core.

## 2. What the gate enforces — connector-general

`tools/check-fecore-metadata-funnel.sh` greps **any** `.getMetadata(<arg>)` under `fe/fe-core/src/main/java` and fails the build unless it is (1) inside `PluginDrivenMetadata.java` itself, (2) marked `getMetadata-funnel-exempt`, (3) a no-arg `getMetadata()`, or (4) a comment. It is entirely connector-agnostic (matches the SPI call *form*, no connector names). Currently **0** exempt markers remain — the write arm is fully funneled — and the gate exits 0.

## 3. Per-connector funnel-memoization table

The funnel guarantees **one `ConnectorMetadata` instance per statement per catalog** for all connectors. Whether that collapses repeated remote loads to one-per-statement depends on where each connector puts its memo:

| Connector | Funneled? | Does its ConnectorMetadata collapse repeated table loads to **one per statement**? | Mechanism & evidence (file:line) |
|---|---|---|---|
| **Iceberg** (ref) | Yes | **YES** — the only true per-statement collapse | `resolveTableForRead` routes through `IcebergStatementScope.sharedTable(session,…)` = `session.getStatementScope().computeIfAbsent(key, loader)` (`IcebergStatementScope.java:59-66`; `IcebergConnectorMetadata.java:644-650`). Plus cross-query `IcebergTableCache` (`:646-648`). One `loadTable` per statement even across distinct `getTableHandle` calls. |
| **Paimon** | Yes | **PARTIAL** — per-*handle*, not per-statement | Fat handle: `getTableHandle` loads + `handle.setPaimonTable(table)` (`PaimonConnectorMetadata.java:211,221`); `resolveTable` prefers `handle.getPaimonTable()` (`PaimonTableResolver.java:63-67`). No statement-scope memo → each *fresh* `getTableHandle` re-issues `catalogOps.getTable`; only paimon's own cross-query `CachingCatalog` (TTL `meta.cache.paimon.table.ttl-second`) dedups across handles. |
| **MaxCompute** | Yes | **PARTIAL** — per-*handle* | Fat handle carries the ODPS `Table`: `getTableHandle`→`getOdpsTable` into handle (`MaxComputeConnectorMetadata.java:124-129`); `getTableSchema` reads `mcHandle.getOdpsTable()` (`:136`). ODPS SDK lazily loads+caches schema on that `Table` object; no statement/instance memo across separate `getTableHandle` calls. |
| **Trino** | Yes | **PARTIAL** — per-*handle*, and re-opens a txn | `getTableHandle` eagerly bakes trino handle + column-handle/metadata maps into `TrinoTableHandle` (`TrinoConnectorDorisMetadata.java:166-179`). `getTableSchema` reuses the handle's `columnHandleMap` (`:200`) but **re-opens a fresh trino transaction and re-calls `getColumnMetadata` per column** (`:194-209`). Each `getTableHandle`/`getTableSchema` = a new `beginTransaction`. |
| **Hive** | Yes | **NO instance/statement memo** — relies on connector-owned cross-query cache | `getTableHandle` (`HiveConnectorMetadata.java:399`) and `getTableSchema` (`:493`) each call `hmsClient.getTable(db,table)`. Collapse lives in the cross-query `CachingHmsClient` decorator keyed by `(db,table)` (`CachingHmsClient.java:53`), owned by the long-lived connector, TTL `meta.cache.hive.*`. Cache-disabled ⇒ live thrift RPC per call; no per-statement guarantee. |
| **Hudi** | Yes (HMS sibling) | **NO** | HMS part cached via `CachingHmsClient` (`getTableHandle`→`hmsClient.getTable`, `HudiConnectorMetadata.java:207`), but the schema read **rebuilds `HoodieTableMetaClient` and `reloadActiveTimeline()` on every** `getSchemaFromMetaClient` call (`:797-806`). No memo of the metaClient/schema. |
| **Jdbc** | Yes | **NO** | `getTableHandle` is a lightweight existence check (`JdbcConnectorMetadata.java:102`). `getTableSchema`→`client.getJdbcColumnsInfo` runs a live JDBC `DatabaseMetaData` column query with **no cache** (`JdbcClient.getJdbcColumnsInfo`, `JdbcClient.java:395`). (Normally served by fe-core's cross-statement schema cache, not the metadata instance.) |
| **Es** | Yes | **NO** | `getTableSchema`→`restClient.getMapping(index)` per call (`EsConnectorMetadata.java:85`); `getColumnHandles` re-invokes `getTableSchema` (`:99`). No memo. |

## 4. The "9 cross-statement background loaders forced through NONE-scope" — connector-general

They route through `PluginDrivenExternalCatalog.buildCrossStatementSession()` (`:1153-1170`), which forces `ConnectorStatementScope.NONE`. Exactly **9** callers, all in connector-agnostic fe-core base classes (no connector-type branch):

1. `PluginDrivenExternalTable.initSchema` (`:448`) — schema cache
2. `PluginDrivenExternalTable.getColumnStatistic` (`:1024`) — column-stat cache
3. `PluginDrivenExternalTable.getChunkSizes` (`:1059`)
4. `PluginDrivenExternalTable.fetchRowCount` (`:1125`) — row-count cache (the fix for `ANALYZE`'s synchronous `fetchRowCount` capturing a live statement scope)
5. `PluginDrivenExternalCatalog.listDatabaseNames` (`:295`) — db-name cache
6. `PluginDrivenExternalCatalog.listTableNamesFromRemote` (`:311`) — table-name cache
7. `PluginDrivenExternalCatalog.fromRemoteDatabaseName` (`:1106`)
8. `PluginDrivenExternalCatalog.fromRemoteTableName` (`:1112`)
9. `MetadataGenerator.dealPluginDrivenCatalog` (`:1265`) — the BE-driven metadata TVF

Under `NONE` the funnel memoizes nothing (`PluginDrivenMetadata.java:62`; `ConnectorSessionBuilder.captureStatementScope:196,200`), so these fill caches that outlive any statement without ever binding into (or being closed with) a live statement's scope. **Connector-general, not iceberg-specific.**

---

## Surprises / corrections to the CLAIM

1. **No connector memoizes on the `ConnectorMetadata` instance itself.** The CLAIM's mental model ("each connector's own ConnectorMetadata memoizes its heavy loads") does not match the code: the memo is always one level up (iceberg statement scope; hive/hudi/paimon connector-level cross-query caches) or one level down (paimon/maxcompute/trino fat handle). "One metadata per statement" is therefore **necessary but not sufficient** — a shared metadata instance with no memo still re-loads.

2. **Only Iceberg realizes the funnel's promise (one load per statement).** It is the sole connector that ties the collapse to `session.getStatementScope()`. Every other connector's "collapse" is either per-handle (breaks across distinct `getTableHandle` calls from different fe-core entrypoints — e.g. `initSchema`, `listPartitions`, `getColumnStatistics`, scan planning each re-resolve a handle) or cross-query TTL (breaks when the connector's cache is disabled). So the per-connector gap is not merely "add a cache" — for parity with iceberg they would need a **statement-scoped** memo, not just any memo.

3. **Trino is internally inconsistent:** it bakes a full `columnMetadataMap` into the fat handle at `getTableHandle` (`:178`) yet `getTableSchema` ignores it and re-derives column metadata through a fresh transaction (`:206-209`) — a redundant re-fetch even when the handle already carries the answer.

4. **Hudi is the weakest:** even within one handle it rebuilds the `HoodieTableMetaClient` and force-reloads the timeline on every schema read (`:800-806`) — a Problem-Class-A loop-amplified heavy op with no hoist, the closest analog to the pre-cutover iceberg regressions this framework was built to fix.