# HMS cutover — catalog/table retype design (2026-07-07)

> Produced by a code-grounded recon (`wf_e0586006-60f`: 8 dimension readers + 2 critics — completeness + ordering/risk) with lead-engineer HEAD verification of the load-bearing facts. Supersedes the "6 independent sub-batches" framing: the recon proves the retype is **one atomic flip** with a large **dormant-precondition** build-out in front of it. **Trust HEAD, not doc line numbers.** Full recon detail: `tool-results/w0bg9i509.output` (per-dimension `capabilities`/`couplingSites`/`gaps`).

---

## 1. Goal / Non-goals

**Goal.** Make a `type=hms` catalog a `PluginDrivenExternalCatalog` holding a plugin `Connector` (`HiveConnectorProvider.getType()=="hms"` already exists), its databases `PluginDrivenExternalDatabase`, and its tables `PluginDrivenMvccExternalTable` — so the heterogeneous HMS catalog (plain-hive + iceberg-on-HMS + hudi-on-HMS) is served by the connector SPI and fe-core sheds every `instanceof HMSExternal*` / `switch(dlaType)`. This unblocks deleting `datasource/hive` + `datasource/hudi` + the ~23 `datasource/iceberg` HMS-support classes.

**Non-goals.** Full-ACID **write** stays hard-rejected (unchanged). Full-ACID + insert-only **read** stays in scope (plugin producer landed dormant, `0b19506acfe`). No new visible catalog type (D-020: one `hms` gateway, iceberg/hudi delegated internally, not a second catalog). No `if(format)`/`instanceof format`/`switch(dlaType)` in fe-core (iron rule); no property parsing in fe-core.

---

## 2. The shape of the work: dormant preconditions → one atomic flip → mechanical delete

The **ordering critic verdict** (confirmed against HEAD + paimon/iceberg precedent): the retype is **necessarily one atomic, catalog-wide switch**. It cannot be staged per-table or per-dimension, because dispatch is by **class identity** (`instanceof PluginDrivenExternalCatalog/Table`), not a runtime toggle. The moment `"hms"` enters `SPI_READY_TYPES` and the GSON tags remap, **every** hms catalog/db/table retypes at once across scan/write/DDL/stats/MVCC/GSON/event-poller/cache-routing.

Three regimes:

### (A) DORMANT — build ahead of the flip, zero user-visible effect
`CatalogFactory.java:103` never routes `hms` through the plugin path until `"hms"` is in `SPI_READY_TYPES`, so all connector-side scaffolding lands dormant (proven by P7.1 DDL / P7.3 write+ACID / P7.4 scan-seam). Everything in §4 is built here.

### (B) THE FLIP — one atomic commit (the "must-be-atomic set")
1. **3 GSON tag remaps** — delete `registerSubtype` + add `registerCompatibleSubtype` for catalog (`GsonUtils.java:366`), db (`:447`), table (`:471`). *These are mutually locked:* you cannot add the compat mapping without first deleting the `registerSubtype` (else `RuntimeTypeAdapterFactory:279` throws "labels must be unique" at static-init → FE won't boot); but deleting `registerSubtype` also kills the **write** registration, which crashes image-write for any live `HMSExternalCatalog` — and `CatalogFactory:134` keeps building those until `"hms"` is in `SPI_READY_TYPES`. **⇒ GSON remap and the SPI_READY flip must ship in the same commit** (a standalone "GSON-first" PR is impossible; paimon `a26eaeff84c` did exactly this in one commit).
2. `SPI_READY_TYPES += "hms"` (`CatalogFactory.java:49-50`) + delete the dead `case "hms"` fallback (`:133-135`).
3. `buildDbForInit` `case HMS` → `new PluginDrivenExternalDatabase` (`ExternalCatalog.java:966-968`, mirror the ICEBERG case).
4. Remove the now-dead `PhysicalPlanTranslator` HMS arms (`:818-846` scan, `:925-928` hudi) + the `DLAType` import (`:65`) — they become unreachable (the `:808` PluginDriven arm wins) but still compile-reference the legacy scan nodes, blocking their deletion.
5. Neutralize the **event-poller gate** (`MetastoreEventsProcessor:116`) and **cache-routing gates** (`ExternalMetaCacheRouteResolver:66`, `HiveExternalMetaCache:203/274`, `IcebergExternalMetaCache:234`) — all four go `false` at the flip **simultaneously and silently** (no crash, no log). Event sync would halt and cache-routing collapse to `ENGINE_DEFAULT` unless handled in the atomic set (or the flip is gated until §4's event/cache work lands). *This is the strongest proof the flip is atomic.*
6. Land `HmsGsonCompatReplayTest` (template: `IcebergGsonCompatReplayTest` / `PaimonGsonCompatReplayTest`).

### (C) DELETE — mechanical, one PR, one cyclic unit
`datasource/hive`, `datasource/hudi`, `datasource/iceberg` (legacy) **import each other cyclically** (`HudiScanNode extends HiveScanNode` :94; `HudiUtils`→`HMSExternalTable`; `IcebergHMSSource`/`IcebergScanNode`/`IcebergExternalMetaCache`→hive; `HMSExternalTable`→`IcebergDlaTable`/`HudiDlaTable`). **Hive cannot be deleted alone.** The deletion unit = `{datasource/hive legacy, datasource/hudi legacy, datasource/iceberg ~23 classes, statistics/HMSAnalysisTask, StatisticsUtil.getIcebergColumnStats + getHiveColumnStats, systable/IcebergSysTable}`, deleted together. (`fe-connector-hive` has **zero** imports of `datasource.hive` — the connector build-out is not entangled; only the fe-core legacy graph is cyclic.)

**Recommended PR shape:** paimon-style **two-PR split** — a revertable **flip-PR** (the atomic set in B) then a mechanical **delete-PR** (C). GSON single-row MVCC does **not** force a squash: `registerCompatibleSubtype` takes a label **string**, which survives class deletion.

---

## 3. The heterogeneity crux + the GSON single-row conflict (the central correctness problem)

One HMS catalog mixes non-MVCC plain-hive + MVCC iceberg/hudi-on-HMS, today carried by **one class** `HMSExternalTable` that implements `MvccTable`/`MTMVRelatedTableIf` for **every** `dlaType`, branching at runtime on a **lazily-probed, non-persisted** `dlaType`. Two independent facts force the **same** answer:

- **GSON:** the persisted `clazz` tag is uniformly `"HMSExternalTable"` (dlaType is transient — excluded by `HiddenAnnotationExclusionStrategy`, never in the JSON), so `registerCompatibleSubtype` can only map that **one label → one class**. Content-based dispatch is impossible in one release (no persisted discriminator).
- **buildTableInternal:** selects base-vs-Mvcc from the **catalog-level** `SUPPORTS_MVCC_SNAPSHOT` capability (`PluginDrivenExternalDatabase.java:53`), uniform for all tables in the catalog.

**⇒ Decision (evidence-forced, matches paimon/iceberg precedent at `GsonUtils:490/494`):** map `"HMSExternalTable"` → **`PluginDrivenMvccExternalTable`** AND have the hive gateway declare `SUPPORTS_MVCC_SNAPSHOT`; resolve hive/hudi/iceberg heterogeneity **at runtime inside the connector by table handle** (hive-handle path degrades MVCC to an empty pin; iceberg-handle delegates to the sibling). The GSON read-target and what `buildTableInternal` builds live **must agree** for round-trip consistency. The safety of the single base-tag mapping rests on the verified invariant that **deserialized external-table objects never survive a replay** (metaCache transient, `initialized` reset to false, tables rebuilt live via `buildTableInternal`, MTMV/BaseTableInfo use id-refs) — pinned by `HmsGsonCompatReplayTest`.

**Consequence to handle (MAJOR):** plain-hive now uses the Mvcc subclass, whose `loadSnapshot(empty,empty)` calls `materializeLatest()` (eager partition listing) vs the legacy `EmptyMvccSnapshot` (lazy). Mitigation: the hive gateway's `beginQuerySnapshot` returns empty for hive handles → empty pin → hive partition reads stay on the normal `listPartitions` path. And `getTableSnapshot` must become **freshness-kind-aware** (see §4 blocker) or MTMV over a hive base table never detects changes.

---

## 4. Dormant precondition build-out (regime A) — the real work

Ordered by the phase plan in §5. Blockers marked ⛔.

### 4.1 Connector auth + property parity (fe-connector-hive)
- ⛔ **Kerberos authenticator.** Today `HiveMetadataOps:73` builds its thrift client with `catalog.getExecutionAuthenticator()` — a **real** authenticator set by `HMSExternalCatalog.initPreExecutionAuthenticator:129`. `PluginDrivenExternalCatalog` deliberately does **not** override it (base no-op), and `HiveConnector:105` uses the raw `context::executeAuthenticated` — unlike `IcebergConnector` it neither builds a plugin-side authenticator nor wraps the context in `TcclPinningConnectorContext` (contrast `IcebergConnector:162-177,783-821`). **Secured HMS would silently degrade to SIMPLE auth.** Fix: give `HiveConnector` the iceberg treatment (lazy plugin-side `HadoopAuthenticator` + `TcclPinningConnectorContext`). **Gate: a Kerberos-HMS smoke test.**
- **Property parity moves connector-side** (fe-core stays property-agnostic): TTL range checks (`file.meta.cache.ttl-second` / `partition.cache.ttl-second`, `checkProperties:108`) → hms `ConnectorFactory.validateProperties`; the `ipc.client.fallback-to-simple-auth-allowed` default (`setDefaultPropsIfMissing:234`) → `HiveConnector.deriveStorageProperties`. Regression tests for both TTL error messages + mixed kerberos/simple auth.

### 4.2 Connector read-side SPI (fe-connector-hive is still a read-mostly skeleton)
- ✅ **`partition_columns` emission — DONE (`32b9526f689`, dormant).** `HiveConnectorMetadata.getTableSchema` did not mark which emitted columns are partition columns → `toSchemaCacheValue` derived **empty** partition columns → every partitioned hive/hudi table read as **unpartitioned** (wrong pruning/count, MTMV breakage). Fix landed = emit the cross-connector **`partition_columns` CSV-of-raw-names table-property** (the *unanimous* mechanism — verified emitted by paimon `PaimonConnectorMetadata:313`, iceberg `IcebergConnectorMetadata:443`, maxcompute `MaxComputeConnectorMetadata:163`; the design's earlier "add a first-class `partitionColumnNames` field, preferred over the string-property hack" was **wrong** — the property is the established convention and hive partition-key names are comma-free identifiers, so the field would only fork a working SPI). Connector also widens a `string` partition column to **`varchar(65533)`** — the exact `ScalarType.MAX_VARCHAR_LENGTH`, **not 65535** (the "65535" in this doc AND in the legacy `HMSExternalTable.java:835` comment are both stale; the constant is 65533) — replicating legacy `initPartitionColumns` (only `PrimitiveType.STRING` is coerced; non-string partition cols and plain string DATA cols keep their mapped types).
- ⛔ **Missing SPI methods:** `listPartitions` (real per-partition `lastModifiedMillis`), `getTableStatistics` (HMS-param rowCount + file-list estimate fallback + dataSize), `buildTableDescriptor` (`THiveTable`/`TTableType.HIVE_TABLE`), `listSupportedSysTables`, `viewExists`/`getViewDefinition` (Presto-view base64 parsing), and `getCapabilities`. Without these, `getNameToPartitionItems`/`fetchRowCount`/`toThrift`/`$partitions`/hive-views all degrade to empty on a flipped table.
- **`getCapabilities`** must declare `SUPPORTS_MVCC_SNAPSHOT` + the per-table caps the legacy `dlaType` gates imply. **⚠ Over-eligibility risk (completeness critic):** `SUPPORTS_TOPN_LAZY_MATERIALIZE` and `SUPPORTS_NESTED_COLUMN_PRUNE` are per-table **file-format** gated in legacy (orc/parquet only, views/hudi excluded); as catalog-wide flags they'd make text/json/csv/view/hudi tables over-eligible (nested prune on non-parquet can read NULL leaves). **⇒ these two need a per-handle format gate, not a blanket capability.**
- **Fail-loud parity:** `HiveTableFormatDetector.detect` returns `UNKNOWN` silently where legacy `supportedHiveTable()` throws `NotSupportedException` ("Unsupported hive input format"). Connector must fail loud in `getTableHandle`/`getTableSchema` (message aligned to connector wording).
- **Read file-format serde nuance (completeness critic):** legacy `getFileFormatType(SessionVariable)` has a `read_hive_json_in_one_column` session-var branch (+ a first-column-string `UserException`) and a `FORMAT_TEXT` vs `FORMAT_CSV_PLAIN` distinction the connector's `HiveFileFormat.detect`+`mapFileFormatType` collapses. Decide: reproduce (needs a session-var surface on `ConnectorSession`) or accept the behavior change (document).

### 4.3 Connector MVCC / MTMV / sys-table SPI
- ⛔ **`getTableSnapshot` freshness-kind-aware.** `PluginDrivenMvccExternalTable.getTableSnapshot` hardcodes `MTMVSnapshotIdSnapshot(snapshotId)`; for a plain-hive empty pin (`-1`) this is a **constant** → MTMV over a hive base table never detects change (**stale MV**). Fix: branch like `getPartitionSnapshot` — when the connector's freshness is `LAST_MODIFIED` (hive), emit a timestamp snapshot (`MTMVTimestampSnapshot` partitioned; `MTMVMaxTimestampSnapshot`-equivalent from a connector-supplied table-level lastDdlTime unpartitioned). Requires the connector to surface table last-DDL/modified time.
- **Sys tables:** iceberg-on-HMS `$snapshots` already throws today (`IcebergSysTable.createSysExternalTable`) → the connector-driven native path is a net improvement. Hive `$partitions` is a legacy **TVF** (`PartitionsSysTable`) — the fe-core resolver supports both native + TVF; decide keep-TVF-routed vs expose-native.

### 4.4 Cross-plugin sibling-connector SPI (iceberg-on-HMS delegation)
- ⛔ **`ConnectorContext.createSiblingConnector(String type, Map props)`** (0 hits today). Default: `throw UnsupportedOperationException` (non-gateway connectors unaffected). Implement in `DefaultConnectorContext` (same fe-core pkg as `ConnectorFactory`) over `ConnectorFactory.createConnector(type, props, this)` → builds a real `IcebergConnector` in the **iceberg plugin's own child-first classloader** (co-packaging iceberg into the hive zip is rejected — 2nd AWS SDK poisons S3). All three TCCL pin loci are then covered **for free** because the sibling is a real `IcebergConnector` (scan-thread auto-pin via provider classloader; write/DDL via the sibling's own `TcclPinningConnectorContext`; worker pool via `pinIcebergWorkerPoolToPluginClassLoader`).
- **Property synthesis (plugin-side):** gateway injects `iceberg.catalog.type=hms` (`IcebergConnectorProperties.TYPE_HMS`) + shares hms metastore/storage/kerberos props before building the sibling.
- **Gateway-owned wrapper handle** (`HmsGatewayTableHandle{ HiveTableType format; ConnectorTableHandle delegate }`): the gateway (hive loader) must **never** `instanceof`/cast the foreign `IcebergTableHandle` (iceberg loader). All dispatch branches on the gateway-owned format enum (same loader) and passes the unwrapped delegate to the sibling.
- **Per-table metadata + write/procedure delegation** (seam A is scan-only): the gateway's `ConnectorMetadata` delegates `getTableHandle`/schema/MVCC internally keyed on `HiveTableFormatDetector` (no new engine seam). For write/procedure (which carry no handle today): add default handle-aware overloads `getWritePlanProvider(handle)`/`getProcedureOps(handle)` mirroring seam A (recommended for symmetry), or a gateway dispatching provider.

### 4.5 Write-chain + read-txn rewire
- The plain-hive **write** path is already class-forked (`UnboundTableSinkCreator`/`BindSink`/`PhysicalPlanTranslator`), so the retype makes it fall through to the live `UnboundConnectorTableSink → PluginDrivenTableSink → PluginDrivenInsertExecutor` driven by the dormant `HiveWritePlanProvider`/`HiveConnectorTransaction` — no new fe-core branches. The whole legacy sink chain becomes deletable.
- ⛔ **Read-ACID query-finish commit is unwired plugin-side.** `HiveScanPlanProvider.planAcidScan` registers a `HiveReadTransaction` (read lock + write-id snapshot) but nothing deregisters/commits it. Fix (fe-core-driven, keeps `ConnectorSession` free of query lifecycle): when the plan provider opens a read txn, `PluginDrivenScanNode` registers a `QueryFinishCallbackRegistry` callback → new `ConnectorScanPlanProvider.releaseReadTransaction(queryId)` → `HiveReadTransactionManager.deregister` (mirrors how `PluginDrivenInsertExecutor` drives the write commit).
- **Two hive write pre-checks** (`BindSink`: reject explicit-partition-spec INSERT `:668`; reject LZO read-only `:678`) → push into the hive connector write path (fail-loud), keeping fe-core connector-agnostic.
- **Env-held read txn mgr** (`Env.hiveTransactionMgr` + accessors `:568/865/1097/1101`) → delete at the read cutover once legacy `HiveScanNode` (its only caller) is deleted; the plugin `HiveReadTransactionManager` owns it per connector.
- **`BIND_BROKER_NAME`** constant (`HMSExternalCatalog:60`, referenced by `ExternalCatalog.bindBrokerName():1320`) → relocate to `BrokerProperties.BIND_BROKER_NAME_KEY` (already the identical `broker.name` key); drop the HMS copy.
- **`pluginCatalogTypeToEngine`** (`CreateTableInfo`) has no `"hms"` entry → returns null → no-ENGINE CREATE TABLE throws. Add `case "hms": return ENGINE_HIVE;`.

### 4.6 Coupling-site neutralization (65 sites; NONE need an `if(format)` to survive)
Every `instanceof HMSExternal*` goes false at the flip and the PluginDriven arm already beside most sites takes over. Residual sites needing a **connector capability/SPI** (not a branch): `canSample`/`isSamplingPartition` (add `SUPPORTS_SAMPLE_ANALYZE`); `partition_values()` TVF (add PluginDriven to the gate + `MetadataGenerator` case); SQL-cache version tracking (generalize the type gate to plugin tables with `getUpdateTime()`); partition-level cache invalidation (add `Connector.invalidatePartition(s)` — pairs with the event pipeline); hive-view config gate (`enable_query_iceberg_views` is iceberg-specific — carry the view-enable/dialect per-connector); SHOW PARTITIONS / SHOW CREATE (connector partition-list / show-create SPI or accept generic rendering). Full list + file:line in the recon output `couplingSites`.

### 4.7 Event pipeline relocation (Model B — flip-gated)
Covered by `hms-event-pipeline-findings-2026-07-07.md`. Thin fe-core role-aware `MasterDaemon` driver + plugin `pollOnce` SPI returning neutral change-descriptors (must cover register/unregister/rename + partition-NAME granularity). fe-core wraps `pollOnce` in an `onPluginClassLoader` pin (R-010). The one poller gate (`MetastoreEventsProcessor:116`) is part of the atomic flip set. `ExternalMetaIdMgr` can be dropped for HMS (ids name-derived) but opcode `470` + a neutral replay handler must survive on-disk (replay-CCE fix already landed `a6dc782d816`).

---

## 5. Internal phase ordering (hard-sequenced)

1. **Connector SPI additions (dormant)** — 4.1 auth+props, 4.2 read-SPI, **4.2a per-column stats SPI (D1=preserve)**, 4.3 MVCC/sys, 4.4 sibling-SPI + gateway delegation, 4.5 read-ACID wiring + write pre-checks + BIND_BROKER + engine-map. Each lands as an independent dormant commit. *Constraints:* sibling-SPI before iceberg-on-hms routing; MVCC capability declaration before the flip or iceberg/hudi lose MVCC.
2. **Coupling-site capability seams (dormant)** — 4.6 add the connector capabilities/SPI methods + the PluginDriven arms beside each HMS arm (still dormant — HMS tables aren't PluginDriven yet).
3. **Event pipeline Model B (dormant driver + plugin SPI)** — 4.7, ready so the poller keeps flowing at the flip.
4. **THE FLIP (atomic commit)** — §2.B, including neutralizing the event/cache gates + dead translator arms + `HmsGsonCompatReplayTest`.
5. **DELETE (mechanical PR)** — §2.C cyclic unit.
6. **Hard e2e gate (R-002)** — §7.

*Global constraints:* GSON remap atomic-with the flip (not earlier); the flip is one commit; delete strictly last; the event + cache-routing work must be **in** the flip set or the flip is gated until it lands.

---

## 6. Decisions — LOCKED (user sign-off 2026-07-07)

- **D1 — Hive metadata statistics fast-path → PRESERVE via new SPI (option b).** Add a `ConnectorStatisticsOps.getColumnStatistics(session, handle, colName)` SPI returning a neutral column-stat DTO (count/ndv/numNulls/dataSize/avgSize) + a `PluginDrivenExternalTable.getColumnStatistic` override. The hive connector ports `getHiveColumnStats`/`setStatData` (no-scan HMS/spark col-stats, query-planning fast path, `enable_fetch_iceberg_stats`-gated for the iceberg sibling); the iceberg connector ports `getIcebergColumnStats`. Table-level rowCount stays via `getTableStatistics` regardless. *(Chosen over dropping-for-parity: the no-ANALYZE fast path is kept high-value for hive.)* **⇒ this pulls the per-column stats SPI into the regime-A build-out (§4.2a).**
- **D2 — Cache ownership → CONNECTOR-OWNED (option a).** Retire the fe-core `HiveExternalMetaCache`/hudi/iceberg-on-hms meta caches; the connector owns scan-side caching (paimon/iceberg-native precedent). Delete the route/loader `instanceof HMSExternalCatalog` sites (`ExternalMetaCacheRouteResolver:66`, `HiveExternalMetaCache:203/274`, `IcebergExternalMetaCache:234`) + the partition-cache-inconsistency self-heal moves connector-side (or is dropped — decide at implementation). This is the largest catalog-adjacent structural item; it lands with the flip set.
- **D3 — Iceberg-on-HMS delivery → BUILD SIBLING SPI BEFORE THE FLIP (option a), no regression.** The cross-plugin sibling-connector SPI (§4.4) is a hard precondition and must land dormant before the flip so existing iceberg-on-HMS tables keep working. If it slips, the flip slips (no fail-loud-reject regression).
- **D4 — PR shape (decide at flip time).** two-PR split (paimon: revertable flip + mechanical delete) vs one squash (iceberg). **Leaning two-PR;** confirm at the flip.
- **D5 — External-view query-time enable gate → DROP entirely, Trino-aligned (user sign-off 2026-07-07).** At the flip, the `BindRelation` plugin-view arm (`:634`) stops gating on `Config.enable_query_iceberg_views` (and the dead HMS arm's `enable_query_hive_views` at `:594` goes away with the arm): an external view is served whenever `isView()` — no per-connector query-time toggle (Trino has none; the two Doris flags were legacy safety valves). Both `enable_query_hive_views` / `enable_query_iceberg_views` become deprecated no-ops (keep the `@ConfField` for one release to avoid "unknown config" on upgrade; remove later). Also neutralize the iceberg-worded time-travel rejection message + the stale `:626-632` invariant comment. **Consequence to bundle deliberately:** this ungates *iceberg* views too, which are already live behind `enable_query_iceberg_views` — so D5 is a **visible iceberg behavior change** and must ship with (or as a deliberate part of) the flip, not leak in early. Satisfies the iron rule with **no** `if(engine)`/OR-of-two-flags.
- **D6 — SHOW CREATE VIEW output → accept the plugin path's decoded form (user sign-off 2026-07-07).** Post-flip a hive view's SHOW CREATE renders the generic `CREATE VIEW <name> AS <getViewText()>` where `getViewText()` is the **Presto/Trino-decoded** SQL (legacy HMS `HiveMetaStoreClientHelper.showCreateTable` emitted the **raw encoded** `/* Presto View: <b64> */`). The decoded form is more readable and is the accepted output; any hive-view SHOW CREATE regression baseline that pinned the raw-encoded bytes is updated to the decoded text at the flip.

### §4.2a — Per-column statistics SPI (from D1=preserve)
- Add `ConnectorStatisticsOps.getColumnStatistics(session, handle, colName)` → neutral column-stat DTO. `PluginDrivenExternalTable.getColumnStatistic` override consults it (query-planning cache-miss fast path). Hive connector ports the HMS/spark col-stats read (incl. partition-level col stats `getHivePartitionColumnStats` for the ANALYZE metadata path); iceberg connector ports `getIcebergColumnStats` behind `enable_fetch_iceberg_stats`. This also gives `HMSAnalysisTask`'s metadata fast path a home (revisit whether `ExternalAnalysisTask` consults the new SPI vs SQL-only FULL analyze — the DTO makes preserving the metadata path feasible).

---

## 7. Success criteria / hard gate (R-002 — do not silently skip, Rule 12)

Docker e2e over a **live** path: (1) transactional-hive read (full-ACID+insert-only in scope; full-ACID write hard-rejected); (2) HMS event replay keeps metadata in sync post-flip; (3) **one heterogeneous catalog** holding plain-hive + iceberg-on-HMS + hudi-on-HMS, each queried through the right provider; (4) MTMV over iceberg-on-HMS + hive base tables (freshness), time-travel; (5) Kerberos-HMS smoke (§4.1). Plus: clean-room adversarial review + ENG-1 capability-twin audit over the coupling sites + `HmsGsonCompatReplayTest`. Precedent: iceberg #64688 / paimon #64446+#64653.

---

## 8. References
- Recon output (full per-dimension detail): `tool-results/w0bg9i509.output`; workflow `wf_e0586006-60f`.
- `iceberg-on-hms-delegation-findings-2026-07-07.md` (sibling handoff, deferred deletions).
- `hms-event-pipeline-findings-2026-07-07.md` (Model B, event descriptors).
- Precedents: `P5-paimon-migration.md` (two-PR flip+delete, GSON), `P6-iceberg-migration.md` + `P6.6-iceberg-flip-blockers-tasklist.md` (one-squash, GSON replay, clean-room). Seam template: `P7.4-scan-provider-per-table-seam-design.md`.
- Tracking: apache/doris#65185.
