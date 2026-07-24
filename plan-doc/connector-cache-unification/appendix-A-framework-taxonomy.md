I have all the grounding I need. Here is the taxonomy.

---

# Taxonomy of the "framework" introduced by `0b4f72582e7`, at HEAD (`branch-catalog-spi`)

Every file:line below is verified against HEAD, not against the commit or plan-docs.

Scope note: the reference commit has three layers. Grounding at HEAD confirms the commit's own framing but sharpens one point — **Layer 2 (the funnel) is 100% connector-agnostic and already live for all 7 `SPI_READY_TYPES`; Layer 1 (the 6 caches) and one of the two gates are iceberg-only *implementations of a replicable pattern*, and the substrate to generalize them (`fe-connector-cache`) already exists and is already partly adopted.**

---

## 1. Layer 2 — the connector-agnostic per-statement `ConnectorMetadata` funnel

**What it is.** A per-statement memoization arena (the Doris analog of Trino's per-transaction `CatalogMetadata`) that guarantees *one `ConnectorMetadata` instance per (statement, catalog)*, so every read / scan / DDL / MVCC / write resolver in a statement shares one metadata — and, for a connector that memoizes its loaded table on that metadata, one loaded remote table.

Pieces, at HEAD:

- **SPI seam (neutral):** `fe-connector-api/.../connector/api/ConnectorStatementScope.java`. Interface with `computeIfAbsent` (line 45), typed `getOrCreateMetadata` default (lines 55-57), best-effort idempotent `closeAll` default no-op (lines 66-67), and the `NONE` no-op scope that memoizes nothing (lines 70-75). Reached from a connector purely via `ConnectorSession.getStatementScope()`, whose SPI default is `NONE` (`ConnectorSession.java:142-144`) — so any connector that does nothing still compiles and behaves byte-identically to "load every time."
- **The funnel itself:** `fe-core/.../datasource/plugin/PluginDrivenMetadata.get(session, connector)` (lines 53-71). Keyed `"metadata:" + catalogId` (line 70); builds via `connector.getMetadata(session)` at most once per statement. It also pins the *building identity* under `"metadata-identity:" + catalogId` and throws `IllegalStateException` if a second Doris principal reuses the instance (lines 63-69) — turning a would-be cross-user credential reuse into a hard error. This is the **only** file in fe-core allowed to call `Connector#getMetadata` (enforced by gate §5).
- **The arena impl:** `fe-core/.../connector/ConnectorStatementScopeImpl.java`. A `ConcurrentHashMap` (line 44) so off-thread scan pumps that reuse the one session reach the same scope; `computeIfAbsent` (lines 49-51); **two-tier `closeAll`** (lines 64-98, guarded by `closed` for idempotency): pass 1 finalizes any `CatalogStatementTransaction` (rolls back an uncommitted txn), pass 2 closes every remaining `AutoCloseable` value (the memoized metadata) — txn finalized *before* the metadata it was minted from is closed.
- **The physical home:** `fe-core/.../nereids/StatementContext.java`. Field `connectorStatementScope` (line 209); lazily built by `getOrCreateConnectorStatementScope()` (lines 656-661); dropped/closed per prepared-statement execution by `resetConnectorStatementScope()` (lines 672-677); **fallback close** in `close()` for statements that never reach a coordinator (external DDL/SHOW/DESCRIBE/EXPLAIN/foreground ANALYZE), guarded by `isReturnResultFromLocal` (lines 995-997).
- **Write-transaction co-holder:** `fe-core/.../datasource/plugin/CatalogStatementTransaction.java`. Minted from the *shared* metadata's write facet via `begin()` (lines 80-84); `finalizeAtStatementEnd()` is the deterministic backstop (lines 100-107). Connector-agnostic — traffics only in neutral `ConnectorWriteOps`/`ConnectorSession`/`ConnectorTransaction` SPI types.

**Lifecycle** (open → memoize → two-tier close):
1. **Open:** `ConnectorSessionBuilder.captureStatementScope()` (`ConnectorSessionBuilder.java:190-203`) reads the scope off the live `ConnectContext`'s `StatementContext` at session-build time (request thread). **This is *not* gated by connector type** — there is no `iceberg`/type branch; any session built from a live statement gets the real scope, else `NONE`.
2. **Memoize:** every resolver calls `PluginDrivenMetadata.get(...)`. Verified breadth at HEAD: **58 call sites across 15 fe-core files** (`PluginDrivenExternalCatalog/Table/Database`, `PluginDrivenMvccExternalTable`, `PluginDrivenScanNode`, `PluginDrivenInsertExecutor`, `BindSink`, `PhysicalPlanTranslator`, `PhysicalExternalRowLevelMergeSink`, `ShowPartitionsCommand`, `IcebergRowLevelDmlTransform`, `ConnectorExecuteAction`, `CallExecuteStmtFunc`, `MetadataGenerator`, `JdbcQueryTableValueFunction`).
3. **Close (two-tier):** primary is the getSplits query-finish callback — `PluginDrivenScanNode` registers `statementScope::closeAll` via `QeProcessorImpl.registerQueryFinishCallback`, skipping `NONE` (lines 1235-1237); fallback is `StatementContext.close()` (line 995). Both idempotent.

**Cross-statement background loaders** are deliberately forced through a `NONE` scope (a read-through, not a hazard): `PluginDrivenExternalCatalog` builds their sessions `.withStatementScope(ConnectorStatementScope.NONE)` (lines 1161, 1168), so a synchronous background `fetchRowCount` cannot capture the live statement's scope.

**Two connector-agnostic memos ride on top** (both byte-identical to re-resolving):
- `PluginDrivenScanNode.resolveScanProvider()` memoizes the `(handle, provider)` pair keyed on `currentHandle` *identity* via the immutable `ResolvedScanProvider` holder (lines 245-275), so the per-split `getFileCompressType`/`getDeleteFiles`/`classifyColumn` hot path stops re-allocating + TCCL-swapping a provider per split. This is the "generic-node provider memo (C14)."
- `PluginDrivenScanNode.metadata()` caches this node's funnel result in a volatile field (lines 189, 203-210) so per-method resolvers don't re-hit the scope map.

**Heterogeneous-gateway extension (still Layer-2 mechanics, lives in the hive connector):** `HiveConnectorMetadata.memoizedSiblingMetadata()` (lines 302-304) memoizes the iceberg/hudi-on-HMS **sibling** metadata on *the same per-statement scope*, keyed `"metadata:" + catalogId + ":" + ownerLabel` (the three gateway connectors share one catalogId, so the owner label keeps them distinct — lines 293-295). It uses only `fe-connector-api` types (no fe-core dep).

**Does Layer 2 already apply to ALL of `SPI_READY_TYPES`?** **Yes, universally, with no iceberg gate.** `SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute, paimon, iceberg, hms}` (`CatalogFactory.java:56-57`); every one is a `PluginDrivenExternalCatalog` whose reads/scans go through `PluginDrivenExternalTable`/`PluginDrivenScanNode`, i.e. through the 58 funnel call sites; and `captureStatementScope()` has no type branch. hudi is not its own type — it is served only as a sibling under the hms gateway (`CatalogFactory.java:51-55`), and *that* path also rides the funnel via `memoizedSiblingMetadata`.

**Caveat worth stating plainly:** the funnel guarantees *one metadata per statement*, but whether that collapses repeated remote loads is **per-connector** — it only helps if that connector's `ConnectorMetadata` (or its long-lived `Connector`) actually memoizes the loaded table/schema. Layer 2 is the seam; Layer 1 is what fills it for iceberg. jdbc/es/trino have **no** connector-side cross-query cache at all (verified: no `MetaCacheEntry`/`CacheSpec`/Caffeine anywhere in `fe-connector-{jdbc,es,trino-connector}/src/main/java`), so for them the funnel collapses only the *within-statement* repeats, not cross-query.

---

## 2. Layer 1 — the six iceberg connector-side caches (a replicable *pattern*, catalogued)

All six live in `fe-connector-iceberg/.../connector/iceberg/`, are package-private `final`, and are **all built on the shared `MetaCacheEntry` substrate** (§3) with the identical construction shape `new MetaCacheEntry<>(name, null, spec, ForkJoinPool.commonPool(), false, true, 0L, true)` — contextual-only (caller supplies the loader per call), manual-miss-load (loader runs outside Caffeine's compute lock, single-flight per key, exception propagates unwrapped and un-cached). All are per-catalog, cross-query, and live on the long-lived `IcebergConnector` (a REFRESH/ADD/MODIFY CATALOG rebuilds the connector → drops the caches).

| # | Class (file) | Heavy op memoized | Key | Value | TTL | Invalidation | Scope | Authz gate (see §4) | Format-specific? |
|---|---|---|---|---|---|---|---|---|---|
| PERF-01 | `IcebergTableCache.java:59-117` | remote `loadTable` (metastore RPC + `metadata.json` read) | `TableIdentifier` (db.table) | **raw iceberg `Table`** | `meta.cache.iceberg.table.ttl-second`, default 24h; `≤0` disables | table / db(namespace) / all (lines 91-109) | cross-query **+** a per-statement "fat handle" (transient `resolvedTable`) | **null** under `session=user` **or** REST vended-creds (`IcebergConnector.java:243-247`) — value carries FileIO credentials | **Format-neutral in spirit** (every connector loads a table object); value type is iceberg-specific |
| PERF-02 | `IcebergPartitionCache.java:58-142` | the `PARTITIONS` metadata-table scan (SDK reads every data+delete manifest of the snapshot) | `(TableIdentifier, snapshotId)` | `List<IcebergRawPartition>` | same 24h knob; `≤0` disables | table / db / all (lines 116-129) | cross-query; shared by MVCC view, `listPartitions`, `SHOW PARTITIONS`, and the 4–6 re-enumerations of one MTMV refresh | **null** under `session=user` (`:254`) — pure metadata but authz-sensitive | **Iceberg-format-specific** (PARTITIONS metadata table, transform partitions) |
| PERF-03 | `IcebergFormatCache.java:61-145` | #64134's unfiltered whole-table `table.newScan().planFiles()` format fallback | `(TableIdentifier, snapshotId)` | format-name `String` | same 24h knob; `≤0` disables | table / db / all (lines 118-131) | cross-query | **null** under `session=user` (`:260`) | **Format-neutral in spirit** (file-format inference); bare `String` value |
| PERF-04 | `IcebergManifestCache.java:62-235` | parsing a manifest's files (remote FileIO read) | `IcebergManifestEntryKey` (manifest path + content) | `ManifestCacheValue` (copied `DataFile`/`DeleteFile` list) | **no TTL**, capacity-bounded (`100_000`), **REFRESH CATALOG only** (lines 224-227) | `invalidateAll` only; REFRESH TABLE deliberately keeps it (immutable manifests) | cross-query + cross-table (any table referencing the manifest) | **exempt** — default-off (`meta.cache.iceberg.manifest.enable`) + read after a per-user load (`IcebergConnector.java` marker `authz-cache-exempt`) | **Genuinely iceberg-format-specific** (manifest files) |
| PERF-05 | `IcebergCommentCache.java:53-111` | per-table `loadTable` that `information_schema.tables` / `SHOW TABLE STATUS` pays N-per-query for the comment | `TableIdentifier` | comment `String` | same 24h knob; `≤0` disables | table / db / all (lines 85-98) | cross-query | **built ONLY** for REST vended-creds **and NOT** `session=user` (`IcebergConnector.java:269-273`); plain catalogs reuse PERF-01 instead | **Format-neutral in spirit** (table comment is universal metadata); bare `String` |
| (survivor) | `IcebergLatestSnapshotCache.java:54-124` | `currentSnapshot()` + latest-schema read | `TableIdentifier` | `CachedSnapshot(snapshotId, schemaId)` | same 24h knob; `≤0` disables | table / db / all (lines 98-115) | cross-query; read by `beginQuerySnapshot` | **null** under `session=user` (`IcebergConnector.java:231-234`) | **Iceberg-specific** (snapshot id **+** schema id atomic pin — the one deviation from paimon's `long`-only mirror) |

Notes grounded at HEAD:
- The commit message says the P6 cutover "kept only the latest-snapshot pin" — so `IcebergLatestSnapshotCache` is the pre-existing survivor; PERF-01/02/03/05 restore the dropped halves and PERF-04 re-wires the opt-in manifest cache. At HEAD all six are the current family.
- Snapshot-keyed caches (PERF-02/03, latest-snapshot) are "always correct" because a snapshot is immutable and a new commit yields a new key; stability across queries comes from the latest-snapshot cache holding the snapshot id stable within the TTL.
- The **per-scan/per-file hoists** (PERF-06 storage-uri normalizer once per scan via `ConnectorContext.newStorageUriNormalizer`; PERF-11 per-file partition-JSON/values/delete-carrier memo) are part of the same *pattern* but are per-statement/per-scan hoists inside `IcebergScanPlanProvider`, not cross-query cache classes; the one fe-core piece of PERF-11 (the provider memo) is Layer-2 and already connector-agnostic (§1).

**Verdict on Layer 1:** it is a *pattern* (contextual `MetaCacheEntry` + snapshot/name key + REFRESH invalidation + authz gate), not universal code. Two members (manifest, PARTITIONS-scan) and the snapshot+schema pin are intrinsically iceberg-format-specific; three (table handle, comment, file-format) are format-neutral in intent and differ only in value type.

---

## 3. The pre-existing shared substrate `fe-connector-cache` — the natural generalization home

Module `fe/fe-connector/fe-connector-cache/.../connector/cache/`:

- **`CacheFactory`** — Caffeine factory; framework-internal (returns Caffeine types, which must never cross into child-first connector code). Connector-side copy of fe-core's `common.CacheFactory` (plugins can't import fe-core).
- **`CacheSpec`** — the `(enable, ttl-second, capacity)` config value; `fromProperties(props, engine, entry, default)` reads `meta.cache.<engine>.<entry>.*`; `CACHE_TTL_DISABLE_CACHE`(0)/`CACHE_NO_TTL`(-1) sentinels. This is exactly the knob every Layer-1 cache uses.
- **`MetaCacheEntry<K,V>`** — the Caffeine-**free** unified cache-entry API: lazy/contextual loading, manual-miss-load (I/O outside Caffeine's lock, striped per-key dedup), key/predicate/full invalidation, and stats (`getLoadSuccessCount`, backing the `loadCountForTest` measurement gates). This is the shared engine under all six iceberg caches.
- **`MetaCacheEntryStats`** — the stats/`isEffectiveEnabled` view.
- **`ConnectorPartitionViewCache<V>`** and **`PartitionViewCacheKey`** — an already-written **generic version of `IcebergPartitionCache`**: same contextual-only, manual-miss-load `MetaCacheEntry` shape, same `CacheSpec` wiring, but keyed by the engine-agnostic `PartitionViewCacheKey` with opaque value `V` and no engine imports. Its own javadoc states it "is the generic version of `IcebergPartitionCache`."

**Who uses the substrate today** (verified imports/instantiations across `fe-connector/*/src/main/java`):
- `MetaCacheEntry`: **hive** (`HiveFileListingCache`), **hms** (`CachingHmsClient`), **iceberg** (all six above), **maxcompute** (`MaxComputePartitionCache`), **paimon** (`PaimonLatestSnapshotCache`). **Not** jdbc / es / trino-connector (none).
- `ConnectorPartitionViewCache` — actual `new …()` sites: **hive** `HiveConnector.java:136`, **iceberg** `IcebergConnector.java:281` & `:284` (mvcc view + list-partitions view), **paimon** `PaimonConnector.java:158`.

**Is it the natural home to generalize Layer-1?** **Yes, and generalization is already underway.** The substrate exists, is Caffeine-free (safe across the loader split), reads a uniform `meta.cache.<engine>.<entry>.*` config namespace, and `ConnectorPartitionViewCache` is a *proof* that the exact iceberg pattern generalizes (it is `IcebergPartitionCache` with the key/value abstracted). The other five iceberg caches are structurally identical (all `new MetaCacheEntry<>(name, null, spec, commonPool, false, true, 0L, true)`), so `IcebergTableCache`/`IcebergCommentCache`/`IcebergFormatCache`/latest-snapshot could each collapse into a typed `ConnectorXViewCache` wrapper the same way.

**One drift flag (Rule 7/12):** `ConnectorPartitionViewCache`'s class javadoc says "this class has **NO consumers yet**" — that is **stale at HEAD**: three connectors (hive, iceberg, paimon) already instantiate it. Worth a doc-only fix.

---

## 4. Layer 3 — authorization cache isolation under `iceberg.rest.session=user`

The hazard: name-only-keyed caches would let a user who can `LIST` but not `LOAD` a table receive metadata a *different* user's `loadTable` produced ("list ≠ load" disclosure), because per-user authorization lives *inside* the delegated `loadTable` round-trip.

**iceberg-REST-specific mechanisms:**
- `IcebergConnector.isUserSessionEnabled()` (`:870-874`) = `rest.session == user` **and** flavor is REST.
- Under it, the connector **nulls** its live cross-query caches in the constructor: latest-snapshot (`:231`), table (`:243`, also nulled for REST vended-creds), partition (`:254`), format (`:260`), and both `ConnectorPartitionViewCache` instances (`:279`, `:282`). `IcebergCommentCache` is *only* built for REST-vended-and-not-session-user (`:269`). So under `session=user` every shared cache is off → `beginQuerySnapshot`/reads go live per-user. Each field carries the reviewed marker `authz-cache-session-user-disabled` or `authz-cache-exempt` (enforced by gate §5).

**fe-core-general mechanisms** (apply to *any* `SUPPORTS_USER_SESSION` connector, not just iceberg — iceberg-REST is simply the only one that currently declares the capability):
- `ExternalCatalog.shouldBypassSchemaCache(SessionContext)` — base default `false` (`ExternalCatalog.java:367-369`); the generic name-keyed schema cache is bypassed live when true.
- `PluginDrivenExternalCatalog` overrides it as `supportsUserSession() && ctx != null && ctx.hasDelegatedCredential()` (`:1212-1214`), with sibling `shouldBypassTableNameCache` (`:1190-1192`) and `shouldBypassDbNameCache` (`:1200-1202`) closing the same disclosure at the db-name/table-name-cache level. Consumed by `ExternalTable.java:439`.

So Layer-3 is split: the *policy predicate* (`shouldBypass*`, capability+credential gated) is connector-general fe-core; the *cache-nulling* is iceberg-connector-local because that's where the caches live.

---

## 5. The two anti-drift build gates

- **`tools/check-fecore-metadata-funnel.sh`** — **connector-GENERAL.** Fails the build on any executable `Connector#getMetadata(<arg>)` in `fe/fe-core/src/main/java` outside the one legal carrier `datasource/plugin/PluginDrivenMetadata.java` (allow-list: the funnel file; a `getMetadata-funnel-exempt` marker; no-arg `getMetadata()`; comment lines). Invariant enforced: *one `ConnectorMetadata` per (statement, catalog) for every connector* — i.e. Layer 2. Its root is `fe/fe-core`, no iceberg specificity (lines 20-59, 67-105).
- **`tools/check-authz-cache-sharding.sh`** — **iceberg-ONLY.** `TARGET_REL` is hard-coded to `fe-connector-iceberg/.../IcebergConnector.java` (line 57); it requires every `final …Cache[ <]` holder field there to carry `authz-cache-session-user-disabled` or `authz-cache-exempt` (lines 65-96). Invariant enforced: *no new shared cross-query cache on IcebergConnector without a declared `session=user` isolation discipline* — i.e. Layer 3, for iceberg. It explicitly notes the fe-core schema cache is a *different* cache protected separately by `shouldBypassSchemaCache` (lines 37-39). If a second connector ever declares `SUPPORTS_USER_SESSION`, this gate would need a new target — it does not generalize as written.

---

## 6. Bottom line

| Framework piece | Already universal? |
|---|---|
| Per-statement `ConnectorMetadata` funnel (`PluginDrivenMetadata`, `ConnectorStatementScope`+`Impl`, `StatementContext` home, two-tier close) | **Yes** — connector-agnostic; live for all 7 `SPI_READY_TYPES` via 58 fe-core call sites; scope capture has no type gate |
| `NONE` no-op scope + background-loader read-through | **Yes** — SPI default; used to shield cross-statement loaders |
| `CatalogStatementTransaction` (one write txn per statement, ordered teardown) | **Yes** — fe-core-internal, neutral SPI types only |
| `PluginDrivenScanNode` provider memo + per-node metadata cache (PERF-11 C14) | **Yes** — keyed on handle identity, no connector branch |
| HMS-gateway sibling metadata memo (`memoizedSiblingMetadata`) | **Yes as mechanism (rides the universal scope)**, but **lives in the hive connector** and is keyed for the iceberg/hudi-on-HMS sibling case |
| `fe-connector-cache` substrate (`CacheFactory`/`CacheSpec`/`MetaCacheEntry`/`Stats`) | **Yes** — shared; adopted by hive, hms, iceberg, maxcompute, paimon (not jdbc/es/trino) |
| `ConnectorPartitionViewCache` / `PartitionViewCacheKey` (generic partition-view cache) | **Yes (universal by design)** — already instantiated by hive, iceberg, paimon; javadoc "no consumers yet" is stale |
| The 6 iceberg caches — **table handle, comment, file-format** | **iceberg-only pattern**, format-neutral in spirit → the natural next candidates to fold into typed `ConnectorXViewCache` wrappers on the substrate |
| The 6 iceberg caches — **PARTITIONS-scan, manifest, latest-snapshot(+schemaId)** | **iceberg-only pattern**, genuinely iceberg-format-specific |
| Per-scan/per-file hoists (PERF-06 storage-uri normalizer, PERF-11 per-file invariant memo) | **iceberg-only** (inside `IcebergScanPlanProvider`); the SPI seam `ConnectorContext.newStorageUriNormalizer` is neutral but only iceberg consumes it |
| Layer-3 policy predicate `shouldBypass{Schema,TableName,DbName}Cache` (capability+credential gated) | **Partial → connector-general in fe-core**, but only iceberg-REST currently declares `SUPPORTS_USER_SESSION`, so only iceberg exercises it |
| Layer-3 cache-nulling under `isUserSessionEnabled()` | **iceberg-only** (the caches being nulled are iceberg-connector-local) |
| Gate `check-fecore-metadata-funnel.sh` | **Yes — connector-general** (enforces Layer 2 across all of fe-core) |
| Gate `check-authz-cache-sharding.sh` | **iceberg-only** (hard-coded to `IcebergConnector.java`; would need extension for any second user-session connector) |

**Net:** the *engine seam* (Layer 2 + its gate + the `fe-connector-cache` substrate + the generic partition-view cache + the general `shouldBypass*` predicate) is already universal and load-bearing for every plugin connector. What is iceberg-specific is the *filling*: the six concrete caches (three format-specific, three format-neutral-but-not-yet-generalized), the per-scan hoists, the cache-nulling, and the second gate. The substrate to universalize the format-neutral half already exists and is already adopted by three connectors.