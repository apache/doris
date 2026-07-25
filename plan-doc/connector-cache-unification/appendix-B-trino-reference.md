I have everything grounded. Returning the analysis.

---

# Trino connector-metadata caching & per-transaction lifecycle — mapped onto the Doris catalog-SPI framework

Scope note: every Doris claim below is hung on HEAD (`branch-catalog-spi`, reference commit `0b4f72582e7`) with `file:line`. Trino claims are grounded in the actual current source I fetched (`InMemoryTransactionManager`, `CatalogMetadata`, `CatalogTransaction`, `SharedHiveMetastoreCache`); where I rely on architectural knowledge rather than a fetched line I say so.

---

## 1. Trino's per-transaction metadata lifecycle, and how faithful the Doris Layer-2 funnel is

### 1a. What Trino actually does

The lifecycle unit in Trino is the **transaction**, not the session and not the statement. Three layers cooperate:

- **`InMemoryTransactionManager.TransactionMetadata`** holds `activeCatalogs`, a `ConcurrentHashMap<CatalogHandle, CatalogMetadata>`. `getTransactionCatalogMetadata(catalogHandle)` lazily does `catalog.beginTransaction(...)` on first touch of a catalog and caches the resulting `CatalogMetadata` for the transaction; `asyncCommit()`/`asyncAbort()` iterate `activeCatalogs.values()` and `commit`/`abort` each exactly once. (fetched from `InMemoryTransactionManager.java`.)
- **`CatalogMetadata`** wraps up to three `CatalogTransaction`s (the normal catalog, `information_schema`, system tables). `getMetadata(session)` returns the normal one; `getMetadataFor(session, handle)` routes by handle. (fetched from `CatalogMetadata.java`.)
- **`CatalogTransaction`** is where the one-metadata-per-transaction guarantee physically lives:
  ```
  @GuardedBy("this") private ConnectorMetadata connectorMetadata;
  synchronized ConnectorMetadata getConnectorMetadata(Session session) {
      if (connectorMetadata == null) {
          ConnectorSession cs = session.toConnectorSession(catalogHandle);
          connectorMetadata = connector.getMetadata(cs, transactionHandle);
      }
      return connectorMetadata;
  }
  ```
  and `commit()`/`abort()` are gated by an `AtomicBoolean finished` so the connector transaction finishes once. (fetched from `CatalogTransaction.java`.)

Two structural properties fall out of this and matter for Doris:

1. **The `ConnectorMetadata` instance lives for the whole transaction and is the memo home.** Because Trino hands the connector one long-lived instance per transaction, connectors keep per-transaction working state *on that instance* (Iceberg's `IcebergMetadata` retains loaded tables/statistics; Hive's metadata holds a per-transaction metastore view). Repeated `resolveTable`/`getTableStatistics` within the transaction therefore collapse **for free** — the engine never needs a separate "load once per statement" seam.
2. **`getMetadata(session, transactionHandle)`** — the transaction handle is woven into the metadata at creation, so read and write are the same transaction by construction. `MetadataManager` (the engine's façade) routes every metadata call to `transactionManager.getCatalogMetadata(...).getMetadata(session)`, i.e. always through this cached instance.

### 1b. What Doris does, and whether the emulation is sound

Doris cannot use the session as the memo home, and the framework is explicit about why:

- `ConnectorSessionImpl` is **immutable** and is **rebuilt** at each seam (`ConnectorSessionBuilder`); the PR cites ~26 rebuilds/statement. I confirmed the load-bearing fact — the session is immutable (`ConnectorSessionImpl.java:36`, all fields `final`) and constructed on demand rather than retained — so it *cannot* carry mutable per-statement memo. I did not independently count 26.
- Instead the memo hangs on the one **`StatementContext`**: `getOrCreateConnectorStatementScope()` (`StatementContext.java:656`) lazily builds a `ConnectorStatementScopeImpl` (`StatementContext.java:209` field). The scope is a `ConcurrentHashMap<String,Object>` (`ConnectorStatementScopeImpl.java:44`).
- Each session **captures the scope reference at build time** (`ConnectorSessionBuilder.captureStatementScope()` :190–203) so off-thread scan pumps that reuse one session still reach the same scope even without a `ConnectContext` thread-local — the direct analog of Trino handing the same `CatalogTransaction` to every operation on the transaction.
- The funnel is **`PluginDrivenMetadata.get(session, connector)`** (`PluginDrivenMetadata.java:53`), which memoizes `connector.getMetadata(session)` under key `"metadata:"+catalogId` on the scope (:70). This is the Doris analog of `CatalogTransaction.getConnectorMetadata`. It is the *only* place fe-core may call `Connector#getMetadata` — enforced build-wide by `tools/check-fecore-metadata-funnel.sh` (present, executable). I count **59** call sites in fe-core routed through it.

**Assessment — is hanging the memo on `StatementContext` (not the session) sound?** Yes, with two caveats worth stating plainly:

- **Sound mechanically.** The session is the wrong home precisely because it is immutable and rebuilt; the `StatementContext` is the one object that lives exactly as long as the statement and is reachable from every seam. The `ConcurrentHashMap` + `computeIfAbsent` gives every caller of a key the same instance (required because scan and write both mutate the shared table/delete-supply). Teardown is deterministic and idempotent: `closeAll()` (`ConnectorStatementScopeImpl.java:65`) runs two ordered passes (finalize any `CatalogStatementTransaction`, then close every `AutoCloseable` metadata), fired from the `getSplits` query-finish callback (`PluginDrivenScanNode.java:1237`) with a `StatementContext.resetConnectorStatementScope()` fallback (`StmtExecutor.java:1077`, `ExecuteCommand.java:95`). This mirrors Trino's `activeCatalogs.values()` commit/abort sweep + `AtomicBoolean finished`.
- **Caveat 1 — statement ≠ transaction.** Trino's unit is the whole `BEGIN…COMMIT` block; a multi-statement Trino transaction shares one `ConnectorMetadata` (and one connector transaction) across all its statements. Doris's unit is **one statement** (the scope key includes `queryId`, e.g. `IcebergStatementScope.java:64`), and a prepared-statement `EXECUTE` explicitly *drops* the scope per execution (`resetConnectorStatementScope`). For external-catalog reads and autocommit external DML (iceberg/hive) this is equivalent, because each such statement is its own transaction. But the read-consistency-within-a-transaction guarantee Trino gives across a multi-statement block is, in Doris, only a within-*statement* guarantee. Fine today; document it so nobody assumes cross-statement metadata sharing.
- **Caveat 2 — the metadata carries no transaction handle.** Trino's `getMetadata(session, transactionHandle)` bakes the transaction in; Doris's `getMetadata(session)` does not, and recovers write-transaction coupling with a *separate* co-holder (`CatalogStatementTransaction`, see §4) plus a **fail-loud identity pin** (`PluginDrivenMetadata.java:63–69`): the memo records the building user and throws if a second identity reuses the instance. Trino never needs that check because a transaction is single-identity by construction; Doris adds it because one shared singleton connector serves all users and a `session=user` metadata bakes in a delegated credential. This is a reasonable adaptation, not a defect.

**Net:** the emulation is faithful in shape (lazy one-metadata-per-(statement,catalog), deterministic teardown, single routing funnel, arch-gate-enforced) and the choice to hang it on `StatementContext` is the correct — indeed the only — option given Doris's immutable-rebuilt session.

---

## 2. Cross-transaction connector caches: the long-lived-shared vs per-transaction-view split

### 2a. Trino

Trino keeps a **long-lived shared cache** separate from a **per-transaction view**, per connector:

- **Metastore.** `CachingHiveMetastore` is the shared, TTL-bounded cache (`getTable`, `getPartition(s)`, `getTableStatistics`/`getPartitionStatistics`, `listPartitionNames`). `SharedHiveMetastoreCache.ImpersonationCachingHiveMetastoreFactory` holds a `LoadingCache<String, CachingHiveMetastore>` **keyed by identity user** (`cache.getUnchecked(identity.getUser())`), configured `expireAfterWrite(userCacheTtl)` + `maximumSize(userCacheMaximumSize)` — i.e. under impersonation the cache is **sharded per identity**, not shared across users (fetched from `SharedHiveMetastoreCache.java`; corroborated by PR #9482 "add impersonation to SharedHiveMetastoreCache and remove per-user metadata caching from CachingHiveMetastore"). Over that shared cache Trino layers a **per-transaction `CachingHiveMetastore`** (the `createPerTransactionCache`/memoize wrapper) so that within one transaction repeated reads are consistent and never re-hit the metastore.
- **Directory listing** is a *separate* cache layer: `CachingDirectoryLister` (shared) with `TransactionScopeCachingDirectoryLister` giving the per-transaction consistent view — deliberately not folded into the metastore cache.
- **Iceberg** caches the resolved `Table` in the `TrinoCatalog` implementations (a Caffeine `Cache<SchemaTableName, Table>` gated by the `iceberg.metadata-cache` / expiration config), and `IcebergMetadata` additionally holds per-transaction working state on the instance.
- **Invalidation** is TTL + explicit: `CachingHiveMetastore.invalidateTable(...)` is called around writes so a write *within the transaction* invalidates the relevant entries; the per-transaction wrapper prevents a stale read of the just-written table.

### 2b. Doris equivalents at HEAD

Doris re-homed exactly this shape inside the connectors (the SPI cutover had left plain-hive caching nothing once a catalog became plugin-driven):

| Trino | Doris (HEAD) |
|---|---|
| `CachingHiveMetastore` (metastore RPCs) | `CachingHmsClient` — decorates `HmsClient`, caches `getTable`/`listPartitionNames`/`getPartitions`/`getTableColumnStatistics`, each on its own `MetaCacheEntry`, keyed to Trino/legacy shape; class doc explicitly cites "Trino `CachingHiveMetastore` shape" (`fe-connector-hms/.../CachingHmsClient.java`) |
| `CachingDirectoryLister` / `TransactionScopeCachingDirectoryLister` | `HiveFileListingCache` — memoizes `FileSystem.listStatus` per `(db,table,location)` on a `MetaCacheEntry`; doc cites "Trino keeps the equivalent `CachingDirectoryLister` as a layer separate from its metastore cache" (`fe-connector-hive/.../HiveFileListingCache.java`) |
| Iceberg `TrinoCatalog` table cache + metadata memo | Two-level: cross-query `IcebergTableCache` (`getOrLoad`, `IcebergTableCache.java:86`) **plus** per-statement `IcebergStatementScope.sharedTable` (`IcebergStatementScope.java:59`), reached from `IcebergScanPlanProvider.resolveTable` (:2353 → `loadRawTable` :2378) |
| Iceberg partition/format/comment/manifest/snapshot metadata caches | `IcebergPartitionCache`, `IcebergFormatCache`, `IcebergCommentCache`, `IcebergManifestCache`, `IcebergLatestSnapshotCache`, plus the generic derived `ConnectorPartitionViewCache` (`IcebergConnector.java:169–203`) |

**How Doris splits shared vs per-statement:** the cross-query caches live as `final` fields on the long-lived per-catalog connector (rebuilt only by `REFRESH CATALOG`), and the per-statement view is the `StatementContext` scope. For iceberg this is a genuine two-level split (`sharedTable` over `IcebergTableCache`) that matches Trino's per-transaction-view-over-shared-cache. For hive, `CachingHmsClient`/`HiveFileListingCache` are shared-only; the per-statement view is only the single `ConnectorMetadata` the funnel memoizes.

**Invalidation** is coarser than Trino by design: Doris uses TTL + `REFRESH TABLE/DATABASE/CATALOG` flush (`IcebergTableCache.invalidate/invalidateDb/invalidateAll`; `CachingHmsClient.flush/flushDb/flushAll`). The `CachingHmsClient` doc states it **does not self-invalidate around writes** ("coarse REFRESH + TTL bound staleness"). Trino *does* invalidate within the transaction around a write. **Gap:** a Doris-side write leaves a TTL-bounded stale window for a subsequent read of the same table (no read-your-write within the coarse cache). Recommendation: either add write-path invalidation to `CachingHmsClient` or force the post-write read through a live (scope-`NONE` / bypass) read, matching Trino's per-transaction invalidation.

**Authorization.** This is the sharpest divergence. Trino, under impersonation, **still caches but shards the cache key by identity** (`LoadingCache<user, CachingHiveMetastore>`). Doris, under `iceberg.rest.session=user`, **disables the caches entirely** — `latestSnapshotCache`/`tableCache`/`partitionCache`/`formatCache`/`commentCache` are set `null` under `isUserSessionEnabled()` (`IcebergConnector.java:231–273`), and fe-core adds `shouldBypassSchemaCache(SessionContext)` so schema is re-read live per user. This is enforced by `tools/check-authz-cache-sharding.sh` (every cache field must carry `authz-cache-session-user-disabled` or `authz-cache-exempt`). It is *safe* (it closes the "can-LIST-cannot-LOAD" metadata-disclosure the name-only keys would leak) but it is **coarser and slower** than Trino: `session=user` catalogs get no cross-query metadata cache at all. Recommendation (future): follow Trino and **key the caches by identity** rather than disabling them, restoring caching under `session=user`. That is a larger change (every key gains a user dimension) and the current "disable" is the correct conservative first cut.

---

## 3. The unification question — does Trino have one generic connector cache, or per-connector caches on a shared toolkit?

**Trino has no single unified connector cache.** It has a shared **low-level toolkit** — `io.trino.cache` (`EvictableCacheBuilder`, `SafeCaches`, `NonEvictableCache`/`NonKeyEvictableLoadingCache`, `CacheStatsMBean`) over Guava/Caffeine — and each connector builds *its own* caches on top: `CachingHiveMetastore`, `CachingJdbcClient`, the Iceberg catalog `tableCache`, `CachingDirectoryLister`. The reused unit is the *builder + eviction-safety + stats* machinery, not a cache and not a cache key model. This is deliberate: the cached objects and their key/invalidation/authorization semantics differ irreducibly per connector (an iceberg `Table` is not an `HmsTableInfo` is not a paimon `Table`).

**Doris is already on this Trino-correct path.** `fe-connector-cache` is exactly the shared toolkit: `MetaCacheEntry` (Caffeine encapsulated behind a Caffeine-free API, striped single-flight locks, manual-miss-load, stats — `MetaCacheEntry.java`), `CacheSpec`, `CacheFactory`, and the one genuinely generic reusable *cache* `ConnectorPartitionViewCache`/`PartitionViewCacheKey`. Adopters confirmed by import: **hive, hms, iceberg, maxcompute, paimon** all build on `MetaCacheEntry`/`CacheSpec`/`ConnectorPartitionViewCache`; the only direct-Caffeine use in a connector is `org/apache/iceberg/DeleteFileIndex.java`, which is vendored iceberg code, not a Doris cache.

**Lesson for the "unify the connector cache framework" goal:**

1. **Do not build a single connector-agnostic metadata cache.** Trino proves the right seam is the toolkit, because the cached value types and their invalidation/authz rules are per-connector. Doris already has that toolkit (`MetaCacheEntry`) and it is already broadly adopted. Unifying *harder* than Trino (one cache to rule them all) would fight the same semantics Trino refused to fight.
2. **The real unification opportunity Doris is missing is not the caches — it is the per-statement dedup.** Trino gets "load each table once per transaction" *for free* because the `ConnectorMetadata` instance is the transaction-lived memo home (§1a-1). Doris's funnel guarantees one `ConnectorMetadata` per statement, but whether repeated `resolveTable` within that statement collapses is **per-connector and not uniform** — see §5. Iceberg routes table loads through the statement scope (`IcebergStatementScope.sharedTable`); the others do not. The highest-value "unification" is a shared **"resolve table via the statement scope"** helper so every connector inherits one-load-per-statement, closing the Variant-B gap uniformly instead of connector-by-connector.
3. **Converge the last bespoke caches onto `MetaCacheEntry`** — notably paimon still lacks a cross-query raw-`Table` cache (§5), and jdbc/es keep their own structures.

---

## 4. Write-path consistency — read + write sharing one metadata / one transaction

**Trino.** Read and write share the identical `ConnectorMetadata` for the transaction (the single `CatalogTransaction.connectorMetadata`): `beginInsert`/`finishInsert`/`beginMerge`/`finishMerge` are invoked on the same instance the reads used, and the connector transaction is finished by `CatalogTransaction.commit()`/`abort()` on the same `transactionHandle`. The write therefore sees exactly the reads' snapshot; there is one handle, one metadata, one commit.

**Doris (HEAD).** The framework copies this "one-metadata-one-write-transaction-per-statement" model deliberately:

- The **8 write-path `getMetadata` seams are routed through the same `PluginDrivenMetadata` funnel**, so read and write share the one memoized `ConnectorMetadata`, guarded by the fail-loud identity pin (`PluginDrivenMetadata.java:63–69`) — a `session=user` metadata baking in one user's delegated credential can never be silently reused to execute another user's write.
- The write transaction is hoisted into **`CatalogStatementTransaction`** (`CatalogStatementTransaction.java`), co-held on the scope next to the shared metadata. `begin(writeHandle)` mints the connector transaction from the **shared metadata's own write facet** (`writeOps.beginTransaction(session, writeHandle)`, :81) and registers it in `PluginDrivenTransactionManager`, so the write inherits exactly the read arm's client/ops — the class doc explicitly names this "mirroring Trino's `CatalogTransaction`: one metadata instance and one transaction per (statement, catalog)."
- Teardown is the deterministic two-pass in `closeAll()`: pass 1 `finalizeAtStatementEnd()` rolls back a transaction the executor never committed (only a mid-flight abort leaves one active), pass 2 closes the shared metadata — transaction is always finished **before** the instance it was minted from is closed (`ConnectorStatementScopeImpl.java:70–96`, `CatalogStatementTransaction.java:100`). `finalizeAtStatementEnd` is a no-op on every normal path (the executor already finished the txn, so the manager no longer holds it), and can never undo a committed write.

**Faithfulness / gaps:**
- Faithful in the essential: read and write provably share one metadata and one connector transaction, torn down in the right order, with a fail-loud cross-identity guard Trino gets structurally.
- **Narrower scope than Trino:** the co-holder is per-*statement* (per `queryId` scope), so a *multi-statement* Doris transaction that mixes external writes and reads does not share one metadata/txn across statements the way a Trino `BEGIN…COMMIT` does. For Doris's autocommit external DML (iceberg/hive) this is equivalent; flag it if multi-statement external transactions are ever targeted.

---

## 5. Summary table — Trino mechanism | Doris equivalent today | gap / recommendation

| Trino mechanism | Doris equivalent at HEAD | Gap / recommendation |
|---|---|---|
| **Transaction is the lifecycle unit**; `TransactionManager.activeCatalogs: Map<CatalogHandle,CatalogMetadata>`, lazy `beginTransaction`, commit/abort sweep | **Statement** is the unit; `PluginDrivenMetadata.get` memoizes `ConnectorMetadata` per `(statement,catalog)` on `ConnectorStatementScopeImpl` (scope keyed incl. `queryId`); `closeAll` two-pass teardown | Sound. Narrower: no metadata sharing across a multi-statement `BEGIN…COMMIT`. Fine for autocommit external DML; **document the statement-not-transaction scope.** |
| `CatalogTransaction.connectorMetadata` (`@GuardedBy(this)`, lazy `connector.getMetadata(session, txnHandle)`) — **the instance is the per-txn memo home**, so repeated table/stat resolves collapse for free | Funnel gives **one `ConnectorMetadata` per statement**, but per-statement *table-load* dedup is only guaranteed where the connector routes through the scope | **Non-uniform.** Only iceberg uses the scope for table load (`IcebergStatementScope.sharedTable`). **Paimon** re-resolves via a transient `Table` on the handle (`PaimonTableResolver.resolve` → `handle.getPaimonTable()`; branch/withScanOptions handles carry `null` → live re-load) and has **no cross-query raw-`Table` cache** (only `PaimonLatestSnapshotCache` + `schemaAtMemo`). Recommend a **shared "resolve table via statement scope" helper** so every connector gets one-load-per-statement. |
| `getMetadata(session, ConnectorTransactionHandle)` — txn handle woven into the metadata | `getMetadata(session)` — no txn handle; write txn is a **separate** `CatalogStatementTransaction` co-holder + identity pin | Acceptable adaptation; co-holder + `PluginDrivenMetadata.java:63–69` identity pin recover the guarantee Trino gets structurally. Keep. |
| `CachingHiveMetastore` (shared, long-lived; getTable/partitions/stats) | `CachingHmsClient` (getTable/listPartitionNames/getPartitions/getTableColumnStatistics on `MetaCacheEntry`) | Faithful shape. **Gap:** no write-path self-invalidation ("coarse REFRESH+TTL"); Trino invalidates within the txn. **Add write-path invalidation or a bypass read for read-your-write.** |
| **Per-transaction** `CachingHiveMetastore` wrapper over the shared cache (read consistency within a txn) | Shared `CachingHmsClient`/`HiveFileListingCache` with **no per-statement scoped view** over them (only the single memoized metadata) | Minor: within one statement a re-list/re-get could observe concurrent change. Low priority (TTL bounds it); add a statement-scoped consistent view if strict intra-statement consistency is needed. |
| `SharedHiveMetastoreCache` shards the cache **per identity** under impersonation (`LoadingCache<user, CachingHiveMetastore>`, `expireAfterWrite`) | Under `iceberg.rest.session=user`, caches are **nulled** (`IcebergConnector.java:231–273`) + fe-core `shouldBypassSchemaCache` | Safe but coarse: `session=user` gets **no** cross-query cache. **Recommend key-by-identity sharding** (Trino model) to restore caching under `session=user`; current disable is the correct conservative first cut. |
| `CachingDirectoryLister` (+ `TransactionScopeCachingDirectoryLister`), **separate** from the metastore cache | `HiveFileListingCache` on `MetaCacheEntry`, separate layer from `CachingHmsClient` | Faithful (doc cites the Trino split). No per-transaction consistency view (see above). |
| Iceberg `TrinoCatalog` Caffeine `tableCache` + `IcebergMetadata` per-txn memo | **Two-level**: cross-query `IcebergTableCache` + per-statement `IcebergStatementScope.sharedTable` | Faithful, arguably more explicit than Trino. Good — this is the reference the other connectors should copy. |
| `io.trino.cache` toolkit (`EvictableCacheBuilder`/`SafeCaches`/`CacheStatsMBean`) reused by all connectors; **no single unified cache** | `fe-connector-cache`: `MetaCacheEntry`/`CacheSpec`/`CacheFactory`/`ConnectorPartitionViewCache`, reused by hive/hms/iceberg/maxcompute/paimon | **Already Trino-correct.** Unify at the *toolkit*, not one cache. Remaining work: converge paimon's raw-table load and jdbc/es onto `MetaCacheEntry`; unify **per-statement dedup**, not the caches. |
| Per-txn teardown: `activeCatalogs` commit/abort iterate; `CatalogTransaction.finished` (`AtomicBoolean`) | `closeAll()` two-pass (finalize `CatalogStatementTransaction`, then close metadata), idempotent, fired from query-finish callback + prepared-stmt reset | Faithful. Good. |
| Single identity per transaction (structural) → no cross-user metadata reuse possible | Shared singleton connector serves all users → **explicit** fail-loud identity pin in the funnel + `check-authz-cache-sharding.sh` | Doris must assert what Trino gets for free; the guards are the right shape. Keep enforced. |

### Bottom line for the redesign

The Doris framework is a faithful, deliberately-Trino-shaped emulation: the funnel = `CatalogTransaction`, `CatalogStatementTransaction` = the one-metadata-one-transaction write model, `CachingHmsClient`/`HiveFileListingCache`/`IcebergTableCache` = `CachingHiveMetastore`/`CachingDirectoryLister`/iceberg catalog cache, and `fe-connector-cache` = the `io.trino.cache` toolkit. The three places it diverges from Trino are all worth tracking:

1. **Per-statement table dedup is non-uniform** — Trino gets it for free from the transaction-lived metadata instance; Doris has it only for iceberg (via the scope). **This is the single highest-value generalization** and the true meaning of "unify the connector cache framework": a shared statement-scope table-resolution helper, not a shared cache. Paimon is the concrete offender (transient-on-handle only, no cross-query table cache).
2. **Write-path cache invalidation** — Doris relies on coarse REFRESH+TTL where Trino invalidates within the transaction (read-your-write gap).
3. **`session=user` authorization** — Doris *disables* caches where Trino *shards by identity*; correct-but-slow, and the natural next step is Trino-style per-identity keying.

Sources: [InMemoryTransactionManager](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/transaction/InMemoryTransactionManager.java), [CatalogMetadata](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/metadata/CatalogMetadata.java), [CatalogTransaction](https://github.com/trinodb/trino/blob/master/core/trino-main/src/main/java/io/trino/metadata/CatalogTransaction.java), [SharedHiveMetastoreCache](https://github.com/trinodb/trino/blob/master/lib/trino-metastore/src/main/java/io/trino/metastore/cache/SharedHiveMetastoreCache.java), [PR #9482 (impersonation in SharedHiveMetastoreCache)](https://github.com/trinodb/trino/pull/9482).