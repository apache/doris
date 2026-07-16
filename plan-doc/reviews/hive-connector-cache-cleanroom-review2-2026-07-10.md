# Hive connector-owned cache — SECOND independent clean-room review (2026-07-10)

> A second, fully independent clean-room run over the same 6 dormant commits
> (`f742651990d` `4fe55d88fab` `7b05df6e55e` `7c0ee1ffb2a` `7bf90a7fe3c` `12e0c9177c2`), same method as
> [hive-connector-cache-cleanroom-review-2026-07-10.md](./hive-connector-cache-cleanroom-review-2026-07-10.md)
> (review #1): 9 blind dimension reviewers → adversarial refutation per finding → cross-check vs the
> signed-off design doc. Different session, different workflow run (`wf_390c0bfc-ca5`; full per-agent
> results in that session's workflow journal). Totals: **14 raised, 12 survived, 2 refuted**.
>
> **Why this doc exists**: the two runs AGREE on everything review #1 surfaced, but this run surfaced
> **three additional confirmed defects** review #1 missed, plus (via a follow-up targeted investigation)
> a **live bug in paimon/iceberg today** with the same shape as one of them. Only the deltas are detailed
> here; agreements are summarized.

## 1. Agreement with review #1 (independently re-derived)

- **FileSystem.get fail-loud** — found by both runs (this run: CONFIRMED high ×2 reviewers, incl. proof the
  "surfaces via the estimate" mitigation is false on three counts). Fixed by `fda344e6022` (restore
  fail-loud on `FileSystem.get`, keep per-directory skip via `HiveDirectoryListingException`); this
  session's follow-up verified the fix satisfies the contract and folded in 3 hardening tests
  (fail-not-cached through the real loader, skippable-vs-loud type split, multi-partition skip scope).
- **REFRESH DATABASE gap** — found by both; review #1 recommended keep-deferred+document, the follow-up
  session chose to fix it (`Connector.invalidateDb` SPI + `RefreshManager.refreshDbInternal` wiring +
  `flushDb`/`invalidateDb` in both cache layers). NOTE: the new `invalidateDb` hook has the same sibling
  gap as §2.1 below.
- **fe-core last-modified branch (C-f) — CLEAN in both runs**: monotonicity-decrease on partition drop is
  legacy parity (legacy `HMSExternalTable.getNewestUpdateVersionOrTime` computes the same decreasable max;
  `Dictionary.hasNewerSourceVersion` throw is a pre-existing property, list as flip-e2e known-issue);
  `pin.getConnectorSnapshot()` provably never null; dormant for paimon/iceberg (only
  `HiveConnectorMetadata` sets `lastModifiedFreshness(true)`).
- **TCCL/classloader — CLEAN** (manual-miss loader runs on the calling pinned thread; `commonPool`
  refresh executor never runs a loader with `autoRefresh=false`; only JDK/plugin types cached).
- **Packaging/byte-neutrality — CLEAN** (this run's 2 refuted findings were here: the
  `fe-connector-cache → fe-connector-hudi` transitive compile edge is harmless; the unrelocated Caffeine
  copy inside `hive-catalog-shade` is CRC-identical to caffeine-2.9.3, single effective version).
- **Decorator method set — CLEAN** (exactly the 4 scan reads cached; writes/DDL/txn and must-be-fresh
  reads pass through).

## 2. DELTA: confirmed defects review #1 did not surface

### 2.1 REFRESH is never forwarded to the iceberg sibling's snapshot cache (HIGH, latent-until-flip)

- **Where**: `HiveConnector.invalidateTable`/`invalidateAll` (and the new `invalidateDb`) flush only
  `CachingHmsClient` + `HiveFileListingCache`. `close()` DOES forward to the built siblings
  (`HiveConnector.java:551+`, fields `icebergSibling:99` / `hudiSibling:107`); the invalidate hooks do not.
  fe-core routes REFRESH only to the catalog's PRIMARY connector, so nothing can ever reach
  `IcebergConnector.latestSnapshotCache` for iceberg-on-HMS tables behind the flipped gateway.
- **Failure scenario**: iceberg-on-HMS table externally updated → `latestSnapshotCache` (86400s
  **access-based** expiry) keeps serving the old snapshot; continuous querying keeps the entry alive
  forever; user runs `REFRESH TABLE`/`REFRESH CATALOG`/(new)`REFRESH DATABASE` → no effect. Staleness is
  **unbounded**, breaking the signed-off acceptance "staleness bounded by TTL + explicit REFRESH", and a
  parity regression (legacy REFRESH dropped the iceberg engine cache via `ExternalMetaCacheRouteResolver`).
  Also weakens the follower-replay "graceful coarsening" argument (replay hits the same non-forwarding hook).
- **Fix (small, symmetric with `close()`)**: forward all three invalidate hooks to the ALREADY-BUILT
  siblings (volatile field read, no force-build). Hudi sibling has no snapshot cache today — forwarding is
  a harmless no-op but keeps the contract uniform.

### 2.2 Doris-initiated DROP TABLE / CREATE TABLE / DROP DATABASE never invalidate the connector caches (HIGH, latent-until-flip)

- **Where**: `PluginDrivenExternalCatalog.dropTable` (ends at `metadata.dropTable` + editlog +
  `unregisterTable`) and `createTable` (ends at `resetMetaCacheNames`) never call
  `connector.invalidateTable`; `unregisterTable`/`unregisterDatabase` reach only fe-core engine caches
  (`ExtMetaCacheMgr`), never the connector. Only `RefreshManager.refreshTableInternal` (REFRESH TABLE;
  INSERT/TRUNCATE/ALTER route here) and `onRefreshCache` (REFRESH CATALOG) reach the connector hooks.
  The decorator deliberately does not self-invalidate around writes (javadoc: "coarse REFRESH + TTL").
- **Failure scenario (post-flip)**: `DROP TABLE t; CREATE TABLE t (new schema/location);` (common in
  ETL/tests) → next `SELECT` rebuilds the fe-core table via `getTableHandle` →
  `CachingHmsClient.getTable(db,t)` cache HIT returns the **dropped** table's `HmsTableInfo`
  (schema/location) → query planned against the wrong schema, reads the old location; CTAS write planning
  likewise. Up to 24h unless an explicit REFRESH intervenes. File-listing entries collide too when the
  recreated table reuses the same location. The §2 staleness acceptance covers EXTERNAL HMS changes, not
  Doris's own DDL — no sign-off covers this. Trino's `CachingHiveMetastore` (the signed-off model)
  self-invalidates on these mutations; legacy invalidated on every `unregisterTable`.
- **Fix options** (decision recorded in §4):
  - **(i) plugin-side, Trino-faithful (recommended for the dormant hive line)**: `CachingHmsClient`
    self-invalidates (`flush(db,table)` after createTable/dropTable/truncateTable/add-dropPartition/
    update*Statistics; `flushDb` after dropDatabase) + `HiveConnectorMetadata.dropTable/createTable/
    dropDatabase` drop the matching `HiveFileListingCache` entries. Fully dormant, zero fe-core change.
  - **(ii) fe-core-side, generic**: `PluginDrivenExternalCatalog.dropTable/createTable/dropDb` call
    `connector.invalidateTable/invalidateDb`. Also fixes the LIVE paimon/iceberg hole (§3) in one shot,
    but touches live paths → needs paimon/iceberg behavior-neutrality argument + its own review.
  - Both is fine too ((i) now in the dormant line, (ii) as the separate live-bug fix).

### 2.3 Partition-cache capacity semantics changed: 100000 now counts request-LISTS, not partitions (MEDIUM)

- **Where**: `CachingHmsClient` `DEFAULT_PARTITION_CAPACITY = 100000` claims to mirror legacy
  `Config.max_hive_partition_cache_num`, but legacy keyed per-partition (100000 partition OBJECTS;
  `HiveExternalMetaCache.java:121`), while `partitionsCache` keys the full requested-name-list and stores
  the whole `List<HmsPartitionInfo>` as ONE entry — `CacheFactory` has `maximumSize` only, no weigher.
  Overlapping requests duplicate partition objects across entries (full-list scans + each distinct pruned
  subset via `applyFilter` + MTMV per-partition singletons via `getPartitionFreshnessMillis`).
- **Failure scenario**: 10k–100k-partition tables + dashboard-style sliding predicates → each distinct
  predicate window caches another multi-thousand-object list, each size-1 to Caffeine, 24h TTL → FE heap
  grows far beyond legacy's bound; OOM reachable under a workload legacy handled.
- **Fix options**: (a) weigher summing list sizes (Trino uses a weigher here) — requires adding optional
  weigher support to the shared `fe-connector-cache` framework (bundled into paimon/iceberg zips → breaks
  this series' "paimon/iceberg byte-identical" claim; needs behavior-neutrality tests + explicit OK);
  (b) much smaller list-entry default with an honest comment (no framework change); (c) document-only.
  **User decision needed** (§4).

### 2.4 Test-adequacy deltas (LOW, production code correct at HEAD)

- Empirically proven mutation-survivable at review time (full module suite stayed green under the
  mutation): the `listFromFileSystem` IOException fold (catch→emptyList would CACHE a poisoned empty
  listing) — **closed** by the tests folded into `fda344e6022`.
- Still open: (a) nothing pins that `createClient` actually wraps with `CachingHmsClient`
  (removing `wrapWithCache` from the production call-site survives the suite; closable via the
  `newMetadata` seam); (b) the PUBLIC `invalidateTable/invalidateAll` hooks are never driven with a BUILT
  metastore client (dropping the metastore flush from them survives the suite; the seam tests cover the
  internals only). Cheap insurance for the two one-line surfaces REFRESH depends on.

## 3. LIVE BUG (today, not dormant): paimon/iceberg drop+recreate serves a stale snapshot pin

Targeted follow-up investigation (this session), same shape as §2.2 but for the LIVE plugin connectors:

- `IcebergLatestSnapshotCache` — key = `TableIdentifier.of(db, table)` (plain names), value =
  `(snapshotId, schemaId)`, 86400s access-based TTL, maxSize 1000. **HOLE**: Doris-initiated
  `DROP TABLE`+`CREATE TABLE` same name never invalidates (`IcebergConnectorMetadata.dropTable/createTable`
  only call `catalogOps`; the fe-core DDL path never reaches `connector.invalidate*`) → next query's
  `beginQuerySnapshot` pins the DROPPED table's snapshot/schema against the new table.
- `PaimonLatestSnapshotCache` — same shape, same **HOLE** (key `Identifier.create(db, table)`, value
  snapshotId, 86400s access-based).
- `PaimonSchemaAtMemo` — **narrow HOLE** (time-travel only): keyed `(db, table, sysTable, branch,
  schemaId)`, correctness rests on "schemaId content is write-once", violated by drop+recreate (fresh
  table reuses schemaId 0). NOT cleared even by `invalidateTable`/`invalidateAll` — only by connector
  rebuild (REFRESH CATALOG).
- NOT affected: `IcebergManifestCache` (path-keyed, unique paths), `IcebergRewritableDeleteStash`
  (queryId-keyed), fe-core schema cache (cleared on drop via `unregisterTable`).
- Mitigations today: bounded by the 24h access TTL (but continuous querying keeps it alive), REFRESH
  TABLE/CATALOG clears the snapshot caches (NOT the schema-at memo), `ttl-second<=0` catalogs immune.
- **Recommended handling**: separate fix line (NOT folded into the dormant hive series): fe-core
  `PluginDrivenExternalCatalog.dropTable/createTable/dropDb` → `connector.invalidateTable/invalidateDb`
  (option (ii) above), plus make paimon `invalidateTable/invalidateAll` also clear `PaimonSchemaAtMemo`.
  Touches live behavior → own commits + own adversarial review + regression coverage.

## 4. Open decisions (need user sign-off)

1. §2.2 fix locus: plugin-side (i), fe-core-side (ii), or both. (Recommended: (i) for the dormant line,
   (ii) as the live-bug fix.)
2. §2.3 capacity fix: weigher in shared framework (Trino-faithful, touches paimon/iceberg-bundled
   framework bytes) vs smaller default vs document-only.
3. §3 live bug: fix now as its own line vs defer with documented risk.

## 5. e2e debt additions (heterogeneous-HMS docker, on top of review #1's list)

- Drop+recreate freshness: `DROP TABLE`+`CREATE TABLE` same name → immediate query sees the new
  schema/location with no REFRESH (hive post-flip; iceberg/paimon live once §3 is fixed).
- REFRESH reaches the iceberg sibling: externally mutate an iceberg-on-HMS table → `REFRESH TABLE` /
  `REFRESH DATABASE` / `REFRESH CATALOG` each unpin the snapshot (and via follower replay).
- Broken storage config fails loud (post-`fda344e6022` contract): bogus scheme/credentials → query errors,
  never 0-rows-as-success.
- An end-to-end read that provably transits `CachingHmsClient` via the real `createClient` (second query
  hits cache, HMS call count flat) — closes §2.4(a) at the e2e level.
