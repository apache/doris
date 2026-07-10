# Connector-cache invalidation follow-ups — step design (2026-07-10)

> Follow-up to the connector-owned-cache clean-room re-reviews
> (`plan-doc/reviews/hive-connector-cache-cleanroom-review-2026-07-10.md` +
> `...-review2-2026-07-10.md`). Three defects the second review surfaced needed user sign-off; this doc
> is the implementer's starting point for the two the user chose to fix now. HEAD `1d9a5e61473`, branch
> `catalog-spi-11-hive`. Every line number below was re-verified against HEAD by a 5-agent recon
> (`wf_daed84c1-4ea`) — trust HEAD, re-confirm before editing.

---

## 0. What this step does (and what it deliberately does NOT)

The second clean-room review raised four cache-invalidation findings. Their HEAD status + disposition:

| review finding | HEAD status | disposition |
|---|---|---|
| §2.1 REFRESH not forwarded to the built iceberg/hudi siblings | **already FIXED** (`forEachBuiltSibling` wired into `HiveConnector.invalidateTable/invalidateDb/invalidateAll/invalidatePartition`, `HiveConnector.java:296/322/342/365/409`) | no action |
| §2.2 Doris-issued DROP/CREATE never invalidate the connector caches (hive, dormant-until-flip) | CONFIRMED present | **FIX 1** (this step) |
| §3 same-shape LIVE bug in paimon/iceberg (drop+recreate serves a stale snapshot/schema pin) | CONFIRMED present, **LIVE today** | **FIX 1 + FIX 1b** (this step) |
| §2.3 partition-object cache bounds by request-LIST count, not partition count (hive, dormant) | CONFIRMED present | **FIX 2** (this step) |

**User sign-off (2026-07-10):**
- **D1 → fix the self-DDL/drop+recreate invalidation once at the fe-core generic layer** (covers dormant
  hive AND live paimon/iceberg in one connector-agnostic change). *Rejected*: per-plugin self-invalidation
  (more surface, and would defer the live bug or fan it across three connectors).
- **D2 → restructure the partition-object cache to per-partition keying** (aligns with BOTH Trino and legacy
  Doris; no shared-framework change). *Rejected*: a weigher (touches the paimon/iceberg-bundled
  `fe-connector-cache`), a smaller default, or comment-only.

Non-goals (recorded so they are not silently dropped): the review's §2.4 test-adequacy inserts
(`createClient` wrap seam, public-hook-with-built-client) — optional cheap insurance, not required by D1/D2;
event-driven incremental invalidation (Model B); deleting legacy fe-core caches (final deletion phase).

---

## 1. FIX 1 — fe-core generic DDL invalidation hook (D1)

### 1.1 The bug (HEAD-verified)
`PluginDrivenExternalCatalog`'s schema-level DDL overrides never reach `connector.invalidate*`:
- `dropTable` (`:536-596`) ends at `metadata.dropTable` + `logDropTable` + `unregisterTable` (`:593-594`);
  the view branch (`:565-575`) ends at `dropView` + `unregisterTable` (`:572`).
- `createTable` (`:380-443`) ends at `metadata.createTable` + `logCreateTable` + `resetMetaCacheNames`
  (`:439`).
- `dropDb` (`:500-525`) ends at `metadata.dropDatabase` + `logDropDb` + `unregisterDatabase` (`:523`).

`unregisterTable`/`unregisterDatabase`/`resetMetaCacheNames` touch only the fe-core ENGINE caches
(`ExtMetaCacheMgr`), never the connector-owned caches. The ONLY paths that reach `connector.invalidate*`
are the three REFRESH verbs (`RefreshManager.refreshTableInternal:245-257` REFRESH TABLE — also reached by
TRUNCATE and ALTER via `afterExternalDdl`; `refreshDbInternal:119-129` REFRESH DATABASE;
`onRefreshCache:252-261` REFRESH CATALOG). So a Doris-issued `DROP TABLE t; CREATE TABLE t (...)` (or the
DB equivalent) keeps serving the dropped object's cached metadata/snapshot until the 24h TTL.

Two victims, different severity:
- **hive** — dormant (`"hms"` not in `SPI_READY_TYPES`); latent until the flip.
- **paimon/iceberg** — **LIVE**: `IcebergLatestSnapshotCache` / `PaimonLatestSnapshotCache` (key = plain
  `(db,table)`, value = pinned `(snapshotId[,schemaId])`, 86400s **access**-based TTL) pin the DROPPED
  table's snapshot onto the recreated same-name table until the TTL (kept alive by continuous querying).

### 1.2 Trino parity (per the user's request to consult Trino — recon-confirmed)
Trino's `CachingHiveMetastore` self-invalidates synchronously after every `createTable`/`dropTable`/
`dropDatabase`/`replaceTable`/`renameTable`/`update*Statistics` (a `finally`-block
`invalidateTable`/`invalidateDatabase`). D1 puts the same eager invalidation at the natural Doris seam —
the generic `PluginDrivenExternalCatalog` DDL entry — so it lands once for every SPI connector instead of
per-plugin.

### 1.3 The change
Add, immediately after each successful mutation (using the already-non-null `connector` field these methods
already dereference, and the REMOTE names — the exact form `RefreshManager` passes and the connectors key
on):
- `dropTable` table branch (after `metadata.dropTable`, `:590`) →
  `connector.invalidateTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName())`.
- `dropTable` view branch (after `dropView`, `:570`) → same call (harmless no-op if the connector has no
  entry for a view; keeps the two branches uniform).
- `createTable` (after `metadata.createTable`, `:427`) →
  `connector.invalidateTable(db.getRemoteName(), createTableInfo.getTableName())`. The table name is NOT
  remote-resolved (parity with the existing code: a new table has no local→remote mapping).
- `dropDb` (after `metadata.dropDatabase`, `:519`) → `connector.invalidateDb(db.getRemoteName())`.

**`dropTable` is the load-bearing call** for the drop+recreate bug (it clears the stale pin so the later
`CREATE`'s exists-probe and the next `SELECT` both go live). `createTable`-invalidation is uniform
belt-and-suspenders (one line, harmless — invalidating a just-created table only forces the next read
live). **`createDb` is intentionally NOT hooked**: a brand-new database has no table-keyed connector
entries to clear that `dropDb` did not already clear, and Doris routes the db-name-list refresh through
`resetMetaCacheNames` already. Documented, not asked.

### 1.4 Behavior / cost analysis (this is an INTENTIONAL live change — NOT byte-neutral)
D1 deliberately changes live paimon/iceberg behavior (that is the §3 fix). It is *strictly beneficial and
bounded*:
- **Correctness**: invalidating a dropped/created table's connector entry is always correct — the entry is
  either stale (drop) or absent (create). No path reads a "must-stay-pinned" value across a DROP/CREATE.
- **Cost**: one `Cache.invalidate`/`asMap().keySet().removeIf` per DDL — O(1)/O(cache-size), off the query
  hot path (DDLs are rare). No new RPC, no force-build (`connector` is already initialized here).
- **No-cache connectors** (jdbc/es and any without caches): `Connector.invalidateTable/invalidateDb` are
  no-op SPI defaults → inert.
- **TCCL**: mirrors `RefreshManager`'s existing `getConnector().invalidateTable(...)` call, which pins
  nothing — `invalidate*` runs the plugin's own method (no reflection-by-name), so no TCCL pin is needed
  (memory `catalog-spi-plugin-tccl-classloader-gotcha` applies to name-reflection, not direct dispatch).
- **hive**: dormant (no `HiveConnector` built pre-flip) — inert until the flip, then armed.

### 1.5 Tests
- fe-core (`PluginDrivenExternalCatalog` DDL): a fake `Connector` recording `invalidateTable/invalidateDb`
  calls; assert `dropTable` (table + view branches) / `createTable` / `dropDb` each invoke it with the
  REMOTE db/table names, and `createDb` does NOT. Assert a no-cache connector path is a no-op (default SPI).
- iceberg / paimon (live): after `invalidateTable(db,t)` the next `beginQuerySnapshot` re-pins (drop the
  cached `(db,t)` entry). See FIX 1b for the paimon schema-memo half.

---

## 2. FIX 1b — paimon `PaimonSchemaAtMemo` invalidation (completes §3 for paimon)

### 2.1 The gap
`PaimonConnector.invalidateTable`/`invalidateAll` (`PaimonConnector.java:233/240`) clear only
`latestSnapshotCache`. The second connector-owned cache, `PaimonSchemaAtMemo` (`PaimonSchemaAtMemo.java`, a
plain `ConcurrentHashMap<MemoKey, PaimonSchemaSnapshot>`, key = `(db,table,sysTable,branch,schemaId)`),
has **no invalidate method at all** — cleared only by its `maxSize` valve or a connector rebuild (REFRESH
CATALOG). A drop+recreate that reuses a `schemaId` (e.g. schema 0) with different content yields a stale
time-travel hit even after FIX 1 routes `invalidateTable` to the connector.

### 2.2 The change
- Add `PaimonSchemaAtMemo.invalidate(String db, String table)` → `cache.keySet().removeIf(k ->
  k matches (db,table))` (add a package-private `MemoKey.matches(db,table)` mirroring its equals fields), and
  `invalidateAll()` → `cache.clear()`. Pure `ConcurrentHashMap` ops; no framework change.
- Wire `PaimonConnector.invalidateTable` → also `schemaAtMemo.invalidate(dbName, tableName)`;
  `invalidateAll` → also `schemaAtMemo.invalidateAll()`.

### 2.3 Cost / correctness
The memo value is immutable and only *skips* a re-read on a hit; dropping entries only forces a re-read
(the pre-memo behavior) — never a stale/wrong value. The keyspace is tiny; `removeIf` scan is negligible.
Iceberg needs no analog: `IcebergManifestCache` is path-keyed (unique paths, drop+recreate cannot collide)
and its `invalidateAll` (REFRESH CATALOG) already drops it; the `latestSnapshotCache` half is covered by
FIX 1 through `IcebergConnector.invalidateTable` (already clears it, `IcebergConnector.java:412`).

### 2.4 Tests
paimon unit: populate the memo via `getOrLoad`, call `invalidateTable(db,t)`, assert the `(db,t)` entries
are gone and other-table entries survive; `invalidateAll` clears all.

---

## 3. FIX 2 — per-partition keying of the hive partition-object cache (D2)

### 3.1 The bug (HEAD-verified)
`CachingHmsClient.partitionsCache` (`CachingHmsClient.java:111`) is
`MetaCacheEntry<PartitionsKey, List<HmsPartitionInfo>>`: `PartitionsKey` (`:381-422`) = `(db, table,
requested-name-LIST)` and the value is the WHOLE list as ONE entry. `DEFAULT_PARTITION_CAPACITY=100000`
(`:105`) with a comment claiming "partition objects" parity — but the shared `CacheFactory` has only
`maximumSize`, no weigher (`CacheFactory.java:109`), so 100000 now bounds **distinct request-lists**, each
holding arbitrarily many partition objects (and duplicating them across overlapping lists). Legacy
`HiveExternalMetaCache` keyed ONE partition object per entry (`:121`, cap
`max_hive_partition_cache_num`); **Trino likewise keys per-partition** (`Cache<HivePartitionName,…>`, flat
`maximumSize`). The list-keyed shape is unique to this cache and under-bounds FE heap.

### 3.2 The design — value-keyed per-partition entries (matches Trino + legacy; no framework change)
The scan caller (`HiveScanPlanProvider.convertPartitions:332-349`) and every other `getPartitions` caller
(`HiveConnectorMetadata:933/1018/1176/1205`, `HiveWritePlanProvider:257`, `HiveConnectorTransaction:503`)
consume the returned `HmsPartitionInfo` **as a set** — they iterate and read `getValues()`/`getParameters()`
/`getLocation()` and never rely on result order or on a 1:1 name↔result correspondence (the delegate
`get_partitions_by_names` never guaranteed order/cardinality anyway). So:

- Change `partitionsCache` to `MetaCacheEntry<PartitionKey, HmsPartitionInfo>`, `PartitionKey = (db, table,
  List<String> values)` — keyed by the partition's OWN values.
- `getPartitions(db, table, partNames)`:
  1. for each requested name, parse it to values (`toPartitionValues`) and `getIfPresent((db,table,values))`;
     hits go straight into the result, unparseable/missing names go into a `missNames` list;
  2. if `missNames` non-empty, `delegate.getPartitions(db,table,missNames)` → for each returned info,
     `put((db,table, info.getValues()), info)` (keyed by the ACTUAL values — always correct) and add to the
     result.
- **Correctness is independent of parse fidelity**: a store always uses the partition's real values; a
  lookup misparse simply misses → the name falls to `missNames` → the delegate reloads it → correct result,
  only no cache benefit for that (rare, escaped-value) name. Distinct partitions have distinct values, so no
  false hit; a requested name with no partition is omitted by the delegate (parity, no negative caching).
- This is the SAME values-keyed correlation `HiveConnectorTransaction:518` already uses
  (`partitionsByValues.get(HiveWriteUtils.toPartitionValues(name))`).

### 3.3 The name→values parser
`HiveWriteUtils.toPartitionValues` (fe-connector-hive) is the byte-faithful port but lives in a module
`fe-connector-hms` cannot import (hms is below hive). Source options at implementation: a small private
helper in `CachingHmsClient` mirroring `HiveWriteUtils.toPartitionValues` (split on `/`, `=`, unescape via
inlined `unescapePathName`), OR `org.apache.hadoop.hive.common.FileUtils` (already a hms-module dependency —
`HmsEventParser` uses `FileUtils.makePartName`). Prefer the FileUtils route to avoid duplicating the
unescape table, unless it complicates the decorator; either is correct (misparse = perf-only).

### 3.4 What stays the same
- `flush(db,table)` / `flushDb(db)` / `flushAll` (`:164-185`) keep using `invalidateIf(key ->
  key.matches(db,table))` / `matchesDb(db)` / `invalidateAll` — `PartitionKey` still carries `(db,table)`,
  so the invalidation interface is unchanged (and stays correct under FIX 1's new callers).
- `DEFAULT_PARTITION_CAPACITY=100000` now correctly bounds **100000 partition objects** (legacy semantics);
  fix the comment to say so.
- The other three caches (`table`/`partition_names`/`column_stats`) are untouched.
- Dormant: no `HiveConnector`/`CachingHmsClient` is built for a live catalog pre-flip.

### 3.5 Tests
`CachingHmsClientTest` additions: (a) two overlapping name-lists share per-partition entries (second call
hits, delegate called only for the new names); (b) a requested name with no partition is omitted and not
negative-cached; (c) `flush(db,table)` drops that table's partition entries only; (d) capacity bounds
partition COUNT (put N>cap distinct partitions → size ≤ cap); (e) an escaped-value name still returns the
correct partition (via reload) — no wrong/dropped result.

---

## 4. Decomposition — independent commits
| step | what | module | live? |
|---|---|---|---|
| **F1** | fe-core `PluginDrivenExternalCatalog` DDL → `connector.invalidateTable/invalidateDb` + tests | fe-core | **live** (paimon/iceberg §3) + arms hive |
| **F1b** | `PaimonSchemaAtMemo.invalidate/invalidateAll` + wire into `PaimonConnector` + tests | fe-connector-paimon | **live** |
| **F2** | `CachingHmsClient` partition cache → per-partition value keying + tests + comment | fe-connector-hms | dormant |

Order: F1 → F1b (both live, review together) → F2 (dormant). Each is its own commit with tests, checkstyle
0, import gate net. **After all land: unified clean-room adversarial re-review** (F1/F1b touch live
paimon/iceberg — per practice + memory `plugindriven-mvcc-table-is-live-not-dormant` / `clean-room-adversarial-review-pref`).

## 5. e2e debt (heterogeneous-HMS docker; on top of the review's list)
- Drop+recreate freshness: `DROP TABLE t; CREATE TABLE t (new schema/loc); SELECT` sees the NEW table with
  no REFRESH — for paimon + iceberg NOW (live), and for hive post-flip.
- `DROP DATABASE d; CREATE DATABASE d; ...` likewise sees no stale table entries.
- paimon time-travel after drop+recreate reusing a schemaId returns the new schema (schema-memo cleared).

## 6. Status (landed 2026-07-10)
- [x] **F1** fe-core generic DDL invalidation hook — `3b66982fedf` (`PluginDrivenExternalCatalogDdlRoutingTest`
      56 pass, checkstyle 0). Live for paimon/iceberg; arms hive post-flip.
- [x] **F1b** paimon `PaimonSchemaAtMemo` invalidate + wiring — `7b8fed012be` (`PaimonSchemaAtMemoTest` 5 pass).
- [x] **F2** `CachingHmsClient` per-partition value keying — `982db925659` (`CachingHmsClientTest` 15 pass).
      Dormant (hive not flipped).
- [x] **Unified clean-room adversarial re-review** of F1/F1b (live paths) — `wf_fe6ddef4-777`, DONE. **Verdict:
      fixes are NET IMPROVEMENTS but INCOMPLETE — 4 follow-ups required (3 live + 1 dormant). See
      `plan-doc/reviews/cache-invalidation-cleanroom-review-2026-07-10.md`.**
- [ ] **R1** DROP/CREATE/DROPDB invalidation is coordinator-only — followers/observers don't propagate on
      replay (mirror `replayTruncateTable`). LIVE.
- [ ] **R2** iceberg/paimon lack an `invalidateDb` override → DROP DATABASE hook + pre-existing REFRESH
      DATABASE are no-ops for their snapshot/schema caches. LIVE (fixes a pre-existing bug too).
- [ ] **R3** `getPartitions` raw `put` bypasses the invalidateGeneration guard (REFRESH-vs-in-flight race).
      DORMANT; needs an additive guarded-put on the connector `MetaCacheEntry`.
- [ ] **R4** RENAME (and maybe replace/CTAS-overwrite) never invalidates the connector cache. LIVE.
- [ ] **e2e** (§5) — owed at the flip / heterogeneous-HMS docker (paimon/iceberg drop+recreate is testable
      NOW as a live regression, once R1/R2/R4 land).
