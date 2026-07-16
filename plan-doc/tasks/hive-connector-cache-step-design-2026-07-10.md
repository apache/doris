# Hive connector-owned scan-side cache — step design (2026-07-10)

> Phase-1 first item of the HMS cutover ("D2" in the execution plan). Produced by a HEAD-grounded
> recon (`wf_d19057ca-5bb`: 8 dimension readers + 3 adversarial critics — completeness / flip-safety /
> scope) with lead-engineer verification of every load-bearing fact. **Starting doc for the implementer.**
> Authoritative parent: `hms-cutover-execution-plan-2026-07-10.md` §2.2 + §2.6. HEAD `d43ba31f3b3`,
> branch `catalog-spi-11-hive`.

---

## 0. TL;DR

At the atomic flip a `type=hms` catalog stops being an `HMSExternalCatalog`, so
`ExternalMetaCacheRouteResolver.addBuiltinRoutes` (`:66-70`) no longer routes it to the fe-core
`HiveExternalMetaCache` / `HudiExternalMetaCache` / `IcebergExternalMetaCache`; routing falls through to
`ENGINE_DEFAULT` (`:72-73`). **This is not a crash** — `registry.resolve("default")` returns a real
`DefaultExternalMetaCache` (verified `ExternalMetaCacheRegistry:39-49`, `ExternalMetaCacheMgr:290-296`),
and schema caching *survives* because a flipped `PluginDrivenExternalTable` inherits
`getMetaCacheEngine()=="default"` (`ExternalTable:229-231`, no override) — the same `ENGINE_DEFAULT`
cache the schema feed and `invalidateTable` both target. **What silently dies is the engine-specific
metadata**: partition names, partition objects, and directory file listings — and the hive connector
**caches nothing today** (verified: no Caffeine/`MetaCacheEntry` anywhere in `fe-connector-hive`;
`HiveScanPlanProvider` header literally says "No file listing cache").

So **D2 exists to preserve legacy performance, not correctness**, plus to fix one coupled correctness
bug (§2.6). The whole step is **purely additive and dormant**: nothing is deleted, nothing changes for
paimon/iceberg/jdbc, and none of it goes live until `"hms"` enters `SPI_READY_TYPES` at the flip.

**Deliverables:** (1) a connector-owned metastore + file-listing cache in `fe-connector-hive`,
(2) `HiveConnector.invalidateTable/invalidateAll` overrides (fe-core wiring already exists), (3) a cheap
real max-partition modify time so flipped `getNewestUpdateVersionOrTime` stops returning constant 0.

---

## 1. Research note (what the recon established, HEAD-verified)

### 1.1 The fe-core cache machinery — survives; only the hms *route* + the 3 engine caches are legacy
- `ExternalMetaCacheMgr` registers **five** engine caches: `default`, `hive`, `hudi`, `iceberg`,
  `doris` (`:290-296`). It has two dispatch families:
  - **instanceof-routed** (`prepareCatalog`/`invalidateCatalog`/`invalidateDb`/`invalidateTable`/
    `invalidatePartitions`/`removeCatalog`) → `ExternalMetaCacheRouteResolver.resolveCatalogCaches`.
    This is the *only* place the `HMSExternalCatalog` gate lives; it collapses to `ENGINE_DEFAULT` at
    the flip.
  - **engine-name-routed** (`*ByEngine`, `hive()/hudi()/iceberg()/doris()`, `getSchemaCacheValue`) —
    unaffected by the collapse.
- `ENGINE_DEFAULT` = `DefaultExternalMetaCache`, which registers **only a `schema` entry**. So the
  collapse target is a working schema-only cache — no throw, no double-cache. **This is the exact state
  native paimon/iceberg catalogs already run in today** (`PluginDrivenExternalCatalog` is not
  `HMSExternalCatalog` → `ENGINE_DEFAULT`), which is why route-collapse is harmless for them and is
  precisely the state hive must reach.
- **Do NOT retire** `AbstractExternalMetaCache` / `ExternalMetaCacheRegistry` /
  `ExternalMetaCacheRouteResolver` / `MetaCacheEntry` / `ExternalMetaCacheMgr` / `DefaultExternalMetaCache`
  — they serve `default`+`doris`+the `ExternalRowCountCache`+the catalog/db-listing cache for every
  external catalog including PluginDriven. Only the **hms route branch** (`resolver:66-70` + its import)
  and the three **engine-cache classes** are legacy.

### 1.2 What `HiveExternalMetaCache` actually caches (the relocation surface)
Four entries (`HiveExternalMetaCache:119-161`):
| entry | key → value | loader hits | classification |
|---|---|---|---|
| `schema` | `SchemaCacheKey → SchemaCacheValue` | `externalCatalog.getSchema` | **redundant** post-flip — served by `ENGINE_DEFAULT` via `getMetaCacheEngine()=="default"`. No connector cache needed. |
| `partition_values` | `PartitionValueCacheKey → HivePartitionValues` | thrift `listPartitionNames` (`:283`) | **scan-side** (partition pruning). Connector must own. |
| `partition` | `PartitionCacheKey → HivePartition` | thrift `getPartition/getPartitions` (`:323/:367`) → inputFormat/location/**parameters** | **scan-side** + the **§2.6 max-modify-time source** (`transient_lastDdlTime` rides in `partition.getParameters()`, free in the thrift response). |
| `file` | `FileCacheKey → FileCacheValue` | **filesystem** `DirectoryLister.listFiles` (`:396`) | **scan hot-path**, the dominant per-scan cost. Connector must own. |

The ACID `getFilesByTransaction` path (`:792-828`) is scan-side but **uncached** even in legacy
(`AcidUtil.getAcidState`); the connector already reimplements it (`HiveAcidUtil` wired at
`HiveScanPlanProvider:191`) — no cache-invalidation coupling to carry over.

### 1.3 The hive connector today caches NOTHING
- No Caffeine/`LoadingCache`/`MetaCacheEntry` in `fe-connector-hive`, `-hms`, or `-metastore-hms`.
  `ThriftHmsClient` "Cached" = connection pool only; every `getTable`/`listPartitionNames`/
  `getPartitions`/`getTableColumnStatistics` is a fresh RPC.
- `HiveScanPlanProvider` re-lists partitions **and** files on every scan (`:321/:326/:358/:361`);
  `HiveConnectorMetadata.getTableHandle`+`getColumnHandles` each do a full `getTable` (`:302/:305/:422`).
- `fe-connector-hive/pom.xml` depends on neither `fe-connector-cache` nor Caffeine (unlike iceberg/paimon).

### 1.4 The connector-owned cache template (paimon / iceberg-native)
- A connector holds its scan cache as **plain final fields on the per-catalog `Connector`**, built in
  the ctor from **catalog properties** (not fe-core `Config`): `IcebergConnector.latestSnapshotCache`
  +`manifestCache`, `PaimonConnector.latestSnapshotCache`.
- Each wrapper owns one `MetaCacheEntry<K,V>` from the shared `fe-connector-cache` framework
  (`CacheFactory`/`CacheSpec`/`MetaCacheEntry`; **Caffeine encapsulated** — never crosses to child code
  as a type; Caffeine pinned **2.9.3**, self-bundled per plugin — hive is like paimon, no transitive
  Caffeine).
- Config keys: `meta.cache.<engine>.<entry>.(enable|ttl-second|capacity)` via
  `CacheSpec.fromProperties`; defaults hardcode legacy `Config` values (24h / capacity). fe-core does
  **not** parse these (honors the no-property-parsing-in-fecore rule).
- **Invalidation is already wired connector-agnostically**: `REFRESH TABLE` →
  `RefreshManager.refreshTableInternal` → `connector.invalidateTable(remoteDb, remoteTable)`
  (`:247-249`); `REFRESH CATALOG` → `PluginDrivenExternalCatalog.onRefreshCache` →
  `connector.invalidateAll()` (`:252-257`). Both are **no-op SPI defaults** (`Connector:306-316`) — hive
  just overrides them. `ADD/MODIFY CATALOG` rebuilds the connector (caches dropped wholesale). TTL/LRU
  evict.

### 1.5 Trino reference (per the user's request to consult Trino)
Trino keeps **all** Hive caching inside the connector, in two separate layers:
- `CachingHiveMetastore` — a decorator wrapping the raw metastore client, caching
  `getTable`/`getPartition`/`listPartitionNames`/`get*Statistics` under
  `hive.metastore-cache-ttl` / `metastore-cache-maximum-size` (+ a strongly-consistent per-transaction
  layer). Invalidated by a connector `flush_metadata_cache` procedure (per-table/per-partition).
- `CachingDirectoryLister` — a **separate** file-status cache (`hive.file-status-cache-*`).

Doris historically did the opposite (central engine-side `HiveExternalMetaCache`). **D2 re-aligns Doris
to Trino's connector-owned, two-layer model.** The Doris-specific frictions Trino has no analog for:
(a) caches must be **transient** and never enter the GSON edit log (naturally satisfied — caches live on
the `Connector`, rebuilt on init/`REFRESH CATALOG`); (b) the atomic `SPI_READY` flip (the exact failure
D2 prevents); (c) file-listing crosses into the plugin's bundled Hadoop classloader (TCCL, §4.5).

### 1.6 §2.6 — the one coupled correctness bug
`PluginDrivenMvccExternalTable.getNewestUpdateVersionOrTime` (`:699-715`) materializes the latest pin
and reads `nameToLastModifiedMillis`; hive's `listPartitions` is **names-only** (all `-1`) → filtered →
`orElse(0L)` → **constant 0**. It never checks `isLastModifiedFreshness` (unlike `getTableSnapshot`
`:616-635`). Consequence: a hive-backed **SQL dictionary** / **MV auto-refresh** never sees a newer
source version (`Dictionary.hasNewerSourceVersion:271-296` needs a monotone-increasing value) — silent,
no crash. The connector *already* has the right value cheaply: `getTableFreshness` (`:1032-1062`) maxes
`transient_lastDdlTime` over partitions (the same value legacy read from `HivePartition.parameters`) —
but it hits `hmsClient.getPartitions` **live** each call, so §2.6's fix must be **backed by the D2
partition cache** or the periodic dictionary poll regresses to repeated uncached round-trips.

---

## 2. Goals / Non-goals

### Goals
1. The hive connector owns a scan-side cache (metastore metadata + directory listing) so that at the
   flip, route-collapse to `ENGINE_DEFAULT` is **performance-neutral** vs legacy.
2. `HiveConnector.invalidateTable/invalidateAll` drop that cache (arms `REFRESH TABLE`/`REFRESH CATALOG`).
3. Fix §2.6: flipped `getNewestUpdateVersionOrTime` surfaces a real, cheap max-partition modify time,
   cache-backed, restoring SQL-dictionary / MV auto-refresh over hive base tables.
4. Everything lands **dormant** (inert while hms is legacy; byte-neutral for all other connectors).

### Non-goals (explicitly out of D2 — recorded so they are not silently dropped)
- **Deleting the fe-core caches + the 4 gates + the resolver hms branch.** Per the standing user
  decision ("delete legacy last; reach a working flip first"), and because these have **zero live
  readers post-flip** (all readers are legacy deletion-unit classes — `HiveScanNode`, `HMSExternalTable`,
  `HiveDlaTable`, etc.), deletion rides the **final deletion phase**, not D2. D2 is purely additive; the
  fe-core caches simply sit **unrouted** (harmless) after the flip until then.
- **Event-driven incremental invalidation (event Model B — the NEXT task).** Post-flip
  `MetastoreEventsProcessor` is `instanceof HMSExternalCatalog`-gated (`:116`) → zero event sync for a
  flipped catalog. D2 accepts interim staleness **bounded by TTL + explicit REFRESH**. Model B re-arms
  the loop and adds `Connector.invalidatePartitions(db,table,names)`; it **depends on D2's cache**, not
  vice-versa (verified: D2 lands first). D2 must **not** attempt partition-granular invalidation.
- **hudi-on-HMS caching regression.** The hudi sibling rebuilds `HoodieTableMetaClient` per query and has
  no cache; post-flip it loses caching entirely (correctness fine, perf regression). This is a **separate
  hudi-connector item** — but see §3 Option A, which would fix it for free.
- **Column statistics caching.** `getColumnStatistics` is uncached today but gated on `numRows>0` and off
  the scan hot-path (planner fast-path only) → defer as a perf item.
- **The two TVF flip-breaks** (`partition_values()` `MetadataGenerator:2091`; `hudi_meta()` `:459`) and
  the **`canSample`/`SUPPORTS_SAMPLE_ANALYZE`** stats-gate inconsistency (`AnalysisManager:1484`) — these
  are live-SQL flip-survivors, but they belong to the **flip's coupling-seam set** (execution-plan §2.3),
  not D2-cache authoring. Recon re-confirmed all three; they are tracked there.

### Refuted worries (checked against HEAD — do NOT spend budget here)
- **`RefreshManager.refreshPartitions` CCE "blocker"** — REFUTED. **Verified: its sole caller is
  `AlterPartitionEvent.java:123`** (event pipeline), which the `instanceof HMSExternalCatalog` event gate
  never fires for a flipped catalog. It is Model-B / event-deletion territory, **not a D2 blocker.**
  (Two critics initially mis-ranked this as a live-SQL blocker; the caller-trace refutes it.)
- **`CatalogMgr.add/dropExternalPartitions` silent no-op** — REFUTED as D2: callers are all event classes
  (`AddPartitionEvent`/`DropPartitionEvent`/`AlterPartitionEvent`) → event-gated → Model B.
- **`replayRefreshTable` partition branch breaks** — REFUTED: post-flip the `instanceof HMSExternalCatalog`
  guard is false → falls to `refreshTableInternal` which already fans out `connector.invalidateTable` for
  PluginDriven. Graceful coarsening, no crash.
- **Schema cache incoherence at flip** — REFUTED (§1.1).
- **Connector references to fe-core `*ExternalMetaCache` are a layering violation** — REFUTED:
  `grep 'import.*ExternalMetaCache'` over `fe-connector/` = zero; all mentions are javadoc. Deletion is
  import-clean.
- **`ENGINE_DEFAULT` routing throws at the flip** — REFUTED (§1.1, it's a registered cache).

---

## 3. ARCHITECTURE DECISION — where the metastore-metadata cache lives (needs sign-off)

Both options put caching **inside the connector** (honoring the LOCKED D2 decision) and both need a
**separate file-listing cache** (§4.4) — Trino keeps these two layers separate too. They differ only in
how the *metastore-metadata* layer is built:

**Option A — `CachingHmsClient` decorator (Trino `CachingHiveMetastore` style).** A
`CachingHmsClient implements HmsClient` in `fe-connector-hms`, wrapping `ThriftHmsClient`, caching
`getTable` / `listPartitionNames` / `getPartitions` / `getTableColumnStatistics` on
`MetaCacheEntry`s keyed by db/table (values are the connector's own `HmsTableInfo`/`HmsPartitionInfo`
DTOs). `HiveConnector` wraps its client once; `invalidateTable/invalidateAll` delegate to the decorator's
`flush(db,table)/flushAll()`.
- **Pros:** most Trino-faithful; **transparent** to all ~30 `hmsClient.*` call-sites in
  `HiveConnectorMetadata` (no cache threading, low error surface); a **reusable class** — the hudi/iceberg
  siblings each hold their own `HmsClient` from the same `fe-connector-hms` module (verified
  `HudiConnector:68`), so later wrapping their clients with the same decorator **fixes the hudi-on-HMS
  caching regression** with zero new machinery.
- **Cons:** caches raw metastore DTOs at RPC granularity (not the higher "resolved partition list" shape);
  its own TTL/key config; forks slightly from the paimon/iceberg *shape* (though not the ownership model).

**Option B — typed cache fields on `HiveConnector` (paimon/iceberg-native shape).** e.g.
`HivePartitionCache` (name+object; value carries max `transient_lastDdlTime` for §2.6) as final fields on
`HiveConnector`, mirroring `PaimonLatestSnapshotCache`; `HiveConnectorMetadata` reads through them.
- **Pros:** matches the existing Doris connector-owned precedent exactly (Rule 11); one obvious home;
  aligns 1:1 with iceberg/paimon `invalidateTable/invalidateAll` lifecycle.
- **Cons:** hive-specific (does nothing for the hudi-on-HMS regression); must thread the cache through each
  metadata method (more touch-points); caches at result granularity.

**Recommendation: Option A.** Hive's `HmsClient` is exactly the thin thrift shape Trino decorates (unlike
paimon/iceberg's rich catalog clients), the decorator is transparent over the many call-sites, and its
reusability cleanly retires the hudi-on-HMS regression. It still honors "connector-owned" and keeps the
file-listing cache separate (Trino-aligned). **This is the primary decision for user sign-off (§8-Q1).**

---

## 4. Design (assuming Option A; Option B differs only in §4.3 placement)

### 4.1 Dependency + packaging (dormant, build-only)
Add `fe-connector-cache` + **Caffeine 2.9.3** to `fe-connector-hive/pom.xml` (paimon pattern — hive
carries no transitive Caffeine). Verify the built plugin zip carries **exactly one** Caffeine 2.9.3
(memory `catalog-spi-connector-cache-framework-caffeine-coherence`: framework is child-loaded per plugin;
each consumer must self-bundle its lowest-common lib).

### 4.2 Cache set (minimum viable for scan parity)
1. **Metastore-metadata cache** (Option A: inside `CachingHmsClient`): `getTable` (table meta incl.
   `sd`+params → also feeds `getTableHandle`/`getColumnHandles`/`getTableStatistics`), `listPartitionNames`
   (pruning), `getPartitions(names)` (per-partition location/inputFormat/**parameters** → also the §2.6
   max-`transient_lastDdlTime` source), `getTableColumnStatistics` (optional; gated, low priority).
2. **File-listing cache** (§4.4): directory `listStatus` results keyed by partition path.
3. **Schema: NOT cached by the connector** — `ENGINE_DEFAULT` covers it (§1.1). Connector `getTableSchema`
   is hit only on a schema-cache miss.

### 4.3 §2.6 — cheap max-partition modify time (dormant fe-core one-liner + connector backing)
- **fe-core** (`PluginDrivenMvccExternalTable.getNewestUpdateVersionOrTime`): add a last-modified branch
  mirroring `getTableSnapshot` — when the query pin's `isLastModifiedFreshness()` is set, return
  `queryTableFreshness().getTimestampMillis()`; else keep the exact existing path.
  **Byte-neutrality constraint (memory `plugindriven-mvcc-table-is-live-not-dormant`):** paimon/iceberg
  set `isLastModifiedFreshness()==false` → they keep the current RANGE/`orElse(0L)` path unchanged. The
  new branch is dormant until a hive (last-modified) connector exists at the flip. Guard so paimon/iceberg
  bytes **and** cost are unchanged.
- **connector**: `getTableFreshness`/`getPartitionFreshnessMillis` read the **D2 metastore-metadata cache**
  (cached `getPartitions`), not a live RPC, so the periodic dictionary poll stays cheap.
- **Parity (verified):** legacy `getNewestUpdateVersionOrTime` returns 0 **only for a genuinely empty
  partition set**; for an unpartitioned table it builds a partition from table params → table
  `transient_lastDdlTime`. The connector's `getTableFreshness` already returns `lastDdlMillis(tableParams)`
  for unpartitioned and `0` for empty-partition-set (`:1039-1046`) — **matching legacy**. Confirm at
  implementation; if a divergence surfaces, surface it for sign-off (do not silently change behavior).
- **Monotonicity:** `Dictionary.hasNewerSourceVersion` THROWS on a *smaller* value than last seen. Confirm
  `transient_lastDdlTime` is non-decreasing across partition rewrites (it is HMS-maintained epoch seconds).

### 4.4 File-listing cache + TCCL
The directory listing runs plugin-side via Hadoop `FileSystem` reflection. **Adopt the template's
contextual + manual-miss + `autoRefresh=false` pattern** so the loader runs on the **caller (scan)
thread**, which `PluginDrivenScanNode.onPluginClassLoader` already TCCL-pins — the safest choice. If a
dedicated listing executor with background refresh is chosen for perf, **pin TCCL to the connector
classloader inside every loader** (mirror the existing save/set/restore at
`HiveConnectorMetadata:719-727`). This same file-listing cache should also back
`estimateDataSizeByListingFiles` (`:815`) so the periodic `ExternalRowCountCache` row-count refresh for a
no-stats plain-hive table is not an uncached re-listing (critic-confirmed 5th-cache coupling).
> Note: `fsCache` (fe-core `FileSystemCache`) is NOT part of D2 — the connector uses Hadoop's native
> `FileSystem` cache (scheme+authority+UGI keyed). Confirm `doAs`/UGI keying doesn't defeat it.

### 4.5 Invalidation scope (coarse only)
- Override `HiveConnector.invalidateTable(db,table)` (drop that table's metastore + file entries) and
  `invalidateAll()` (clear all). fe-core wiring already exists (§1.4) — no fe-core change.
- **REFRESH DB gap:** only `invalidateTable`+`invalidateAll` reach the connector; `REFRESH DB` on a
  flipped catalog won't drop per-table entries for that db. **Recommendation: accept per-table/all
  coverage** for D2 (REFRESH CATALOG covers it; a db-level verb is Model-B-adjacent). Documented, not asked.
- Partition-granular invalidation + event re-arming = **Model B**, out of scope.

### 4.6 Dormancy proof
- `"hms"` is absent from `SPI_READY_TYPES` (`CatalogFactory:55-56`) → **no `HiveConnector` is built for
  hms** until the flip; the cache fields/decorator exist but are never instantiated for a live catalog.
- `invalidateTable/invalidateAll` overrides fire only for a `PluginDrivenExternalCatalog` holding a
  `HiveConnector` — none exist for hms pre-flip.
- The §2.6 fe-core branch only executes for `isLastModifiedFreshness()==true` — no live connector sets it.
- ⇒ Every sub-step is byte-neutral for paimon/iceberg/jdbc/doris and inert for hms until the flip.

---

## 5. Decomposition — ordered dormant commits (mirrors the iceberg/hudi lines)

| step | what | notes |
|---|---|---|
| **C-a** | pom: add `fe-connector-cache` + Caffeine 2.9.3 | build-only, dormant |
| **C-b** | metastore-metadata cache (Option A: `CachingHmsClient` in `-hms`; or Option B fields), from `meta.cache.hive.*` props, **unconsulted** | dormant field/decorator, not yet wired |
| **C-c** | route `getTable`/`listPartitionNames`/`getPartitions` through it **and** back `getTableFreshness`/`getPartitionFreshnessMillis` with it | dormant (connector not built for hms yet) |
| **C-d** | file-listing cache + route `HiveScanPlanProvider` listing + `estimateDataSizeByListingFiles` through it (TCCL) | separable, highest complexity |
| **C-e** | implement `HiveConnector.invalidateTable/invalidateAll` | arms REFRESH TABLE/CATALOG |
| **C-f** | §2.6 fe-core `getNewestUpdateVersionOrTime` last-modified branch (reads the now-cheap cached max time) | byte-neutral for paimon/iceberg |

Each is an independent reviewable commit with its own tests, exactly like every prior connector step.
After all land, run a unified adversarial re-review (clean-room, per project practice) before the flip.

---

## 6. Edge cases / risks
- **Byte-neutrality for live PluginDriven MVCC connectors** (§4.3) — the single sharpest correctness risk;
  guard the §2.6 branch on `isLastModifiedFreshness()` and add a paimon/iceberg no-change test.
- **TCCL split-brain** on async listing threads (§4.4) — prefer the pinned-scan-thread loader.
- **Caffeine version split-brain** in the plugin zip (§4.1) — verify exactly one 2.9.3.
- **Interim event staleness** (§2 non-goals) — bounded by TTL + REFRESH until Model B; call it out, don't
  hide it (Rule 12).
- **Cache must never enter the GSON edit log** (§1.5c) — keep all state on the `Connector`/decorator; no
  `*Info`/handle carries a cache reference.

## 7. Testing (dormant-testable)
- `CachingHmsClient` (or the typed cache): hit/miss, TTL/capacity from props, `flush(db,table)`/`flushAll`,
  value identity — same-loader unit tests in `fe-connector-hive`/`-hms`.
- §2.6: a `PluginDrivenMvccExternalTableTest` asserting (a) `isLastModifiedFreshness` → freshness millis;
  (b) paimon/iceberg (snapshot-id) path **unchanged** (byte/cost parity); (c) unpartitioned → tableDdl,
  empty-partition-set → 0.
- Invalidation: `HiveConnector.invalidateTable/invalidateAll` drop the right entries.
- checkstyle 0 + import gate net, per project bar.
- **e2e-owed (flip / Phase 4):** cache hit under a real flipped hms catalog; REFRESH TABLE/CATALOG
  end-to-end; dictionary/MV auto-refresh over a hive base sees new data; hudi-on-HMS perf (separate).

---

## 8. Open decisions — RESOLVED (user sign-off 2026-07-10)

- **Q1 — metastore-metadata cache placement → Option A: `CachingHmsClient` decorator (Trino-style).**
  A `CachingHmsClient implements HmsClient` in `fe-connector-hms` wrapping `ThriftHmsClient`; transparent
  to the ~30 call-sites; reusable so the hudi/iceberg siblings can later wrap their own clients (fixes the
  hudi-on-HMS regression for free). §3.
- **Q2 — file-listing cache scope → include it in this step (step C-d).** Full legacy parity at the flip;
  accept the TCCL/executor complexity (prefer the pinned-scan-thread loader, §4.4). Do NOT defer.
- **Settled without asking** (recorded, will proceed unless corrected): deletion of the fe-core caches →
  final deletion phase, not D2; §2.6 → design for exact legacy parity, surface only if divergent; REFRESH
  DB → accept per-table/all coverage; column-stats → defer; hudi-on-HMS perf → separate hudi item (or free
  via Q1-Option-A).

---

## 9. TODO (fill after Q1/Q2 sign-off)
- [x] C-a pom dep + Caffeine 2.9.3 (+ verify single-copy in zip) — `f742651990d`
- [x] C-b metastore-metadata cache (per Q1) from `meta.cache.hive.*`, unconsulted + tests — `4fe55d88fab`
- [x] C-c wire metadata reads + freshness probes through it + tests — `7b05df6e55e` (wrapWithCache seam in
      HiveConnector.createClient; transparent decorator => all HiveConnectorMetadata reads + both freshness
      probes cache-backed by the single wrap; HiveConnectorClientCacheTest 4; hive 244 green)
- [x] C-d file-listing cache (per Q2) + TCCL + row-count backing + tests — `7c0ee1ffb2a` (HiveFileListingCache,
      connector-owned + shared by scan provider AND estimate; (db,table,location) key; contextual+manual-miss
      loader on the TCCL-pinned caller thread; dir/hidden filter parity; failure-not-cached; splitFile refactored
      to primitives; HiveConnectorMetadata 7-arg production ctor; HiveFileListingCacheTest 9; hive 253 green)
- [x] C-e `invalidateTable/invalidateAll` overrides + tests — `7bf90a7fe3c` (drop BOTH layers per table / all;
      no force-build of the client; package-private client-overloads + fileListingCacheForTest()/size() seams;
      HiveConnectorInvalidateTest 3; hive 256 green)
- [x] C-f §2.6 fe-core last-modified branch (byte-neutral guard) + tests — `12e0c9177c2` (isLastModifiedFreshness
      pin gate mirrors getTableSnapshot; snapshot-id connectors byte/cost-neutral; PluginDrivenMvccExternalTableTest
      +3 = 56 green + checkstyle 0)
- [ ] unified adversarial re-review before the flip  ← NEXT (all 6 dormant commits C-a…C-f landed)
- [x] update HANDOFF.md per step (per-phase discipline)
