# Design — Porting the connector hand-rolled caches onto the copied cache framework

Status: **DONE — all three caches ported, unit + full-module verified; flip-gated e2e pending (2026-07-01)** ·
Branch: `catalog-spi-10-iceberg`
Parent design: [metacache-framework-unification-design.md](./metacache-framework-unification-design.md) (§5 P3–P5)
Scope this round (user, 2026-07-01): **iceberg + paimon together** — all three hand-rolled caches.

> Naming note: this doc avoids internal task codenames; it refers to the caches by what they do.

---

## 1. Problem

Three connector caches are hand-rolled `ConcurrentHashMap` ports of fe-core framework entries (each
reimplements TTL/eviction by hand). The previous step copied the framework core (`MetaCacheEntry` +
`CacheFactory` + `MetaCacheEntryStats` + `CacheSpec`) into the standalone module `fe-connector-cache`
(package `org.apache.doris.connector.cache`). This step makes the three caches **actually use** that
framework, so the hand-rolled `ConcurrentHashMap` machinery is retired.

| Cache | Today (hand-rolled) | Target (framework) |
|---|---|---|
| iceberg latest-snapshot (`IcebergLatestSnapshotCache`) | CHM, access-TTL, cap 1000, clear-on-overflow | `MetaCacheEntry` contextual, access-TTL, cap 1000 |
| iceberg manifest (`IcebergManifestCache`) | CHM, no-TTL, cap 100000, clear-on-overflow | `MetaCacheEntry` contextual, no-TTL, cap 100000 |
| paimon latest-snapshot (`PaimonLatestSnapshotCache`) | CHM, access-TTL, cap 1000, clear-on-overflow | `MetaCacheEntry` contextual, access-TTL, cap 1000 |

---

## 2. The load-bearing finding this step must fix first: Caffeine coherence per plugin

The framework's底层 is **Caffeine**. Under the independent-copy strategy, `fe-core` does **not** depend on
`fe-connector-cache`; therefore the framework classes (in the parent-first `org.apache.doris.connector.*`
prefix) resolve **parent → miss (fe-core lacks them) → child**, i.e. they load **child-first from the
plugin's own bundled jar** and link against the **plugin's own bundled Caffeine**. This corrects the P1②
pom note's implicit "single app-loader identity" assumption (true only under the abandoned *move* strategy).

Consequences (verified against the plugin poms / assemblies / dependency tree):
- **iceberg plugin bundles Caffeine 2.9.3** (from `iceberg-core`; the vendored `DeleteFileIndex` pins it).
  The framework was compiled against **3.2.3** → 3.2.3-compiled bytecode running on 2.9.3 = binary-skew risk.
- **paimon plugin bundles NO usable Caffeine** (`com.github.ben-manes.caffeine` absent from its tree; any
  paimon-internal caffeine is shaded to `org.apache.paimon.shade.*` and unusable) → loading `MetaCacheEntry`
  would `NoClassDefFoundError`.

**Fix (this step, prerequisite):**
1. Compile `fe-connector-cache` against **Caffeine 2.9.3** (the lowest version any consumer runs — iceberg's),
   `provided` (bundles nothing). **Verified: builds + all 20 framework tests green against 2.9.3** →
   `MetaCacheEntry`/`CacheFactory` use only APIs present in both 2.9.3 and 3.2.3, so the bytecode is
   binary-safe on 2.9.3 (iceberg) and would also run on 3.2.3.
2. **paimon plugin: add `com.github.ben-manes.caffeine:caffeine:2.9.3`** (default/compile scope → bundled in
   its plugin zip) so the framework has a Caffeine to link against at runtime.

fe-core is untouched (it keeps its own `datasource.metacache` + its own 3.2.3). This is Trino-aligned: Trino
plugins are self-contained classloaders that bundle their own dependencies (including caching libs); the SPI
layer stays dependency-free. Keeping `fe-connector-api/spi` Caffeine-free and letting each plugin carry its
own Caffeine matches that model.

---

## 3. Design — thin adapters over `MetaCacheEntry` (surgical)

Keep each cache class as a **thin adapter** with its existing public method signatures and value types
(`CachedSnapshot`, `ManifestCacheValue`, keys), but replace the internal `ConcurrentHashMap` with a single
`MetaCacheEntry`. This keeps every call site (`IcebergConnector`, `IcebergConnectorMetadata`,
`IcebergScanPlanProvider`, `PaimonConnector`, `PaimonConnectorMetadata`) unchanged. It is the minimal
realization of "port internals onto the framework" (parent design §5) and folds P5 (delete CHM) into the port.

**Common wiring for all three:**
- `contextualOnly = true`, `loader = null` → the per-call loader is supplied via `entry.get(key, missLoader)`,
  matching today's `getOrLoad(key, loader)` shape exactly (the loader closes over the table/handle).
- `autoRefresh = false`, `manualMissLoadEnabled = false`, `refreshAfterWriteSeconds = 0`. No background
  refresh (the hand-rolled caches never refreshed); Caffeine `get(key, fn)` gives per-key single-flight (an
  improvement over today's "load outside lock, tolerate harmless double-load"), computing on the caller thread.
- `refreshExecutor = ForkJoinPool.commonPool()` (Caffeine's own default). `MetaCacheEntry`'s ctor requires a
  non-null `ExecutorService`; commonPool is shared, daemon, needs no lifecycle, and only runs Caffeine's
  internal maintenance (never our loader, since loads are synchronous on the caller) → no TCCL concern.
- `invalidate(key)` → `entry.invalidateKey(key)`; `invalidateAll()` → `entry.invalidateAll()`.
- Caffeine `maximumSize` eviction (LRU-ish) **replaces** clear-on-overflow. Safe: all cached values are
  reload-safe (latest live snapshot / immutable manifest content). This is the point of adopting the framework.

**TTL-semantics translation (CRITICAL correctness point — gets a dedicated test):**
The hand-rolled caches treat **`ttl-second <= 0` as "disabled / always read live"**. But `CacheSpec`/
`MetaCacheEntry` treat **`ttl == -1` as "no expiration (ENABLED)"** and only `ttl == 0` as disabled. So the
adapter must NOT pass a `<= 0` ttl straight through. Mapping:
- latest-snapshot adapters: `ttlSeconds <= 0` → build a **disabled** spec (`CacheSpec.of(true, 0, cap)` →
  `isCacheEnabled` false → `get(key, loader)` loads every call, caches nothing = "always live"); else
  `CacheSpec.of(true, ttlSeconds, cap)` (access-TTL via `expireAfterAccess`, cap).
- manifest adapter: always enabled, no expiry → `CacheSpec.of(true, CacheSpec.CACHE_NO_TTL, 100_000)`
  (`isCacheEnabled(true, -1, 100000)` = true; `toExpireAfterAccess(-1)` = no expiry; cap 100000). The
  external enable-gate (`meta.cache.iceberg.manifest.enable`) stays where it is (the scan provider decides
  whether to take the manifest-planning path at all); the adapter itself is unconditionally on when consulted.

**Behavior deviations kept as-is (pre-existing connector simplifications; NOT changed here — surgical):**
- manifest catalog-invalidation does **not** call `ManifestFiles.dropCache(io)` (legacy fe-core did). Flag
  as a pre-existing follow-up; not introduced or fixed by this port.
- latest-snapshot value carries only `(snapshotId, schemaId)`, not `IcebergPartitionInfo` (legacy did). Same:
  pre-existing, out of scope.

---

## 4. Implementation plan (independent commits) — ALL DONE

- **C1 — packaging prerequisite (`24e4c830aeb`):** `fe-connector-cache` Caffeine `3.2.3 → 2.9.3` (`provided`).
  Child-first per-plugin linkage against iceberg's 2.9.3. Build + 20 framework tests green.
- **C2 — iceberg latest-snapshot adapter (`0be2679a7ac`):** `IcebergLatestSnapshotCache` now holds a
  `MetaCacheEntry<TableIdentifier, CachedSnapshot>`; `CachedSnapshot`/`getOrLoad`/`invalidate`/`invalidateAll`
  unchanged. Test: dropped injectable-clock timing test, added a `-1` disable-trap guard. 5/5 + connector 6/6.
- **C3 — iceberg manifest adapter (`bc27505eace`):** `IcebergManifestCache` now holds a
  `MetaCacheEntry<IcebergManifestEntryKey, ManifestCacheValue>`; static `loadManifestCacheValue` I/O kept as
  the per-call miss loader. 4/4 + scan-provider 88/88.
- **C4 — paimon (`47c4bcc6fd9`):** added Caffeine 2.9.3 to the paimon plugin pom; `PaimonLatestSnapshotCache`
  now holds a `MetaCacheEntry<Identifier, Long>`. Plugin zip verified to bundle exactly `caffeine-2.9.3.jar`
  (no conflict). 5/5 + connector 4/4.
- **Doc-fix (`808c0cb0f0c`):** corrected stale "single Class identity" comments in `CacheSpec.java` javadoc +
  iceberg pom + the two adapter-test javadocs (from the review's one confirmed, doc-only finding).

**Chosen framework flags (all three adapters):** `contextualOnly=true`, `loader=null` (per-call missLoader via
`get(key, missLoader)`), `autoRefresh=false`, `manualMissLoadEnabled=true` (loader runs OUTSIDE Caffeine's
compute lock, single-flight; AND makes the disabled path a definitive bypass — not reliant on async
`maximumSize(0)` eviction), `refreshAfterWriteSeconds=0`, `executor=ForkJoinPool.commonPool()`. `size()` via
`forEach` count (accurate map membership); `isEnabled()` via `stats().isEffectiveEnabled()`.

**Verification:** full iceberg + full paimon module suites green (0 failures); checkstyle 0 × 3 modules;
import gate clean on my files. **Clean-room adversarial review (3 lenses + adversarial verify):** 1 confirmed
(doc-only, fixed in `808c0cb0f0c`); all behavior/packaging/framework-API findings refuted (capacity==0 disable
= unreachable, callers hardcode 1000; timed-expiry-coverage = deliberate/covered at framework layer).

**Still flip-gated (NOT run — no cluster this session):** `test_iceberg_table_meta_cache` /
`test_paimon_table_meta_cache` + a redeploy classloader smoke check (the one thing that end-to-end proves the
plugin-bundled `MetaCacheEntry` links the plugin's Caffeine correctly).

---

## 5. Risk analysis

- **Caffeine binary skew (iceberg 2.9.3):** mitigated by compiling the framework against 2.9.3 (C1) and
  proven by the framework tests passing on 2.9.3. Residual: only a redeploy/classloader smoke test can prove
  the child-first linkage end-to-end in a live plugin — **flip-gated, cannot run this session (no cluster)**;
  marked pending. Unit tests validate logic.
- **paimon new dependency:** adds ~1–2 MB Caffeine to the paimon plugin zip (accepted by the user).
- **TTL `<= 0` semantics flip** (the `-1` = no-expiry trap): guarded by the dedicated adapter mapping + a unit
  test asserting `ttl <= 0` (incl. `-1`) disables (loads every call).
- **Concurrency change** (double-load → single-flight): an improvement, no correctness regression (values
  reload-safe). Manifest I/O now runs inside Caffeine's per-key compute (single-flight, per-key lock only) —
  matches legacy fe-core's default `get(key, loader)` path.
- **Split-brain:** N/A under copy — fe-core and connectors never share a cache object; no Caffeine type
  crosses the boundary (adapters expose only the Caffeine-free `MetaCacheEntry` API; `CacheFactory` stays
  framework-internal).

## 6. Test plan

**Unit (per commit):** adapter behavior — (a) enabled: same loader value served across calls (loader invoked
once), (b) `ttl <= 0` incl. `-1`: loader invoked every call, nothing cached, (c) `invalidate` drops one key
(next call reloads), (d) `invalidateAll` clears. Manifest: (a) miss loads + parses once, hit reuses,
(b) `invalidateAll` clears. Timed-expiry mechanics are Caffeine's and stay covered by the framework module's
own `MetaCacheEntryTest` (not re-proven per adapter). Mutation-check the TTL-mapping guard.

**E2E (flip-gated, cannot run now):** `test_iceberg_table_meta_cache` / `test_paimon_table_meta_cache`
(stale-vs-refresh row counts) + a redeploy classloader smoke check that the plugin-bundled `MetaCacheEntry`
runs against the plugin's Caffeine. Marked pending redeploy.
