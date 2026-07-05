# Design — Unifying the external meta-cache framework for connector reuse

Status: **Option A — INDEPENDENT-COPY variant (user, 2026-07-01, revised); fe-core untouched; P1① CacheSpec done** ·
Branch: `catalog-spi-10-iceberg` · Date: 2026-07-01

> **⚠️ DECISION REVISED (2026-07-01, later) — "independent copy", NOT "move".**
> The user narrowed Option A: build a **standalone** cache framework inside the new module
> `fe-connector-cache` (package `org.apache.doris.connector.cache`) that serves **only the fe-connector
> connectors**; **fe-core's existing `org.apache.doris.datasource.metacache` framework is left completely
> untouched** (zero fe-core edits). The two live side-by-side as **independent duplicates** during the
> migration; the fe-core copy is deleted **only after every connector has migrated onto the SPI framework**.
> This supersedes the original "move + repoint ~16 fe-core importers" wording throughout §4-A/§5/§8 below —
> wherever those say "move the framework out of fe-core / fe-core imports the moved core", read instead
> "**copy** the class into `connector.cache`; fe-core keeps its original". Rationale: smallest blast radius
> (fe-core byte-identical to HEAD → zero regression risk), fully decoupled, and it sidesteps the fe-core↔
> connector coupling the move would have introduced. Accepted cost: temporary duplication (removed at the end).
>
> **Original decision (2026-07-01, earlier — SUPERSEDED):** user chose **Option A** as "move the generic
> framework into a shared connector-visible module; connectors OWN their caches." Post-decision verification
> showed A's headline risk — the Caffeine classloader split-brain — is **avoidable**, because
> `org.apache.doris.connector.*` is loaded **parent-first** (single app-loader identity) and `MetaCacheEntry`
> encapsulates Caffeine. That split-brain analysis still holds for the independent-copy variant.

Supersedes the narrower [metacache-connector-cachespec-design.md](./metacache-connector-cachespec-design.md)
(the `CacheSpec` validation restore — done, unit-verified). That work stands; the `CacheSpec` mirror it
created is one of the things this larger migration would collapse.

> **Goal (user, 2026-07-01):** not merely moving `CacheSpec`, but making the whole
> `org.apache.doris.datasource.metacache` cache *framework* reusable, so connector-side hand-rolled caches
> (e.g. `fe-connector-iceberg` `IcebergManifestCache`) are **served by the framework** — concretely, the
> connector's manifest cache should be *承接* (taken over) by fe-core's `IcebergExternalMetaCache.manifestEntry`.

---

## 1. Problem

After the SPI flip, native `type=iceberg`/`type=paimon` catalogs run as `PluginDrivenExternalCatalog` and
their metadata caching moved **into the plugin**, hand-reimplemented because the import gate forbids
connectors from importing fe-core. The result is **three connector caches that are byte-for-byte ports of
fe-core framework entries**, each a plain `ConcurrentHashMap` reimplementing TTL/eviction by hand (their own
javadoc: *"the connector cannot import fe-core, so this is a PORT, not a reuse"*,
`IcebergManifestCache.java:34-37`):

| Connector cache | Impl | fe-core framework entry it duplicates |
|---|---|---|
| `IcebergManifestCache` + `ManifestCacheValue` | CHM, no-TTL, cap 100000, clear-on-overflow | `IcebergExternalMetaCache.manifestEntry` (`contextualOnly`, `CacheSpec.of(false, CACHE_NO_TTL, 100_000)`) + `IcebergManifestCacheLoader` |
| `IcebergLatestSnapshotCache` (snapshotId+schemaId) | CHM, access-TTL, cap 1000 | `IcebergExternalMetaCache.tableEntry` latest-snapshot projection (drops `IcebergPartitionInfo`) |
| `PaimonLatestSnapshotCache` (snapshotId) | CHM, access-TTL, cap 1000 | **none live** — restores the deleted legacy `PaimonExternalMetaCache` table cache |

The framework itself (`MetaCacheEntry` + Caffeine, striped-lock miss-load, `refreshAfterWrite`, stats,
predicate invalidation) is strictly richer than these hand-rolled maps. The duplication is real drift risk
(the `CacheSpec` fork already diverged: `DdlException`→`IllegalArgumentException`, `NumberUtils`→inlined).

---

## 2. Research findings (verified against code)

### 2.1 The framework model
A concrete cache `extends AbstractExternalMetaCache`, declares slots in its ctor via
`MetaCacheEntryDef.of(...)` (loader + `autoRefresh`) or `.contextualOnly(...)` (no loader, caller supplies a
miss-loader at `get(K, missLoader)`), and `registerEntry(def) → EntryHandle<K,V>`. `initCatalog(id, props)`
materializes one `MetaCacheEntry<K,V>` per catalog per def; `newMetaCacheEntry` bridges
`CacheSpec.fromProperties(props, engine, entry, defaultSpec)` → Caffeine (ttl→`expireAfterAccess`,
capacity→`maximumSize`, `refreshAfterWrite=Config.external_cache_refresh_time_minutes*60` when
enabled+autoRefresh) (`AbstractExternalMetaCache.java:184-303`, `MetaCacheEntry.java:78-110`,
`common/CacheFactory.java:58-102`). Manual-miss-load (128 striped locks + `invalidateGeneration`) is gated by
`Config.enable_external_meta_cache_manual_miss_load` (`MetaCacheEntry.java:209-261`).

**Migratability (per-class):**
- **Generic / dependency-free:** `MetaCacheEntryDef`, `MetaCacheEntryStats`, `CatalogEntryGroup`,
  `ExternalMetaCacheRegistry`, and `common/CacheFactory` (Caffeine-only content). `MetaCacheEntry` and legacy
  `MetaCache` are generic **except** they read 2 `Config` static knobs. `CacheSpec` is generic except its
  validators throw fe-core `DdlException` (already forked into `connector-api`).
- **Hard fe-core-bound:** `AbstractExternalMetaCache` (`Env`, `CatalogIf`, `ExternalCatalog/Table`,
  `CacheException`), and `ExternalMetaCacheRouteResolver` (**source-specific** `instanceof
  IcebergExternalCatalog/HMSExternalCatalog/…`).

### 2.2 fe-core usage (concrete caches × entries)
`IcebergExternalMetaCache` {`table`,`view`,`manifest`,`schema`}, `HiveExternalMetaCache`
{`schema`,`partition_values`,`partition`,`file` — opts out of predicate invalidation, drives it manually},
`HudiExternalMetaCache`, `DorisExternalMetaCache` — all constructed once at startup and registered in
`ExternalMetaCacheMgr` (`:290-296`). The highlighted `manifestEntry` =
`contextualOnly(CacheSpec.of(false, CACHE_NO_TTL, 100_000))` (`IcebergExternalMetaCache.java:98-100`).

### 2.3 Liveness verdict (decisive)
For a **native `type=iceberg`/`type=paimon`** catalog (now `PluginDrivenExternalCatalog`), the **entire
`IcebergExternalMetaCache` — including `manifestEntry` — is DEAD**:
- `ExternalMetaCacheRouteResolver` routes by concrete class; `PluginDrivenExternalCatalog extends
  ExternalCatalog` (not `IcebergExternalCatalog`) → routes only to `DefaultExternalMetaCache`
  (`ExternalMetaCacheRouteResolver.java:62-80`).
- `PhysicalPlanTranslator` matches `PluginDrivenExternalTable` first → `PluginDrivenScanNode`; the
  `IcebergScanNode` branch (`:884`) is dead for these catalogs (`:843-894`).
- `getManifestCacheValue`'s only caller chain is `IcebergManifestCacheLoader` ← `IcebergScanNode` — live
  **only for iceberg-on-HMS** (`type=hms`, `DlaType.ICEBERG`), which is deliberately excluded from
  `SPI_READY_TYPES`.
- The live manifest cache for PluginDriven iceberg is `IcebergConnector.manifestCache`, dropped by
  `PluginDrivenExternalCatalog.onRefreshCache → connector.invalidateAll()` (`:254-263`).

**LIVE:** `DefaultExternalMetaCache.schema` (serves PluginDriven iceberg *and* paimon schema — PluginDriven
tables inherit engine `"default"`); Hive/Hudi/Doris caches; iceberg-on-HMS's `IcebergExternalMetaCache`.
**DEAD (for native iceberg/paimon):** all of `IcebergExternalMetaCache`'s entries. **Paimon:** has *no*
fe-core meta cache at all (`PaimonExternalMetaCache` was deleted at cutover).

> So "让 `manifestEntry` 承接 `IcebergManifestCache`" means **reactivating the framework as the owner** of
> the connector's manifest cache — not wiring into a currently-live fe-core entry (it's dead for native
> iceberg).

### 2.4 Constraints (all verified)
1. **Import gate** (`tools/check-connector-imports.sh`, `fe-connector/pom.xml` validate phase): package-prefix
   based — forbids `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}` in connectors,
   whitelists `thrift|connector|extension|filesystem`. A generic class is blocked purely by its package, so
   any shared class must re-home under `org.apache.doris.connector.*`.
2. **Caffeine / zero-third-party charter:** `fe-connector-api` and `fe-connector-spi` declare **zero**
   third-party deps (spi pom: *"Zero third-party external dependencies — only JDK and Doris internal SPI
   interfaces"*). fe-core has Caffeine 3.2.3; the **iceberg plugin uses Caffeine 2.9.3** (declared compile,
   vendored `DeleteFileIndex`) and plugin zips bundle 3.2.3 child-first. Putting Caffeine on the connector
   classpath breaks the charter **and** re-opens the classloader split-brain (cf. MEMORY
   `catalog-spi-plugin-tccl-classloader-gotcha`).
3. **Plugin-zip single identity:** the assembly excludes `fe-connector-api/spi`, `fe-extension-spi`,
   `fe-filesystem-api` → these load once on the app classloader (single `Class` identity across the
   boundary). Caffeine is **not** excluded → stays duplicated per plugin.
4. **fe-core depends on `fe-connector-api` + `fe-connector-spi`** (`fe-core/pom.xml:100-110`) → a class placed
   there is usable by both sides. `ConnectorContext` (connector-spi) is the existing fe-core→connector
   injection seam: it already exposes `getMetaInvalidator()→ConnectorMetaInvalidator.NOOP` (`:105`) and
   `executeAuthenticated` (the TCCL-pin entry, `:94`).

---

## 3. Goals / non-goals

**Goals**
- One meta-cache abstraction serving both fe-core-hosted engines and plugin connectors — connectors stop
  hand-rolling `ConcurrentHashMap` caches and reuse the framework (Caffeine eviction, TTL, refresh, stats,
  invalidation semantics).
- Collapse the `CacheSpec` fork (connector-api mirror vs fe-core original) to a single source of truth.
- Preserve today's REFRESH CATALOG/TABLE invalidation semantics and the `<= 0 ttl disables` behavior.

**Non-goals**
- Not migrating iceberg-on-HMS off the legacy path in this change (keeps `IcebergExternalMetaCache` live
  there; its native-iceberg entries stay dead-but-present until hms flips).
- Not changing hive/hudi/doris cache behavior (their catalogs haven't flipped).
- Not moving the `Env`/catalog-coupled framework core (`AbstractExternalMetaCache`, `RouteResolver`) out of
  fe-core.

---

## 4. Architecture options

The fork below **determines everything else**. All three satisfy the goal of "one abstraction"; they differ
on *where the cache lives* and *what crosses the boundary*.

### Option A — Move the generic framework into a shared connector module; connectors OWN caches ✅ CHOSEN
Move `CacheFactory`+`CacheSpec`+`MetaCacheEntry`+`MetaCacheEntryDef`+`MetaCacheEntryStats`+
`MetaCacheEntryInvalidation`+`CatalogEntryGroup`+`ExternalMetaCacheRegistry` into
`org.apache.doris.connector.api.cache`, **add Caffeine to `fe-connector-api`**, inject the 2 `Config` knobs.
fe-core keeps `AbstractExternalMetaCache`/`RouteResolver`/`Mgr`/concrete engine caches and imports the moved
core.
- ✅ Single source of truth; connectors get the full Caffeine machinery (Caffeine eviction, `refreshAfterWrite`,
  stats, striped-lock miss-load) locally; collapses the `CacheSpec` fork.
- ✅ **Split-brain is AVOIDABLE (verified post-decision).** `ConnectorPluginManager` loads
  `org.apache.doris.connector.*` **parent-first** (`:64-65`) → the moved framework classes have a **single
  app-loader identity**. `MetaCacheEntry`'s cross-boundary API is **Caffeine-free** (Caffeine is internal;
  `stats()` returns the generic `MetaCacheEntryStats`), so the framework's app-loader Caffeine never meets the
  plugin's child-first Caffeine (iceberg's vendored 2.9.3, used only by `org.apache.iceberg.*` child-loaded
  code). They coexist without sharing objects.
- ⚠️ **The load-bearing rule that keeps it safe:** connectors must interact **only** through the Caffeine-free
  `MetaCacheEntry` API. `CacheFactory`'s public API *returns* Caffeine types (`LoadingCache<K,V>`), so it must
  stay **framework-internal** — a connector calling it directly would receive an app-loader Caffeine object
  that its child-first loader re-resolves → ClassCast (the MEMORY tccl-gotcha failure mode).
- ⚠️ **Real remaining costs:** (1) adds Caffeine as a compile dep to `fe-connector-api` — **breaks the stated
  "zero third-party dependencies" charter** (a policy change, technically clean under parent-first). (2) Inject
  the 2 `Config` knobs into `MetaCacheEntry` (ctor params) since it cannot read fe-core `Config`. (3) Blast
  radius: **~16 fe-core files** import the framework/`CacheFactory` (iceberg×3, hive×2, hudi, doris,
  `ExternalCatalog`/`ExternalDatabase`/`ExternalMetaCacheMgr`, `property/metastore`, `tablefunction`) plus the
  2 non-metacache `CacheFactory` callers (`ExternalRowCountCache`, `FileSystemCache`) — all mechanical import
  updates + the ctor-knob threading. (4) `SchemaCacheValue`→`catalog.Column`: keep the schema-value concern in
  fe-core's `AbstractExternalMetaCache` (don't drag `Column` connector-visible).

### Option B — fe-core keeps owning caches; connector delegates via an SPI handle on `ConnectorContext` ✅ RECOMMENDED
Keep the whole framework in fe-core. Add a **dependency-free** handle in `fe-connector-spi`:
```java
public interface ConnectorMetaCache<K, V> {
    V get(K key, java.util.function.Supplier<V> loader);   // contextual miss-load
    V getIfPresent(K key);
    void invalidate(K key);
    void invalidateAll();
    // stats() optional
}
```
obtained via a new `ConnectorContext.getMetaCache(String entryName, CacheSpec defaultSpec)` (default returns
a trivial no-cache impl for back-compat), **implemented in fe-core by wrapping a `MetaCacheEntry`**.
Connectors delete their `ConcurrentHashMap` caches and call the injected handle; `IcebergManifestCache`
becomes a thin caller of a fe-core-backed `MetaCacheEntry` — i.e. `manifestEntry` *承接* it. Only a neutral
interface + the already-dependency-free `CacheSpec` cross the boundary — **no Caffeine crosses**.
- ✅ Satisfies **all three** hard constraints at once (import gate, zero-third-party charter, Caffeine
  split-brain); ✅ reuses fe-core framework + `Config` knobs + stats + striped-lock miss-load; ✅ matches the
  existing `ConnectorContext` seam direction and TCCL-pin machinery; ✅ **directly realizes the user's
  "manifestEntry 承接" framing**; ✅ smallest blast radius (mostly *reactivates* dead fe-core code for the
  connector rather than touching hot paths).
- ⚠️ Cache **state lives in fe-core** keyed by connector-supplied keys → value/key types are generic `<K,V>`
  (or `Object` + cast) across the seam; loader lambdas re-enter connector code → **must pin TCCL** (machinery
  exists: `TcclPinningConnectorContext`); per-cache plumbing to wire each entry.

### Option C — Hybrid: shared dependency-free *declaration* layer, fe-core-hosted Caffeine
Promote only the dependency-free declaration classes (`CacheSpec`, `MetaCacheEntryDef`, `Stats`,
`MetaCacheEntryInvalidation` minus `forNameMapping`, `Registry`, `CatalogEntryGroup`) to connector-api as a
shared vocabulary; keep `MetaCacheEntry`/`CacheFactory`/Caffeine in fe-core behind the Option-B handle.
Connectors *author* full `MetaCacheEntryDef`s; fe-core materializes them.
- ✅ Collapses the `CacheSpec` fork and gives connectors real def/invalidation vocabulary, no Caffeine on the
  connector classpath. ❌ Two-layer split = more moving parts; marginal benefit over B unless connectors need
  to author full defs/predicates rather than consume a get/invalidate handle.

### Decision & recommendation
The research pass recommended **B** (least blast radius, keeps the charter). The user chose **A** (single
source of truth; connectors self-contained). Post-decision verification **de-risked A's headline objection**:
the Caffeine split-brain is avoidable (parent-first `org.apache.doris.connector.*` + Caffeine-encapsulated
`MetaCacheEntry`), so A is viable. The residual, accepted trade-off is the **"zero third-party" charter break**
on `fe-connector-api` (Caffeine compile dep) plus the ~18-file mechanical blast radius. The §5 plan below is
for **Option A**.

---

## 5. Proposed migration (Option A) — phased

> Ordering: move the framework first (no behavior change), then port each connector cache onto it, then
> collapse the duplicates and dead code. Each phase is independently buildable/testable and a separate commit.
> Guiding invariant: **connectors touch only the Caffeine-free `MetaCacheEntry` API; `CacheFactory` and any
> Caffeine-typed API stay framework-internal.**

**P1 — relocate the generic framework to `org.apache.doris.connector.api.cache` (no behavior change).**
Move `CacheFactory`, `MetaCacheEntry`, `MetaCacheEntryDef`, `MetaCacheEntryStats`,
`MetaCacheEntryInvalidation` (drop the `NameMapping`-coupled `forNameMapping` factory, or move `NameMapping`
too — §6 Q3), `CatalogEntryGroup`, `ExternalMetaCacheRegistry`, and the single `CacheSpec`. Change
`MetaCacheEntry` to take the 2 knobs as ctor params (`refreshAfterWriteSeconds`,
`manualMissLoadEnabled`) instead of reading `Config`. Add Caffeine as a `fe-connector-api` compile dep. Update
the ~16 fe-core importers + the 2 `CacheFactory` callers (`ExternalRowCountCache`, `FileSystemCache`) to the
new package; fe-core call sites pass the `Config`-derived knob values (so fe-core behavior is byte-identical).
Keep `AbstractExternalMetaCache` (Env/catalog glue) + `ExternalMetaCacheRouteResolver` + `Mgr` + concrete
`*ExternalMetaCache` in fe-core, now importing the moved core. Collapse the connector-api `CacheSpec` mirror
into this one shared copy (validators throw `IllegalArgumentException`; fe-core's dead `checkLongProperty`
callers need no change — §6 Q4). **Gate:** `check-connector-imports.sh` green (moved classes are under the
whitelisted `connector.` prefix); full fe-core + connector build green; all existing cache tests unchanged.

**P2 — connector cache-construction helper.** Give connectors a small, Caffeine-free way to build a
per-catalog cache without the Env-coupled `AbstractExternalMetaCache`: either a thin
`MetaCacheEntry` builder taking `(name, CacheSpec, refreshExecutor, knobs)`, or expose a
`ConnectorContext.getSharedRefreshExecutor()` so the plugin reuses fe-core's `commonRefreshExecutor` (avoids a
per-connector thread pool). Decide executor ownership (§6 Q5). Connectors keep OWNING the cache instance
(field on `IcebergConnector`/`PaimonConnector`, dropped on rebuild/REFRESH).

**P3 — port the latest-snapshot caches.** Replace `IcebergLatestSnapshotCache` /`PaimonLatestSnapshotCache`
internals with a `MetaCacheEntry` (access-TTL from `meta.cache.<engine>.table.ttl-second`, capacity 1000),
keeping their key/value types (`TableIdentifier`/`Identifier` → `CachedSnapshot`/`long`). Decide the
`IcebergPartitionInfo` fidelity gap (§6 Q6). `Connector.invalidateTable/invalidateAll` → `entry.invalidateKey/
invalidateAll`.

**P4 — port the manifest cache.** Replace `IcebergManifestCache` with a `contextualOnly` `MetaCacheEntry`
(`CacheSpec.of(false, CACHE_NO_TTL, 100_000)`, `get(key, missLoader)`); unify `ManifestCacheValue` +
`IcebergManifestEntryKey` into one shared copy (already byte-identical ports — move to a connector-visible
package both sides use). Preserve "REFRESH TABLE keeps the manifest cache" (invalidateTable skips it), and
decide the `ManifestFiles.dropCache(io)` fidelity gap (§6 Q6).

**P5 — collapse duplicates + dead code.** Delete the connector `ConcurrentHashMap` cache classes; optionally
prune the now-dead native-iceberg `IcebergExternalMetaCache` entries (kept only for iceberg-on-HMS until hms
flips — §6 Q7).

**Testing.** Unit: `MetaCacheEntry` behavior after the Config-knob extraction (TTL/capacity/invalidate/
miss-load) in its new home; the ported connector caches (TTL expiry via injectable clock as today). Regression:
`test_iceberg_table_meta_cache` / `test_paimon_table_meta_cache` (stale-vs-refresh row counts) and
`IcebergScanPlanProviderTest` (manifest path) stay green. **Classloader:** an explicit test/redeploy check that
a plugin-built `MetaCacheEntry` runs against app-loader Caffeine and never receives a `CacheFactory`/Caffeine
object across the boundary (guards the one failure mode that makes A unsafe — cf. MEMORY tccl-gotcha).

---

## 6. Remaining decisions (Option A) for the user
The ownership fork (Q1) and Caffeine-on-classpath (Q2) are **settled by choosing A**. These remain:

1. **Charter break — confirm.** Adding Caffeine as a `fe-connector-api` compile dep breaks its stated
   "zero third-party dependencies" charter. Verified split-brain-safe (parent-first + encapsulation), but it is
   a real policy change to the SPI module. Accept? (Alternative to soften it: put the framework +
   Caffeine in a *new* module `fe-connector-cache` under `org.apache.doris.connector.*` — still parent-first,
   keeps `-api`/`-spi` pure. **Recommended.**)
2. **Target module** — `fe-connector-api` (existing, but breaks its charter) vs a **new `fe-connector-cache`**
   module (keeps `-api`/`-spi` dependency-free; both are parent-first via the `connector.` prefix). Recommend
   the new module.
3. **`MetaCacheEntryInvalidation.forNameMapping`** — it couples to `datasource.NameMapping`. Drop that factory
   (connectors don't need it; fe-core keeps its own), or move `NameMapping` to a connector-visible package too?
4. **`CacheSpec` collapse** — one shared copy in the new home; validators throw `IllegalArgumentException`
   (fe-core's only `checkLongProperty` callers are dead). Confirm this is the surviving behavior.
5. **Refresh executor ownership** — connectors reuse fe-core's `commonRefreshExecutor` via a new
   `ConnectorContext.getSharedRefreshExecutor()` (no extra plugin thread pool) vs each connector owns one.
   Recommend sharing fe-core's.
6. **Behavior/fidelity parity** — porting to `MetaCacheEntry` *upgrades* connector caches from
   clear-on-overflow to real Caffeine eviction (+ optional `refreshAfterWrite`/stats). Adopt the richer
   behavior, or configure to match today exactly (no refresh, no stats)? Also: re-add
   `ManifestFiles.dropCache(io)` on catalog invalidation and `IcebergPartitionInfo` in the snapshot value, or
   keep the connector's current simplifications?
7. **Dead-code + paimon scope** — prune the now-dead native-iceberg `IcebergExternalMetaCache` entries now (or
   keep while iceberg-on-HMS stays legacy)? And is paimon in scope this round (it needs a *new* snapshot entry,
   not a wrap of an existing one), or iceberg-first?

---

## 7. Risks / open questions
- **The one failure mode that makes A unsafe:** a Caffeine-typed object (`LoadingCache` from `CacheFactory`,
  or a raw `Cache`) crossing to connector (child-first) code → ClassCast. Mitigation: keep `CacheFactory`
  package-private / framework-internal; connectors use only the Caffeine-free `MetaCacheEntry` API; add a
  redeploy classloader smoke test (MEMORY `catalog-spi-plugin-tccl-classloader-gotcha`).
- **Caffeine version skew** — iceberg's plugin compiles against caffeine **2.9.3** (vendored `DeleteFileIndex`,
  child-first, `org.apache.iceberg.*`); the framework uses app-loader **3.2.3**. Safe *because* they never
  share objects — but confirm no plugin code passes a 2.9.3 cache into a framework call.
- **iceberg-on-HMS keeps `IcebergExternalMetaCache` LIVE** — its native-iceberg entries are dead but the class
  can't be deleted until hms flips; the moved framework must keep serving it unchanged.
- **`SchemaCacheValue` → `catalog.Column`** — the schema-value concern stays in fe-core's
  `AbstractExternalMetaCache`; do not move `getSchemaValue`/`wrapSchemaValidator` (would drag `Column`
  connector-visible).
- **Hive invalidation asymmetry** — Hive uses `none()` + manual partition-granular `invalidate*`; the move must
  not assume uniform predicate invalidation (don't regress hive).
- **Load-bearing, re-verify before coding:** the exact `getOrLoad` loader bodies in
  `Iceberg/PaimonConnectorMetadata.beginQuerySnapshot`; and whether metadata/sys-table queries against a
  PluginDriven iceberg catalog secretly depend on the dead `IcebergExternalMetaCache` path.

---

## 8. Implementation progress (2026-07-01)

### P1 — DONE so far: module skeleton + build wiring (verified)
- Created `fe/fe-connector/fe-connector-cache/` — `pom.xml` (parent `fe-connector`; **Caffeine 3.2.3
  `provided`** so it compiles against the app-loader copy and bundles nothing → no split-brain, no version
  match to babysit; junit test), plus `src/main/java/org/apache/doris/connector/cache/package-info.java`.
- Registered `<module>fe-connector-cache</module>` in `fe/fe-connector/pom.xml` (after `-spi`).
- Added the `fe-connector-cache` dependency to `fe/fe-core/pom.xml`.
- **Verified:** `mvn -pl fe-connector/fe-connector-cache install` → BUILD SUCCESS; gate confirms the module
  adds zero forbidden imports. (Dropped a `org.jetbrains:annotations` dep — unmanaged version; re-add with a
  pinned version when `CacheFactory`/`MetaCacheEntry` land, which use `@NotNull`/`@Nullable`.)

### ⚠️ Gate FALSE POSITIVE exposed (pre-existing, NOT this task; NOT a real violation) — user-confirmed 2026-07-01
Adding the new module invalidated the maven-build-cache entry for the `fe-connector` aggregator, which
**re-ran the `check-connector-imports` gate and it exits 1** — the Phase-1/2 builds had only passed because
the gate result was cached. The **sole** flag is a **FALSE POSITIVE**, not a rule violation:
`fe-connector-hms/.../org/apache/hadoop/hive/metastore/HiveMetaStoreClient.java:21-22` imports
`org.apache.doris.datasource.hive.HiveVersionUtil{,.HiveVersion}`, but that resolves to a **verbatim copy
vendored INSIDE fe-connector-hms** (`fe-connector-hms/.../org/apache/doris/datasource/hive/HiveVersionUtil.java`
— same package name as fe-core's, but a self-contained file: imports only guava `Strings` + log4j). The
patched client and the vendored util are in the **same module**; `fe-connector-hms` has **zero** fe-core
dependency, so the import resolves locally — the connector does **not** depend on fe-core. The gate is a naive
package-prefix grep (`org\.apache\.doris\.(catalog|common|datasource|…)`, `check-connector-imports.sh:48-54`)
that can't tell a same-module vendored `datasource.*` class from fe-core's, hence the false positive.
**Do NOT re-architect / re-expose `HiveVersionUtil`** — the vendored copy IS the connector-visible mechanism,
nothing is broken. It only matters as a build nuisance: the exit-1 fails a **cache-clean** `fe-connector`
reactor build (and would fail CI). Workaround: `-Dexec.skip=true` steps past the gate exec (`-pl <module>`
without `-am` does NOT work for the leaf connectors — they hit `${revision}`). If ever worth silencing for
real, refine the gate to skip same-module vendored classes — a tooling tweak, not a code change.

### P1① — DONE (2026-07-01): `CacheSpec` as an INDEPENDENT copy (per the revised copy strategy)
The CacheSpec step landed as a **copy**, not a move (see the revised-decision callout at the top of this doc):
- **fe-core reverted to HEAD.** The prior in-tree "move" WIP (had deleted `datasource.metacache.CacheSpec` +
  `CacheSpecTest`, repointed ~10 fe-core importers to `connector.cache.CacheSpec`) was `git checkout`-reverted.
  fe-core's `datasource.metacache.CacheSpec` and every importer are now **byte-identical to HEAD** (0 fe-core
  source edits). Verified: no fe-core source imports `org.apache.doris.connector.cache`.
- **`fe-connector-cache` owns an independent `CacheSpec`** (+ `CacheSpecTest`, 14 tests) under
  `org.apache.doris.connector.cache`; validators throw `IllegalArgumentException`.
  `mvn -pl fe-connector/fe-connector-cache install` → BUILD SUCCESS, **14/14** green.
- **Connectors repointed** `connector.api.cache.CacheSpec` → `connector.cache.CacheSpec`
  (`IcebergConnectorProvider`, `IcebergCatalogFactory`, `IcebergScanPlanProvider`, `PaimonConnectorProvider`);
  both connector poms add the `fe-connector-cache` dependency; the old `connector.api.cache.CacheSpec` (+ test)
  is deleted. The new CacheSpec public API is **byte-identical** to the deleted connector-api copy (verified by
  signature diff), so the repoint compiles by construction.
- **`fe-core → fe-connector-cache` pom dependency REMOVED.** The skeleton commit (`f6063a0c87c`) had added it
  for the abandoned move; under copy fe-core imports nothing from `connector.cache`, so it is deleted to keep
  fe-core decoupled. `fe-connector-cache` is **NOT** excluded from any `plugin-zip.xml` → it **bundles
  per-plugin** (child-loaded). Safe for `CacheSpec`: it is a JDK-only value/validation class that never crosses
  the fe-core↔connector boundary as an object (only its `IllegalArgumentException`, a JDK type, crosses).
- **Import gate:** `check-connector-imports.sh` reports only the pre-existing HMS `HiveVersionUtil` violation
  (`4acb5f91e1a`, unrelated); my files add zero forbidden imports.
- **Connector build:** `-pl fe-connector/fe-connector-iceberg,fe-connector-paimon -am package -DskipTests
  -Dexec.skip=true` (paimon needs `package` for HiveConf; `exec.skip` steps past the pre-existing HMS gate) →
  **BUILD SUCCESS** — iceberg + paimon (+ `fe-connector-cache`) all compile/package against the repoint.

### P1② — DONE (2026-07-01): framework core (`MetaCacheEntry`/`CacheFactory`/`MetaCacheEntryStats`) as an INDEPENDENT copy
Copied the **3 classes a connector actually needs to own+use a cache** into `org.apache.doris.connector.cache`;
fe-core's `datasource.metacache` originals are **untouched** (0 fe-core edits this step).
- **`MetaCacheEntryStats`** — byte-identical copy (pure JDK data holder), package change only.
- **`CacheFactory`** — faithful copy; only change vs fe-core = package + **dropped the cosmetic `@NotNull`**
  (no jetbrains-annotations dep on this module, so `org.jetbrains:annotations` was NOT added — simpler than the
  earlier plan). Framework-internal: its public methods return Caffeine types, must not cross to connectors.
- **`MetaCacheEntry`** — faithful copy; the **two fe-core `Config` static reads are now ctor-injected**:
  `refreshAfterWriteSeconds` (already in seconds — fe-core's `Config.*_minutes * 60` is done at the call site)
  and `manualMissLoadEnabled`. `@Nullable` dropped (no jsr305). Added a 4-arg convenience ctor for the common
  connector case (autoRefresh=false, contextualOnly=false, no-refresh, no-manual) + the full 8-arg ctor. Public
  API is **Caffeine-free** → safe for connectors (child-first) to hold.
- **Scope narrowed vs the original plan (Rule 2):** `MetaCacheEntry` does **not** reference `MetaCacheEntryDef`,
  `MetaCacheEntryInvalidation`, `CatalogEntryGroup`, or `ExternalMetaCacheRegistry` — those are the
  Env-coupled **registry/declaration** machinery (`AbstractExternalMetaCache` territory). A connector owns a
  single `MetaCacheEntry` instance directly (not via the Env registry), so **none of them are copied**. Copy
  them later only if a concrete connector cache turns out to need them (P3/P4).
- **Tests:** new `MetaCacheEntryTest` (JUnit 5; the fe-core test's `Config` toggle becomes a ctor arg) covers
  loader get/hit-miss/lastError, disabled short-circuit + manual-path reload-every-time, capacity eviction
  (reflective `cleanUp()` → `evictionCount`), contextual-only reject-default-get, key/predicate/all
  invalidation, manual-miss-load concurrent dedup. `mvn -pl fe-connector/fe-connector-cache install` →
  **BUILD SUCCESS**, MetaCacheEntry **6/6** + CacheSpec **14/14**, checkstyle 0 violations.
- ~~plugin-zip exclusions~~ still **DROPPED** (copy bundles per-plugin, Caffeine `provided` → uses the plugin's
  bundled Caffeine); fe-core still does not depend on this module.

### P1 — remaining / next
The framework core is now copied and independently usable. Next is **P2–P5 (port the three hand-rolled
connector caches onto the copied `MetaCacheEntry`)** per §5:
`IcebergLatestSnapshotCache` / `PaimonLatestSnapshotCache` → a loader-backed `MetaCacheEntry`
(access-TTL, cap 1000); `IcebergManifestCache` → a `contextualOnly` `MetaCacheEntry`
(`CacheSpec.of(false, CACHE_NO_TTL, 100_000)`, `get(key, missLoader)`). Then delete the `ConcurrentHashMap`
cache classes.
- **Load-bearing but not yet re-verified** (confirm before porting): exact `getOrLoad` loader bodies in
  `Iceberg/PaimonConnectorMetadata.beginQuerySnapshot`; the connectors' current
  `invalidateTable`/`invalidateAll` wiring; whether any connector cache needs predicate invalidation (would
  need `MetaCacheEntry.invalidateIf`, already present) or a shared vs per-connector refresh `ExecutorService`.
