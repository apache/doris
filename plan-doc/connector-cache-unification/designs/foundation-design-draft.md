Verification confirms the load-bearing recon claims: `ConnectorStatementScope.computeIfAbsent` lives in fe-connector-api (not fe-core); `ConnectorPartitionViewCache[V]` + `PartitionViewCacheKey(db,table,snapshotId,schemaId)` already exist with the exact contextual-only MetaCacheEntry octet; the "NO consumers yet" javadoc is confirmed stale (iceberg, hive, paimon all wire it — IcebergConnector.java:281,284, HiveConnector.java:134, PaimonConnector.java:158); and the gate hardcodes `TARGET_REL` to IcebergConnector.java with the `FIELD_DECL` + two-marker logic. Design follows.

---

## Summary

The owner-signed plan is buildable with a strikingly small footprint because **most of the foundation already exists** and the recon overturns one premise of the mandate:

- **(A) Generic cache wrapper** is a near-trivial extension of an already-shipped, already-consumed class. `ConnectorPartitionViewCache[V]` is byte-for-byte the octet that iceberg's five cross-query caches hand-roll; the five differ only in value type (all can be opaque `[V]`), the MVCC axis (already covered by the 4-tuple key), and the property-namespace entry name (the one axis the current class hardcodes). Generalizing = promote it to `ConnectorMetadataCache[V]` with an `entryName` ctor arg + a neutral key name. Iceberg retrofits with zero behavior/cost change; hudi/mc/es instantiate typed copies.

- **(B) The D4 seam does NOT need to grow fe-core.** The per-statement memo arena (`ConnectorStatementScope.computeIfAbsent`, `ConnectorSession.getStatementScope()`, backed by `ConnectorStatementScopeImpl` on `StatementContext`) is **already complete and already connector-agnostic, and lives in fe-connector-api** — which iceberg and hudi depend on, and fe-core does **not**. Iceberg's only connector-private piece is a 9-line key-convention wrapper (`IcebergStatementScope.sharedTable`). The minimal D4 seam is a single static convenience helper in fe-connector-api that standardizes that key convention. This means **iron rule A is not actually crossed** — the sanctioned fe-core exception can be declined. This is the #1 residual owner decision.

- **(C) The gate generalization** is a discovery change, not a new mechanism: replace the hardcoded `TARGET_REL` with a two-stage scan (enumerate `*Connector.java`, keep only files whose `getCapabilities()` *declares* `SUPPORTS_USER_SESSION` via `.add(...)`/`EnumSet.of(...)`, excluding `.contains(...)` guards and comments), then run the existing field+marker logic verbatim on each. Today it resolves to exactly `{iceberg}`, so behavior is unchanged and immediately testable.

- **(D) Consumption**: hudi (flagship) consumes both B (metaClient + schema memo per statement — the ~6x/~4x redundancy) and A (a `(table, instant)` cross-query partition cache); maxcompute consumes B (collapse the 14–17-site `getTableHandle` fan-out, killing the repeated remote `exists()` probe) — it already has a cross-query partition cache; es consumes B only (per-scan hoist of `EsMetadataState`, split mapping from shard routing), and must **not** get a cross-query cache (iron rule E).

- **(E) Sequencing** is foundation-first: cache wrapper PR → api seam PR (+ iceberg retrofit, proven by existing tests) → flagship hudi PR (with e2e) → small mc PR → small es PR → gate PR (orthogonal, land any time).

The design deliberately targets **only the two real cross-query consumers (iceberg + hudi)** plus the two per-statement-only consumers (mc + es). It does not add a `[K]` type parameter, an identity-sharding dimension, or a credential abstraction that no D1 connector needs (Rule 2).

---

## A. Generic cache wrapper (fe-connector-cache)

### A.1 What is being generalized (grounded)

The five iceberg cross-query caches (`IcebergTableCache`, `IcebergLatestSnapshotCache`, `IcebergPartitionCache`, `IcebergFormatCache`, `IcebergCommentCache`) each construct the identical contextual-only entry — `new MetaCacheEntry[...]("iceberg-<x>", null, spec, ForkJoinPool.commonPool(), false, true, 0L, true)` — expose `getOrLoad(key, Supplier)` = `entry.get(key, ignored -> loader.get())`, `isEnabled()` = `entry.stats().isEffectiveEnabled()`, and `invalidate`/`invalidateDb`/`invalidateAll` via `invalidateKey`/`invalidateIf`/`invalidateAll`. They differ only in **key type**, **value type**, and the **db-match predicate**. `ConnectorPartitionViewCache[V]` (fe-connector-cache) already captures this exact shape for one case, keyed by `PartitionViewCacheKey(db, table, snapshotId, schemaId)` with `matches`/`matchesDb` predicates on the key — and is already a live consumer in iceberg/hive/paimon. It hardcodes exactly two axes: `ENTRY_PARTITION_VIEW = "partition_view"` (the property namespace + entry name) and the key class.

The manifest cache is explicitly **excluded** — it is path-keyed, no-TTL, default-off, iceberg-`DataFile`-typed, authz-exempt (read only after a per-user `resolveTable`), and carries a per-scan `ScanStats` stash. It is not a cross-query generalization target.

### A.2 Generic class shape

**Layer**: fe-connector-cache, package `org.apache.doris.connector.cache` (the connector-side copy — JDK+Caffeine only, zero fe-core deps; NOT fe-core's `datasource/metacache`).

Promote `ConnectorPartitionViewCache[V]` to a general `ConnectorMetadataCache[V]`:

```
public final class ConnectorMetadataCache[V] {
    private final MetaCacheEntry[ConnectorTableKey, V] entry;

    // Framework-native ctor: per-entry namespace meta.cache.<engine>.<entryName>.(enable|ttl-second|capacity)
    public ConnectorMetadataCache(String engine, String entryName, Map[String,String] props) {
        CacheSpec spec = CacheSpec.fromProperties(
                props == null ? Map.of() : props, engine, entryName, CacheSpec.of(true, 86400L, 1000L));
        this.entry = new MetaCacheEntry[](engine + "." + entryName, null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    // Pre-resolved ctor: lets a connector share ONE catalog-level knob across N caches (iceberg back-compat)
    public ConnectorMetadataCache(String name, CacheSpec spec) {
        this.entry = new MetaCacheEntry[](name, null, spec,
                ForkJoinPool.commonPool(), false, true, 0L, true);
    }

    public boolean isEnabled() { return entry.stats().isEffectiveEnabled(); }
    public V get(ConnectorTableKey key, Supplier[V] loader) { return entry.get(key, ignored -> loader.get()); }
    public void invalidateTable(String db, String table) { entry.invalidateIf(k -> k.matches(db, table)); }
    public void invalidateDb(String db) { entry.invalidateIf(k -> k.matchesDb(db)); }
    public void invalidateAll() { entry.invalidateAll(); }
}
```

- **Key**: rename `PartitionViewCacheKey` → **`ConnectorTableKey(db, table, snapshotId, schemaId)`** (the class body is already generic; only its name is partition-flavored). Unused axes pass `-1`. Every one of the five iceberg caches maps onto it:
  - table-handle / comment / latest-snapshot: `(db, table, -1, -1)`
  - file-format: `(db, table, snapshotId, -1)`
  - partition (raw): `(db, table, snapshotId, -1)`
  - partition-view (existing): `(db, table, snapshotId, schemaId)`
  
  This is one key type for all six. Iceberg's caches currently key by `TableIdentifier` / composite `(TableIdentifier, snapshotId)`; retrofit swaps them to `ConnectorTableKey`, functionally identical (a `TableIdentifier` is `db.table`).

- **Value**: stays **opaque `[V]`**. This is load-bearing: `comment`/`file-format` are `String`, but `table-handle` is a raw `org.apache.iceberg.Table` (credentialed), `latest-snapshot` is a 2-long `CachedSnapshot`, `partition` is `List[IcebergRawPartition]`. A wrapper that owned a concrete value type would be useless to any second connector. Opaque `[V]` serves all.

- **Config — expose BOTH styles**: the framework-native per-entry ctor (`meta.cache.<engine>.<entry>.*`, recommended for hudi/mc/es) **and** the pre-resolved `CacheSpec` ctor so iceberg keeps its single shared knob `meta.cache.iceberg.table.ttl-second` across all five caches (operator back-compat). This also retires the `ttl<=0 -> CACHE_TTL_DISABLE_CACHE` mapping that is copy-pasted five times today (it moves into `CacheSpec.of`/`fromProperties`, already there).

- Keep the existing `ConnectorPartitionViewCache[V]` as a 3-line thin subclass/factory that calls the new ctor with `entryName="partition_view"`, so its three current consumers (iceberg/hive/paimon) are untouched byte-for-byte. **Fix the stale "NO consumers yet" javadoc** (confirmed false at HEAD).

### A.3 How iceberg is retrofitted without behavior/cost change

Delete the five hand-rolled cache classes; replace each field with a typed `ConnectorMetadataCache[V]` built from the **same pre-resolved `CacheSpec`** iceberg already derives from `resolveTableCacheTtlSecond` (default 86400, capacity 1000). Because the octet, the loader wiring, the `isEffectiveEnabled` gate, and the invalidation semantics are identical, cache hits/misses/eviction are unchanged; existing iceberg cache tests (`*ForTest` accessors, `loadCountForTest`) prove invariance.

**Iron rule D at the fault line**: the wrapper stays value-opaque and **knows nothing about credentials**. The credential policy stays exactly where it is today — in the *connector*, which nulls the field under its gate:
- table-handle: null when `isUserSessionEnabled() || restVendedCredentialsEnabled(props)` (stricter, because the value carries FileIO credentials).
- comment: null unless `restVendedCredentialsEnabled && !isUserSessionEnabled()`.
- format/partition/latest-snapshot: null under `isUserSessionEnabled()`.

A null field ⇒ `get()` never called ⇒ live bypass (exactly today's behavior). The wrapper never needs a credential flag; the gate script (§C) enforces the marker on the field declaration.

- **Layer**: fe-connector-cache (connector-side, child-loaded). **Iron-rule impact**: none on A — fe-core source does not grow; the new memo is connector-side (rule A honored); the class holds no property parsing beyond `CacheSpec.fromProperties`, which already lives here (rule C honored).
- **Trino alignment**: matches Trino's "shared low-level toolkit (`io.trino.cache`), per-connector caches, NO single unified cache." **Divergence**: we do not port Trino's per-identity `LoadingCache[user, ...]` sharding — Doris keeps the binary enable/disable (null field) cut, which is correct for D1 (no connector needs identity keying).
- **Consumers**: iceberg (retrofit, 5 caches), hudi (`(table,instant)` partition cache), and — if they ever add a cross-query cache — mc/es. Not needed by es this round (rule E).

---

## B. Minimal fe-core statement-scope seam

### B.1 The premise correction

D4 was framed as extracting a **fe-core** seam that crosses iron rule A. Recon (unanimous across agents, verified above) shows the substrate is **already built and already in fe-connector-api**:

- `ConnectorStatementScope.computeIfAbsent(String key, Supplier[T])` — the whole memo primitive, opaque `Object` values under string keys; `NONE` runs the loader every call (fe-connector-api).
- `ConnectorSession.getStatementScope()` — neutral access point, defaults to `NONE` (fe-connector-api).
- `ConnectorStatementScopeImpl` (ConcurrentHashMap, two-pass idempotent `closeAll`) hung on `StatementContext` with per-execution reset — **already in fe-core, needs zero change**.
- Iceberg's only connector-private code is `IcebergStatementScope.sharedTable(session, db, table, Supplier[Table])`: null-session ⇒ load; else key = `"iceberg.table:" + catalogId + ":" + db + ":" + table + ":" + queryId` ⇒ `computeIfAbsent`.
- Iceberg and hudi both depend on **fe-connector-api**, and on **neither fe-core nor fe-connector-cache** (a literal fe-core class would be unreachable by the retrofit; fe-connector-cache has no dep on ConnectorSession).

### B.2 The seam: one static helper in fe-connector-api

**Layer**: fe-connector-api, beside `ConnectorStatementScope` (connector-side SPI module).

```
public final class ConnectorStatementScopes {
    private ConnectorStatementScopes() {}

    /** Resolve db.table once per statement and share the one object across every resolver.
     *  keyNamespace namespaces the value TYPE so a heterogeneous gateway statement touching
     *  two connectors cannot collide on the same (db,table) and hit a ClassCastException. */
    public static [T] T resolveInStatement(ConnectorSession session, String keyNamespace,
                                           String db, String table, Supplier[T] loader) {
        if (session == null) {
            return loader.get();                       // == ConnectorStatementScope.NONE
        }
        String key = keyNamespace + ":" + session.getCatalogId() + ":" + db + ":" + table
                + ":" + session.getQueryId();
        return session.getStatementScope().computeIfAbsent(key, loader);
    }
}
```

This adds **no new interface, no new type on the SPI, no fe-core source** — it reuses the existing `computeIfAbsent` primitive and standardizes the key convention iceberg hand-rolls. `keyNamespace` is a **required** parameter (not a hardcoded prefix): it both reproduces iceberg's exact existing key and prevents cross-connector value-type collisions in a gateway statement.

### B.3 Retrofit + consumption

- **Iceberg (retrofit)**: `IcebergStatementScope.sharedTable` collapses to `return ConnectorStatementScopes.resolveInStatement(session, "iceberg.table", db, table, loader);` — byte-identical key (namespace `"iceberg.table"` reproduces the `"iceberg.table:"` prefix), so the 4 funnel sites (`resolveTableForRead:646`, scan `:2360`, write `:704`, txn `:212`) keep identical hits/misses and `NONE` fallthrough. `rewritableDeleteSupply` stays iceberg-private (it is a `(catalogId, queryId)`-keyed scan→write delete bridge, not a table resolution, and is **out of the seam**).
- **Hudi (consumes)**: wrap the metaClient build — `HoodieTableMetaClient mc = ConnectorStatementScopes.resolveInStatement(session, "hudi.metaclient", db, table, () -> buildMetaClient(...));` — collapsing the ~6 builds/query to 1. Hudi has no per-statement memo today (two independent `HoodieTableMetaClient.builder()` sites), so this is a clean new consumer. Derived state (`TableSchemaResolver`/Avro/`InternalSchema`) is then computed once off the single memoized metaClient, killing the ~4x re-parse. Hudi needs **no new module dependency** for the seam (already depends on fe-connector-api).

### B.4 The sanctioned fe-core exception (only if the owner insists / for the mc fan-out)

The maxcompute redundancy is a *fe-core artifact*: `PluginDrivenExternalTable.resolveConnectorTableHandle` (fe-core:116) calls `metadata.getTableHandle` fresh at 14–17 sites. Two placements collapse it:

- **B1 (recommended, fe-core flat)**: memoize *inside* the connector's `getTableHandle` via `resolveInStatement(session, "maxcompute.handle", db, table, ...)`. All 14–17 fe-core calls then hit the connector's per-statement memo; the remote `exists()`+`get` fires once. fe-core untouched.
- **B2 (sanctioned D4 exception, generic)**: wrap the one fe-core call site in the *existing* primitive — `session.getStatementScope().computeIfAbsent("tablehandle:" + catalogId + ":" + db + ":" + remoteName + ":" + queryId, () -> metadata.getTableHandle(...))`. This is the **smallest possible reading** of the sanctioned exception: **no new fe-core type, no new SPI method** — just a memoized call site reusing `computeIfAbsent`, benefiting every connector (including non-migrated ones) uniformly. fe-core source grows by ~3 lines in an existing method.

**Recommendation**: use **B1** for the two named seam consumers (iceberg + hudi resolve raw `Table`/metaClient objects connector-side). Use **B2** for maxcompute if the owner wants the generic fan-out collapsed without touching mc's `getTableHandle` — mc is the connector that most justifies the sanctioned exception, and even then it costs zero new SPI surface.

- **Iron-rule impact**: B1 = none (rule A intact). B2 = the sanctioned minimal exception, ~3 lines, reusing an existing primitive.
- **Trino alignment**: this recreates, at *statement* granularity, Trino's per-*transaction* "load each table once / read+write share one metadata." **Divergence**: Doris's unit is the statement (scope keyed by queryId, reset per prepared EXECUTE), so nothing may assume cross-statement reuse.
- **Rule D safety**: the L1 memo is single-identity by construction — a statement resolves one catalog under one identity (`PluginDrivenMetadata` fail-loud pin), scope is per-statement + GC'd at end. So the memo never crosses users even when L2 (the §A cross-query cache) is disabled for a vended catalog. **Lifecycle caveat for hudi**: `ConnectorStatementScopeImpl.closeAll` closes any stored `AutoCloseable` at statement end; if a hudi version's `HoodieTableMetaClient` (or a wrapper) is `AutoCloseable`/holds a `HoodieStorage`/`FileSystem`, verify auto-close-at-statement-end is desired, else memoize a non-closeable projection.
- **Consumers**: iceberg (retrofit), hudi (metaClient), maxcompute (handle, B1 or B2), es (metadata state, §D).

---

## C. Authz gate generalization

**Layer**: `tools/check-authz-cache-sharding.sh` + its self-test — pure tooling, touches no product source (rule A trivially honored). Wired at fe-connector reactor `validate` phase with `${project.basedir}` as root (already reaches all connector modules; no pom change).

### C.1 The change: discovery replaces the hardcoded target

Replace `TARGET_REL='fe-connector-iceberg/.../IcebergConnector.java'` with a three-stage scan:

- **Stage 1 — enumerate**: `find "${ROOT}" -path '*/src/main/java/*' -name '*Connector.java'` (excluding `target/`).
- **Stage 2 — filter to declarers**: keep a file only if it **declares** the capability, matching the declaration form and excluding references/comments:
  - INCLUDE lines matching `\.add\([^)]*ConnectorCapability\.SUPPORTS_USER_SESSION` OR `(EnumSet|Set|ImmutableSet)\.of\([^)]*SUPPORTS_USER_SESSION`
  - after stripping comment lines (`^\s*\*`, `^\s*//`) and **excluding** any line containing `.contains(`.
  
  This is the single load-bearing distinction: it admits `IcebergConnector.java:860` (`capabilities.add(...SUPPORTS_USER_SESSION)`) and rejects `HiveConnector.java:543` (`sibling.getCapabilities().contains(...SUPPORTS_USER_SESSION)` — a fail-loud guard, not a declaration) and the fe-connector-api javadoc mentions. A static grep does **not** try to evaluate iceberg's `if (isUserSessionEnabled())` runtime guard — any source-level `.add` means the connector *can* carry the capability, so its caches are in scope unconditionally (correct conservative altitude).
- **Stage 3 — classify verbatim**: run the **existing** `FIELD_DECL='final ([A-Za-z_][A-Za-z0-9_]*)?Cache[ <]'` + two-marker (`authz-cache-session-user-disabled` / `authz-cache-exempt`, on-line-or-line-above) logic on each declaring file. No change to marker vocabulary or RED/GREEN semantics — iceberg keeps passing unchanged, and the existing self-test stays valid.

### C.2 Invariants and guards

- **Scan the OWNER file only** (`*Connector.java`), never the whole module: the constructor-injected reference fields in `*ConnectorMetadata`/`*ScanPlanProvider` are unmarked *by design*; a whole-module scan would false-positive on all of them. State this as an explicit invariant.
- **Anti-no-op floor**: if Stage 2 yields **zero** declarers, `exit` non-zero (mirroring today's `exit 2` for missing target). Without this, a capability rename or grep drift silently turns the gate into pass-everything — the highest-risk failure of any discovery gate.
- **Self-test additions**: (a) a fixture connector with a `.contains(SUPPORTS_USER_SESSION)` guard + comment + an unmarked cache field MUST NOT be scanned (proves REFERENCE excluded); (b) a second fixture that DECLARES via `.add`/`EnumSet.of` with an unmarked cache MUST fail (proves multi-connector discovery). Keep the iceberg fixture as the primary declarer.

### C.3 Scope reality

Today only iceberg declares the capability; hudi/mc/es have no `getCapabilities` override and vend static (not per-user) credentials, so their new caches are authz-safe **without** the gate and correctly fall outside it (rule D). D3 is therefore **forward-insurance**: it changes behavior for exactly one connector now (no-op vs iceberg) but auto-enrolls any future session=user adopter before an unguarded cross-query cache can leak. It is **orthogonal** to D1/D2 and does not block them. Documented residual: whether the hive-gateway path (a sibling could declare the capability, guarded at `HiveConnector.java:543`) is in-gate-scope or explicitly out (the front door never declares it).

Note: the fe-core defense layer (`PluginDrivenExternalCatalog.shouldBypass{TableName,DbName,Schema}Cache`, keyed off `getCapabilities().contains(SUPPORTS_USER_SESSION)` at runtime) is **already** connector-agnostic and needs no change — it auto-covers any future declarer. The gate only closes the connector-side hole where a new `*Cache` field is added and forgotten.

---

## D. Connector consumption (hudi / maxcompute / es)

### D.1 Hudi (flagship) — consumes B (primary) + A (secondary)

- **metaClient + schema memo (B)**: route both `HoodieTableMetaClient.builder()` sites through `resolveInStatement(session, "hudi.metaclient", db, table, () -> buildMetaClient(...))`. Collapses ~6 builds → 1/statement and, since `TableSchemaResolver`/Avro/`InternalSchema` are derived off the single memoized client, ~4 schema re-parses → 1. This is the flagship win, and the **per-statement** tier is the correct one: `buildMetaClient` reloads the active timeline for freshness, so a cross-query cache of the raw metaClient would be wrong; the per-statement memo is exactly right.
- **Wrap `CachingHmsClient`**: hudi is HMS-backed (depends on fe-connector-hms/metastore-hms). Its HMS access should sit behind the shared `CachingHmsClient` (Trino `CachingHiveMetastore` shape, coarse REFRESH+TTL, TCCL-pinned per the memory notes on `ThriftHmsClient.doAs`) exactly as iceberg-on-HMS and hive do — so metastore round-trips (`getTable`, listing) are shared cross-query at the HMS layer, beneath the per-statement metaClient memo.
- **`(table, instant)` partition cache (A)**: a `ConnectorMetadataCache[List[HudiPartition]]` with `entryName="partition"`, key `ConnectorTableKey(db, table, instant-as-snapshotId, -1)`. The instant (commit time) is hudi's MVCC coordinate, so pinning it into the key makes the cache always-correct (rule E: new instant ⇒ new key; plus bounded TTL + REFRESH invalidation). This is the secondary, TTL-bounded win — build it after the per-statement memo.
- **per-scan hoist**: the two scan-planning sites reuse the single memoized metaClient/schema via B.
- **Pom**: add `fe-connector-cache` dependency (currently absent). **Verify** hudi's assembly/shade bundles **Caffeine ≥ 2.9.3** — hudi does not receive it transitively the way iceberg does via iceberg-core; otherwise the child-loaded `MetaCacheEntry` `NoClassDefFound`s at runtime though it compiles (per the framework-coherence memory note).
- **Iron rules**: A honored (all new memo/caches connector-side); D honored (hudi vends no per-user credentials, no `SUPPORTS_USER_SESSION`, so the `(table,instant)` name-keyed cache needs no per-user disabling); E honored (instant in key + TTL). **Trino**: mirrors Trino keeping the fresh per-transaction view (timeline) distinct from the long-lived metastore cache.

### D.2 Maxcompute — consumes B only

- **per-metadata handle memo (B)**: the seam target is `resolveConnectorTableHandle`'s 14–17-site fan-out (recon corrects the report's "10"). Use **B1** (wrap `MaxComputeConnectorMetadata.getTableHandle` in `resolveInStatement(session, "maxcompute.handle", db, table, ...)`) or the sanctioned **B2** (memoize the fe-core call site). Either collapses to one `getTableHandle`/statement, which **drops the redundant remote `tables().exists()` HEAD probe + lazy `tables().get()`** to once. Within one resolved handle the ODPS metadata already amortizes (`odpsTable.getSchema()` reused by schema/columns/scan); the fan-out is the only leak.
- **No new cross-query cache**: maxcompute already owns a cross-query `MaxComputePartitionCache` keyed `(db, table)` (owned in the Connector, so the §C owner-file scan covers it). It uses a static AK/SK identity and does not declare `SUPPORTS_USER_SESSION` — so it is rule-D-compliant as-is and needs no §A wrapper this round.
- **Verify**: ODPS uses a static (not per-user) identity, so the resolved handle is safe to share per-statement (and would even be cross-query-safe) — confirm before treating it as shareable (rule D). Confirm `getTableHandle` has `ConnectorSession` access for the B1 wrap.
- **Iron rules**: A honored (B1) or sanctioned-minimal (B2); D honored. **Trino**: same "resolve once per unit" as iceberg.

### D.3 Es — consumes B only, must NOT get a cross-query cache

- **per-scan hoist of `EsMetadataState` (B)**: `fetchMetadataState` fires 2x/query (planScan + buildScanNodeProperties). Memoize the **mapping/schema half** per statement via `resolveInStatement(session, "es.metadata_state", db, table, ...)` → 1 fetch.
- **mapping/field-context carrier**: `getTableSchema` currently discards the raw mapping string that `resolveFieldContext` later needs (`EsConnectorMetadata.java:85-93`), forcing ~4 remote `getMapping` calls/query. Attach the raw mapping to a **statement-scoped `EsMetadataState`** (or the `EsTableHandle`), not to the generic `ConnectorTableSchema` (which must stay connector-agnostic — rule B). `resolveFieldContext` then reuses it, collapsing ~4 `getMapping` → 1.
- **CRITICAL split (rule E)**: `EsMetadataFetcher.fetch()` currently bundles `fetchMapping()` (reuse-safe) with `fetchShards()` (live shard routing). The reuse layer may memoize the **mapping** within a statement but **shard routing must stay per-statement fresh and must NEVER be a cross-query name-keyed cache**. So the memoized `EsMetadataState` carries only mapping/field-context; `fetchShards` is always re-run. Es gets **no §A cross-query cache**.
- **Iron rules**: A honored (state connector-side); B honored (raw mapping rides the handle/state, not the generic schema); E honored (shard routing stays per-statement). **Trino**: mirrors Trino keeping `CachingDirectoryLister`/shard state as a fresh per-transaction layer distinct from the metadata cache.

---

## E. Sequencing

Foundation first, then flagship, then the two small PRs, then the gate. Each PR is independently landable and independently verified.

1. **PR-1 — Generic cache wrapper (fe-connector-cache).** Add `ConnectorMetadataCache[V]` (both ctors); rename `PartitionViewCacheKey` → `ConnectorTableKey` (mechanical, updates iceberg/hive/paimon imports); make `ConnectorPartitionViewCache[V]` a thin `entryName="partition_view"` specialization; **fix the stale javadoc**. Pure addition + rename; no behavior change. Verify: reactor test-compile + existing partition-view tests.

2. **PR-2 — Statement-scope seam (fe-connector-api) + iceberg retrofit.** Add `ConnectorStatementScopes.resolveInStatement`; collapse `IcebergStatementScope.sharedTable` to delegate (byte-identical key). Verify: iceberg's existing per-statement memo tests + full iceberg suite prove cost-invariance. **This is where the owner's D4 decision is exercised** — land it as the fe-connector-api helper (rule A intact); only if the owner insists on the literal fe-core exception, add the B2 memoized call site as a separate, clearly-flagged commit.

3. **PR-3 (optional, foundation-first) — Iceberg cache convergence.** Retrofit iceberg's five hand-rolled caches onto `ConnectorMetadataCache[V]` using the pre-resolved-`CacheSpec` ctor (shared `table.ttl-second` knob), keeping the connector-side credential null-gates. Guarded by existing iceberg cache tests + `loadCountForTest`. **Residual**: do this now (max convergence, foundation-first mandate) vs defer (narrower diff, avoids re-touching audited caches).

4. **PR-4 — Flagship hudi.** Pom dep on fe-connector-cache + Caffeine-bundling verification; wire metaClient + schema memo via the seam (B); ensure HMS access sits behind `CachingHmsClient`; add the `(table, instant)` cross-query partition cache via A; per-scan hoist. **E2E regression required** (heterogeneous + standalone hudi-on-HMS, per the memory rule that new HMS-delegated capabilities need e2e). Largest PR.

5. **PR-5 — Maxcompute (small).** Memoize `getTableHandle` per statement (B1) — or the fe-core B2 site if chosen in PR-2; drop the redundant `exists()` probe. Verify: query-count/remote-op assertions on a SELECT.

6. **PR-6 — Es (small).** Per-scan hoist `EsMetadataState` via B; attach raw mapping to the statement-scoped state; split `fetch()` so shard routing stays per-statement. Verify: `getMapping` call-count drops to 1; shard routing still re-runs per statement.

7. **PR-7 — Gate generalization (D3).** Rewrite `check-authz-cache-sharding.sh` to two-stage discovery + anti-no-op floor; extend the self-test fixtures. Orthogonal — can land any time, but recommended **after PR-1/PR-2** so the marker contract and `*Cache` type convention are stable, and **before** hudi/mc/es would ever declare session=user.

PR-1 and PR-2 are the foundation and unblock everything. PR-4/5/6 depend on both. PR-7 is independent.

---

## Risks

- **Iceberg retrofit re-touches audited caches.** Converging the five caches (PR-3) widens the diff over previously-audited code. Mitigation: the octet + `CacheSpec` + invalidation are provably identical; gate on existing `*ForTest`/`loadCountForTest` assertions; keep the shared-knob ctor so operator-facing config is byte-identical. If the owner prefers a narrower blast radius, PR-3 can be deferred and iceberg's five caches left as-is (they already work), with `ConnectorMetadataCache` used only by new consumers — but that forgoes the foundation-first convergence the mandate asks for.
- **Hudi Caffeine bundling.** Compiles but `NoClassDefFound`s at runtime if the hudi assembly doesn't self-carry Caffeine ≥ 2.9.3. Mitigation: verify hudi's shade/assembly config (not just `<dependencies>`) and add an explicit Caffeine dep if not transitively present; smoke-test with a redeploy.
- **Hudi metaClient lifecycle in the statement scope.** If a hudi version's `HoodieTableMetaClient` (or a memoized wrapper) is `AutoCloseable`/holds a `FileSystem`/`HoodieStorage`, `closeAll` auto-closes it at statement end — desirable only if nothing outside the statement retains it. Mitigation: verify per hudi version; if not, memoize a non-closeable projection, not the client.
- **Gate discovery drift → silent no-op.** A capability rename or grep-form change could empty the declarer set. Mitigation: the anti-no-op floor (`exit` non-zero on zero declarers) + the two new self-test fixtures.
- **Es shard-routing regression.** Any reuse layer that accidentally memoizes `fetchShards` output cross-query (or even reuses stale routing across a statement in a way that misroutes) breaks correctness. Mitigation: the memoized `EsMetadataState` must, by construction, hold only mapping/field-context; keep `fetchShards` a separate always-fresh call; assert routing re-runs.
- **Cross-connector key collision in a gateway statement.** Two connectors memoizing the same `(db, table)` under the same namespace would collide on the opaque `Object` and `ClassCastException`. Mitigation: `keyNamespace` is a required, connector-distinct parameter in `resolveInStatement` (and the §A cache is per-connector-instance, so no cross-connector sharing there).
- **Credential leak via a future generic cache.** If a future connector caches a credentialed value under only the session=user gate, the name-keyed cache could leak (list≠load). Mitigation: the wrapper stays value-opaque and the credential gate stays in the connector (null the field); the §C gate enforces the marker on every `*Cache` field. Residual: whether to harden this from convention to a type/marker contract (see below).

---

## Residual owner decisions

1. **D4 placement (highest priority).** Recon shows the reachable home for the seam is **fe-connector-api**, not fe-core — iceberg/hudi cannot depend on fe-core, and the memo primitive already exists there. Do you accept re-reading "fe-core seam" as "fe-connector-api helper" (`ConnectorStatementScopes.resolveInStatement`), leaving **fe-core source flat and iron rule A untouched**? If you still want the literal fe-core exception, its minimal form is the B2 memoized call site in `resolveConnectorTableHandle` — **zero new fe-core type, ~3 lines reusing the existing `computeIfAbsent`** — used chiefly to collapse maxcompute's fan-out generically. (Recommendation: fe-connector-api helper for iceberg+hudi; B2 only if you want mc's fan-out collapsed without touching the mc connector.)

2. **Iceberg cache convergence now vs later (PR-3).** Retrofit all five iceberg caches onto `ConnectorMetadataCache` this round (foundation-first, max convergence, wider diff over audited code) — or add the generic class for new consumers only and leave iceberg's five as-is (narrower, less convergence)?

3. **Config namespace for hudi/mc/es caches.** Framework-native per-entry knobs (`meta.cache.<engine>.<entry>.*`, recommended) or a single shared per-catalog knob like iceberg's `table.ttl-second`? Affects operator surface and any back-compat mapping (`CacheSpec.applyCompatibilityMap`) for existing mc/es TTL properties.

4. **Comment cache as a first-class entry?** Iceberg builds `commentCache` only for vended catalogs because plain catalogs serve the comment path from the table-handle cache. Do hudi/mc/es have a table-handle cache to piggyback, or does each need a standalone comment path? Decides whether the wrapper models "comment" as a first-class entry or leaves it per-connector. (Given D1 scope, likely leave per-connector — none of hudi/mc/es requested a comment cache.)

5. **Latest-snapshot / MVCC-pin shape.** Iceberg's pin is `(snapshotId, schemaId)`, paimon's is a single long, hudi's is an instant, mc/es have no snapshot axis. Keep the pin as opaque `[V]` over the 4-tuple key (recommended — one key, `-1` sentinels) rather than a fixed 2-axis value type?

6. **Credential-value hardening: contract vs convention.** Should the wrapper *refuse* credentialed values by a type/marker constraint, or keep the current convention (connector nulls the field under its gate + the §C marker enforces it)? Convention is the smaller design and matches today; a contract is insurance against a future connector caching a credentialed value under only the session=user gate.

7. **Gate strictness on empty declarer set.** Hard build FAIL (recommended) vs warning when Stage 2 finds zero declarers? And is the hive-gateway sibling path (guarded at `HiveConnector.java:543`) in-gate-scope or explicitly documented as out?

8. **maxcompute cross-query shareability.** Confirm ODPS uses a static (non-per-user) identity so a resolved handle is safe beyond a single statement — this decides whether mc's handle memo may be statement-scoped only (safe default) or could later be promoted cross-query (rule D).