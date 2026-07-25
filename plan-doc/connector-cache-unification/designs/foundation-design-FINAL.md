Verification complete. All disputed load-bearing claims confirmed at HEAD:
- **BLOCKER real**: iceberg cache fields exist on BOTH `IcebergConnector.java` (marked, constructed) AND `IcebergConnectorMetadata.java:152,156,159,163,170,171` (unmarked, injected). Gate scans only `*Connector.java`. Hive constructs `partitionViewCache` unconditionally at `HiveConnector.java:134`, unmarked, and is excluded from the gate by the `.contains()` filter (`:543`).
- invalidateDb divergence real: all 5 iceberg caches use `Namespace.of(dbName)` + `id.namespace().equals(ns)` (`IcebergTableCache.java:102-103` et al.), not String equality.
- `ConnectorStatementScopeImpl.closeAll` pass-2 closes every `AutoCloseable` (`:65,82`); scope is `StatementContext`-owned with a reset+close path (`StatementContext.java:656,673-676,995-996`).
- mc/es have zero `getCapabilities`/`SUPPORTS_USER_SESSION` in main src (static identity, rule-D safe). mc's `partitionCache` is constructed on the Connector (`MaxComputeDorisConnector.java:79`).

Below is the final synthesized design.

---

## Final design summary

The owner-signed, foundation-first plan is buildable, and the adversarial pass **shrinks it further**: the recommended path adds **zero fe-core source lines**, so iron rule A is literally untouched, not merely "minimally crossed." Three reviewer objections are accepted as behavior-changing and force revisions; two proposals are struck; one reviewer either/or is rejected with reasoning.

Accepted revisions (most consequential first):
1. **Strike B2 entirely** (all three reviews). The maxcompute fan-out is collapsed connector-side (B1). No "sanctioned fe-core exception" is left on the table — a signed-off but avoidable fe-core mutation is how avoidable mutations get merged (Review 1 #1), and wrapping the shared `resolveConnectorTableHandle` funnel changes the call-count contract for jdbc/paimon/trino/hive that are out of this round's test scope (Review 3 #1). **fe-core grows by 0 lines. Iron rule A is untouched, not spent.**
2. **Redesign gate D3 from owner-file field-marker scan to module-wide construction-site scan** (Review 2 BLOCKER, Review 3 #3). The generic wrapper's real holders already live unmarked on `*ConnectorMetadata` today (verified) and can move anywhere tomorrow; a field-marker-on-`*Connector.java` gate is structurally blind to them and verifies a *comment*, not the *null-gate*. The gate must key off `new …Cache(` construction expressions and assert the capability null-gate at the construction site.
3. **Soften the iceberg retrofit from "byte-identical" to "functionally equivalent for Doris single-level namespaces," gated by an explicit `invalidateDb` parity test** (Review 1 #4, Review 3 #2). The retrofit relocates db-extraction from iceberg `Namespace` equality to String equality; keep db-extraction in the connector's key builder.
4. **Make the hudi non-closeable projection mandatory, not optional** (Review 3 #5). Hudi is the first `AutoCloseable`-risk value in the scope.
5. **Resolve the hudi freshness/(A)-cache tension** (Review 3 #6) rather than deferring: re-derive the latest *completed* instant fresh per statement (cheap, from the memoized metaClient's timeline), cache the *expensive partition list* cross-query under `(table, instant)`. Hits occur precisely when the table has not committed (the common case); a new commit mints a new instant → new key → miss → fresh load. Freshness is preserved and the win is real — the either/or framing is rejected, but its "latest-completed + fresh-per-statement" pin is adopted.
6. **Delete the `ConnectorPartitionViewCache[V]` compat subclass** (Review 1 #3): its three consumers are already rewritten for the key rename in PR-1, so keep one type (`ConnectorMetadataCache[V]`) and drop the inheritance layer.
7. **Verify the prepared-EXECUTE scope lifecycle before PR-2** (Review 3 #4) and centralize the `keyNamespace` set (Review 3 #7) and legacy telemetry names (Review 3 #8).

The design targets only the two real cross-query consumers (iceberg + hudi) plus two per-statement-only consumers (mc + es). No `[K]` type parameter, identity-sharding dimension, or credential abstraction is added (Rule 2).

---

## A/B/C/D components (revised)

### A — Generic cross-query cache wrapper (fe-connector-cache)

**Layer**: `fe/fe-connector/fe-connector-cache`, package `org.apache.doris.connector.cache` (JDK+Caffeine only, verified zero fe-core imports — the fe-core `datasource/metacache/` `CacheSpec`/`MetaCacheEntry` is an independent duplicate this track never touches; iron rule A clean for the whole A track).

**What is generalized (grounded).** `ConnectorPartitionViewCache[V]` already is the octet iceberg hand-rolls: `new MetaCacheEntry[...](engine + "." + entryName, null, spec, ForkJoinPool.commonPool(), false, true, 0L, true)` (`ConnectorPartitionViewCache.java:70-71`), `matches`/`matchesDb` on the key (`PartitionViewCacheKey.java:66-72`). The five iceberg caches differ only in **key type**, **value type**, and the **db-match predicate**. It hardcodes exactly two axes: `ENTRY_PARTITION_VIEW="partition_view"` and the key class. The manifest cache is **excluded** (path-keyed, no-TTL, default-off, `DataFile`-typed, `authz-cache-exempt` — read only after a per-user `resolveTable`; `IcebergConnector.java:203`).

**Class shape.** Promote to `ConnectorMetadataCache[V]`:
- **Value stays opaque `[V]`** — load-bearing: values are `String` (comment/format), raw credentialed `org.apache.iceberg.Table` (table-handle), 2-long `CachedSnapshot` (latest-snapshot), `List[…]` (partition). A concrete value type would be useless to a second connector.
- **Key**: rename `PartitionViewCacheKey` → `ConnectorTableKey(db, table, snapshotId, schemaId)` (body already generic; only its name is partition-flavored). Unused axes pass `-1`. One key type for all six cases.
- **Two ctors**: framework-native per-entry (`meta.cache.[engine].[entry].{enable|ttl-second|capacity}`, recommended for hudi/mc/es) **and** pre-resolved `CacheSpec` (so iceberg keeps its single shared `meta.cache.iceberg.table.ttl-second` knob across all five caches — operator back-compat). The `ttl<=0 -> CACHE_TTL_DISABLE_CACHE` mapping that is copy-pasted five times today moves into `CacheSpec.of`/`fromProperties` (already there).
- **Delete `ConnectorPartitionViewCache[V]`** (revision #6). Iceberg/hive/paimon are touched by the rename in PR-1 anyway; have them construct `ConnectorMetadataCache[V]` directly (hive currently does `new ConnectorPartitionViewCache[](\"hive\", props)` at `HiveConnector.java:134` → `new ConnectorMetadataCache[](\"hive\", \"partition_view\", props)`). One type, no inheritance layer for a non-SPI toolkit class.
- **Fix the stale javadoc** `"this class has NO consumers yet"` (`ConnectorPartitionViewCache.java:33`, confirmed false — iceberg `:197,199`, hive `:134`, paimon consume it).
- **Pin legacy telemetry names in the iceberg retrofit** (Review 3 #8): the framework-native ctor derives `name = engine + \".\" + entryName` (→ `iceberg.table`), but iceberg's live entry names are `iceberg-table`, `iceberg-partition`, `iceberg-latest-snapshot`, etc. (`IcebergTableCache.java:70`). The iceberg retrofit MUST pass the exact legacy `name` via the pre-resolved `CacheSpec` ctor so dashboards/log-greps/`*ForTest` accessors don't silently break.

**Credential policy stays connector-side (iron rule D).** The wrapper is value-opaque and knows nothing about credentials. The connector nulls the field under its gate exactly as today (verified `IcebergConnector.java:231,243,254,260` — `isUserSessionEnabled()`/`restVendedCredentialsEnabled` ternaries): table-handle null when `isUserSessionEnabled() || restVendedCredentialsEnabled`; comment null unless `restVended && !userSession`; format/partition/latest-snapshot null under `isUserSessionEnabled()`. A null field ⇒ `get()` never called ⇒ live bypass. **Trino alignment**: shared low-level toolkit + per-connector caches, no unified cache; we deliberately do NOT port Trino's per-identity `LoadingCache[user,…]` sharding — Doris keeps the binary null-field cut (correct for D1, no connector needs identity keying).

### B — Statement-scope seam (fe-connector-api ONLY; fe-core untouched)

**Premise correction (verified).** The memo substrate is already built and already in **fe-connector-api**: `ConnectorStatementScope.computeIfAbsent(key, Supplier)` (`:45`), `ConnectorSession.getStatementScope()` defaulting to `NONE` (`:142`), backed by `ConnectorStatementScopeImpl` (ConcurrentHashMap, idempotent `closeAll`) hung on `StatementContext` (`:209,656`). Iceberg and hudi depend on fe-connector-api and on **neither fe-core nor fe-connector-cache** — a literal fe-core class would be *unreachable* by the retrofit. **The D4 mandate is satisfied with 0 fe-core source lines; iron rule A is not crossed at all.**

**The seam: one bare static helper + a namespace registry in fe-connector-api** (beside `ConnectorStatementScope`):

```
public final class ConnectorStatementScopes {
    private ConnectorStatementScopes() {}
    // resolve db.table once per statement; keyNamespace namespaces the value TYPE so a heterogeneous
    // gateway statement touching two connectors cannot collide on (db,table) -> ClassCastException.
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

- No new interface, no new type on `ConnectorSession`, no new SPI method — it reuses the existing primitive and standardizes the **security-critical** key convention (dropping `queryId` leaks cross-statement; dropping `catalogId` collides across a cross-catalog MERGE). Centralizing this once is a real correctness win over two connectors re-deriving it (Review 1 #2 grants this).
- **Namespace registry** (Review 3 #7): define the `keyNamespace` constants (`iceberg.table`, `hudi.metaclient`, `maxcompute.handle`, `es.metadata_state`) in one small holder beside `ConnectorStatementScopes`, documented as a reviewed-uniqueness invariant mirroring the existing metadata-funnel key convention (`ConnectorStatementScope.java:51`). `catalogId` already sits inside the key, so two connectors under different catalogs can't collide even on an equal namespace — but the registry prevents same-catalog namespace reuse.

**Retrofit + consumption:**
- **Iceberg (retrofit)**: `IcebergStatementScope.sharedTable` collapses to `return ConnectorStatementScopes.resolveInStatement(session, \"iceberg.table\", db, table, loader);` — namespace `iceberg.table` reproduces the existing `\"iceberg.table:\"` prefix byte-for-byte, so the funnel sites keep identical hits/misses/`NONE` fallthrough. `rewritableDeleteSupply` stays iceberg-private (a `(catalogId,queryId)`-keyed scan→write delete bridge, not table resolution — out of the seam). **Residual (Review 1 #2): whether to land this retrofit in PR-2 or defer it** — retrofitting audited iceberg widens the diff for no functional gain, but it is also the parity proof. Recommendation: keep it in PR-2 guarded by iceberg's existing memo tests; if the owner wants a narrower blast radius, hudi can be the sole first consumer and iceberg converges later.
- **Hudi (consumes)**: `HoodieTableMetaClient mc = ConnectorStatementScopes.resolveInStatement(session, \"hudi.metaclient\", db, table, () -> buildMetaClient(...))` collapses the ~6 builds/query → 1. Derived `TableSchemaResolver`/Avro/`InternalSchema` compute once off the single memoized client (~4x re-parse → 1). **MANDATORY (Review 3 #5, revision #4)**: memoize a **non-closeable projection** (the resolved schema/timeline snapshot callers need), NOT the raw `HoodieTableMetaClient`, because `closeAll` pass-2 closes every stored `AutoCloseable` at statement end (`ConnectorStatementScopeImpl.java:82`) and hudi is the first connector data-object that may be closeable / hold a `HoodieStorage`/`FileSystem`. This ordering was validated for iceberg's non-closeable `Table`, never for a closeable client.
- **Maxcompute (consumes, B1 only)**: memoize inside `MaxComputeConnectorMetadata.getTableHandle` via `resolveInStatement(session, \"maxcompute.handle\", db, table, …)`. All fan-out sites in `resolveConnectorTableHandle` then hit the connector memo; the remote `exists()` HEAD probe + lazy `get()` fires once. **fe-core untouched. B2 struck.** Precondition verified: `ConnectorSessionBuilder.captureStatementScope()` reads `sc.getOrCreateConnectorStatementScope()` (`:202`), so all in-statement `buildConnectorSession()` calls share one scope instance — B1 genuinely fires.
- **mc claim scoped (Review 1 #5)**: the collapse applies to the in-statement `buildConnectorSession()` subset only; `buildCrossStatementSession()` callers (refresh/cross-statement paths) bind a non-per-statement scope and by design do not share the memo. Do not "fix" them into it.

**Rule D safety.** The L1 memo is single-identity by construction — a statement resolves one catalog under one identity (`PluginDrivenMetadata` fail-loud pin), scope per-statement + GC'd at end. It never crosses users even when the L2 §A cache is disabled for a vended catalog.

### C — Authz gate generalization (tools/check-authz-cache-sharding.sh) — REDESIGNED

> **SUPERSEDED (2026-07-24, owner decision): the gate was REMOVED, not generalized.** Two rounds of adversarial
> clean-room review showed that structurally verifying "this cache is null under session=user" in a shell
> script requires understanding arbitrary Java boolean/multi-line syntax (compound `&&`/`||` guards, multi-line
> lambda loaders, string literals, nested ternaries) — intractable and fragile (false positives break builds on
> legit code; false negatives silently miss leaks). The runtime `IcebergConnectorCacheTest` already proves the
> invariant for the real caches; the gate (`tools/check-authz-cache-sharding.sh` + self-test + the
> `fe-connector/pom.xml` execution) was deleted and replaced by an explicit `ATTN` comment at the iceberg cache
> sites. See [`../progress.md`](../progress.md) "2026-07-24 (5)". The redesign notes below are retained for history only.

Pure tooling; touches no product source. The original owner-file field-marker scan is **rejected** (Review 2 BLOCKER, Review 3 #3): verified that iceberg holds cache fields on both `IcebergConnector.java` (marked) and `IcebergConnectorMetadata.java:152-171` (unmarked injected), and hive constructs an unmarked cross-query `partitionViewCache` at `HiveConnector.java:134` that the `.contains()` filter excludes from scope. A field-marker gate verifies a comment near a declaration, not the actual null-gate, and is blind to any holder off `*Connector.java`.

**Redesigned gate — construction-site scan, location-independent:**

- **Stage 1 — enumerate declaring modules**: `find` all `*Connector.java` under `*/src/main/java/*` (exclude `target/`); keep a module whose `*Connector.java` **declares** the capability via `\.add\([^)]*ConnectorCapability\.SUPPORTS_USER_SESSION` OR `(EnumSet|Set|ImmutableSet)\.of\([^)]*SUPPORTS_USER_SESSION`, after stripping comment lines (`^\s*\*`, `^\s*//`). This admits `IcebergConnector.java:860` (`.add(...)`).
- **Stage 1b — gateway modules in scope (Review 2 major)**: ALSO keep any module whose `*Connector.java` **reads** a sibling capability via `\.getCapabilities\(\)\.contains\([^)]*SUPPORTS_USER_SESSION` — this is the gateway signature (`HiveConnector.java:543`). Gateways delegate to per-user siblings and hold live cross-query caches, so they are in-scope, not excluded. Additionally assert the fail-loud front-door guard is present (the `throw` at `HiveConnector.java:546`); if a gateway reads the sibling capability but lacks the fail-loud assertion, FAIL.
- **Stage 2 — scan construction sites module-wide** (the key change): within each in-scope module, grep ALL `src/main/java` files for construction expressions `new (ConnectorMetadataCache|[A-Za-z_][A-Za-z0-9_]*Cache)[ <(]` (the holder idiom), distinguishing them from injected ctor-param reference fields (which are not `new`). Each construction site must either:
  - sit inside a capability null-gate — its assignment RHS is a `isUserSessionEnabled() ? null : new …` (or equivalent capability-gated) ternary — the `authz-cache-session-user-disabled` contract, **now checked at construction, not just as a field comment**; OR
  - carry an on-line/line-above `authz-cache-exempt` marker with justification.
- **Stage 3 — hive's `partitionViewCache` (Review 2 major)**: require it to carry an explicit `authz-cache-exempt` marker documenting "front door never declares SUPPORTS_USER_SESSION + fail-loud sibling guard," so the exemption is a reviewed claim, not an invisible gap. (It is unconditionally constructed and unmarked today — `HiveConnector.java:112,134`.)
- **Anti-no-op floor (Review 2, design §C.2)**: if Stage 1+1b yields **zero** in-scope modules, `exit` non-zero — a capability rename or grep drift must fail loud, never silently turn the gate into pass-everything.

**Self-test additions**: (a) fixture with `.contains(SUPPORTS_USER_SESSION)` + fail-loud guard + an `authz-cache-exempt` cache → PASS (gateway path); (b) same gateway but MISSING the fail-loud guard → FAIL; (c) fixture declaring via `.add`/`EnumSet.of` with a `new …Cache(` constructed unconditionally (no null-gate, no marker) → FAIL (proves construction-site + null-gate check); (d) fixture with a marked field but constructed unconditionally → FAIL (proves the marker-detached-from-guard hole is closed). Keep iceberg as the primary declarer fixture.

**Scope reality**: today Stage 1 resolves to `{iceberg}` and Stage 1b to `{hive}`; hudi/mc/es have no capability override (verified) and vend static credentials, so their caches are authz-safe without the gate and correctly fall outside it. D3 is **forward-insurance**, orthogonal to D1/D2, and does not block them. The fe-core defense layer (`PluginDrivenExternalCatalog.shouldBypass*Cache`, keyed off runtime `.contains(SUPPORTS_USER_SESSION)`, `:1289`) is already connector-agnostic and needs no change.

### D — Connector consumption

**D.1 Hudi (flagship) — consumes B (primary) + A (secondary).**
- **metaClient + schema memo (B)**: route both `HoodieTableMetaClient.builder()` sites through `resolveInStatement(session, \"hudi.metaclient\", …)` — memoizing a **non-closeable schema/timeline projection** (mandatory, §B). `buildMetaClient` reloads the active timeline for freshness, so the per-statement tier is the correct one for the raw client; a cross-query cache of the raw metaClient would be wrong.
- **HMS layer**: hudi is HMS-backed; its metastore access sits behind the shared `CachingHmsClient` (Trino `CachingHiveMetastore` shape, coarse REFRESH+TTL, TCCL-pinned per the `ThriftHmsClient.doAs` memory) exactly as iceberg-on-HMS/hive, so `getTable`/listing round-trips are shared cross-query beneath the per-statement memo.
- **`(table, instant)` partition cache (A) — tension resolved (revision #5, rejecting Review 3 #6's either/or)**: a `ConnectorMetadataCache[List[HudiPartition]]`, `entryName=\"partition\"`, key `ConnectorTableKey(db, table, instant-as-snapshotId, -1)`. **Pin (adopting Review 2 minor + Review 3 #6's fresh requirement)**: the instant MUST be `metaClient.getActiveTimeline().filterCompletedInstants().lastInstant()` (latest *completed*, never inflight/requested), **re-derived fresh each statement** from the statement-scoped metaClient. This is cheap (timeline read); the *expensive* partition-listing is what the cache saves. Cross-query hits occur when the table has not committed between queries (the common case — a stable table's latest-completed-instant is the same value each statement, so the key is identical); a new commit mints a new instant → new key → miss → fresh load. Freshness is therefore preserved AND hit rate is real — Review 3's "near-zero hit rate" holds only for a table committing on every query, which is not the steady state. TTL+REFRESH is the backstop (iron rule E). The cached value is a pure `List[HudiPartition]` projection with **no live metaClient/FileSystem reference** (assert in test).
- **Pom**: add `fe-connector-cache` dependency (absent today) and **verify hudi's shade/assembly self-bundles Caffeine ≥ 2.9.3** — hudi does not receive it transitively as iceberg does via iceberg-core; otherwise the child-loaded `MetaCacheEntry` `NoClassDefFound`s at runtime though it compiles (framework-coherence memory note).
- Iron rules: A honored (all memo/caches connector-side); D honored (hudi vends no per-user credentials, no `SUPPORTS_USER_SESSION`); E honored (instant-in-key + TTL + fresh derivation).

**D.2 Maxcompute — consumes B only (B1).**
- Memoize `MaxComputeConnectorMetadata.getTableHandle` per statement (`maxcompute.handle`); collapses the ~14–17-site `resolveConnectorTableHandle` fan-out → 1, dropping the redundant remote `tables().exists()` HEAD probe + lazy `tables().get()` to once. Within one resolved handle the ODPS metadata already amortizes (`odpsTable.getSchema()`).
- **No new cross-query cache**: mc already owns `MaxComputePartitionCache` keyed `(db,table)`, constructed on the Connector (`MaxComputeDorisConnector.java:79` — the §C construction-site scan covers it). It uses static AK/SK, declares no `SUPPORTS_USER_SESSION` (verified) → rule-D-compliant as-is, no §A wrapper this round.
- Verify: ODPS static identity confirmed → handle safe to share per-statement (residual: whether it could later be promoted cross-query). Confirm `getTableHandle` has `ConnectorSession` access for the B1 wrap.

**D.3 Es — consumes B only, must NOT get a cross-query cache (iron rule E).**
- **per-scan hoist of `EsMetadataState` (B)**: `fetchMetadataState` fires 2x/query (planScan + buildScanNodeProperties). Memoize the **mapping/schema half** per statement via `resolveInStatement(session, \"es.metadata_state\", …)` → 1 fetch. Es has no `*Cache` field at HEAD (grep empty), so there is nothing to regress yet — PR-6 introduces the memo.
- **mapping/field-context carrier**: `getTableSchema` discards the raw mapping string `resolveFieldContext` later needs (`EsConnectorMetadata.java:85-93`), forcing ~4 remote `getMapping`/query. Attach the raw mapping to the **statement-scoped `EsMetadataState`** (or `EsTableHandle`), NOT to the generic `ConnectorTableSchema` (must stay connector-agnostic — iron rule B). Collapses ~4 `getMapping` → 1.
- **CRITICAL split (iron rule E)**: `EsMetadataFetcher.fetch()` bundles `fetchMapping()` (reuse-safe) with `fetchShards()` (live shard routing). The memoized `EsMetadataState` carries **only** mapping/field-context; `fetchShards` is **always re-run per statement** and is **never** a cross-query name-keyed cache. Es gets no §A cache.

---

## Sequencing

Foundation → flagship → two small PRs → gate. Each PR independently landable + verified.

1. **PR-0 (verification gate, blocks PR-2)** — confirm the prepared-EXECUTE scope lifecycle (Review 3 #4). Verified: `connectorStatementScope` is `StatementContext`-owned with a reset+close path (`StatementContext.java:672-677`) and closes at result return (`:995-996`). **Mechanism correction (2026-07-23 recon):** a prepared `EXECUTE` does NOT get a fresh `StatementContext` — it deliberately *reuses* one across executions (`ExecuteCommand.java:89`). Isolation is provided not by a fresh context but by an explicit `statementContext.resetConnectorStatementScope()` at the top of every `ExecuteCommand.run()` (`ExecuteCommand.java:95`, before all branches) plus a per-retry reset (`StmtExecutor.java:1077`). Recon also confirmed external/connector tables genuinely reach this path (they never take the OLAP short-circuit fast path — that rule matches `logicalOlapScan()` only, `LogicalResultSinkToShortCircuitPointQuery.java:88,97` — so an external-table prepared query always runs the normal `executor.execute()` planner, re-resolving the table each execution). The reset call is therefore reachable and load-bearing, but had NO test proving `ExecuteCommand` invokes it (existing tests cover only the reset primitive). Encode as a **wiring test** that drives `ExecuteCommand.run()` (two EXECUTEs of one prepared stmt must NOT share a memoized table). Cheap; do it before shipping the shared helper.
2. **PR-1 — Generic cache wrapper (fe-connector-cache).** Add `ConnectorMetadataCache[V]` (both ctors); rename `PartitionViewCacheKey` → `ConnectorTableKey`; **delete** `ConnectorPartitionViewCache[V]` and repoint iceberg/hive/paimon to construct `ConnectorMetadataCache[V]` directly; fix the stale javadoc. Pure addition + rename + deletion; no behavior change. Verify: reactor test-compile + existing partition-view tests.
3. **PR-2 — Statement-scope seam (fe-connector-api) + iceberg retrofit.** Add `ConnectorStatementScopes.resolveInStatement` + namespace registry; collapse `IcebergStatementScope.sharedTable` to delegate (byte-identical key). **fe-core: 0 lines. B2 does not exist.** Verify: iceberg per-statement memo tests + full iceberg suite prove cost-invariance. (Residual: defer iceberg retrofit to narrow blast radius — Review 1 #2.)
4. **PR-3 (foundation-first) — Iceberg cache convergence.** Retrofit the five hand-rolled caches onto `ConnectorMetadataCache[V]` via the pre-resolved-`CacheSpec` ctor (shared knob + **pinned legacy names**), keeping connector-side credential null-gates. **Gated on the new `invalidateDb` parity test** (revision #3). Residual #2: now vs later.
5. **PR-4 — Flagship hudi.** Pom dep + Caffeine-bundling verification; metaClient + schema memo via B (**non-closeable projection**); HMS behind `CachingHmsClient`; the `(table, completed-instant, fresh-per-statement)` cross-query partition cache via A; per-scan hoist. **E2E regression required** (heterogeneous + standalone hudi-on-HMS, per the HMS-delegated-capability memory rule), including a concurrent-commit partition-listing test.
6. **PR-5 — Maxcompute (small).** B1 memo inside `getTableHandle`; drop the redundant `exists()` probe. Verify: remote-op call-count on a SELECT.
7. **PR-6 — Es (small).** Per-scan hoist `EsMetadataState` via B; attach raw mapping to statement-scoped state; split `fetch()` so shard routing stays per-statement. Verify: `getMapping` → 1; shard routing re-runs per statement.
8. **PR-7 — Gate generalization (D3, redesigned).** Construction-site module-wide scan + gateway handling + anti-no-op floor + null-gate assertion; extend self-test fixtures. Orthogonal; recommended after PR-1/PR-2 (marker/`*Cache` conventions stable) and before any of hudi/mc/es would declare session=user.

PR-1 + PR-2 are the foundation and unblock PR-3/4/5/6. PR-7 is independent. PR-0 blocks PR-2.

---

## Risk register

| # | Risk | Severity | Mitigation | Owner of proof |
|---|------|----------|------------|----------------|
| R1 | **Gate blind spot** — a future per-user connector holds a `ConnectorMetadataCache` off `*Connector.java` and leaks cross-user | High (was BLOCKER) | Construction-site module-wide scan (§C Stage 2) + null-gate assertion + anti-no-op floor; self-test fixtures (c)/(d) | PR-7 self-test |
| R2 | **Marker/guard drift** — copy-paste keeps the `authz-cache-session-user-disabled` comment but drops the `isUserSessionEnabled() ? null :` guard | High | Gate asserts the capability-gated ternary at the construction RHS, not just the field comment; self-test fixture (d) | PR-7 self-test |
| R3 | **Hive gateway silent hole** — live unmarked `partitionViewCache` + per-user sibling delegation fall outside gate | High | Gateway modules in-scope (Stage 1b); require `authz-cache-exempt` on `partitionViewCache` + assert fail-loud front-door guard | PR-7 self-test (a)/(b) |
| R4 | **Iceberg `invalidateDb` semantic drift** — `Namespace` equality → String equality diverges for multi-level namespaces | Med | Parity test asserting identical eviction (incl. multi-level case or a proof Doris iceberg namespaces are single-level); keep db-extraction in connector key builder | PR-3 |
| R5 | **Hudi metaClient use-after-close** — `closeAll` pass-2 closes a closeable client whose derived objects escape the statement | Med | Memoize a non-closeable projection (mandatory); test a scan pump reading memoized hudi state after nominal statement end | PR-4 |
| R6 | **Hudi Caffeine bundling** — compiles, `NoClassDefFound`s at runtime | Med | Verify hudi shade/assembly self-carries Caffeine ≥ 2.9.3; add explicit dep if absent; redeploy smoke test | PR-4 |
| R7 | **Prepared-EXECUTE stale memo** — scope + queryId both reused → cross-execution stale table | Med | PR-0 lifecycle verification + test (two EXECUTEs must not share memoized table) | PR-0 |
| R8 | **Es shard-routing regression** — reuse layer memoizes `fetchShards` output | Med | `EsMetadataState` holds only mapping/field-context by construction; `fetchShards` always fresh; assert routing re-runs | PR-6 |
| R9 | **Cross-connector `keyNamespace` collision** — gateway statement → `ClassCastException` | Low | Required distinct `keyNamespace` param + central registry; `catalogId` inside key | PR-2 |
| R10 | **Iceberg telemetry name drift** — `iceberg-table` → `iceberg.table` breaks dashboards/`*ForTest` | Low | Pin exact legacy `name` strings via pre-resolved ctor; assert names in cache tests | PR-3 |
| R11 | **PR-3 re-touches audited caches** | Low | Provably-identical octet/`CacheSpec`/invalidation (except R4); gate on `*ForTest`/`loadCountForTest`; deferrable if convergence not worth the diff | PR-3 |
| R12 | **Hudi (A) cache staleness** if latest-instant is itself cross-query cached instead of fresh-derived | Low | Pin latest-*completed*-instant, re-derived fresh per statement from the memoized timeline; only the partition list is cross-query cached | PR-4 |

---

## Verification plan (UT/e2e/call-count gates)

**Behavior-invariance gates (iceberg retrofit — must prove zero change):**
- **UT / call-count**: existing iceberg `*ForTest` accessors + `loadCountForTest` on all five caches — hits/misses/evictions unchanged pre/post PR-3. (`IcebergTableCache.java:70` legacy names asserted.)
- **UT parity (R4)**: `invalidateDb(db)` on the retrofitted `ConnectorTableKey` path evicts exactly the entries the `Namespace.of(db) + id.namespace().equals` path evicts — including a multi-level-namespace fixture OR an explicit assertion that Doris iceberg namespaces are provably single-level.
- **UT (R7)**: two `EXECUTE`s of one prepared statement do NOT share a memoized `Table` (fresh scope per execution).
- **Symbol-level**: full fe-connector reactor `test-compile` BUILD SUCCESS after PR-1's rename + `ConnectorPartitionViewCache` deletion (strongest single invariance signal, per the zero-conflict-rebase memory).
- **Byte-key check**: assert the retrofitted iceberg statement-scope key string equals the pre-retrofit `\"iceberg.table:\" + catalogId + \":\" + db + \":\" + table + \":\" + queryId`.

**Perf-win gates (new consumers — must prove the redundancy collapses):**
- **Hudi (call-count UT)**: one `SELECT` builds `HoodieTableMetaClient` exactly 1x (was ~6) and parses schema/`InternalSchema` exactly 1x (was ~4); assert via a counting test double on `buildMetaClient`.
- **Hudi (e2e, required)**: heterogeneous HMS gateway + standalone hudi-on-HMS run the same SELECT/partition-listing and assert identical results (HMS-delegated-capability memory rule); plus a **concurrent-commit** e2e — list partitions across a live hudi commit, assert no partial/stale set (validates the latest-completed + fresh-per-statement pin, R12).
- **Hudi (UT, R5)**: the `(table,instant)` cached value holds no live metaClient/`FileSystem` reference (pure `List[HudiPartition]`); a scan pump reading memoized hudi state after nominal statement end does not hit a closed resource.
- **Maxcompute (call-count UT)**: one `SELECT` resolves `getTableHandle` 1x (was 14–17) and fires the remote `tables().exists()` HEAD probe 1x; assert via a counting ODPS client double, scoped to the in-statement `buildConnectorSession()` path (not `buildCrossStatementSession`, Review 1 #5).
- **Es (call-count UT)**: one `SELECT` calls `getMapping` 1x (was ~4) via the statement-scoped `EsMetadataState`; **and** `fetchShards()` still runs per statement (routing freshness) — assert both counts.

**Gate self-tests (PR-7):** fixtures (a) gateway with guard → PASS, (b) gateway missing guard → FAIL, (c) declarer with unconditional `new …Cache(` → FAIL, (d) marked field constructed unconditionally → FAIL, plus (e) zero-declarer input → non-zero exit (anti-no-op). Iceberg remains the primary declarer fixture and must still PASS unchanged.

**e2e note:** groovy regression requires a live cluster and does not run in this environment; the hudi e2e is specified for CI, not local.

---

## Residual owner decisions

1. **D4 placement (confirm the re-read).** Recon proves the reachable home is **fe-connector-api**, not fe-core (iceberg/hudi depend on neither fe-core nor fe-connector-cache; the memo primitive already exists in fe-connector-api). Accept re-reading "fe-core seam" as "fe-connector-api helper," leaving **fe-core source flat and iron rule A untouched (0 lines)**? All three reviews recommend this and recommend **striking B2 outright** rather than keeping it as a signed-off fe-core option. Recommendation: accept; strike B2; mc uses B1.
2. **Iceberg statement-scope + cache retrofit timing.** Retrofit iceberg in PR-2/PR-3 now (foundation-first, max convergence, proves parity, wider diff over audited code) — or let hudi be the sole first consumer and converge iceberg later/never (narrower blast radius, forgoes the mandate's convergence)? (Review 1 #2 / Risk R11.)
3. **Config namespace for hudi/mc/es caches.** Framework-native per-entry knobs (`meta.cache.[engine].[entry].*`, recommended) or a single shared per-catalog knob like iceberg's `table.ttl-second`? Affects operator surface and any `CacheSpec.applyCompatibilityMap` back-compat for existing mc/es TTL properties.
4. **Comment cache as a first-class entry?** Iceberg builds `commentCache` only for vended catalogs (plain catalogs serve comments from the table-handle cache). None of hudi/mc/es requested a comment cache — leave "comment" per-connector (recommended) rather than modeling it in the wrapper?
5. **MVCC-pin shape.** Keep the pin as opaque `[V]` over the 4-tuple `ConnectorTableKey` with `-1` sentinels (recommended — iceberg `(snapshotId,schemaId)`, paimon single long, hudi instant, mc/es none all fit one key), rather than a fixed 2-axis value type?
6. **Credential-value hardening: contract vs convention.** Keep the current convention (connector nulls the field + §C construction-site null-gate assertion, now enforced) — the smaller design matching today — or add a type/marker contract that makes the wrapper *refuse* credentialed values? Given the redesigned gate now checks the null-gate at construction (R2 closed), convention is likely sufficient; a contract is extra insurance.
7. **Gate strictness + gateway posture.** Hard build FAIL on zero declarers (recommended) — confirmed. And confirm the hive gateway is **in-gate-scope with a required fail-loud-guard assertion** (this design's position, Review 2 major) rather than documented as out-of-scope.
8. **Maxcompute cross-query shareability.** Confirm ODPS uses a static (non-per-user) identity so a resolved handle could later be promoted cross-query — this round it is statement-scoped only (safe default). Verified: mc declares no `SUPPORTS_USER_SESSION`; owner to confirm the identity model for any future promotion.