I have enough grounding. Writing the adversarial review.

---

# Adversarial design review — catalog-SPI cache unification (foundation-first)

Lens: behavior parity + Trino portability. I verified the load-bearing recon claims against HEAD; they largely hold (`ConnectorStatementScope.computeIfAbsent` is in fe-connector-api `ConnectorStatementScope.java:45`; `getStatementScope()` defaults to `NONE` at `ConnectorSession.java:142`; `ConnectorPartitionViewCache`/`PartitionViewCacheKey` exist with the 4-tuple and the stale "NO consumers yet" javadoc at `ConnectorPartitionViewCache.java:34`; iceberg wires it at `IcebergConnector.java:281,283`; only `IcebergConnector.java:860` declares `SUPPORTS_USER_SESSION` via `.add`, `HiveConnector.java:543` is a `.contains` guard; the gate hardcodes `TARGET_REL` at `check-authz-cache-sharding.sh:57`). The design is buildable, but several parity/portability claims are overstated. Objections most-severe-first.

---

## 1. [blocker — for the B2 variant] The sanctioned fe-core exception (B2) memoizes `getTableHandle` for EVERY connector, not just maxcompute

**Issue.** §B.4-B2 and PR-5 wrap the *generic* fe-core funnel `PluginDrivenExternalTable.resolveConnectorTableHandle` (fe-core) in `computeIfAbsent`. That funnel is the single site all connectors flow through. Memoizing it changes the call-count contract of `getTableHandle` for jdbc/paimon/trino/hive/iceberg — every connector outside this round's D1 scope and outside its test coverage.

**Reasoning.** The design frames "benefits every connector uniformly" (§B.4-B2) as an advantage, but that is precisely the parity hazard: today `resolveConnectorTableHandle` is called fresh at 14–17 sites, and any connector that relies on that — a remote `exists()` probe used as a liveness/authorization check, a freshness re-read, a side-effecting handle build — silently loses those repeats. You cannot assert parity for connectors you are not testing this round. This also sits at the exact fault line of iron rule A (fe-core grows) with an unbounded blast radius, which is the opposite of "minimal sanctioned exception."

**Fix.** Drop B2. Use **B1 (connector-local memo inside `MaxComputeConnectorMetadata.getTableHandle`)** for maxcompute too — it collapses the same fan-out with zero fe-core growth and zero cross-connector reach. If the owner insists on a fe-core site, scope it so only the maxcompute path is memoized (e.g. gate on the connector already opting in), never the shared funnel unconditionally. The design's own recommendation already leans B1; make it the only option.

---

## 2. [major] Iceberg retrofit is NOT byte-identical: it changes db-invalidation from iceberg `Namespace` equality to plain `String` equality

**Issue.** §A.2/§A.3 claim the retrofit onto `ConnectorTableKey(db,table,…)` is "functionally identical (a `TableIdentifier` is `db.table`)." But four of the five caches key by `TableIdentifier` and invalidate a db via **iceberg Namespace equality**, not string equality:
- `IcebergTableCache.java:102-103`: `Namespace ns = Namespace.of(dbName); entry.invalidateIf(id -> id.namespace().equals(ns))`
- same in `IcebergLatestSnapshotCache.java:108-110`, `IcebergPartitionCache.java:121-123`, and format cache.

`ConnectorTableKey`/`PartitionViewCacheKey` invalidate via `Objects.equals(this.db, db)` (`PartitionViewCacheKey.java:71-73`).

**Reasoning.** For single-level namespaces these coincide, but the retrofit relocates db extraction from "iceberg's `Namespace` object at invalidation time" to "a `String db` frozen into the key at build time." That diverges for any multi-level namespace, and it moves a piece of iceberg-specific identity logic into the generic key construction — exactly the kind of silent semantic drift on already-audited, already-shipped code the mandate says to avoid. "Same cache keys, same call counts" is not established for the invalidateDb path.

**Fix.** Before PR-3, add an explicit parity test that `invalidateDb` on the retrofitted key evicts exactly the same entries as the `Namespace.equals` path (including a multi-level-namespace case, or an assertion that iceberg-on-Doris namespaces are provably always single-level). Keep the db-extraction in the connector's key builder, not in the generic class. Treat PR-3 (§ residual #2) as deferrable if this parity can't be cheaply proven.

---

## 3. [major] The generalized gate silently protects only caches placed on `*Connector.java`; nothing enforces that a `SUPPORTS_USER_SESSION` connector puts its cross-query caches there

**Issue.** §C.2 keeps "scan the OWNER file only (`*Connector.java`)" and treats constructor-injected `*Cache` fields on `*ConnectorMetadata`/`*ScanPlanProvider` as "unmarked by design." Generalizing Stage-1/2 to "any declarer" makes this file-scoping a load-bearing, N-connector invariant with zero enforcement.

**Reasoning.** Today it is safe only by luck: iceberg happens to hold all five caches on `IcebergConnector` (`IcebergConnector.java:169-200`). A future (or even this round's hudi) session=user adopter that holds its `(table,instant)` cache on `HudiConnectorMetadata` or a scan provider would declare the capability, pass the gate (the declaring `*Connector.java` has no unmarked `*Cache` field), and leak. The gate's whole purpose — "added a new cross-query cache and forgot to isolate it becomes a build failure" — is defeated for any cache not physically on the Connector class.

**Fix.** When a connector is in the declarer set, scan its whole module for `*Cache` **holder** fields (final `…Cache` type), not just `*Connector.java`; OR add a companion invariant check that a declaring connector may hold authz-sensitive `*Cache` fields only on the Connector class and fail otherwise. The §C.2 "false-positive on injected reference fields" concern is real, but the fix is to distinguish holder (`new …Cache(`) from injected reference (ctor param) — not to narrow the scan to one file.

---

## 4. [major] The statement-scope seam's correctness rests entirely on `queryId` lifecycle, which the design extends to three new connectors without verifying it

**Issue.** `resolveInStatement` (and iceberg's existing `IcebergStatementScope.sharedTable`, `IcebergStatementScope.java:64`) key by `session.getQueryId()`. The whole cross-statement-isolation and prepared-EXECUTE-reuse safety claim (§B.2, §B "Rule D safety," `ConnectorStatementScopeImpl.java:64-98` idempotent `closeAll`) depends on: (a) `queryId` is stable within one statement across the request thread and all off-thread scan pumps, and (b) `queryId` differs per prepared `EXECUTE` re-execution.

**Reasoning.** Iceberg already depends on this, so it is pre-existing for iceberg — but the design generalizes the dependency to hudi/mc/es and to a shared helper, and asserts "reset per prepared EXECUTE" (§B.3, "reused prepared context sees each execution's own table") as fact without grounding it. If `queryId` is NOT refreshed per EXECUTE while the `StatementContext`/scope IS reset, the seam is safe (scope cleared); if the scope is reused but `queryId` is refreshed, also safe; but if BOTH are reused (scope + queryId), you get cross-execution stale memo — a correctness bug. This is unverified and it is the foundation everything else builds on. Residual #8 asks about mc identity but nobody verified the queryId/scope reset interaction.

**Fix.** Before PR-2, verify against `ExecuteCommand`/`StatementContext` that either the scope is reset OR `queryId` changes on every prepared re-execution (ideally both), and encode it as a test: two EXECUTEs of one prepared statement must NOT share a memoized table. Don't ship the shared helper on an assumed lifecycle.

---

## 5. [major] Hudi is the first `AutoCloseable` connector value in the scope; `closeAll` use-after-close is a hard requirement, not a "caveat"

**Issue.** §B.3/§D.1 route `HoodieTableMetaClient` (and derived `TableSchemaResolver`/`InternalSchema`) through the scope. `ConnectorStatementScopeImpl.closeAll` (`:84-96`) closes every stored `AutoCloseable` at statement end. Until now the only closeable scope value was `ConnectorMetadata`; the shared iceberg `Table` is not closeable, so this path was never exercised for a connector data object.

**Reasoning.** If any hudi version's `HoodieTableMetaClient` is `AutoCloseable` or holds a `HoodieStorage`/`FileSystem`, and any consumer (BE-thrift schema assembly, an off-thread scan pump, a derived schema object that lazily touches the FS) retains it past the point `closeAll` fires, you get use-after-close on the flagship connector. The impl comment claims `closeAll` "runs after the scan off-thread pumps have quiesced," but that ordering was validated for iceberg's non-closeable `Table`, not for a closeable metaClient whose derived objects may outlive the store slot. The design lists this as risk #3 with mitigation "memoize a non-closeable projection" — but leaves it optional.

**Fix.** Make it mandatory for the flagship: memoize a **non-closeable projection** (the resolved schema/timeline snapshot the callers actually need), never the raw closeable client, unless close-at-statement-end is provably safe for that hudi version AND no derived object escapes. Add a test that exercises a scan pump reading the memoized hudi state after nominal statement end.

---

## 6. [major] Hudi's secondary `(table, instant)` cross-query cache (A) conflicts with the freshness claim — it delivers either ~zero cross-query benefit or staleness

**Issue.** §D.1 says `buildMetaClient` "reloads the active timeline for freshness" (so the per-statement metaClient memo is correct), then also proposes a cross-query `(table, instant)` partition cache keyed by `instant-as-snapshotId`.

**Reasoning.** These pull opposite directions. If the latest instant is resolved fresh each statement (the stated freshness goal), then each statement's `(table,instant)` key is a fresh instant with near-zero cross-query hit rate — the cache buys almost nothing. If instead the latest-instant resolution is itself cached cross-query (à la iceberg `latestSnapshotCache`) to get hits, you reintroduce the TTL staleness window the freshness claim was avoiding, and rule E's "new instant ⇒ new key" is vacuous because you're pinning a stale instant. The design asserts both benefits without resolving the tension.

**Fix.** Decide explicitly: either (a) hudi latest-instant is cross-query cached under bounded TTL+REFRESH (accept iceberg-equivalent staleness, real hit rate) — then document the staleness parity; or (b) latest-instant is per-statement fresh — then drop the (A) cross-query cache for hudi this round (per-statement memo already captures the win) rather than shipping a cache that can't hit. Don't claim both.

---

## 7. [minor] Cross-connector `keyNamespace` collision is a runtime `ClassCastException`, mitigated only by hand-assigned strings with no registry

**Issue.** §B.2 makes `keyNamespace` a required param and §Risks notes the collision, but the namespaces are free-form strings (`"iceberg.table"`, `"hudi.metaclient"`, `"maxcompute.handle"`, `"es.metadata_state"`) stored as opaque `Object` in one `ConcurrentHashMap` (`ConnectorStatementScopeImpl.java:44`). A duplicate namespace across two connectors in a heterogeneous gateway statement is caught only at runtime as a `ClassCastException`.

**Reasoning.** The metadata-funnel already treats its key convention (`"metadata:" + catalogId` plus owner label, `ConnectorStatementScope.java:51`) as a reviewed invariant. The new seam adds a parallel, larger key space with no central definition — easy to collide under refactor, invisible until a gateway query hits it.

**Fix.** Put the namespaces in one place (a small constants holder / enum next to `ConnectorStatementScopes` in fe-connector-api) and document uniqueness as a reviewed invariant, mirroring the funnel-key convention. Optionally fold `catalogId` before `keyNamespace` so two connectors under different catalog ids can't collide even with an equal namespace.

---

## 8. [minor] `ConnectorMetadataCache` telemetry/cache names change unless the retrofit is pinned to the old names

**Issue.** Each iceberg cache today has a distinct entry name (`"iceberg-table"`, `"iceberg-partition"`, `"iceberg-latest-snapshot"`, … at `IcebergTableCache.java:70`, `IcebergPartitionCache.java:97`, etc.), surfaced through `MetaCacheEntry.stats()`. The framework-native ctor derives `name = engine + "." + entryName` (§A.2) → `"iceberg.table"`, a different string.

**Reasoning.** Any operator dashboard, log grep, or `*ForTest`/`loadCountForTest` assertion keyed on the old names breaks silently. The design says iceberg uses the pre-resolved `CacheSpec` ctor (which takes `name`), so it *can* keep old names — but that must be stated as a retrofit requirement, not left to the framework-native derivation.

**Fix.** Pin the retrofit to pass the exact legacy `name` strings; add it to the PR-3 checklist and assert names in the existing cache tests so a drift fails.

---

## 9. [nit] "Iron rule A is not actually crossed" is true only if the owner accepts re-reading the mandate; the fe-core B2 form still exists in the doc as a live recommendation

Residual #1 correctly surfaces that the reachable home is fe-connector-api (iceberg/hudi can't depend on fe-core, confirmed: they import `org.apache.doris.connector.api.*`). But the doc simultaneously recommends B2 (fe-core growth) for maxcompute. Pick one posture: if the design's thesis is "rule A stays intact," then B2 should be removed, not recommended — see objection #1. Leaving both makes the "rule A untouched" headline conditional on a decision the doc elsewhere pre-empts.

---

## Overall verdict

**Request changes.** The foundation genuinely mostly exists and the direction is sound, but the design overstates parity in three shipped-code-touching places: the fe-core B2 memo (blocker for that variant — unbounded cross-connector blast radius, use B1), the iceberg db-invalidation semantics change (major — not byte-identical, `Namespace` vs `String` equality), and the gate's unenforced "caches live on `*Connector`" invariant (major — defeats the gate's purpose for any cache held off the Connector class). Two more majors are unverified-foundation risks (queryId/prepared-EXECUTE lifecycle; hudi `AutoCloseable` use-after-close) and one is an internal contradiction (hudi freshness vs the (A) cross-query cache). None are unfixable; all are cheap to close before the respective PRs, and PR-1 (generic wrapper) plus a lifecycle-verified PR-2 can proceed once objections #4 and the #2 parity test are settled.