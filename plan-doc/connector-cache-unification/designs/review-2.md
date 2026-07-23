Verification complete against HEAD. All load-bearing recon claims confirmed: the octet in `ConnectorPartitionViewCache` (line 70-71) is byte-identical to iceberg's hand-rolled caches (`IcebergTableCache:70`, `IcebergCommentCache:65`); the "NO consumers yet" javadoc (`ConnectorPartitionViewCache.java:34`) is stale — iceberg (2 fields, `IcebergConnector:197,199`), hive (`HiveConnector:112`), paimon (`PaimonConnector:143`) all consume it; `ConnectorStatementScope.computeIfAbsent` is in fe-connector-api; `fe-connector-cache` has ZERO fe-core imports (verified — the two CacheSpec/MetaCacheEntry copies are independent duplicates, so D2 lands entirely outside fe-core); gate hardcodes `TARGET_REL` to IcebergConnector.java; `IcebergConnector:860` declares via `.add(...SUPPORTS_USER_SESSION)`, `HiveConnector:543` and fe-core `PluginDrivenExternalCatalog:1289` use `.contains(...)`; mc/es have no `getCapabilities` override.

Additionally verified the seam-efficacy substrate: `ConnectorSessionBuilder.captureStatementScope()` (line 190-202) reads `sc.getOrCreateConnectorStatementScope()` off the live `ConnectContext`, so every in-statement `buildConnectorSession()` shares ONE scope instance — B1 genuinely collapses the mc fan-out with no fe-core change.

---

# Adversarial design review — fe-core seam (D4) + generic wrapper (D2)

## 1. [major] Strike B2 entirely — do not leave a "sanctioned fe-core exception" on the table

The design proves B1 (memoize inside mc's connector-side `getTableHandle` via the statement scope) collapses the 14–17-site `getTableHandle` fan-out. I verified the precondition B1 depends on: all in-statement `buildConnectorSession()` calls resolve to the *same* `ConnectorStatementScopeImpl` (`ConnectorSessionBuilder.java:202`), so B1's memo actually fires. That means **B2 delivers nothing B1 doesn't** — it only adds ~3 lines to the fe-core `resolveConnectorTableHandle` method (`PluginDrivenExternalTable.java:116-119`).

Under Iron Rule A, an exception that buys zero capability over a connector-side alternative is not "minimal," it's *avoidable*. Keeping B2 described as "the smallest possible reading of the sanctioned exception" is exactly how an avoidable fe-core mutation gets merged: a later executor sees a signed-off option and takes it. **Fix:** delete B2 from the plan; state affirmatively that the D4 mandate is satisfied with **0 fe-core source lines** and iron rule A is literally untouched. The owner's D4 sign-off is honored by the fe-connector-api helper alone (or by objection #2's private-helper alternative).

## 2. [major] The fe-connector-api helper is a Rule-2 net-add that the 2 real consumers don't structurally require

`ConnectorStatementScopes.resolveInStatement` is offered as "no new fe-core source" — true, fe-connector-api ≠ fe-core, so **iron rule A is not breached**. But the mandate asks whether the SPI surface is truly minimal, and this adds a new public type to the SPI module to serve exactly two consumers, one of which (iceberg) *already has a working private helper* (`IcebergStatementScope.sharedTable`, 9 lines, `IcebergStatementScope.java:59`). Hudi can mirror it as a connector-private `HudiStatementScope.sharedMetaClient` — zero shared surface, each connector owns its own key convention, which the design itself concedes must be connector-distinct (the `keyNamespace` param is *required* precisely because the convention can't be shared). The helper deduplicates only ~4 lines (null-guard + `catalogId:db:table:queryId` assembly).

Counter-argument I'll grant honestly: that assembled key is **security-critical** — dropping `queryId` leaks across statements, dropping `catalogId` collides across a cross-catalog MERGE. Centralizing it once is a legitimate correctness win over two connectors re-deriving it and one getting it wrong. So this is a real tension, not a clean cut.

**Fix (if centralized):** keep it a bare static util (no new interface, no new method on `ConnectorSession`) — the design already does this — but **do NOT retrofit iceberg in the same PR (PR-2)**. Retrofitting working, audited iceberg code to delegate widens the blast radius over previously-signed-off code for no functional gain. Let hudi be the sole first consumer; converge iceberg later or never. If the owner prefers absolute minimum SPI surface, decline the helper and give hudi its own private mirror.

## 3. [minor] The compat subclass `ConnectorPartitionViewCache extends ConnectorMetadataCache` is speculative preservation

The design keeps `ConnectorPartitionViewCache<V>` as a thin `entryName="partition_view"` subclass "so its three consumers stay untouched byte-for-byte." But those three consumers (iceberg/hive/paimon) are *already* being rewritten in PR-1 for the `PartitionViewCacheKey → ConnectorTableKey` rename. Since they're touched anyway, have them call `ConnectorMetadataCache<V>("...","partition_view", props)` directly and **delete `ConnectorPartitionViewCache`** — one fewer type, no inheritance layer retained for a back-compat nobody external depends on (it's a connector-side toolkit class, not an SPI contract). Rule 2: don't keep an abstraction to avoid a rename you're already doing.

## 4. [minor] "Byte-for-byte identical" iceberg retrofit overstates `invalidateDb` equivalence

`IcebergTableCache.invalidateDb`/`IcebergCommentCache.invalidateDb` use `id.namespace().equals(Namespace.of(dbName))` on a `TableIdentifier` (`IcebergTableCache.java:101-104`). Rekeying to `ConnectorTableKey(db,table,-1,-1)` swaps this to string `matchesDb(db)`. These are equivalent *only* because Doris iceberg namespaces are single-level `[db]`; a multi-level namespace would diverge. Soften the retrofit claim from "byte-identical" to "functionally equivalent for Doris's single-level db namespace," and gate explicitly on the existing `invalidateDb` tests rather than asserting invariance by inspection. (Connector-side, not an iron-rule issue.)

## 5. [nit] mc "14–17 → 1" over-claims: `buildCrossStatementSession` callers don't share the memo

Several `resolveConnectorTableHandle` callers use `buildCrossStatementSession()` (`PluginDrivenExternalTable.java:460,1036,1071`; `PluginDrivenExternalCatalog.java:297,313`), which by design bind a non-per-statement scope. Those won't share B1's per-statement memo (nor should they — they're refresh/cross-statement paths). The fan-out collapse applies to the `buildConnectorSession()` in-statement subset only. State this so the win isn't over-claimed and so no one "fixes" the cross-statement paths into the memo.

---

## Positive verifications (not objections)
- **D2 does not touch fe-core.** The wrapper lands in `fe-connector-cache`, which has zero fe-core imports; the fe-core `datasource/metacache/` copy is a separate duplicate the design never touches. Iron rule A is clean for the entire cache-wrapper track.
- **The generic wrapper is justified by concrete consumers, not hypotheticals.** 6 real consumers (iceberg ×5 + hudi ×1). The opaque `[V]` and the 4-tuple key with `-1` sentinels are driven by the actual value/key shapes at HEAD (raw `Table`, `String`, `CachedSnapshot`, `List<...>`; `TableIdentifier` vs `(id,snapshotId)`). The two-ctor split is backed by iceberg's real shared-knob pattern (`meta.cache.iceberg.table.ttl-second`, `IcebergTableCache.java:44`). No speculative `[K]` param, identity-sharding, or credential abstraction is added. Rule 2 satisfied.
- **The credential-null-gate stays connector-side.** Verified iceberg's session-user-disabled fields (`IcebergConnector.java:197,199,279-284`); the wrapper stays value-opaque and credential-agnostic. Iron rule D discipline is preserved by construction.

---

**Overall verdict:** No blockers. The design, under its *recommended* path (fe-connector-api helper + B1), already respects iron rule A — it adds **0 fe-core source lines** and the D2 wrapper never touches fe-core. The two avoidable-byte risks are (a) B2 being left as a live fe-core option that should be struck outright (#1), and (b) the SPI helper + in-PR iceberg retrofit being wider than the 2 consumers strictly need (#2). Both are "minimize the surface," not "the surface is wrong." Adopt: strike B2, keep the helper connector-private OR at minimum defer the iceberg retrofit, and delete the compat subclass.