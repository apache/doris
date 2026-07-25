I have enough grounding. Writing the adversarial review.

---

# Adversarial design review — cache-unification (AUTHZ + FRESHNESS lens)

Verification anchored at HEAD. The recon's structural claims hold: `ConnectorPartitionViewCache<V>` is the octet the design describes (fe-connector-cache/…/ConnectorPartitionViewCache.java:46-110), it is live in iceberg/hive/paimon, the "NO consumers yet" javadoc is stale (same file:33-34), the gate hardcodes `TARGET_REL` to IcebergConnector.java (tools/check-authz-cache-sharding.sh:52-54), and hudi/mc/es declare no `SUPPORTS_USER_SESSION`. But three cache-safety objections survive, and the most severe one attacks the exact question in my mandate.

## [BLOCKER] D3's generalized gate cannot see the generic wrapper's real instantiation sites — a future per-user connector that builds its `ConnectorMetadataCache` outside `*Connector.java` gets zero coverage

**Issue.** The gate scans **field declarations on the owner `*Connector.java` file only** (script:52-54, `FIELD_DECL` at :80, and §C.2 enshrines "Scan the OWNER file only … never the whole module" as an invariant). But the generic wrapper is already declared in **two** places per connector — the Connector (marked) *and* the `*ConnectorMetadata` (unmarked, injected):
- iceberg: IcebergConnector.java:197-200 (marked) vs IcebergConnectorMetadata.java:170-171 (unmarked)
- hive: HiveConnector.java:112 vs HiveConnectorMetadata.java:241
- paimon: PaimonConnector.java:143 vs PaimonConnectorMetadata.java:108
- maxcompute: MaxComputeDorisConnector.java:61 vs MaxComputeConnectorMetadata.java:79

Today this is safe only because every connector *also* constructs and holds the instance on the Connector, where the marker sits. The gate's guarantee is therefore "every `*Cache` field textually present in `*Connector.java` of a declaring connector is marked" — **not** "every cross-query cache instance reachable by a declaring connector is isolated."

**Reasoning.** Generalization actively worsens this. Today's `Iceberg*Cache` are dedicated classes the author naturally constructs in the Connector. A generic, injectable `ConnectorMetadataCache<V>` is trivially constructed *anywhere* — a metadata object, a `*ScanPlanProvider`, a factory. A future connector that declares `SUPPORTS_USER_SESSION` and does `new ConnectorMetadataCache<>(...)` inside its `*ConnectorMetadata` (never touching `*Connector.java`, exactly as the injected field already lives there today) produces a name-keyed cross-query cache the gate structurally cannot see. That is precisely the "future per-user connector silently gets a name-keyed cross-query cache the gate fails to catch" failure I was told to hunt. The design bakes the hole in by declaring owner-file-only an invariant.

**Fix.** Gate on the **construction expression**, location-independently: within any connector module whose `*Connector.java` declares `SUPPORTS_USER_SESSION`, grep module-wide for `new ConnectorMetadataCache<` / `new ConnectorPartitionViewCache<` (and the residual `new *Cache(` holders) and require each such construction site to carry a marker or sit under a capability null-gate. Keep the owner-file field-decl scan as a secondary net. A construction-site gate is inherently immune to where the field is *declared*.

## [MAJOR] The marker is an unverified, copy-pasteable comment; the opaque wrapper moves the only real safety (the null-gate) into a construction idiom the gate never checks

**Issue.** Safety is entirely convention: the connector writes `isUserSessionEnabled() ? null : new ConnectorPartitionViewCache<>(...)` (IcebergConnector.java:279-284) and the wrapper is value-opaque (ConnectorPartitionViewCache.java:46, "holding an opaque value V"). The gate only checks that the *marker string* is present near the field decl — it never checks that construction is actually null-gated.

**Reasoning.** With a generic wrapper, PR-1/PR-4/PR-5 will copy both the field+marker line *and* the ternary idiom into new connectors. A mis-paste that keeps the marker but drops the `isUserSessionEnabled() ? null :` guard (or guards on the wrong flag) is a silent cross-user leak that passes the build GREEN — the marker asserts a discipline the code no longer honors. This is a pre-existing weakness, but generalization multiplies the copy-paste surface and detaches the marker (on the field) from the guard (at construction), making drift easier.

**Fix.** Tie the gate to the construction guard, not just the field marker: for a `authz-cache-session-user-disabled` field, assert its assignment RHS is a capability-gated ternary (or the field is provably null under the capability). Add a RED self-test fixture: field marked disabled, constructed unconditionally → must FAIL. Without this, D3 verifies a comment, not a behavior.

## [MAJOR] Stage-2's `.contains(` exclusion drops HiveConnector, which already holds an unmarked, unconditional, name-keyed cross-query cache AND is the gateway delegating to per-user iceberg/hudi

**Issue.** §C.2 excludes lines with `.contains(`, so HiveConnector (whose only `SUPPORTS_USER_SESSION` reference is `sibling.getCapabilities().contains(...)` at HiveConnector.java:543) is classified a non-declarer and never scanned. Yet HiveConnector.java:112/134 holds `partitionViewCache`, an **unmarked, unconditionally-constructed, (db,table,-1,-1)-keyed cross-query cache** (comment at :108-111 says hive has "NO session=user … cache-disabling convention … constructed unconditionally"). And hive is the HMS gateway that delegates to per-user iceberg/hudi siblings.

**Reasoning.** Today this is safe by a *runtime* guarantee, not the gate: the front door "never declares SUPPORTS_USER_SESSION" (comment :537) and fail-louds if a sibling does (:543-546). The design's residual #7 frames hive as merely "in-gate-scope or documented out." That undersells it: hive has a *live* cross-query cache that the generalized gate is *structurally blind to* by construction (the `.contains(` exclusion), so the entire safety of the gateway path rests on one runtime assertion the gate does not verify. If a future refactor makes the hive front door per-user (or the fail-loud assert is weakened), the gate stays GREEN while hive's `partitionViewCache` leaks.

**Fix.** Treat a connector that *reads* a sibling's `SUPPORTS_USER_SESSION` (the gateway signature) as in-scope, and gate the presence of the fail-loud front-door assertion. At minimum, require hive's `partitionViewCache` to carry an explicit `authz-cache-exempt` marker with the "front door never per-user + fail-loud guard" justification, so the exemption is a reviewed claim rather than an invisible gap.

## [MINOR] Hudi `(table, instant)` key: correctness needs "latest **completed** instant, re-read fresh per statement" — the design asserts correctness without pinning either

**Issue.** §D.1 keys the cross-query partition cache by `(db, table, instant-as-snapshotId, -1)` and claims "always-correct (new instant ⇒ new key)." But correctness depends on two unstated properties: (a) the instant is the latest **completed** instant (not a requested/inflight one), and (b) it is re-derived **fresh each statement** from the per-statement metaClient that reloads the active timeline.

**Reasoning.** If the key uses an inflight/requested instant, a concurrent writer can bind the entry to a partial partition set. If the instant is sourced from anything longer-lived than the per-statement metaClient, a stale instant can serve a stale partition list while still labeled "latest." The instant-in-key argument is only as strong as the freshness of the instant that forms it.

**Fix.** Specify: the key uses `metaClient.getActiveTimeline().filterCompletedInstants().lastInstant()` (or equivalent), re-derived from the statement-scoped metaClient each statement. Add an e2e that lists partitions across a concurrent hudi commit and asserts no partial/stale set (this also satisfies the memory rule that HMS-delegated capabilities need e2e).

## [MINOR] Enforce, don't just intend, the hudi/es freshness boundaries

**Issue/Reasoning.** Two intent-level safeties need a test to be real: (1) es — the memoized `EsMetadataState` must hold only mapping/field-context and `fetchShards()` must re-run every statement (es has **no** `*Cache` field at HEAD — grep empty — so there is nothing to regress *yet*, but PR-6 introduces the memo); (2) hudi — the raw metaClient must stay in the per-statement seam and must never be promoted into the cross-query `ConnectorMetadataCache` layer, and the `closeAll` AutoCloseable caveat (§B.3 / Risks) must be checked so a statement-end close cannot corrupt the derived `(table,instant)` list (which stores `List<HudiPartition>`, not the client — so OK, but assert it).

**Fix.** Add assertions/tests: es shard routing re-runs per statement; the hudi cross-query cache value type is a pure projection with no live metaClient/FileSystem reference. No design change, but make these gate-tested rather than convention.

## [NIT] Rename churn + stale javadoc

`PartitionViewCacheKey → ConnectorTableKey` re-touches iceberg/hive/paimon main+test imports (mechanical, low risk). Fix the confirmed-stale "this class has NO consumers yet" javadoc (ConnectorPartitionViewCache.java:33-34) in PR-1 as the design already notes.

---

**Overall verdict: DO NOT SHIP D3 AS DESIGNED.** The A/B/D/E cache *placements* survive the authz+freshness lens (all D1 connectors use static creds; hudi's instant-in-key is sound once "completed + fresh" is pinned; es shard routing stays per-statement). But the gate generalization (D3) is the weak seam: as specified, it verifies owner-file field markers, while the generic wrapper's real instantiation sites already live unmarked in `*ConnectorMetadata` today and can move anywhere tomorrow — so a future per-user connector can obtain a name-keyed cross-query cache the gate cannot see (BLOCKER), the marker is an unverified comment detached from the actual null-gate (MAJOR), and hive's live unmarked cross-query cache plus its per-user delegation path fall structurally outside the gate (MAJOR). Re-scope D3 to gate cache **construction expressions** module-wide within capability-declaring (and gateway) connectors, and assert the null-gate, before the wrapper is generalized — otherwise the generalization ships a wider attack surface with a narrower gate.