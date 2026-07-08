# Hudi-on-HMS Delegation Plan (fold Hudi into the HMS-catalog cutover)

Authoritative, code-grounded plan. HEAD = `75670ae4193d76859c5b261a8ea6bce33d046e00` on branch
`catalog-spi-11-hive`. All line numbers below are read fresh from HEAD (the `gitStatus` snapshot in the
task prompt, `d24e4af`, was stale). Planning only — **no code in this document**.

---

## 0. Scope, signed decisions, and the model-mismatch resolution

### 0.1 Scope
Fold Hudi into the single HMS-catalog SPI cutover so that, when `"hms"` enters
`CatalogFactory.SPI_READY_TYPES`, a **hudi-on-HMS** table is served by the SPI stack with **no regression**
versus the legacy `HudiScanNode` path. This plan covers: making `fe-connector-hudi` gateway-ready as a
**sibling** connector, the gateway divert (mirroring iceberg), closing the no-regression capability gaps
(incremental read, time travel, MVCC snapshot, partition enumeration, partition-value fidelity,
schema-at-instant), and the read-only write-reject safety net. It also inventories the flip-time dead-code
deletion and the hard flip-blockers.

### 0.2 The three signed decisions (FIXED — 2026-07-08, do not re-litigate)
1. **Sibling delegation.** Hudi-on-HMS tables are served by **delegating to a hudi SIBLING connector**, exactly
   mirroring how iceberg-on-HMS delegates to an iceberg sibling. The `type=hms` hive plugin is the **gateway**;
   it builds the hudi sibling via `context.createSiblingConnector("hudi", ...)`, per-handle diverts
   scan/metadata to it, and owns its lifecycle. There is **no** standalone `type=hudi` catalog.
2. **No regression.** Hudi incremental read + time travel + MVCC snapshot (all supported by legacy
   `HudiScanNode`) must be **closed before the flip**. Leaving them deferred at the flip would regress hudi
   users and is not acceptable.
3. **Plan first.** This written plan lands before any code.

### 0.3 Model-mismatch resolution (DV-005 — RESOLVED, not blocking)
At HEAD, `fe-connector-hudi` declares `HudiConnectorProvider.getType() == "hudi"`
(`HudiConnectorProvider.java:36-38`) as if hudi were a standalone catalog type. But Doris has **no** standalone
`type=hudi` catalog: `CatalogFactory.SPI_READY_TYPES` (`CatalogFactory.java:49-50` =
`{jdbc, es, trino-connector, max_compute, paimon, iceberg}`) contains neither `hms` nor `hudi`; the factory
switch has a `case "hms"` but **no** `case "hudi"`; there is **no** `HudiExternalCatalog` class. Hudi is
parasitic on HMS (`HMSExternalTable.dlaType == HUDI`).

**Resolution under the sibling model:** `getType() == "hudi"` is **kept** — it is precisely the lookup key that
`context.createSiblingConnector("hudi", ...)` resolves, and `createSiblingConnector` bypasses
`SPI_READY_TYPES` (it goes straight through `DefaultConnectorContext.createSiblingConnector` →
`ConnectorFactory.createConnector`). Therefore:
- **Never** add `"hudi"` to `SPI_READY_TYPES` and **never** add a `case "hudi"` to the factory. Doing so would
  build a standalone `PluginDrivenExternalCatalog` around `HudiConnector` with no fe-core catalog class = the
  exact DV-005 bug re-created.
- The `HudiConnectorProvider` javadoc (`:30`, *"dedicated Hudi catalogs that connect to HMS"*) is now
  **misleading and dangerous** — it invites a future maintainer to "fix" DV-005 by promoting hudi into
  `SPI_READY_TYPES`. It must be corrected to *"sibling-only type string, used only via createSiblingConnector
  by the hive HMS gateway; never a user-facing catalog type; never add to SPI_READY_TYPES"* (see **HD-A0**).

The single flip toggle remains **one line**: add `"hms"` to `SPI_READY_TYPES`. That flip converts hive +
iceberg-on-HMS + hudi-on-HMS **simultaneously** (see §4).

---

## 1. Current state

### 1.1 The hudi sibling connector — what works, what is stubbed (HEAD)

**Real (the protected ~25% anchor):**
- `HudiConnector` (`HudiConnector.java`) implements `getMetadata` (`:60`), no-arg `getScanPlanProvider()`
  (`:65`), `close` (`:103`); builds a `ThriftHmsClient` from raw props via `createClient` (`:80-100`) using
  `context::executeAuthenticated` (`:98`). **No** per-handle overloads, **no** write/procedure/transaction
  surface, **no** `getCapabilities`.
- `HudiConnectorMetadata` implements `listDatabaseNames`, `databaseExists`, `listTableNames`, `getTableHandle`,
  `getColumnHandles`, `applyFilter` (EQ/IN partition pruning → `HudiTableHandle.prunedPartitionPaths`),
  `getTableSchema` (`:199`), `getProperties`. COW/MOR detection via `detectHudiTableType` (substring match).
  It overrides **none** of `{resolveTimeTravel, applySnapshot, beginQuerySnapshot, getMvccPartitionView,
  listPartitions, listPartitionNames, listPartitionValues, getTableFreshness, getPartitionFreshnessMillis,
  buildTableDescriptor}` — all inherit SPI no-op defaults.
- `HudiScanPlanProvider` (`HudiScanPlanProvider.java`): snapshot-only split planning. `isCow =
  "COPY_ON_WRITE".equals(...)` (`:92`); COW native via `fsView.getLatestBaseFilesBeforeOrOn(partition,
  queryInstant)` (`collectCowSplits :204-208`); MOR via `fsView.getLatestMergedFileSlicesBeforeOrOn(...)`
  (`collectMorSplits :232-237`) with `useNative = logs.isEmpty() && !filePath.isEmpty()` (`:249`) else a JNI
  split (`HudiScanRange` → `THudiFileDesc`). `getScanNodeProperties` emits `file_format_type` /
  `table_format_type` + `location.*` passthrough.

**Stubbed / deferred (the no-regression gaps):**
- `HudiScanPlanProvider.planScan` **always** reads `timeline.lastInstant()` (`:103-108`) and ignores any pin →
  time travel and incremental read are not honored.
- No `resolveTimeTravel` / `applySnapshot` / `beginQuerySnapshot` → time-travel / @incr / MVCC-pin all inherit
  empty defaults.
- No `listPartitions*` / freshness → a partitioned hudi table's MVCC view is empty (DV-007).
- No `schema_id` / `history_schema_info` / field-ids → schema-evolution degrades to BE name-matching (DV-006).
- **Partition-value parse infidelity (newly surfaced, not in prior batch lists):**
  `HudiScanPlanProvider.parsePartitionValues` (`:317-334`) only handles hive-style `k=v` fragments — a
  fragment **without** `=` is **silently dropped**, and it never URL-unescapes. Legacy
  `HudiPartitionUtils.parsePartitionValues` (`:63-89`) maps **positionally** when a fragment lacks `k=`, has a
  single-column-whole-path fallback, and URL-unescapes every value via `FileUtils.unescapePathName`.
  Consequence: for Hudi's **default** non-hive-style partitioning (`hive_style_partitioning=false`, paths like
  `2024/01`), the connector builds an **empty** partition-value map → BE returns **NULL** partition columns on
  a plain snapshot read; escaped chars (`%20`, `%2F`) arrive un-unescaped. This bites the **most basic**
  unpruned read (where `resolvePartitions` falls back to `getAllPartitionPaths`, which returns positional
  paths). **This is a snapshot-path correctness regression on the "safe anchor" and must be fixed** (see
  **HD-A3**).

### 1.2 Legacy functionality to reproduce (the no-regression bar)
From `datasource/hudi/**` + `PhysicalPlanTranslator.visitPhysicalHudiScan` + `HudiScanNode`:
- **Time travel** `FOR TIME AS OF`: normalize `value.replaceAll("[-: ]", "")` to an instant, resolve on the
  completed timeline; **reject** `FOR VERSION AS OF` with *"Hudi does not support FOR VERSION AS OF, please use
  FOR TIME AS OF"* (`HudiScanNode` `:206-220`).
- **Incremental read** `@incr(begin,end)`: `LogicalHudiScan.withScanParams` builds
  COW/MOR/EmptyIncrementalRelation from `hoodie.datasource.read.begin/end.instanttime`; `getIncrementalSplits`
  uses `collectSplits()` (COW) / `collectFileSlices()` (MOR); `getLocationProperties` ships
  `incrementalRelation.getHoodieParams()` to BE. **RO-as-RT quirk**: a COW `_ro` table with
  `hoodie.query.as.ro.table=true` is read as MOR for incremental. `CheckPolicy` injects a **row-level**
  predicate `_hoodie_commit_time > begin AND <= end` (gated on `LogicalHudiScan && HMSExternalTable`).
- **MVCC snapshot / freshness**: `HudiMvccSnapshot` wraps `TablePartitionValues + long timestamp`;
  freshness/last-commit instant drives MTMV.
- **Partition enumeration**: `HudiScanNode.getPrunedPartitions` used fe-core `selectedPartitions` (+ dummy
  partition for unpartitioned); runtime-filter partition prune via `getPartitionInfoMap`.
- **Schema evolution**: per-split `THudiFileDesc.schema_id` from `InternalSchema` + `addToHistorySchemaInfo`
  (`HudiUtils.getSchemaInfo`, nested names lowercased).
- **BE descriptor**: legacy hudi rides `TTableType.HIVE_TABLE` / `THiveTable` (`HudiScanNode extends
  HiveScanNode`), with storage props merging BE-format (`AWS_*`) + Hadoop-format (`fs.*`).
- **Read-only**: Doris never writes hudi; INSERT/DELETE/MERGE/EXECUTE are rejected.

### 1.3 The iceberg sibling template (what to mirror)
- **One sibling holder**: `HiveConnector.java` `:65` `ICEBERG_CONNECTOR_TYPE="iceberg"`, `:87` `volatile
  Connector icebergSibling`, `:237-252` `getOrCreateIcebergSibling()` =
  `context.createSiblingConnector(ICEBERG_CONNECTOR_TYPE, IcebergSiblingProperties.synthesize(properties))`,
  `:362-374` `close()` forwards to it.
- **Property synthesis**: `IcebergSiblingProperties.synthesize` copies the gateway prop map verbatim and
  injects the literal `iceberg.catalog.type=hms` flavor.
- **getTableHandle divert**: `HiveConnectorMetadata.getTableHandle` `:256-257` — `if (tableType ==
  HiveTableType.ICEBERG) return siblingMetadata(session).getTableHandle(...)`, returning the **raw** foreign
  iceberg handle (never cast/stamped). HUDI is **explicitly not diverted** (`:254-255` comment).
- **Per-handle guard-and-forward**: every gateway consumer discriminates by the gateway's OWN hive-loader type,
  `if (!(handle instanceof HiveTableHandle)) return siblingMetadata(session)...` — ~30 sites in
  `HiveConnectorMetadata` (`:295, :353, :546, :592, :647, :797, :853, :876, :943, :969, :1010, :1045, :1055,
  :1065, :1075, :1085, :1095, :1104, :1114, :1308, :1332, :1355`, …), all resolving through the single helper
  `siblingMetadata(session)` (`:209-210`, `icebergSiblingSupplier.get().getMetadata(session)`). At the
  connector level: `getScanPlanProvider(handle)` (`:120-124`), `getWritePlanProvider(handle)` (`:142-146`),
  `getProcedureOps(handle)` (`:160-164`) all do `handle instanceof HiveTableHandle ? this : icebergSibling`.
- **Detection**: `HiveTableFormatDetector.detect` already returns `HiveTableType.HUDI` (input-format set or
  `flink.connector=hudi`); the `HUDI` enum constant exists.

**The template's one unsolved-for delta:** it assumes **exactly one** sibling. `ConnectorTableHandle` is a
bare marker — `interface ConnectorTableHandle extends Serializable {}` (zero methods, *"Opaque table handle.
Connector implementations define their own subclasses."*). With a **second** sibling, the binary
`else → iceberg` fallthrough is wrong (a hudi handle would go to the iceberg sibling), and neither foreign
handle can be `instanceof`'d across the loader split. This is the single genuinely-new design problem (§3B).

---

## 2. PhysicalHudiScan routing analysis (the connector-agnostic answer)

**Question:** after the flip, how does a hudi-on-HMS table reach a hudi-aware scan carrying
incremental/snapshot params, given the "KEY WRINKLE" that hudi uses a distinct `PhysicalHudiScan` node?

**Answer (HEAD-verified — the wrinkle dissolves; the flipped table rides the GENERIC path):**

1. **Binding.** `BindRelation` builds `LogicalHudiScan` **only** for `case HMS_EXTERNAL_TABLE` gated on
   `hmsTable.getDlaType() == DLAType.HUDI` (`BindRelation.java:603-609`) — i.e. only for a **legacy**
   `HMSExternalTable`. The `case PLUGIN_EXTERNAL_TABLE` arm (`:623, :654-658`) **unconditionally** returns a
   plain `LogicalFileScan`, carrying `unboundRelation.getScanParams()` (so `@incr`/AS-OF params are **not lost**
   at the plan-tree level — resolution just moves connector-side).
2. **Post-flip table class.** When `"hms"` enters `SPI_READY_TYPES`, an hms catalog becomes a
   `PluginDrivenExternalCatalog` and its hudi table is a `PluginDrivenMvccExternalTable` (the gateway declares
   `SUPPORTS_MVCC_SNAPSHOT` catalog-wide) — `getType() == PLUGIN_EXTERNAL_TABLE`. So it binds to
   `LogicalFileScan`, **never** `LogicalHudiScan`.
3. **Translation.** `LogicalFileScan → PhysicalFileScan → PhysicalPlanTranslator.visitPhysicalFileScan`
   (`:803`), whose `table instanceof PluginDrivenExternalTable` branch (`:814-822`) routes to the **generic**
   `PluginDrivenScanNode` (identical path to iceberg/paimon). `visitPhysicalFileScan` also threads
   `getTableSnapshot()`/`getScanParams()` onto the node (`:865-866`), which
   `PluginDrivenScanNode.pinMvccSnapshot` → `MvccUtil` resolves via the connector.
4. **The `visitPhysicalHudiScan` fail-loud is DEAD for the flipped path.** Its `PluginDrivenExternalTable`
   branch with the incremental throw (`:909-910`) and the time-travel throw (`:913-914`) is **unreachable** —
   nothing ever binds a `PluginDrivenExternalTable` to a `LogicalHudiScan`. **Correction to the task framing:**
   `visitPhysicalHudiScan:901-919` is *not* the real flip control point; deleting those throws is a no-op for
   the flipped path. The real no-regression channel is the generic MVCC seam below.

**The connector-agnostic seam for incremental + time travel (already built, connector-agnostic, used by
paimon today):** `PluginDrivenMvccExternalTable.loadSnapshot` (`:319`) → `toTimeTravelSpec` (`:328`, mapping
`@incr` → `ConnectorTimeTravelSpec.incremental(params.getMapParams())` at `:416-417`, FOR TIME/VERSION AS OF
otherwise) → `metadata.resolveTimeTravel(session, handle, spec)` (`:347`). An **empty** resolve throws a
kind-specific user error `notFoundMessage(spec)` (`:349`) — i.e. today an `@incr`/AS-OF query on hudi would
**fail loud** (a regression, since legacy served it), not silently read latest. The resolved snapshot is
pinned onto the handle via `metadata.applySnapshot` (`:375`) before `planScan`. **Paimon fully implements
this** (`PaimonConnectorMetadata.resolveTimeTravel :498` with the `INCREMENTAL` case `:552`, `beginQuerySnapshot
:432`) — it is the exact template for hudi.

**Consequence for the plan:** do **not** try to route a plugin hudi table to `LogicalHudiScan` (that requires a
source-specific `detect==HUDI` arm in `BindRelation` = iron-rule violation, and drags `org.apache.hudi.*` +
`HMSExternalTable` casts into fe-core). Serve hudi-on-HMS through the generic `PluginDrivenScanNode`. Re-home
incremental + time travel **connector-side** via `resolveTimeTravel`/`applySnapshot`/a pin-honoring
`planScan`. Delete the bespoke `LogicalHudiScan`/`PhysicalHudiScan`/`HudiScanNode` + the 4 fe-core
`*IncrementalRelation` classes at flip-time (§4), after porting their logic into `fe-connector-hudi`.

**No new fe-core SPI is required** for incremental/time-travel — `ConnectorTimeTravelSpec.Kind.INCREMENTAL`
and the whole `loadSnapshot` dispatch already exist. The lift is entirely connector implementation.

---

## 3. Ordered sub-steps

Legend for each step: **Lands** (what) · **Mode** (dormant-buildable / dormant-verifiable / flip-or-delete) ·
**Touch points** · **Deps** · **Risk** · **Proving test**.

"dormant-buildable" = compiles and lands behind the closed gate but has **no live path** to prove correctness
pre-flip. "dormant-verifiable" = additionally provable by a same-loader unit test. Nothing in Groups A–D
touches `SPI_READY_TYPES`.

### Group A — Make the hudi connector gateway-ready as a sibling

#### HD-A0 — DV-005 guardrail (doc-only, zero risk)
- **Lands:** correct `HudiConnectorProvider` / `HudiConnector` javadocs to *"sibling-only type string via
  createSiblingConnector; never a user-facing catalog type; never add to SPI_READY_TYPES"*. Add a guard comment
  at `CatalogFactory.java:50` next to `SPI_READY_TYPES`.
- **Mode:** dormant-verifiable (doc). **Touch points:** connector (`HudiConnectorProvider.java:26-38`), fe-core
  comment (`CatalogFactory.java:49-50`) — comment only, no logic. **Deps:** none. **Risk:** none. **Proving
  test:** n/a (review).

#### HD-A1 — Hudi sibling holder + property synthesis (mirror iceberg S1/S2)
- **Lands:** `HudiSiblingProperties.synthesize(gatewayProps)` returning a **verbatim defensive copy** (hudi
  needs **no** flavor key — there is no `iceberg.catalog.type` analogue; `HudiConnector.createClient` reads
  `hive.metastore.uris`/`uri` + raw `hadoop.*`/`fs.*`/`dfs.*`/`hive.*`/`s3.*` directly). Add
  `HiveConnector.hudiSibling` (`volatile Connector`) + `getOrCreateHudiSibling()` =
  `context.createSiblingConnector("hudi", HudiSiblingProperties.synthesize(properties))` (fail-loud when null:
  *"hudi connector plugin not available"*); extend `close()` to forward to it.
- **Mode:** dormant-buildable. **Touch points:** connector-hive (`HiveConnector.java` new field + method mirror
  of `:237-252`, `close` mirror of `:362-374`), new `HudiSiblingProperties` in connector-hive (do **not** import
  hudi-loader constants — hardcode any literal keys, as `IcebergSiblingProperties` does). **Deps:** none.
  **Risk:** low — the sibling is lazy and unreferenced until HD-B2. **Proving test:** unit — synthesize returns
  a defensive copy equal to input; `getOrCreateHudiSibling` memoizes one instance and `close` forwards (use a
  same-loader fake resolved through a stubbed `createSiblingConnector`).
- **Note:** the hudi plugin zip must ship to `connector_plugin_root` and **must not** be co-packaged into the
  hive zip (second AWS SDK poisons S3 — same rule as iceberg).

#### HD-A2 — Kerberos plugin-auth for the hudi sibling (verify / close)
- **Lands:** ensure the hudi sibling gets the same plugin-side UGI `doAs` treatment the hive gateway built,
  rather than a plain `context::executeAuthenticated` (`HudiConnector.java:98`) which may **downgrade** a
  Kerberized HMS post-flip. Either share the gateway's plugin authenticator or mirror
  `HiveConnector.buildPluginAuthenticator`.
- **Mode:** dormant-buildable. **Touch points:** connector-hudi (`HudiConnector.createClient`). **Deps:** none.
  **Risk:** medium — a Kerberized hudi-on-HMS regresses at flip if unaddressed; **secondary** (only affects
  Kerberos deployments). **Proving test:** flip-time e2e against a Kerberized HMS (no same-loader proof).

#### HD-A3 — Partition-value parse fidelity (snapshot-path no-regression FIX)
- **Lands:** replace `HudiScanPlanProvider.parsePartitionValues` (`:317-334`) with logic equivalent to legacy
  `HudiPartitionUtils.parsePartitionValues` (`:63-89`): positional mapping when a fragment lacks `k=`,
  single-column-whole-path fallback, and `FileUtils.unescapePathName` on every value. Also resolve the
  **partition-source consistency** gap: `applyFilter` prunes over HMS `listPartitionNames` (hive-style
  `k=v/...`) while the unpruned `resolvePartitions` fallback (`:299-307`) lists over Hudi metadata
  `getAllPartitionPaths` (positional) — the two return **different-shaped** identifiers. Pick **one**
  authoritative partition source keyed on `useHiveSyncPartition` (as legacy did) so the pruned name-shape
  equals the `fsView.getLatest*(partitionPath)` relative-path shape.
- **Mode:** dormant-buildable (correctness only provable on BE). **Touch points:** connector-hudi
  (`HudiScanPlanProvider.parsePartitionValues`, `resolvePartitions`; port the `HudiPartitionUtils`
  positional/unescape logic into the connector). **Deps:** none (independent of HD-A1/B). **Risk:** **HIGH** —
  this is a silent NULL-partition-column / wrong-value regression on the **most basic** non-hive-style
  partitioned snapshot read; it is *not* covered by any prior batch step. **Proving test:** flip-time e2e over a
  `hive_style_partitioning=false` table (path `2024/01`) and a table with an escaped partition value (space,
  `/`), asserting non-NULL, correctly-unescaped partition columns vs legacy `HudiScanNode`.

#### HD-A4 — Force-JNI session flag + COW/MOR detection hardening
- **Lands:** thread the `force_jni` session flag through `ConnectorSession` into `planScan`'s `useNative`
  decision **in both directions**: (a) COW-always-native must yield to force-JNI (legacy `canUseNativeReader`);
  (b) the MOR no-delta-log → native **downgrade** in `HudiScanRange.populateRangeParams` must **not** fire when
  force-JNI is set (legacy guards it with `!isForceJniScanner()` — `force_jni` is a correctness escape hatch).
  Harden `detectHudiTableType` so an `UNKNOWN` result cannot silently pick the MOR/JNI path for a COW table.
- **Mode:** dormant-buildable. **Touch points:** connector-hudi (`HudiScanPlanProvider.planScan`,
  `HudiScanRange.populateRangeParams`, `HudiConnectorMetadata.detectHudiTableType`). **Deps:** none. **Risk:**
  medium — the `planScan.useNative` decision and the `populateRangeParams` downgrade must stay **consistent** or
  a slice is planned native but described JNI (or vice-versa). **Proving test:** unit for the session-flag
  plumbing (same-loader, with a fake session); flip-time e2e with `force_jni=true` on COW and MOR.

#### HD-A5 — BE table descriptor + BE-canonical storage props
- **Lands:** `HudiConnectorMetadata.buildTableDescriptor` → `TTableType.HIVE_TABLE` / `THiveTable` (copy the
  gateway's `HiveConnectorMetadata` output — legacy hudi rides HIVE_TABLE). In
  `HudiScanPlanProvider.getScanNodeProperties`, source BE-facing storage via
  `ConnectorContext.getBackendStorageProperties()` (canonical `AWS_*`) **plus** the Hadoop passthrough
  (`fs.*`) for the JNI reader — mirroring legacy `getLocationProperties`' dual merge — instead of the current
  key-prefix copy, so private-bucket credentials reach BE.
- **Mode:** dormant-buildable. **Touch points:** connector-hudi (`HudiConnectorMetadata.buildTableDescriptor`
  new override; `HudiScanPlanProvider.getScanNodeProperties`). **Deps:** none. **Risk:** medium — a generic
  `SCHEMA_TABLE` descriptor is likely wrong BE-side; the native COW reader needs `AWS_*`, the JNI MOR reader
  needs `fs.*`. Note the **be-java-ext shared classpath** rule: verify the hudi plugin zip carries what the JNI
  reader needs (commons-lang / transitive deps) or BE `NoClassDefFound`. **Proving test:** flip-time e2e over an
  S3-backed private-bucket hudi table (COW native + MOR JNI).
- **Simplification vs iceberg:** the **name-based** `buildTableDescriptor` path needs **no** hudi divert (unlike
  iceberg's ICEBERG_TABLE divert) — legacy hudi's descriptor is identical to hive's. Only the **handle-based**
  override here is needed. Do not add a redundant name-based hudi divert.

### Group B — Gateway divert (mirror iceberg + the ONE new design decision)

#### HD-B1 — Three-way foreign-handle routing (SIGNED 2026-07-08: **Option 1 — `Connector.ownsHandle(handle)` neutral SPI**; the other option below is retained only for context, do not re-litigate)
- **Problem:** with two siblings, the binary `if (handle instanceof HiveTableHandle) hive else iceberg` breaks
  — a `HudiTableHandle` (raw hudi-loader type, cast-invisible across the loader split) would be wrong-routed to
  the iceberg sibling → cross-loader `ClassCastException` or silently-wrong answers. `ConnectorTableHandle`
  carries no format tag (verified: zero methods). This affects **every** per-handle site: the ~30
  `siblingMetadata(session)` forwards in `HiveConnectorMetadata` and the connector-level
  `getScanPlanProvider/getWritePlanProvider/getProcedureOps(handle)` (+ any `beginTransaction(handle)`).
- **Two mechanisms (must pick one — this is the flip-blocking decision the plan surfaces for explicit
  sign-off):**
  - **Option 1 — `ownsHandle` SPI predicate (RECOMMENDED).** Add a neutral parent-first default to the
    `Connector` interface: `default boolean ownsHandle(ConnectorTableHandle h) { return false; }`, overridden by
    each sibling with its **own** in-loader `instanceof` (`IcebergConnector`: `h instanceof
    IcebergTableHandle`; `HudiConnector`: `h instanceof HudiTableHandle`). Gateway routes:
    `hive-handle → hive; else icebergSibling.ownsHandle(h) → iceberg; else hudiSibling.ownsHandle(h) → hudi;
    else fail-loud`. fe-core **never** calls `ownsHandle` (stays format-agnostic).
    - *Cost:* one new parent-first SPI default method.
    - *Benefit:* the routing **logic** is **same-loader unit-testable** — each fake overrides `ownsHandle` with
      its own class check, and two distinct app-loader fake handle classes are distinguishable. This aligns with
      the project's dormancy-testing discipline (every dormant step gets a same-loader unit test).
  - **Option 2 — classloader-identity routing (zero new SPI).** Capture each sibling's loader
    (`icebergSibling.getClass().getClassLoader()`, `hudiSibling.getClass().getClassLoader()`) and route a
    foreign handle by `handle.getClass().getClassLoader() == thatLoader`. Connector-agnostic, keeps the
    raw-handle-return invariant, **no** SPI change.
    - *Cost / feasibility hazard:* the routing is **NOT** same-loader unit-testable — in a same-loader test both
      fakes are app-loader classes, so `handle.getClass().getClassLoader()` collapses to the **same** loader and
      the discriminator cannot distinguish them. Correctness is only provable with a two-URLClassLoader fixture
      or a flip-time redeploy smoke.
- **Recommendation:** **Option 1 (`ownsHandle`)** — the one-method SPI cost buys dormant-verifiability of the
  single most correctness-critical routing seam, which is otherwise invisible until the flip. Option 2 is the
  fallback if adding an SPI method is rejected. **Either way**, the gateway's negative/hive test stays on the
  gateway's OWN `HiveTableHandle` (loader-safe), and the cross-loader non-CCE guarantee still needs a
  two-URLClassLoader fixture (§4).
- **Lands:** replace the single `icebergSiblingSupplier` in `HiveConnectorMetadata` (`:187`) with a handle-aware
  resolver (`Function<ConnectorTableHandle, ConnectorMetadata>` or a `siblingMetadata(session, handle)` that
  dispatches via the chosen mechanism); change the ~30 forward sites to route by handle; change the three
  connector-level `HiveConnector.get*Provider(handle)` (`:120-124, :142-146, :160-164`) to the 3-way resolver.
- **Mode:** dormant-verifiable (Option 1) / dormant-buildable (Option 2). **Touch points:** fe-connector-api
  (`Connector.java` — one new default, Option 1 only), connector-hive (`HiveConnector`, `HiveConnectorMetadata`
  — refactor the live iceberg spine), connector-hudi + connector-iceberg (`ownsHandle` overrides, Option 1).
  **Deps:** HD-A1 (needs the hudi sibling holder to reference). **Risk:** **HIGH / flip-blocker** — this
  **mutates the already-landed, shared iceberg delegation spine** that the iceberg W-steps depend on; sequence
  so existing iceberg per-handle behavior/tests are preserved (the iceberg arm must be byte-behavior-identical
  after the refactor). **Proving test:** Option 1 — same-loader unit test with an iceberg-fake and hudi-fake
  each overriding `ownsHandle`, asserting each foreign handle routes to its own sibling and a hive handle stays
  hive; **plus** a two-URLClassLoader fixture (shared with the iceberg S5 plan) proving no CCE across the loader
  split. Option 2 — two-URLClassLoader fixture only.

#### HD-B2 — getTableHandle HUDI divert + sibling lifecycle (the arming pivot — LAST connector step)
- **Lands:** add, adjacent to the iceberg arm, `if (tableType == HiveTableType.HUDI) return
  hudiSiblingMetadata(session).getTableHandle(session, dbName, tableName);` in
  `HiveConnectorMetadata.getTableHandle` (`:256`), returning the **raw** `HudiTableHandle`. Keep the
  `UNKNOWN && !view` fail-loud below intact. This is the first site that produces a foreign hudi handle, which
  lights up the 3-way guard-and-forward everywhere.
- **Mode:** dormant-buildable (dead until `"hms"` enters `SPI_READY_TYPES`). **Touch points:** connector-hive
  (`HiveConnectorMetadata.getTableHandle`). **Deps:** **HD-B1 strictly first** (else the foreign hudi handle
  mis-routes to iceberg on every per-handle call), **and** all of Group C + HD-D1 complete (else diverting HUDI
  regresses incremental/time-travel/MVCC/write-reject at flip). **Risk:** high — this is the pivot; ordering is
  load-bearing. **Proving test:** same-loader unit — with the hudi sibling present, `getTableHandle` on a
  HUDI-detected table returns the sibling's raw handle (not a `HiveTableHandle`); the gateway/detector must
  agree with the sibling's own COW/MOR detection (`HiveTableFormatDetector` is exact-input-format match;
  `HudiConnectorMetadata.detectHudiTableType` is substring — ensure they never disagree on HUDI-vs-UNKNOWN).
- **Detection order:** iceberg is checked before hudi (a table carrying both resolves iceberg — legacy parity,
  already in the detector). Do not let the HUDI arm swallow the genuine-UNKNOWN fail-loud.

### Group C — No-regression gap closure (all connector-side; all before HD-B2 / flip)

All Group C steps build a shared spine: a **pin field on `HudiTableHandle`** (e.g. `queryInstant` +
`incrementalWindow`/`hoodieParams`) that `applySnapshot` stamps and `planScan` honors. Build that spine once
(shared by HD-C2/C3).

#### HD-C1 — MVCC surface: beginQuerySnapshot / getMvccPartitionView / listPartitions / freshness (DV-007)
- **Lands:** on `HudiConnectorMetadata`:
  - `beginQuerySnapshot` → pin the **latest completed instant** as a snapshot-id-kind `ConnectorMvccSnapshot`
    (mirroring legacy `HudiUtils.getLastTimeStamp`). **Must NOT** set `lastModifiedFreshness=true` (hudi
    instants are monotonic snapshot-ids; that flag routes to the hive-style on-demand freshness probe hudi does
    not implement).
  - `getMvccPartitionView` → empty (hudi is LIST-partitioned, not RANGE — the generic path then uses
    `listPartitions`, exactly like paimon).
  - `listPartitions` / `listPartitionNames` → port `HudiUtils.getPartitionValues` +
    `HudiPartitionUtils.getAllPartitionNames` + `useHiveSyncPartition`, returning per-partition
    `lastModifiedMillis` = the instant (so `getPartitionSnapshot` takes the `MTMVSnapshotIdSnapshot` branch, not
    the `-1` sentinel that over-refreshes MTMV).
  - `getTableFreshness` / `getPartitionFreshnessMillis` → last-commit instant (decide the freshness model —
    snapshot-instant vs last-modified — from legacy `HudiDlaTable`).
- **Mode:** dormant-buildable. **Touch points:** connector-hudi (`HudiConnectorMetadata` new overrides; port
  `HudiPartitionUtils`/`HudiUtils` partition-listing fragments). **Deps:** none (independent of the pin spine).
  **Risk:** high — without this a partitioned hudi table's MVCC view is empty → the table looks UNPARTITIONED
  with a `-1` snapshot id: partition pruning, `selectedPartitionNum`, and MTMV freshness all break vs legacy.
  `listLatestPartitions` renders partition items via `HiveUtil.toPartitionValues` with a
  `checkState(values.size()==types.size())` — the partition-name format must match the column count or it
  throws. **Secondary consequence (elevate to scalability no-regression):** the generic `PluginDrivenScanNode`
  batch-mode split throttling keys off `setSelectedPartitions(fileScan.getSelectedPartitions())`, populated
  from the MVCC `nameToPartitionItem` — **empty** without `listPartitions` → a large partitioned hudi table
  never engages batch mode and `planScan` eagerly enumerates every partition's file slices in one call
  (planning-time OOM risk that legacy `HudiScanNode` throttled). **Proving test:** flip-time e2e — partitioned
  hudi table shows correct `selectedPartitionNum` under a partition predicate; hudi MTMV detects a new commit;
  a many-partition table plans without OOM.
- **MTMV scope decision (needs user input):** if hudi MTMV base tables are in the no-regression bar, freshness
  must be real; else document the deferral explicitly (check legacy hudi MTMV support before deciding).

#### HD-C2 — Time travel (FOR TIME AS OF) via resolveTimeTravel + pin-honoring planScan
- **Lands:** `HudiConnectorMetadata.resolveTimeTravel`: `Kind.TIMESTAMP` → normalize
  `value.replaceAll("[-: ]", "")`, validate the instant exists on the completed timeline, return a
  `ConnectorMvccSnapshot` carrying the pinned instant (empty return when not-found so fe-core renders
  `notFoundMessage`; a `DorisConnectorException` for TZ/parse propagates as-is). `Kind.SNAPSHOT_ID` /
  `VERSION_REF` → **fail loud** with the exact legacy message *"Hudi does not support FOR VERSION AS OF, please
  use FOR TIME AS OF"* (align tests to this wording; do not reproduce per-action tricks). `applySnapshot` stamps
  the pinned instant on the `HudiTableHandle`. `HudiScanPlanProvider.planScan` uses `handle.queryInstant` when
  present instead of `timeline.lastInstant()` (`:108`), feeding
  `getLatestBaseFilesBeforeOrOn`/`getLatestMergedFileSlicesBeforeOrOn` (both already take an instant — COW & MOR
  pin support is a plumb-through, not new merge logic).
- **Mode:** dormant-buildable. **Touch points:** connector-hudi (`HudiConnectorMetadata.resolveTimeTravel` /
  `applySnapshot` new; `HudiTableHandle` new `queryInstant` field; `HudiScanPlanProvider.planScan`). **Deps:**
  the pin spine (shared with HD-C3); HD-C1's `beginQuerySnapshot` (the latest-instant implicit pin makes a
  non-time-travel read byte-identical). **Risk:** medium — the `[-:space]` strip is load-bearing (lexicographic
  instant compare); schema-at-instant intersects DV-006 (see HD-C4/C5). **Proving test:** unit for the
  VERSION-reject message and timestamp normalization (same-loader); flip-time e2e `FOR TIME AS OF` on COW and
  MOR vs legacy.

#### HD-C3 — Incremental read (@incr) — port IncrementalRelation + close the COW row-level filter
- **Lands:** port `COWIncrementalRelation`, `MORIncrementalRelation`, `EmptyIncrementalRelation`,
  `IncrementalRelation` from fe-core `datasource/hudi/source/` **into** `fe-connector-hudi` (recoding split
  output to `HudiScanRange`, and re-homing the fe-core helpers they pull in — `LocationPath`, `TableFormatType`,
  `HudiPartitionUtils` — **this is a real port, not a verbatim lift**, and the construction driver
  `LogicalHudiScan.withScanParams` is HMSExternalTable-coupled and must be re-implemented from the connector's
  own metaclient/serde params). Add `HudiConnectorMetadata.resolveTimeTravel` `Kind.INCREMENTAL`: validate the
  begin/end window against the timeline, carry begin/end + `hoodie.*` passthrough on the snapshot properties;
  `applySnapshot` threads the window onto `HudiTableHandle`; `planScan` branches to incremental split collection
  (COW `collectSplits()` / MOR `collectFileSlices()` — dispatch on table type, each throws on the wrong type as
  legacy `getIncrementalSplits` does), honoring `fallbackFullTableScan()` → degrade to a normal snapshot scan
  (not error); emit `hoodie.datasource.read.begin/end.instanttime` into `getScanNodeProperties`. Reproduce the
  **RO-as-RT** quirk connector-side (a COW `_ro` table read as MOR for incremental). `@incr` lists **LATEST**
  partitions + LATEST schema (do **not** pin schema for incremental).
- **Mode:** mixed (port = dormant-buildable; correctness = flip-time e2e). **Touch points:** connector-hudi
  (new ported relation classes; `HudiConnectorMetadata.resolveTimeTravel`/`applySnapshot`;
  `HudiScanPlanProvider.planScan` incremental branch; `HudiScanRange` for MOR JNI params). **Deps:** the pin
  spine (HD-C2); HD-A3 (partition parse) for correct partition identifiers. **Risk:** **HIGHEST correctness
  landmine — see §5.1.** The COW `_hoodie_commit_time > begin AND <= end` **row-level** predicate that legacy
  injected via `CheckPolicy` is **skipped** on the generic `LogicalFileScan` path, and the iron rule forbids
  re-adding it in fe-core. Because a COW update **rewrites the whole base file** (carrying unchanged older-commit
  rows forward), file-level selection alone returns **out-of-window rows = silent wrong results**. The connector
  must push a `_hoodie_commit_time` range predicate through the scan-range/thrift, OR BE's native/JNI reader
  must honor the begin/end instant window at **row** granularity — **this must be validated by e2e, not
  assumed** (Paimon is **not** a precedent here — its incremental is snapshot-delta, not row-filtering over
  carried-forward base rows). **Proving test:** flip-time e2e `@incr(begin,end)` on COW and MOR, with a
  deliberately-built fixture whose base file **straddles the window boundary** (older rows carried forward),
  asserting only in-window rows return, vs legacy `HudiScanNode`.

#### HD-C4 — Schema evolution / schema_id / history_schema_info / field-ids (DV-006)
- **Lands:** port field-id onto `HudiColumnHandle` (from Hudi `InternalSchema`), per-commit `InternalSchema`
  version tracking (`HudiSchemaCacheValue.getCommitInstantInternalSchema`), an Avro/`InternalSchema` →
  `TColumnType`/`TField` converter (port `HudiUtils.getSchemaInfo`), and a
  `getScanNodeProperties`/`populateScanLevelParams` hook that sets `current_schema_id` + `history_schema_info`
  and per-split `THudiFileDesc.schema_id`. **Every** `history_schema_info` nested `TField.name` **must be
  lowercased at every level** (MEMORY: `history_schema_info-lowercase-nested-names`). Tie schema-at-instant to
  the time-travel pin (`getTableSchema(pinnedHandle, snapshot)`). Also decide `getTableAvroSchema(true)` (meta
  fields) vs the current no-arg (`HudiScanPlanProvider.java:115`, DV-008 gap-2).
- **Mode:** dormant-buildable (correctness only on BE). **Touch points:** connector-hudi (`HudiColumnHandle`,
  `HudiConnectorMetadata` schema resolution, `HudiScanRange`/`HudiScanPlanProvider` scan-level params). **Deps:**
  HD-C2 (schema-at-instant). **Risk:** hardest sub-area; **scoping decision for the user** (see §4/§5.2). **Do
  NOT** ship the half-fix `current==file==-1` schema_id — DV-006 records it degrades to BE `ConstNode`
  (case-sensitive) which is **strictly worse** than emitting nothing (BE then uses lowercased
  `by_parquet_name`/`by_orc_name` name-match). **Proving test:** flip-time e2e over a schema-evolved
  (rename/reorder) hudi table + a mixed-case nested-struct-field table, on real BE (see §5.2 for the
  SIGABRT-direction unknown).

#### HD-C5 — JNI column lists at the pinned instant (time-travel MOR correctness)
- **Lands:** `HudiScanPlanProvider.planScan` currently resolves JNI `column_names`/`column_types` from
  `getTableAvroSchema()` (no-arg, latest, no meta fields) **once per scan** (`:114-120`). Under time travel a
  pinned-instant MOR/JNI read would ship **LATEST** column lists (schema-at-instant mismatch). Re-derive JNI
  columns at the pinned instant (and honor the meta-field decision from HD-C4). Keep `column_names`/`types` as
  typed **lists** (un-joined) so comma-bearing hive types are not shattered.
- **Mode:** dormant-buildable. **Touch points:** connector-hudi (`HudiScanPlanProvider.planScan`,
  `HudiScanRange`). **Deps:** HD-C2 (pin), HD-C4 (schema/meta-field decision). **Risk:** medium — only bites
  pinned-instant MOR/JNI reads over an evolved schema. **Proving test:** flip-time e2e `FOR TIME AS OF` on a
  schema-evolved MOR table.

### Group D — Write-reject safety net (hudi is read-only)

#### HD-D1 — Explicit read-only reject for hudi write / procedure / DDL
- **Problem:** once HD-B2 diverts HUDI to a foreign handle, that handle is non-hive, so the gateway's
  per-handle `getWritePlanProvider(handle)` (`:142-146`), `getProcedureOps(handle)` (`:160-164`), and
  `supportedWriteOperations(handle)` (SPI default derives from `getWritePlanProvider(handle)`) — plus the
  name-based DDL diverts (createTable/dropTable) — would, under the **binary** discriminator, forward it to the
  **iceberg** sibling → a confusing iceberg error or a bad write instead of the legacy read-only rejection.
- **Lands:** under HD-B1's 3-way routing, the hudi arm of the gateway's write/procedure/DDL methods must **not**
  forward to any sibling: `supportedWriteOperations(hudiHandle)` → empty (so the generic INSERT/DML gates in
  `PhysicalPlanTranslator` throw the standard "does not support INSERT/row-level DML"),
  `getWritePlanProvider(hudiHandle)` → throw an explicit *"hudi tables are read-only in this catalog"*
  `DorisConnectorException` (defensive — do not rely on the sibling returning null),
  `getProcedureOps(hudiHandle)` → null (no procedures, like plain-hive). No `HudiWritePlanProvider` is ever
  built. `beginTransaction` for a hudi handle must never be reached (writes rejected before txn open).
- **Mode:** dormant-buildable. **Touch points:** connector-hive (`HiveConnector` write/procedure/txn per-handle
  methods; `HiveConnectorMetadata` DDL divert sites). **Deps:** HD-B1. **Risk:** medium — relying on "the hudi
  sibling has no writer" is fragile (that only helps if hudi handles are routed **to** the hudi sibling, not to
  iceberg); route to an **explicit** reject. **Proving test:** same-loader unit — a hudi-fake handle on the
  write/procedure paths yields the explicit reject, not an iceberg-sibling call; flip-time e2e asserts hudi
  INSERT/DELETE/MERGE/EXECUTE all fail loud read-only on a heterogeneous HMS catalog (MEMORY:
  `hms-iceberg-delegation-needs-e2e` — hudi's e2e is READ parity + write-reject assertion, since hudi is
  read-only).

### 3.x Dependency summary
- HD-A1 → HD-B1 (routing needs the sibling holder).
- HD-B1 **strictly before** HD-B2 (else the first foreign hudi handle mis-routes to iceberg).
- All of Group C + HD-D1 **before** HD-B2 / flip (else the flip regresses live hudi users).
- HD-C2 and HD-C3 share the `HudiTableHandle` pin spine — build once.
- HD-C1 (`listPartitions`) and HD-C3 (incremental) share ported `HudiPartitionUtils`/`HudiUtils` fragments —
  port once.
- Port HD-C3/HD-C4 logic **strictly before** the flip-time deletion of fe-core `datasource/hudi/source/*` (the
  `*IncrementalRelation` classes are the reference logic; deleting first loses the source of truth).
- HD-B2 is the **last** connector step (arming pivot); the flip must not precede it.

---

## 4. Flip-time residuals + hard flip-blockers

### 4.1 The flip (one line, all-or-nothing)
Add `"hms"` to `CatalogFactory.SPI_READY_TYPES` (`:49-50`). This converts **hive + iceberg-on-HMS +
hudi-on-HMS simultaneously** — hudi **cannot** be flipped independently. Consequence: the already-complete
iceberg sibling spine is **not enough**; the whole hms flip is now gated on hudi's no-regression build-out.

### 4.2 Hard flip-blockers (must be GREEN before adding `"hms"`)
- **B1 — 3-way foreign-handle routing (HD-B1).** The binary `else → iceberg` is **actively wrong** once a hudi
  handle exists.
- **B2 — time travel (HD-C2) + incremental (HD-C3).** Legacy supports both; without the connector impl,
  `resolveTimeTravel` returns empty → `loadSnapshot` **fails loud** on a legacy-supported query = regression.
  Includes the **COW row-level `_hoodie_commit_time`** correctness (§5.1) — the single biggest risk.
- **B3 — MVCC / listPartitions (HD-C1).** Else a partitioned hudi table has an empty MVCC view + `-1` snapshot
  → pruning / `selectedPartitionNum` / MTMV all break; plus batch-mode / OOM regression.
- **B4 — read-only write/procedure reject (HD-D1).** Else hudi DML mis-routes to the iceberg sibling.
- **B5 — getTableHandle HUDI divert + sibling lifecycle (HD-B2).**
- **B6 — snapshot-path partition-value fidelity (HD-A3).** Silent NULL/wrong partition columns on the most
  basic non-hive-style partitioned read.
- **Crash-avoidance (must-fix IF HD-C4 is implemented, independent of DV-006 scope):** any emitted
  `history_schema_info` nested `TField.name` must be lowercase (MEMORY) — a mixed-case nested name can drive BE
  `StructNode.children.at()` → `std::out_of_range` → whole-DB SIGABRT. Note the **direction unknown** in §5.2:
  the safe baseline may be *emit nothing*, in which case this is a property of the fix, not of the flip.
- **Secondary (verify):** Kerberos plugin-auth for the hudi sibling (HD-A2).

### 4.3 Residuals needing explicit user scoping (may or may not gate the flip)
- **DV-006 schema evolution (HD-C4/C5).** A **narrow** regression: only schema-evolved (rename/reorder) hudi
  tables degrade; plain and non-evolved reads are unaffected under the safe baseline. **Not** among the three
  named no-regression items (incremental / time-travel / MVCC). User decides whether schema-evolved hudi tables
  are in the no-regression target (see §5.2).
- **DV-008 gap-2**: `getTableAvroSchema(true)` meta-field inclusion (column-set difference).
- **Runtime-filter partition prune** (`getPartitionInfoMap` → per-split partition-value map): a pruning
  speedup, not a correctness gap (BE still filters) — deferrable.
- **MTMV freshness model** for hudi (HD-C1) — scope with the DV-006 decision.

### 4.4 Post-flip dead-code deletion (flip-or-delete-time; port first)
Reachable **only** while a legacy `HMSExternalCatalog` with `dlaType==HUDI` exists — zero live callers after
the flip; delete alongside the legacy hive/iceberg deletion:
- fe-core: `LogicalHudiScan`, `PhysicalHudiScan`, `LogicalHudiScanToPhysicalHudiScan`, the
  `visitPhysicalHudiScan` method (incl. its dead `PluginDrivenExternalTable` branch + fail-loud throws
  `PhysicalPlanTranslator.java:895-925`), the `BindRelation.java:603-609` `dlaType==HUDI` arm, the
  `CheckPolicy` `_hoodie_commit_time` predicate injection, `RuleSet`/`AggregateStrategies` hudi entries,
  `datasource/hudi/source/*` (`HudiScanNode` + `COW/MOR/Empty IncrementalRelation` + `HudiSplit`),
  `datasource/hudi/*` (`HudiMvccSnapshot`, `HudiExternalMetaCache`, `HudiUtils`, `HudiPartitionUtils`,
  `HudiSchemaCache*`), `HudiDlaTable`. P3 tracks these as batch-E/T10 (~2403 LOC).
- **Guard:** do not delete `datasource/hudi/source/*` until HD-C3/HD-C4 have ported the reference logic.

---

## 5. Open risks / unknowns

### 5.1 (BIGGEST) COW-incremental row-level `_hoodie_commit_time` filter — possibly unfixable under the iron rules
Legacy injects `_hoodie_commit_time > begin AND <= end` via `CheckPolicy`, gated on `LogicalHudiScan &&
HMSExternalTable`. The flipped hudi table rides the generic `LogicalFileScan` path, so this injection is
**skipped**, and the iron rule forbids a `PluginDrivenExternalTable` arm in `CheckPolicy`/fe-core. In COW, an
update **rewrites the whole base file**, copying unchanged (older-commit) rows forward; a base file selected by
commit-range file selection therefore contains rows **below** the window. Without the row predicate, COW
incremental returns **out-of-window rows = silent wrong results**. **The unknown:** whether BE's native/JNI
Hudi reader applies the begin/end instant window at **row** granularity for COW base files when given
`hoodie.datasource.read.begin/end.instanttime`. If it does not, there is **no iron-rule-compliant fe-core fix**
— the connector must push the predicate through the scan-range/thrift, or BE must do it natively. This is
simultaneously a correctness landmine, possibly unfixable under the constraints, not unit-testable, and only
detectable with a purpose-built straddling-base-file fixture. **Action:** resolve this **first** in HD-C3 by
inspecting the BE Hudi reader (BE tree is absent from this FE checkout — verify against the BE source before
committing to an approach), and build the straddling fixture deliberately.

### 5.2 Schema-absence SIGABRT direction — recon agents contradicted; reconciled but BE-unverified here
Recon agent 1 claimed *absence* of `history_schema_info` risks a BE `StructNode` `out_of_range` SIGABRT on
mixed-case nested fields (a flip-blocker). The critics reconciled this against BE code
(`table_schema_change_helper.h/.cpp`): when `!params.__isset.history_schema_info` (the connector's current
emit-nothing state), BE uses `by_parquet_name`/`by_orc_name` which **lowercases** file field names →
**case-insensitive** name match, **no** `ConstNode`, **no** SIGABRT. The `ConstNode` `out_of_range` exposure is
reachable **only** via the field-id path, which requires `history_schema_info` to **be set**. **Working
conclusion:** *emitting nothing is the safe baseline*; DV-006 is a **narrow evolved-schema regression, not a
flip-blocker crash** for plain or mixed-case reads; the lowercase-nested-name discipline is a **must-IF-HD-C4-is-
implemented** guard, not a must-fix-before-flip. **Unknown / caveat:** the **BE tree is not present in this
checkout**, so this reconciliation could not be re-verified from source here — **verify against BE
`table_schema_change_helper.*` before finalizing HD-C4 scope**, and treat the SIGABRT-direction as confirmed
only after that read.

### 5.3 PhysicalHudiScan routing — RESOLVED (not an open risk)
§2 answers it and HEAD confirms it: a flipped hudi-on-HMS table is a `PluginDrivenExternalTable` →
`LogicalFileScan` → `PhysicalFileScan` → `PluginDrivenScanNode`; `visitPhysicalHudiScan`'s plugin branch is
dead post-flip. **Affirm, do not treat as open.** The residual risk is only that a reviewer "fixes" the missing
`PhysicalHudiScan` by re-adding a `dlaType`/`instanceof` arm to `BindRelation` — an iron-rule violation to be
called out in review.

### 5.4 Dormant-buildable ≠ dormant-verifiable
Every H-step compiles behind the closed gate, but two classes of correctness have **no live path** pre-flip:
- **HD-B1 routing correctness** across the loader split — invisible to same-loader tests. Mechanism choice
  decides how invisible (§3B): `ownsHandle` makes the routing **logic** same-loader unit-testable;
  classloader-identity does not (both fakes share the app loader). Either way the cross-loader non-CCE guarantee
  needs a **two-URLClassLoader fixture**.
- **HD-C1..C5 runtime correctness** (instant-pinned reads, incremental windows, MOR log-merge, schema
  evolution, partitioned pruning, partition-value fidelity) — method-unit-testable in isolation, but
  end-to-end correctness requires **BE + real hudi tables = flip-time e2e only** (no standalone `type=hudi`
  catalog exists to exercise the sibling pre-flip).

**Pre-committed non-unit harnesses (the literal flip gate, per MEMORY `hms-iceberg-delegation-needs-e2e`):**
(1) a **two-URLClassLoader routing fixture** proving iceberg/hudi foreign handles route to the correct sibling
without CCE (shared with the iceberg S5 plan); (2) a **heterogeneous-HMS e2e** over COW+MOR snapshot reads,
`FOR TIME AS OF`, `@incr(begin,end)` on **both** COW and MOR (incl. the straddling-base-file fixture),
schema-evolved + mixed-case-nested-field tables, non-hive-style + escaped partition values, and partitioned
pruning — asserting **byte-identical** results vs the legacy `HudiScanNode` path (same tables) **and** vs an
independent hudi read; plus hudi INSERT/DELETE/MERGE/EXECUTE fail-loud read-only assertions.

### 5.5 Refactoring the live iceberg spine (HD-B1)
Generalizing the single `icebergSiblingSupplier` + the binary per-handle discriminators to a 3-way resolver
**mutates already-landed, shared iceberg delegation code** (dormant but live-once-flipped). Sequence to
preserve the existing iceberg per-handle behavior exactly (byte-behavior-identical iceberg arm), and re-run the
iceberg sibling unit/two-loader tests after the refactor.

---

## 6. References
- **iceberg sibling template:** `plan-doc/tasks/hms-cutover-sibling-connector-decomposition-2026-07-08.md`
  (S0–S6; §2 handle-discrimination contract; §5 no-dormant-testable CCE boundary).
- **Hudi migration task:** `plan-doc/tasks/P3-hudi-migration.md` (batches A–C landed; batch-E/T10 deferrals;
  ~2403 LOC dead-code inventory).
- **Deviations:** `plan-doc/deviations-log.md` — **DV-005** (standalone-type mismatch → resolved by sibling),
  **DV-006** (schema_id / history_schema_info / field-ids), **DV-007** (listPartitions / MVCC),
  **DV-008** (`getTableAvroSchema(true)` meta-field inclusion).
- **Public tracking issue:** apache/doris#65185 (catalog-SPI umbrella).
- **MEMORY:** `plugin-tccl-classloader-gotcha`, `history-schema-info-lowercase-nested-names`,
  `hms-iceberg-delegation-needs-e2e`, `plugindriven-no-source-specific-code`, `no-property-parsing-in-fecore`.

### Key HEAD source anchors (verified at `75670ae4193`)
- `fe/fe-core/.../datasource/CatalogFactory.java:49-50` (SPI_READY_TYPES; the flip line).
- `fe/fe-connector/fe-connector-hive/.../HiveConnector.java:65,87,99,120-124,142-146,160-164,237-252,362-374`.
- `fe/fe-connector/fe-connector-hive/.../HiveConnectorMetadata.java:187,209-210,256-257` + ~30
  `!(handle instanceof HiveTableHandle)` forward sites.
- `fe/fe-connector/fe-connector-hive/.../IcebergSiblingProperties.java` (synthesize template).
- `fe/fe-connector/fe-connector-hudi/.../HudiConnectorProvider.java:36-38` (getType; misleading javadoc `:30`).
- `fe/fe-connector/fe-connector-hudi/.../HudiConnector.java:60,65,80-100,103`.
- `fe/fe-connector/fe-connector-hudi/.../HudiConnectorMetadata.java` (overrides only getTableSchema `:199`
  among the MVCC/time-travel set; `applyFilter`, `detectHudiTableType`).
- `fe/fe-connector/fe-connector-hudi/.../HudiScanPlanProvider.java:92,103-108,115,144,204-208,232-249,284-311,
  317-334` (lastInstant pin gap; parsePartitionValues infidelity).
- `fe/fe-core/.../datasource/hudi/HudiPartitionUtils.java:63-89` (legacy positional + unescape parse).
- `fe/fe-core/.../nereids/rules/analysis/BindRelation.java:603-609` (HUDI→LogicalHudiScan HMS-only),
  `:623,654-658` (PLUGIN_EXTERNAL_TABLE → LogicalFileScan).
- `fe/fe-core/.../nereids/glue/translator/PhysicalPlanTranslator.java:803-822` (generic PluginDrivenScanNode),
  `:865-866` (scanParams/snapshot threading), `:895-925` (dead visitPhysicalHudiScan plugin branch + fail-loud).
- `fe/fe-core/.../datasource/PluginDrivenMvccExternalTable.java:154,161,263,319,328,347,349,353,364,375,
  397-417` (the connector-agnostic MVCC/time-travel/incremental seam).
- `fe/fe-connector/fe-connector-paimon/.../PaimonConnectorMetadata.java:432,498,552` (working resolveTimeTravel
  + INCREMENTAL template).
- `fe/fe-connector/fe-connector-api/.../Connector.java:66-67,87-88,107-108,187-188` (per-handle SPI defaults;
  **no** `ownsHandle` today — HD-B1 Option 1 adds it).
- `fe/fe-connector/.../ConnectorTableHandle.java` (bare marker, zero methods).