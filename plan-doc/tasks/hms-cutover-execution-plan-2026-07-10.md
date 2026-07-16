# HMS cutover — up-to-date execution plan (2026-07-10)

> Produced by a HEAD-grounded recon (`wf_94feaccd-d60`: 6 dimension readers + 2 adversarial critics — atomic-set integrity + completeness) with lead-engineer verification of the load-bearing facts. **Supersedes the STATUS/phasing of `hms-cutover-retype-design-2026-07-07.md` §4/§5** (its §2 flip-shape, §3 heterogeneity crux, §6 D1–D6 decisions, and §7 hard-gate remain valid). The 07-07 doc was written *before* almost its entire §4 build-out landed; this doc reconciles it against HEAD (`db27455b4e1`). **Trust HEAD, not doc line numbers** — every line number below re-verified.

---

## 0. TL;DR — the one thing that changed

The 07-07 plan framed the cutover as **dormant build-out → one atomic flip → mechanical delete**. Two weeks of work went entirely into the **hudi delegation line** (HD-A0 … HD-D1 + the arming pivot HD-B2), so **all connector-side dormant steps are now DONE**. But the plan's §5 assumed two large **fe-core** subsystems (event-pipeline relocation + connector-owned cache) would also land dormant before the flip. **They were never started (0% at HEAD).**

**⇒ The flip is NOT executable as one commit today.** The atomic flip commit *neutralizes* four gates (the HMS event poller + three cache-routing sites). Those gates going false is only *safe* if their replacements already exist. They don't. Executing the flip at HEAD would make **event sync silently halt forever** and **cache routing silently collapse to `ENGINE_DEFAULT`** — no crash, no log. Both critics independently ranked this the #1 finding (blocker).

So the accurate remaining structure is **four** phases, and the real remaining *work* is Phase 1 (two new fe-core subsystems + a handful of seams/extractions), not the flip commit itself:

| Phase | What | Status |
|---|---|---|
| **0. Connector dormant build-out** | Read-SPI, sibling delegation (iceberg S0–S6 + W1–W5 + WC1–WC4), whole hudi line (HD-A0…HD-D1 + HD-B2) | ✅ **DONE** |
| **1. Pre-flip fe-core build-out (dormant)** | **Event-pipeline Model B** + **connector-owned cache (D2)** + 3 coupling seams + deletion-unblocking extractions + W6 verify | ❌ **0% — the real remaining work** |
| **2. THE ATOMIC FLIP (one commit)** | `SPI_READY += hms` + 3 GSON remaps + `buildDbForInit` + dead-arm/INC-5/D5 removals + gate rewires + replay test | ❌ not started (Phase-1-gated) |
| **3. DELETE (mechanical PR)** | The legacy cyclic import unit (~90 classes) | ❌ last |
| **4. e2e / hard gates (R-002)** | Two non-unit harnesses + consolidated e2e matrix + 4 correctness-risk rows + clean-room + capability-twin | ❌ green-before-flip |

### 0.1 Decisions recorded (user sign-off 2026-07-10)
- **Flip order = BUILD-FIRST (no outage).** Land the event-pipeline relocation + connector-owned cache dormant *before* the flip. No silent event-sync/cache-collapse window. (Trino-aligned: engine owns HA/replication, plugin owns fetch/parse.)
- **Auto-refresh = keep parity, built into the cache subsystem.** The hive connector surfaces a real max-partition modify time so SQL-dictionary / MV-on-hive-base auto-refresh matches today. `getNewestUpdateVersionOrTime` parity rides on D2. (Resolves §7 Q3.)
- **Priority = reach a WORKING FLIP (new code path live) first; DELETE legacy code LAST.** Do not fuss PR packaging now. **⇒ the deletion-unblocking extractions (§2.4) move OUT of Phase 1 and INTO Phase 3 (deletion)** — they are only needed to delete the legacy classes, which is deferred to the end; post-flip the legacy `HiveUtil`/`HiveSplit`/`IcebergUtils` classes still exist (dead-for-hms) so live consumers keep compiling. (Resolves §7 Q2 + supersedes §8.)
- **⇒ Phase 1 remaining work reduces to:** connector-owned cache (D2, with max-partition-time) → event-pipeline Model B → the 3 loud-break coupling seams → W6 verify. Then the atomic flip. Then (last) extractions + delete.

---

## 1. Where we are (DONE ledger — reconciles the 07-07 §4 blockers)

Every ⛔ blocker and build-out item the 07-07 doc §4/§4.2a/§6 listed is **landed and HEAD-verified**:

- **§4.1 Kerberos authenticator** — done (`e63b03fb490` gateway; `561f777d5f9` hudi sibling). *Only the write-path half (W6) remains — see Phase 1.*
- **§4.2 read-side SPI** — all done: `partition_columns` emission, `listPartitions`/`listPartitionNames`, `getTableStatistics` (3-tier), `buildTableDescriptor` (`THiveTable`), `viewExists`/`getViewDefinition`/`dropView`, `getCapabilities`, file-format serde, `HiveTableFormatDetector` parity, per-table Top-N marker.
- **§4.2a per-column stats (D1=preserve)** — done (`cbf9526f776`).
- **§4.3 freshness-kind-aware `getTableSnapshot`** — done (`3d784673ca4`). *This resolves the 07-07 §3 "MAJOR consequence" (plain-hive stale-MV).*
- **§4.4 sibling-connector SPI + iceberg delegation** — done (S0–S6) + all write/procedure/DDL delegation (W1–W5) + write-chain (WC1–WC4).
- **§4.5 deletion-unblockers** — `BIND_BROKER_NAME` relocated (`BrokerProperties.BIND_BROKER_NAME_KEY`); `pluginCatalogTypeToEngine("hms")→ENGINE_HIVE`; LZO read-reject ported connector-side; read-ACID query-finish release (WC4).
- **Whole hudi line** — HD-A0…A5, HD-B1 (3-way routing), HD-C1 (MVCC/partitions), HD-C2 (FOR TIME AS OF), HD-C3 INC-1…4 (@incr), HD-C4/C5 (schema evolution), HD-D1 (read-only reject), **HD-B2 (getTableHandle diverts HUDI = the arming pivot).**
- **Capability preconditions for the flip** — `HiveConnector.getCapabilities` declares `SUPPORTS_MVCC_SNAPSHOT`+`VIEW`+`COLUMN_AUTO_ANALYZE` (dormant); `PluginDrivenExternalDatabase.buildTableInternal:53-60` picks `PluginDrivenMvccExternalTable` off that flag → a flipped hms table gets the right class **for free**.

**⇒ The connector is flip-ready. fe-core is not.**

---

## 2. Phase 1 — the real remaining work (dormant fe-core build-out)

Everything here can and must land **dormant** (inert while hms is still legacy), each an independent reviewable commit, exactly like the connector steps. Ordered by size/criticality.

### 2.1 ⛔ Event-pipeline relocation — Model B (the largest un-built blocker)
Design: `hms-event-pipeline-findings-2026-07-07.md` (Model B). **0% built** (grep `pollOnce`/`getNextNotification` across `fe-connector` = none; the poller still `instanceof HMSExternalCatalog` at `MetastoreEventsProcessor.java:116`).
- Thin fe-core role-aware `MasterDaemon` driver that probes each connector for an optional event-source capability (a **capability probe, not `instanceof`**) → `connector.pollOnce(lastSyncedEventId, isMaster)` returning a **neutral change-descriptor list** (must cover register/unregister/rename + **partition-NAME** granularity — closes the `invalidatePartition` values-vs-names gap).
- Plugin (`fe-connector-hms`) severs fetch+parse only (`getNextNotification`/`getCurrentNotificationEventId` on `HmsClient`, deserializers, event→descriptor mapping). No fe-core imports plugin-side.
- fe-core wraps `pollOnce` in an `onPluginClassLoader` pin (R-010 — today there is **no** pin in the poller path).
- Retain follower→master `REFRESH CATALOG` forward + HA/edit-log in fe-core.
- Keep **opcode 470** (`OP_ADD_META_ID_MAPPINGS`) + a neutral replay handler on disk (the replay-CCE fix already landed `a6dc782d816`); `ExternalMetaIdMgr` can otherwise be dropped for HMS.
- **New SPI:** `Connector.invalidatePartition(s)` (pairs with the descriptor granularity).

### 2.2 ⛔ D2 — connector-owned scan-side cache (retire the fe-core caches)
Decision D2 (LOCKED 07-07) = connector-owned. **0% built.** All three fe-core caches present; `ExternalMetaCacheRouteResolver.java:66` still routes hms→HIVE+HUDI+ICEBERG caches. The hive connector must own scan-side caching (paimon/iceberg-native precedent) so that at the flip, routing collapsing to `ENGINE_DEFAULT` is *harmless*. Retire `HiveExternalMetaCache` / `HudiExternalMetaCache` / `IcebergExternalMetaCache` and the four routing `instanceof` gates (`ExternalMetaCacheRouteResolver:66`, `HiveExternalMetaCache:203/274`, `HudiExternalMetaCache:221`, `IcebergExternalMetaCache:234`) **with the flip set**. Also unblocks §2.6 below.

### 2.3 Coupling seams that break LOUD at the flip (must be dormant-staged) — ✅ **ALL LANDED (dormant)**
> Detailed design + landing = `hive-coupling-seams-step-design-2026-07-10.md` (S1–S4 + W6). All chose FULL parity (user 2026-07-10). Each an independent dormant commit; clean-room review pending (deliberately not started).
- **`partition_values()` TVF** — ✅ **S1** `166515cdc88` (PluginDriven gate+arm + `getNameToPartitionValues` SPI, mirror `$partitions`).
- **`hudi_meta()` / TIMELINE TVF** — ✅ **S2** `d8f2d01978a` (KEEP, connector-driven: neutral `getMetadataTableRows` SPI + `SUPPORTS_METADATA_TABLE` + `HudiConnector` impl; MetadataGenerator sheds `org.apache.hudi`).
- **`canSample` / `SUPPORTS_SAMPLE_ANALYZE`** — ✅ **S3** `8469a033abd` (FULL port, not the degrade: capability + sample task + `listFileSizes` SPI + distribution-column parity).
- **auto-analyze per-table gate** — ✅ **S4** `89c6f9454bb` (drop hive connector-wide `SUPPORTS_COLUMN_AUTO_ANALYZE`; per-table marker; **sibling-capability reflection** at the delegation branch so iceberg-on-HMS keeps auto-analyze + regains Top-N/nested-prune, hudi-on-HMS excluded — Trino table-redirection semantics).

### 2.4 Deletion-unblocking extractions (→ RE-SEQUENCED to Phase 3 per the 2026-07-10 decision)
> **Deferred to the deletion phase** (deletion is last). Post-flip the legacy classes still exist (dead-for-hms), so their live consumers keep compiling; these extractions are only needed at the moment of deletion. Listed here for completeness; do them in Phase 3, immediately before deleting the dirs.

Three members of the deletion unit are consumed by **live surviving code** and must be relocated to a surviving util *before* the unit can be deleted:
- `HiveUtil.toPartitionValues` — consumed by **live** `PluginDrivenMvccExternalTable.java:296`.
- `HiveUtil.isLzoInputFormat` — consumed by **live** `BaseExternalTableDataSink.java:86`.
- `HiveSplit` branch in **live** `FileQueryScanNode.java:465` (remove the branch).
- **`IcebergUtils` 6-member extraction** to a new fe-core SDK-free util (`ICEBERG_ROW_ID_COL`, `ICEBERG_LAST_UPDATED_SEQUENCE_NUMBER_COL`, `ICEBERG_ROW_LINEAGE_MIN_VERSION`, `isIcebergRowLineageColumn(String)`, `isIcebergRowLineageColumn(Column)`, `getEffectiveIcebergFormatVersion` with its 3 iceberg string constants inlined). **The target util does not exist at HEAD.** Repoint `BindExpression`, `CreateTableInfo:1145-1171`, `IcebergMergeCommand`, `IcebergUpdateCommand`.

### 2.5 W6 — write-path TCCL pin (verify-first) — ✅ **VERIFIED FALSE GAP, NO CODE**
Recon confirmed the pin is already carried: an iceberg/hudi-on-HMS write is delegated to the sibling's per-handle write provider, and the sibling already wraps its write/commit in its own `TcclPinningConnectorContext` (`IcebergConnector` around `executeAuthenticated`, classloader-thread-independent). So there is **no unpinned fe-core write-path** at this seam. Over-cautious comment softened (`f53a71f5260`, doc-only). Iceberg-on-HMS write no-CCE stays **e2e-owed** (Phase 4).

### 2.6 `getNewestUpdateVersionOrTime` real max-partition-time (coupled to D2)
Flipped plain-hive returns constant `0` (names-only `listPartitions` → every `lastModifiedMillis` UNKNOWN `-1` → filtered → `orElse(0L)`), so a **hive-backed SQL dictionary / MV auto-refresh never sees a newer source version → silently stale**. Fix needs the connector to surface a real max-partition modify time cheaply — **which needs D2 first**. Alternatively, sign off an explicit temporary regression + e2e assertion. `PluginDrivenMvccExternalTable.java:713-714`.

---

## 3. Phase 2 — THE ATOMIC FLIP (one commit, canonical union checklist)

This is the "must-be-atomic set." All members ship in **one** commit (the GSON remaps + `SPI_READY` are mutually locked; the gates go false together). **This commit is only correct once Phase 1 has landed.** Line numbers HEAD-verified.

1. **`SPI_READY_TYPES += "hms"`** + delete the dead `case "hms" → new HMSExternalCatalog` fallback. `CatalogFactory.java:55-56` (set) / `:139-141` (fallback). *(07-07 doc said :49-50 / :133-135 — stale.)*
2. **3 GSON remaps** — delete `registerSubtype` + add `registerCompatibleSubtype`: catalog→`PluginDrivenExternalCatalog` (`GsonUtils.java:366`), db→`PluginDrivenExternalDatabase` (`:447`), **table→`PluginDrivenMvccExternalTable`** (`:471`, target confirmed by the paimon `:489-490` / iceberg `:493-494` precedent). Mutually locked with (1) via `RuntimeTypeAdapterFactory:280` "labels must be unique" static-init throw **and** loss of write-registration (a live `HMSExternalCatalog` would crash image-write). Precedent: paimon `a26eaeff84c` did `SPI_READY`+GSON in one commit.
3. **`buildDbForInit` case HMS → `new PluginDrivenExternalDatabase`** (`ExternalCatalog.java:967-968`, mirror the ICEBERG arm). *Must-not-omit:* else a freshly-created (non-replayed) hms catalog silently builds legacy `HMSExternalDatabase→HMSExternalTable` under a PluginDriven catalog — heterogeneous legacy/plugin objects, no error.
4. **Remove dead `PhysicalPlanTranslator` arms + `DLAType` import** — the `instanceof HMSExternalTable` scan arm `:824-853` and `visitPhysicalHudiScan` `:895-943` become unreachable (PluginDriven arm `:814` wins) but compile-reference legacy scan nodes. Drop them + import `:65`. *(07-07 said :818-846/:925-928 — stale.)*
5. **INC-5 (NEW — 07-07 predates it):** remove the now-**stale** `visitPhysicalHudiScan` throws for FOR TIME AS OF / @incr (`:910`, `:914`) — HD-C2/C3 landed the connector support they reject → dead-on-arrival otherwise — and the legacy `CheckPolicy` hudi incremental arm (`CheckPolicy.java:94-99`, comment already says "deleted at the cutover"). Forward `tableSnapshot`/`scanParams` to `PluginDrivenScanNode`.
6. **D5 (LOCKED 07-07):** drop both `enable_query_hive_views` / `enable_query_iceberg_views` gates (serve any `isView()`), neutralize the iceberg-worded message + time-travel throw, deprecate both `@ConfField` to no-ops one release. `BindRelation.java:594` (dead hive arm), `:634-636` (iceberg gate+throw). **Visible iceberg behavior change — must ship in the flip, not leak early.**
7. **Rewire the 4 gates** — event poller (`MetastoreEventsProcessor:116`) → Model B driver; cache routing (`ExternalMetaCacheRouteResolver:66` + the 3 legacy-cache gates) → D2 connector cache. **Only safe because Phase 1 landed.**
8. **`HmsGsonCompatReplayTest`** (template: `Iceberg`/`PaimonGsonCompatReplayTest`) — pins the single-base-tag round-trip + the invariant that deserialized external-table objects never survive a replay (metaCache transient, `initialized` reset, rebuilt via `buildTableInternal`).

---

## 4. Phase 3 — DELETE (mechanical, one PR, the cyclic unit)

`datasource/hive` (52), `datasource/hudi` incl. `hudi/source/*` (15), `datasource/iceberg` (exactly 23) import each other cyclically (`HudiScanNode extends HiveScanNode:94`; `HudiUtils→HMSExternalTable`; iceberg scan/cache→hive; `HMSExternalTable→IcebergDlaTable/HudiDlaTable/HudiUtils/IcebergUtils`) → **cannot delete hive alone**. Connector build-out is **not** entangled (`fe-connector/**` has zero real `datasource.hive/.hudi/.iceberg` imports — the one grep hit is a vendored same-FQN copy). The full unit:

- **datasource/hive (52):** catalog/db/table, DLA tables, HMS clients, write chain, cache, utils, `source/{HiveScanNode,HiveSplit}`. `event/*` (21) — **delete only after** the Model B relocation (it is the current event driver).
- **datasource/hudi + hudi/source/* (15)** — `HudiScanNode`, `HudiSplit`, `IncrementalRelation`+`{COW,MOR,Empty}IncrementalRelation`, `HudiUtils`, `HudiExternalMetaCache`, `HudiMvccSnapshot`, `HudiPartitionUtils`, schema/meta-client cache keys. *(07-07 doc never enumerated these — hudi-line addition.)*
- **datasource/iceberg (23)** — TIER-1 scan cluster (9) + TIER-2 metadata (14, incl. `IcebergUtils` after the 6-member extraction).
- **Nereids legacy chains** (die at the flip, compile-referenced until Phase-2 removals): `LogicalHiveTableSink`/`PhysicalHiveTableSink`/`HiveInsertExecutor`/`HiveTransactionManager`; `LogicalHudiScan`/`PhysicalHudiScan`/`LogicalHudiScanToPhysicalHudiScan` + wiring (`BindRelation:604`, `RuleSet:201,248`, `AggregateStrategies:750`, `StatsCalculator:849`, `RelationVisitor:98`). *(hudi-scan chain also undoc'd in 07-07.)*
- **statistics/HMSAnalysisTask**, `StatisticsUtil.getIcebergColumnStats`/`getHiveRowCount`/`getTotalSizeFromHMS` + `:1008` branch (NB: 07-07's "`StatisticsUtil.getHiveColumnStats`" is wrong — that's a **private** method inside `HMSExternalTable:892`), **systable/IcebergSysTable** (already a throwing dead-end).
- **`Env.hiveTransactionMgr`** field + init + accessors (`Env.java:568/865/1097-1102`) — sole consumer is legacy `HiveScanNode` (`:141/147/319`) → delete in the **same** PR as `HiveScanNode`.

**Deletion ordering (topological):** (a) Phase-1 extractions (§2.4) land first; (b) Phase-2 flip removes all compile-references (dead arms, INC-5, hudi-scan wiring, `bindHiveTableSink`); (c) then the cyclic unit deletes mechanically; (d) `event/*` deletes only after Model B replaced the entrypoints.

---

## 5. Phase 4 — e2e / hard gates (R-002 — do not silently skip, Rule 12)

The 07-07 §7 gate is **materially stale** (predates the whole hudi line, names none of its rows, and predates the sibling-decomposition's two-loader fixture + W6). Two named non-unit harnesses — **neither exists yet**:

- **Harness (i) — two-URLClassLoader routing fixture:** loads iceberg/hudi handles in a child-first loader and proves the gateway diverts each foreign handle to the correct sibling **with no CCE** across the loader split. Unit tests can't: test classpaths load everything on the app loader, so an illegal foreign-handle cast passes silently (the shipped `HiveConnectorThreeWayRoutingTest` is same-loader — proves only `ownsHandle` logic).
- **Harness (ii) — heterogeneous-HMS docker e2e:** ONE `type=hms` catalog holding plain-hive + iceberg-on-HMS + hudi-on-HMS, each served by the right provider, mixed queries in one session correct.

**Consolidated e2e matrix** (every landed step's "⚠残留 e2e" note): transactional-hive read (full-ACID+insert-only in scope, full-ACID write hard-rejected); HMS event replay in sync post-flip; iceberg-on-HMS **INSERT/DELETE/MERGE/OVERWRITE/ALTER(14)/EXECUTE** == independent iceberg catalog (NET-NEW, signed 2026-07-08); MTMV freshness + time-travel over iceberg-on-HMS & hive base; hudi COW/MOR + hive-sync/non-hive-sync partition-value fidelity; hudi FOR TIME AS OF; hudi **@incr straddling-base-file** fixture; hudi schema-evolved reads (rename/reorder + **mixed-case nested struct must not SIGABRT**); hudi dropped-partition-set-at-instant; hudi MTMV refresh; hudi write/DDL/EXECUTE **fail-loud reject**; Kerberos-HMS smoke; `HmsGsonCompatReplayTest`; **coupling-seam rows (S1–S4, all dormant → live at flip):** `partition_values()` over heterogeneous HMS == legacy hive rows; `hudi_meta()` timeline rows == the 4 p2 suites (`timestamp/action/state/state_transition_time`, enableHudiTest); hive `ANALYZE … WITH SAMPLE` FULL-vs-SAMPLE stat assertions on **text + orc + partitioned + bucketed** plain-hive; background auto-analyze admits plain-hive + iceberg-on-HMS but **NOT** hudi-on-HMS; iceberg-on-HMS regains Top-N lazy + nested-column prune (via the S4 sibling-capability reflection) == an independent iceberg catalog.

**4 rows are genuine CORRECTNESS risks the flip cannot ship without** (both critics ranked these above the docker run):
1. **hudi COW-@incr row-level `_hoodie_commit_time` filter** — INC-4 delivered the iron-rule-compliant fix (neutral `ConnectorExpression` reverse-converter + `CheckPolicy.collectConnectorSyntheticPredicates`, no fe-core source branch); the "possibly unfixable" §5.1 worry is **superseded**, but the straddling-base-file correctness is **e2e-owed**.
2. **W6 write-path TCCL pin** — unpinned name-reflection → CCE on iceberg-on-HMS write.
3. **WC4 read-ACID query-finish release** — a broken release leaks the metastore shared read lock.
4. **Kerberos-HMS smoke** — silent SIMPLE-auth degrade.

**Human-review gates (§7, unchanged):** clean-room adversarial review over the flip+delete diffs; ENG-1 capability-twin audit over the coupling sites (now grown to include the hudi/write-delegation surface).

---

## 6. Refuted worries — do NOT spend flip budget here

Both critics verified these against HEAD and refuted them:
- **SQL-cache "stale-result correctness bug"** — **REFUTED (safe).** A flipped hive table reports `PLUGIN_EXTERNAL_TABLE`; `NereidsSqlCacheManager:441-443/480/506-507` returns `CHANGED_AND_INVALIDATE_CACHE` for it every time → cache **off**, never stale (same as paimon/iceberg-native). Deferrable perf item, **not** a flip gate. (Reconciled: dim-3 over-ranked it, dim-4 is correct.)
- **Plain-hive "eager `materializeLatest` vs lazy `EmptyMvccSnapshot`" regression** — **REFUTED.** Designed for byte-parity (`PluginDrivenMvccExternalTable:622` comment); the identical live path paimon/iceberg already run. e2e-owed only.
- **MTMV parity** — safe (`PluginDrivenMvccExternalTable` implements `MTMVRelatedTableIf`+`MTMVBaseTableIf`).
- **`IcebergExternalMetaCache:234` "silent-false gate"** — nuance: it would *throw* if reached, but post-flip it is **unreached** (downstream of the cache-route master). Retired with D2.

---

## 7. Open decisions

**Resolved 2026-07-10 (see §0.1):** Q1 pre-flip build vs outage → **build-first**. Q2 packaging → **don't fuss; delete last; reach working flip first.** Q3 `getNewestUpdateVersionOrTime` → **keep parity, built into D2 cache.**

**Still open (I'll take the recommended default as I reach each, unless you say otherwise):**
4. **explicit-partition-spec INSERT on flipped hive** — reject fail-loud (legacy parity) vs gain static-partition-overwrite support. *Lean: reject fail-loud (parity).* Decide + e2e at the flip.
5. **`hudi_meta()` TVF** — connector-driven timeline path vs drop the TVF. *Lean: drop unless a consumer needs it.*
6. **`SUPPORTS_SAMPLE_ANALYZE`** — port `canSample` vs accept hive sample→FULL degrade. *Lean: port (cheap, keeps parity).*
7. **`DorisConnectorException` → user-error mapping** in `ConnectProcessor.handleQueryException` — add now vs accept catch-all. *Lean: add (cleaner for all plugin connectors).*

*Settled / no decision needed:* hudi schema-evolution scope = **FULL parity** (user sign-off 2026-07-09, memory `hudi-schema-evolution-full-parity-signoff`) → the schema-evolution e2e rows are flip-blockers, not deferrable. `SUPPORTS_NESTED_COLUMN_PRUNE` = deliberately deferred (safe-off). SHOW CREATE TABLE generic form = accepted (D6-adjacent). TTL validation + file-format serde nuance = deferrable.

---

## 8. Packaging (D4) — user steer 2026-07-10

User: *don't fuss PR packaging; the priority is to reach a working flip (new code path live); delete legacy code last.* So the working sequence is:

- **Phase-1 build-out = independent dormant commits** (as every connector step so far), each inert while hms is legacy: **D2 cache (with max-partition-time) → Event Model B → the 3 loud-break coupling seams (§2.3) → W6 verify.**
- **Phase-2 = the atomic flip** → new code path live. This is the milestone the user cares about ("走新的代码逻辑即可").
- **Phase-3 (LAST) = the deletion-unblocking extractions (§2.4) + delete the ~90-class cyclic unit.** Deferred to the end; the exact PR split (single vs two) is decided then, not now.

Precedents kept for reference only: paimon `#64446` flip + `#64653/#64655` delete; iceberg `#64688` squash. Tracking: `apache/doris#65185`.

---

## Appendix — line-number corrections vs the 07-07 doc (all re-verified at HEAD `db27455b4e1`)

| Site | 07-07 doc | HEAD |
|---|---|---|
| `SPI_READY_TYPES` | CatalogFactory.java:49-50 | **:55-56** |
| dead `case "hms"` fallback | :133-135 | **:139-141** |
| GSON catalog/db/table remaps | :366 / :447 / :471 | **unchanged** |
| `buildDbForInit` case HMS | ExternalCatalog.java:966-968 | **:967-968** |
| PhysicalPlanTranslator scan arm | :818-846 | **:824-853** |
| PhysicalPlanTranslator hudi visitor / throws | :925-928 | **:895-943 / throws :910,:914** |
| `RuntimeTypeAdapterFactory` "labels must be unique" | fe-core :279 | **fe-common :280** |
| `CreateTableInfo` hms→engine | "missing → returns null" | **DONE: :943-947 → ENGINE_HIVE** |
| §4.1 Kerberos / §4.2 read-SPI / §4.3 freshness / §4.4 sibling / §4.5 unblockers | ⛔ TODO | **all DONE (see §1)** |
| §4.6 seams / §4.7 event / D2 cache | assumed phased before flip | **0% built (Phase 1)** |
