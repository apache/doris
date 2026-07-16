# HMS SPI cutover — the atomic flip (2026-07-10)

> Phase 2 of the HMS cutover: route catalog type `hms` (plain-hive + iceberg-on-HMS + hudi-on-HMS) through the
> plugin SPI so all production flows use the new connector code. **No legacy code deleted** — that is Phase 3.
> Grounded on HEAD by two recon workflows (`wf_c0e650aa-b62` flip-site grounding + 2 adversarial critics;
> `wf_78b374d6-a29` full instanceof-surface classification + force-init design). The execution-plan doc
> (`hms-cutover-execution-plan-2026-07-10.md` §3) was the starting checklist but was **corrected** here — see §"Corrections".

---

## 1. What shipped

Two dormant prerequisites (event sync), then the atomic flip, then the view-gate cleanup.

### Commit A — follower event-cursor feed → new driver (dormant)
`ExternalMetaIdMgr.replayMetaIdMappingsLog` fed the **legacy** `MetastoreEventsProcessor.updateMasterLastSyncedEventId`.
Post-flip a hive catalog is a `PluginDrivenExternalCatalog` driven by the new `MetastoreEventSyncDriver`; without
repointing, its `masterLastSyncedEventIdMap` never populates → `masterUpperBound` stays -1 → `HmsEventSource.pollForFollower`
returns nothing → **followers silently stop applying incremental metastore changes**. Now dual-armed:
`PluginDrivenExternalCatalog` → new driver, else legacy processor (both key by catalogId, no HMS cast). Dormant pre-flip.

### Commit B — master/follower force-init hook (dormant)
`MetastoreEventSyncDriver.realRun` skipped un-initialized catalogs (to keep idle paimon/iceberg inert), so a flipped
hms catalog **never queried on an FE never seeds its cursor → never event-synced**. Added a per-cycle force-init gated
on the pre-init `getType()=="hms"` (reads `catalogProperty`, no `makeSureInitialized`), on every FE (no isMaster gate —
each FE needs the catalog initialized to obtain its event source / seed its own cursor / forward `REFRESH CATALOG`),
one-shot via `!isInitialized()`, swallow-on-throw like the legacy poller. Dormant pre-flip.

### Commit C — THE ATOMIC FLIP
- `CatalogFactory`: `SPI_READY_TYPES += "hms"`; remove the dead `case "hms" → new HMSExternalCatalog` + its import.
- `GsonUtils`: three `registerSubtype` → `registerCompatibleSubtype` remaps (catalog→`PluginDrivenExternalCatalog`,
  db→`PluginDrivenExternalDatabase`, **table→`PluginDrivenMvccExternalTable`** — the hive connector declares
  `SUPPORTS_MVCC_SNAPSHOT`, matching paimon/iceberg); remove the three orphaned HMS imports. Locked with `SPI_READY`
  (label-uniqueness static-init throw + write-registration) → one commit.
- `ExternalCatalog.buildDbForInit`: HMS arm → `new PluginDrivenExternalDatabase` (mirrors the ICEBERG arm; replay-defense).
- `HmsGsonCompatReplayTest` (new): pins that legacy `HMSExternal{Catalog,Database,Table}` tags replay as the PluginDriven
  classes (table → the MVCC variant, exact-class assert). Mirrors `Iceberg`/`PaimonGsonCompatReplayTest`.
- Disable 3 legacy fe-core tests (`HmsCatalogTest`, `HmsQueryCacheTest`, `HiveDDLAndDMLPlanTest`) — they create a
  `type=hms` catalog via the **routed** path (no hms provider on the fe-core test classpath → throws) and assert legacy
  `HMSExternal*` behavior. `@Disabled` with a Phase-3 pointer (removed with the legacy subsystem). **User sign-off:
  disable + defer (2026-07-10).**

### Commit D — view-gate cleanup (D5)
Post-flip a hive view is `PLUGIN_EXTERNAL_TABLE` → hits the shared plugin-view arm in `BindRelation` (today
iceberg-worded). Removed the `enable_query_iceberg_views` config gate (served unconditionally), neutralized the
"iceberg view not supported with snapshot time/version travel" message → "view not supported ..."; deprecated both
`enable_query_hive_views`/`enable_query_iceberg_views` `@ConfField`s to retained no-ops; updated the 16 assertions in
`test_iceberg_view_query_p0.groovy`. **Visible iceberg behavior change — ships with the flip. User sign-off: do now (2026-07-10).**

---

## 2. Corrections to the execution-plan §3 checklist (verified against HEAD)

- **"Rewire the 4 gates" is WRONG.** Both `instanceof HMSExternalCatalog` gates (`MetastoreEventsProcessor:116`,
  `ExternalMetaCacheRouteResolver:66`) must be **LEFT** — they self-exclude a flipped PluginDriven catalog and stay
  correct for any still-legacy HMS catalog; deleting them breaks legacy sync/invalidation. The **cache** path
  auto-engages (connector `CachingHmsClient` + base metaCache; invalidation via PluginDriven overrides) — zero edits,
  co-proven by paimon/iceberg. The **event** path needed the two ADD-feeds above, not gate removal.
- **Dead Nereids arms are NOT removed in the flip.** The iceberg flip (precedent, in HEAD) left its dead
  `PhysicalPlanTranslator`/`IcebergScanNode` arms; they + the hms/hudi arms + the legacy `BindRelation`
  `HMS_EXTERNAL_TABLE` arm + `CheckPolicy` hudi arm are all dead-harmless post-flip and are removed together in Phase 3.
  (Keeps this flip minimal per "其他的先不动".)
- **Phase-1 was already DONE at HEAD** (event Model B + connector cache D2 + R1–R4), contrary to the plan doc's stale §2.

## 3. instanceof-HMS surface (full classification — `wf_78b374d6-a29`)
15 DUAL_ARMED_SAFE (write/DDL sinks, INSERT OVERWRITE, CREATE TABLE engine, TVFs, SHOW CREATE DB, MetadataGenerator),
9 LEAVE_DEAD_HARMLESS (legacy caches/scan internals — Phase 3), 5 ALREADY_COVERED_BY_SEAM (S1/S3/S4 + MaterializeProbe),
and 3 SINGLE_ARMED read-path items that are **plugin-model parity, not bugs** (accepted, no action):
- `BindRelation:729` — flipped hive loses SQL-result-cache eligibility → **same as paimon/iceberg-native** (cache off,
  never stale, plan §6).
- `ShowPartitionsCommand:154/222` — WHERE/ORDER-BY column strictness → flipped hive now matches already-flipped
  paimon/iceberg (which skip the guard).
`RefreshManager:216` was flagged by a critic as a partition-invalidation regression but is **NOT** — the live
event-driven path (`refreshPartitions:307-311`) is already flip-aware (partition-granular `connector.invalidatePartition`),
and the replay fallback (`refreshTableInternal:268-270`) is connector-aware full-table invalidation (safe superset).

---

## 4. Verification
- `mvn -pl fe-core -am test-compile`: **BUILD SUCCESS, 0 Checkstyle** (whole reactor).
- Targeted `mvn test` (17 run, 0 failures, 0 errors, 1 skipped): `HmsGsonCompatReplayTest` 3/3,
  `IcebergGsonCompatReplayTest` 3/3, `PaimonGsonCompatReplayTest` 3/3, `ExternalMetaCacheRouteResolverTest` 7/7 green
  (still routes a directly-constructed legacy `HMSExternalCatalog` to hive+hudi+iceberg); `HmsCatalogTest` **skipped** (@Disabled).
- Clean-room adversarial review (`wf_728cad25-62a`, 4 independent reviewers + per-finding adversarial verify):
  **CLEAN** — 1 finding, 0 confirmed real. The one minor finding (flipped hive loses Nereids SQL-result-cache
  eligibility at `BindRelation:729`) was verified **by-design**: fail-safe, `enable_hive_sql_cache` defaults off,
  parity with paimon/iceberg-native, and acknowledged by the disabled `HmsQueryCacheTest` (§3 above).
- e2e is owed to the user (they run it). See §5.

## 5. e2e owed (user runs)
Heterogeneous `type=hms` catalog: plain-hive + iceberg-on-HMS + hudi-on-HMS in one session; hive/iceberg/hudi read,
write, DDL, procedure, MTMV freshness, time-travel, `@incr`; **follower event-sync staleness** (the new follower feed
has never run for any catalog — hms is the first event consumer); Kerberos-HMS smoke; upgrade-image GSON replay;
`partition_values()`/`hudi_meta()`/sample-analyze/auto-analyze coupling-seam rows; hive view query (now served
unconditionally). Full matrix: `hms-cutover-execution-plan-2026-07-10.md` §4/§5.

## 6. NOT done (Phase 3, deferred)
Delete the ~90-class legacy cyclic unit (`datasource/hive|hudi|iceberg`), the dead Nereids arms + INC-5 stale throws +
`CheckPolicy` hudi arm + legacy `BindRelation` HMS arm, the deletion-unblocking extractions (`HiveUtil`/`HiveSplit`/
`IcebergUtils`), and the 3 disabled tests. Ordering + set: execution plan §2.4/§3/§4.
