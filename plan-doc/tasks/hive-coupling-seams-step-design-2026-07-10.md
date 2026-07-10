# Hive coupling-seams step — design (2026-07-10)

> Phase-1 remaining fe-core build-out after D2 cache + event-pipeline landed. Authoritative plan =
> `hms-cutover-execution-plan-2026-07-10.md` §2.3 (loud-break seams) + §2.5 (W6). This doc distills the
> HEAD-grounded recon (`wf_dfe1cb86-df4`: 4 seam readers + completeness critic) and records the user's
> three parity decisions (2026-07-10). **Trust HEAD, not these line numbers — re-verify on edit.**
> Every seam ships as an INDEPENDENT dormant commit (inert while hms is legacy), same discipline as the
> connector steps. Clean-room adversarial review at the end.

## Why these exist

At the atomic flip a hms table becomes a `PluginDrivenMvccExternalTable` (type `PLUGIN_EXTERNAL_TABLE`),
not an `HMSExternalTable`. fe-core sites that `instanceof HMSExternalTable` / cast / gate on
`HMSExternalCatalog` break — loud (throw) or silent (wrong result / degrade). We stage the fixes dormant
now so the flip commit only has to swap gates, never grow behavior.

## User decisions (2026-07-10) — all chose FULL PARITY

1. **Timeline TVF (`hudi_meta`)** → **KEEP, rework connector-driven.** (Recon overturned the plan's DROP
   lean: 4 p2 hudi suites consume it — `test_hudi_meta` asserts it directly; `test_hudi_incremental` /
   `test_hudi_partition_prune` / `test_hudi_timetravel` use it as a commit-timestamp source.)
2. **Sampled ANALYZE (`ANALYZE … WITH SAMPLE`) on hive** → **FULL PORT** so hive keeps working (today it
   works via `HMSAnalysisTask.doSample`; a no-op flip would make it throw `DdlException`, unlike a silent
   FULL fallback the plan assumed). Chose parity over the recon's "accept degrade" lean.
3. **Background column auto-analyze eligibility** → **per-table gate excluding hudi-on-HMS**, exactly like
   legacy (`dlaType HIVE || ICEBERG`, HUDI excluded). Mirrors the Top-N / nested-prune per-table pattern.

W6 (write-path TCCL) = **verified false gap, no code** (pin already lives on the iceberg sibling's
`TcclPinningConnectorContext`, `IcebergConnector.java:174`, threaded through hive's per-handle delegation).
Only owes an e2e. Optional: soften the over-cautious comment at `HiveConnector.java:206-208`.

---

## Seam 1 — `partition_values()` TVF (loud break; LIVE for paimon/iceberg once landed)

**Break:** `PartitionValuesTableValuedFunction.analyzeAndGetTable` gate (`:113`) throws
`"Catalog of type 'hms' is not allowed in ShowPartitionsStmt"` for a `PluginDrivenExternalCatalog`;
downstream casts to `HMSExternalTable` (`:130-133`, `:170`) would CCE; `MetadataGenerator`
`partitionValuesMetadataResult` switch (`:2090`) has only `HMS_EXTERNAL_TABLE`.

**Fix = mirror the already-done `$partitions` TVF** (`PartitionsTableValuedFunction` gate `:172-176`,
allowed-types `:184-186`, plugin arm `:201-209`; `MetadataGenerator.dealPluginDrivenCatalog`). Edits:
- (A) gate `:113` → add `|| catalog instanceof PluginDrivenExternalCatalog`.
- (B) `getTableOrMetaException` `:124-125` → add `TableType.PLUGIN_EXTERNAL_TABLE`.
- (C) `:130-136` → add a `PluginDrivenExternalTable` arm doing `isPartitionedTable()` (no HMS cast);
  keep the HMS arm.
- (D) `getTableColumns` `:170` → hoist to base `((ExternalTable) table).getPartitionColumns(
  MvccUtil.getSnapshotFromContext(table))` (`ExternalTable.getPartitionColumns(Optional<MvccSnapshot>)`
  `:468`) — resolves for both legacy HMS and plugin without a source cast, no branch.
- (E) `MetadataGenerator` `:2090` → add `case PLUGIN_EXTERNAL_TABLE ->
  partitionValuesMetadataResultForPluginTable(table, colNames)`; new method feeds the EXISTING TCell
  type-switch (`:2144-2181`).
- **Values source (Opt B, chosen):** add SPI method `PluginDrivenExternalTable.getNameToPartitionValues(
  Optional<MvccSnapshot>) : Map<String,List<String>>` (name → per-column values in partition-column
  order), refactor the extraction loop already in `getNameToPartitionItems` (`:753-764`) so both share it.
  Keeps `MetadataGenerator` symmetric with the HMS arm (`getHivePartitionValues().getNameToPartitionValues()`).

**Byte-parity:** the new arm must map `null` / `TablePartitionValues.HIVE_DEFAULT_PARTITION` → NULL TCell
(HMS path `:2140`) and preserve partition-column ORDER. For paimon/iceberg this is a NEW capability (no
parity target, just correctness); for hive it is e2e-owed post-flip.

**Not dormant:** paimon/iceberg are already `PluginDrivenExternalCatalog`, so edits A/C/E go LIVE for them
at merge — this is a deliberate expansion consistent with `$partitions` (which already did it). ⇒
**unit/regression-testable NOW** against paimon/iceberg (partitioned table returns rows; unpartitioned
throws "not a partitioned table"). Iron rules: dispatch on `PluginDrivenExternalCatalog`/base
`ExternalTable`, never `instanceof HMSExternal*`; no property parsing (values come from connector
`listPartitions`).

## Seam 2 — `hudi_meta()` / TIMELINE TVF (silent break + delete-time compile break) → KEEP connector-driven

**Break:** `MetadataGenerator.hudiMetadataResult` gate `:459` `!(dorisTable instanceof HMSExternalTable)`
→ post-flip returns `"The specified table is not a hudi table"`; body casts (`:463`) and reaches
`ExternalMetaCacheMgr.hudi(...).getHoodieTableMetaClient(...).getActiveTimeline()` (deletion-unit classes)
+ imports `org.apache.hudi` timeline classes into fe-core (`:128-129`, used only here `:469,:473`).

**Fix (KEEP):** add a connector-neutral metadata-rows SPI (mirror `ConnectorProcedureResult` row-return),
e.g. `ConnectorCapability.SUPPORTS_METADATA_TABLE` + a connector method returning neutral rows; implement
in `HudiConnector` (timeline data already connector-side: `HudiMetaClientExecutor` /
`HoodieTableMetaClient.getActiveTimeline().getInstants()`). Rewrite `hudiMetadataResult` to gate on the
generic plugin/capability type and delegate; drop the `HMSExternalTable` cast and the `org.apache.hudi`
imports from fe-core. Dormant: while legacy no hms table is `PluginDriven`, so the old HMS arm still
serves; at the flip the plugin arm activates. Iron rules: timeline iteration + parsing stays in
`HudiConnector` (fe-core sheds `org.apache.hudi`); pin TCCL on the delegated read (bundled hudi
reflection). Parity target = the 4 p2 suites' rows (`timestamp/action/state/state_transition_time`) —
e2e-owed (enableHudiTest). **The old body is removed at the delete step regardless.**

## Seam 3 — `ANALYZE … WITH SAMPLE` on hive → FULL PORT (3 coordinated pieces)

**Break:** flipped hive table is `PluginDrivenMvccExternalTable` → `AnalysisManager.canSample` (`:1480`,
HMS arm `:1484-1485` casts + `getDlaType()==HIVE`) returns false → `buildAndAssignJob` (`:224`) throws
`DdlException("… doesn't support sample analyze.")`. Today hive WITH SAMPLE WORKS (via
`HMSAnalysisTask.doSample` `:218-270` + `getSampleInfo` `:344-379` reading `getChunkSizes` `:972-981`).
A naïve `canSample=true` only converts the clean build-time error into a runtime
`NotImplementedException` from `ExternalAnalysisTask.doSample` (`:119`) / `ExternalTable.getChunkSizes`
(`:420`). Also `AnalyzeTableCommand.isSamplingPartition` (`:315`, `:322`) degrades (critic) — port too.

**Fix (port), all connector-agnostic:**
1. `ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE` (new). Hive emits it PER-TABLE (marker path) for its
   plain-hive tables only — legacy gated `dlaType==HIVE`, so iceberg-on-HMS / hudi-on-HMS excluded; iceberg/
   paimon-native withhold it (keep their current reject → cross-connector unchanged).
2. `AnalysisManager.canSample` arm: `table instanceof PluginDrivenExternalTable &&
   ((PluginDrivenExternalTable) table).supportsSampleAnalyze()` where `supportsSampleAnalyze()` uses the
   existing `hasScanCapability`/`PER_TABLE_CAPABILITIES_KEY` path (mirror `supportsTopNLazyMaterialize`).
   Same treatment for `isSamplingPartition`.
3. `PluginDrivenExternalTable.createAnalysisTask` → return a sample-capable task (port
   `HMSAnalysisTask.doSample`+`getSampleInfo`+`needLimit`); `PluginDrivenExternalTable.getChunkSizes`
   override → real per-file byte sizes via a new `Connector` chunk-sizes SPI (connector supplies raw
   byte lengths like `HMSExternalTable.getChunkSizes`; Doris-type slot-width math stays fe-core).

**Not dormant-unit-testable end-to-end** (issues real sampling SQL) → e2e-owed on heterogeneous HMS.
Iron rules: capability must be per-table (a connector-wide flag would source-branch by proxy and admit
iceberg/hudi-on-HMS); connector returns raw facts, fe-core does type math.

## Seam 4 — background column auto-analyze eligibility → per-table gate (exclude hudi-on-HMS)

**Break (silent, documented residual):** post-flip `StatisticsUtil.supportAutoAnalyze` (`:989`; dead HMS
arm `:1008-1011` = `HIVE||ICEBERG`) resolves via `PluginDrivenExternalTable.supportsColumnAutoAnalyze()`
(`:223-230`) which reads CONNECTOR-WIDE `SUPPORTS_COLUMN_AUTO_ANALYZE` (`HiveConnector.getCapabilities`
`:278`). Connector-wide can't express the legacy per-dlaType gate → declaring it admits hudi-on-HMS
(legacy excluded) = silent expansion; withholding it drops plain-hive = silent degrade. Comment
`HiveConnector.java:247-249` already flags it as "residual … gate per-handle or explicitly accept".

**Fix (per-table, chosen):** change `supportsColumnAutoAnalyze()` to resolve via `hasScanCapability(
SUPPORTS_COLUMN_AUTO_ANALYZE)` (additive: native iceberg/paimon still declare it connector-wide →
unchanged); REMOVE it from `HiveConnector`'s connector-wide `EnumSet` and instead emit the per-table
marker for hive-type + iceberg-on-HMS tables, NOT hudi-on-HMS. Iron rules: connector decides per-table by
emitting the marker; fe-core never inspects dlaType/format. Byte-parity for iceberg/paimon-native = they
keep the connector-wide flag → unchanged. e2e-owed: hudi-on-HMS NOT auto-analyzed, hive/iceberg-on-HMS are.

---

## TODO (each = independent dormant commit; re-verify line #s at edit)

- [x] **S1** `partition_values` plugin arm (edits A–E + `getNameToPartitionValues` SPI). ✅ commit
      `166515cdc88`. fe-core BUILD SUCCESS + 0 checkstyle + import-gate clean. Functional test (paimon/
      iceberg live rows; hive post-flip == legacy) = e2e-owed. **Additive for paimon/iceberg**, dormant hive.
- [ ] **S4** auto-analyze per-table gate (`supportsColumnAutoAnalyze` → `hasScanCapability`; hive emits
      per-table marker, drops connector-wide flag). ⚠ **INVESTIGATE FIRST** (hidden depth): post-flip do
      iceberg-on-HMS / hudi-on-HMS tables resolve `SUPPORTS_COLUMN_AUTO_ANALYZE` from the HIVE connector or
      from their delegated sibling connector? Legacy admitted `dlaType==ICEBERG` too. If iceberg-on-HMS
      already gets it via the iceberg sibling's connector-wide flag, then hive only needs to emit the marker
      for plain-hive (and NOT hudi), and the "exclude hudi" fix reduces to: does the hudi sibling declare
      it? Nail this before editing. Per-table marker precedent = `HiveConnectorMetadata.java:402-414`
      (`SUPPORTS_TOPN_LAZY_MATERIALIZE`).
- [ ] **S2** `hudi_meta` connector-driven (neutral metadata-rows SPI + `HudiConnector` impl + rewrite
      `hudiMetadataResult`, shed `org.apache.hudi` from fe-core). Dormant unit test for the plugin arm.
- [ ] **S3** sample-analyze full port (capability + per-table `supportsSampleAnalyze`/`isSamplingPartition`
      arms + sample-capable plugin task + chunk-sizes SPI + hive marker/impl).
- [ ] (optional) soften `HiveConnector.java:206-208` comment (W6 doc-only).
- [ ] clean-room adversarial review over all seam commits; fix confirmed findings.
- [ ] update HANDOFF + this doc's checkboxes; record e2e-owed rows into execution-plan §4.

## e2e-owed (Phase 4, do-not-drop)
partition_values over heterogeneous HMS == legacy hive rows; hudi_meta timeline rows == the 4 p2 suites;
hive ANALYZE WITH SAMPLE FULL-vs-SAMPLE stat assertions; auto-analyze admits hive/iceberg-on-HMS but not
hudi-on-HMS; W6 iceberg-on-HMS write no-CCE on bundled-AWS S3.
