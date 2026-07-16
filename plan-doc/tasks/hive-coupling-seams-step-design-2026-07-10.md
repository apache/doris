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
- [x] **S4** auto-analyze per-table gate. ✅ commit `89c6f9454bb`. **Recon RESOLVED the hidden depth:**
      iceberg-on-HMS resolves capability from the **HIVE** connector, NEVER the iceberg sibling (proven at
      HEAD by the completeness critic) — so dropping the hive connector-wide flag alone would silently
      regress iceberg-on-HMS (legacy admitted `dlaType==ICEBERG`). Fix: `supportsColumnAutoAnalyze()` →
      `hasScanCapability`; drop the hive connector-wide flag (de-admits hudi-on-HMS); emit the per-table
      marker for plain-hive; **and (user chose Option C, full parity) reflect the OWNING sibling's
      connector-wide capability set onto the delegated iceberg/hudi-on-HMS schema as a per-table marker**
      (`HiveConnectorMetadata.reflectSiblingScanCapabilities`, Trino table-redirection semantics). This
      restores iceberg-on-HMS auto-analyze AND closes the same-root-cause Top-N / nested-column-prune loss
      for iceberg-on-HMS in one place; hudi-on-HMS (sibling declares neither) is correctly withheld. 0
      checkstyle, import-gate clean, 4 suites green. Parity e2e-owed.
- [x] **S2** `hudi_meta` connector-driven. ✅ commit `d8f2d01978a`. Neutral `getMetadataTableRows` SPI
      (`List<List<String>>` — TVF owns the schema, lighter than `ConnectorProcedureResult`) +
      `SUPPORTS_METADATA_TABLE` + `HudiConnector` impl (full active timeline, TCCL-pinned) + dual-arm
      `hudiMetadataResult` (HMS arm sources from the relocated `HudiExternalMetaCache.getTimelineRows`).
      **MetadataGenerator sheds its two `org.apache.hudi` imports.** Dormant unit tests for the plugin arm.
      Timeline-row parity e2e-owed (4 p2 suites, enableHudiTest).
- [x] **S3** sample-analyze full port. ✅ commit `8469a033abd`. `SUPPORTS_SAMPLE_ANALYZE` (per-table,
      plain-hive only) + additive `canSample`/`isSamplingPartition` arms + `PluginDrivenSampleAnalysisTask`
      (verbatim `doSample`/`getSampleInfo`/`needLimit`) + `ConnectorStatisticsOps.listFileSizes` SPI +
      `getChunkSizes` override + **(user chose full parity) distribution-column port** (`DISTRIBUTION_COLUMNS_KEY`,
      fe-core lowercases) + `StatisticsAutoCollector` force-FULL refined to `&& !supportsSampleAnalyze()`.
      Sampling SQL round-trip + estimator + per-partition listing are e2e-owed.
- [x] (optional) soften `HiveConnector` W6 write-path TCCL comment (doc-only). ✅ commit `f53a71f5260`.
- [x] **clean-room adversarial review over all seam commits (S1–S4 + W6); fix confirmed findings.** ✅
      Multi-agent clean-room (`wf_498114c4-3e1`: 7 independent finders judging code cold vs each commit's pre-image +
      legacy, 3-lens adversarial verify per finding, completeness critic; 27 agents). 4 findings survived (2 refuted).
      All fixed + verified (fe-connector-hive BUILD SUCCESS, 0 checkstyle):
      - **HIGH (S2+S4 cross-cutting)** `a5015800abd`: `reflectSiblingScanCapabilities` reflects the hudi sibling's
        `SUPPORTS_METADATA_TABLE` onto the delegated per-table marker → post-flip `supportsMetadataTable()`==true and
        the `hudi_meta()`/TIMELINE gate PASSES, but `HiveConnectorMetadata` had NO `getMetadataTableRows` override
        (every other per-handle read guard-and-forwards) → a foreign `HudiTableHandle` fell through to the SPI-default
        empty list = OK-but-EMPTY timeline vs the still-live HMS arm's real one. Fix = add the guard-and-forward
        override (mirrors listFileSizes) + drive it in the delegation completeness-lock (`EXPECTED_METHODS` +
        `RecordingSiblingMetadata` + row assertion). iceberg-on-HMS unaffected (no `SUPPORTS_METADATA_TABLE`).
      - **LOW (S4 test gap)** `a5015800abd`: hudi-on-HMS auto-analyze WITHHOLDING (the headline parity claim) had no
        representative test — fixtures used an iceberg-shape sibling (already carries auto-analyze) or an empty sibling
        (isEmpty early-return), so a reflect-side over-add passed silently. Added
        `foreignHandleSchemaWithholdsAutoAnalyzeFromRealHudiSibling` (real hudi shape {METADATA_TABLE}, no auto-analyze)
        + corrected the misleading empty-sibling mutation comment.
      - **LOW (S3 test gap)** `a63ab391171`: capabilities guard omitted `SUPPORTS_SAMPLE_ANALYZE` /
        `SUPPORTS_METADATA_TABLE` connector-wide pins → declaring either connector-wide would silently over-admit
        iceberg/hudi-on-HMS. Added both assertFalse pins.
      - **LOW (S3, real error-path)** `d85c16f2cda` (**user chose fail-loud parity**): `listFileSizes` swallowed a
        listing `RuntimeException` to empty → sample `scaleFactor` collapses to 1.0 while `TABLESAMPLE` still fires →
        silent stat undercount with the task SUCCESS; legacy `HMSExternalTable.getChunkSizes` failed loud. Fix = drop
        the catch (keep the finally TCCL-restore) so the error propagates like legacy; a genuinely empty table stays
        natural-empty. Deterministic propagation unit test added (injects a throwing `HiveFileListingCache`).
      - **S1 (`partition_values`) + W6 = CLEAN** (no surviving findings; paimon/iceberg byte+cost invariance confirmed).
- [x] update HANDOFF + this doc's checkboxes; record e2e-owed rows into execution-plan §4.

## Discovered follow-ups (surfaced by recon/impl, NOT in the 3 seams — do-not-drop)
- **Delete-step build-break (out-of-seam):** `StatisticsAutoCollector.java:36` and `StatisticsCache.java:44`
  import `org.apache.hudi.common.util.VisibleForTesting` (annotation-only, wrong package). Harmless now
  (hudi still on fe-core's classpath via the delete-unit), but a compile break the moment the hudi jar
  leaves fe-core at the delete step. Swap to the guava/doris `@VisibleForTesting` then. NOT fixed here
  (unrelated to hudi_meta; surgical scope).
- **Partition-level FULL analyze (inherent to the plugin migration, not S3):** `ExternalAnalysisTask.doFull`
  (which every flipped table incl. the new sample task inherits) does not do the legacy
  `HMSAnalysisTask.doPartitionTable` partition-level analyze under `enablePartitionAnalyze`. This is a
  property of ALL plugin tables (iceberg/paimon too), not introduced by S3; track for a full-analyze parity
  pass if partition-level external analyze is required.

## e2e-owed (Phase 4, do-not-drop)
partition_values over heterogeneous HMS == legacy hive rows; hudi_meta timeline rows == the 4 p2 suites;
hive ANALYZE WITH SAMPLE FULL-vs-SAMPLE stat assertions (text + orc + partitioned + bucketed); auto-analyze
admits hive/iceberg-on-HMS but not hudi-on-HMS; iceberg-on-HMS regains Top-N lazy / nested-column prune via
the sibling-capability reflection; W6 iceberg-on-HMS write no-CCE on bundled-AWS S3.
