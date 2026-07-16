# P7 cutover — scope map & sub-batch decomposition (recon 2026-07-06)

> Produced by a 6-agent code-grounded recon (`wf_536a2968-2c8`) after the FINAL plugin-side write/read commit `0b19506acfe`. Raw findings: workflow journal + `tool-results/blgola3k6.txt` (this session). **Purpose**: correct the HANDOFF framing ("next = flip + delete") — the remaining "cutover" is the entire rest of P7 (event pipeline + heterogeneous routing split + hudi/iceberg-on-HMS delegation + write-chain retype + flip + delete + ACID gate), the largest & riskiest chunk of the whole catalog-SPI migration. **Trust HEAD, not doc line numbers.**

## What is DONE (committed, dormant)
- **P7.1** DDL/partition metadata ops (`HiveConnectorMetadata` create/drop db+table, truncate).
- **P7.3** plugin-side write transaction + committer + `HiveWritePlanProvider` + read-side ACID producer + read-txn manager (all in `fe-connector-hive`/`-hms`, `0b19506acfe`). **Dormant** — hive not in `SPI_READY_TYPES`.
- Neutral read-txn seam `QueryFinishCallbackRegistry` (fe-core `qe`) — **live**, used by legacy `HiveScanNode`.
- Generic connector write path (`UnboundConnectorTableSink → Logical/PhysicalConnectorTableSink → PluginDrivenTableSink → PluginDrivenInsertExecutor`) — **live** for iceberg/paimon/maxcompute. Paimon legacy sink **fully deleted** = cleanest precedent.

## What REMAINS (all still fe-core, blocks the flip) — 6 sub-batches

| # | Sub-batch | Size | Independent? | Gates |
|---|-----------|------|--------------|-------|
| **A** | **Per-table multi-format dispatch seam (D-020) + `tableFormatType` threading** — the KEYSTONE | XL-core | critical path | unblocks B, C, and the DLA retype in E |
| **B** | iceberg-on-HMS delegation (route ICEBERG tables to `-iceberg` via hms gateway; per-column stats SPI; extract `IcebergUtils` pure helpers) | L | rides A | delete 23 datasource/iceberg + 4 blockers |
| **C** | hudi live cutover (migrate `datasource/hudi` 15 files into `fe-connector-hudi`; dismantle `HudiDlaTable`/`HudiScanNode extends HiveScanNode`; port or fail-loud deferred incremental/schema-evo/time-travel) | L | rides A | delete `datasource/hudi` + frees `HiveScanNode`/`HivePartition`/`HMSSchemaCacheValue` |
| **D** | Event pipeline → `fe-connector-hms` (21 event classes + processor; neutral `ConnectorMetaInvalidator`; thin fe-core role-aware driver; R-010 thread lifecycle + TCCL) | L | **independent** (parallel to A/B/C) | removes `instanceof HMSExternalCatalog` poller gate + edit-log replay CCE hazard |
| **E** | DLA retype + write-chain retype + read-txn rewire (neutralize 42 `instanceof HMSExternal*` + 22 `dlaType` across 8 areas; retype `HMSExternalTable`→generic; delete legacy hive sink 6-8 files; add `ConnectorSession` query-finish seam; remove `HiveTransactionMgr` from `Env`; neutralize `HMSAnalysisTask`; relocate `BIND_BROKER_NAME`) | XL | rides A + partly D | last blocker before delete |
| **F** | Flip + GSON compat + delete legacy + gates (add `"hms"` to `SPI_READY_TYPES`; `GsonUtils` 3-factory `registerCompatibleSubtype`; staged cross-connector delete of `datasource/hive`+`hudi`+23 iceberg classes; clean-room adversarial review; ENG-1 capability-twin audit over 85 sites; GSON replay test; **ACID/event/heterogeneous docker e2e hard gate R-002**; user sign-off) | XL | strictly last | — |

## The keystone (A) — why it's first
A real `type=hms` catalog is **heterogeneous**: one catalog holds plain-hive (non-MVCC) + iceberg-on-HMS (MVCC) + hudi-on-HMS, today carried by ONE class `HMSExternalTable` + lazy `dlaType` probe + `switch(dlaType)` everywhere. After the flip, a hms catalog becomes a `PluginDrivenExternalCatalog` that wraps **ONE** connector (`PluginDrivenExternalCatalog.connector` is single; `PluginDrivenScanNode` always calls `connector.getScanPlanProvider()` no-arg). **There is no per-table connector/provider selection seam anywhere** (`Connector.getScanPlanProvider()` no-arg; `ConnectorTableHandle` empty marker, no `getTableType`; `tableFormatType` computed by connectors but DROPPED at `PluginDrivenExternalTable.toSchemaCacheValue`). So without A, iceberg-on-HMS/hudi tables cannot be routed to the right scanner and B/C/E cannot proceed. A is shared by all three formats and is a public-SPI change → settle it first.

**Trino comparison**: Trino uses **one connector per format** (separate `hive`/`iceberg`/`delta`/`hudi` catalogs) plus **table redirection** — when a hive catalog encounters an iceberg table it redirects to a configured iceberg catalog (`hive.iceberg-catalog-name`). Doris's locked decision **D-020** is the analogous idea kept inside one gateway: the `"hms"` connector (`HiveConnectorProvider`) overrides `getScanPlanProvider(handle)` and **delegates** to the `-iceberg`/`-hudi` providers by table type. So `fe-connector-hive` becomes the "hms gateway" depending on `-iceberg`/`-hudi`. Approach is locked; **open implementation decisions** (defer to A's design): discriminator shape (enum `getTableType` vs `tableFormatType` string vs per-table capability); whether `tableFormatType` is persisted in the schema-cache/gson image or recomputed.

## Highest-risk flip decision (surfaces in E/F, coupled to A)
**GSON single-row MVCC conflict**: historically hive + hudi-on-HMS + iceberg-on-HMS all persist as one `clazz=HMSExternalTable` tag. `registerCompatibleSubtype` maps one tag → ONE class, but MVCC is **subclass-gated** (`PluginDrivenMvccExternalTable extends PluginDrivenExternalTable`): plain-hive/hudi need the base class, iceberg-on-HMS needs the Mvcc subclass. Resolutions: (a) re-persist the three types under distinct discriminators during the routing split, or (b) **move MVCC-ness off the subclass to a runtime `ConnectorCapability` check** so one row targets the base class. Also: plain-hive is non-MVCC with **timestamp/lastDdlTime freshness** (`MTMVMaxTimestampSnapshot`), but `PluginDrivenMvccExternalTable.getTableSnapshot` is snapshot-id-only → needs a timestamp-freshness mode or a separate non-Mvcc MTMV-capable table.

## Recommended sequencing
`A (keystone)` → then `B` + `C` (both ride A; can overlap) and `D` (independent, parallel anytime) → `E` (retype + write-chain, rides A) → `F` (flip + delete + gates, strictly last). F carries the clean-room review + ENG-1 twin audit + GSON replay test + docker ACID e2e as the terminal hard gate (per iceberg #64688 / paimon #64446+#64653 precedent).

## Per-sub-batch open decisions (defer sign-off to each sub-batch's recon/design)
- **A**: discriminator shape (enum vs string vs capability); persist `tableFormatType` vs recompute.
- **D (OQ-EVT)**: structured register/unregister SPI seam vs degrade-to-invalidate + lazy re-list; poller location (full move vs thin fe-core role driver); keep `ExternalMetaIdMgr` for HMS or drop (ids derivable via `Util.genIdByName`); partition-NAME invalidation SPI extension vs table-level over-invalidate.
- **C (OQ-SHARE)**: delete `HiveScanNode`/`HiveSplit`/`HivePartition`/`HMSSchemaCacheValue` (paimon precedent: plugin has its own `ConnectorScanRange`) vs co-move a shared base into `fe-connector-hms`; port deferred hudi features now vs fail-loud.
- **B (OQ-COLSTATS)**: add per-column `ConnectorStatisticsOps` method to preserve `enableFetchIcebergStats` vs drop per-column HMS-iceberg stats; keep shrunk `IcebergUtils` (SDK-linked) vs extract pure helpers to make fe-core iceberg-SDK-free.
- **E**: how the connector reaches the query-finish seam (add `registerQueryFinishCallback` to `ConnectorSession` vs fe-core-driven registration); `BIND_BROKER_NAME` relocation home; clean the vestigial iceberg leftovers (`UnboundIcebergTableSink`, dead `InsertOverwrite` branch) or leave.
- **F (packaging)**: separate flip PR then delete PR (paimon model) vs combined squash (iceberg model); clean-room review before flip (iceberg) vs after flip before delete (paimon); make fe-core fully hive/hudi-SDK-free (P4 odps model) vs leave STILL-CONSUMED deps.

## Reusable (do NOT delete)
`PluginDrivenExternalCatalog/Database`, `PluginDrivenExternalTable`, `PluginDrivenMvccExternalTable`, `PluginDrivenSysExternalTable`, `ConnectorTimeTravelSpec`, `ConnectorProcedureOps`, whole write-path SPI, `DistributionSpecHiveTableSinkHash/UnPartitioned` (misleadingly hive-named but shared by the connector path), `QueryFinishCallbackRegistry`.
