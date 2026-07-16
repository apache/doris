# HMS cutover §4.4 S6 — name-based divert + capability residuals: findings (2026-07-08)

> Code-grounded recon (`wf_6ac0e047-768`: 4 dimension readers — buildTableDescriptor / view-family / other-no-handle / capability-residuals — + a completeness/correctness critic that independently re-verified every call against HEAD). Authoritative scope for §4.4 S6. Trust HEAD over line numbers.

---

## Verdict (headline)

**S6 requires ZERO new divert code.** Every no-handle *read* method resolves to **NO_DIVERT**; the only per-table divergences are **write/DDL residuals** or the **connector-wide capability residual**, none of which is a "thin name-based read divert". With S0–S5 done, the **§4.4 read-delegation spine is COMPLETE**. S6's deliverable is this document + the HANDOFF residual list — no connector code change.

This CORRECTS the decomposition's S6 sketch ("re-run `detect(db,table)` and forward view family + `buildTableDescriptor` for ICEBERG"): each of those diverts is proven unnecessary or actively harmful below.

## Why each no-handle method is NO_DIVERT

- **`buildTableDescriptor` — NO_DIVERT (byte-identical, a divert is pure waste).** Hive's override is format-agnostic: it unconditionally emits `TTableType.HIVE_TABLE` + `THiveTable(db,table,{})` (`HiveConnectorMetadata.java:382-390`). The sibling is **always** the hms flavor (`IcebergSiblingProperties.synthesize` forces `iceberg.catalog.type=hms`), so iceberg's `buildTableDescriptor` takes its **hms branch** (`IcebergConnectorMetadata.java:677-684`) emitting the **identical** construction — it never reaches the `ICEBERG_TABLE` branch. **Legacy parity confirmed**: `HMSExternalTable.toThrift` emitted `HIVE_TABLE` **unconditionally**, incl. an iceberg-on-HMS table (`dlaType==ICEBERG`). BE does not consult the descriptor table-type for an iceberg-on-HMS scan (JNI scan; iceberg-ness rides the per-file format params). A divert would add a fresh `hmsClient.getTable(detect)` per `toThrift` purely to reproduce identical bytes. **Keep the existing hive override** (it beats the SPI-default `null → SCHEMA_TABLE` fallback); add no fork.
- **View family `viewExists` / `getViewDefinition` / `dropView` — NO_DIVERT (legacy-parity AND undiscriminable name-only).** Two independent reasons:
  1. **Parity.** For the supported iceberg-on-HMS **TABLE** (`table_type=ICEBERG`, no view text), `viewExists`=`isViewTable(getTable)` keys on HMS view-text presence → **false**, keeping it on the table path where `getTableHandle` already diverts (S4). A hive view → true → hive view path. An iceberg-on-HMS **VIEW** stores a **hive-dialect SQL** in HMS `viewOriginalText` (iceberg `HiveViewOperations.newHMSView`), so it is served through the hive view path — byte-parity with legacy `HMSExternalTable.isView`/`getViewText` (which was view-text-only, dlaType-independent). *(This last is a runtime HMS-metadata fact about the bundled `iceberg-hive-metastore` lib, PLAUSIBLE but not verifiable from fe HEAD; it does not change the conclusion either way.)*
  2. **Undiscriminable.** A name-only method has no handle, and `HiveTableFormatDetector.detect` returns **UNKNOWN** for an iceberg view (its `table_type='iceberg-view'` ≠ `'ICEBERG'`, and a view has no data input format). So the "re-run detect and forward" strategy that works for TABLES **cannot classify a view at all**.
- **`listViewNames` — NO_DIVERT (a divert would REGRESS).** Deliberately un-overridden: hive `listTableNames` (HMS `get_all_tables`) already includes every `VIRTUAL_VIEW`, so the SPI-default empty makes fe-core's `addAll` merge a no-op (each view listed once = legacy parity). Overriding to the sibling (`IcebergConnectorMetadata.listViewNames` → `ViewCatalog.listViews`) would **double-list** every view.
- **`getTableComment` — NO_DIVERT (parity; a divert would REGRESS).** Hive does not override it → SPI default `""`. Legacy `HMSExternalTable.getComment()` returned `""` **unconditionally** for all HMS tables incl. iceberg-on-HMS → exact byte-parity. Iceberg's own override returns the real comment, so a divert would surface a comment **legacy never showed**.
- **`getDatabase` / `listDatabaseNames` / `databaseExists` / `listTableNames` / `getProperties` / `supportsCreateDatabase` / `createDatabase` / `getPrimaryKeys` — NO_DIVERT (format-agnostic or inert).** All db/catalog/connector-level; the shared HMS already yields the mixed-catalog union and the sibling wraps the same HMS. Diverting `listTableNames` would **regress** (iceberg's version subtracts views). `getPrimaryKeys` is SPI-default-empty on both connectors (a divert can't change the answer).

## Residuals for the flip (document, do NOT build in S6)

1. **`dropDatabase` FORCE cascade — write/DDL residual.** The force branch raw-`hmsClient.dropTable`s each table (no iceberg purge/cleanup). **Legacy parity** (`HiveMetadataOps.dropDbImpl → dropTableImpl(true) → client.dropTable`, no purge) — the HMS entry is removed correctly, it only *leaks* iceberg data/metadata files vs the sibling's `purge=true`. Correcting it needs per-table detect+sibling-forward+location-cleanup → belongs with the **write/DDL delegation substep**, not a thin read divert.
2. **`createTable` engine-routing — write/DDL residual.** Always builds a hive HMS table; a NEW table has nothing to `detect()`. Hive-vs-iceberg CREATE routing is a request/engine decision = write/DDL substep.
3. **`beginTransaction` / `getWritePlanProvider` — write delegation residual.** `HiveConnector.getWritePlanProvider()` has **no per-handle variant** (unlike `getScanPlanProvider(handle)`); routing per-table writes/EXECUTE to an iceberg sibling needs new **per-handle SPI overloads** (see decomposition §6). Separate substep after the read spine.
4. **`SUPPORTS_COLUMN_AUTO_ANALYZE` over-admits hudi-on-HMS — capability residual (already documented at `HiveConnector.java:139-144`).** `getCapabilities` is connector-wide (one EnumSet, no handle) → cannot be per-handle-gated. Legacy `StatisticsUtil.supportAutoAnalyze` admitted HIVE+ICEBERG but **not** HUDI; the connector-wide flag admits hudi too. Cost-only (extra background FULL auto-analyze), not a correctness bug; no per-handle escape. Gate per-handle or explicitly accept at the iceberg/hudi delegation substep.
5. **`supportsLatestSnapshotPreload` under-admits iceberg/hudi-on-HMS — capability-shaped residual (NOT enum-backed).** fe-core `TableIf` default `false`; `PluginDrivenExternalTable` does not override it. Legacy `HMSExternalTable` returned true for HUDI+ICEBERG, false for HIVE → the plugin **under-admits** iceberg/hudi (loses a pre-lock latest-snapshot preload). **Latency-only, no correctness effect** (opt-in via `enable_preload_external_metadata`). Wiring it needs a new capability or per-handle sibling delegation → deferred to the iceberg/hudi delegation substep.
6. **First-class iceberg-on-HMS VIEWS — explicit NON-GOAL at the flip.** Legacy never surfaced iceberg ViewCatalog views through the HMS catalog (only via the separate native `type=iceberg` catalog). Adding them to the mixed catalog is a NEW feature (needs a view-aware discriminator such as `table_type='iceberg-view'` + sibling `viewExists`/`loadView` routing), not an S6 residual.

## Other capability flags — parity, no residual

- **`SUPPORTS_VIEW`**: machinery-only; the per-table `isView()` is resolved by `viewExists` (already correct). No divergence.
- **`SUPPORTS_METADATA_PRELOAD`**: legacy returned true unconditionally → exact parity; opt-in, no correctness.
- **`SUPPORTS_MVCC_SNAPSHOT`**: admission-parity (all legacy HMS tables are `MvccTable`); per-table snapshot semantics handled downstream in `beginQuerySnapshot` (per-handle, already diverted).

## References
- Recon: `wf_6ac0e047-768` (full output `tasks/w4stwou6n.output`). Decomposition: `hms-cutover-sibling-connector-decomposition-2026-07-08.md` (§3 S6 sketch corrected here). Design: `hms-cutover-retype-design-2026-07-07.md`.
- Tracking: apache/doris#65185.
