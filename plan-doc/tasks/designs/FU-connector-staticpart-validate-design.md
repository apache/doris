# FU-connector-staticpart-validate — static-partition validation on the flipped connector sink path

Status: IMPLEMENTED (unit + mutation pending final green); e2e flip-gated (redeploy + rerun `test_iceberg_static_partition_overwrite`).

## Symptom
`test_iceberg_static_partition_overwrite` Test Case 4 (fresh empty table):
```
INSERT OVERWRITE TABLE tb1 PARTITION (invalid_col='value') SELECT * FROM tb1
```
Expected `exception "Unknown partition column"`. Actual: statement reached **physical translation** and died with
`Cannot find snapshot with ID 5830...` (a stale snapshot from the dropped prior incarnation of `tb1`).

## Root cause (confirmed in code + FE log)
With the iceberg router flipped, an iceberg `INSERT OVERWRITE ... PARTITION(col=val)` is a
`PluginDrivenExternalCatalog` → `UnboundConnectorTableSink` → `BindSink.bindConnectorTableSink` (BindSink.java:862).
The legacy validator `BindSink.validateStaticPartition` (BindSink.java:810, `"Unknown partition column"` /
`"Cannot use static partition syntax for non-identity partition field"`) is only wired to the dead
`bindIcebergTableSink`/`UnboundIcebergTableSink` path (`instanceof IcebergExternalTable` is false at runtime).
`bindConnectorTableSink` **materializes** static partition values but never **validates** them — the materialize
block silently skips an unknown column (`if (column != null)`), so `invalid_col` slips through to scan planning.

- **D1 (primary, fixed):** missing static-partition validation on the connector sink path.
- **D2 (secondary, NOT fixed — artifact of D1):** the invalid statement, allowed to proceed, reads a
  freshly-recreated empty `tb1` and pins a stale snapshot from the dropped incarnation. Confirmed narrow: both
  observed `Cannot find snapshot` errors are the **same** TC4 statement (`SqlHash=9fce206b...`); every valid case
  in the suite writes before it reads, so none pin a stale snapshot. Fixing D1 (fail loud at analysis, before the
  scan is planned) makes D2 unreachable for this suite. D2 flagged for separate follow-up.

## Fix (neutral SPI, connector-side — per ironclad rule: no `instanceof Iceberg*` in fe-core)
Mirrors the existing `ConnectorWriteOps.validateRowLevelDmlMode` precedent exactly (analysis-time neutral SPI, the
iceberg property knowledge + message live in the connector, `DorisConnectorException` → `AnalysisException`).

1. **fe-connector-api** `ConnectorWriteOps.java`: new default no-op
   `validateStaticPartitionColumns(session, handle, List<String> names)`.
2. **fe-connector-iceberg** `IcebergConnectorMetadata.java`: `@Override` — load table, `PartitionSpec`, and for
   each name reproduce the legacy checks byte-for-byte (unpartitioned / unknown field / non-identity transform),
   keyed by partition **field** name (`category_bucket` for `bucket(4,category)`), throwing `DorisConnectorException`.
3. **fe-core** `BindSink.java`: in `bindConnectorTableSink`, after `staticPartitionColNames`, call new private helper
   `checkConnectorStaticPartitions(...)` (plumbing mirrors `IcebergRowLevelDmlTransform.checkPluginMode`): resolve
   connector/session/handle, delegate the spec checks, convert `DorisConnectorException` → `AnalysisException`; the
   connector-agnostic literal-value check (`PARTITION value must be a literal`) stays here where the Nereids
   expression is available. Runs only when the PARTITION clause is non-empty.

## Verification
- Unit: `IcebergConnectorMetadataTest` +5 (`validateStaticPartitionColumns*`): identity accepted; unknown col
  (incl. bucket source col `id`) → "Unknown partition column"; `id_bucket` → non-identity; unpartitioned → "is not
  partitioned"; empty → no-op (no table load). Each carries WHY + MUTATION.
- Modules compile: fe-connector-api, fe-connector-iceberg, fe-core.
- e2e flip-gated (redeploy): rerun `test_iceberg_static_partition_overwrite` (TC4 + non-identity family TC29–38).

## TC20 column-count message — RESOLVED (user chose option A, 2026-07-01)
`INSERT OVERWRITE PARTITION(...) select * ... where 1=0` expects `"Expected 2 columns but got 4"`, but the
connector path's count-check (`bindConnectorTableSink`) threw the terser `"insert into cols should be corresponding
to the query output"` — dropping the "Expected N columns but got M" detail that legacy and the sibling count-check
in the same file emit. Fix: restore the detail on the connector-path count-check (BindSink.java, the
`boundSink.getCols().size() != child.getOutput().size()` guard just before `requiresFullSchemaWriteOrder()`).
Connector-agnostic, one message string; verified by the same regression suite (flip-gated rerun).

## Watch (separate, flagged — NOT in this fix)
- **D2 stale snapshot on read-after-drop-create** (latent): reading a freshly-created empty table whose name
  previously held a table with snapshots pins the stale snapshot. Narrow (this suite never exercises it in a
  valid path); needs its own investigation. Flagged, not fixed.
