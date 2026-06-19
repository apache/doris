# FIX-R1-TABLE — Summary

> Source finding: `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §R1 (table) (MINOR, regression).
> Design: `FIX-R1-TABLE-design.md`. Design red-team: `wf_19fd7785-165` (2 lenses, finder→verifier, 0 actionable).

## Problem

`PluginDrivenExternalCatalog.createTable` (the **generic** SPI bridge for all `SPI_READY_TYPES` —
paimon/maxcompute/es/jdbc/trino) reported `ERR_TABLE_EXISTS_ERROR` (MySQL errno **1050** / SQLSTATE `42S01`)
only for the **local-cache-conflict** arm. A table existing **only remotely** (absent from this FE's cache),
created without `IF NOT EXISTS`, fell through to `metadata.createTable` → `DorisConnectorException` →
re-wrapped as a **generic** `DdlException` (errno `ERR_UNKNOWN_ERROR` = 0). CREATE still failed; only the
error code / SQLSTATE / message regressed.

## Root Cause

The bridge ported only the **local** arm's 1050 report. Both legacy ops report 1050 for **both** arms:
legacy paimon `PaimonMetadataOps.performCreateTable` (remote `:195`, local `:212`) and legacy maxcompute
`MaxComputeMetadataOps.createTableImpl` (remote `:184`, local `:195`, verified via git). So 1050-for-remote
is exact parity for both live cut-over connectors.

## Fix

`PluginDrivenExternalCatalog.java` — dropped the `if (localExists)` guard. Reaching that point already
guarantees `(remoteExists || localExists) && !isIfNotExists`, so `ErrorReport.reportDdlException(
ERR_TABLE_EXISTS_ERROR, tableName)` now runs unconditionally there, short-circuiting **before**
`metadata.createTable`. Comment rewritten to document both arms + the legacy parity refs.

### Behavioral delta
- **remote-only existing table, no IF NOT EXISTS:** generic `DdlException` → `DdlException` with errno **1050**
  + "Table '<t>' already exists" (legacy parity); `metadata.createTable` no longer called (it only threw).
- **local conflict / IF NOT EXISTS (CTAS no-INSERT) / create-succeeds:** unchanged.
- **es/jdbc/trino (non-create-overriding):** a `CREATE TABLE` on an *existing* table now surfaces 1050 instead
  of the old fall-through error — benign/arguably more accurate; non-existing-table path unchanged.

## Tests (`PluginDrivenExternalCatalogDdlRoutingTest`)

- **Rewrote** `testCreateTableExistingTableWithoutIfNotExistsStillErrors` →
  `testCreateTableExistingRemoteTableWithoutIfNotExistsReportsErrno1050`: it previously encoded the **buggy**
  fall-through (`verify(metadata).createTable`); now asserts `getMysqlErrorCode() == ERR_TABLE_EXISTS_ERROR`
  + `verify(metadata, never()).createTable` + no editlog.
- **Strengthened** `testCreateTableLocalConflictWithoutIfNotExistsRejects` with the same errno assertion +
  refreshed its mutation comment (the two tests together pin "report 1050 on EITHER arm").
- Added `import org.apache.doris.common.ErrorCode`.

**RED→GREEN verified empirically:** re-adding the `if (localExists)` guard turns the remote test red
("Expected DdlException … but nothing was thrown" — the buggy path falls through to the no-op mock
`createTable`); removing it → green. Local test stays green under the mutation (correct — it guards the
other arm).

## Result

- `PluginDrivenExternalCatalogDdlRoutingTest` 26/0/0, `PluginDrivenExternalTableEngineTest` 12/0/0; fe-core
  compiles; checkstyle clean (validate phase). Build cache disabled.
- Diagnostic/contract-only change (error code/SQLSTATE/message); CREATE outcome (failure) unchanged.
- **e2e:** reaching "remote exists, local-cache absent" needs multi-FE / external create; paimon e2e gated
  (`enablePaimonTest=false`) → none added (documented; fail-loud).
