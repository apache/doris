# FIX-R1-TABLE — restore MySQL errno 1050 for CREATE TABLE on a remote-existing table

> Single-task loop (AGENT-PLAYBOOK): design → design red-team → implement → impl verify → build+UT → commit → summary.
> Source finding: `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §R1 (table) (MINOR, regression, confirmed).

# Problem

`PluginDrivenExternalCatalog.createTable` (the **generic** SPI bridge — paimon/maxcompute/es/jdbc/trino all
route through it) reports `ERR_TABLE_EXISTS_ERROR` (MySQL errno **1050**, SQLSTATE `42S01`, "Table '%s'
already exists") **only for the local-cache-conflict arm**. A table that exists **only remotely** (absent
from this FE's cache) with no `IF NOT EXISTS` falls through to `metadata.createTable`, which throws
`DorisConnectorException("…already exists")`, re-wrapped at `:319` as a **generic** `DdlException`
(errno 0 / `ERR_UNKNOWN_ERROR`). The CREATE still fails — only the error code / SQLSTATE / message regress.

```java
// PluginDrivenExternalCatalog.java:298-314 (current)
if (remoteExists || localExists) {
    if (createTableInfo.isIfNotExists()) { ... return true; }      // both arms no-op on IF NOT EXISTS
    if (localExists) {                                              // <-- LOCAL arm only
        ErrorReport.reportDdlException(ERR_TABLE_EXISTS_ERROR, createTableInfo.getTableName());
    }
}
// remoteExists && !localExists && !ifNotExists  falls through to metadata.createTable -> generic DdlException
```

# Root Cause

The bridge re-implements legacy's remote-then-local existence probe but only ported the **local** arm's
1050 report. Both legacy ops reported 1050 for **both** arms:
- legacy paimon `PaimonMetadataOps.performCreateTable`: remote `:195`, local `:212`.
- legacy maxcompute `MaxComputeMetadataOps.createTableImpl`: remote `:184`, local `:195`.

So 1050-for-remote is exact parity for **both** live cut-over connectors, not a paimon-only concern.

# Design

Drop the `if (localExists)` guard. At that point the code is already inside `if (remoteExists || localExists)`
and past the `isIfNotExists()` early-return, so `(remoteExists || localExists) && !ifNotExists` is
guaranteed — report `ERR_TABLE_EXISTS_ERROR` unconditionally there. `ErrorReport.reportDdlException` throws,
short-circuiting **before** `metadata.createTable` (so the remote-only case no longer reaches the connector).

```java
if (remoteExists || localExists) {
    if (createTableInfo.isIfNotExists()) { ... return true; }
    // !IF NOT EXISTS: a table existing remotely OR only in the local FE cache must be rejected here with
    // MySQL errno 1050, mirroring legacy {Paimon,MaxCompute}MetadataOps (both report ERR_TABLE_EXISTS_ERROR
    // for the remote arm AND the local arm). Reporting before metadata.createTable also keeps the
    // local-cache-only conflict from being CREATED remotely (lower_case_meta_names case-fold).
    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, createTableInfo.getTableName());
}
```

This is the minimal change and is **byte-equivalent to legacy** for both arms.

## Behavioral delta

- **remote-only existing table, no IF NOT EXISTS:** generic `DdlException` → **`DdlException` with errno
  1050** + message "Table '<t>' already exists" (legacy parity). CREATE still fails; `metadata.createTable`
  is no longer called for this case (it only threw anyway — no lost side effect).
- **local-cache conflict / IF NOT EXISTS / create-succeeds:** unchanged.
- **Generic bridge → applies to every SPI connector** (paimon/maxcompute/es/jdbc/trino). 1050 for an
  existing table is the universally-correct MySQL contract; parity verified for the two live connectors.

# Implementation Plan

Single edit in `PluginDrivenExternalCatalog.java`: replace the `if (localExists) { report }` arm with an
unconditional `report` (and update the comment `:304-313`). No SPI/connector/BE change.

# Risk Analysis

- **Diagnostic/contract-only** (error code/SQLSTATE/message); CREATE outcome (failure) unchanged → MINOR.
- errno 1050 is a documented MySQL contract some ORMs/migration tools branch on (SQLSTATE 42S01) → worth
  restoring (MINOR, not NIT).
- **Reachability narrow:** table exists remotely but absent from this FE cache — stale cache / other-FE /
  external (Spark/Flink) create.
- **No lost side effect:** the connector's createTable for an existing table only throws; short-circuiting
  before it changes nothing but the surfaced error.
- **Cross-connector (red-team `wf_19fd7785-165`, 0 actionable):** the change is in the shared bridge; parity
  verified for paimon + maxcompute (both legacy ops report 1050 for the remote AND local arm). No connector
  relied on the remote-exists fall-through for a side effect, and no regression/e2e test pins the old generic
  message or asserts `createTable` is called on a remote-existing table. **Nuance (NIT):** es/jdbc/trino
  implement `getTableHandle` (so `remoteExists` can be true) but do not override `createTable`; for those
  connectors a `CREATE TABLE` on an *existing* table now surfaces "already exists" (1050) instead of the old
  fall-through error — benign and arguably more accurate; the non-existing-table path is unchanged.

# Test Plan

## Unit Tests (`PluginDrivenExternalCatalogDdlRoutingTest`)

- **Update** `testCreateTableExistingTableWithoutIfNotExistsStillErrors` (`:523`) — it currently encodes the
  **buggy** fall-through (`verify(metadata).createTable(...)`). Rewrite to the corrected contract:
  remote-exists + !IF NOT EXISTS → `DdlException` with `getMysqlErrorCode() == ERR_TABLE_EXISTS_ERROR`, and
  `verify(metadata, never()).createTable(...)` (short-circuit before the connector) + no editlog. This is
  the **mutation-killing** test: restoring the `if (localExists)` guard makes the remote case fall through →
  errno reverts to `ERR_UNKNOWN_ERROR` and createTable is called → red.
- **Strengthen** `testCreateTableLocalConflictWithoutIfNotExistsRejects` (`:555`) — add
  `getMysqlErrorCode() == ERR_TABLE_EXISTS_ERROR` so both arms pin the 1050 contract (local arm already
  passed pre-fix; this documents the unified contract).

## E2E Tests

Reaching "remote exists, local-cache absent" needs multi-FE or an external create; paimon e2e is gated
(`enablePaimonTest=false`). → no e2e added (documented; fail-loud).
