# Problem

The paimon connector never overrides `getTableStatistics` from the `ConnectorMetadata` SPI. The FE table object `PluginDrivenExternalTable.fetchRowCount()` resolves the connector table handle and then calls `metadata.getTableStatistics(session, handle)`; when that returns `Optional.empty()` (or a row count < 0) it falls back to `UNKNOWN_ROW_COUNT` (-1). Because `PaimonConnectorMetadata` inherits the default `ConnectorStatisticsOps.getTableStatistics` (which returns `Optional.empty()`), every paimon plugin table — normal AND system tables (`PluginDrivenSysExternalTable extends PluginDrivenExternalTable`, inherits the same `fetchRowCount`) — reports a base-table row count of -1 (UNKNOWN).

Legacy `PaimonExternalTable.fetchRowCount()` and `PaimonSysExternalTable.fetchRowCount()` returned the REAL row count (sum of planned-split record counts). The regression is silent: cost model / cardinality degrades (Nereids join-order, broadcast decisions; per the review, `StatsCalculator.disableJoinReorderIfStatsInvalid` forces `DISABLE_JOIN_REORDER=true` for the whole query when rowCount==-1), and `SHOW TABLE STATUS` / `information_schema.tables` reports -1. No wrong rows, no crash — just degraded plans and missing stats. Confirmed MAJOR (review §8, Finding 5.1).

# Root Cause (confirmed in current code, cite file:line I actually read)

- `PaimonConnectorMetadata` (read in full, `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java`) implements `ConnectorMetadata` but has NO `getTableStatistics` method anywhere in the class. It therefore inherits the default.
- `ConnectorStatisticsOps.java:30-34` — the default `getTableStatistics(...)` returns `Optional.empty()`.
- `PluginDrivenExternalTable.java:436-453` — `fetchRowCount()` does `metadata.getTableStatistics(session, handleOpt.get())`; on empty / rowCount<0 returns `UNKNOWN_ROW_COUNT` (the `return UNKNOWN_ROW_COUNT` at :445 and :452).
- `PluginDrivenSysExternalTable.java:41` — `extends PluginDrivenExternalTable`, inheriting that same `fetchRowCount` (so sys tables get -1 too; review Finding 5.1).
- Legacy reference — `PaimonExternalTable.java:209-221` computes `rowCount += split.rowCount()` over `getBasePaimonTable().newReadBuilder().newScan().plan().splits()`, returns `rowCount > 0 ? rowCount : UNKNOWN_ROW_COUNT` (i.e. 0 → -1). `PaimonSysExternalTable.java:200-211` is byte-identical against `getSysPaimonTable()`.
- The connector ALREADY drives that exact paimon idiom in `PaimonScanPlanProvider.java:178-186` (`table.newReadBuilder().newScan().plan().splits()`) and reads `split.rowCount()` (`PaimonScanPlanProvider.java:327`), so the SDK call shape is proven inside the module.
- `ConnectorTableStatistics.java:35-48` — ctor is `(rowCount, dataSize)`; `UNKNOWN = (-1,-1)`. The JDBC reference `JdbcConnectorMetadata.java:142-153` shows the established override shape: compute row count, return `Optional.of(new ConnectorTableStatistics(rowCount, -1))` when `rowCount >= 0`, else `Optional.empty()`.

# Design

Override `getTableStatistics` in `PaimonConnectorMetadata`, computing the row count by summing `Split.rowCount()` over the planned splits of the resolved live `Table` — exactly the legacy computation, ported into the connector.

Two constraints drive the shape:

1. **No fe-core import** — respected: the fix lives entirely in the connector module and uses only paimon SDK types (`Table`, `Split`) plus the SPI `ConnectorTableStatistics` / `Optional`. No new fe-core dependency.

2. **Offline unit-testability via a seam** — this is the load-bearing design decision and the reason I do NOT inline `table.newReadBuilder().newScan().plan().splits()` directly in the metadata method. The whole MVCC/time-travel logic was deliberately structured so `PaimonConnectorMetadata` calls plain-`long`-returning `PaimonCatalogOps` seam methods (see the seam Javadoc at `PaimonCatalogOps.java:79-83`: "return plain `long`s ... so the metadata layer's logic ... is unit-testable offline with `RecordingPaimonCatalogOps`"). The test double `FakePaimonTable.newReadBuilder()` THROWS `UnsupportedOperationException` (`FakePaimonTable.java:237-239`), so a metadata method that called `newReadBuilder()` directly could never be exercised offline. To stay consistent with the module's established pattern AND keep the new logic testable, add a single `long rowCount(Table table)` method to the `PaimonCatalogOps` seam:

   - `CatalogBackedPaimonCatalogOps.rowCount(Table)` does the real paimon work (`newReadBuilder().newScan().plan().splits()` sum), mirroring legacy line-for-line.
   - `PaimonConnectorMetadata.getTableStatistics` resolves the table via the existing `resolveTable(handle)` (the same sys-aware resolver every metadata read path uses), calls `catalogOps.rowCount(table)`, applies the legacy `>0 ? n : UNKNOWN` rule, and wraps in `ConnectorTableStatistics(rowCount, -1)` (dataSize left UNKNOWN, matching JDBC reference and the fact legacy never computed a base-table dataSize here).

`resolveTable` is sys-aware (`PaimonConnectorMetadata.java:931-937` → `PaimonTableResolver.resolve`), so a sys handle resolves its OWN synthetic table and `rowCount` plans the sys table's splits — this single override gives BOTH normal and sys paimon tables their real count, closing Finding 5.1 with the same code (no separate sys path needed, mirroring how legacy had two parallel-but-identical `fetchRowCount` bodies).

dataSize: returned as -1 (UNKNOWN). Legacy `fetchRowCount` produced only a row count; there is no legacy base-table dataSize to port, and JDBC's reference override also returns dataSize=-1. Keeping dataSize unknown is faithful and avoids inventing a number.

Error handling: a planning failure (remote IO, etc.) should NOT crash stats collection (which runs in background analysis / SHOW paths). Return `Optional.empty()` (→ FE falls back to -1) on exception, logging a warning — consistent with the connector's other best-effort read paths (e.g. `listDatabaseNames` `PaimonConnectorMetadata.java:96-99`, `collectPartitions` swallowing `TableNotExistException` at :869-873). This preserves the legacy END STATE (-1) on failure without a louder regression than legacy (legacy would have propagated, but legacy ran inside `fetchRowCount` whose callers tolerate exceptions becoming -1; empty-on-failure is the safer, equivalent-visible-result choice and matches the SPI's empty-if-unavailable contract).

# Implementation Plan

**File 1 — `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonCatalogOps.java`**

Add one seam method to the interface (next to the E5 MVCC lookups block, with Javadoc matching the existing seam-rationale style):

```java
/**
 * Returns the total row count of {@code table} = sum of {@code split.rowCount()} over
 * {@code table.newReadBuilder().newScan().plan().splits()} (legacy
 * PaimonExternalTable.fetchRowCount / PaimonSysExternalTable.fetchRowCount). Returns a plain
 * {@code long} (never a paimon Split list) so the metadata layer's >0-else-UNKNOWN logic is
 * unit-testable offline with RecordingPaimonCatalogOps (FakePaimonTable.newReadBuilder() throws).
 */
long rowCount(Table table);
```

Implement in `CatalogBackedPaimonCatalogOps` (add `import org.apache.paimon.table.source.Split;`):

```java
@Override
public long rowCount(Table table) {
    long rowCount = 0;
    for (Split split : table.newReadBuilder().newScan().plan().splits()) {
        rowCount += split.rowCount();
    }
    return rowCount;
}
```

**File 2 — `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java`**

Add imports: `org.apache.doris.connector.api.ConnectorTableStatistics`, `org.apache.paimon.table.Table` is already imported. Add the override (placed near the other read/stat methods, e.g. after `getColumnHandles`):

```java
/**
 * Returns the base-table row count = sum of planned-split row counts (legacy
 * PaimonExternalTable.fetchRowCount, lines 209-221: rowCount>0 ? rowCount : UNKNOWN). Shared by
 * normal AND system paimon tables: fe-core PluginDrivenSysExternalTable inherits
 * PluginDrivenExternalTable.fetchRowCount, and resolveTable is sys-aware, so a sys handle plans
 * its OWN synthetic table's splits. Returns Optional.empty() (-> fe-core -1) when the count is 0
 * (legacy parity) or planning fails (best-effort, like the other connector read paths). dataSize
 * is left UNKNOWN (-1): legacy computed no base-table dataSize here.
 */
@Override
public Optional<ConnectorTableStatistics> getTableStatistics(
        ConnectorSession session, ConnectorTableHandle handle) {
    PaimonTableHandle paimonHandle = (PaimonTableHandle) handle;
    long rowCount;
    try {
        rowCount = catalogOps.rowCount(resolveTable(paimonHandle));
    } catch (Exception e) {
        LOG.warn("Failed to compute Paimon row count for {}", paimonHandle, e);
        return Optional.empty();
    }
    if (rowCount > 0) {
        return Optional.of(new ConnectorTableStatistics(rowCount, -1));
    }
    return Optional.empty();   // 0 rows -> UNKNOWN, legacy parity
}
```

**File 3 — `fe/fe-connector/fe-connector-paimon/src/test/java/org/apache/doris/connector/paimon/RecordingPaimonCatalogOps.java`** (test fake — MUST implement the new seam method or the module won't compile)

Add a configurable field + recorded method:

```java
// ---- FIX-TABLE-STATS: row-count seam ----
long rowCount;                 // configurable result
Table lastRowCountTable;       // capture which table the metadata layer planned

@Override
public long rowCount(Table table) {
    log.add("rowCount");
    lastRowCountTable = table;
    return rowCount;
}
```

No production code outside the connector module changes. `ConnectorStatisticsOps` / `ConnectorTableStatistics` / `PluginDrivenExternalTable.fetchRowCount` are already wired and need no edits — the gap was purely the missing override.

# Risk Analysis

- **Parity vs legacy**: Exact port. Legacy summed `split.rowCount()` over `newReadBuilder().newScan().plan().splits()` and returned `>0 ? n : -1`; the seam impl is identical and the metadata wrapper reproduces the `>0` gate and the 0→UNKNOWN mapping. The only intentional divergence is failure handling (legacy let the exception propagate up `fetchRowCount`; we return empty→-1). The user-visible result is the same fallback (-1) but we avoid surfacing a transient planning error as a query-killing exception during background stats collection — strictly safer, and aligned with the SPI's empty-if-unavailable contract and the module's other best-effort read paths.
- **dataSize**: -1 (UNKNOWN). Legacy never produced a base-table dataSize in `fetchRowCount`, so this is not a regression; any future dataSize work is additive.
- **Shared-code blast radius**: Adding a method to the `PaimonCatalogOps` interface forces every implementor to provide it. There are exactly two: `CatalogBackedPaimonCatalogOps` (production, updated) and `RecordingPaimonCatalogOps` (test, updated). I grepped the module; no other implementor exists. `ConnectorStatisticsOps` / `ConnectorTableStatistics` are untouched, so no other connector (JDBC, MaxCompute) is affected. `fetchRowCount` in fe-core is untouched.
- **Cost-model direction**: Going from a constant -1 to a real positive count CHANGES plans (re-enables join reorder, may flip broadcast/shuffle). This is the intended correction (restoring legacy behavior). It is a planning change, not a correctness change — results stay identical; only plan shape/perf moves toward the legacy baseline.
- **Edge cases**:
  - Empty table (0 rows) → `rowCount==0` → empty → -1, matching legacy (which logged and returned -1).
  - System tables → resolved via sys-aware `resolveTable`; `rowCount` plans the sys table's own splits. Closes Finding 5.1 with the same code path; no extra sys branch.
  - Time-travel / branch handles: `resolveTable` honors the handle's pinned identity (branch/scan options already on the handle), so stats reflect the handle's view. In practice `fetchRowCount` is called on the base table object (analysis path), so the handle is the latest base — but the code is correct for any handle the SPI is asked about.
  - Planning cost: each `getTableStatistics` call drives a real `plan()` (one remote scan-plan), same cost as legacy `fetchRowCount`. Called from analysis / SHOW paths, not per-query-hot, so acceptable and unchanged from legacy.

# Test Plan

## Unit Tests

New test class `PaimonConnectorMetadataStatisticsTest` in `fe/fe-connector/fe-connector-paimon/src/test/java/org/apache/doris/connector/paimon/`, driving a `RecordingPaimonCatalogOps` fake (no Mockito, fully offline), mirroring `PaimonConnectorMetadataMvccTest`'s `metadataWith(ops)` pattern. Each test FAILS before the fix (default SPI returns `Optional.empty()` for every input, so the positive-count and sys assertions fail) and PASSES after.

Intent encoded (WHY, not just WHAT):

1. **`positiveRowCount_returnedAsStatistics`** — `ops.rowCount = 42`; build a base `PaimonTableHandle` with `setPaimonTable(fakeTable)`; assert `getTableStatistics(...)` returns present with `getRowCount()==42` and `getDataSize()==-1`. WHY: a real positive count must reach the FE cost model (not -1) so join-reorder is not force-disabled. Also assert `ops.log` contains `"rowCount"` and `ops.lastRowCountTable == fakeTable`, proving the metadata layer planned the RESOLVED table (not some other handle) — locks the intent that stats are computed from the table the handle denotes.

2. **`zeroRowCount_mapsToUnknownEmpty`** — `ops.rowCount = 0`; assert result is `Optional.empty()`. WHY: encodes the legacy 0→UNKNOWN(-1) contract; a future change that returned `(0,-1)` instead of empty would corrupt the FE which treats 0 as a real cardinality. This test fails if someone drops the `>0` gate.

3. **`planningFailure_returnsEmptyNotThrow`** — a `RecordingPaimonCatalogOps` subclass (or a flag) whose `rowCount` throws `RuntimeException`; assert `getTableStatistics` returns `Optional.empty()` and does NOT propagate. WHY: stats collection must be best-effort; a transient remote failure must not kill the query/analysis. Locks the deliberate divergence from legacy's propagate-up behavior.

4. **`systemTableUsesResolvedSysTable`** — build a sys handle via `PaimonTableHandle.forSystemTable(db, tbl, "snapshots", false)` with `setPaimonTable(sysFake)`; `ops.rowCount = 7`; assert present with rowCount==7 and `ops.lastRowCountTable == sysFake`. WHY: proves the single override serves system tables through the sys-aware `resolveTable` (closes Finding 5.1) — a future refactor that special-cased only normal tables would fail this.

(`RecordingPaimonCatalogOps` gains the `rowCount` field + `lastRowCountTable` capture described above; `FakePaimonTable` is reused as-is — these tests never call `newReadBuilder()` because the seam is faked, which is the whole point of the seam.)

## E2E Tests

Live-only / CI-skipped, because a real row count requires a live paimon catalog with committed snapshots; the module's `PaimonLiveConnectivityTest` is the existing live harness and is not run in the offline gate. Recommended live regression (paimon p2 external suite, gated on a live paimon env): after `CREATE CATALOG` + `USE` a known paimon table with N committed rows, assert `SELECT COUNT(*)` and the reported stats agree — specifically that `SHOW TABLE STATUS`/`information_schema.tables.table_rows` reports N (not -1) for both a normal table AND a `tbl$snapshots` system table, matching the pre-migration (legacy) baseline. Note in the suite that this is a stats-only assertion (no row-correctness change) and that it must run against the same fixture used for the legacy parity baseline so any drift from legacy `fetchRowCount` is caught. This cannot run in the offline unit gate (no live catalog), hence CI-skipped/live-only.

---

# ✅ IMPL SUMMARY (2026-06-11)

**Status: DONE — build+UT green (PaimonConnectorMetadataStatisticsTest 4/0; PaimonConnectorMetadataMvccTest 37/0 unchanged; imports clean; HEAD uncommitted).**

## Fix (3 production + 1 test-fake file, all in connector module; fe-core untouched)
- `PaimonCatalogOps.java`: added `long rowCount(Table)` to the interface + `import org.apache.paimon.table.source.Split`; implemented in `CatalogBackedPaimonCatalogOps` (sum `split.rowCount()` over `newReadBuilder().newScan().plan().splits()`).
- `PaimonConnectorMetadata.java`: added `import ConnectorTableStatistics` + the `getTableStatistics` override — resolves the table via the sys-aware `resolveTable`, calls `catalogOps.rowCount`, applies legacy `>0 ? n : UNKNOWN`, wraps `(rowCount, -1)`; best-effort `try/catch → Optional.empty()` on planning failure.
- `RecordingPaimonCatalogOps.java` (test fake): added `rowCount` / `lastRowCountTable` / `throwOnRowCount` + the `rowCount` override (required for module compile).

## Tests (new `PaimonConnectorMetadataStatisticsTest`, 4): positive→stats(+lastRowCountTable proof), zero→empty, planning-failure→empty-not-throw, system-table→resolved-sys-table.

## Notes
- Single override serves normal AND sys paimon tables via the sys-aware `resolveTable` (closes Finding 5.1; no separate sys path).
- dataSize left UNKNOWN(-1): legacy computed none here.
- The only two `PaimonCatalogOps` implementors (production `CatalogBackedPaimonCatalogOps`, test `RecordingPaimonCatalogOps`) both updated — verified no other implementor exists.

## Live-e2e (gated, NOT run): SHOW TABLE STATUS / information_schema.tables.table_rows == N (not -1) for a normal table AND a `tbl$snapshots` sys table, against the legacy-parity fixture.
