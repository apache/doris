# FIX-INCR-SCAN-RESET — Summary

> P2-1 (MAJOR). Commit `f08bc22b9bd`. Connector-only (no SPI / no BE). Design:
> `FIX-INCR-SCAN-RESET-design.md`. Pre-coding design red-team: `wf_ffd11631-ed2` (DESIGN-SOUND).

## Problem
A Paimon `@incr(...)` read can return the wrong rows — or hard-fail — when the base table **persists**
a `scan.snapshot-id` / `scan.mode` option (legal & mutable via `ALTER TABLE SET`, `TBLPROPERTIES`, or
`table-default.*` catalog options).

## Root Cause
`PaimonIncrementalScanParams.validate()` deliberately **stripped** legacy's defensive null-reset of
`scan.snapshot-id` / `scan.mode` (legacy `PaimonScanNode.validateIncrementalReadParams:842-843,846`,
applied via `baseTable.copy(...)` `:896`), justified by a **wrong** rationale ("a fresh per-query `Table`
can't inherit `scan.*`"). The table options come from the **persisted** `TableSchema`, so a stale
`scan.snapshot-id` is present on every fresh load. Without the reset, `resolveScanTable`'s
`Table.copy(scanOptions)` merges the stale `scan.snapshot-id` with `incremental-between`; paimon 1.3.1 then
either **throws** (`IllegalArgumentException: "[incremental-between] must be null when you set
[scan.snapshot-id,scan.tag-name]"`) or **silently** resolves to `FROM_SNAPSHOT` at the stale id (wrong
`@incr` rows, and a wrong pinned schema via `tryTimeTravel`).

## Fix
**Option 2** (unanimous red-team pick; keeps the shared SPI null-free, surgical):
- New `PaimonIncrementalScanParams.applyResetsIfIncremental(scanOptions)` — gated on the presence of
  `incremental-between` / `incremental-between-timestamp`, returns a fresh map seeded with
  `scan.snapshot-id=null` + `scan.mode=null` then the original options; otherwise returns the input
  unchanged (no false positive on a genuine snapshot/tag pin). Uses the class's existing key constants
  (no literals → no detector drift). Strict legacy parity: only those two keys.
- `PaimonScanPlanProvider.resolveScanTable` wraps the `Table.copy(...)` argument with the helper — one edit
  covers **both** callers (native/JNI scan `planScanInternal` + JNI serialized-table `getScanNodeProperties`)
  through the single chokepoint. The null values are created locally and consumed immediately by paimon's
  `copyInternal` (`v == null ? options.remove(k) : options.put(k, v)`) — never stored, serialized, or placed
  in `ConnectorMvccSnapshot`.
- `validate()` is unchanged (still null-free), so the shared `ConnectorMvccSnapshot` SPI contract and the
  two existing "no-null" tests stay intact. Corrected the now-refuted "byte-parity on a freshly-loaded base"
  rationale in `PaimonIncrementalScanParams` javadoc/inline, `PaimonConnectorMetadata` INCREMENTAL comment,
  and the two existing tests' WHY-comments (assertions unchanged).

### Why not Option 1 (re-emit nulls through the SPI)
Mechanically works, but breaks the shared `ConnectorMvccSnapshot` null-free contract (future iceberg/hudi),
depends on a silent gap in `Builder.properties(Map)` that a future null-hardening would re-break, and would
invert two green tests + the `getProperties()` javadoc. Same engine behavior, none of that.

## Tests
- `PaimonScanPlanProviderTest.resolveScanTableResetsStalePinForIncrementalRead` (**NEW**, real
  `FileSystemCatalog` table with a persisted `scan.snapshot-id`) — **proven fail-before** (neutered fix →
  `IllegalArgumentException` at the `resolveScanTable` call) / pass-after.
- `PaimonIncrementalScanParamsTest` **+4** unit tests (helper seeds the resets for snapshot & timestamp
  windows; passes non-incremental pins through unchanged; no-op for empty/null); reworded the keep-null-free
  `validate()` test (assertions unchanged).
- `PaimonConnectorMetadataMvccTest` — reworded the refuted WHY-comment (assertions unchanged).

## Result
- Connector suites green: `PaimonIncrementalScanParamsTest` 20/0/0, `PaimonScanPlanProviderTest` 44/0/0,
  `PaimonConnectorMetadataMvccTest` 37/0/0. `BUILD SUCCESS`.
- Checkstyle 0 violations; import-gate clean.
- Live `@incr`-over-persisted-`scan.snapshot-id` E2E is **CI-gated** (`enablePaimonTest=false`) — not run
  here; noted as gated.
