# P4-T06d — FIX-READ-SPLIT — byte_size split size sentinel

Status: implemented (not committed). Scope: one-line production change in the MaxCompute
connector + one CI-runnable UT. Sibling of FIX-READ-DESC (already done/committed).

## Problem
After the `max_compute` cutover (T06b), reads route through the PluginDriven SPI path. With the
**default** split strategy `byte_size` (`MCConnectorProperties.DEFAULT_SPLIT_STRATEGY =
SPLIT_BY_BYTE_SIZE_STRATEGY`), `SELECT count(*)` / `SELECT *` return **silently corrupt data**
(wrong row counts / column values, no error). `row_offset` strategy and the limit-optimization
single-split path are unaffected.

## Root Cause
BE has no `split_type` field on the wire. `MaxComputeJniScanner` classifies a split purely by the
numeric `split_size` it receives:
- `be/src/format/table/max_compute_jni_reader.cpp:70` → `properties["split_size"] =
  std::to_string(range.size)`.
- `MaxComputeJniScanner.java:125-128` → `if (splitSize == -1) BYTE_SIZE else ROW_OFFSET`; then in
  `open()` (:207-211) builds `IndexedInputSplit(sessionId, startOffset)` (BYTE_SIZE) or
  `RowRangeInputSplit(sessionId, startOffset, splitSize)` (ROW_OFFSET).

Legacy back-filled the sentinel: `MaxComputeScanNode.java:657-662` →
`MaxComputeSplit(BYTE_SIZE_PATH, splitIndex, /*length=*/-1, /*fileLength=*/splitByteSize, ...)`,
so `rangeDesc.setSize(getLength()) = -1`.

The cutover connector did NOT: `MaxComputeScanPlanProvider`'s byte_size branch used
`.length(splitByteSize)` (= default 268435456) → `MaxComputeScanRange.populateRangeParams`'s
`rangeDesc.setSize(getLength())` = 268435456. BE sees `split_size != -1`, mis-classifies the
byte_size split as ROW_OFFSET, and reads via `RowRangeInputSplit(..., rowCount=268435456)` →
corrupt data.

## Design
Restore the legacy sentinel in the byte_size branch only: emit `length = -1`, so
`getLength() → setSize(-1)`. This is byte-exact with legacy `MaxComputeSplit(..., length=-1,
fileLength=splitByteSize, ...)`:
- `setSize = -1` (sentinel)
- `setStartOffset = splitIndex`
- path string = `"[ splitIndex , -1 ]"` (same as legacy `getStart()=splitIndex`, `getLength()=-1`)

The real byte size is not needed in the range — the byte split was already computed in the ODPS
session (`SplitOptions.SplitByByteSize(...)`); BE reconstructs the split from
`IndexedInputSplit(sessionId, splitIndex)`. The sentinel is a **private contract between the
MaxCompute connector and its BE-side `MaxComputeJniScanner`**, keyed inside the connector's own
`MaxComputeScanRange`/provider (the `getTableFormatType()=="max_compute"` branch), not in any
generic fe-core/PluginDriven layer. No SPI/thrift change.

row_offset (`:290 .length(count)`) and limit-optimization (`:338 .length(rowsToRead)`) branches are
**unchanged** — they correctly carry the real row count that BE reads as `RowRangeInputSplit` size.

## Implementation Plan
- `MaxComputeScanPlanProvider.java` byte_size branch (`:272`): `.length(splitByteSize)` →
  `.length(-1L)` + a comment that `-1` is the BE BYTE_SIZE/ROW_OFFSET sentinel (mirrors legacy
  `MaxComputeScanNode`). DONE.
- `MaxComputeScanRange.java`: **unchanged** — `setSize(getLength())` and `Builder.length` default
  (already `-1`) need no edit; the fix flows through naturally.

## Risk — corrected impact analysis (3 consumers, not 2)
The parent `P4-cutover-fix-design.md` claimed (and grep "fully confirmed") that `getLength()` for a
byte_size range flows ONLY to `setPath` (`MaxComputeScanRange.java:120`) and `setSize` (`:122`).
**That is wrong.** `getLength()` has a THIRD consumer:

1. `MaxComputeScanRange.populateRangeParams` `setPath` (cosmetic path string `:120`).
2. `MaxComputeScanRange.populateRangeParams` `setSize` (`:122`) — the BE sentinel; the load-bearing
   one.
3. `PluginDrivenSplit.java:42` passes `scanRange.getLength()` into `FileSplit.length`, read
   downstream by:
   - `FederationBackendPolicy.java:499` — `primitiveSink.putLong(split.getLength())` in
     consistent-hash backend assignment.
   - `FileQueryScanNode.java:430` — `totalFileSize += split.getLength()`.

After the fix, consumers (1)-(3) see `-1` instead of `268435456`. **This is BENIGN and improves
legacy parity** (legacy `MaxComputeSplit` also used `length=-1`; the buggy cutover diverged from
legacy here too). Concretely:
- (a) **Consistent-hash split→BE placement** will differ from the **current buggy build** (because
  the hashed `getLength()` changes from 268435456 to -1). This is invisible/benign for correctness
  and matches legacy. Do NOT mistake this A/B placement difference for a regression during
  validation.
- (b) **`totalFileSize` goes negative** for byte_size scans (one `-1` per split). This is
  pre-existing legacy behavior, used only for stats/cost/explain/logging, not correctness. It
  propagates to profile/explain numbers and any cost heuristic keyed on `totalFileSize`. Low risk,
  pre-existing.

Other guarantees (verified):
- Cross-connector impact: **zero**. The sentinel is private to MaxCompute ↔ `MaxComputeJniScanner`;
  the change is strictly inside the connector's byte_size branch. jdbc/es/trino/hive/hudi each carry
  real file byte sizes, unrelated to this sentinel. (Note: any future generic use of
  `ConnectorScanRange.getLength()==-1` by other code paths would need re-examination.)
- No edit-log/replay/HA concern: the change is purely query-plan-time scan-range construction, not
  persisted.
- checkstyle / import gate: only a literal-arg change; `-1L` matches existing long-literal style; no
  new imports/types. (Verified: 0 violations, import gate clean.)

## Test Plan
**UT — CI-runnable guard (the only one that runs in normal CI):**
`fe-connector-maxcompute/.../MaxComputeScanRangeTest.java` (JUnit 5; module has no fe-core / no
Mockito). It drives the **provider's real byte_size split-building path** (`buildSplitsFromSession`,
invoked via reflection) with offline Serializable fakes for `TableBatchReadSession` /
`InputSplitAssigner` (returning a real `IndexedInputSplit`), then asserts the produced range's
`rangeDesc.getSize() == -1`. Two tests:
- `byteSizeBranchEmitsMinusOneSizeSentinel` — asserts size == -1 (plus startOffset == splitIndex and
  path == `"[ 7 , -1 ]"`). **This guards the provider's CHOICE, not just the range mechanism**:
  reverting the byte_size branch to `.length(splitByteSize)` makes it FAIL with
  `expected: <-1> but was: <268435456>` (verified by a real revert — see below).
- `rowOffsetBranchKeepsRealRowCount` — contrast: the row_offset branch carries the real row count
  (never the -1 sentinel), locking the intent that ONLY byte_size uses -1 (guards against an
  over-broad "set everything to -1" fix).

Each assertion message encodes WHY (Rule 9): BE distinguishes BYTE_SIZE vs ROW_OFFSET solely by
`size == -1`; a wrong value → silent corrupt read.

**Why provider-level (not the weak range-level UT):** the parent design proposed a UT that builds a
range with `.length(-1)` itself then asserts `getSize()==-1`. That is weak — it sets length=-1
itself, so it would NOT fail if the provider reverted to `.length(splitByteSize)`; it locks the
range mechanism, not the fix. This UT instead exercises the changed provider line, so it is a real
regression red point.

**E2E (NOT run in normal CI):** `regression-test/suites/external_table_p2/maxcompute/
test_external_catalog_maxcompute.groovy` is an `external_table_p2` suite requiring **live
MaxCompute/ODPS credentials**, so it is **SKIPPED in normal CI**. It is therefore NOT an unattended
guard for this fix — the UT above is the only CI-runnable automated guard. Under default byte_size
strategy, the suite's read assertions (count(*), select *, int_types, mc_parts) read corrupt data
before the fix and should match the legacy `.out` baseline after, but this requires a manual /
credentialed run.

## Boundary notes
- **FIX-READ-SPLIT alone does NOT yield correct reads unless FIX-READ-DESC is also present**
  (already done/committed). They are independent blockers on the same read path: FIX-READ-DESC fixes
  the table descriptor (endpoint/quota/project/table/properties for BE auth+addressing);
  FIX-READ-SPLIT fixes the per-split sentinel. The JNI scanner also requires `time_zone`
  (`MaxComputeJniScanner.java:139` `requireNonNull`), injected by the BE JNI framework
  (`jni_reader.cpp` `_scanner_params.emplace("time_zone", ...)`) for all JNI scanners — not by the
  descriptor; this fix neither helps nor regresses it.
- Production change keeps legacy `MaxComputeScanNode.java` untouched (keep baseline, read-only
  reference).
