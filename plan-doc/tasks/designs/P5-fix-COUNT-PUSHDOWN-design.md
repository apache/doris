# P5 fix #8 — `FIX-COUNT-PUSHDOWN` (M-2)

> Round-2 severity MAJOR (round-1 MINOR), perf-parity. User signed off (2026-06-12):
> **Proceed** (SPI change accepted) + **connector collapse-to-one** for the count-split shape.
> Param shape `boolean countPushdown` and **paimon-only** scope decided as engineering calls
> (overridable, not overridden).

## Problem
After cutover, `COUNT(*)` over a plugin-driven paimon table is **result-correct but slow**: BE is
already in COUNT mode (the `TPushAggOp.COUNT` enum reaches it via `FileScanNode.toThrift`), but the
connector never emits a precomputed row count, so every split carries `table_level_row_count = -1`
and BE falls back to **materializing the full post-merge row set just to count it**
(`file_scanner.cpp:1298-1326`). PK / merge-on-read tables pay the full merge + deletion-vector cost.

## Root cause (verified against current code, recon `wf_1ce48c93-325`)
The fix has three independent halves; only one was missing:
1. **Emit half — ALREADY BUILT.** `PaimonScanRange.Builder.rowCount(long)` → prop `paimon.row_count`
   → `populateRangeParams` → `formatDesc.setTableLevelRowCount(n)` (else `-1`). Byte-identical to
   legacy `PaimonScanNode:303-308`. No new thrift, **no BE change**.
2. **COUNT enum → BE — ALREADY WORKS.** `PhysicalPlanTranslator:873` sets `pushDownAggNoGroupingOp`
   on the `PluginDrivenScanNode` (it is NOT excluded — Nereids accepts any `LogicalFileScan`);
   `FileScanNode.toThrift:90` ships it. BE is in COUNT mode.
3. **Signal + compute — MISSING (the bug).**
   - The merged count `dataSplit.mergedRowCount()` is **Paimon-SDK-only** → must be connector-side.
   - The COUNT **signal** `getPushDownAggNoGroupingOp()==COUNT` lives only on the fe-core node and is
     **read by nobody** — `PluginDrivenScanNode.getSplits` never reads it (grep 0 hits) and it is not
     in `planScan` / `ConnectorSession` / `ConnectorContext` / the handle.

So this is **NOT pure-connector** (correcting the initial framing): the signal must cross the SPI
boundary. Threading it via `ConnectorSession` (the FIX-FORCE-JNI precedent) was **rejected** — the
agg-op is a per-query planner output, not a SET-variable; that would be a silent untyped channel.

## Design (minimal, 3 files)
- **SPI** (`ConnectorScanPlanProvider`): add ONE new **default** `planScan` overload carrying
  `boolean countPushdown`, mirroring the existing 4→5→6-arg delegation chain (`limit`,
  `requiredPartitions` were added this exact way). Default delegates to the 6-arg → other connectors
  (hive/iceberg/maxcompute) are untouched (no-op).
- **fe-core** (`PluginDrivenScanNode.getSplits`): read `getPushDownAggNoGroupingOp()==TPushAggOp.COUNT`
  and pass it into the new overload. **No post-loop math** (the collapse lives in the connector).
- **connector** (`PaimonScanPlanProvider`): extract the existing 4-arg body into a private
  `planScanInternal(..., boolean countPushdown)`; 4-arg delegates with `false`, the new 7-arg with
  the flag. Add the count short-circuit:
  - **collapse-to-one** (user's choice): accumulate `mergedRowCount()` of every count-eligible split
    into `countSum`, keep the **first** eligible split as the representative, and after the loop emit
    **one** JNI-serialized count range carrying `countSum` via `Builder.rowCount`. This == legacy's
    `≤10000` path (`singletonList(first)` + `assignCountToSplits([one], sum)` → one split bearing the
    full total), applied universally.
  - Splits **without** a precomputed merged count fall through to the **normal native/JNI routing**
    (unchanged) so BE still counts them from file metadata / by reading.
  - Two new members: pure static `isCountPushdownSplit(boolean, DataSplit)` (the eligibility gate,
    mirrors `shouldUseNativeReader`/`isForceJniScannerEnabled` precedent so the routing decision is
    mutation-testable) and `buildCountRange(...)` (JNI range + `rowCount`, honors the cpp-reader flag).

### Ordering (correctness-critical)
The count branch is the **first arm** of the per-split routing — count-eligible splits must NOT also
emit data ranges, or BE would re-scan and double-count against deletion vectors / PK merge.

## Deviation from legacy (logged → `deviations-log.md`)
Legacy, for `countSum > 10000` (`COUNT_WITH_PARALLEL_SPLITS`), spreads the count over
`parallelExecInstanceNum * numBackends` splits for parallelism (`PaimonScanNode:485-491`). The
connector **always collapses to one** count split (it has no access to `numBackends`, an fe-core-only
concern). Perf-only divergence: a single CountReader emits `countSum` empty rows in one fragment
instead of N. CountReader does no IO, so impact is small; for very large counts the count-emit is not
parallelized. Result is identical.

## Risk analysis
- **Result correctness:** unchanged — counts come from the SDK's post-merge `mergedRowCount()`;
  mixed tables count each split exactly once (else-if/continue chain). The `-1` sentinel stays on all
  non-count ranges.
- **cpp-reader serialization:** count range is JNI-serialized like `buildJniScanRange`, honoring the
  `enable_paimon_cpp_reader` format (BE wraps any inner reader in CountReader regardless).
- **Other connectors:** default no-op overload → zero behavior change.
- **Batch path:** paimon does not opt into `supportsBatchScan`; only the synchronous `getSplits`
  path runs, which is where the flag is threaded.

## Test plan
Offline fail-before/pass-after IS drivable (the harness builds a REAL `DataSplit`, unlike the
schema-evolution/#7-SiteB cases):
- **Unit (connector, `PaimonScanPlanProviderTest`):**
  1. `isCountPushdownSplit(true, realSplit)==true` & `realSplit.mergedRowCount()==2`; `(false, ...)==false`
     (pure-static eligibility gate; mutation = drop `countPushdown &&` / always-false → red).
  2. **End-to-end `planScan(...countPushdown=true)`** over a real local PK table (2 rows): exactly ONE
     range carrying `paimon.row_count == "2"`; `countPushdown=false` → NO range carries `paimon.row_count`.
     This is the gold fail-before (neuter the count branch → red).
- **fail-before verification:** neuter `isCountPushdownSplit`→false (and/or drop the count branch),
  rerun → the two count tests red, the rest green; restore → all green.
- **live-e2e (CI-gated):** real BE CountReader selection / EXPLAIN `pushdown agg=COUNT (n)` —
  existing legacy paimon count regression covers the BE contract (no BE change).

## Files
- `fe/fe-connector/fe-connector-api/.../scan/ConnectorScanPlanProvider.java` — +1 default overload (**SPI change**).
- `fe/fe-core/.../datasource/PluginDrivenScanNode.java` — read agg-op + thread flag (+ `TPushAggOp` import).
- `fe/fe-connector/fe-connector-paimon/.../PaimonScanPlanProvider.java` — count branch + 2 helpers.
- `fe/fe-connector/fe-connector-paimon/.../PaimonScanPlanProviderTest.java` — count tests.

## Log entries
- `decisions-log.md`: SPI signature change signed off; collapse-to-one; param=boolean; paimon-only.
- `deviations-log.md`: the `>10000` parallel-split-trim divergence (collapse-to-one).
- `01-spi-extensions-rfc.md`: note the count-pushdown overload joins the limit/requiredPartitions chain.
