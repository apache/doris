# P5 fix #9 — `FIX-NATIVE-SUBSPLIT` (M-3)

> Round-2 severity MAJOR (round-1 MINOR), perf-parity (parallelism). User signed off (2026-06-12):
> **implement now**. Pure-connector, **zero SPI**, **zero fe-core** (engineering-confirmed by recon
> `wf_ad764bf6-1c9`).

## Problem
After cutover, a large native (ORC/Parquet) paimon data file gets **one** scanner — no intra-file
parallelism. The connector (`PaimonScanPlanProvider` native arm) emits exactly ONE `PaimonScanRange`
per `RawFile` (`.start(0).length(file.length())`). Legacy `PaimonScanNode:434-465` sub-splits each
large ORC/Parquet file via `determineTargetFileSplitSize` + `fileSplitter.splitFile`. Result is
correct (BE reads the whole file either way); only read parallelism regresses.

## Recon findings (verified vs source, `wf_ad764bf6-1c9`)
1. **Real gap, not a no-op.** ORC/Parquet infer `compressType=PLAIN` (`FileSplitter:115` via
   `Util.inferFileCompressTypeByPath`), so `FileSplitter`'s `(!splittable || compressType != PLAIN)`
   gate (`:116`) is NOT taken → real slicing at `:129-144` runs. Connector never slices.
2. **DV × sub-split is SAFE — no guard needed.** Paimon deletion-vector rowids are GLOBAL file row
   positions; BE's native readers report global row positions even within a partial byte range (ORC
   `getRowNumber()` seeded from stripe; Parquet `first_row` accumulated across all row groups before
   the split-skip). BE's `_kv_cache` shares the parsed DV bitmap across sub-splits keyed by
   `path+offset`. Iceberg uses the identical position-delete machinery on routinely-split files.
   **Rule: attach the SAME unmodified per-`RawFile` `DeletionFile` (path/offset/length) to EVERY
   sub-range — do NOT re-base offsets** (legacy parity, `PaimonScanNode:459-460`). (BE multi-range
   + DV is asserted from BE source; true end-to-end proof is live-e2e — but the legacy native path
   already ships exactly this wire shape, so it is not a new BE code path.)
3. **Pure-connector.** The compute is long arithmetic over 5 session vars read via the
   `VariableMgr.toMap` channel (`ConnectorSession.getSessionProperties()`), exactly like
   `isCppReaderEnabled`/`isForceJniScannerEnabled`. The connector cannot import fe-core
   `FileSplitter`/`SessionVariable`, so the math is re-stated with plain longs. `start`/`length`/
   `fileSize` already serialize to BE (`PaimonScanRange.Builder` → `PluginDrivenSplit` FileSplit
   ctor → `FileQueryScanNode.createFileRangeDesc` `setStartOffset/setSize/setFileSize`).
   **No SPI change, no fe-core change, no user sign-off beyond the P2 scope ack.**
4. **Only the specified-size branch is reachable.** The connector passes `blockLocations=null` and a
   target size that is **always > 0** (paimon is never batch-mode; the smallest target is
   `max_initial_file_split_size`=32MB). So `FileSplitter`'s block-based branch (`:147+`) is dead for
   the connector; only `:129-144` (the `> 1.1D` loop) must be ported.

## Design (pure-connector, surgical)
**Session keys + defaults (byte-identical to `SessionVariable`, verified):**
`file_split_size`=0, `max_initial_file_split_size`=32MB(33554432), `max_file_split_size`=64MB(67108864),
`max_initial_file_split_num`=200, `max_file_split_num`=100000.

**Two pure-static helpers (Rule 9 mutation-testable seams; mirror `shouldUseNativeReader`):**
- `computeFileSplitOffsets(long fileLength, long targetSplitSize) → List<long[]>` — ports
  `FileSplitter.splitFile`'s specified-size branch byte-for-byte, including the **`> 1.1D` tail guard**
  (the last range absorbs a remainder up to 1.1× the target instead of a tiny tail split).
  `fileLength<=0` → empty (legacy skips empty files); `targetSplitSize<=0` → single whole-file range
  (defensive; never happens on the connector path).
- `determineTargetSplitSize(fileSplitSize, maxInitialSplitSize, maxSplitSize, maxInitialSplitNum,
  maxFileSplitNum, totalNativeFileSize) → long` — ports `determineTargetFileSplitSize` +
  `applyMaxFileSplitNumLimit` (`max(target, ceil(total/maxNum))`). The `isBatchMode → 0` branch is
  omitted (paimon is never batch).

**Glue (non-static):** `resolveTargetSplitSize(session, dataSplits)` reads the 5 session vars
(`sessionLong` helper, null/blank/parse-tolerant like `isCppReaderEnabled`) + sums
`rawFile.fileSize()` over native-eligible splits, then calls the pure static. Computed **lazily once**
on the first native split (legacy `hasDeterminedTargetFileSplitSize` parity).

**Native arm change:** replace the single `buildNativeRange(file, df, fmt, partVals)` call with a loop
over `computeFileSplitOffsets(file.length(), targetSplitSize)`, each emitting a sub-range with the
SAME `deletionFile`. `buildNativeRange` gains `(start, length)` params (was hardwired `0/file.length()`);
`fileSize` stays `file.length()`.

**No DV guard** (DV-bearing files sub-split safely, §2). **Count-pushdown splittable gate (legacy
parity):** legacy passes `splittable = !applyCountPushdown` to `fileSplitter.splitFile`. Most
count-eligible splits are siphoned to the count arm (`isCountPushdownSplit`), BUT a native-eligible
split whose merged count is **not** precomputed (e.g. a deletion vector with null cardinality) is NOT
siphoned and reaches the native arm with `countPushdown=true`. Legacy keeps such a split **whole**
(`splittable=false → one whole-file split`); the connector mirrors this by passing target size `0`
to `buildNativeRanges` under count pushdown (→ single whole-file range). Correctness-neutral either
way (BE sets per-scanner agg=NONE when a DV is present without a row count, so disjoint sub-ranges
count independently and the COUNT operator sums correctly), but matching legacy keeps the split count
byte-exact and avoids an undocumented divergence. The native arm is otherwise gated to ORC/Parquet
(`supportNativeReader`) = always PLAIN/splittable.

## Out of scope / known gaps (honest)
- **Split-weight / target-size scheduling nicety:** legacy sets a per-split weight + targetSplitSize on
  the `FileSplit` for `FederationBackendPolicy` balancing. The connector's native ranges omit
  `selfSplitWeight` (**pre-existing** — the single-range native path already omitted it; #9 does not
  regress it, it just emits more ranges). Scheduling-quality only, not correctness → [DV-033].
- **Block-based splitting branch** (`FileSplitter:147+`) not ported — unreachable for the connector.

## Risk analysis
- **Correctness:** unchanged. BE reads the same bytes whether 1 or N ranges; DV applies by global row
  position per the recon. Contiguous tiling `[0..fileLength)` with no gap/overlap (unit-asserted).
- **#8 interaction:** count-eligible splits are siphoned to the count arm before the native gate.
  Under count pushdown a native-eligible split that is NOT count-eligible (no precomputed merged
  count) is kept WHOLE in the native arm (the count-pushdown splittable gate above) — legacy parity,
  no sub-split, correctness-neutral.
- **Tiny files:** `fileLength <= 1.1 × target` → 1 whole-file range (unchanged behavior).

## Test plan
- **Pure-static unit (fail-before drivable):**
  - `computeFileSplitOffsets`: 250MB/64MB → `[0,64][64,64][128,64][192,58]` (1.1× tail keeps 58MB, no
    5th split); 256MB/64MB → 4 even; `fileLen ≤ 1.1×target` → 1 whole-file; `fileLength≤0` → empty;
    `target≤0` → single. Assert contiguous tiling (no gap/overlap, Σlength = fileLength, last = remainder).
  - `determineTargetSplitSize`: `file_split_size>0` returns it; total above/below
    `max_file_split_size × max_initial_file_split_num` flips 64MB↔32MB; `max_file_split_num` floor
    raises the size; defaults when keys absent.
- **End-to-end (offline via real local table + `sessionWithProps`):** set a tiny `file_split_size` so
  even the sub-KB fixture file splits → assert `planScan` emits ≥2 native ranges that tile
  `[0..fileLength)` contiguously. Do NOT pin the exact count (parquet byte size is encoder-dependent).
- **DV-on-every-sub-range (Rule 9, load-bearing):** `buildNativeRanges` with a `DeletionFile` + a
  target that forces ≥2 ranges → assert EVERY sub-range carries the same (normalized) deletion file.
  fail-before = attach the DV only to the first sub-range → red (a real DV-bearing split is live-e2e
  only, so this is driven via the package-private `buildNativeRanges` with a hand-built `RawFile`+DV).
- **Count-pushdown whole-file:** `buildNativeRanges(..., targetSplitSize=0)` → exactly one whole-file
  range (the count-pushdown splittable gate); mutation = sub-split under count pushdown → red.
- **fail-before:** neuter `computeFileSplitOffsets` to a single whole-file range → the multi-range
  assertions go red; restore → green.
- **live-e2e (CI-gated):** real large-file parallelism + DV-bearing-file multi-range read — existing
  legacy paimon regression covers the BE contract (no BE change).

## Files
- `fe/fe-connector/fe-connector-paimon/.../PaimonScanPlanProvider.java` — 5 constants, 2 static helpers,
  `sessionLong` + `resolveTargetSplitSize`, native-arm loop, `buildNativeRange(+start,+length)`.
- `fe/fe-connector/fe-connector-paimon/.../PaimonScanPlanProviderTest.java` — static-math tests +
  end-to-end sub-split test; update 3 existing `buildNativeRange` call sites to the new signature.

## Log entries
- `decisions-log.md`: D-055 (P2 scope sign-off = implement; pure-connector design).
- `deviations-log.md`: DV-033 (split-weight scheduling nicety not ported — pre-existing, perf-only).
- `01-spi-extensions-rfc.md`: none (zero SPI).
