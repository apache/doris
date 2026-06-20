# FIX-A1 — proportional split weight on the FE FileSplit — SUMMARY

> Deviation 4/5 of the P6 deviation→fix batch. Single-task loop: design → design red-team (`wf_c8345c28-ee6`)
> → implement → impl-verify → build+UT (RED→GREEN) → commit. Detail: `FIX-A1-SPLIT-WEIGHT-design.md`.

## Problem

`PluginDrivenSplit`'s ctor never set `FileSplit.selfSplitWeight` / `targetSplitSize`, so
`FileSplit.getSplitWeight()` returned `SplitWeight.standard()` (uniform) for every plugin-driven split.
Legacy paimon set both, so `FederationBackendPolicy` distributed splits across BEs by **proportional**
(by-size) weight. Under the SPI all paimon splits got uniform weight — an FE BE-assignment skew (no rows /
route / BE-read / result change).

## Root cause (the non-obvious gap)

The task-list framed it as "the weight is already computed, just not threaded." Tracing the real code showed
that holds only for **JNI/count** splits. Two gaps had to be closed:
1. The connector's **native** ranges (`buildNativeRange`) never set `selfSplitWeight` (Builder default 0).
   Legacy native sub-splits used `selfSplitWeight = length (+ deletionFile.length())`
   (`PaimonSplit:72,112`). Native ORC/Parquet is the *default* read path, so leaving it 0 would have made
   every native split's weight `clamp(0/denom)=0.01` (uniform) — defeating the fix.
2. The weight **denominator** (legacy `PaimonScanNode:499` = `fileSplitSize>0 ? : max_file_split_size`,
   64 MB default) is a *different* value from the connector's existing file-splitting `targetSplitSize`, and
   was carried nowhere.

## Fix

- **SPI `ConnectorScanRange`**: two new default getters `getSelfSplitWeight()` / `getTargetSplitSize()`,
  sentinel `-1` = "not provided". Connector-agnostic — all 6 non-paimon impls (jdbc/es/trino/maxcompute/
  hive/hudi) inherit `-1` → keep `standard()` (no regression).
- **`PluginDrivenSplit` ctor**: set the FileSplit fields only when `weight >= 0 && target > 0` (`0` is a
  real weight; `target > 0` guards div-by-zero). Generic, no source-specific branching.
- **`PaimonScanRange`**: new `targetSplitSize` field (default `-1`) + Builder + `@Override
  getTargetSplitSize()`; `getSelfSplitWeight()` marked `@Override`.
- **`PaimonScanPlanProvider`**: `resolveSplitWeightDenominator(session)` (exact legacy formula), computed
  once and threaded as `weightDenominator` (named to avoid transposition with the file-split target) to
  every builder; `buildNativeRange` now sets `selfSplitWeight = length + DV` and `targetSplitSize =
  weightDenominator`; JNI/count add `targetSplitSize`. Applied to every split type incl. count pushdown.

Legacy parity verified exactly by the design red-team (4 lenses, all "design sound"): native `length+DV`,
denominator `fileSplitSize>0 ? : 64MB`, JNI/count `Σ fileSize`/`rowCount`, `fromProportion(clamp(w/T,
0.01,1.0))` math. FE-only — the BE-thrift `paimon.self_split_weight` (A3) stays gated on `paimonSplit != null`.

## Tests (RED→GREEN; design red-team's 6 actionable findings all folded in)

- **fe-core `PluginDrivenSplitWeightTest`** (the regression): `W=50,T=100→rawValue 50`; `W=1→clamp 1`;
  `W=0→clamp 1` (the `>=0` gate); `-1/-1→standard 100`; one-field-only → standard. Exact `getRawValue()`
  asserts so RED (standard 100) ≠ GREEN.
- **fe-connector-api `ConnectorScanRangeWeightDefaultsTest`**: defaults are `-1` (no-regression sentinel).
- **connector `PaimonScanPlanProviderTest`** (+5): Builder sentinel/round-trip; `buildNativeRange` weight
  `=length+DV` + denominator; `buildNativeRanges` positional-swap guard (split count follows the file-split
  target, every range carries the denominator); count-pushdown (target 0) still carries the denominator;
  `resolveSplitWeightDenominator` legacy formula. Updated the 6 existing changed-signature call-sites.

## Result

fe-connector-api 44/0, fe-connector-paimon 298/0/1skip (PaimonScanPlanProviderTest 57/0, +5 A1), fe-core
`PluginDrivenSplitWeightTest` 5/0; checkstyle 0; import-check 0; clean rebuild BUILD SUCCESS. **RED-verified
by mutation runs:** fe-core ctor-gate-off → the 3 proportional cases fail (`expected 50/1/1, got 100`=standard),
the 2 no-weight cases stay green; connector native-weight-drop / sentinel→0 / denominator→0 / arg-swap each →
its target test fails. Design red-team `wf_c8345c28-ee6` (4 lenses, all sound, 6 actionable folded in);
impl-verify `wf_3381cfaa-205` (2 lenses, both COMMIT_AS_IS, 0 actionable). e2e gated (`enablePaimonTest=false`)
— NOT run. Next deviation: **B-R2-be** (last of the 5).
