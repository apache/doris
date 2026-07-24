# FIX-A3 — JNI split `self_split_weight` omitted when weight is 0 — SUMMARY

> P6 deviation→fix (1 of 5). Severity NIT (profile-parity only). Design + design red-team
> (`wf_3f2cd605-2a8`, 9 candidates → 0 actionable on the code) + RED→GREEN UT + impl-verify (APPROVE).

## Problem

The paimon connector emitted the BE thrift profile property `paimon.self_split_weight` only when the
computed weight was `> 0`. A JNI split whose genuine weight is exactly **0** (a non-`DataSplit` system
split with `rowCount()==0`, or a `DataSplit` whose files sum to `fileSize==0`) therefore left the
property unset, and BE fell back to the `-1` "unset" sentinel instead of `0`.

## Root Cause

`PaimonScanRange` constructor gated on the value: `if (builder.selfSplitWeight > 0)`
(`fe-connector-paimon/.../PaimonScanRange.java:92`). `selfSplitWeight` is a primitive `long`
defaulting to `0`, so the `> 0` check doubled as a crude "is-set?" proxy — conflating native splits
(which never set a weight, default 0; correctly suppressed) with JNI splits whose genuine weight is 0
(incorrectly suppressed). The property is consumed only on the JNI branch of `populateRangeParams`
and feeds only BE's `_max_time_split_weight_counter` profile counter (`jni_reader.cpp:246`); BE
defaults to `-1` when the thrift field is unset (`paimon_jni_reader.cpp:95`).

Legacy `PaimonScanNode.setPaimonParams:274` sets the weight **unconditionally on the JNI branch and
never on the native branch** — so the parity rule is simply "emit iff JNI split."

## Fix

One-line gate change (+ explanatory comment) in `PaimonScanRange` constructor:

```java
if (builder.paimonSplit != null) {                     // was: if (builder.selfSplitWeight > 0)
    props.put("paimon.self_split_weight", String.valueOf(builder.selfSplitWeight));
}
```

Gating on the JNI marker (`paimonSplit`) rather than the value emits the genuine weight (incl. 0) for
every JNI split, never adds the key to native splits (exact legacy parity, no cosmetic drift), and is
symmetric with the consumer (`populateRangeParams` reads the prop iff `paimon.split` present). Both
JNI build sites (`buildJniScanRange`, `buildCountRange`) always set both `paimonSplit` and
`selfSplitWeight`, so the gate can neither under- nor over-emit; weight is provably ≥ 0
(fileSize-sum / `rowCount()`), so this is BE-thrift-identical to the task-list's literal "drop the
`> 0` gate" while being the cleanest, most legacy-faithful form. No SPI/interface/BE change.

## Tests

New `PaimonScanRangeSelfSplitWeightTest` (3 tests, direct `PaimonScanRange.Builder`):
1. **JNI split, weight 0** — drives `populateRangeParams` and asserts `isSetSelfSplitWeight()` &&
   `getSelfSplitWeight()==0` (BE-visible, load-bearing) + props `"0"`. **RED before** the fix
   (verified by an actual run: 1 failure on unfixed code — prop absent → thrift unset → BE -1).
2. **JNI split, weight > 0** — positive coverage (no prior test asserted this property).
3. **Native split** — `!isSetSelfSplitWeight()` (BE-visible native parity) + props key absent
   (gate-choice pin: switching to `>= 0` would make this RED).

## Result

- RED→GREEN verified by separate runs: unfixed → 1 failure (test 1); fixed → **3/0/0**.
- Full paimon module: **283 run / 0 failures / 0 errors / 1 skipped** (gated e2e); checkstyle 0
  violations; `tools/check-connector-imports.sh` exit 0.
- Design red-team (3 lenses → verifier): core fix CONFIRMED correct; only doc/test-plan refinements
  (folded in). Impl-verify reviewer: **APPROVE**, no actionable issues.
- e2e is gated (`enablePaimonTest=false`) — NOT run (profile-counter parity is not asserted by any
  regression suite).
