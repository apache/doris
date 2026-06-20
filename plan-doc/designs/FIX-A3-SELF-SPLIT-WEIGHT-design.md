# FIX-A3 — JNI split `self_split_weight` omitted when weight is 0

> Source: `task-list-P6-deviation-fixes.md` §A3 / `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §R1 (be).
> Severity: **NIT** (profile-parity only). Smallest blast radius of the 5 deviation→fixes.

## Problem

The paimon connector gates emission of the BE thrift profile property `paimon.self_split_weight`
on the **value** `selfSplitWeight > 0`. A JNI split whose computed weight is exactly **0** therefore
leaves the property unset, and BE falls back to `-1` instead of `0`.

Weight-0 JNI splits are real:
- a non-`DataSplit` metadata/system split with `split.rowCount() == 0`
  (`buildJniScanRange:732` → `splitWeight = split.rowCount()`), or
- a `DataSplit` whose data files sum to `fileSize == 0` (`computeSplitWeight:885-891`).

Legacy emitted the weight **unconditionally** for every JNI split (incl. 0).

## Root Cause

`PaimonScanRange` constructor, `fe-connector-paimon/.../PaimonScanRange.java:92`:

```java
if (builder.selfSplitWeight > 0) {                                   // <-- value gate
    props.put("paimon.self_split_weight", String.valueOf(builder.selfSplitWeight));
}
```

The `> 0` predicate was doing double duty as a crude "is this set?" proxy. `selfSplitWeight` is a
primitive `long` defaulting to `0`, so it conflates two distinct things:
1. native splits (which never call `.selfSplitWeight(...)`, default 0) — should NOT emit, and
2. JNI splits whose genuine weight is 0 — **should** emit (this is the bug).

The property is **consumed** only on the JNI branch of `populateRangeParams:185-197`
(inside `if (paimonSplitVal != null)`), via `rangeDesc.setSelfSplitWeight(...)`. It feeds only BE's
`_max_time_split_weight_counter` (`be/.../jni_reader.cpp:246`, a `ConditionCounter`) — a **profile
counter**. It never influences rows / counts / predicates / schema / routing.

## Legacy parity (the authoritative reference)

`PaimonScanNode.setPaimonParams` (fe-core `datasource/paimon/source/PaimonScanNode.java:253-287`):

- **JNI/cpp branch** (`split != null`, line 274): `rangeDesc.setSelfSplitWeight(paimonSplit.getSelfSplitWeight())`
  — **unconditional**, no `> 0` guard. Emitted for every JNI split including weight 0.
- **Native branch** (`split == null`, lines 275-287): `setSelfSplitWeight` is **never called**.
  Native splits never carry the thrift weight → BE defaults `-1`.

So legacy's rule is simply: **emit the weight iff the split is a JNI split.**

## Design

Change the constructor gate from a value check to the JNI-split check, exactly mirroring legacy and
making the property's lifecycle symmetric (emitted iff consumed — `populateRangeParams` reads it only
when `paimon.split` is present):

```java
// FIX-A3: emit the self-split-weight for every JNI split, incl. weight 0. Legacy
// PaimonScanNode.setPaimonParams:274 sets it unconditionally on the JNI branch (never on native);
// the old `selfSplitWeight > 0` gate was a buggy is-set proxy that dropped a genuine weight-0 JNI
// split (rowCount-0 sys split / fileSize-0 DataSplit) -> BE read the -1 "unset" sentinel instead of
// 0, corrupting the profile _max_time_split_weight_counter. Gate on the JNI marker (paimonSplit) so
// native splits keep parity (no weight); this is also exactly when populateRangeParams reads it.
if (builder.paimonSplit != null) {
    props.put("paimon.self_split_weight", String.valueOf(builder.selfSplitWeight));
}
```

### Why gate on `paimonSplit != null`, not `>= 0`

`task-list-P6-deviation-fixes.md` §A3 phrases the fix as "drop the `> 0` gate ... always emit." Since
the weight is always ≥ 0 (a fileSize-sum or a `rowCount()`; `computeSplitWeight:885-891`), "drop the
gate" / `>= 0` / `paimonSplit != null` are all behaviorally identical at the **BE thrift level** for
JNI splits — they all emit the genuine weight incl. 0. The choice below is only about which form is the
cleanest, most legacy-faithful expression.

Both fix the reported bug (BE thrift identical for JNI). But `>= 0` would also start writing
`paimon.self_split_weight=0` into the **props map of native splits** (they default to weight 0).
That key is never read on the native branch, so it is harmless to BE — but it is a needless divergence
from legacy (which never set the weight on native) and a cosmetic change to native splits' internal
props. Gating on the JNI marker:
- emits 0 for JNI splits (fixes A3),
- never adds the key to native splits (exact legacy parity, no cosmetic drift),
- is symmetric with the consumer (`populateRangeParams` reads it iff `paimon.split` present).

Both JNI build sites (`buildJniScanRange:742-748`, `buildCountRange:773-781`) always call
`.paimonSplit(...)` **and** `.selfSplitWeight(...)`, so this never under-emits for a real JNI split.

## Implementation Plan

Single-line change in `fe/fe-connector/fe-connector-paimon/.../PaimonScanRange.java` constructor
(line 92): replace `if (builder.selfSplitWeight > 0)` with `if (builder.paimonSplit != null)` + the
explanatory comment above.

No SPI/interface change, no BE change, no other call-site change.

## Risk Analysis

- **Native splits:** unchanged at the BE thrift level (native branch of `populateRangeParams` never
  reads/sets the weight). With the JNI-marker gate they also keep an unchanged props map (no new key).
- **JNI splits with weight > 0:** unchanged (still emitted).
- **JNI splits with weight 0:** now emit `0` (the fix). BE reads `0` instead of `-1` — corrects the
  profile counter; no functional path touched.
- **Negative weight:** not reachable — weight is a fileSize-sum or a `rowCount()`, both ≥ 0. Even if
  it were, legacy emitted unconditionally, so emitting it is parity-correct.
- No correctness/perf/route impact — profile-only. No regression test currently asserts this line
  (so nothing to update; we ADD coverage).

## Test Plan

### Unit Tests (fe-connector-paimon, `org.apache.doris.connector.paimon`)

New `PaimonScanRangeSelfSplitWeightTest` (direct `PaimonScanRange.Builder`, same style as
`PaimonScanRangePartitionNullTest`). No existing test asserts `self_split_weight` (verified: 0 hits in
the test tree) → all three are NEW coverage, nothing to update.

1. **JNI split, weight 0 — the fix, BE-visible (load-bearing):** drive `populateRangeParams` and
   assert `rangeDesc.isSetSelfSplitWeight() && rangeDesc.getSelfSplitWeight() == 0` — this is the
   legacy `:274` parity target and proves BE reads `0`, not the `-1` unset sentinel. Also assert the
   props map carries `paimon.self_split_weight == "0"`. RED before: with the `> 0` gate, prop absent
   → `populateRangeParams` never calls `setSelfSplitWeight` → `isSetSelfSplitWeight()` false → BE -1.
2. **JNI split, weight > 0 — positive coverage (NEW):** prop present and matches; pins the positive
   case keeps working (no prior test covered it).
3. **Native split (no `paimonSplit`) — native unaffected, BE-visible:** drive `populateRangeParams`
   on a native range and assert `!rangeDesc.isSetSelfSplitWeight()` (native never carries the weight,
   legacy parity — native branch sets ORC/PARQUET, never the weight). Additionally assert the props
   map does NOT contain `paimon.self_split_weight` — this is the only assertion that pins the chosen
   JNI-marker gate over `>= 0` (with `>= 0`, native gains a BE-invisible `=0` key); labeled as the
   gate-choice pin, distinct from the BE-visible parity assertion above.

RED→GREEN mutation: restoring the old `> 0` gate turns test 1 red (weight-0 JNI: prop absent +
`isSetSelfSplitWeight()` false). Switching to `>= 0` turns test 3's props-key-absent assertion red.

### E2E Tests

None. Profile-counter parity is not asserted by any regression suite and constructing a deterministic
weight-0 JNI split end-to-end (paimon SDK split with rowCount 0 / fileSize 0) is not worth a live
suite for a NIT. e2e is gated (`enablePaimonTest=false`) regardless.

## Build / Verify

- `mvn -f .../fe/pom.xml -pl :fe-connector-paimon -am package -Dassembly.skipAssembly=true
  -Dmaven.build.cache.enabled=false -DfailIfNoTests=false` (HiveConf in shade jar; checkstyle in
  `validate`).
- `tools/check-connector-imports.sh` must stay exit 0 (no fe-core import added).
- Confirm the new test fails on the pre-fix gate (mutation), passes on the fix.
