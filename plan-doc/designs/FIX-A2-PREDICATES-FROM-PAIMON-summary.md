# FIX-A2 — re-emit the legacy `predicatesFromPaimon:` EXPLAIN line — SUMMARY

> P6 deviation→fix (2 of 5). Severity MINOR (diagnostic-only). Design + design red-team
> (`wf_c67cb558-ff4`, 13 candidates → 0 actionable on code; folded doc/test refinements) + RED→GREEN UT
> + impl-verify (APPROVE, with its own mutation check).

## Problem

Legacy `PaimonScanNode.getNodeExplainString` (`PaimonScanNode.java:660-668`) printed the Paimon
`Predicate` objects actually pushed to the SDK (or ` NONE`):

```
predicatesFromPaimon:
    <predicate-1>
```

The SPI scan path lost it. The connector's `appendExplainInfo` emitted only `paimonNativeReadSplits=` +
VERBOSE `PaimonSplitStats`; the generic node emits `PREDICATES: <sql>` (the Doris-level conjuncts), NOT
the SDK-converted set. So a silently-dropped conjunct (the converter drops LTZ / FLOAT / unsupported CAST)
was no longer observable.

## Root Cause

Pure missing-port. The diagnostic value is the delta between `PREDICATES:` (all conjuncts) and
`predicatesFromPaimon:` (the pushed subset), which `PaimonPredicateConverter.convert` narrows by silently
null-skipping unconvertible predicates.

## Fix

In the connector's `appendExplainInfo`, **deserialize the already-present `paimon.predicate` prop** (the
exact `List<Predicate>` pushed to BE — `getScanNodeProperties:579` always emits it via
`InstantiationUtil.serializeObject` + Base64) and render the legacy block, placed **between** the
`paimonNativeReadSplits=` line and the VERBOSE `PaimonSplitStats` block (exact legacy order
`PaimonScanNode:657-671`). New private helper `appendPredicatesFromPaimon`.

Chosen over the task-list's suggested "re-run the converter" because the filter is not in the SPI seam
(it carries only the props map) and the provider is re-instantiated per call (no field). Deserializing
the existing prop renders precisely what BE receives, with the smallest change: no SPI signature change,
no new prop, no redundant serialization, no field, no BE impact (`populateScanLevelParams` reads props
key-by-key, so an unread key would not reach BE anyway).

Robustness: gated on `paimon.predicate != null` (absent ≠ empty → skip, preserving the existing
exact-equality explain tests); decode failure → `LOG.warn` + skip (never breaks EXPLAIN); null
deserialized list → skip before the label. Decode with `Predicate.class.getClassLoader()` (the plugin CL,
TCCL-independent).

## Tests

4 new tests in `PaimonScanExplainTest` (build `paimon.predicate` exactly as production does —
`InstantiationUtil.serializeObject` + Base64):
1. **Non-empty pushed predicate** — exact-equality incl. double-prefix indent (RED before: line absent).
2. **Empty list → `predicatesFromPaimon: NONE`** (RED before).
3. **Ordering** — `paimonNativeReadSplits < predicatesFromPaimon < PaimonSplitStats` under VERBOSE (RED before).
4. **Absent prop → skip** (mirrors the sibling synthetic-keys-absent guard; pins absent ≠ empty; green
   pre-fix — pins the contract that keeps the existing exact-equality tests green).

## Result

- RED→GREEN by separate runs: unfixed → 3 failures (tests 1-3); fixed → `PaimonScanExplainTest` **17/0/0**.
- Full paimon module: **287 run / 0 failures / 0 errors / 1 skipped** (gated e2e); checkstyle 0;
  `tools/check-connector-imports.sh` exit 0.
- Design red-team: design sound + legacy-faithful, no BLOCKER/MAJOR; folded refinements (corrected the
  "no BE bloat" rationale → "no redundant serialization"; null→skip before label; added the absent→skip
  test; doc wording). Impl-verify: **APPROVE** (ran its own neuter → 3 tests RED, then restored).
- e2e gated (`enablePaimonTest=false`) — NOT run (no regression suite asserts this line).
