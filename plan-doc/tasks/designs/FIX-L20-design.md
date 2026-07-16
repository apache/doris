# FIX-L20 — maxcompute EQ predicate emits `==` instead of `=`

## Problem (HEAD-verified)
`MaxComputePredicateConverter.convertComparison` (fe-connector-maxcompute) hand-writes the ODPS
operator symbols in a `switch`, and the `EQ` arm emits Java's `==`:

```java
// MaxComputePredicateConverter.java:169-170
case EQ:
    opDesc = "==";
    break;
```

The built string (`new RawPredicate(columnName + " " + opDesc + " " + value)`) is pushed to ODPS.
MaxCompute — like standard SQL — has **no `==` operator**; equality is a single `=`. So an equality
predicate pushes `id == 5`, which ODPS does not accept: the pushdown is lost and the scan degrades to
a full scan (a silent perf regression). The other five arms (`!=`,`<`,`<=`,`>`,`>=`) are correct.

## Root cause — why legacy never had this
Legacy fe-core (`1da88365e85^:MaxComputeScanNode.convertExprToOdpsPredicate`) did **not** hand-write
symbols. It mapped Doris' operator to the ODPS SDK's own `BinaryPredicate.Operator` enum and took the
symbol from the SDK:

```java
switch (binaryPredicate.getOp()) {
    case EQ: odpsOp = BinaryPredicate.Operator.EQUALS; break;  // ...
}
stringBuilder.append(odpsOp.getDescription());   // authoritative ODPS symbol
```

The migration to the plugin replaced this "map-to-SDK-enum + `getDescription()`" with a hand-written
symbol table, and the `EQ` entry drifted to Java's `==` (the other five happened to be copied right).
The defect is structural: **a human should not re-type what the SDK already defines.**

## Evidence (static, decisive — no live A/B needed)
- **SDK bytecode** (`odps-sdk-table-api-0.48.8-public.jar`, `BinaryPredicate$Operator`):
  `EQUALS` description = `"="`, `NOT_EQUALS`=`"!="`, `GREATER_THAN`=`">"`, `LESS_THAN`=`"<"`,
  `GREATER_THAN_OR_EQUAL`=`">="`, `LESS_THAN_OR_EQUAL`=`"<="`. So legacy pushed `id = 5`.
- **connector-api** `ConnectorComparison.Operator.EQ` itself declares symbol `"="` (`:35`).
- Both the ODPS SDK and Doris' own SPI agree the symbol is `=`; only the converter's hand-written
  table says `==`. This eliminates the "does ODPS tolerate `==`?" uncertainty the review flagged.

## Decision (user, 2026-07-13)
User asked "why not do it like the old code / why didn't the old code have this?" → **restore the legacy
pattern**: map `ConnectorComparison.Operator` to the ODPS SDK `BinaryPredicate.Operator` and emit
`getDescription()`, rather than merely patching `"=="`→`"="` in the hand-written table. This fixes the
bug *and* removes the whole class of hand-typed-symbol drift (single authoritative source = the SDK).

## Change
- Rewrite `convertComparison`'s `switch` to produce a `BinaryPredicate.Operator` (EQ→EQUALS, NE→NOT_EQUALS,
  LT→LESS_THAN, LE→LESS_THAN_OR_EQUAL, GT→GREATER_THAN, GE→GREATER_THAN_OR_EQUAL); build the RawPredicate
  string with `odpsOp.getDescription()`. Keep `RawPredicate` (the custom value formatting for
  datetime/string quoting stays — same as legacy, which also used RawPredicate here).
- `default:` still `throw UnsupportedOperationException` → caught by `convertOne` → `NO_PREDICATE`.
  This covers `EQ_FOR_NULL` (`<=>`), which has **no** ODPS `BinaryPredicate` equivalent and must not be
  pushed — matching legacy's `default: odpsOp = null` (skip). BE re-filters.
- Add import `com.aliyun.odps.table.optimizer.predicate.BinaryPredicate` (ODPS SDK; the module already
  imports `RawPredicate`/`Attribute`/`CompoundPredicate`/`UnaryPredicate` from the same package — no
  fe-core dependency introduced).

Only behavioral delta: `EQ` output `id ==  5` → `id =  5`. NE/LT/LE/GT/GE byte-identical to today.

## Companion (task-list): IN-direction regression test
The IN-polarity fix (`col IN (...)`, not the reversed form) has no dedicated test. Add guards so a future
regression is caught offline:
- `testEqualsEmitsSingleEqualsNotDoubleEquals` — EQ pushes `= ` and never `==` (RED on current code).
- `testAllComparisonOperatorsEmitSdkSymbols` — NE/LT/LE/GT/GE map to their SDK symbols (guards the switch).
- `testEqForNullIsNotPushedDown` — `EQ_FOR_NULL` → `NO_PREDICATE` (not pushed as `<=>`).
- `testInListEmitsColumnThenValues` / `testNotInListEmitsColumnThenNotIn` — direction guard for IN/NOT IN.

## Iron rule
Entirely in fe-connector-maxcompute, switching on the connector-api `ConnectorComparison.Operator` and
mapping to the ODPS SDK enum. No fe-core touch, no source-name branching, no property parsing.
`bash tools/check-connector-imports.sh` must stay exit 0.

## Adversarial self-check
- *Could NE/LT/… change?* No — `getDescription()` returns exactly the strings the arms already emit.
- *Could dropping the hand-written table lose an operator?* The SDK enum whitelists the 6 ODPS-supported
  operators; `EQ_FOR_NULL` and anything else fall to `default: throw` → dropped (safe superset; BE filters).
- *Is `==` maybe secretly accepted by ODPS?* Irrelevant now — the SDK's own canonical form is `=`, and we
  align to the authority; keeping `==` would be knowingly diverging from both SDK and legacy.
- *TCCL / classloader?* None — pure string building, no reflection.

## Blast radius
Only equality predicate pushdown. No offline golden asserts the operator string (regression suites under
`external_table_p2/maxcompute/*` are live-gated, real ODPS). Perf-only correctness (restores lost pushdown).

## e2e (live-gated)
Real ODPS `type=maxcompute` catalog: `SELECT ... WHERE k = <v>` — assert EXPLAIN/profile shows the predicate
pushed (not full scan) and the result set matches; `WHERE k IN (...)` / `NOT IN (...)` direction correct.
Cannot run locally (needs live ODPS credentials); registered live-gated.
