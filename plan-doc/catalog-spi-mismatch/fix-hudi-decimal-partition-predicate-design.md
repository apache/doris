# Fix — Hudi decimal partition-predicate literal rendering

## Problem
Hudi partition pruning drops rows for a decimal partition column when the equality
literal carries trailing zeros. E.g. on a `decimal(8,4)` partition column, `WHERE d = 1`
(literal arrives as `BigDecimal "1.0000"`) fails to match the Hive-canonical stored
partition value `"1"` → the partition is pruned away → data silently missing (no error).

## Root Cause
`HudiConnectorMetadata.extractLiteralValue` (HEAD `:1063-1079`) special-cases only
`LocalDateTime`, then falls through to `String.valueOf(val)` for everything else. A
`BigDecimal` therefore renders with its declared scale (`"1.0000"`), while the stored
partition value (HMS partition names / Hudi relative paths, both Hive-canonical) is
trailing-zero-trimmed (`"1"`). `matchesPredicates` (`:1158-1169`) does an exact-string
`allowedValues.contains(actualValue)` compare → miss → partition dropped.

The sibling `HiveConnectorMetadata.extractLiteralValue` (`:2345-2352`) already fixed this
(PR #65473) with a `BigDecimal` branch: `stripTrailingZeros().toPlainString()`. Hudi's
method was copied from Hive but only mirrored the `LocalDateTime` branch, not the
`BigDecimal` one. Trigger is broad: any literal whose text has trailing zeros Hive strips
(`d = 1`, `d = 1.0`, `d = 1.50` on a scaled column).

## Design
Mirror Hive's `BigDecimal` branch verbatim into Hudi's `extractLiteralValue`. Pure
string rendering, gated on `instanceof BigDecimal`; no other type is affected. Zero
fe-core delta (respects the "fe-core source only shrinks" iron rule — every hop lives in
`fe-connector-hudi`).

## Implementation Plan
1. Add `import java.math.BigDecimal;` (sorts before `java.time.LocalDateTime` at `:57`).
2. Insert the `BigDecimal` branch (with the same explanatory comment as Hive) after the
   `LocalDateTime` branch and before `return String.valueOf(val);`.

## Risk Analysis
- The fix is correct only if the stored partition value is trailing-zero-trimmed, which
  holds because both stored-value sources are Hive-style partition names. Mirrors Hive.
- `BigDecimal.ZERO.stripTrailingZeros()` can render `"0E-N"` on old JDKs, but
  `toPlainString()` normalizes to `"0"` — Hive already relies on this exact composition.
- Byte-safe: pure FE-side string rendering, no BE-facing serialization; non-decimal
  literals untouched.

## Test Plan
### Unit Tests
Add to `HudiPartitionPruningTest` a decimal-partition EQ pruning test mirroring the
existing DATE non-regression test: a `decimal` partition column, a scaled literal
(`BigDecimal "1.0000"`) against a stored partition value `"1"`, assert the partition is
retained (currently it would be dropped). Hudi has zero decimal coverage today.

### E2E Tests
Deferred with the batch's e2e (iceberg/paimon/hudi TVF + pruning e2e is being staged at
the P6.6 cutover; unit coverage pins the behavior now).
