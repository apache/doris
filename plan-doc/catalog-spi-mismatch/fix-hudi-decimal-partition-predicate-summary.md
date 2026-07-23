# Summary — Hudi decimal partition-predicate literal rendering

## Problem
Hudi partition pruning silently dropped rows for a decimal partition column when the
equality literal carried trailing zeros (e.g. `WHERE d = 1` / `d = 1.0` on a `decimal(8,4)`
partition column returned 0 rows instead of the matching partition's rows).

## Root Cause
`HudiConnectorMetadata.extractLiteralValue` special-cased only `LocalDateTime` and fell
through to `String.valueOf(val)`, so a `BigDecimal "1.0000"` rendered with its scale and
never string-equalled the Hive-canonical stored partition value `"1"` in
`matchesPredicates`. The sibling `HiveConnectorMetadata` already fixed this (#65473) with a
`BigDecimal` branch; Hudi mirrored only the datetime branch, not the decimal one.

## Fix
Mirrored Hive's `BigDecimal` branch verbatim into `HudiConnectorMetadata.extractLiteralValue`
(`((BigDecimal) val).stripTrailingZeros().toPlainString()`) + added `import
java.math.BigDecimal;`. Connector-local, zero fe-core delta.

## Tests
Added `HudiPartitionPruningTest.testDecimalPartitionPredicatePrunesTrailingZeros`: a decimal
partition column with a scaled literal (`BigDecimal "1.0000"`) against a stored partition
value `"1"`, asserting the partition is retained. Mutation (dropping the branch) turns it red.

## Result
`HudiPartitionPruningTest`: 13 tests, 0 failures, BUILD SUCCESS. Checkstyle clean.
