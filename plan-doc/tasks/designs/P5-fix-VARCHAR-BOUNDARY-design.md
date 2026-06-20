# P5-fix FIX-VARCHAR-BOUNDARY (P4 / review §5 N10.1)

> Pure-connector, display-only exact-parity restore. One of 3 actionable P4 MINOR/NIT items (user-signed scope, 2026-06-12).

## Problem
Read-direction type mapping widens a paimon `VARCHAR(65533)` column to `STRING` on the plugin
path, whereas legacy reports `VARCHAR(65533)`. Observable in `DESCRIBE` / `SHOW CREATE TABLE` /
`information_schema.columns` only — data and read correctness are identical (STRING is a strict
superset of the bounded VARCHAR; no truncation either way).

## Root Cause
`PaimonTypeMapping.toVarcharType` (`fe-connector-paimon/.../PaimonTypeMapping.java:113-119`):

```java
if (len <= 0 || len >= 65533) {     // <-- `>= 65533` over-widens the exact-fit max VARCHAR
    return ConnectorType.of("STRING");
}
return ConnectorType.of("VARCHAR", len, 0);
```

Legacy `PaimonUtil.paimonPrimitiveTypeToDorisType` (`fe-core/.../paimon/PaimonUtil.java:239-244`):

```java
if (varcharLen > 65533) {           // <-- `> 65533`: 65533 falls through to VARCHAR(65533)
    return ScalarType.createStringType();
}
return ScalarType.createVarcharType(varcharLen);
```

`65533 == ScalarType.MAX_VARCHAR_LENGTH` is a legal exact-fit VARCHAR, not the STRING wildcard.
The connector's `>=` is an off-by-one at exactly that boundary value.

## Design
Change the boundary comparison `>= 65533` → `> 65533` to match legacy byte-for-byte.

Keep the `len <= 0` defensive guard untouched (Rule 3 — surgical). It has no legacy equivalent
but is unreachable from real paimon (paimon `VarCharType` minimum length is 1), so it causes no
observable divergence and removing it would be a behaviorally-inert cosmetic edit.

CHAR is already at parity (`len > 255` on both sides) — out of scope.

## Implementation Plan
1 file, 1 char: `PaimonTypeMapping.java:115` `len >= 65533` → `len > 65533`.

## Risk Analysis
None. Pure FE-side reported-type metadata; no thrift/BE/SPI surface; no behavioral change other
than the intended boundary. The only newly-affected input is exactly `len == 65533`.

## Test Plan
### Unit Tests
New focused `PaimonTypeMappingReadTest` (read-direction sibling of `PaimonTypeMappingToPaimonTest`)
pinning the boundary intent:
- `VarCharType(65532)` → `VARCHAR(65532)` (below boundary, unchanged).
- `VarCharType(65533)` → `VARCHAR(65533)` (**the fix**; fail-before: connector returns STRING).
- `VarCharType(65534)` → `STRING` (above boundary, unchanged; matches legacy `> 65533`).

WHY (Rule 9): encodes that 65533 is the exact-fit max VARCHAR (= MAX_VARCHAR_LENGTH), not a
STRING wildcard — distinguishing `>=` (wrong) from `>` (correct). A test that only checked a
mid-range length could not fail when the boundary regresses.

Fail-before: with `>=`, the 65533 assertion is red. Pass-after: green.

### E2E Tests
None added. Reported-type parity is covered by the existing legacy paimon DESCRIBE/SHOW regression
(CI-gated); this fix carries no BE change.
