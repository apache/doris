# FIX-AUTOINC-REJECT (P4-T06e) — design

> 8th cutover-fix (DDL/列校验). Scope: fe-connector-api (SPI additive field) + fe-core (converter)
> + fe-connector-maxcompute (validation). Additive SPI field, zero-break for the other 6
> connectors. Surgical (Rule 3).
> Source: clean-room re-review DG-5 / F24 (`plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md`,
> §DG-5 / §C domain-3 / §D F24). Minor regression. UT-only truth-gate (no live ODPS needed: the
> rejection is pure FE-side validation, never reaches ODPS).
> User decision (honored): **ADD SPI FIELD (full parity), NOT a deviation.**

## Problem

Legacy MaxCompute `CREATE TABLE` explicitly rejected `AUTO_INCREMENT` columns with a clear
error. After the SPI cutover, `CREATE TABLE ... (id INT AUTO_INCREMENT, ...) ENGINE=...` against a
MaxCompute catalog **silently succeeds, dropping the auto-inc semantics** — a data-model
regression. The user expects the column to behave as auto-increment; MaxCompute cannot store it,
and the table is created anyway with `id` as a plain column. No error, no warning.

Two enabling conditions make the bug live:

1. **Nereids upstream does NOT reject auto-inc for external (non-OLAP) tables.** The historical
   claim in `P4-maxcompute-migration.md:117` ("nereids upstream already rejects") is FALSE for
   auto-inc. `ColumnDefinition.validate(boolean isOlap, ...)` is the only nereids gate, and its
   sole auto-inc check is line 666-667 — and that fires **only for generated columns**
   (`generatedColumnDesc.isPresent()`), not plain auto-inc columns. There is no `isOlap==false`
   path that rejects a bare auto-inc column. So an auto-inc column flows cleanly through nereids
   analysis into the connector create-table request.
2. **The SPI carrier cannot represent auto-inc.** `ConnectorColumn` has no `isAutoInc` field, so
   even if the connector wanted to reject it, the flag is invisible by the time it reaches
   `validateColumns`.

## Root Cause   (confirmed file:line — cutover vs legacy)

Verified against the actual code on branch `catalog-spi-05`:

- **SPI carrier drops the flag.** `ConnectorColumn`
  (`fe/fe-connector/fe-connector-api/.../ConnectorColumn.java:25-99`) has exactly 6 fields:
  `name, type, comment, nullable, defaultValue, isKey` (lines 27-32). No `isAutoInc`. Two ctors:
  5-arg (`:34-37`, delegates to 6-arg with `isKey=false`) and 6-arg (`:39-47`). `equals`/`hashCode`
  (`:73-93`) cover only those 6 fields.
- **Converter drops the flag.** `CreateTableInfoToConnectorRequestConverter.convertColumns`
  (`fe/fe-core/.../connector/ddl/CreateTableInfoToConnectorRequestConverter.java:83-93`) builds
  each `ConnectorColumn` from a `ColumnDefinition` passing `d.getName(), type, d.getComment(),
  d.isNullable(), null, d.isKey()` — it reads `isKey()` but never reads `getAutoIncInitValue()`.
  A column is auto-inc when `getAutoIncInitValue() != -1` (default `-1`, field decl
  `ColumnDefinition.java:69`; getter `:651-652`; the `!= -1` semantics are also how `toSql` decides
  to emit `AUTO_INCREMENT`, `:225-230`).
- **Connector validation cannot see it.** `MaxComputeConnectorMetadata.validateColumns`
  (`fe/fe-connector/fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java:424-438`, called
  from `createTable` at `:348` after the `tableExist` short-circuit) checks only: empty/null
  (`:425-428`), duplicate name (`:431-433`), and representable type (`:436`, via
  `MCTypeMapping.toMcType`). There is no auto-inc check because the flag was never carried.

Net: auto-inc reaches the connector but is invisible there, so it is silently dropped.

## Parity Reference   (exact legacy code being mirrored)

Legacy `MaxComputeMetadataOps.validateColumns`
(`fe/fe-core/src/main/java/org/apache/doris/datasource/maxcompute/MaxComputeMetadataOps.java:416-437`),
the auto-inc half is lines **422-425** (verified verbatim):

```java
private void validateColumns(List<Column> columns) throws UserException {
    if (columns == null || columns.isEmpty()) {
        throw new UserException("Table must have at least one column.");
    }
    Set<String> columnNames = new HashSet<>();
    for (Column col : columns) {
        if (col.isAutoInc()) {                                                    // :422
            throw new UserException(                                              // :423
                    "Auto-increment columns are not supported for MaxCompute tables: " + col.getName());  // :424
        }                                                                         // :425
        if (col.isAggregated()) { ... }                                           // :426-429  OUT OF SCOPE (F31)
        ...
    }
}
```

We mirror the auto-inc branch (`:422-425`) exactly, including the error message text.

**Out of scope (do NOT add):** the aggregation-column branch (`:426-429`). Per report F31 it is
already covered by the non-OLAP key-column path; this fix touches auto-inc only.

## Design   (chosen approach + WHY)

**User-chosen direction (honored): add an `isAutoInc` field to the SPI `ConnectorColumn`** and
thread it end-to-end (converter → connector validation), restoring full legacy parity rather than
registering a deviation.

WHY this over the alternatives:
- It is the only approach that gives **full parity**: the connector re-rejects auto-inc with the
  same message legacy used, instead of accepting-and-documenting (a deviation the user explicitly
  declined).
- It follows the **established additive-SPI pattern** in this codebase (P0-1/2/3 capabilities, the
  P1-4 6-arg `planScan` overload, and the very `isKey` field that was itself added as a 6-arg
  overload over a 5-arg base): add a NEW ctor overload + field with a `default` that makes the
  prior arity delegate with the safe default (`isAutoInc=false`). All existing `new
  ConnectorColumn(` call sites keep compiling and keep `isAutoInc=false`, so the 7 other connectors
  (es/jdbc/hive/hudi/iceberg/paimon/trino) and all read-path producers are zero-break.
- It is minimal (Rule 2): one field + one ctor + one getter + equals/hashCode update in the SPI,
  one arg in the converter, one `if` in the connector. Nothing speculative; no SPI method-signature
  change (only an additive ctor).

The `defaultValue`-carrier gap noted in the converter comment (`:87-89`) is unrelated and stays
untouched (Rule 3).

## Implementation Plan   (ordered, file-by-file)

### 1. `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorColumn.java`   (SPI, additive)

- Add field after `isKey` (`:32`): `private final boolean isAutoInc;`
- Keep the 5-arg ctor (`:34-37`) unchanged (still delegates to the 6-arg).
- Change the existing **6-arg** ctor (`:39-47`) so it **delegates** to the new 7-arg with
  `isAutoInc=false` (preserves existing behavior for the 6-arg call sites — EsTypeMapping:185
  isKey=true, converter:90):
  ```java
  public ConnectorColumn(String name, ConnectorType type, String comment,
          boolean nullable, String defaultValue, boolean isKey) {
      this(name, type, comment, nullable, defaultValue, isKey, false);
  }
  ```
- Add the new **7-arg** ctor (the only one that assigns all fields):
  ```java
  public ConnectorColumn(String name, ConnectorType type, String comment,
          boolean nullable, String defaultValue, boolean isKey, boolean isAutoInc) {
      this.name = Objects.requireNonNull(name, "name");
      this.type = Objects.requireNonNull(type, "type");
      this.comment = comment;
      this.nullable = nullable;
      this.defaultValue = defaultValue;
      this.isKey = isKey;
      this.isAutoInc = isAutoInc;
  }
  ```
  (The 5-arg ctor continues to call the 6-arg, which now reaches the 7-arg with `isAutoInc=false`.)
- Add getter after `isKey()` (`:69-71`): `public boolean isAutoInc() { return isAutoInc; }`
- Update `equals` (`:81-88`): add `&& isAutoInc == that.isAutoInc`.
- Update `hashCode` (`:90-93`): add `isAutoInc` to `Objects.hash(...)`.
- `toString` (`:95-98`): leave unchanged (optional per issue; auto-inc not part of the existing
  textual contract — Rule 3, no speculative change).

### 2. `fe/fe-core/src/main/java/org/apache/doris/connector/ddl/CreateTableInfoToConnectorRequestConverter.java`   (passthrough)

- In `convertColumns` (`:90-92`), pass the auto-inc flag as the new 7th arg:
  ```java
  out.add(new ConnectorColumn(
          d.getName(), type, d.getComment(),
          d.isNullable(), null, d.isKey(), d.getAutoIncInitValue() != -1));
  ```
  No new imports needed (`ColumnDefinition` already imported, `getAutoIncInitValue()` is on it).

### 3. `fe/fe-connector/fe-connector-maxcompute/src/main/java/org/apache/doris/connector/maxcompute/MaxComputeConnectorMetadata.java`   (validation, parity)

- In `validateColumns` (`:430-437` loop body), add the auto-inc check FIRST inside the loop,
  mirroring legacy ordering (`:422-425`):
  ```java
  for (ConnectorColumn col : columns) {
      if (col.isAutoInc()) {
          throw new DorisConnectorException(
                  "Auto-increment columns are not supported for MaxCompute tables: " + col.getName());
      }
      if (!seen.add(col.getName().toLowerCase())) {
          ...
  ```
  `DorisConnectorException` is already imported (`:25`). No other change.
- **Make `validateColumns` package-private** (drop `private` at `:424`) so the connector unit test
  can invoke it directly. Reason: `validateColumns` is only reachable via `createTable`, which
  first calls `structureHelper.tableExist(odps, ...)` (`:337`) — that needs a live ODPS handle, and
  the maxcompute test module has **no Mockito and no fe-core** (pom has only `junit-jupiter`), so
  the structureHelper cannot be stubbed. Package-private + a brief comment matches the existing
  `MaxComputeBuildTableDescriptorTest` pattern (construct metadata with `null` odps/structureHelper
  and call a method that never dereferences them; `validateColumns` only uses the static
  `MCTypeMapping.toMcType`, so null fields are safe). Add a one-line comment:
  `// package-private for unit test; reached only via createTable() in production.`

## Blast Radius

**SPI ctor is additive (default `isAutoInc=false`) — prove zero-break for all callers.**

All 12 production `new ConnectorColumn(` call sites + 1 test fixture, enumerated and verified by
grep over `fe/`:

| # | call site | arity used | after change |
|---|---|---|---|
| 1 | `EsTypeMapping.java:131` | 5-arg | compiles; `isAutoInc=false` (via 5→6→7 delegation) |
| 2 | `EsTypeMapping.java:185` | **6-arg, isKey=true** | compiles; `isKey=true` preserved, `isAutoInc=false` |
| 3 | `HiveConnectorMetadata.java:253` | 5-arg | unchanged, false |
| 4 | `ThriftHmsClient.java:303` (hms) | 5-arg | unchanged, false |
| 5 | `HudiConnectorMetadata.java:279` | 5-arg | unchanged, false |
| 6 | `IcebergConnectorMetadata.java:157` | 5-arg | unchanged, false |
| 7 | `JdbcConnectorMetadata.java:130` | 5-arg | unchanged, false |
| 8 | `JdbcConnectorMetadata.java:270` | 5-arg | unchanged, false |
| 9 | `MaxComputeConnectorMetadata.java:138` (data cols, read) | 5-arg | unchanged, false |
| 10 | `MaxComputeConnectorMetadata.java:150` (part cols, read) | 5-arg | unchanged, false |
| 11 | `PaimonConnectorMetadata.java:190` | 5-arg | unchanged, false |
| 12 | `TrinoConnectorDorisMetadata.java:186` | 5-arg | unchanged, false |
| 13 | `CreateTableInfoToConnectorRequestConverter.java:90` (fe-core) | 6→**7-arg** | **CHANGED** (this fix) |
| 14 | `ConnectorColumnConverter.java:78` (fe-core) | 6-arg (passes `cc.isKey()`) | compiles; false |
| 15 | `PluginDrivenExternalTable.java:139` (fe-core) | 6-arg | compiles; false |
| 16 | `PhysicalPlanTranslator.java:663` (fe-core) | 6-arg | compiles; false |
| 17 | `PluginDrivenExternalTablePartitionTest.java:171-173,207` (fe-core test) | 5-arg | compiles; false |

- **Only call site #13 changes.** Every other call site keeps its arity; the additive default
  `false` means each produced `ConnectorColumn` is byte-for-byte equivalent in behavior to before
  (auto-inc was always implicitly false). #2 (es, isKey=true via 6-arg) still routes through the
  delegating 6-arg ctor and keeps isKey=true.
- **No SPI method-signature change.** `ConnectorMetadata` and all interface methods are untouched;
  only a new ctor overload is added. No overrider in any connector needs updating.
- **No existing test assertions break.** `PluginDrivenExternalTablePartitionTest:171-207` uses the
  5-arg ctor and asserts on partition pruning, not on auto-inc — unaffected.
  `CreateTableInfoToConnectorRequestConverterTest` asserts name/nullable/comment/partition/bucket,
  none of which change for its fixtures (all use non-auto-inc `ColumnDefinition`s, so
  `isAutoInc==false`) — unaffected.
- **fe-connector-api consumers rebuilt:** since the SPI module (`fe-connector-api`) changes, the
  build must rebuild api + maxcompute + fe-core (operational note). No es/jdbc/hive/hudi/iceberg/
  paimon/trino source edits.
- **Import-gate:** no connector module gains an fe-core import (the new field lives in
  fe-connector-api; the converter that reads `getAutoIncInitValue()` is already in fe-core). The
  maxcompute change uses only already-imported symbols. `bash tools/check-connector-imports.sh`
  stays green.

## Risk Analysis

- **Risk: a legitimate non-auto-inc CREATE TABLE wrongly rejected.** Mitigated: the gate is
  `getAutoIncInitValue() != -1`, the exact same predicate `toSql` (`:225`) uses to emit
  `AUTO_INCREMENT`; default is `-1`. The converter test asserts a normal column yields
  `isAutoInc()==false`.
- **Risk: behavior drift for the other 6 connectors.** Eliminated by additive default `false` —
  proven above; their producers never set auto-inc, and their `validateColumns` (if any) do not
  read it.
- **Risk: package-private `validateColumns` widens API surface.** Minimal: it stays package-private
  (not public), is documented as test-only, and the method is pure FE-side validation. Matches the
  module's existing offline-test idiom.
- **Risk: equals/hashCode change breaks a set/map keyed on ConnectorColumn.** Low: adding a field
  to equals/hashCode is the correct invariant (two columns differing only in auto-inc ARE
  different). No production code keys collections on `ConnectorColumn` identity across the auto-inc
  boundary (all producers default false, so existing keys are unchanged in value).
- **Risk: aggregation half (F31) erroneously added.** Explicitly excluded per issue and report;
  only the auto-inc branch is mirrored.
- **Truth-gate:** UT is sufficient here — the rejection is pure FE validation that throws before
  any ODPS RPC, so no live ODPS e2e is required (unlike the write-path blockers).

## Test Plan

### Unit Tests

#### A. Connector validation — `fe-connector-maxcompute`

- **File:** `fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/MaxComputeValidateColumnsTest.java` (new)
- **Class:** `MaxComputeValidateColumnsTest`
- **Setup:** construct `new MaxComputeConnectorMetadata(null, null, "proj", "ep", "quota", emptyMap)`
  (same offline idiom as `MaxComputeBuildTableDescriptorTest`); call the now package-private
  `validateColumns(List<ConnectorColumn>)` directly.
- **Tests:**
  - `autoIncColumnIsRejected` — list = `[new ConnectorColumn("id", ConnectorType.of("INT"), "",
    false, null, false, true)]` → assert `DorisConnectorException` thrown AND message contains
    `"Auto-increment columns are not supported for MaxCompute tables: id"`.
    **WHY (Rule 9):** MaxCompute physically cannot store auto-increment; legacy rejected it loudly
    (`MaxComputeMetadataOps:422-425`). Silent acceptance is a data-model regression — the user's
    auto-inc intent is dropped without warning. This test fails if the connector ever stops
    rejecting auto-inc.
  - `nonAutoIncColumnPasses` — list = `[new ConnectorColumn("id", ConnectorType.of("INT"), "",
    false, null, false, false)]` → assert `validateColumns` returns without throwing.
    **WHY:** guards against over-rejection — a normal column must still create successfully; the
    gate must key on the flag, not reject all columns.
- **MUTATION:** removing the `if (col.isAutoInc()) throw ...` block in
  `MaxComputeConnectorMetadata.validateColumns` makes `autoIncColumnIsRejected` go red (no
  exception). This is the one-line production revert that re-introduces the regression.

#### B. Converter passthrough — `fe-core`

- **File:** `fe/fe-core/src/test/java/org/apache/doris/connector/ddl/CreateTableInfoToConnectorRequestConverterTest.java` (existing — add tests)
- **Class:** `CreateTableInfoToConnectorRequestConverterTest`
- **Tests:**
  - `autoIncInitValueIsPropagatedAsIsAutoInc` — build a `ColumnDefinition` with
    `autoIncInitValue != -1` using the public 10-arg ctor
    (`new ColumnDefinition("id", IntegerType.INSTANCE, false, null, ColumnNullableType.NOT_NULLABLE,
    1L /*autoIncInitValue*/, Optional.empty(), Optional.empty(), "", Optional.empty())`), run
    `convert(...)`, assert `req.getColumns().get(0).isAutoInc() == true`.
    **WHY (Rule 9):** the connector can only reject what the converter carries. This proves the
    flag survives the `ColumnDefinition → ConnectorColumn` boundary, i.e. the converter does not
    re-drop it. Without passthrough, the connector gate (Test A) is dead code.
  - `plainColumnIsNotAutoInc` — existing-style `ColumnDefinition` (default `autoIncInitValue == -1`)
    → assert `isAutoInc() == false`.
    **WHY:** guards the `!= -1` predicate boundary — a normal column must map to false, not true
    (catches an inverted/constant-true mistake).
- **MUTATION:** reverting the converter to the 6-arg ctor (dropping the 7th arg, i.e. not passing
  `d.getAutoIncInitValue() != -1`) makes `autoIncInitValueIsPropagatedAsIsAutoInc` go red
  (`isAutoInc()` would be false).

#### C. SPI equals/hashCode — `fe-connector-api`

- **File:** `fe/fe-connector/fe-connector-api/src/test/java/org/apache/doris/connector/api/ConnectorColumnTest.java` (new)
- **Class:** `ConnectorColumnTest`
- **Tests:**
  - `equalsAndHashCodeDistinguishAutoInc` — two columns identical except
    `isAutoInc` (`...false, false` vs `...false, true`) → assert `!a.equals(b)` and (best-effort)
    `a.hashCode() != b.hashCode()`.
    **WHY (Rule 9):** auto-inc is now a semantic discriminator; two columns differing only by it are
    genuinely different. If equals/hashCode ignored the field, collections deduping
    `ConnectorColumn`s could collapse an auto-inc column onto a plain one, silently re-dropping the
    flag downstream.
  - `defaultCtorsLeaveAutoIncFalse` — `new ConnectorColumn("c", ConnectorType.of("INT"), "", true,
    null)` (5-arg) and the 6-arg form both report `isAutoInc() == false`.
    **WHY:** locks the additive-default contract — proves the 7 other connectors and read-path
    producers (which use 5/6-arg) keep `isAutoInc=false`, i.e. zero behavior change.
- **MUTATION:** removing `&& isAutoInc == that.isAutoInc` from `equals` makes
  `equalsAndHashCodeDistinguishAutoInc` go red.

### E2E Tests

- No live ODPS e2e required for this fix: the rejection is pure FE-side validation that throws
  before any ODPS RPC. CI is UT-only anyway and skips live ODPS.
- Optional regression-test coverage (CI-skip, for the standing live truth-gate documentation):
  `regression-test/suites/external_table_p2/maxcompute/test_mc_create_table.groovy` (or the
  existing MC DDL suite if present) could add a case asserting
  `CREATE TABLE ... (id INT AUTO_INCREMENT) ...` raises an error containing "Auto-increment columns
  are not supported for MaxCompute tables". Note: skipped in CI; runs only against a real ODPS
  project.
