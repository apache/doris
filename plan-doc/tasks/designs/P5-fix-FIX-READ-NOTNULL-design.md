> **✅ USER DECISION (2026-06-11): restore legacy parity** — implement the recommended `boolean nullable = true;` in `PaimonConnectorMetadata.mapFields`. Do NOT propagate paimon NOT NULL; do NOT touch the shared `ConnectorColumnConverter`.

# Problem

On the paimon READ path, the new SPI connector propagates a paimon field's `NOT NULL` constraint into the resulting Doris `Column` (`isAllowNull=false`). The legacy `datasource/paimon` code path instead hard-coded every paimon-derived Doris column to `isAllowNull=true` (nullable), regardless of the paimon field's own nullability.

This is a result-changing parity regression. The most common trigger is paimon **primary-key tables**: paimon forces every PK column to `NOT NULL`, so under the new path nearly every paimon PK table now exposes `NOT NULL` Doris columns where legacy exposed nullable ones. Nereids uses column nullability to drive null-rejecting simplifications (e.g. `IS NULL` folding, `Coalesce`/anti-join rewrites). When a `NOT NULL` external column can still produce a NULL at read time (schema-evolution default-fill, etc.), those simplifications can drop rows or misevaluate predicates — outcomes legacy never permitted because it always declared the column nullable.

# Root Cause (confirmed in current code)

The read-path column nullability is decided in exactly one place and is propagated verbatim through the bridge:

- `fe/fe-connector/fe-connector-paimon/.../PaimonConnectorMetadata.java:945` — inside `mapFields(List<DataField>, List<String>)` (lines 939-954):
  ```java
  boolean nullable = field.type().isNullable();      // line 945
  columns.add(new ConnectorColumn(field.name().toLowerCase(), connectorType, comment, nullable, null));
  ```
  `mapFields` is the single mapping shared by both read entrypoints via `buildTableSchema` (line 207):
  - latest path `getTableSchema(session, handle)` at lines 148-163 (`fields = table.rowType().getFields()`),
  - at-snapshot path `getTableSchema(session, handle, snapshot)` at lines 181-197 (`fields = schema.fields()`).

- The fe-core bridge does **not** re-force nullable: `fe/fe-core/.../ConnectorColumnConverter.java:65-70`:
  ```java
  return new Column(cc.getName(), dorisType, cc.isKey(), null,
          cc.isNullable(), cc.getDefaultValue(), ...);   // isAllowNull = cc.isNullable()
  ```
  So a paimon `NOT NULL` field → `ConnectorColumn(nullable=false)` → Doris `Column(isAllowNull=false)`. `SlotReference.fromColumn` then sets the nereids slot nullability from `column.isAllowNull()`, reaching the optimizer.

Legacy hard-codes nullable=`true`:
- `fe/fe-core/.../paimon/PaimonExternalTable.java:349-354` builds each column with the 8-arg `Column` ctor (`Column.java:256-257` = `(name, type, isKey, aggregateType, isAllowNull, comment, visible, colUniqueId)`), passing the **literal `true`** for `isAllowNull` (not `field.type().isNullable()`).
- `fe/fe-core/.../paimon/PaimonSysExternalTable.java:257-268` does the same (literal `true`) for system tables.

Trigger universality confirmed: paimon's `Schema` normalization forces every primary-key field to `NOT NULL` (`copy(false)`), so PK tables — the core paimon case — flip nullability metadata under the new path.

# Design

**Restore legacy parity by forcing read-path Doris columns nullable in `mapFields`.**

- The fix is a one-line behavioral change confined to the paimon connector module (`mapFields` in `PaimonConnectorMetadata.java`). This respects the connector no-fe-core-import rule: `mapFields` already lives entirely in the connector, builds `ConnectorColumn` (an fe-connector-api type), and touches no fe-core classes.
- **Do NOT** push the fix into `ConnectorColumnConverter.convertColumn` (fe-core, shared by every connector). MaxCompute and future connectors may legitimately want real nullability; the legacy paimon "always nullable" behavior is paimon-specific and belongs in the paimon connector.
- Complex-type child nullability (ARRAY item / MAP value / STRUCT field) is already reconstructed with Doris-default container nullability in `ConnectorColumnConverter.convertType` (review §10 overview), so the only divergence from legacy is the top-level `Column.isAllowNull` flag. Fixing `mapFields` fully closes the gap.

**Parity-vs-improvement flag (needs user confirmation):**
This change is a **pure parity restore**. The alternative — keeping precise `NOT NULL` propagation — would be a behavior *improvement* only if the planner is guaranteed never to derive wrong results from a `NOT NULL` external column. That guarantee does not hold today (schema-evolution default-fill can surface NULLs into a paimon `NOT NULL` column at read time, and nereids is then permitted to fold null-rejecting predicates). Recommendation: take the parity restore now. If "precise nullability" is later desired, it must be a separate, explicitly gated decision with planner-correctness verification — not folded into this fix.

# Implementation Plan

File: `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java`, method `mapFields` (lines 939-954).

Change line 945 from:
```java
boolean nullable = field.type().isNullable();
```
to (force nullable for legacy parity, with an explaining comment):
```java
// Legacy parity: PaimonExternalTable / PaimonSysExternalTable always built each Doris column
// with isAllowNull=true regardless of the paimon field's NOT NULL flag. Paimon PK columns are
// always NOT NULL, so propagating that would flip nullability metadata for almost every PK table
// and let nereids fold null-rejecting predicates the legacy path never permitted (rows can still
// read as NULL under schema-evolution default-fill). Keep columns nullable; do not propagate the
// paimon NOT NULL constraint on the read path.
boolean nullable = true;
```

Notes:
- `field` (`DataField`) is still referenced for name/type/comment, so no unused-variable issue.
- No signature change, no other call sites. `buildTableSchema` and both `getTableSchema` overloads inherit the fix automatically.
- No fe-core, fe-connector-api, or shared-converter edits.

# Risk Analysis

- **Parity vs legacy**: After the change, paimon read-path columns are nullable=true in both the latest and at-snapshot paths, exactly matching `PaimonExternalTable.java:349-354` and `PaimonSysExternalTable.java:257-268`. Net effect is to *remove* a divergence introduced by the SPI port.
- **Shared-code blast radius**: Zero. The edit is inside the paimon connector's private `mapFields`. `ConnectorColumnConverter` (shared with MaxCompute and future connectors) is untouched, so other connectors' nullability semantics are unaffected.
- **Edge cases**:
  - System tables ($-suffixed) also flow through `mapFields`/`buildTableSchema`, so they too get restored to legacy `true` — matching `PaimonSysExternalTable`.
  - Complex types: only the top-level column flag changes; ARRAY/MAP/STRUCT inner nullability is already Doris-default and unaffected.
  - DDL / write path (`PaimonTypeMapping.toPaimonType`, `PaimonSchemaBuilder`) is a separate direction (Doris→paimon) and does not call `mapFields`; it is not touched and its `.copy(nullable)` behavior is preserved.
  - `column.uniqueId` (Finding 10.2, MINOR) is a separate, unreachable-today gap and is intentionally out of scope here.
- **Downside of the restore**: a genuinely-NOT-NULL paimon column will now report nullable in Doris metadata (e.g. `DESC`/`SHOW CREATE TABLE` shows `NULL`). This is the long-standing legacy behavior, accepted as the cost of correctness; explicitly flagged above for user confirmation.

# Test Plan

## Unit Tests

Location: `fe/fe-connector/fe-connector-paimon/src/test/java/org/apache/doris/connector/paimon/PaimonConnectorMetadataTest.java` (offline harness: `RecordingPaimonCatalogOps` + `FakePaimonTable`, metadata built with a null real catalog).

Build a `RowType` mixing a NOT NULL field and a nullable field. Paimon API confirmed (paimon-api `DataType.notNull()` / `nullable()`; `RowType.Builder.field(String, DataType)` preserves the type's own nullability; `DataTypes.INT()` is nullable by default):
```java
RowType rt = RowType.builder()
        .field("id", DataTypes.INT().notNull())   // paimon NOT NULL (PK-like)
        .field("val", DataTypes.INT())             // paimon nullable
        .build();
```

Test 1 — `getTableSchemaForcesColumnsNullableForLegacyParity` (latest path):
- Arrange a `FakePaimonTable` with the above rowType (PK = `["id"]`), set on `RecordingPaimonCatalogOps`; obtain a `PaimonTableHandle` via `getTableHandle`.
- Act: `ConnectorTableSchema schema = metadata.getTableSchema(null, handle);`
- Assert: for the `id` column, `schema.getColumns().get(0).isNullable() == true` (and `val` too).
- WHY comment: encodes intent — "legacy always declared paimon columns nullable so nereids cannot fold null-rejecting predicates on a NOT NULL external column; a paimon PK NOT NULL field MUST still surface as nullable to Doris." MUTATION that makes it red: reverting `mapFields` to `field.type().isNullable()` (the `id` column becomes `isNullable()==false`).
- This FAILS before the fix (today `id` is non-nullable) and PASSES after.

Test 2 — `getTableSchemaAtSnapshotAlsoForcesNullable` (at-snapshot path):
- Drive `getTableSchema(session, handle, snapshot)` with a snapshot whose `getSchemaId() >= 0`, using `RecordingPaimonCatalogOps.schemaAt` to return a `PaimonSchemaSnapshot` whose `fields()` include the NOT NULL `id`.
- Assert the same nullable=true outcome.
- WHY comment: the two read entrypoints share `mapFields`; this pins that the snapshot/time-travel read path also obeys legacy nullable parity and cannot drift from the latest path.

(If the at-snapshot fake plumbing is heavier than the existing harness supports, Test 2 may be folded into Test 1's assertions by exercising whichever `getTableSchema` overload the harness already drives elsewhere in this file; the load-bearing assertion is `isNullable()==true` on the NOT NULL field. Both overloads route through line 945, so one well-placed assertion is sufficient to fail-before/pass-after; the second test is added for explicit drift protection.)

## E2E Tests

No new live test required for the fix itself; the connector UT above fully covers the metadata-level behavior offline. The downstream *correctness* consequence (planner folding null-rejecting predicates on a NOT NULL external column that reads NULL via schema evolution) is a live-only scenario: it needs a real paimon table, a schema-evolution-added NOT NULL column, and the BE read path, which the offline harness cannot reproduce. Any such regression check belongs in the existing paimon regression-test suite (live-only / CI-gated behind real paimon catalog credentials) and is out of scope for this unit-level parity restore. Flag for the user: if they want an end-to-end guard, it should assert that a query with an `IS NULL` / `COALESCE` predicate over a paimon PK column returns the same rows as legacy.

---

# ✅ IMPL SUMMARY (2026-06-11)

**Status: DONE — build+UT green (PaimonConnectorMetadataTest 12/0, incl. 2 new; imports clean; HEAD uncommitted).**

## Fix (1 production file: `PaimonConnectorMetadata.java`, method `mapFields`)
Changed the single line `boolean nullable = field.type().isNullable();` → `boolean nullable = true;` (with an explaining comment). Pure connector, no fe-core / shared-converter edit. Both read entrypoints (`getTableSchema` latest + at-snapshot) inherit the fix via the shared `buildTableSchema`/`mapFields`.

## Tests (2 new in `PaimonConnectorMetadataTest`)
- `getTableSchemaForcesColumnsNullableForLegacyParity`: a paimon `INT().notNull()` (PK) field surfaces as `isNullable()==true`.
- `getTableSchemaAtSnapshotAlsoForcesNullable`: the at-snapshot path (`schemaAt` seam + `ConnectorMvccSnapshot.builder().schemaId(5)`) also forces nullable (drift protection).

## Note
- Write path (`PaimonTypeMapping.toPaimonType` / `PaimonSchemaBuilder`, Doris→paimon) is the opposite direction and does NOT call `mapFields` — untouched (its `PaimonSchemaBuilderTest`/`PaimonTypeMappingToPaimonTest` nullable assertions are about that direction and are unaffected).

## Live-e2e (gated, NOT run): IS NULL / COALESCE over a paimon PK column vs legacy rows.
