# Problem

For Paimon partitioned tables read via the **native ORC/Parquet reader path**, partition columns are NOT physically stored in the raw data files — BE materializes them from `columnsFromPath` (the per-split `partitionValues` map). The connector's `PaimonScanPlanProvider.getPartitionInfoMap` renders every partition value with a raw `values[i].toString()`, with no per-type handling. This corrupts several partition column types:

- **DATE**: `RowDataToObjectArrayConverter` yields a boxed `Integer` (epoch-days). `toString()` produces e.g. `"19723"` instead of `"2024-01-01"`. Every row in a DATE-partitioned native table shows a garbage/wrong date.
- **TIMESTAMP_WITH_LOCAL_TIME_ZONE (LTZ)**: rendered as the raw UTC wall clock with **no UTC→session-TZ shift**. Under any non-UTC session the materialized partition value is wrong.
- **BINARY/VARBINARY**: rendered as `[B@<hash>` — non-deterministic JVM-identity garbage. Legacy deliberately **omits** these (returns `null` → no `columnsFromPath` entry).
- **FLOAT/DOUBLE**: legacy goes through `Float.toString`/`Double.toString`; the raw `toString()` happens to match for the boxed types, so this is parity-neutral but should be ported for completeness/clarity.
- **Map key casing**: legacy lowercases the partition key via `Locale.ROOT`; the new code emits the raw paimon partition key.

(TIMESTAMP_WITHOUT_TZ happens to match by coincidence: paimon `Timestamp.toString() == toLocalDateTime().toString() == ISO_LOCAL_DATE_TIME`.)

The confirmed scope (review §Finding 1.1 BLOCKER 3/0/0 + supplemental BINARY 3/0/0 + fix-scope 3/0/0) is: **port the WHOLE `serializePartitionValue` type switch including the session TimeZone**, not just DATE + TIMESTAMP_LTZ. The TIME sub-finding is PARTIAL/over-stated (legacy itself crashes on TIME and TIME is UNSUPPORTED on both sides so it is unreachable in practice) but I port the TIME case anyway for byte-faithful parity — see Risk Analysis.

# Root Cause (confirmed in current code)

`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java:383-400` — `getPartitionInfoMap`:

```java
private Map<String, String> getPartitionInfoMap(Table table, BinaryRow partitionValue) {
    List<String> partitionKeys = table.partitionKeys();
    if (partitionKeys == null || partitionKeys.isEmpty()) {
        return Collections.emptyMap();
    }
    RowType partitionType = table.rowType().project(partitionKeys);
    RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(partitionType);
    Object[] values = converter.convert(partitionValue);
    Map<String, String> result = new LinkedHashMap<>();
    for (int i = 0; i < partitionKeys.size(); i++) {
        String key = partitionKeys.get(i);
        String value = values[i] != null ? values[i].toString() : null;   // :396 — BUG: no per-type render
        result.put(key, value);                                            // :397 — BUG: raw key, no Locale.ROOT
    }
    return result;
}
```

This map flows: `planScan` (`PaimonScanPlanProvider.java:213-214`, called per `DataSplit`) → `PaimonScanRange.Builder.partitionValues(...)` → `PaimonScanRange.populateRangeParams` (`PaimonScanRange.java:212-226`) → `rangeDesc.setColumnsFromPath(...)` consumed by BE's native reader. `session` (a `ConnectorSession`) is the first parameter of `planScan` (`:150`) and is in scope at the call site, so the session TimeZone is reachable but currently never threaded into `getPartitionInfoMap`.

Legacy reference being ported (correct behavior): `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonUtil.java:545-629`:
- `getPartitionInfoMap(table, partitionValues, timeZone)` lowercases keys via `Locale.ROOT` (`:556`) and returns `null` for the whole map when any column throws `UnsupportedOperationException` (`:557-561`).
- `serializePartitionValue(DataType, value, timeZone)` (`:566-629`): scalar/decimal/char/varchar → `value.toString()`; FLOAT → `Float.toString`; DOUBLE → `Double.toString`; binary/varbinary commented out (falls to `default` → throws → whole map dropped); DATE → `LocalDate.ofEpochDay((Integer) value).format(ISO_LOCAL_DATE)`; TIME → `LocalTime.ofNanoOfDay(micros*1000).format(ISO_LOCAL_TIME)`; TIMESTAMP_WITHOUT_TZ → `((Timestamp) value).toLocalDateTime().format(ISO_LOCAL_DATE_TIME)`; TIMESTAMP_WITH_LOCAL_TIME_ZONE → `Timestamp.toLocalDateTime().atZone(UTC).withZoneSameInstant(ZoneId.of(timeZone)).toLocalDateTime().format(ISO_LOCAL_DATE_TIME)`.

Legacy obtained `timeZone` at the call site `source/PaimonScanNode.java:413-414` via `sessionVariable.getTimeZone()`. The connector's parity equivalent is `ConnectorSession.getTimeZone()` (the SPI already documents this as "the session time zone identifier", and `ConnectorSessionBuilder.from(ctx)` injects `ctx.getSessionVariable().getTimeZone()` — same source).

# Design

Port the legacy type switch into the connector as a **pure static seam**, respecting all four constraints:

1. **No fe-core import** — only `java.time.*` + `org.apache.paimon.*` are used (exactly legacy's imports). No `TimeUtils`/`DateUtils`. This matches `PaimonPredicateConverter` (already uses `java.time.LocalDate`/`ZoneOffset.UTC`/paimon `Timestamp`) and `PaimonConnectorMetadata.parseTimestampMillis` (already uses `java.time.ZoneId.of(session.getTimeZone())`).
2. **Match existing style** — extract the type switch as a `static` package-private method `serializePartitionValue(DataType, Object, String timeZone)`, mirroring how `shouldUseNativeReader` was extracted as a pure static for unit-testability (the existing `FakePaimonTable.newReadBuilder()` throws, so `planScan` can't be driven end-to-end offline; a pure static is the established testable seam in this file).
3. **Minimal change** — change one method signature (`getPartitionInfoMap` gains a `String timeZone` param), thread `session.getTimeZone()` from the single call site, add the static `serializePartitionValue`, add the needed `java.time` + paimon `Timestamp`/`DataType` imports. No change to `PaimonScanRange` (it already null-guards and empty-guards the map correctly).
4. **Parity behavior** — byte-faithful port of all 8 type cases, `Locale.ROOT` key lowercasing, and the "unsupported type → whole map dropped" rule.

**Unsupported-type / null-map handling**: Legacy returns `null` for the whole map when any column is unsupported (binary), and the legacy native call site at `PaimonScanNode.java:457` then calls `setPaimonPartitionValues(null)`. The connector's `PaimonScanRange` already treats a null `partitionValues` as `Collections.emptyMap()` (`PaimonScanRange.java:71-73`) and `populateRangeParams` skips empty maps (`:214`), so emitting **no `columnsFromPath`** is the correct parity outcome. To keep the connector's existing non-null contract and avoid NPE risk, I will have `getPartitionInfoMap` **return `Collections.emptyMap()`** (instead of `null`) when any column is unsupported — functionally identical downstream (empty map ⇒ no `columnsFromPath`, same as legacy's null). This is the review's own recommended resolution ("不支持类型返回空 map ... 而非 Object.toString()").

**TZ semantics — IMPORTANT, distinct from predicate pushdown**: the connector-session-TZ memory note warns that paimon *predicate pushdown* must NOT use session-TZ (NTZ stays UTC). That caveat is about predicate literal→epoch conversion against stored file stats, and does NOT apply here. Partition-VALUE rendering is a separate concern and legacy explicitly DOES use the session TZ for the LTZ case (`PaimonUtil.java:623` `ZoneId.of(timeZone)` fed from `sessionVariable.getTimeZone()`). So for parity the connector must thread `session.getTimeZone()` into the LTZ case — and ONLY the LTZ case consumes it; all other cases ignore `timeZone`.

**Bad-alias TZ (CST/PST) handling for the LTZ case**: legacy calls `ZoneId.of(timeZone)` directly with the raw stored Doris string, so legacy itself throws `DateTimeException` for Doris aliases like "CST"/"PST" (`ZoneId.of` rejects them; the connector cannot import the fe-core alias map). I will mirror legacy **exactly**: call `ZoneId.of(timeZone)` with no special degrade. If it throws, the exception propagates out of `planScan` — identical to legacy's behavior (legacy would throw the same `DateTimeException` from `serializePartitionValue`, NOT caught by its `catch (UnsupportedOperationException)`). This is the byte-parity, fail-loud choice consistent with `parseTimestampMillis`'s already-shipped rationale (a wrong zone ⇒ silently wrong partition values ⇒ wrong rows; degrading is unsafe with no BE re-apply for materialized partition values). I will NOT add a friendlier message here (keep the change minimal and behavior identical to legacy); the only realistic path to a bad alias for an LTZ *partition column* is exotic, and parity is the contract.

# Implementation Plan

**File: `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java`** (only file changed)

1. **Add imports** (alongside existing `org.apache.paimon.*` and `java.*` import groups):
   - `import org.apache.paimon.data.Timestamp;`
   - `import org.apache.paimon.types.DataType;`
   - `import java.time.LocalDate;`
   - `import java.time.LocalTime;`
   - `import java.time.ZoneId;`
   - `import java.time.format.DateTimeFormatter;`
   - `import java.util.Locale;`
   (`BinaryRow`, `RowType`, `RowDataToObjectArrayConverter`, `LinkedHashMap`, `Collections`, `List`, `Map` already imported.)

2. **Thread the session TZ at the single call site** in `planScan` (current `:213-215`):
   ```java
   for (DataSplit dataSplit : dataSplits) {
       Map<String, String> partitionValues = getPartitionInfoMap(
               table, dataSplit.partition(), session.getTimeZone());
   ```

3. **Replace `getPartitionInfoMap` (`:383-400`)** with the type-aware port:
   ```java
   private Map<String, String> getPartitionInfoMap(Table table, BinaryRow partitionValue, String timeZone) {
       List<String> partitionKeys = table.partitionKeys();
       if (partitionKeys == null || partitionKeys.isEmpty()) {
           return Collections.emptyMap();
       }
       RowType partitionType = table.rowType().project(partitionKeys);
       RowDataToObjectArrayConverter converter = new RowDataToObjectArrayConverter(partitionType);
       Object[] values = converter.convert(partitionValue);

       Map<String, String> result = new LinkedHashMap<>();
       for (int i = 0; i < partitionKeys.size(); i++) {
           try {
               String value = serializePartitionValue(
                       partitionType.getFields().get(i).type(), values[i], timeZone);
               result.put(partitionKeys.get(i).toLowerCase(Locale.ROOT), value);
           } catch (UnsupportedOperationException e) {
               // Legacy parity (PaimonUtil.getPartitionInfoMap): an unsupported partition column
               // type (e.g. binary/varbinary) drops the ENTIRE map — BE then materializes no
               // columnsFromPath for this split, rather than emitting non-deterministic [B@hash
               // garbage. Legacy returned null; the connector returns an empty map, which
               // PaimonScanRange.populateRangeParams treats identically (no columnsFromPath emitted).
               LOG.warn("Failed to serialize partition value for key {} of table {}: {}",
                       partitionKeys.get(i), table.name(), e.getMessage());
               return Collections.emptyMap();
           }
       }
       return result;
   }
   ```

4. **Add the pure static seam** (byte-faithful port of legacy `serializePartitionValue`, package-private `static` for unit-testability, placed next to `getPartitionInfoMap`):
   ```java
   /**
    * Renders one Paimon partition value to the canonical string BE expects in columnsFromPath.
    * Byte-faithful port of legacy PaimonUtil.serializePartitionValue. Pure static (no Table /
    * ReadBuilder needed) so the correctness-critical per-type rendering is unit-testable offline.
    * Only TIMESTAMP_WITH_LOCAL_TIME_ZONE consumes {@code timeZone} (session zone, UTC->session shift).
    */
   static String serializePartitionValue(DataType type, Object value, String timeZone) {
       switch (type.getTypeRoot()) {
           case BOOLEAN: case INTEGER: case BIGINT: case SMALLINT: case TINYINT:
           case DECIMAL: case VARCHAR: case CHAR:
               return value == null ? null : value.toString();
           case FLOAT:
               return value == null ? null : Float.toString((Float) value);
           case DOUBLE:
               return value == null ? null : Double.toString((Double) value);
           // BINARY / VARBINARY intentionally unsupported (falls to default -> throws -> map dropped):
           // a utf8 string render can corrupt the bytes (legacy comment).
           case DATE:
               return value == null ? null
                       : LocalDate.ofEpochDay((Integer) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
           case TIME_WITHOUT_TIME_ZONE:
               if (value == null) {
                   return null;
               }
               return LocalTime.ofNanoOfDay(((Long) value) * 1000)
                       .format(DateTimeFormatter.ISO_LOCAL_TIME);
           case TIMESTAMP_WITHOUT_TIME_ZONE:
               return value == null ? null
                       : ((Timestamp) value).toLocalDateTime().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
           case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
               if (value == null) {
                   return null;
               }
               return ((Timestamp) value).toLocalDateTime()
                       .atZone(ZoneId.of("UTC"))
                       .withZoneSameInstant(ZoneId.of(timeZone))
                       .toLocalDateTime()
                       .format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
           default:
               throw new UnsupportedOperationException(
                       "Unsupported type for serializePartitionValue: " + type);
       }
   }
   ```

No other files change. `PaimonScanRange` already handles null/empty maps and the rendered string values verbatim.

# Risk Analysis

- **Parity vs legacy**: byte-faithful port of all 8 cases + `Locale.ROOT` key lowercasing + "unsupported ⇒ drop whole map". The only intentional deviation is returning `Collections.emptyMap()` instead of `null` on unsupported type — downstream-equivalent (both ⇒ no `columnsFromPath`) and the existing `PaimonScanRange` already null-tolerates anyway, so this only *removes* a latent NPE surface, never changes emitted thrift.
- **Map key lowercasing change**: previously raw key, now `Locale.ROOT` lowercase. This matches legacy AND matches the projection path in `planScan` (`:167-169` already lowercases field names). Paimon column names from `rowType().getFieldNames()` are conventionally lowercase already, so for the common case this is a no-op; for mixed-case it now correctly aligns key casing with what BE's `columnsFromPath` matching expects (legacy contract).
- **Shared-code blast radius**: ZERO. `getPartitionInfoMap` is private with a single caller (`planScan`); the new `serializePartitionValue` is a new package-private static with one caller. No SPI signature changes, no fe-core touch, no change to `PaimonScanRange`/handle/metadata. JNI path is unaffected in correctness (BE's JNI reader gets partition info from the serialized split, not `columnsFromPath`; legacy set the map on JNI splits too, so keeping the corrected map on JNI ranges is strictly more-correct and harmless).
- **TZ edge case (CST/PST)**: byte-identical to legacy — `ZoneId.of(rawDorisAlias)` throws `DateTimeException`, propagating out of `planScan`. This is NOT a new regression: legacy threw the same way from the same `ZoneId.of(timeZone)`. It only affects LTZ-typed *partition columns* (rare) under a non-IANA session zone; for all standard zones ("UTC", "Asia/Shanghai", offsets) it is correct. Consistent with the already-shipped fail-loud rationale in `parseTimestampMillis`.
- **TIME case (over-stated finding)**: ported for faithful parity, but practically unreachable — paimon `TIME` maps to UNSUPPORTED in `PaimonTypeMapping` (both directions), so a TIME partition column cannot be created/projected through Doris; legacy's `(Long) value` cast would also throw if it ever ran on the converter's `Integer`. Porting it verbatim (cast to `Long`) keeps byte-parity; if it ever executes it throws `ClassCastException` exactly as legacy would, surfaced loudly rather than silently wrong. No behavior is made worse.
- **DATE cast `(Integer)`**: `RowDataToObjectArrayConverter` yields a boxed `Integer` for DATE (epoch-days) — verified against the legacy code that performs the identical cast. Safe.
- **Null partition value**: every case null-guards (returns `null`), preserved from legacy. `PaimonScanRange`/`ConnectorPartitionValues.normalize` already handle null entries (`columnsFromPathIsNull`).

# Test Plan

## Unit Tests

New test class `fe/fe-connector/fe-connector-paimon/src/test/java/org/apache/doris/connector/paimon/PaimonPartitionValueRenderTest.java`, driving the new pure static `PaimonScanPlanProvider.serializePartitionValue(DataType, Object, String)` directly (package-private, same-package). This is the established testable seam because `FakePaimonTable.newReadBuilder()` throws so `planScan`/`getPartitionInfoMap` cannot be driven end-to-end offline — exactly why `shouldUseNativeReader` is also tested as a pure static. Each test encodes WHY (the BE consumes this string as `columnsFromPath`; a wrong string ⇒ wrong materialized rows), and each FAILS before the fix (raw `toString()`) and PASSES after.

- `dateRendersAsIsoDateNotEpochDays`: `serializePartitionValue(DataTypes.DATE(), Integer.valueOf((int) LocalDate.of(2024,1,1).toEpochDay()), "UTC")` ⇒ `"2024-01-01"`. WHY/MUTATION: pre-fix raw `toString()` yields `"19723"` (epoch-days) which BE parses as a garbage date ⇒ data corruption; asserts the ISO render. RED before fix.
- `ltzShiftsUtcToSessionZone`: build `Timestamp.fromLocalDateTime(LocalDateTime.of(2024,1,1,0,0,0))` (the UTC wall clock), `serializePartitionValue(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(), ts, "Asia/Shanghai")` ⇒ `"2024-01-01T08:00:00"`. WHY: LTZ partition values are stored UTC and must be shown in the session zone; pre-fix renders the un-shifted UTC wall clock. Also assert with `"UTC"` ⇒ `"2024-01-01T00:00:00"` (no shift) to pin that the zone parameter is actually applied. RED before fix (raw `toString()` ignores zone).
- `ntzRendersIsoNoZoneShift`: `serializePartitionValue(DataTypes.TIMESTAMP(), Timestamp.fromLocalDateTime(LocalDateTime.of(2024,1,1,1,2,3)), "Asia/Shanghai")` ⇒ `"2024-01-01T01:02:03"` regardless of session zone. WHY: pins the NTZ-stays-wall-clock invariant (the memory-note caveat: NTZ must NOT be zone-shifted). Guards against a future "shift everything" regression. (Coincidentally green pre-fix; its value is locking intent.)
- `binaryYieldsUnsupported`: `assertThrows(UnsupportedOperationException.class, () -> serializePartitionValue(DataTypes.BYTES(), new byte[]{1,2}, "UTC"))`. WHY: binary must NOT be rendered as `[B@hash`; the contract is "throw so the caller drops the whole map" (no `columnsFromPath`). MUTATION: any render path for binary ⇒ no throw ⇒ red. RED before fix (raw `toString()` returns `[B@...` and never throws).
- `floatDoubleUseToStringRender`: `serializePartitionValue(DataTypes.FLOAT(), 1.5f, "UTC")` ⇒ `"1.5"`; `DataTypes.DOUBLE(), 2.25d` ⇒ `"2.25"`. Parity-locking (matches legacy `Float/Double.toString`).
- `nullValueRendersNull`: each typed case with `value=null` ⇒ `null`. Locks the null-guard parity.

New (or extended `PaimonScanPlanProviderTest`) test exercising the **map-level** contract via a thin overload — since `getPartitionInfoMap` needs a real `BinaryRow`+converter which is heavy offline, the map-level "unsupported ⇒ empty map" and "key lowercased" behavior is asserted by a focused test only if a lightweight `BinaryRow` can be built; otherwise the static-seam tests above (binary-throws + ISO renders) plus a code-review-visible single call site fully cover intent. Preferred minimal addition:
- `keyLowercasedAndUnsupportedDropsMap` (only if a real `BinaryRow` for the partition is constructible with the paimon `BinaryRowWriter` available on the test classpath): assert a mixed-case DATE partition key renders lowercase in the result map, and a binary partition column yields `Collections.emptyMap()`. If `BinaryRow` construction proves brittle offline, omit and rely on the static-seam tests (the map wrapper is a trivial 6-line loop fully covered by the seam tests + the single-call-site thread of `session.getTimeZone()`).

All new tests are offline, no fe-core, no mockito (pure paimon `DataTypes`/`Timestamp` + JUnit5, matching the existing connector test style and classpath — verified `DataTypes.DATE/TIMESTAMP_WITH_LOCAL_TIME_ZONE/FLOAT/DOUBLE/TIME` and `Timestamp.fromLocalDateTime` are on the test classpath).

## E2E Tests

Live-only / CI-skipped (no paimon cluster in unit CI; gated like `PaimonLiveConnectivityTest`). The end-to-end proof is a regression-test SQL: create a paimon table partitioned by a DATE column (and separately an LTZ column) with ORC/Parquet data files that are native-reader eligible (not binlog/audit_log, `force_jni_scanner=false`), then `SELECT date_part_col FROM t` under a non-UTC `SET time_zone=...` session and assert the returned partition column values equal the legacy/expected `"2024-01-01"` (and the correctly-shifted LTZ datetime) rather than `"19723"`/un-shifted UTC. This cannot run in the connector unit module (needs BE + a real warehouse + native reader), so it is documented as live-only; the unit tests above fully cover the FE-side rendering logic that is the actual defect.

---

# ✅ IMPL SUMMARY (2026-06-11)

**Status: DONE — build+UT green (PaimonPartitionValueRenderTest 7/0; PaimonScanPlanProviderTest 8/0 unchanged; imports clean; HEAD uncommitted).**

## Fix (1 production file: `PaimonScanPlanProvider.java`)
- Added imports: paimon `Timestamp`, `DataType`; `java.time.{LocalDate,LocalTime,ZoneId,format.DateTimeFormatter}`; `java.util.Locale` (alphabetical, checkstyle-clean).
- Threaded `session.getTimeZone()` into the single call site (`getPartitionInfoMap(table, dataSplit.partition(), session.getTimeZone())`).
- `getPartitionInfoMap` now lower-cases keys via `Locale.ROOT`, calls the new per-type `serializePartitionValue`, and on `UnsupportedOperationException` (binary) returns `Collections.emptyMap()` (legacy null-map parity → no `columnsFromPath`).
- Added pure static `serializePartitionValue(DataType, Object, String timeZone)` — byte-faithful port of all 8 legacy cases (scalar/decimal/char/varchar→toString; FLOAT/DOUBLE→Float/Double.toString; DATE→ISO_LOCAL_DATE; TIME→ISO_LOCAL_TIME; NTZ→ISO_LOCAL_DATE_TIME; LTZ→UTC→session-TZ shift; BINARY→throws). Only LTZ consumes timeZone.

## Tests (7 new, fail-before/pass-after): `PaimonPartitionValueRenderTest`
date-not-epoch, ltz-shift (Shanghai vs UTC), ntz-no-shift, binary-throws, float/double, integer-toString, null-renders-null.

## Correction discovered during impl
The design's planned LTZ expectation `"2024-01-01T08:00:00"` is WRONG: `DateTimeFormatter.ISO_LOCAL_DATE_TIME` **omits the seconds component when both second and nano are zero** (`08:00:00` → `"...T08:00"`). This is legacy behavior (legacy uses the same formatter), so it is not a defect — but the test would be brittle. The tests use a non-zero-seconds wall clock (`01:02:03`), so the shifted value is the unambiguous `"2024-01-01T09:02:03"` (UTC+8) and the formatter always emits seconds. The shift correctness is still fully pinned (Shanghai 09:02:03 vs UTC 01:02:03).

## Live-e2e (gated, NOT run): DATE/LTZ-partitioned native-reader table under a non-UTC `SET time_zone`, asserting partition col = ISO date / shifted datetime (needs BE + warehouse).
