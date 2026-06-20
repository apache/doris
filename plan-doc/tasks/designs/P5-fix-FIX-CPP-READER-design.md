# Problem

When `enable_paimon_cpp_reader=true`, BE routes Paimon JNI-format splits to the C++ reader (`PaimonCppReader`) instead of the Java JNI reader. The C++ reader deserializes the split with Paimon's **native binary** format (`paimon::Split::Deserialize`). The new connector (`PaimonScanPlanProvider`) ignores the `enable_paimon_cpp_reader` session flag entirely and **always** serializes the split with Java object serialization (`InstantiationUtil.serializeObject`). So when a user (or the regression harness, which randomizes the flag) turns the flag on, BE's `PaimonCppReader::_decode_split` runs a native deserialize over a Java-serialized blob and fails hard with `Status::InternalError("paimon-cpp deserialize split failed: ...")`. The query dies; no rows are read.

The legacy `PaimonScanNode` honored the flag by switching the split serialization format to Paimon-native (`DataSplit.serialize`) when the flag was on and the split was a `DataSplit`. The connector dropped that branch.

# Root Cause (confirmed in current code)

**Connector always Java-serializes, never reads the flag.**
`PaimonScanPlanProvider.buildJniScanRange` (`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java:320-339`) unconditionally does `String serializedSplit = encodeObjectToString(split);` (`:330`). `encodeObjectToString` (`:465-473`) is `InstantiationUtil.serializeObject(obj)` + STANDARD base64 — Java object serialization only. The flag `enable_paimon_cpp_reader` is read nowhere in the connector (verified: `grep` for `enable_paimon`/`getSessionProperties` in the connector main source returns nothing). The `planScan` method receives a `ConnectorSession session` (`:149-153`) but never threads it into `buildJniScanRange`.

**Legacy honored the flag — the branch the connector dropped.**
`fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonScanNode.java:260-268`:
```
if (split != null) {
    rangeDesc.setFormatType(TFileFormatType.FORMAT_JNI);
    if (sessionVariable.isEnablePaimonCppReader() && split instanceof DataSplit) {
        fileDesc.setPaimonSplit(PaimonUtil.encodeDataSplitToString((DataSplit) split));
    } else {
        fileDesc.setPaimonSplit(PaimonUtil.encodeObjectToString(split));
    }
    ...
}
```
The two encoders differ by wire format:
- `PaimonUtil.encodeObjectToString` (`PaimonUtil.java:519-526`): `InstantiationUtil.serializeObject` + base64 → **Java serialization** (for the Java JNI reader).
- `PaimonUtil.encodeDataSplitToString` (`PaimonUtil.java:533-543`): `split.serialize(new DataOutputViewStreamWrapper(baos))` + base64 → **Paimon native binary** (javadoc: "compatible with paimon-cpp reader"). Applies only to `DataSplit`.

**BE confirms the format split is correctness-critical, not cosmetic.**
`be/src/exec/scan/file_scanner.cpp:1079-1101`: for `FORMAT_JNI` + `table_format_type == "paimon"`, BE selects `PaimonCppReader` iff `_state->query_options().enable_paimon_cpp_reader`, else `PaimonJniReader`. `be/src/format/table/paimon_cpp_reader.cpp:311-330` (`_decode_split`): base64-decodes `paimon_split` then `paimon::Split::Deserialize(...)` (native), returning `InternalError` on failure. The Java JNI reader instead expects the Java-serialized blob. So the FE format MUST match the flag.

**The flag is reachable and exercised, not theoretical.**
`enable_paimon_cpp_reader` is a visible, non-removed session var (`SessionVariable.java:2884-2887`, name constant `:774`), so `VariableMgr.toMap` emits it by name (`VariableMgr.java:930-948` skips only REMOVED/INVISIBLE). `ConnectorSessionBuilder.extractSessionProperties` (`ConnectorSessionBuilder.java:116-127`) forwards the whole `toMap` result into `ConnectorSession.getSessionProperties()`. The regression harness randomizes it (`SessionVariable.java:3875` `this.enablePaimonCppReader = random.nextBoolean();`). Default is `false`, so default reads are unaffected; flag-on reproduces deterministically.

# Design

Mirror the legacy branch inside the connector, as a small, pure, unit-testable seam — exactly the shape already used for the native-vs-JNI routing decision (`shouldUseNativeReader`, a static at `:366`).

1. Read the flag once in `planScan` from the session: `boolean cppReader = isCppReaderEnabled(session);` where `isCppReaderEnabled` reads `session.getSessionProperties().get("enable_paimon_cpp_reader")` and parses it as boolean (default false). This is the only place the flag is consumed; no fe-core import — `ConnectorSession` and its `getSessionProperties()` are the established SPI channel (same channel MaxCompute uses for its tunables).

2. Thread the flag into `buildJniScanRange(...)` as a new `boolean useCppFormat` parameter.

3. Inside `buildJniScanRange`, select the encoder via a new pure static `encodeSplit(Split split, boolean useCppFormat)`:
   - If `useCppFormat && split instanceof DataSplit` → Paimon-native: `((DataSplit) split).serialize(new DataOutputViewStreamWrapper(baos))` + STANDARD base64 (port of `encodeDataSplitToString`).
   - Else → existing Java path: `encodeObjectToString(split)`.

   The `instanceof DataSplit` guard is **load-bearing parity**: non-`DataSplit` system splits (the `nonDataSplits` loop, `:206-210`) and the empty-RawFiles JNI fallback for a `DataSplit` (`:246-251`) MUST stay Java-serialized even when the flag is on, because the native binary format only exists for `DataSplit`. Both call sites pass the flag, but the static's guard keeps non-DataSplit on Java automatically — matching legacy's single `split instanceof DataSplit` gate.

Constraints honored:
- **No fe-core import**: `DataOutputViewStreamWrapper` (paimon-common, already on the connector classpath — same package as the already-imported `org.apache.paimon.io.DataFileMeta`) and `DataSplit.serialize` are pure Paimon SDK. The flag comes through the connector SPI session, not via `SessionVariable`.
- **Minimal/surgical**: no new class; one new static encoder + one new boolean param + one flag-read helper, all in `PaimonScanPlanProvider`. `PaimonScanRange` and `PaimonTableHandle` are untouched (the wire property `paimon.split` and the BE-side `populateRangeParams` are format-agnostic — they carry an opaque base64 string either way; BE picks the decoder off `enable_paimon_cpp_reader`, which is already plumbed independently to BE via `SessionVariable.toThrift`/`query_options`).
- **Style match**: the pure-static + per-call-site-flag pattern is identical to `shouldUseNativeReader`/`supportNativeReader`.

Note: BE already learns the flag through its own `query_options.enable_paimon_cpp_reader` (set by `SessionVariable.toThrift`, `SessionVariable.java:5526`) — that path is independent of the connector and unchanged. The bug is purely that FE's chosen serialization format must AGREE with the flag BE will read. This fix makes them agree.

# Implementation Plan

**File: `.../connector/paimon/PaimonScanPlanProvider.java`**

Add imports:
```java
import org.apache.paimon.io.DataOutputViewStreamWrapper;
import java.io.ByteArrayOutputStream;
```

Add a session-flag constant + reader near the other constants (`:91-99`):
```java
// Session variable name (byte-identical to SessionVariable.ENABLE_PAIMON_CPP_READER) surfaced
// through ConnectorSession.getSessionProperties() (VariableMgr.toMap). When true, BE routes the
// JNI-format paimon split to PaimonCppReader, which deserializes the NATIVE paimon binary format
// (paimon::Split::Deserialize), so FE must serialize a DataSplit with that format, not Java serde.
private static final String ENABLE_PAIMON_CPP_READER = "enable_paimon_cpp_reader";

static boolean isCppReaderEnabled(ConnectorSession session) {
    if (session == null) {
        return false;
    }
    String v = session.getSessionProperties().get(ENABLE_PAIMON_CPP_READER);
    return Boolean.parseBoolean(v);   // null/"false" -> false (legacy default)
}
```

In `planScan` (`:148-255`): compute the flag once after resolving the handle, and pass it to every `buildJniScanRange` call:
```java
boolean cppReader = isCppReaderEnabled(session);
...
// non-DataSplit loop (:207-210)
ranges.add(buildJniScanRange(split, tableLocation, defaultFileFormat,
        Collections.emptyMap(), false, cppReader));
...
// DataSplit JNI fallback (:248-250)
ranges.add(buildJniScanRange(dataSplit, tableLocation, defaultFileFormat,
        partitionValues, true, cppReader));
```

Change `buildJniScanRange` signature + encoder selection (`:320-339`):
```java
private PaimonScanRange buildJniScanRange(Split split, String tableLocation,
        String defaultFileFormat, Map<String, String> partitionValues,
        boolean isDataSplit, boolean cppReader) {
    long splitWeight = isDataSplit ? computeSplitWeight((DataSplit) split) : split.rowCount();
    String serializedSplit = encodeSplit(split, cppReader);
    return new PaimonScanRange.Builder()
            .fileFormat("jni")
            .paimonSplit(serializedSplit)
            .tableLocation(tableLocation)
            .partitionValues(partitionValues)
            .selfSplitWeight(splitWeight)
            .build();
}
```

Add the pure encoder static (next to `encodeObjectToString`, `:465-473`):
```java
/**
 * Selects the split serialization that matches the BE reader the engine will use.
 * When the paimon-cpp reader is enabled AND the split is a {@link DataSplit}, serialize with
 * Paimon's NATIVE binary format ({@code DataSplit.serialize}) so BE's PaimonCppReader
 * ({@code paimon::Split::Deserialize}) can decode it. Otherwise (flag off, or a non-DataSplit
 * system split that has no native format) fall back to Java object serialization for the Java
 * JNI reader. Mirrors legacy PaimonScanNode.setPaimonParams + PaimonUtil.encodeDataSplitToString.
 */
static String encodeSplit(Split split, boolean cppReader) {
    if (cppReader && split instanceof DataSplit) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ((DataSplit) split).serialize(new DataOutputViewStreamWrapper(baos));
            return new String(BASE64_ENCODER.encode(baos.toByteArray()), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize Paimon DataSplit (native format): "
                    + e.getMessage(), e);
        }
    }
    return encodeObjectToString(split);
}
```
(Uses the existing `BASE64_ENCODER` STANDARD encoder, matching legacy `Base64.getEncoder()` and the BE `base64_decode`.)

**File: `.../paimon/PaimonScanPlanProviderTest.java`** — add UTs (below).

# Risk Analysis

- **Parity vs legacy**: byte-exact. Legacy native encoder = `DataSplit.serialize(DataOutputViewStreamWrapper)` + `Base64.getEncoder()`; ported verbatim. Legacy gate = `enablePaimonCppReader && split instanceof DataSplit`; ported verbatim (flag via SPI session + the `instanceof DataSplit` guard inside `encodeSplit`). The Java-serialization default path is unchanged, so flag-off behavior is bit-for-bit identical to today.
- **Non-DataSplit / fallback splits**: must NOT switch to native even with the flag on (no native binary exists for them). The `instanceof DataSplit` guard preserves this; verified against legacy which only specializes `DataSplit`. A regression here (e.g. forcing native for all) would re-break system tables and the no-raw-file JNI fallback under the cpp reader — covered by a UT below.
- **Shared-code blast radius**: zero. Change is confined to `PaimonScanPlanProvider`. `PaimonScanRange`/`PaimonTableHandle`/thrift/BE are untouched; the wire property `paimon.split` stays an opaque base64 string and BE's reader selection already keys off its own `query_options.enable_paimon_cpp_reader` (independent path, unchanged).
- **Classpath**: `DataOutputViewStreamWrapper` is in paimon-common (transitive via paimon-core; same package as already-imported `org.apache.paimon.io.DataFileMeta`). No new dependency. Verified the class is present in `paimon-common-1.3.1.jar` and `DataSplit`/`Split` in `paimon-core`.
- **Edge cases**: (a) flag value casing/whitespace — `Boolean.parseBoolean` is null-safe and case-insensitive, defaults false; matches the boolean session var emission `"true"`/`"false"`. (b) `session == null` (defensive, e.g. some test paths) → treated as flag-off. (c) COUNT-pushdown / native-reader ranges never call `buildJniScanRange`, so they are inherently unaffected (they don't carry a `paimon.split`).
- **Out of scope (intentionally not bundled)**: the separate partition-render BLOCKER and statistics MAJOR are tracked under their own fixes; this change touches only split serialization selection.

# Test Plan

## Unit Tests (in `PaimonScanPlanProviderTest`, offline, run in CI)

These fail before the fix (the seam `encodeSplit` and flag-read don't exist / are never applied) and pass after, and they encode WHY (the format MUST match the flag BE will read), not just WHAT.

1. **`cppReaderFlagSelectsNativeBinaryForDataSplit`** — Build a REAL `DataSplit` offline using the proven `PaimonTableSerdeRoundTripTest` recipe (local `FileSystemCatalog` over `LocalFileIO` under `@TempDir`, real partitioned/keyed table, write a couple rows via the table's `BatchWriteBuilder`, then `table.newReadBuilder().newScan().plan().splits()` to obtain a genuine `DataSplit`). Assert:
   - `encodeSplit(dataSplit, /*cppReader*/ true)` base64-decodes (STANDARD) to bytes that `DataSplit.deserialize(DataInputViewStreamWrapper)` round-trips back to an equal `DataSplit` (the native format BE's `paimon::Split::Deserialize` consumes), AND
   - it does NOT equal `encodeObjectToString(dataSplit)` (proves the format actually changed).
   - WHY: pins that flag-on yields the native wire format BE cpp reader can decode. MUTATION: dropping the `cppReader` branch → both encodings equal / native deserialize fails → red.

2. **`cppReaderFlagOffKeepsJavaSerialization`** — Same real `DataSplit`. Assert `encodeSplit(dataSplit, false)` equals `encodeObjectToString(dataSplit)` (Java serde, byte-for-byte). WHY: default reads must be untouched. MUTATION: always-native → red.

3. **`nonDataSplitStaysJavaSerializedEvenWithCppFlag`** — A `Split` that is NOT a `DataSplit` (a tiny test `Split` stub, or a non-DataSplit obtained from a system table). Assert `encodeSplit(stub, /*cppReader*/ true)` equals `encodeObjectToString(stub)` — native format is never applied to non-DataSplit. WHY: the `instanceof DataSplit` parity gate (system splits / no-raw-file fallback have no native binary form). MUTATION: removing the `instanceof DataSplit` guard → `ClassCastException`/wrong format → red.

4. **`isCppReaderEnabledReadsSessionProperty`** — Using a minimal `ConnectorSession` (the existing `TzSession` pattern, overriding `getSessionProperties`):
   - `{"enable_paimon_cpp_reader":"true"}` → `isCppReaderEnabled` true;
   - `"false"` / absent / `null` session → false.
   - WHY: pins the exact SPI key (`"enable_paimon_cpp_reader"`, byte-identical to `SessionVariable.ENABLE_PAIMON_CPP_READER`) and the default-false semantics. MUTATION: wrong key, or defaulting true → red.

(Existing serde round-trip test `PaimonTableSerdeRoundTripTest` already covers the serialized-Table wire; these new tests add the serialized-SPLIT wire for the cpp path.)

## E2E Tests

Live/CI-skipped (env-gated, like `PaimonLiveConnectivityTest`): the true end-to-end proof needs a running BE with the paimon-cpp reader and a real Paimon table. The regression suite already randomizes `enable_paimon_cpp_reader` (`random.nextBoolean()`), so existing paimon read regressions (e.g. `external_table_p2/paimon/*`) will exercise both branches once run against a BE; before the fix a flag-on run dies with `paimon-cpp deserialize split failed`, after the fix it reads correctly. No new BE-dependent test is added in this connector-only change; the offline UTs above pin the FE-side format contract deterministically in CI.

---

# ✅ IMPL SUMMARY (2026-06-11)

**Status: DONE — build+UT green (PaimonScanPlanProviderTest 12/0, incl. 4 new; imports clean; HEAD uncommitted).**

## Fix (1 production file: `PaimonScanPlanProvider.java`)
- Imports: `java.io.ByteArrayOutputStream`, `org.apache.paimon.io.DataOutputViewStreamWrapper`.
- Constant `ENABLE_PAIMON_CPP_READER = "enable_paimon_cpp_reader"` + static `isCppReaderEnabled(ConnectorSession)` (reads `session.getSessionProperties()`, `Boolean.parseBoolean`, null-safe default false).
- `planScan` reads the flag once (`boolean cppReader = isCppReaderEnabled(session)`) and passes it to both `buildJniScanRange` call sites.
- `buildJniScanRange` gains a `boolean cppReader` param; serialization switched from `encodeObjectToString` → new static `encodeSplit(split, cppReader)`.
- `encodeSplit`: when `cppReader && split instanceof DataSplit` → native `DataSplit.serialize(DataOutputViewStreamWrapper)` + STANDARD base64; else → existing Java `encodeObjectToString`. The `instanceof DataSplit` guard is load-bearing parity.

## Tests (4 new in `PaimonScanPlanProviderTest`)
- `cppReaderFlagSelectsNativeBinaryForDataSplit` / `cppReaderFlagOffKeepsJavaSerialization`: build a REAL `DataSplit` offline (local FileSystemCatalog + write 2 rows via BatchWriteBuilder + plan().splits()); native wire round-trips via `DataSplit.deserialize` and differs from the Java leg; flag-off equals the Java leg byte-for-byte.
- `nonDataSplitStaysJavaSerializedEvenWithCppFlag`: a `NonDataSplitStub implements Split` stays Java-serialized even with flag on (instanceof guard).
- `isCppReaderEnabledReadsSessionProperty`: exact key + default-false (true/false/absent/null-session).

## Note
- `encodeObjectToString` kept PRIVATE; the flag-off parity test reproduces the Java serde inline (`feJavaEncode`, identical to `PaimonTableSerdeRoundTripTest`'s helper) rather than widening production visibility.

## Live-e2e (gated, NOT run): the regression harness randomizes `enable_paimon_cpp_reader`; existing paimon read suites exercise both branches against a real BE.
