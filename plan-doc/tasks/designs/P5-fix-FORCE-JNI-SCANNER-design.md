# P5 fix design — `FIX-FORCE-JNI-SCANNER` (rereview2 #7 = M-1)

> Source finding: `plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md` (M-1; 3/3 confirmed, C+P+R upheld).
> Re-verified against **current** code (4-scout + adversarial-synthesizer workflow `wf_0e3e0976-b53` + independent reads).
> Scope: **MAJOR, pure connector, no SPI, no BE param** — unambiguous (per HANDOFF, no user decision needed).

---

## Problem

Legacy honors the session var `force_jni_scanner`: `SET force_jni_scanner=true` routes **all** data splits to the JNI reader, bypassing the native ORC/Parquet readers. This is the **escape hatch** used to dodge native-reader bugs (e.g. the B2 schema-evolution class of bug).

The cutover (plugin) connector **never reads `force_jni_scanner`**. Its split router consults only `paimonHandle.isForceJni()` — the **name-derived** binlog/audit_log flag (`PaimonConnectorMetadata.java:335`, computed from the system-table name, zero session input). So on the connector path, ORC/Parquet **always** take the native reader and the escape hatch is silently gone.

Repro: `SET force_jni_scanner=true; SELECT * FROM paimon_orc_table` → connector still uses the native reader.

## Root Cause

The native-vs-JNI routing gate ports only **two** of legacy's **three** conjuncts.

Legacy gate (`PaimonScanNode.java:430`):
```java
} else if (!forceJniScanner && !forceJniForSystemTable && supportNativeReader(optRawFiles)) {
```
- `forceJniScanner` = `sessionVariable.isForceJniScanner()` (`:361`) — the **session** escape hatch.
- `forceJniForSystemTable` = `shouldForceJniForSystemTable()` (`:367`) — binlog/audit NAME force.
- `supportNativeReader(optRawFiles)` — all raw files are `.orc`/`.parquet`.

Connector pure static (`PaimonScanPlanProvider.java:509-511`):
```java
static boolean shouldUseNativeReader(boolean forceJni, Optional<List<RawFile>> optRawFiles) {
    return !forceJni && supportNativeReader(optRawFiles);   // forceJni == forceJniForSystemTable only
}
```
The first conjunct (`!forceJniScanner`) is **dropped**. This dropped conjunct **is** M-1.

The session var is fully reachable with **no fe-core import**: `SessionVariable.FORCE_JNI_SCANNER = "force_jni_scanner"` is a `@VarAttr` (`SessionVariable.java:772,2879`) with no `INVISIBLE` flag and not `REMOVED`, so `VariableMgr.toMap` emits it into `ConnectorSession.getSessionProperties()` — the **exact same channel** the connector already reads for its sibling `enable_paimon_cpp_reader` (`isCppReaderEnabled`, `PaimonScanPlanProvider.java:166-171`). The literal string `"force_jni_scanner"` is the contract.

## Design

Pure connector, one file (`PaimonScanPlanProvider.java`) + its test. No SPI signature change, no fe-core import, no BE serialization (legacy serializes nothing for this var — grep confirms only fe-core lines 361/367/430 reference it).

### 1. Read the session var (mirror `isCppReaderEnabled`)
- New constant `FORCE_JNI_SCANNER = "force_jni_scanner"` (byte-identical to `SessionVariable.FORCE_JNI_SCANNER`).
- New package-private static `isForceJniScannerEnabled(ConnectorSession session)`: null-guard → `false`; else `Boolean.parseBoolean(session.getSessionProperties().get(FORCE_JNI_SCANNER))`. Byte-for-byte mirror of `isCppReaderEnabled` (default false = legacy default, so normal reads unaffected).

### 2. Site A (CORRECTNESS) — native router, `shouldUseNativeReader`
Add `forceJniScanner` as an **explicit third parameter**, mirroring legacy's three-boolean gate 1:1:
```java
static boolean shouldUseNativeReader(boolean forceJni, boolean forceJniScanner,
        Optional<List<RawFile>> optRawFiles) {
    return !forceJni && !forceJniScanner && supportNativeReader(optRawFiles);
}
```
Call site (`:295`):
```java
if (shouldUseNativeReader(paimonHandle.isForceJni(), isForceJniScannerEnabled(session), optRawFiles)) {
```

**Why a 3rd param, not a call-site OR** (deliberate override of the workflow synthesizer's "call-site OR" suggestion; sides with the legacy-parity scout):
- `force_jni_scanner` is a **routing** input semantically identical to the existing `forceJni` param (`forceJniForSystemTable`); legacy treats them as **sibling booleans in the same gate** (`:430`). The faithful shape is a sibling param, not a hidden OR. (The `cppReader = isCppReaderEnabled(session)` precedent one line above is a *serialization-format* flag, a different role — not the right analogy.)
- **Rule 9 (tests verify intent):** the routing decision is offline-undrivable (`FakePaimonTable.newReadBuilder()` throws), so the pure static is the **only** unit-testable seam for routing. A call-site OR would leave the new dimension testable only via the helper's *string-parsing* test, which **cannot fail when the routing logic changes** — exactly the anti-pattern Rule 9 forbids. The 3rd param makes `shouldUseNativeReader(false, /*forceJniScanner*/ true, native-eligible) == JNI` a mutation-tested fact.
- Cost: 3 existing test call sites add a `false` arg (mechanical; also makes them explicit). `forceJni` is **OR-sibling, never replaced** — replacing it would re-break binlog/audit_log routing.

### 3. Site B (cleanliness, correctness-NEUTRAL) — schema-evolution emit gate
`getScanNodeProperties:436`:
```java
if (!paimonHandle.isForceJni() && !isForceJniScannerEnabled(session)) {
    buildSchemaEvolutionParam(table).ifPresent(v -> props.put(SCHEMA_EVOLUTION_PROP, v));
}
```
`paimon.schema_evolution` is the **native-reader-only** field-id dictionary. BE consumes it **only** on the native ORC/Parquet path (`PaimonOrcReader`/`PaimonParquetReader::on_before_init_reader` → `TableSchemaChangeHelper::gen_table_info_node_by_field_id`, `be/.../paimon_reader.cpp:51-54,188-191`); the JNI reader (`paimon_jni_reader.cpp`) and cpp reader (`paimon_cpp_reader.cpp`) **never** reference it, and BE dispatch (`file_scanner.cpp:1045-1058`) only rewrites a range to native when `!paimon_split.__isset`. When `force_jni_scanner=true`, Site A routes **every** DataSplit to JNI (non-DataSplits were already JNI) → **zero** native ranges → the dict is dead weight.

- KEEPING the emit is harmless (JNI ignores it; costs only the base64 serialize/transport). SKIPPING it is safe (nobody consumes it).
- **Why gate it anyway:** same root cause (var never consulted) + the connector's **own comment** (`:434-435`) already documents the dict as "Only meaningful when the table can take the native path (a DataTable read *without force_jni_scanner*); JNI splits never consult it." Leaving the gate blind to the session var contradicts that documented contract. The comment is updated to state the gate now honors `force_jni_scanner`.
- Both sites read the **identical** helper, so they cannot disagree (each SPI call gets a fresh provider — no shared instance state).

### Out of scope (verified separate findings — do NOT touch)
- **M-2 count pushdown** — connector has no count branch (legacy runs it *before* the native gate, `:421`). Separate.
- **M-3 native sub-split sizing** — connector emits one range per RawFile vs legacy `fileSplitter.splitFile` (`:445-453`). Separate.
- **`IgnoreSplitType`** (`IGNORE_JNI`/`IGNORE_NATIVE`, legacy `:389/431/471`) — a different unimplemented session gate, **not** keyed on `force_jni_scanner`. Separate.
- Non-DataSplit handling (`:281-285`) already unconditional JNI; unchanged.
- No BE param for `force_jni_scanner` (legacy adds none).

## Implementation Plan
1. `PaimonScanPlanProvider.java`:
   - add `FORCE_JNI_SCANNER` constant after `ENABLE_PAIMON_CPP_READER` (`~:134`);
   - add `isForceJniScannerEnabled(ConnectorSession)` after `isCppReaderEnabled` (`~:172`);
   - `:295` pass `isForceJniScannerEnabled(session)` as the new 2nd arg;
   - `:509-511` widen `shouldUseNativeReader` to 3 args + update its javadoc (`:492-508`) to note the session escape hatch (legacy parity `:430`);
   - `:436` add `&& !isForceJniScannerEnabled(session)` + refresh the comment.
2. Update the 3 existing `shouldUseNativeReader` test calls (`:206/222/231`) → add `/*forceJniScanner*/ false`.

## Risk Analysis
- **Default-off:** `force_jni_scanner` defaults `false`; absent/empty/null → `false` (null-guard). Normal reads route exactly as today. Zero regression risk on the default path.
- **`fuzzy=true`** (`SessionVariable.java:2880`, same as `enable_paimon_cpp_reader`): under fuzzed/random-session regression runs the var may flip true → any live e2e asserting *native*-path behavior must set `force_jni_scanner=false` explicitly. Harness property of both vars, not a connector defect; noted for the e2e author.
- **Site B null-session:** existing `getScanNodePropertiesSkipsSchemaEvolutionForNonFileStoreTable` passes `session=null`; null-guarded helper → `!isForceJniScannerEnabled(null) == true` → test stays green (verified, no NPE).
- **binlog/audit_log:** `forceJni` is OR-sibling, never replaced → name-force routing intact.

## Test Plan

### Unit Tests (offline FE, `PaimonScanPlanProviderTest`)
1. **`isForceJniScannerEnabledReadsSessionProperty`** (clone of `isCppReaderEnabledReadsSessionProperty`): assert `true` for `{"force_jni_scanner":"true"}`, `false` for `"false"`, `false` for empty map ("absent ⇒ default false"), `false` for null session. WHY: pins the exact key + default-false + null-safety that routing hinges on. RED-MUTATION: wrong key / default-true → flips → red.
2. **`forceJniScannerRoutesNativeEligibleSplitToJni`**: `assertFalse(shouldUseNativeReader(/*forceJni*/ false, /*forceJniScanner*/ true, Optional.of([parquetRawFile(...)])))`. WHY: with `force_jni_scanner=true`, a native-eligible normal-table split must route to JNI (legacy parity `:430`). RED-MUTATION: drop `!forceJniScanner` → returns native → red.
3. **Updated existing 3** (`forceJniSysTable…`, `nonForcedSplitWithRawFiles…`, `nonForcedSplitWithoutNativeFiles…`): add `false` for the new param. The `(false,false,native)→native` and `(true,_,native)→JNI` cases stay pinned; regression guard that the widened signature didn't perturb routing.

### Site B test coverage (honest limitation — Rule 12)
A dedicated red-before/green-after for the schema-evolution *emit suppression* is **not feasible offline**: `buildSchemaEvolutionParam` requires a real `FileStoreTable` with a `SchemaManager`, but the offline harness only has `FakePaimonTable` (returns empty dict regardless), matching the suite's deliberate offline-pure convention. So dropping the Site B gate would not turn any offline test red. Site B is therefore covered by: (a) the shared `isForceJniScannerEnabled` helper test (its only variable term), (b) code inspection + BE-source evidence of correctness-neutrality, (c) the gated live e2e. Stated explicitly rather than claimed.

### E2E Tests (CI-gated, live paimon — not run here)
`SET force_jni_scanner=true; SELECT * FROM <paimon_orc/parquet_table>` → reader=JNI (vs native when false). Real escape-hatch behavior is a **live-e2e-only** gate; no offline harness drives BE reader selection. Not added as a new suite (no live paimon fixture in this branch); noted as the true end-to-end check.

## SPI / docs impact
- **None to SPI/RFC** — pure connector, no `ConnectorContext`/`Connector` surface change.
- No `decisions-log` entry (scope was unambiguous; the 3rd-param-vs-OR choice is an internal engineering call, recorded here).
- No `deviations-log` entry — the fix achieves **full legacy parity** for `force_jni_scanner` routing; Site B is connector-specific (legacy has no schema-evolution dict) and is correctness-neutral, not a parity deviation.
