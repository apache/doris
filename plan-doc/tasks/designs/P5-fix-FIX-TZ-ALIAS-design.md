# Problem

`FOR TIME AS OF '<datetime-string>'` against a Paimon table fails with a `DorisConnectorException` whenever the session `time_zone` is a Doris zone alias that `java.time.ZoneId.of(String)` does not recognize — specifically **CST, PST, EST**. CST is Doris's default region alias for `Asia/Shanghai`, so this breaks datetime-string time-travel under the **default** configuration, not merely an edge case.

Legacy (`fe-core` `PaimonUtil.getPaimonSnapshotByTimestamp`) resolved the *same* session-zone string successfully via the fe-core Doris alias map, so this is a parity regression introduced by the SPI cutover.

Report reference: `plan-doc/reviews/P5-paimon-fullpath-review-2026-06-11.md` Finding 3.1 ("FOR TIME AS OF datetime-string 在 session time_zone CST/PST/EST 下失败, legacy 成功", MAJOR, CONFIRMED 3/0/0).

# Root Cause (confirmed in current code)

`PaimonConnectorMetadata.parseTimestampMillis` resolves the session zone with a bare, alias-less `ZoneId.of`:

- `fe/fe-connector/fe-connector-paimon/.../PaimonConnectorMetadata.java:538-547` — `zoneId = java.time.ZoneId.of(session.getTimeZone());` inside a `try`, and on `DateTimeException` it throws a `DorisConnectorException` telling the user the zone "is not a standard zone id". There is **no alias map**.

This is reached from `resolveTimeTravel` TIMESTAMP case at `PaimonConnectorMetadata.java:418-419` (`long millis = parseTimestampMillis(session, spec);`), for non-digital specs only (`spec.isDigital()` short-circuits at :530-531).

Legacy path (still in tree), confirmed firsthand:
- `fe/fe-core/.../datasource/paimon/PaimonUtil.java:660` — `DateTimeUtils.parseTimestampData(timestamp, 3, TimeUtils.getTimeZone()).getMillisecond();`
- `fe/fe-core/.../common/util/TimeUtils.java:131-138` — `getTimeZone()` returns `TimeZone.getTimeZone(ZoneId.of(timezone, timeZoneAliasMap))`.
- `fe/fe-core/.../common/util/TimeUtils.java:106-117` — `timeZoneAliasMap` is built as `new TreeMap<>(String.CASE_INSENSITIVE_ORDER)` seeded with `ZoneId.SHORT_IDS` (`putAll`) and then overridden with exactly four entries: `CST -> Asia/Shanghai`, `PRC -> Asia/Shanghai`, `UTC -> UTC`, `GMT -> UTC`.

The connector dropped the *entire* alias map (both `SHORT_IDS` and the 4 overrides), so `ZoneId.of` falls back to its native parsing, which rejects CST/PST/EST.

### Correction to the report's literal suggestion (verified by JDK harness)

The report's `suggestion` (line 111) says to inline **only the 4 explicit entries**. I ran a JDK harness (mirroring the reviewers' methodology) and proved that is **insufficient**:

- 4-entry-only map: `ZoneId.of("CST",m)=Asia/Shanghai`, but `ZoneId.of("PST",m)` **THROWS** `ZoneRulesException` and `ZoneId.of("EST",m)` **THROWS**.
- Full legacy map (`SHORT_IDS` + 4 overrides, case-insensitive): `CST->Asia/Shanghai`, `PST->America/Los_Angeles`, `EST->-05:00`, `PRC->Asia/Shanghai`, `UTC->UTC`, `GMT->UTC`; truly-unknown ids (`XYZ`, `NOPE/ZZZ`) still THROW.

So **PST and EST resolve via `ZoneId.SHORT_IDS`, not via the 4 puts.** The byte-faithful fix must replicate the *full* legacy map (`SHORT_IDS` overlaid with the 4 overrides, case-insensitive), or PST/EST — two of the three aliases the report names — remain broken.

# Design

Replicate the legacy `timeZoneAliasMap` as a small private static constant **inside the connector** (no fe-core import; `ZoneId.SHORT_IDS` is JDK-provided, and the 4 overrides are literal strings — exactly the "DLF inline keys" technique already used in B1). Then change the single `ZoneId.of(tz)` call to the two-arg `ZoneId.of(tz, ALIAS)` form, identical to legacy `TimeUtils.getTimeZone()`.

Key decisions, justified:

1. **Replicate the full map, not 4 entries.** Build `new TreeMap<>(String.CASE_INSENSITIVE_ORDER)`, `putAll(ZoneId.SHORT_IDS)`, then the 4 overrides — byte-identical to `TimeUtils` static initializer. This is the only construction that resolves CST/PST/EST exactly as legacy.

2. **Case-insensitive**, matching legacy. `checkTimeZoneValidAndStandardize` (`TimeUtils.java:314`) validates `SET time_zone` against this case-insensitive map and stores the value as-entered, so any case Doris accepts must resolve here too.

3. **Still fail loud on truly-unknown ids.** `ZoneId.of(tz, ALIAS)` throws `DateTimeException` for ids absent from the map; the existing `try/catch -> DorisConnectorException` is kept, so the "wrong zone -> wrong snapshot -> silently wrong rows" landmine the original comment warns about is still guarded for genuinely-unsupported ids. We only stop rejecting the ids legacy accepted.

4. **No new dependency.** The connector module has no guava (verified). Use `java.util.TreeMap` + `java.util.Collections.unmodifiableMap` — the exact idiom already in `PaimonScanRange.java:72/96` and `PaimonTableHandle.java:124`. Keep `java.time.*` fully qualified inline, as the surrounding method already does (`java.time.ZoneId`, `java.time.DateTimeException`).

5. **Surgical.** One new constant + one changed line + javadoc correction. No signature changes, no SPI changes, no other call sites (the only `ZoneId.of` in the connector is this one — verified).

# Implementation Plan

### File 1 — `PaimonConnectorMetadata.java` (connector main)

**(a) Add a private static constant** (place it near the other private statics / above `parseTimestampMillis`, ~:528):

```java
/**
 * Doris session time-zone alias map, replicated from fe-core
 * {@code TimeUtils.timeZoneAliasMap} (TimeUtils.java:106-117). The connector cannot import fe-core,
 * so the map is rebuilt here byte-for-byte: {@link java.time.ZoneId#SHORT_IDS} (the JDK-provided
 * short ids, which is where "PST"/"EST" resolve) overlaid with the four Doris overrides
 * (CST/PRC -> Asia/Shanghai, UTC/GMT -> UTC). Case-insensitive, exactly like legacy, because
 * {@code SET time_zone} stores the alias verbatim in any case.
 */
private static final Map<String, String> SESSION_TIME_ZONE_ALIASES;

static {
    Map<String, String> m = new java.util.TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    m.putAll(java.time.ZoneId.SHORT_IDS);
    m.put("CST", "Asia/Shanghai");
    m.put("PRC", "Asia/Shanghai");
    m.put("UTC", "UTC");
    m.put("GMT", "UTC");
    SESSION_TIME_ZONE_ALIASES = java.util.Collections.unmodifiableMap(m);
}
```

(`Map` is already imported; `TreeMap`/`Collections`/`ZoneId` referenced fully-qualified to match the existing inline `java.time.*` style and avoid touching the import block. If preferred, `TreeMap`/`Collections` may instead be added to the existing `java.util.*` import group — both pass checkstyle; pick whichever the implementer finds cleaner, but keep `java.time.ZoneId` qualified since the method body already does.)

**(b) Change the one resolution line** at `:540`:

```java
// before
zoneId = java.time.ZoneId.of(session.getTimeZone());
// after
zoneId = java.time.ZoneId.of(session.getTimeZone(), SESSION_TIME_ZONE_ALIASES);
```

**(c) Update the method javadoc** at `:512-526`. The current text frames CST/PST/EST as a "KNOWN LIMITATION" / deliberate fail-loud. Rewrite to: the connector replicates the legacy alias map so CST/PRC/UTC/GMT and the `SHORT_IDS` aliases (PST/EST/...) resolve byte-identically to legacy `TimeUtils.getTimeZone()`; the `try/catch -> DorisConnectorException` now fires only for ids absent from BOTH `ZoneId.of`'s native set AND the alias map (genuinely unsupported), preserving the "no silent degrade to a wrong zone" invariant. Keep the `millis < 0` guard note.

### File 2 — `PaimonConnectorMetadataMvccTest.java` (connector test)

The existing test `resolveTimestampStringWithUnsupportedZoneAliasThrowsClearError` (~:304-326) **encodes the now-wrong intent** ("CST must fail loud"). It must be updated as part of the fix (changing intent legitimately changes the test):

- **Re-point the fail-loud test** to a genuinely-unknown id (e.g. `"XYZ"` or `"NOPE/ZZZ"`), keeping the `DorisConnectorException` + "standard"/"zone id" message assertions. This still pins the no-silent-degrade contract.
- **Add new parity tests** (see Test Plan) proving CST/PST/EST now resolve like legacy.

# Risk Analysis

- **Parity vs legacy:** After the change, `ZoneId.of(tz, SESSION_TIME_ZONE_ALIASES)` is the same call legacy makes via `TimeUtils.getTimeZone()` (`ZoneId.of(timezone, timeZoneAliasMap)`), and the downstream `DateTimeUtils.parseTimestampData(value, 3, TimeZone.getTimeZone(zoneId))` is identical to `PaimonUtil.java:660`. The map is a byte-for-byte replica of `TimeUtils.java:106-117`. Net effect: every id legacy accepted now resolves identically; ids legacy rejected still fail loud.
- **Blast radius:** Zero shared/fe-core code touched. The new constant is `private static` in one connector class; the only behavioral change is one extra arg to one `ZoneId.of` call reached only from `resolveTimeTravel`'s TIMESTAMP non-digital branch. Digital timestamps (`spec.isDigital()`, :530) never touch `ZoneId.of` and are unaffected (an existing test, `resolveTimestampDigitalUnaffectedByUnsupportedZoneAlias`, locks this).
- **`ZoneId.SHORT_IDS` stability:** It is a JDK-frozen constant (CST/PST/EST mappings are fixed). Legacy uses the same source, so the connector and legacy track each other across JDK versions automatically — no drift risk versus legacy.
- **Edge cases:**
  - Offset ids (`+08:00`) and full IANA ids (`Asia/Shanghai`): pass through unchanged (resolved by `ZoneId.of`'s native parsing even with the map present) — verified.
  - Case: legacy is case-insensitive; the replica `TreeMap(CASE_INSENSITIVE_ORDER)` matches. (Native `ZoneId.of` is case-sensitive, but Doris normally stores these aliases uppercase; matching legacy is the safe choice.)
  - Truly-unknown id: still throws `DorisConnectorException` (no silent wrong-zone) — preserved, and re-tested.
  - `millis < 0` guard at :550-551: untouched.
- **Why not the report's literal 4-entry suggestion:** it leaves PST/EST broken (proven by harness). Adopting it would partially "fix" the finding while silently leaving two of the three named aliases failing — a Rule 12 "fail loud" violation. Flagged for the reviewer in notes.

# Test Plan

## Unit Tests (connector test dir, no fe-core, no live cluster)

All in `PaimonConnectorMetadataMvccTest.java`, reusing the existing `TzSession`, `RecordingPaimonCatalogOps`, `normalHandle`, `metadataWith`, and the byte-parity reference pattern already established by `resolveTimestampStringParsedWithSessionTimeZone` (:276-302) which captures `ops.snapshotIdAtOrBeforeArg`.

1. **`resolveTimestampStringResolvesCstAliasToShanghai` (NEW)** — FAILS before (throws `DorisConnectorException`), PASSES after.
   - `new TzSession("CST")`, literal `"2023-11-15 00:00:00"`, `spec.timestamp(literal, false)`.
   - Reference: `expectedShanghai = DateTimeUtils.parseTimestampData(literal, 3, TimeZone.getTimeZone("Asia/Shanghai")).getMillisecond()` and `expectedUtc` for UTC; assert the two differ (test self-guard).
   - Assert `ops.snapshotIdAtOrBeforeArg == expectedShanghai` (NOT `expectedUtc`).
   - WHY comment: CST is Doris's default alias for `Asia/Shanghai`; legacy resolved it via the alias map. Pinning the *Shanghai* millis (not UTC, not a throw) is the byte-parity intent. MUTATION: alias-less `ZoneId.of` -> throws (red); a wrong override (CST->UTC) -> captures `expectedUtc` (red).

2. **`resolveTimestampStringResolvesPstAndEstViaShortIds` (NEW)** — FAILS before, PASSES after. This is the test that specifically guards the report-suggestion correction.
   - For `"PST"`: reference `DateTimeUtils.parseTimestampData(literal, 3, TimeZone.getTimeZone("America/Los_Angeles"))`; assert captured arg equals it.
   - For `"EST"`: reference `TimeZone.getTimeZone(ZoneId.of("-05:00"))`; assert captured arg equals it.
   - WHY comment: PST/EST resolve through `ZoneId.SHORT_IDS`, NOT the 4 explicit overrides; a fix that inlined only the 4 entries would leave these throwing. This test fails under both the buggy original AND the incomplete 4-entry "fix", and only passes when the full `SHORT_IDS`+overrides map is replicated. MUTATION: dropping `putAll(ZoneId.SHORT_IDS)` -> PST/EST throw (red).

3. **`resolveTimestampStringWithGenuinelyUnknownZoneFailsLoud` (REPURPOSED from the existing `...UnsupportedZoneAliasThrowsClearError`)** — PASSES before and after (intent preserved, only the input changes from `"CST"` to a truly-unknown id).
   - `new TzSession("NOPE/ZZZ")` (or `"XYZ"`); assert `DorisConnectorException` whose message contains the offending id and "standard" + "zone id".
   - WHY comment: a zone id absent from BOTH `ZoneId.of`'s native set and the alias map must fail loud with actionable guidance — never silently degrade to a wrong zone (wrong snapshot -> silently wrong rows). The fix narrows the failure set to *genuinely* unknown ids; it must not become a silent UTC fallback. MUTATION: catching and degrading to UTC -> assertThrows finds nothing (red).

4. **(Keep as-is)** `resolveTimestampDigitalUnaffectedByUnsupportedZoneAlias` (:328+) and `resolveTimestampStringParsedWithSessionTimeZone` (:276) — both must continue to pass, locking that the digital path bypasses `ZoneId.of` and that a standard IANA session zone is honored.

## E2E Tests

Live-only / CI-skipped (the connector module has no live Paimon cluster in unit CI; `PaimonLiveConnectivityTest` is the existing live harness). Manual / regression-suite reproduction:

```sql
SET time_zone = 'CST';
SELECT * FROM <paimon_catalog>.<db>.<tbl> FOR TIME AS OF '2023-11-15 00:00:00';
```
Before: query fails with `DorisConnectorException` ("CST is not a standard zone id ..."). After: returns the at-or-before snapshot rows, identical to legacy (which resolves CST as `Asia/Shanghai`). Repeat with `SET time_zone='PST'` / `'EST'`. These belong in the paimon time-travel regression group (live, requires a real Paimon catalog with multiple snapshots), so they are not part of the connector unit suite; the unit tests above provide the byte-parity coverage deterministically without a cluster.

---

# ✅ IMPL SUMMARY (2026-06-11)

**Status: DONE — build+UT green (PaimonConnectorMetadataMvccTest 37/0; imports clean; HEAD uncommitted).**

## Fix (1 production file: `PaimonConnectorMetadata.java`)
- Added `private static final Map<String,String> SESSION_TIME_ZONE_ALIASES` built in a static block: `new TreeMap<>(String.CASE_INSENSITIVE_ORDER)` → `putAll(ZoneId.SHORT_IDS)` → 4 overrides (CST/PRC→Asia/Shanghai, UTC/GMT→UTC) → `Collections.unmodifiableMap`. **Full SHORT_IDS map per the HANDOFF correction** (4-entry-only leaves PST/EST throwing).
- Changed the single `ZoneId.of(session.getTimeZone())` → `ZoneId.of(session.getTimeZone(), SESSION_TIME_ZONE_ALIASES)`.
- Rewrote the method javadoc: removed the "KNOWN LIMITATION / CST·PST·EST fail-loud" framing; now states it resolves byte-identically to legacy `TimeUtils.getTimeZone()` and fail-loud fires only for ids absent from BOTH ZoneId.of's native set AND the map. Error message unchanged (still contains "standard"/"zone id").

## Tests (`PaimonConnectorMetadataMvccTest`)
- **Repurposed** `resolveTimestampStringWithUnsupportedZoneAliasThrowsClearError` → `resolveTimestampStringWithGenuinelyUnknownZoneFailsLoud` (input `"CST"`→`"XYZ"`; same DorisConnectorException + "standard"/"zone id" assertions — intent "fail loud on unknown" preserved).
- **Added** `resolveTimestampStringResolvesCstAliasToShanghai` (CST→Asia/Shanghai byte-parity) and `resolveTimestampStringResolvesPstAndEstViaShortIds` (PST→America/Los_Angeles, EST→-05:00 — the report-suggestion correction guard).

## Deviation from design (Rule 9, documented)
The design said "keep `resolveTimestampDigitalUnaffectedByUnsupportedZoneAlias` as-is". I changed its session zone `"CST"`→`"XYZ"`. Rationale: after the fix CST RESOLVES, so a CST session would no longer prove the digital-bypass (the string path would parse fine too) — the test would pass even if someone removed the `spec.isDigital()` short-circuit, losing its mutation-catching power. `"XYZ"` still throws on the string path, so the test keeps discriminating. This is a faithful improvement, not a scope change.

## Live-e2e (gated, NOT run): `SET time_zone='CST'/'PST'/'EST'; SELECT ... FOR TIME AS OF '<datetime>'` against a live paimon catalog with multiple snapshots.
