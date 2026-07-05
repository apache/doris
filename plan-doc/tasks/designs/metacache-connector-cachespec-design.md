# Design — Shared connector-side `CacheSpec` (restore meta-cache property validation)

Status: **implemented (Phase 1 + Phase 2), unit-verified; docker regressions pending cluster** ·
Branch: `catalog-spi-10-iceberg` · Date: 2026-07-01

Fixes: `test_iceberg_table_meta_cache` (CREATE + ALTER `meta.cache.iceberg.table.ttl-second='-2'`
expect `exception "is wrong"`, currently not thrown).

---

## 1. Problem / root cause (verified in code)

`org.apache.doris.datasource.metacache.CacheSpec` (fe-core) splits two concerns:

- **Parse** — `fromProperties(...)` is *best-effort*: `NumberUtils.toLong(value, default)` silently
  falls back on a bad value; **never throws**.
- **Validate** — `checkLongProperty(value, minValue, key)` / `checkBooleanProperty(value, key)` are a
  *separate, explicit* step that throws
  `DdlException("The parameter " + key + " is wrong, value is " + value)` (`CacheSpec.java:126-148`).

The old `IcebergExternalCatalog.checkProperties()` called both — 4 `checkLongProperty` + 2
`checkBooleanProperty` (`IcebergExternalCatalog.java:88-105`). The new SPI connectors kept only the
best-effort parse (`IcebergConnector.resolveTableCacheTtlSecond:155-167`,
`PaimonConnector.resolveTableCacheTtlSecond:125-137`) and **dropped the validation** at cutover
(`PaimonConnector.java:122-123` comment: "the legacy CacheSpec check was dropped at cutover"). So
`ttl-second=-2` now parses to `-2` (treated as "cache disabled") instead of being rejected.

**Where validation must live now.** The SPI already provides the hook and the exception bridge:

```
CREATE/ALTER CATALOG
  → CatalogMgr.checkProperties()  (create @560, alter @658)
  → PluginDrivenExternalCatalog.checkProperties()            (fe-core:179-190)
      try   { ConnectorFactory.validateProperties(type, props) }
      catch (IllegalArgumentException e) { throw new DdlException(e.getMessage()); }   // verbatim
  → ConnectorPluginManager → XxxConnectorProvider.validateProperties(props)            // connector side
```

- `iceberg` and `paimon` are both in `CatalogFactory.SPI_READY_TYPES` (`CatalogFactory.java:50`), so
  this path is **live** for both, on **both CREATE and ALTER** (`CatalogMgr:560,658`).
- The wrapper catches **`IllegalArgumentException` only**. `DorisConnectorException extends
  RuntimeException` (NOT `IllegalArgumentException`), so the connector validator **must throw
  `IllegalArgumentException`** — which is exactly the existing convention
  (`AbstractHmsMetaStoreProperties:115`, etc.).
- Current `IcebergConnectorProvider.validateProperties:61-64` and
  `PaimonConnectorProvider.validateProperties:72-75` validate **metastore** properties only; they never
  touch the cache knobs.

**Hard constraint.** `tools/check-connector-imports.sh` (wired into `fe/fe-connector/pom.xml`
`validate` phase) forbids fe-connector modules from importing
`org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`. fe-core's `CacheSpec`
sits in `datasource.metacache` and imports `common.DdlException`, so connectors **cannot** import it.
The shared code must be a **connector-side copy**, not a cross-boundary dependency.

---

## 2. Goals / non-goals

**Goals**
1. Restore the dropped meta-cache property validation so invalid values are rejected at CREATE **and**
   ALTER, with the exact legacy message substring `is wrong`.
2. Express the validation once, in a shared fe-connector module, reused by multiple connectors
   (iceberg + paimon now; hudi/others can adopt later).
3. Match old-code semantics **exactly** (keys, min values, boolean rule, message).

**Non-goals**
- Not moving fe-core's `CacheSpec` or the Env/Caffeine-coupled metacache machinery
  (`AbstractExternalMetaCache`, `MetaCacheEntry`, …) — those stay in fe-core (user decision:
  connector-side **copy**, keep fe-core's own).
- Not migrating hive/HMS onto the shared class (it uses a different legacy inline rule —
  `NumberUtils.toInt(v,-1) < 0`, effective min **0**, rejects `-1`; iceberg/paimon accept `-1`).
  Changing hive would alter its `-1` semantics. Out of scope.
- Not re-wiring iceberg's inert `.table.enable` / `.table.capacity` knobs to real behavior.

---

## 3. Decisions (from user, 2026-07-01)

- **D1 — Approach:** create a connector-side **copy** of `CacheSpec` in `fe-connector-api`; fe-core
  keeps its own copy. (Not a full move/single-source.)
- **D2 — Scope:** restore validation for **iceberg + paimon** together (add the `-2 → is wrong`
  negative to the paimon test too). hudi/jdbc untouched (no `*meta_cache*` test, no surfaced need).

---

## 4. Design

### 4.1 New shared class

`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/cache/CacheSpec.java`
— package `org.apache.doris.connector.api.cache`.

Faithful copy of fe-core `CacheSpec`, with exactly two adaptations required by the module boundary:

1. **Validators throw `IllegalArgumentException`** instead of `DdlException` (same message text
   verbatim: `"The parameter " + key + " is wrong, value is " + value`). This is what the SPI wrapper
   catches and re-wraps into `DdlException`, so the user-visible message is identical to legacy.
2. **Drop the `org.apache.commons.lang3.math.NumberUtils` dependency** — `fe-connector-api` has zero
   third-party deps (only `fe-thrift` provided). Inline the long parse in `getLongProperty` with a
   `try/catch (NumberFormatException) → default` (the connectors' own `propLong` already does this).

Everything else (immutable value object, `PropertySpec`/`Builder`, the three `fromProperties`
overloads, `isCacheEnabled`, `toExpireAfterAccess`, `metaCacheKeyPrefix`/`isMetaCacheKeyForEngine`,
`applyCompatibilityMap`, constants `CACHE_NO_TTL`/`CACHE_TTL_DISABLE_CACHE`) is JDK-only and copied
unchanged. `fe-connector-api` is excluded from the plugin-zip and loaded once by the app classloader,
so there is no duplicate-class / classloader-split hazard.

Placement rationale: `fe-connector-api` is the value-object home (siblings `DorisConnectorException`,
`ConnectorPropertyMetadata`), is visible to `fe-connector-spi` and every connector (transitively), and
is already a fe-core dependency.

### 4.2 Wire validation into the providers (Phase 1 — required)

Add the legacy checks to each provider's `validateProperties`, **before** the metastore validate, so
invalid cache knobs fail fast (and, for paimon, before the existing dead-knob warning).

**Iceberg** — `IcebergConnectorProvider.validateProperties` (6 knobs, min values byte-exact to
`IcebergExternalCatalog.java:88-105`):

| key | check | min |
|---|---|---|
| `meta.cache.iceberg.table.enable` | boolean | — |
| `meta.cache.iceberg.table.ttl-second` | long | `-1` |
| `meta.cache.iceberg.table.capacity` | long | `0` |
| `meta.cache.iceberg.manifest.enable` | boolean | — |
| `meta.cache.iceberg.manifest.ttl-second` | long | `-1` |
| `meta.cache.iceberg.manifest.capacity` | long | `0` |

**Paimon** — `PaimonConnectorProvider.validateProperties` (3 knobs, byte-exact to deleted
`PaimonExternalCatalog.checkProperties`, recovered from git `0214560d5f3^`):

| key | check | min |
|---|---|---|
| `meta.cache.paimon.table.enable` | boolean | — |
| `meta.cache.paimon.table.ttl-second` | long | `-1` |
| `meta.cache.paimon.table.capacity` | long | `0` |

Both validators are `null`-tolerant (absent key → skip), so unset knobs and all currently-valid
catalogs are unaffected. Paimon keeps `warnIgnoredDeadTableCacheKeys` (valid-but-ignored warning) —
validation runs first and only rejects *invalid* values, matching old strictness without losing the
operator-friendly warning.

Key constants: iceberg already declares them in `IcebergConnectorProperties` (`:62-67`); paimon
declares only `TABLE_CACHE_TTL_SECOND` today — add `TABLE_CACHE_ENABLE` / `TABLE_CACHE_CAPACITY`
constants (or reuse `DEAD_TABLE_CACHE_PREFIX + "enable"/"capacity"`).

### 4.3 Adopt the shared parse to remove duplication (Phase 2 — recommended, separate commit)

So the copy is a genuinely *reused* expression (not validator-only) and the hand-rolled duplicates
disappear, refactor the connectors' best-effort parsers onto the shared `CacheSpec`:

- Iceberg: `IcebergScanPlanProvider.isManifestCacheEnabled`/`propLong` (`:1354-1375`) →
  `CacheSpec.isCacheEnabled(...)`; `IcebergConnector.resolveTableCacheTtlSecond` and
  `IcebergCatalogFactory` manifest option (`:114-121`) → shared parse.
- Paimon: `PaimonConnector.resolveTableCacheTtlSecond` / `schemaCacheTtlSecondOverride`
  (`:125-137,158-172`) → shared parse.

Behavior-preserving (identical formula), but touches the scan-plan path — guarded by existing
`IcebergScanPlanProviderTest`. **Deferrable**: if we skip Phase 2, trim the copy to the validators +
key helpers to avoid unused methods.

---

## 5. Tests

- **Iceberg** `test_iceberg_table_meta_cache.groovy` — **unchanged**; its CREATE (`:70-83`) and ALTER
  (`:149-152`) `-2 → "is wrong"` assertions are exactly what Phase 1 makes pass.
- **Paimon** `test_paimon_table_meta_cache.groovy` — **add** a CREATE negative
  (`meta.cache.paimon.table.ttl-second='-2'` → `exception "is wrong"`) and an ALTER negative
  (`alter catalog ... set properties("meta.cache.paimon.table.ttl-second"="-2")` → `exception "is
  wrong"`), mirroring iceberg. Keep the existing `ttl-second='0'` positive.
- **Unit (JUnit)** in `fe-connector-api`: port `CacheSpecTest`'s validator cases to the copy, asserting
  `IllegalArgumentException` (not `DdlException`) with message containing the key; cover `-1` accepted
  (min `-1`), `-2` rejected, non-numeric rejected, non-bool rejected, `null` skipped.
- **Provider unit tests**: extend `PaimonConnectorValidatePropertiesTest` (and add an iceberg
  equivalent if none) with the `-2`/garbage cache-knob negatives + valid positives.
- Regression gate: `tools/check-connector-imports.sh` must stay green (new class imports no fe-core).

---

## 6. Ordered TODO

**Phase 1 — restore validation (required)**
1. Add `fe-connector-api/.../connector/api/cache/CacheSpec.java` (copy; validators →
   `IllegalArgumentException`; inline long-parse, no `NumberUtils`).
2. Add `CacheSpecTest` in `fe-connector-api` (validator + parse cases).
3. `IcebergConnectorProvider.validateProperties`: add the 6-knob checks (before metastore validate).
4. `PaimonConnectorProvider.validateProperties`: add the 3-knob checks (before `warnIgnored…`); add the
   two paimon key constants.
5. Paimon test: add CREATE + ALTER `-2 → "is wrong"` negatives.
6. Extend provider validate unit tests (paimon; iceberg if applicable).
7. Build fe (`fe-connector-api`, `fe-connector-iceberg`, `fe-connector-paimon`) + run new JUnit +
   `check-connector-imports.sh` + checkstyle. Verify iceberg + paimon meta-cache regressions locally.

**Phase 2 — remove duplication (recommended, separate commit)**
8. Refactor iceberg scan-path + factory parse onto shared `CacheSpec`; keep `IcebergScanPlanProviderTest` green.
9. Refactor paimon parse onto shared `CacheSpec`.

**Wrap-up**
10. Update `plan-doc/HANDOFF.md`; commit per phase.

---

## 7. Risks / alternatives

- **Exception type.** If a validator threw `DorisConnectorException` it would escape the
  `catch (IllegalArgumentException)` bridge and NOT surface as `is wrong`. Mitigation: throw
  `IllegalArgumentException` (matches existing metastore validators). Covered by unit + regression.
- **Duplication (fe-core vs connector copy).** Two `CacheSpec` classes can drift. Accepted per D1; the
  logic (message + min-compare) is tiny/stable. Full single-source move remains a future option.
- **Paimon behavior change.** Restoring paimon validation reintroduces a check intentionally removed at
  cutover; accepted per D2. Only *invalid* values are rejected — valid catalogs unaffected; the
  dead-knob warning is preserved.
- **Phase 2 scan-path touch.** Behavior-preserving but in a hot path; isolated to a separate commit and
  gated by existing tests. Skippable (trim copy) if undesired.
- **Min-value divergence with hive** (iceberg/paimon accept `-1`, hive rejects it). Deliberately not
  unified; `minValue` stays a caller argument so each connector keeps its legacy rule.

---

## 8. Implementation outcome (2026-07-01)

**Phase 1 (validation restored)** — new `fe-connector-api/.../connector/api/cache/CacheSpec.java`
(faithful copy; validators throw `IllegalArgumentException`; long-parse inlined) + `CacheSpecTest`.
`IcebergConnectorProvider.validateProperties` now checks the 6 iceberg knobs and
`PaimonConnectorProvider.validateProperties` the 3 paimon knobs (before its dead-knob warning). Added
paimon `TABLE_CACHE_ENABLE`/`TABLE_CACHE_CAPACITY` constants. Paimon regression got CREATE+ALTER
`-2 → "is wrong"` negatives.

**Phase 2 (dedup)** — `IcebergScanPlanProvider.isManifestCacheEnabled` and
`IcebergCatalogFactory.appendManifestCacheProperties` now parse via the shared `CacheSpec.fromProperties`
+ `CacheSpec.isCacheEnabled`, deleting the hand-rolled `propLong` / `getBoolean` / `getLong` (and the now
unused `NumberUtils` import). This restores legacy no-trim semantics (fe-core `CacheSpec` never trimmed;
the connector's `propLong` had added trimming) — a behavior change only for whitespace-padded numeric
values, which no test/regression uses.

**Scoping deviation (surfaced):** the ttl-only best-effort parsers — `IcebergConnector`
`resolveTableCacheTtlSecond` and `PaimonConnector` `resolveTableCacheTtlSecond` /
`schemaCacheTtlSecondOverride` — were **left as-is**. They are not `isCacheEnabled` re-implementations, and
each carries an operator `LOG.warn` on a bad value that `CacheSpec` does not; routing them through
`CacheSpec.fromProperties` would drop the warning and over-read enable/capacity. Deduping them would be a
regression, so they stay.

**Test-intent conflict resolved (Rule 7):** the pre-existing paimon unit test
`deadTableCacheKeyIsAcceptedNotRejected` asserted `meta.cache.paimon.table.capacity=-5` is *accepted*
(warn-only) — the exact post-cutover behavior this task reverses. Replaced with `rejectsMalformedMetaCacheKnob`
+ `acceptsValidMetaCacheKnobs` per the restore directive.

**Verification:** `fe-connector-{api,iceberg,paimon}` reactor build (`-am`, `-Drevision=1.2-SNAPSHOT`)
BUILD SUCCESS; checkstyle clean; full suites green — CacheSpecTest 9/9,
IcebergConnectorValidatePropertiesTest 11/11, PaimonConnectorValidatePropertiesTest 15/15, whole iceberg
module 892/0-fail (1 pre-existing skip). Regression-suite audit: no other suite feeds an invalid
`meta.cache.*` value that the restored validation would newly reject. **Not run** (no cluster): the
docker regressions `test_iceberg_table_meta_cache` / `test_paimon_table_meta_cache` — same code path is
covered by the provider unit tests.

**Commit/staging caveat:** `IcebergScanPlanProvider.java` carried pre-existing uncommitted hunks
(~L991, ~L1003) from prior branch WIP before this task; the Phase 2 edit now shares that file. Isolating
the metacache change into its own commit needs care (that file also contains unrelated WIP).
`IcebergCatalogFactory.java` was otherwise clean. Not committed — awaiting user.
