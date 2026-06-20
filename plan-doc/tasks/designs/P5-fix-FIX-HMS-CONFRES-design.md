# Problem

When a Paimon HMS catalog is created with `hive.conf.resources` pointing at an external `hive-site.xml` (e.g. `CREATE CATALOG ... 'paimon.catalog.type'='hms','hive.conf.resources'='hive-site.xml'`), the legacy code loaded that file into the `HiveConf` used to build the catalog, so every key in it (custom `hive.metastore.*`, SASL `qop`, kerberos, socket/timeout, SSL truststore, metastore-URI override, custom Thrift transport) reached the live `HiveMetaStoreClient`. The new SPI connector path silently drops the file: `PaimonCatalogFactory.buildHmsHiveConf` reconstructs the `HiveConf` from the raw property map only and never opens `hive.conf.resources`. Result: any HMS catalog whose connection-critical settings live *only* in an external `hive-site.xml` connects with a degraded/wrong `HiveConf` and fails the handshake or behaves incorrectly against the metastore — a silent parity regression now that paimon is in `SPI_READY_TYPES` (cutover gate open).

Confirmed by the clean-room review: CONFIRMED 3 / REFUTED 0 / PARTIAL 0 (path LIVE + file content truly dropped + legacy truly loaded it into the catalog `HiveConf`). The one inaccurate detail in the finding (it claimed legacy `PaimonHMSMetaStoreProperties`'s `ExecutionAuthenticator` is reused — the connector actually builds auth independently via `ConnectorContext.executeAuthenticated`) is orthogonal and does not change the confirmed defect.

# Root Cause (confirmed in current code)

**Legacy loaded the file as the BASE of the catalog HiveConf.** `fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/HMSBaseProperties.java:195-210` (`checkAndInit`): `this.hiveConf = loadHiveConfFromFile(hiveConfResourcesConfig)` (line 197) then overlays user `hive.*` overrides (line 199), then `hive.metastore.uris` (line 200), then the timeout default. `loadHiveConfFromFile` (`HMSBaseProperties.java:188-193`) delegates to `CatalogConfigFileUtils.loadHiveConfFromHiveConfDir(resourceConfig)`. That `HiveConf` is consumed by `PaimonHMSMetaStoreProperties.buildHiveConfiguration` (`fe/fe-core/.../metastore/PaimonHMSMetaStoreProperties.java:77-86`, `conf = hmsBaseProperties.getHiveConf()`) and passed to `CatalogFactory.createCatalog` in `initializeCatalog` (lines 89-101).

**The file loader resolves names against a config dir.** `fe/fe-common/src/main/java/org/apache/doris/common/CatalogConfigFileUtils.java:95-102` (`loadHiveConfFromHiveConfDir`): comma-splits the resource list, prepends `Config.hadoop_config_dir` (= `$DORIS_HOME/plugins/hadoop_conf/`, `Config.java:2961`), requires each file to exist, and `HiveConf.addResource(Path)` each. This is filesystem + FE-`Config` work — inherently an fe-core/fe-common concern.

**The connector never opens the file.** `fe/fe-connector/fe-connector-paimon/.../PaimonCatalogFactory.java:363-425` (`buildHmsHiveConf`) builds a fresh `new HiveConf()` and only: copies verbatim `hive.*` map keys (366-370), sets `hive.metastore.uris` (372-375), copies a fixed auth-key set (378-398), sets kerberos-conditional keys (392-412), defaults the socket timeout (418-420), overlays storage config (423). The Javadoc at lines 349-360 explicitly states loading the external `hive-site.xml` is DEFERRED because "legacy resolved it through fe-core `CatalogConfigFileUtils`, which the connector cannot import." The connector consumes it at `PaimonConnector.java:158-159` (`buildHmsHiveConf(properties)` → `CatalogContext.create(options, hc)`). `hive.conf.resources`, if present, falls through `buildHmsHiveConf` as a non-`hive.*` key and is dropped entirely.

**Why the connector "cannot import" it.** `fe/fe-connector/fe-connector-paimon/pom.xml` depends on `fe-connector-spi`, `fe-connector-api`, `fe-thrift` (provided), paimon, hadoop-common, hive-common — **no `fe-common` and no `fe-core`**. So `CatalogConfigFileUtils`, `Config.hadoop_config_dir`, and `EnvUtils.getDorisHome()` are all unavailable to the connector. The file-resolution must happen on the FE side and be handed to the connector.

# Design

**Resolve the file FE-side; merge connector-side.** The connector already has the established pattern for "FE-owned config the connector needs": `ConnectorContext`. The JDBC driver-dir case routes `Config` values through `ConnectorContext.getEnvironment()` (`DefaultConnectorContext.buildEnvironment` → consumed by `PaimonConnector.resolveFullDriverUrl`). Filesystem resolution of `hive.conf.resources` is the same shape, but the *value* is a set of key/value pairs parsed from XML, not a single string — so a dedicated typed hook is cleaner than stuffing serialized XML into the env map.

Add one default method to the SPI:

```java
// ConnectorContext (fe-connector-spi)
/** Resolves comma-separated hive config resource file names (relative to the FE's
 *  hadoop_config_dir) into a flat key->value map. Default: empty (no file support). */
default Map<String, String> loadHiveConfResources(String resources) {
    return Collections.emptyMap();
}
```

- **fe-core override** (`DefaultConnectorContext`) implements it by calling the existing `CatalogConfigFileUtils.loadHiveConfFromHiveConfDir(resources)` and flattening the returned `HiveConf` (which is a Hadoop `Configuration`) into a `Map<String,String>` via iteration. This reuses the EXACT legacy loader (same `hadoop_config_dir`, same comma-split, same fail-if-missing), so file-resolution semantics are byte-identical to legacy and live entirely in fe-core where `Config`/filesystem access belongs.
- **connector merge** stays pure and fe-core-free: `buildHmsHiveConf` gains an overload that takes the pre-resolved `Map<String,String>` of file keys and applies them as the BASE of the `HiveConf`, before the user `hive.*` overrides — matching legacy precedence (file is base, user `hive.*` wins, then `hive.metastore.uris`, then timeout default). `PaimonConnector` resolves the file via the new context hook and passes the map in.

This is the report's preferred remediation ("route `hive.conf.resources` resolution through a `ConnectorContext` hook; FE loads the file via `CatalogConfigFileUtils` and passes resolved key/values into connector properties"). It keeps the no-fe-core-import rule intact: the connector never touches `CatalogConfigFileUtils`/`Config`/filesystem; it only receives an already-resolved map. `buildHmsHiveConf` stays PURE (map in, conf out) for offline unit testing — the impure file read sits behind the context hook.

**Right side for each piece:**
- File discovery + parsing (filesystem, `Config.hadoop_config_dir`, fail-on-missing): **fe-core** (`DefaultConnectorContext`), reusing `CatalogConfigFileUtils`.
- SPI surface: **fe-connector-spi** (`ConnectorContext`), default no-op so other connectors are unaffected.
- HiveConf assembly + precedence: **connector** (`PaimonCatalogFactory` / `PaimonConnector`), unchanged purity.

**Rejected alternative — bridge merges a legacy-built HiveConf:** would require the connector to receive a live `HiveConf` object across the plugin classloader boundary, re-introducing exactly the cross-loader `Configuration`/`HiveConf` identity hazard already flagged at `PaimonConnector.java:152-157`. Passing a plain `Map<String,String>` avoids that.

# Implementation Plan

**1. `fe/fe-connector/fe-connector-spi/.../ConnectorContext.java`** — add default hook (alongside `getEnvironment`):

```java
import java.util.Collections;
import java.util.Map;
...
/**
 * Resolves the catalog's {@code hive.conf.resources} (comma-separated hive-site.xml file
 * names under the FE's hadoop_config_dir) into a flat key->value map the connector can
 * overlay onto its HiveConf. The default returns empty (no external file support); the
 * fe-core context loads the files via CatalogConfigFileUtils, matching legacy HMS behavior.
 *
 * @throws RuntimeException if a referenced file is missing/unreadable (fail-loud, legacy parity)
 */
default Map<String, String> loadHiveConfResources(String resources) {
    return Collections.emptyMap();
}
```

**2. `fe/fe-core/.../connector/DefaultConnectorContext.java`** — implement it:

```java
@Override
public Map<String, String> loadHiveConfResources(String resources) {
    if (Strings.isNullOrEmpty(resources)) {
        return Collections.emptyMap();
    }
    HiveConf hc = CatalogConfigFileUtils.loadHiveConfFromHiveConfDir(resources); // legacy loader, fail-loud
    Map<String, String> out = new HashMap<>();
    for (Map.Entry<String, String> e : hc) {   // Configuration is Iterable<Map.Entry<String,String>>
        out.put(e.getKey(), e.getValue());
    }
    return out;
}
```
(new imports: `org.apache.doris.common.CatalogConfigFileUtils`, `org.apache.hadoop.hive.conf.HiveConf`, `com.google.common.base.Strings`.) Note `HiveConf extends Configuration implements Iterable<Map.Entry<String,String>>`, so the flatten loop is the standard idiom and gives effective (resolved) values.

**3. `fe/fe-connector/fe-connector-paimon/.../PaimonCatalogFactory.java`** — add an overload of `buildHmsHiveConf` that seeds file keys as the base; keep the existing 1-arg signature delegating with an empty map so all current call sites / tests compile unchanged:

```java
public static HiveConf buildHmsHiveConf(Map<String, String> props) {
    return buildHmsHiveConf(props, java.util.Collections.emptyMap());
}

public static HiveConf buildHmsHiveConf(Map<String, String> props, Map<String, String> hiveConfResources) {
    HiveConf hiveConf = new HiveConf();
    // External hive-site.xml (hive.conf.resources) as the BASE, resolved FE-side
    // (legacy HMSBaseProperties.checkAndInit line 197: load file first, user hive.* overrides win).
    if (hiveConfResources != null) {
        hiveConfResources.forEach(hiveConf::set);
    }
    // ... existing body unchanged (user hive.* verbatim now correctly OVERRIDE the file base) ...
}
```
Update the lines 349-360 Javadoc: the DEFERRED note becomes "loaded via `ConnectorContext.loadHiveConfResources` and overlaid as the base."

**4. `fe/fe-connector/fe-connector-paimon/.../PaimonConnector.java`** — in the HMS branch (lines 146-161) resolve the file and pass it in:

```java
case PaimonConnectorProperties.HMS: {
    Map<String, String> hiveConfFiles = context.loadHiveConfResources(
            PaimonCatalogFactory.firstNonBlank(properties, "hive.conf.resources"));
    HiveConf hc = PaimonCatalogFactory.buildHmsHiveConf(properties, hiveConfFiles);
    return createCatalogFromContext(CatalogContext.create(options, hc), flavor,
            "Failed to create Paimon catalog with HMS metastore");
}
```
(`"hive.conf.resources"` is a literal; consider a `PaimonConnectorProperties` constant for consistency with the existing key constants.) Scope: HMS branch only — legacy only loaded the file for the HMS flavor (DLF builds its own `HiveConf` from DLF keys, unaffected).

**Precedence verification (legacy parity):** legacy order = file (base) → user `hive.*` overrides → `hive.metastore.uris` → timeout default → kerberos-conditional keys. New order after fix = file (base, step added first) → user `hive.*` verbatim → uri/auth/kerberos/timeout. Identical: a key present in both the file and as a user `hive.*` prop resolves to the user value in both.

# Risk Analysis

- **Parity vs legacy:** file-resolution reuses the *same* `CatalogConfigFileUtils.loadHiveConfFromHiveConfDir`, so `hadoop_config_dir` base, comma-split, and fail-on-missing-file are identical. Precedence (file as base, user `hive.*` wins) matches `HMSBaseProperties.checkAndInit`. One subtle divergence to call out in tests: legacy keeps the loaded file as a live `HiveConf` and `addResource`s it (lazy, effective values resolved on read), whereas the fe-core hook eagerly flattens to a `Map` of effective values then re-`set`s them. For plain key/value `hive-site.xml` content these are equivalent; the only theoretical difference is XML `<final>` flags or variable-substitution edge cases, which are not connection-critical and not exercised by HMS catalogs in practice. Acceptable and strictly closer to legacy than today's total drop.
- **Shared-code blast radius:** `ConnectorContext.loadHiveConfResources` is a NEW default method returning empty — zero behavior change for every other connector (maxcompute, jdbc siblings, trino) and for the `RecordingConnectorContext` test double (inherits the no-op default unless a test overrides it). `DefaultConnectorContext` gains one method; no existing method touched. The new `buildHmsHiveConf(props)` 1-arg overload delegates, so all existing callers/tests (`PaimonCatalogFactoryTest` 5 HMS tests, `PaimonConnector`) compile and behave unchanged.
- **Edge cases:** (a) `hive.conf.resources` absent/blank → hook gets blank → returns empty map → behaves exactly as today (no regression). (b) Referenced file missing → `CatalogConfigFileUtils` throws `IllegalArgumentException` ("Config resource file does not exist") which propagates out of CREATE CATALOG — fail-loud, matching legacy (today it is silently ignored, which is the very bug). This makes a previously-silent misconfiguration loud; intended per the finding's "at least make the drop loud." (c) Trino/plugin isolated classloaders: not applicable — paimon's `DefaultConnectorContext` runs in fe-core's classloader where `Config`/filesystem are reachable; only the resolved `Map<String,String>` crosses into the connector, avoiding the `HiveConf`/`Configuration` cross-loader identity hazard noted at `PaimonConnector.java:152-157`.
- **Live-runtime caveat (unchanged by this fix):** the pre-existing B7 note that the live `metastore=hive` Thrift client is host-provided at cutover still stands; this fix only restores the file content into the `HiveConf` and does not alter the classloader story.

# Test Plan

## Unit Tests
All in the connector test dir; all PURE/offline (no live metastore).

**`PaimonCatalogFactoryTest` (new tests; FAIL before fix because the 2-arg overload + base-merge do not exist / file keys are dropped):**

1. `buildHmsHiveConfOverlaysResolvedHiveConfResourcesAsBase` — call `buildHmsHiveConf(props("uri","thrift://nn:9083"), map("hive.metastore.sasl.qop","auth-conf","hive.metastore.thrift.transport","custom"))`. Assert both file-only keys land in the `HiveConf`. WHY: encodes that connection-critical keys present *only* in the external `hive-site.xml` reach the catalog `HiveConf` (the exact failure scenario). MUTATION: dropping the file map (today's behavior) → red.
2. `buildHmsHiveConfUserHivePropOverridesFileResource` — file map `{"hive.metastore.uris":"thrift://FILE:9083"}` plus user prop `props("hive.metastore.uris","thrift://USER:9083","uri","thrift://nn:9083")`. Assert the effective `hive.metastore.uris` is the user/uri value, not the file value. WHY: encodes legacy precedence (file is base, user `hive.*` and the resolved `uri` win) — a test that can only pass if the file is applied FIRST. MUTATION: applying the file map AFTER the user keys → red.
3. `buildHmsHiveConfSingleArgUsesEmptyResources` — `buildHmsHiveConf(props("uri","thrift://nn:9083"))` still produces the same conf as before (uri + timeout default). WHY: proves the back-compat overload is a true no-op extension. MUTATION: 1-arg overload diverging → red.

**`RecordingConnectorContext` (extend harness):** add an overridable `Map<String,String> hiveConfResources` field and `loadHiveConfResources` override returning it (recording the requested `resources` string). This lets connector-level tests inject resolved file keys without touching the filesystem.

4. (connector-level, in a `PaimonConnector`-oriented test or extend `PaimonCatalogFactoryTest`'s scope via the recording context) `hmsBranchRoutesHiveConfResourcesThroughContext` — drive the HMS create path with a `RecordingConnectorContext` whose `loadHiveConfResources("hive-site.xml")` returns `{"hive.metastore.sasl.qop":"auth-conf"}`; assert the context was asked for exactly the `hive.conf.resources` value and (via a seam on the assembled `HiveConf`, or by asserting the recorded request string) that the connector wired the hook. WHY: proves the connector actually CALLS the FE hook for the HMS flavor and feeds the result into `buildHmsHiveConf` (intent: no silent drop), not merely that the pure builder works. MUTATION: HMS branch not calling `loadHiveConfResources` → red.

`DefaultConnectorContext.loadHiveConfResources` flatten logic is fe-core (filesystem-touching); covered by E2E rather than a connector UT, since the connector module has no `fe-common`/`Config` access to exercise the real loader offline. A focused fe-core UT writing a temp `hive-site.xml` under a `hadoop_config_dir` and asserting the flattened map is optional and belongs in fe-core's test tree, not the connector's.

## E2E Tests
Live-only / CI-skipped (real HMS required, gated like the existing `PaimonLiveConnectivityTest`): `CREATE CATALOG ... 'paimon.catalog.type'='hms','hive.conf.resources'='hive-site.xml'` where the `hive-site.xml` under `plugins/hadoop_conf/` carries a connection-critical key absent from the inline DDL (e.g. an alternate `hive.metastore.uris` or `hive.metastore.sasl.qop`); assert the catalog connects and a `SELECT`/`SHOW TABLES` succeeds using the file-sourced setting. This is live-only because it requires a real metastore + on-disk config dir and exercises the Thrift client that is host-provided only at cutover; it cannot run in the offline connector unit harness.

# Notes
Root cause confirmed firsthand against current code. Key correction vs the report's line references: the legacy loader `CatalogConfigFileUtils` lives in **fe-common** (not fe-core), but the paimon connector pom depends on neither, so the no-import constraint still forces the FE-side-resolve / connector-side-merge split. The cleanest, lowest-blast-radius fix is a new default-no-op `ConnectorContext.loadHiveConfResources` hook implemented in `DefaultConnectorContext` (reusing the exact legacy loader) plus a 2-arg `buildHmsHiveConf` overload that seeds the resolved keys as the HiveConf base — preserving legacy precedence and keeping the pure builder offline-testable.

---

# ✅ IMPL SUMMARY (2026-06-11)

**Status: DONE — connector build+UT green (PaimonCatalogFactoryTest 41/0 + PaimonHmsConfResWiringTest 1/0); fe-core compiles clean; imports clean; HEAD uncommitted.**

## Fix (SPI + fe-core bridge + connector; default-no-op so other connectors unaffected)
- `ConnectorContext.java` (fe-connector-spi): added `default Map<String,String> loadHiveConfResources(String resources)` → empty.
- `DefaultConnectorContext.java` (fe-core): override reuses the EXACT legacy loader `CatalogConfigFileUtils.loadHiveConfFromHiveConfDir` (same hadoop_config_dir / comma-split / fail-if-missing) and flattens the `HiveConf` (Iterable<Map.Entry>) to a `Map`. Imports: `CatalogConfigFileUtils`, `HiveConf`, guava `Strings`.
- `PaimonCatalogFactory.java`: new 2-arg `buildHmsHiveConf(props, hiveConfResources)` seeds the file keys as the HiveConf BASE, before user `hive.*` overrides (legacy precedence); 1-arg overload delegates with empty map (back-compat).
- `PaimonConnector.java`: HMS branch resolves `hive.conf.resources` via `context.loadHiveConfResources(...)` and passes it to the 2-arg builder. HMS-only (DLF builds its own HiveConf).

## Tests
- `PaimonCatalogFactoryTest` (3 new): file-keys-as-base, user-hive.*-overrides-file-base, 1-arg-back-compat.
- `PaimonHmsConfResWiringTest` (new): drives the connector HMS create path with `RecordingConnectorContext.failAuth=true` (fails fast at executeAuthenticated, AFTER the hook is called, BEFORE any metastore connection) → asserts `loadHiveConfResources("hive-site.xml")` was invoked. `RecordingConnectorContext` extended with the hook recorder.

## Correction discovered during impl
The design's planned test 2 used `hive.metastore.uris` for the precedence check, but that key is ALSO resolved separately via the `HMS_URI` alias (firstNonBlank, where the explicit `hive.metastore.uris` key out-ranks the `uri` alias), which muddied the assertion (initial run: expected `thrift://nn` got `thrift://USER`). Rewrote the test to use a non-uri key (`hive.metastore.sasl.qop`) so it cleanly isolates the file-base-vs-user-hive.* precedence. Production behavior was correct; only the test's expectation was wrong.

## Not run (per design)
- fe-core UT for the `DefaultConnectorContext.loadHiveConfResources` flatten (filesystem-touching) — covered by E2E; the flatten is a trivial loop over the proven legacy loader. fe-core compile verified.
- Live-e2e (gated): `CREATE CATALOG ... 'hive.conf.resources'='hive-site.xml'` with a connection-critical key only in the file.
