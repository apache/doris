# P5-fix-JDBC-DRIVER-URL — design

> **Finding**: B-8a (functional BLOCKER) + B-8b (security) from `reviews/P5-paimon-rereview2-2026-06-11.md`.
> **Task**: #4 in `task-list-P5-rereview2-fixes.md`. **Flavor scope**: JDBC metastore only.
> **Re-confirmed against current code (2026-06-11, HEAD `667f779af04`)** — all line numbers below are CURRENT (the review's `:549-565` etc. had drifted after #3 added ~200 lines to `PaimonScanPlanProvider.java`).

---

## Problem

A paimon catalog with `paimon.catalog.type=jdbc` and a dynamic JDBC driver (`driver_url`):

- **B-8a (functional)** — native/JNI scan fails. The connector forwards the JDBC driver location to BE
  **raw** (a bare `mysql.jar`) and **drops the `paimon.jdbc.*` alias** form, so:
  - `jdbc.driver_url=mysql.jar` → BE does `new URL("mysql.jar")` → `MalformedURLException` (BE
    `JdbcDriverUtils.registerDriver:42`).
  - `paimon.jdbc.driver_url=…` → silently dropped before it ever reaches BE → BE has no driver.
- **B-8b (security)** — `driver_url` is loaded into the **FE JVM** (`URLClassLoader` in
  `registerJdbcDriver`) and shipped to BE with **no** format / allow-list / secure-path validation, and a
  **stale disclaimer** claims the path is unreachable ("paimon is not in `SPI_READY_TYPES`") — false since
  the B7 cutover added paimon to `SPI_READY_TYPES`.

## Root Cause

### B-8a — `PaimonScanPlanProvider.getBackendPaimonOptions()` (`:611-627`)
```java
for (Map.Entry<String, String> entry : properties.entrySet()) {
    String key = entry.getKey();
    if (key.startsWith("jdbc.") || key.equals("warehouse")
            || key.equals("uri") || key.equals("metastore")
            || key.equals("catalog-key")) {
        options.put(key, entry.getValue());          // (1) RAW value — no resolution
    }                                                 // (2) startsWith("jdbc.") DROPS paimon.jdbc.*
}
```
These options are JSON-encoded into `paimon.options_json` (`:380-396`) and sent to BE. BE
`PaimonJdbcDriverUtils.registerDriverIfNeeded` reads `paimon.jdbc.driver_url` **or** `jdbc.driver_url`
(both aliases) then `JdbcDriverUtils.registerDriver` does `new URL(driverUrl)` — which **requires** a
scheme-bearing URL (`file://`/`http://`/`https://`).

**Legacy parity** — `PaimonJdbcMetaStoreProperties.getBackendPaimonOptions:164-176` emits exactly two
keys, **resolved**:
```java
backendPaimonOptions.put(JDBC_DRIVER_URL,   JdbcResource.getFullDriverUrl(driverUrl)); // resolved
backendPaimonOptions.put(JDBC_DRIVER_CLASS, driverClass);
```

### B-8b — `PaimonConnector`
- No `preCreateValidation` override (the connector uses `PaimonConnectorProvider.validateProperties` →
  `PaimonCatalogFactory.validate`, which has **no** driver-url security check).
- `resolveFullDriverUrl:232-246` resolves a bare name to `file://{jdbc_drivers_dir}/{name}` but performs
  **no** validation; `registerJdbcDriver:257-269` feeds the result straight into a `URLClassLoader`.
- Stale disclaimer comment `:225-230`. Paimon **is** in `SPI_READY_TYPES` (`CatalogFactory.java:51`).

## SPI seam (no new surface)

Both hooks already exist and are wired:
- `Connector.preCreateValidation(ConnectorValidationContext)` — default no-op; called by
  `PluginDrivenExternalCatalog` during CREATE CATALOG for every plugin catalog (before `testConnection`).
- `ConnectorValidationContext.validateAndResolveDriverPath(driverUrl)` →
  `DefaultConnectorValidationContext` → `JdbcResource.getFullDriverUrl` (format + `checkCloudWhiteList`
  vs `jdbc_driver_url_white_list` + `jdbc_driver_secure_path`). **Reference impl**:
  `JdbcDorisConnector.preCreateValidation:129-160` (calls `validateAndResolveDriverPath`, then checksum,
  then BE connectivity test).

> NOTE the stale comment's "cf. `sanitizeJdbcUrl`" is the **wrong** hook — `ConnectorContext.sanitizeJdbcUrl`
> sanitizes a JDBC **connection** URL (`jdbc:mysql://…`), not the **driver-jar** path. The driver-jar
> security hook is `validateAndResolveDriverPath`.

**Config defaults are permissive** (`jdbc_driver_secure_path="*"`, `jdbc_driver_url_white_list={}`), so
B-8b is **hardened-config parity** (legacy also loads any jar by default), not a default-exploitable hole.
B-8a is the hard functional blocker. Per the task-list, **both fold into one fix**.

## Design (connector-only; zero new SPI)

### Part A — B-8a functional (resolution + alias) — `PaimonScanPlanProvider.getBackendPaimonOptions()`
Keep the existing forwarding loop (preserves `uri`/`jdbc.user`/`jdbc.password`/`warehouse`/raw `jdbc.*` —
unchanged, currently-working). **After** it, emit the canonical resolved driver keys, overriding any raw
`jdbc.driver_url`/`jdbc.driver_class` the loop copied:
```java
String driverUrl = PaimonCatalogFactory.firstNonBlank(properties, PaimonConnectorProperties.JDBC_DRIVER_URL);
if (StringUtils.isNotBlank(driverUrl)) {
    Map<String, String> env = context != null ? context.getEnvironment() : Collections.emptyMap();
    options.put("jdbc.driver_url", PaimonCatalogFactory.resolveDriverUrl(driverUrl, env));   // resolved
    String driverClass = PaimonCatalogFactory.firstNonBlank(properties, PaimonConnectorProperties.JDBC_DRIVER_CLASS);
    if (StringUtils.isNotBlank(driverClass)) {
        options.put("jdbc.driver_class", driverClass);
    }
}
```
- `firstNonBlank(JDBC_DRIVER_URL)` reads **both** `paimon.jdbc.driver_url` and `jdbc.driver_url`.
- Emits the canonical `jdbc.driver_url`/`jdbc.driver_class` keys (BE accepts both alias forms; canonical
  matches legacy).

### Part B — extract the shared resolver — `PaimonCatalogFactory`
Move the resolution body out of `PaimonConnector.resolveFullDriverUrl` into a **pure static**
`PaimonCatalogFactory.resolveDriverUrl(String driverUrl, Map<String,String> env)` (no behavior change), so
the FE-registration path and the BE-options path resolve **identically** (correctness, not just DRY — a
divergence would register one jar in FE and request a different path on BE). `PaimonConnector.resolveFullDriverUrl`
becomes a thin delegate `return PaimonCatalogFactory.resolveDriverUrl(driverUrl, context.getEnvironment());`.

### Part C — B-8b security — `PaimonConnector.preCreateValidation(ConnectorValidationContext)`
```java
@Override
public void preCreateValidation(ConnectorValidationContext ctx) throws Exception {
    if (!PaimonConnectorProperties.JDBC.equalsIgnoreCase(PaimonCatalogFactory.resolveFlavor(properties))) {
        return;
    }
    String driverUrl = PaimonCatalogFactory.firstNonBlank(properties, PaimonConnectorProperties.JDBC_DRIVER_URL);
    if (StringUtils.isNotBlank(driverUrl)) {
        // Enforce FE format / jdbc_driver_url_white_list / jdbc_driver_secure_path at CREATE CATALOG.
        // Throws -> CREATE CATALOG fails. Mirrors JdbcDorisConnector.preCreateValidation.
        ctx.validateAndResolveDriverPath(driverUrl);
    }
}
```
Do **not** `storeProperty` the resolved URL back (parity: legacy keeps the raw property and resolves
on-demand; storing would change `SHOW CREATE CATALOG` display and diverge from the JDBC reference
connector, which stores only the checksum, never a mutated `driver_url`).

### Part D — cleanup
Replace the stale disclaimer comment `:225-230` with an accurate note (validation enforced at
`preCreateValidation`; BE-bound resolution in `getBackendPaimonOptions`; paimon is in `SPI_READY_TYPES`).

## Scope boundary (deliberate — Rule 2 surgical)

- **Only `driver_url` + `driver_class`** get alias+resolution — this is the exact legacy
  `getBackendPaimonOptions` parity (it emits only those two). The pre-existing forwarding of
  `uri`/`jdbc.user`/`jdbc.password`/`warehouse`/`catalog-key`/raw `jdbc.*` is left **unchanged**.
- The `paimon.jdbc.user`/`paimon.jdbc.password`/`paimon.jdbc.uri` **BE-side** alias handling (same
  `startsWith` filter would drop them) is a **separate pre-existing** behavior **not flagged by B-8a** and
  not part of legacy `getBackendPaimonOptions` → **out of scope** (logged as a watch item, not fixed here,
  to avoid speculative scope creep). The **FE catalog** already normalizes those aliases via
  `buildCatalogOptions` (`PaimonCatalogFactoryTest.jdbcSetsMetastoreUriUserAndRawJdbcKeys`).
- **No** validation added to the FE `maybeRegisterJdbcDriver`/`resolveFullDriverUrl` path — the
  `ConnectorValidationContext` hook isn't available there, and `preCreateValidation` gates catalog
  creation before that path runs for any new catalog. Pre-existing catalogs reloaded after restart =
  pre-existing gap, out of scope.

## Risk Analysis

1. **Resolution divergence (low)** — the connector resolver is a simplified subset of legacy
   `getFullDriverUrl` (no file-existence / old-`jdbc_drivers/` fallback / cloud download). For the common
   case (`mysql.jar`, default dir) both yield `file://$DORIS_HOME/plugins/jdbc_drivers/mysql.jar`. A jar
   present only in the legacy old dir resolves to the new dir and BE fails to find it. **Pre-existing**
   simplification already used by the FE path; reused unchanged. → log in deviations-log.
2. **Fail-fast at CREATE (intended)** — `validateAndResolveDriverPath` requires a bare-name jar to exist
   at CREATE CATALOG (was lazy at first scan). This is stricter but **correct** and matches the JDBC
   connector. A CI-gated e2e creating a JDBC catalog without the jar present would now fail at CREATE
   instead of first scan (it would have failed either way).
3. **No effect on non-JDBC flavors** — both Part A and Part C are gated on `metastore==jdbc` / `driverUrl`
   present. filesystem/hms/rest/dlf unchanged.
4. **`context==null` (offline)** — Part A guards `context != null`; resolver falls back to
   `doris_home="."`. Part C receives the `ConnectorValidationContext` as a method param (never null on the
   real path; tests pass a fake).

## Implementation Plan

1. `PaimonCatalogFactory`: add `public static String resolveDriverUrl(String driverUrl, Map<String,String> env)`
   (body moved verbatim from `PaimonConnector.resolveFullDriverUrl`).
2. `PaimonConnector`: `resolveFullDriverUrl` delegates to the static; add `preCreateValidation` override
   (Part C); replace stale comment (Part D).
3. `PaimonScanPlanProvider`: extend `getBackendPaimonOptions` (Part A); make it **package-private** for the
   unit test.
4. Tests (below). Build `-pl :fe-connector-paimon -am`; checkstyle; import-gate.

## Test Plan

### Unit (connector — no fe-core)
**`PaimonScanPlanProviderTest`** (direct `getBackendPaimonOptions`, package-private):
- `resolvesBareDriverUrl` — jdbc flavor + `jdbc.driver_url=mysql.jar` → emitted `jdbc.driver_url`
  `startsWith("file://")` && `endsWith("mysql.jar")`. **Fail-before**: equals raw `mysql.jar`.
- `honorsPaimonJdbcAlias` — jdbc flavor + `paimon.jdbc.driver_url=mysql.jar` +
  `paimon.jdbc.driver_class=com.mysql.cj.jdbc.Driver` → `jdbc.driver_url` present (resolved) +
  `jdbc.driver_class=com.mysql.cj.jdbc.Driver`. **Fail-before**: both absent (alias dropped by filter).
- `preservesSchemeUrl` — `jdbc.driver_url=file:///opt/d/mysql.jar` → unchanged.
- `nonJdbcFlavorEmpty` — filesystem flavor → empty map (regression guard).

**New `PaimonConnectorPreCreateValidationTest`** (recording `ConnectorValidationContext` fake):
- jdbc + `jdbc.driver_url` → `validateAndResolveDriverPath` called once w/ the url. **Fail-before**: not
  called (no override).
- jdbc + `paimon.jdbc.driver_url` alias → called once (alias honored).
- non-jdbc flavor → not called.
- jdbc, no driver_url → not called.
- fake throws (disallowed url) → `preCreateValidation` propagates.

### E2E (CI-gated — DO NOT claim it ran)
`regression-test/suites/.../test_paimon_jdbc_catalog*`: JDBC catalog with bare `driver_url=mysql.jar` in
`plugins/jdbc_drivers` + native ORC/Parquet read → BE registers the driver (no `MalformedURLException`),
rows correct. Gate: requires a live JDBC metastore + driver jar.

## Decisions / logs to update
- **No new SPI** → task-list "SPI? = maybe" resolves to **no**; note in `01-spi-extensions-rfc.md` that
  the fix reuses existing `preCreateValidation` + `validateAndResolveDriverPath` (no surface change).
- `deviations-log.md`: simplified resolver vs legacy `getFullDriverUrl` (risk #1); BE-side
  `paimon.jdbc.{user,password,uri}` alias handling out of scope (watch item).
- No user-signable decision required (in-scope blocker, existing hooks). If the simplified-resolver
  deviation or the alias scope-out needs sign-off, surface before commit.
