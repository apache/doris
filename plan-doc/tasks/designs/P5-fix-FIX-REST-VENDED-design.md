# Problem

A Paimon catalog created with `'type'='paimon','paimon.catalog.type'='rest'` against a REST server that vends **per-table temporary cloud-storage credentials** (e.g. DLF/Aliyun OSS or S3 STS tokens) returns no usable storage credentials to BE on the SPI read path. Any `SELECT` over such a table that lands on the **native ORC/Parquet reader** (the common case) sends BE a scan-range location-properties map with *no* valid `AWS_*` credentials, so the object-store client fails with access-denied / 403 and the data files are unreadable. Legacy (pre-SPI) Paimon read succeeded because it fetched the per-table vended token in `PaimonScanNode.doInitialize()` and pushed the normalized credentials to BE.

Scope clarification (from the review, confirmed): the JNI reader path is **not** broken — BE's `PaimonJniScanner` deserializes the `RESTTokenFileIO` (its `catalogContext`/`identifier`/`path`/`token` fields are non-transient) and self-serves credentials. Only **native-reader-eligible REST tables** lose credentials. Because native read is the default for ORC/Parquet, this is a BLOCKER.

# Root Cause (confirmed in current code)

The SPI scan path never extracts vended credentials from the live Paimon `Table`'s `RESTTokenFileIO`, and the only credential keys it forwards to BE are *static* catalog-level keys.

- `PaimonScanPlanProvider.getScanNodeProperties` (`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java:306-315`) emits BE storage properties **only** by copying static entries from the catalog `properties` map whose key starts with `hadoop.`/`fs.`/`dfs.`/`hive.`/`s3.`/`cos.`/`oss.`/`obs.`, re-keyed as `location.<key>`. There is zero per-table token extraction. The resolved live `Table` (with its `RESTTokenFileIO`) is in hand at `PaimonScanPlanProvider.java:265` (`resolveScanTable(paimonHandle)`) but its `fileIO()` is never consulted.
- `PluginDrivenScanNode.getLocationProperties` (`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java:307-317`) just strips the `location.` prefix and ships the remainder verbatim to BE as `params.setProperties(...)`.
- BE's native S3/object-store client (`be/src/util/s3_util.cpp:541-561`, keys defined at `:146-150`) consumes **only** normalized `AWS_ACCESS_KEY` / `AWS_SECRET_KEY` / `AWS_TOKEN` / `AWS_ENDPOINT` / `AWS_REGION`. It does **not** understand raw paimon token keys (`fs.oss.accessKeyId`, `s3.access-key`, …). So even the static `location.s3.*` passthrough would not produce working credentials without normalization — and the vended token is never fetched at all.

Legacy reference that the SPI path dropped:
- `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonScanNode.java:170-176` — in `doInitialize()` legacy calls `VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(metastoreProps, baseStorageMap, source.getPaimonTable())`, then `CredentialUtils.getBackendPropertiesFromStorageMap(...)` (`:176`) to get the BE-facing `AWS_*` map, returned to BE via `getLocationProperties()` (`:650-651`).
- `fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonVendedCredentialsProvider.java:48-68` — `extractRawVendedCredentials` pulls the raw token via `((RESTTokenFileIO) table.fileIO()).validToken().token()`.
- `fe/fe-core/src/main/java/org/apache/doris/datasource/credentials/AbstractVendedCredentialsProvider.java:43-83` + `CredentialUtils.java:55-87` — filter to cloud prefixes, run `StorageProperties.createAll(...)` (normalizes arbitrary token key shapes + derives region/endpoint), then `getBackendConfigProperties()` → the `AWS_*` map.

The normalization (`StorageProperties.createAll`) recognizes many key aliases and derives region from endpoint; it lives in `org.apache.doris.datasource.property.storage.*` (fe-core). The connector module **must not import fe-core**, so it cannot call `StorageProperties.createAll` directly. This is the core constraint shaping the design.

# Design

Restore legacy timing/scope (per-table token fetched at scan-plan time on the live, snapshot-pinned `Table`) while keeping the heavy `StorageProperties` normalization on the fe-core side, reached through a new **`ConnectorContext` SPI hook**. This mirrors how the connector already routes other fe-core-only concerns (`executeAuthenticated`, `sanitizeJdbcUrl`) through `ConnectorContext`.

Two-part split:
1. **Connector side (pure paimon SDK, no fe-core):** extract the raw vended token from the resolved `Table`'s `RESTTokenFileIO`. This is exactly the body of legacy `PaimonVendedCredentialsProvider.extractRawVendedCredentials`, but living in the connector and using only `org.apache.paimon.rest.RESTTokenFileIO` / `RESTToken` (both on the connector's compile classpath via `paimon-core`→`paimon-common`; verified `RESTTokenFileIO`/`RESTToken` present in `paimon-common-1.3.1.jar` / `paimon-hive-connector-3.1-1.3.1.jar`).
2. **fe-core bridge (via new `ConnectorContext.vendStorageCredentials(rawToken)` default hook):** take the raw token map and return the BE-facing normalized map by delegating to the *existing* `StorageProperties.createAll(...)` + `CredentialUtils.getBackendPropertiesFromStorageMap(...)` machinery. The default is a no-op (`return Collections.emptyMap()`); `DefaultConnectorContext` overrides it using the catalog's `CatalogProperty` (already available to `PluginDrivenExternalCatalog`, which constructs the context). The connector emits each returned `<k,v>` as `location.<k>`.

Why a `ConnectorContext` hook and not re-porting normalization into the connector:
- `StorageProperties.createAll` is large, alias-rich, and fe-core-resident; re-porting it violates "minimal change" and "no fe-core import," and would silently drift from the canonical normalization (the sibling P9 S3/OSS finding shows how partial re-ports lose keys).
- The hook keeps a single source of truth and matches the legacy data flow 1:1 (raw token → `StorageProperties.createAll` → `getBackendConfigProperties`).
- Timing/scope parity: the connector calls the hook inside `getScanNodeProperties` on the already-resolved, snapshot-pinned `Table` — same point legacy fetched it (per scan, per table).

Gating parity: legacy only vends for REST (`isVendedCredentialsEnabled` ⇔ `PaimonRestMetaStoreProperties`). The connector gates on the table actually carrying a `RESTTokenFileIO` (`table.fileIO() instanceof RESTTokenFileIO`), which is strictly equivalent for the read path and needs no metastore-type plumbing. Non-REST flavors return an empty raw map → hook not called → behavior unchanged.

Static-vs-vended precedence: legacy merges base storage map then overlays vended (`getStoragePropertiesMapWithVendedCredentials` replaces base when vended succeeds). The connector keeps emitting the existing static `location.*` keys, then overlays the vended `location.AWS_*` keys (vended wins on collision), preserving legacy semantics for hybrid catalogs.

# Implementation Plan

### 1. New SPI hook — `ConnectorContext.vendStorageCredentials`
File: `fe/fe-connector/fe-connector-spi/src/main/java/org/apache/doris/connector/spi/ConnectorContext.java`

Add a default method (no-op so all other connectors/tests are unaffected):
```java
/**
 * Normalizes raw per-table vended cloud-storage credentials (the token map a REST
 * catalog returns, e.g. fs.oss.accessKeyId / s3.access-key) into the BE-facing
 * storage-property map (AWS_ACCESS_KEY / AWS_SECRET_KEY / AWS_TOKEN / AWS_ENDPOINT /
 * AWS_REGION). The engine performs the same StorageProperties normalization it uses
 * for static catalog credentials. Returns an empty map when the input is empty or the
 * deployment has no normalization machinery.
 */
default Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) {
    return Collections.emptyMap();
}
```

### 2. fe-core bridge — `DefaultConnectorContext`
File: `fe/fe-core/src/main/java/org/apache/doris/connector/DefaultConnectorContext.java`

`DefaultConnectorContext` is constructed in `PluginDrivenExternalCatalog.createConnectorFromProperties` (`:148-150`), which has `catalogProperty`. Thread a `Supplier<CatalogProperty>` (or the two derived inputs) into the context and implement the override by reusing the *existing* legacy helpers (no new normalization logic):
```java
@Override
public Map<String, String> vendStorageCredentials(Map<String, String> rawVendedCredentials) {
    if (rawVendedCredentials == null || rawVendedCredentials.isEmpty()) {
        return Collections.emptyMap();
    }
    try {
        Map<String, String> filtered = CredentialUtils.filterCloudStorageProperties(rawVendedCredentials);
        if (filtered.isEmpty()) {
            return Collections.emptyMap();
        }
        List<StorageProperties> vended = StorageProperties.createAll(filtered);
        Map<StorageProperties.Type, StorageProperties> map = vended.stream()
                .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
        return CredentialUtils.getBackendPropertiesFromStorageMap(map);
    } catch (Exception e) {
        LOG.warn("Failed to normalize vended credentials", e);
        return Collections.emptyMap();   // fail soft, same as legacy provider
    }
}
```
Construction change: at `PluginDrivenExternalCatalog.java:150` pass the catalog property supplier into the `DefaultConnectorContext` ctor (new overload); `CatalogFactory.java:106` keeps the no-property overload (no vended support there — only the plugin-driven live path needs it). Note: this reuses `AbstractVendedCredentialsProvider`'s exact normalization steps; it does NOT add new credential logic. (Alternative, even smaller: call `PaimonVendedCredentialsProvider`/`VendedCredentialsFactory` indirectly is not possible here because the connector has already extracted the raw token; passing the raw map to `StorageProperties.createAll` is the precise tail of the legacy flow.)

### 3. Connector — extract token + emit normalized location keys
File: `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java`

Thread `ConnectorContext` into the provider:
- `PaimonConnector.getScanPlanProvider()` (`PaimonConnector.java:91-95`): pass `context` into a new `PaimonScanPlanProvider(properties, catalogOps, context)` ctor (keep existing 2-arg ctor for the offline unit tests that pass no context, or default `context=null`).

Add a pure static extractor (port of legacy `extractRawVendedCredentials`, paimon-SDK-only):
```java
static Map<String, String> extractVendedToken(Table table) {
    if (table == null) {
        return Collections.emptyMap();
    }
    FileIO fileIO = table.fileIO();
    if (!(fileIO instanceof RESTTokenFileIO)) {
        return Collections.emptyMap();
    }
    RESTToken token = ((RESTTokenFileIO) fileIO).validToken();
    Map<String, String> raw = token == null ? null : token.token();
    return raw == null ? Collections.emptyMap() : new HashMap<>(raw);
}
```
In `getScanNodeProperties`, after the existing static `location.*` loop (`PaimonScanPlanProvider.java:306-315`), overlay vended creds (only when a context is present):
```java
if (context != null) {
    Map<String, String> vendedBeProps =
            context.vendStorageCredentials(extractVendedToken(table));
    for (Map.Entry<String, String> e : vendedBeProps.entrySet()) {
        props.put("location." + e.getKey(), e.getValue());   // vended overlays static
    }
}
```
`table` here is `resolveScanTable(paimonHandle)` (already at `:265`), so the token is fetched on the live, snapshot-pinned table — legacy timing/scope.

### 4. Test fake plumbing
File: `fe/fe-connector/fe-connector-paimon/src/test/java/org/apache/doris/connector/paimon/FakePaimonTable.java`

`FakePaimonTable.fileIO()` currently throws (`:122-125`). Add a settable `FileIO fileIO` field (default `null`, with a `setFileIO(...)`), returning it from `fileIO()` so the scan tests can inject a hand-written `RESTTokenFileIO`-shaped double. (See Test Plan for the no-Mockito approach — the module forbids Mockito.)

Files touched (summary): `ConnectorContext.java` (SPI), `DefaultConnectorContext.java` + `PluginDrivenExternalCatalog.java` (fe-core bridge wiring), `PaimonScanPlanProvider.java` + `PaimonConnector.java` (connector), `FakePaimonTable.java` (test fake), plus the new/extended tests below.

# Risk Analysis

- **Parity vs legacy:** The normalization path (`filterCloudStorageProperties` → `StorageProperties.createAll` → `getBackendConfigProperties`) is *identical* to legacy; only the token-extraction site moves into the connector. The gate changes from "metastore is `PaimonRestMetaStoreProperties`" to "table's `fileIO` is `RESTTokenFileIO`" — equivalent for the read path (a REST catalog table yields a `RESTTokenFileIO`; non-REST yields a different `FileIO`), and strictly more precise (won't try to vend on a REST catalog table that happens to have a non-token FileIO). Fail-soft on any extraction/normalization error matches legacy (`AbstractVendedCredentialsProvider` returns null/empty on exception).
- **Shared-code blast radius:** The new `ConnectorContext` method is a `default` no-op, so every other connector (maxcompute, etc.) and every existing `ConnectorContext` implementation (including the test `RecordingConnectorContext`) compiles and behaves unchanged. The `DefaultConnectorContext` ctor gains an overload; the existing 2-arg/3-arg ctors are preserved, and `CatalogFactory.java:106` keeps using the no-vended overload, so non-plugin-driven paths are untouched. The `PluginDrivenExternalCatalog` change only adds a supplier argument.
- **Static-key precedence:** Overlaying vended after static means a catalog that *also* set static `s3.*` keys gets the vended token winning — matches legacy (`getStoragePropertiesMapWithVendedCredentials` replaces base with vended when vended succeeds). Note static `location.s3.*` keys remain raw (un-normalized) and are not consumed by BE's native S3 client — that's the separate sibling P9 finding and out of scope here; this fix does not regress it.
- **Token freshness / expiry:** `validToken()` returns a currently-valid token at plan time (legacy did the same in `doInitialize`). Long-running scans that outlive token TTL are a pre-existing legacy limitation, not introduced here.
- **Edge cases:** empty token map → empty overlay (no-op); non-REST FileIO → empty (no-op); `context == null` (offline unit tests using the 2-arg ctor) → skipped, so the offline harness keeps working; null `validToken()`/`token()` → empty, no NPE. JNI path unchanged (it never used `location.*` creds; BE self-serves from the serialized `RESTTokenFileIO`).
- **No-fe-core-import rule:** The connector only references `org.apache.paimon.rest.{RESTToken,RESTTokenFileIO}` and `org.apache.paimon.fs.FileIO` (paimon SDK) plus the `ConnectorContext` SPI. No `org.apache.doris.datasource.*` import added to the connector. Verified the classes resolve from `paimon-core`/`paimon-common` on the connector classpath.

# Test Plan

## Unit Tests
All connector-side tests must use **hand-written fakes, no Mockito** (the module's harness comment and `pom.xml` forbid Mockito). Provide a tiny hand-written `RESTTokenFileIO` double is not possible (it's a concrete class with no no-arg ctor and a final-ish `validToken`), so split tests at the two seams:

1. `PaimonScanPlanProviderTest.extractVendedToken_*` (new, in connector test dir):
   - Because `extractVendedToken` keys on `instanceof RESTTokenFileIO`, drive it with a `FakePaimonTable` whose `fileIO()` returns (a) `null`, (b) a plain hand-written `FileIO` double (not a `RESTTokenFileIO`), and assert an **empty** map — proving non-REST tables vend nothing (INTENT: never leak/attempt vended creds for non-REST). The positive `RESTTokenFileIO.validToken()` branch is covered by the bridge test + E2E (a `RESTTokenFileIO` cannot be hand-constructed offline without a live REST stack). FAILS before if extraction logic were wrong; the current code has no extraction at all, so these tests pin the new contract.

2. `PaimonScanPlanProviderTest.getScanNodeProperties_overlaysVendedCreds` (new): use a hand-written `ConnectorContext` double whose `vendStorageCredentials(raw)` returns a fixed map `{AWS_ACCESS_KEY=ak, AWS_SECRET_KEY=sk, AWS_TOKEN=tok, AWS_ENDPOINT=ep}` regardless of input, wire it through the 3-arg `PaimonScanPlanProvider` ctor with a `FakePaimonTable`. Assert the returned props contain `location.AWS_ACCESS_KEY=ak` … and that a colliding static key is overridden by the vended value. This FAILS before the fix (no overlay loop, no context param) and PASSES after. INTENT: the connector forwards normalized vended creds to BE under `location.*` with vended-wins precedence.

3. `PaimonScanPlanProviderTest.getScanNodeProperties_noContext_unchanged` (new): construct with the 2-arg ctor (`context == null`) and assert the property set equals the pre-fix static-only set — guards the offline path and proves no NPE / no behavior change when the hook is absent.

4. `DefaultConnectorContextVendTest` (new, fe-core test dir — fe-core may use Mockito/real objects): feed a raw OSS token map (`fs.oss.accessKeyId`/`fs.oss.accessKeySecret`/`fs.oss.securityToken`/`fs.oss.endpoint`, mirroring `PaimonVendedCredentialsProviderTest`) into `vendStorageCredentials` and assert the result contains the normalized BE keys `AWS_ACCESS_KEY`/`AWS_SECRET_KEY`/`AWS_TOKEN`/`AWS_ENDPOINT` (and that an empty input yields empty). This pins that the bridge reuses `StorageProperties.createAll` correctly and matches the legacy `PaimonVendedCredentialsProviderTest` + `getBackendPropertiesFromStorageMap` expectations (which already asserts `AWS_*` keys at `PaimonVendedCredentialsProviderTest.java:286-291`). FAILS before (method is a no-op default) and PASSES after.

5. `RecordingConnectorContext` (connector test fake) needs no change — it inherits the no-op default, confirming the SPI addition is backward compatible.

## E2E Tests
- A regression case under `regression-test/` analogous to the existing `test_paimon_s3.groovy`, but for a **REST/DLF catalog that vends per-table OSS/S3 credentials with no static `s3.*`/`oss.*` keys**, then `SELECT` a native-readable (ORC/Parquet) table and assert correct rows. This is **live-only / CI-skipped**: it requires a real Paimon REST server (or DLF) that issues vended STS tokens and a private OSS/S3 bucket — there is no offline double for `RESTTokenFileIO.validToken()` (it calls the REST server). Mark it gated behind the existing live-credential regression conf (same gating model as the live `PaimonLiveConnectivityTest`). The unit tests above cover the FE-side wiring deterministically; the E2E validates the end-to-end BE read with a real vended token.

---

# ✅ IMPL SUMMARY (2026-06-11)

**Status: DONE — connector UT green (PaimonScanPlanProviderTest 15/0); fe-core UT green (DefaultConnectorContextVendTest 2/0); fe-core compiles; imports clean; HEAD uncommitted.**

## Fix (SPI + fe-core bridge + connector; default-no-op so other connectors unaffected)
- `ConnectorContext.java` (fe-connector-spi): added `default Map<String,String> vendStorageCredentials(Map raw)` → empty.
- `DefaultConnectorContext.java` (fe-core): override replicates the EXACT legacy `AbstractVendedCredentialsProvider` tail — `CredentialUtils.filterCloudStorageProperties` → `StorageProperties.createAll` → `Collectors.toMap(StorageProperties::getType, identity)` → `CredentialUtils.getBackendPropertiesFromStorageMap`; fail-soft (empty) on any error. Added LOG + imports.
- `PaimonScanPlanProvider.java` (connector): 3-arg ctor adding `ConnectorContext context` (2-arg delegates with null for offline tests); pure static `extractVendedToken(Table)` (port of legacy `extractRawVendedCredentials`, paimon SDK only — gates on `fileIO() instanceof RESTTokenFileIO`); `getScanNodeProperties` overlays `context.vendStorageCredentials(extractVendedToken(table))` as `location.*` AFTER the static loop (vended wins on collision).
- `PaimonConnector.java`: `getScanPlanProvider()` passes `context` to the 3-arg ctor.

## Tests
- `PaimonScanPlanProviderTest` (3 new): extractVendedToken empty for null/non-REST FileIO (uses `LocalFileIO` as a real non-REST double); getScanNodeProperties overlays vended AWS_* (with vended-wins-on-collision); no-context path unchanged.
- `DefaultConnectorContextVendTest` (new fe-core): a raw OSS token normalizes to `AWS_ACCESS_KEY`/`AWS_SECRET_KEY`/`AWS_TOKEN`; empty/null → empty. Exercises the REAL StorageProperties normalization (the connector tests use a fake context).
- `FakePaimonTable` test fake: `fileIO()` now returns a settable field (was throw).

## Deviation from design (documented, simpler + lower blast-radius)
The design's "Construction change" said to thread a `Supplier<CatalogProperty>` into the `DefaultConnectorContext` ctor and change `PluginDrivenExternalCatalog`/`CatalogFactory`. **Not needed**: the shown impl (and the actual fix) uses ONLY the `rawVendedCredentials` param — `StorageProperties.createAll(filtered)` is self-contained. So NO ctor change, NO `PluginDrivenExternalCatalog`/`CatalogFactory` change. The connector's `context` is already a `DefaultConnectorContext` (built at `PluginDrivenExternalCatalog:150`), so `context.vendStorageCredentials(...)` resolves to the override regardless.

## Live-e2e (gated, NOT run): a REST/DLF catalog vending per-table OSS/S3 STS tokens (no static keys) → SELECT a native-readable table; needs a live REST server + private bucket (no offline double for `RESTTokenFileIO.validToken()`).
