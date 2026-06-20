# P5-fix-FIX-STATIC-CREDS-BE — design

> Task #2 (BLOCKER) of `plan-doc/task-list-P5-rereview2-fixes.md`. Finding **B-9** from `plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md` (3/3 CONFIRMED). The third credential seam (static→BE-scan), missed by both the prior round and the 8 fixes (review §9.3).
> Re-confirmed against current code (HEAD `20b19d19dd8`, post-#1) on 2026-06-11. Line numbers below are CURRENT.
> **User-signed scope** (D-048): full legacy-parity — replace the whole raw `location.*` passthrough loop with the engine's canonical backend-storage map.

## Problem

The paimon connector copies **static catalog-level storage credentials/config verbatim** into the BE scan-node properties. `PaimonScanPlanProvider.getScanNodeProperties:372-381` iterates the raw catalog `properties` and, for any key prefixed `s3.`/`cos.`/`oss.`/`obs.`/`hadoop.`/`fs.`/`dfs.`/`hive.`, emits `location.<rawkey> = <value>`. The fe-core bridge `PluginDrivenScanNode.getLocationProperties:307-317` only **strips** the `location.` prefix — it never normalizes. So BE's native ORC/Parquet (FILE_S3) reader receives `s3.access_key` / `oss.access_key` / … , but it parses **only** the canonical `AWS_ACCESS_KEY` / `AWS_SECRET_KEY` / `AWS_ENDPOINT` / `AWS_REGION` / `AWS_TOKEN` (BE `s3_util.cpp:146-150`).

Result on any **private** object-store bucket (S3 / OSS / COS / OBS): the native reader gets **no usable credentials → 403 / AccessDenied**. Public buckets and the JNI path are unaffected (the serialized paimon `Table` carries its own `FileIO`). The bare `AWS_*` / `access_key` form (no `s3.` prefix) is dropped entirely by the prefix filter.

This is distinct from the two credential seams already fixed:
- **FIX-STORAGE-CREDS** fixed the *catalog FileIO* seam (canonical → `fs.s3a.*` for paimon's own metadata reads).
- **FIX-REST-VENDED** fixed the *vended (REST) scan→BE* seam (`ConnectorContext.vendStorageCredentials`).
- **This (B-9)** is the *static catalog creds → BE scan* seam — review §9.3, seam #3.

## Root Cause

Legacy `PaimonScanNode.getLocationProperties:650-652` returns **only** `backendStorageProperties`, computed at `:176` as:
```java
backendStorageProperties = CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesMap);
```
where `storagePropertiesMap` is the catalog's **parsed** `StorageProperties` map (`getStoragePropertiesMap()`, vended-merged for REST). `getBackendPropertiesFromStorageMap` walks each `StorageProperties` and calls `getBackendConfigProperties()`, which yields the BE-canonical keys: `AbstractS3CompatibleProperties:106-120` emits `AWS_ENDPOINT/AWS_REGION/AWS_ACCESS_KEY/AWS_SECRET_KEY/AWS_TOKEN/…`; `HdfsProperties:163-200` emits the resolved `hadoop.`/`dfs.` config (user overrides **plus** legacy defaults like `ipc.client.fallback-to-simple-auth-allowed`).

The cutover replaced this single normalized call with a raw prefix-copy loop. The connector **cannot** import fe-core's `StorageProperties` / `CredentialUtils` (SPI boundary, `tools/check-connector-imports.sh`), so it had no access to the normalization — hence the raw copy.

## Design

**A new `ConnectorContext` SPI hook `getBackendStorageProperties()`** that returns exactly legacy's `getBackendPropertiesFromStorageMap(storagePropertiesMap)`. The engine already holds the authoritative parsed map: `DefaultConnectorContext.storagePropertiesSupplier` (= `catalogProperty.getStoragePropertiesMap()`) was wired in fix #1 for `normalizeStorageUri`. The connector replaces its raw passthrough loop with one overlay of this map; the existing `vendStorageCredentials` overlay stays **after** it (vended wins on collision — legacy precedence). This mirrors the `vendStorageCredentials` / `normalizeStorageUri` seams exactly (the task-list's recommended pattern) and is the single source of truth — no re-ported normalization that could drift.

**Why full replacement (D-048, user-signed), not object-store-only**: `getBackendPropertiesFromStorageMap` is the exact legacy `getLocationProperties()` value. For HDFS catalogs it is **strictly ≥** the current passthrough — `HdfsProperties.getBackendConfigProperties()` preserves every user `hadoop.`/`dfs.`/`fs.`/`juicefs.` override **and** adds the legacy-derived defaults the current loop drops (the review §211 MINOR, folded in for free). It also drops the `hive.*` keys the connector currently leaks — legacy never sends those to the scan location props, so dropping them **restores** parity. One SPI call replaces a fiddly prefix loop.

### SPI (`fe-connector-spi/ConnectorContext.java`) — new default no-op method
```java
/** Returns the catalog's static storage credentials/config normalized to BE-canonical scan
 *  properties (AWS_* for object stores, hadoop/dfs for HDFS) — the engine runs the same
 *  CredentialUtils.getBackendPropertiesFromStorageMap legacy/iceberg/hive use. BE's native reader
 *  only understands these canonical keys; a connector that copies raw catalog aliases (s3.access_key,
 *  oss.access_key, …) to BE gets no usable creds (403 on private buckets). The connector cannot do
 *  this itself (must not import fe-core StorageProperties). Default = empty, so every other connector
 *  is unaffected. */
default Map<String, String> getBackendStorageProperties() { return Collections.emptyMap(); }
```

### fe-core impl (`DefaultConnectorContext.java`) — real normalization
```java
@Override
public Map<String, String> getBackendStorageProperties() {
    // Mirror legacy PaimonScanNode.getLocationProperties(): the catalog's parsed StorageProperties
    // map -> BE-canonical keys (AWS_* / hadoop / dfs). Single source of truth (the SAME
    // getBackendPropertiesFromStorageMap legacy/iceberg/hive use), so no drift. Empty when the
    // catalog wires no storage map (non-plugin ctors; local-FS warehouse) -> no overlay, parity.
    return CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesSupplier.get());
}
```
`CredentialUtils` is already imported (used by `vendStorageCredentials`). The `storagePropertiesSupplier` field/ctor already exist (added in #1). **No ctor change.** Fail-behavior: `getBackendPropertiesFromStorageMap` on the parsed map does not throw (the map is already validated at catalog creation); an empty map yields an empty result (no overlay) — correct for credential-less warehouses, unlike `normalizeStorageUri` which must fail-loud on an un-normalizable *path*.

### connector (`PaimonScanPlanProvider.getScanNodeProperties`) — replace the raw loop
Replace the `for (entry : properties) { if (prefix s3./oss./… /hive.) props.put("location."+key, val) }` loop (`:372-381`) with:
```java
// Static catalog-level storage credentials/config, normalized to BE-canonical keys (AWS_* for
// object stores, hadoop/dfs for HDFS). Ports legacy PaimonScanNode.getLocationProperties() =
// getBackendPropertiesFromStorageMap(storagePropertiesMap); BE's native reader only understands the
// canonical keys, so the raw catalog aliases (s3.access_key, …) must be translated before they
// leave FE. The connector cannot import fe-core StorageProperties -> delegates to the
// ConnectorContext seam. Empty when no context (offline unit tests) -> no storage props emitted
// (never the broken raw aliases).
if (context != null) {
    for (Map.Entry<String, String> e : context.getBackendStorageProperties().entrySet()) {
        props.put("location." + e.getKey(), e.getValue());
    }
}
```
The vended overlay (`:388-393`) stays immediately after — vended overlays static, wins on key collision (legacy precedence preserved). No new connector imports (`Map`/`LinkedHashMap` already imported) → import-gate stays clean.

## Implementation Plan
1. `ConnectorContext.java`: add `getBackendStorageProperties()` default returning `Collections.emptyMap()`.
2. `DefaultConnectorContext.java`: add the `getBackendStorageProperties()` override (one line; reuses the existing supplier + `CredentialUtils` import).
3. `PaimonScanPlanProvider.java`: replace the static prefix-copy loop with the `getBackendStorageProperties()` overlay (context-gated).
4. Tests (below). Build `:fe-core -am` (SPI + fe-core) then `:fe-connector-paimon -am`.
5. `tools/check-connector-imports.sh` must stay clean.

## Risk Analysis
- **Regression on public/`s3://`/`hdfs://` warehouses**: for a correctly-configured catalog, `getStoragePropertiesMap()` holds the matching `StorageProperties`, so `getBackendPropertiesFromStorageMap` produces the same canonical keys legacy produces. Legacy ships exactly this map → parity. Public buckets get the same `AWS_*` (possibly empty creds + anonymous provider) as legacy.
- **HDFS catalogs**: full replacement is strictly ≥ the old passthrough (preserves user `hadoop.`/`dfs.`/`fs.`/`juicefs.` + adds legacy defaults). Behavioral delta is an *improvement* matching legacy. The only dropped keys are `hive.*`, which legacy never sent to scan location props.
- **Empty storage map** (local-FS warehouse, or non-plugin ctor): returns empty → no overlay. Legacy `getBackendPropertiesFromStorageMap({})` is also empty → parity. BE reads local files without creds.
- **No-context (offline unit tests only)**: the static overlay is skipped (gated, like the vended overlay) → no `location.*` storage props. Production always wires the context (`PaimonConnector:93`), so this only affects unit tests. The old offline behavior (emitting raw `location.s3.*`) was the *bug* — emitting nothing offline is correct.
- **Vended precedence**: vended overlay runs after the static overlay (unchanged), so vended still wins on collision. For REST catalogs the static map may lack keys; the per-table vended overlay supplies them — same two-step structure as today, only the static step is fixed.
- **`AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS` edge (investigated → non-issue)**: unlike legacy (which merges vended INTO the static `StorageProperties` then normalizes ONCE, so keys are present before the provider-type is computed), the connector normalizes static and vended in two steps. So a REST catalog that *also* carries a static object-store endpoint **without** static keys could have the static overlay emit `AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS` (blank-key branch of `AbstractS3CompatibleProperties:124-128`), which the vended overlay (carrying keys → no provider-type) would not clear. **Verified harmless against BE**: both `s3_util.cpp` credential providers (`_get_aws_credentials_provider_v1:383-389`, `_v2:448-455`) check explicit `ak`/`sk` **first** and return `SimpleAWSCredentialsProvider`, never reaching the `Anonymous` branch when keys are present. So the vended keys always win on BE; no regression. (For the primary B-9 case — static catalog WITH keys — the provider-type is null and never emitted.)
- **Other connectors**: identity (empty) SPI default; only paimon calls it. Zero impact on es/jdbc/maxcompute/trino. (hive/hudi connectors carry the same latent raw-passthrough pattern but are out of scope here.)

## Test Plan

### Unit Tests
1. **`DefaultConnectorContextBackendStoragePropsTest` (fe-core)** — build a real OSS `StorageProperties` map via `StorageProperties.createAll({oss.endpoint, oss.access_key, oss.secret_key})` (same machinery a real catalog uses), construct `DefaultConnectorContext` with it, assert `getBackendStorageProperties()` carries `AWS_ACCESS_KEY=ak` / `AWS_SECRET_KEY=sk` / a non-blank `AWS_ENDPOINT`, and carries **no** raw `oss.access_key` key. Assert the no-supplier ctor (`new DefaultConnectorContext("c",1L)`) returns empty. *Encodes WHY: BE only consumes canonical AWS_*; mutation = returning the raw oss.* keys or the no-op default → red.*
2. **`PaimonScanPlanProviderTest` (connector)** — three changes:
   - **new** `getScanNodePropertiesNormalizesStaticCreds`: connector `properties` holds the raw `s3.access_key`; a context returns canonical `{AWS_ACCESS_KEY,AWS_SECRET_KEY,AWS_ENDPOINT}` from `getBackendStorageProperties()`. Assert `location.AWS_ACCESS_KEY` etc. present **and** `location.s3.access_key` **absent** (the raw alias is no longer leaked). *WHY: the B-9 bug is the raw alias reaching BE; mutation = re-introducing the raw passthrough → red.*
   - **modify** `getScanNodePropertiesOverlaysVendedCreds`: static now comes from `getBackendStorageProperties()` (`{AWS_ACCESS_KEY=static-ak, AWS_ENDPOINT=static-ep}`); vended `{AWS_ACCESS_KEY=vended-ak, AWS_SECRET_KEY, AWS_TOKEN, AWS_ENDPOINT=vended-ep}`. Assert vended wins the `AWS_ACCESS_KEY`/`AWS_ENDPOINT` collision and adds `AWS_SECRET_KEY`/`AWS_TOKEN`. *WHY: vended overlays static (legacy precedence); mutation = overlaying static after vended → red.*
   - **modify** `getScanNodePropertiesNoContextUnchanged` → `getScanNodePropertiesNoContextNoStorageProps`: 2-arg ctor (no context), raw `s3.endpoint` in props. Assert **no** `location.*` storage key is emitted (no NPE; the broken raw alias is never shipped). *WHY: the connector cannot normalize without the engine seam; mutation = NPE on null context, or re-adding the raw passthrough → red.*

### E2E Tests (CI-gated — note, do not claim run)
- `test_paimon_*` native-read suites over a **private** S3/OSS bucket (static `s3.access_key`/`oss.access_key` catalog): `SELECT *` over raw parquet/orc must return rows (not 403). Requires live private-bucket creds → CI-gated.

## SPI / logs
- New SPI method `ConnectorContext.getBackendStorageProperties` → register in `plan-doc/01-spi-extensions-rfc.md` (§22 / E14).
- User-signed scope decision (full legacy-parity replacement) → `plan-doc/decisions-log.md` D-048.
- No deviation: this is exact legacy parity (unlike #1's static-vs-vended scope note).

## Result (2026-06-11 — implemented + verified)
- **Implemented exactly as designed**: SPI `ConnectorContext.getBackendStorageProperties` (empty default); `DefaultConnectorContext` override = `CredentialUtils.getBackendPropertiesFromStorageMap(storagePropertiesSupplier.get())` (reuses the #1-wired supplier + existing `CredentialUtils` import — **no ctor change**); `PaimonScanPlanProvider.getScanNodeProperties` replaces the raw prefix-copy loop with a context-gated overlay of that map (vended overlay stays after → vended wins).
- **ANONYMOUS-leak edge investigated → non-issue**: traced to BE `s3_util.cpp` — both credential providers (`_v1:383-389`, `_v2:448-455`) prefer explicit `ak`/`sk` over `cred_provider_type`, so a static-leaked `AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS` is never consulted when vended keys are present. No regression; no deviation logged.
- **Build + UT (green)**: `mvn test -pl :fe-core -am -Dtest=DefaultConnectorContext*` → `DefaultConnectorContextBackendStoragePropsTest` 2/0/0 (new), `DefaultConnectorContextNormalizeUriTest` 4/0/0 + `DefaultConnectorContextVendTest` 2/0/0 (unbroken). `mvn test -pl :fe-connector-paimon -am` → BUILD SUCCESS, module **217/0/0** (1 CI-gated skip); `PaimonScanPlanProviderTest` 18→19. Checkstyle clean (build-bound). `tools/check-connector-imports.sh` clean.
- **Fail-before/pass-after proven**: reverted the connector main change → `getScanNodePropertiesNormalizesStaticCreds` (AWS_ACCESS_KEY null) + `getScanNodePropertiesNoContextNoStorageProps` (raw alias shipped) go **red**; restored → green. (The 3rd test pins vended precedence, orthogonal — stays green either way.)
- **Tests added/changed**: fe-core `DefaultConnectorContextBackendStoragePropsTest` (OSS static creds → AWS_*, raw alias absent; no-supplier → empty); connector `PaimonScanPlanProviderTest` (+new `getScanNodePropertiesNormalizesStaticCreds`; modified `getScanNodePropertiesOverlaysVendedCreds` to canonical-key collision; renamed `getScanNodePropertiesNoContextUnchanged`→`getScanNodePropertiesNoContextNoStorageProps`).
- **Not run (CI-gated)**: live private-bucket (S3/OSS) native-read e2e — noted as gated, not executed.
- Logged: SPI RFC §22 (E14), decisions-log D-048 (full legacy-parity scope, user-signed).
