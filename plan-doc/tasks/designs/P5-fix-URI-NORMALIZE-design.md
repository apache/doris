# P5-fix-FIX-URI-NORMALIZE — design

> Task #1 (BLOCKER) of `plan-doc/task-list-P5-rereview2-fixes.md`. Findings **B-7DF** (data-file path) + **B-7DV** (deletion-vector path) from `plan-doc/reviews/P5-paimon-rereview2-2026-06-11.md`.
> Re-confirmed against current code (HEAD `98a73bf7692`) + 4-area recon workflow + adversarial synthesis (2026-06-11). Line numbers below are CURRENT.

## Problem

The paimon connector sends native ORC/Parquet **data-file** paths and **deletion-vector (DV)** paths to BE **without URI scheme normalization**. The paimon SDK emits paths in the warehouse's native scheme (`oss://`, `cos://`, `obs://`, `s3a://`, or the OSS `bucket.endpoint` authority form). BE's file factory is **scheme-dispatched** and its S3 reader only recognizes canonical `s3://`. Result on any S3-compatible (non-AWS) warehouse:

- **B-7DF (data-file)**: native ORC/Parquet read **fails outright** — `S3URI::parse` rejects `oss://…`.
- **B-7DV (deletion-vector)**: BE cannot open the DV index → **deleted rows silently reappear** (merge-on-read corruption — the more dangerous failure: wrong rows, not a hard error).

Pure `s3://` / `hdfs://` warehouses are unaffected (their scheme is already canonical). JNI read path is unaffected (the serialized paimon `Table` carries its own `FileIO`).

## Root Cause

Legacy `PaimonScanNode` normalizes **both** paths through the **2-arg normalizing** factory `LocationPath.of(path, storagePropertiesMap)` → `StorageProperties.validateAndNormalizeUri()` (e.g. `OSSProperties.validateAndNormalizeUri` rewrites `oss://bucket.endpoint/p` → `oss://bucket/p`, then the S3-compatible base rewrites scheme → `s3://`):
- data-file: `datasource/paimon/source/PaimonScanNode.java:443`
- DV: `…/PaimonScanNode.java:296-297` (`…toStorageLocation().toString()`)

The cutover dropped this. The two paths now reach BE raw via **two structurally different mechanisms**:

1. **Data-file path** (3-hop chain, all raw):
   - `PaimonScanPlanProvider.java:270` — `.path(file.path())` stores the raw paimon-SDK path into `PaimonScanRange`.
   - `PluginDrivenSplit.java:65-68` — `buildPath()` calls the **single-arg** `LocationPath.of(pathStr)` (`LocationPath.java:163-169`), which sets `normalizedLocation = location` verbatim, `storageProperties = null` — **no normalization**.
   - `FileQueryScanNode.java:568` — `rangeDesc.setPath(fileSplit.getPath().toStorageLocation().toString())`; `toStorageLocation()` (`LocationPath.java:404`) wraps the raw `normalizedLocation`. **This is the only writer of the data-file path to BE thrift.**

2. **DV path** (connector-baked into thrift):
   - `PaimonScanPlanProvider.java:282` — `builder.deletionFile(df.path(), …)` stores the raw DV path.
   - `PaimonScanRange.java:194` — `deletionFile.setPath(deletionPath)` writes it straight into `TPaimonDeletionFileDesc`, **inside the connector's `populateRangeParams`**, which fe-core invokes opaquely (`PluginDrivenScanNode.java:762`). **fe-core never sees the DV path as a separable value.**

The connector **cannot** import fe-core's `LocationPath` / `StorageProperties` (SPI boundary, enforced by `tools/check-connector-imports.sh`).

## Design

**A new `ConnectorContext` SPI normalization hook, called by the connector at both raw sites.** This is the only seam that fixes **both** paths with one uniform mechanism. A fe-core-bridge-only fix (normalize in `PluginDrivenSplit.buildPath`) is **impossible for the DV path** — the connector bakes it into thrift before fe-core can intercept it. Mixing two mechanisms (bridge for data-file, SPI for DV) would be inconsistent; the SPI hook covers both and keeps format-specific thrift construction in the connector (the established design, `PluginDrivenScanNode.java:761`). This mirrors the existing `ConnectorContext.vendStorageCredentials` credential seam (the task-list's recommended pattern) exactly.

### SPI (`fe-connector-spi/ConnectorContext.java`) — new default no-op method
```java
/** Normalizes a raw storage URI a connector emits (e.g. paimon native data-file / DV path like
 *  oss://…, cos://…, s3a://…) to BE's canonical form (s3://…) using the catalog's storage
 *  properties. BE's scheme-dispatched file factory only recognizes the canonical scheme; a
 *  connector emitting native file paths MUST route them through this hook. The connector cannot do
 *  this itself (must not import fe-core LocationPath/StorageProperties). Default = identity, so
 *  every other connector and any already-canonical path is unaffected. Fail-loud on error. */
default String normalizeStorageUri(String rawUri) { return rawUri; }
```

### fe-core impl (`DefaultConnectorContext.java`) — real normalization
```java
@Override
public String normalizeStorageUri(String rawUri) {
    if (Strings.isNullOrEmpty(rawUri)) {
        return rawUri;
    }
    // Mirror legacy PaimonScanNode's 2-arg LocationPath.of(path, storagePropertiesMap):
    // scheme-normalize (oss/cos/obs/s3a -> s3, bucket.endpoint -> bucket) so BE's S3 factory
    // can open the file. Fail-loud (StoragePropertiesException propagates) — a path that cannot
    // be normalized would otherwise silently corrupt reads (esp. DV merge-on-read).
    return LocationPath.of(rawUri, storagePropertiesSupplier.get()).toStorageLocation().toString();
}
```
`DefaultConnectorContext` gains a `Supplier<Map<StorageProperties.Type, StorageProperties>> storagePropertiesSupplier` (lazy — invoked at scan time, catalog fully initialized). New 4-arg ctor; the existing 2-arg/3-arg ctors delegate with a `Collections::emptyMap` supplier (identity-preserving for non-plugin catalogs — `LocationPath.of(x, {})` would throw, but those ctors are never used by paimon and the method is only called by paimon).

### catalog wiring (`PluginDrivenExternalCatalog.java:150`)
```java
new DefaultConnectorContext(name, id, this::getExecutionAuthenticator,
        () -> catalogProperty.getStoragePropertiesMap())
```

### connector call sites (`PaimonScanPlanProvider.java`, native-reader branch only)
- `:270` → `.path(normalizeUri(file.path()))`
- `:282` → `builder.deletionFile(normalizeUri(df.path()), df.offset(), df.length())`
- private helper `normalizeUri(String raw)` = `context != null ? context.normalizeStorageUri(raw) : raw` (null-guard mirrors the existing `vendStorageCredentials` guard at `:363` for the offline unit-test path).

JNI path (`buildJniScanRange`) and `getScanNodeProperties` are **not** touched (JNI carries its own FileIO; credential keys are a separate fix #2).

## Implementation Plan
1. `ConnectorContext.java`: add `normalizeStorageUri` default method.
2. `DefaultConnectorContext.java`: add `storagePropertiesSupplier` field + 4-arg ctor (existing ctors delegate with empty-map supplier) + `normalizeStorageUri` override + `LocationPath` import.
3. `PluginDrivenExternalCatalog.java:150`: pass the storage-props supplier.
4. `PaimonScanPlanProvider.java`: add `normalizeUri` helper; apply at `:270` and `:282`.
5. Tests (below). Build `:fe-core -am` (SPI+fe-core) then `:fe-connector-paimon -am`.
6. `tools/check-connector-imports.sh` must stay clean (no new fe-core import in connector).

## Risk Analysis
- **Regression on s3:// / hdfs:// (common path)**: `normalizeStorageUri` now runs on every native path. For an `s3://`/`hdfs://` warehouse, `getStoragePropertiesMap()` contains the matching type → `validateAndNormalizeUri` is a no-op/idempotent → same `s3://`/`hdfs://` reaches BE. Legacy uses the identical 2-arg `of()`, so parity holds. Verified: the catalog's storage type == the warehouse scheme, so the map always has the entry for a working catalog.
- **Fail-loud**: matches legacy (2-arg `of()` throws `StoragePropertiesException` on missing props). A wrong/un-normalizable path → loud failure instead of silent BE corruption (Rule 12).
- **Vended-vs-static map (scope note)**: legacy overlays vended creds via `VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials`; this fix uses the **static** `getStoragePropertiesMap()`. For **scheme** normalization the vended overlay is irrelevant (vended creds change `AWS_*` keys, not the scheme/bucket form) **as long as the warehouse endpoint is statically configured** (the overwhelmingly common case for OSS/COS/OBS — you need the endpoint to connect). The only divergence is a *pure-vended, no-static-storage-config* REST catalog, where `getStoragePropertiesMap()` may lack the entry and normalization throws. That edge overlaps the credential seam fixes (#2 `FIX-STATIC-CREDS-BE` / `FIX-REST-VENDED`) and is **explicitly out of scope** here; tracked in `deviations-log.md`.
- **Other connectors**: SPI method has an identity default; only paimon calls it. Zero impact on es/jdbc/maxcompute/trino.

## Test Plan

### Unit Tests
1. **`DefaultConnectorContextNormalizeUriTest` (fe-core)** — construct `DefaultConnectorContext` with a real OSS `StorageProperties` map (built from `oss.endpoint`/`oss.access_key`/… via `StorageProperties.createAll`), assert `normalizeStorageUri("oss://bkt/warehouse/f.parquet")` → `s3://…`. Assert identity for `s3://…` input. Assert empty-map supplier ctor + non-normalizable input behavior (fail-loud). *Encodes WHY: BE only opens canonical scheme; mutation = returning raw oss:// → red.*
2. **`PaimonScanPlanProviderTest` (connector) — wiring** — extend `RecordingConnectorContext` with a `normalizeStorageUri` override that records the call and applies a deterministic `oss://`→`s3://` rewrite. Build a native `DataSplit` (RawFile + DeletionFile with `oss://` paths) through `planScan`; assert the resulting `PaimonScanRange` carries **both** the normalized data-file path (via `getPath()`) **and** the normalized DV path (via `getProperties().get("paimon.deletion_file.path")`). Assert the offline no-context path still emits raw (preserves existing offline behavior). *Encodes WHY: both raw sites must route through the hook; mutation = dropping either call site → red on that path.*

### E2E Tests (CI-gated — note, do not claim run)
- `test_paimon_*` deletion-vector + native-read suites over an **OSS** warehouse (DELETE then SELECT; assert deleted rows stay deleted and native ORC/Parquet rows return). Requires live OSS creds → CI-gated.

## SPI / logs
- New SPI method `ConnectorContext.normalizeStorageUri` → register in `plan-doc/01-spi-extensions-rfc.md`.
- Vended-vs-static map scope decision → `plan-doc/deviations-log.md`.
- No user-sign-off decision (approach pre-blessed by task-list fix-sketch: "add a ConnectorContext path-normalization SPI hook (mirror the FIX-REST-VENDED seam)").

## Result (2026-06-11 — implemented + verified)
- **Implemented exactly as designed**: SPI `ConnectorContext.normalizeStorageUri` (identity default); `DefaultConnectorContext` override via 2-arg `LocationPath.of` + a lazy `storagePropertiesSupplier` (new 4-arg ctor, existing ctors delegate empty-map); `PluginDrivenExternalCatalog` wires `() -> catalogProperty.getStoragePropertiesMap()`; connector routes BOTH data-file + DV paths through `normalizeUri` (extracted package-private `buildNativeRange`).
- **Build + UT (green)**: `mvn test -pl :fe-connector-paimon -am` → BUILD SUCCESS, module 216/0/0 (1 CI-gated skip); `PaimonScanPlanProviderTest` 15→18 (+3 new wiring tests). `mvn test -pl :fe-core -am -Dtest=DefaultConnectorContext*` → BUILD SUCCESS, `DefaultConnectorContextNormalizeUriTest` 4/0/0, `DefaultConnectorContextVendTest` 2/0/0 (ctor change non-breaking). Checkstyle 0 violations (all modules). `tools/check-connector-imports.sh` clean.
- **Tests added**: fe-core `DefaultConnectorContextNormalizeUriTest` (oss→s3, s3 idempotent, null/blank, empty-map fail-loud); connector `PaimonScanPlanProviderTest` (both-paths normalized + call count, DV-less, no-context raw).
- **Not run (CI-gated)**: live OSS-warehouse + DV e2e — noted as gated, not executed.
- Logged: SPI RFC §21 (E13), deviations-log DV-025 (static-vs-vended map scope).
