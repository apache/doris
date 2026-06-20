# FIX-REST-VENDED-URI-NORMALIZE — Design

> Source: `reviews/P5-paimon-rereview3-2026-06-12.md` §D.1 (P9-1, **BLOCKER**); task `task-list-P5-rereview3-fixes.md` FIX-1.
> Scope (user-approved 2026-06-12): route the vended-overlay storage map into native URI normalization (legacy parity).

## Problem

`SELECT` over a Paimon **REST**-catalog table on **object storage** (oss/cos/obs/s3a), using the
native reader (ORC/Parquet — the default), throws during FE planning:

```
StoragePropertiesException: No storage properties found for schema: oss
```

It worked under legacy paimon. The only escape hatch today is `SET force_jni_scanner=true` (which
dodges the native path entirely). So every native REST-on-object-store read is broken.

## Root Cause

Native URI normalization uses the **static** catalog storage-properties map, which is **empty by
design for REST** catalogs (vended creds are per-table/dynamic, so `CatalogProperty.initStorageProperties`
seeds an empty static map when vended creds are enabled).

Call chain (verified against current tree):
- Connector `PaimonScanPlanProvider.normalizeUri:485-487` → `context.normalizeStorageUri(rawUri)`.
- fe-core `DefaultConnectorContext.normalizeStorageUri:193-204` → `LocationPath.of(rawUri, storagePropertiesSupplier.get())` (the 2-arg overload; `normalize=true` is supplied internally by the 2-arg→3-arg delegation at `LocationPath.java:181`).
- The supplier is the catalog-static map (`PluginDrivenExternalCatalog`), **empty for REST**.
- `LocationPath.of:135-140` → `findStorageProperties(type, schema, {}) == null` → `throw new UserException("No storage properties found for schema: " + schema)` → wrapped as `StoragePropertiesException` (a `RuntimeException`).

`shouldUseNativeReader:783` has **no flavor gate**, so REST native reads reach `normalizeUri` on the
data-file path (`buildNativeRange:439`) **and** the deletion-vector path (`:448`).

**Why it slipped through twice**: DV-025 deferred this exact corner to FIX-STATIC-CREDS-BE /
FIX-REST-VENDED, but those fixed **credential down-flow to BE** (`getScanNodeProperties` overlay,
`:546-562`), not `normalizeStorageUri`. The deferral was never closed → still live.

### Legacy parity reference
`paimon/source/PaimonScanNode.doInitialize:171-176` computes a **vended-overlay** storage map once:

```java
storagePropertiesMap = VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials(
        catalog...getMetastoreProperties(), catalog...getStoragePropertiesMap(), source.getPaimonTable());
```

and uses **that** map for `LocationPath.of` at `:443` (data file) and `:296` (deletion vector).

Confirmed semantics of `getStoragePropertiesMapWithVendedCredentials` (→ `PaimonVendedCredentialsProvider`
→ `AbstractVendedCredentialsProvider`):
- REST metastore + table has a `RESTTokenFileIO` with a valid token + the filtered token yields ≥1
  cloud-storage prop → returns a **vended-only** typed map built from the token
  (`filterCloudStorageProperties` → `StorageProperties.createAll` → index by `Type`). The factory uses
  it **as-is, discarding the base/static map** (vended *replaces* static — for REST the static map is
  empty anyway, so no practical difference, but we replicate it exactly).
- Otherwise (non-REST, no token, filtered-empty, or any exception) → provider returns `null` → factory
  **falls back to the base/static map**.

The connector already extracts that raw token: `extractVendedToken(table):584-595` (gated on
`fileIO instanceof RESTTokenFileIO`; empty for non-REST), and already feeds it to
`context.vendStorageCredentials(...)` for the BE credential overlay (`:558`).

## Design

**Approach (a)** from the task list (recommended): add an SPI overload
`ConnectorContext.normalizeStorageUri(String rawUri, Map<String,String> rawVendedCredentials)` that
normalizes against the **vended-overlay** map (legacy parity). The connector passes the raw vended
token it already extracts; fe-core builds the typed `StorageProperties` map (it cannot be done in the
connector — `LocationPath`/`StorageProperties` are fe-core-only).

Rejected alternatives:
- (b) vended-aware supplier — vended creds are per-table/dynamic; the supplier is catalog-static. Wrong layer.
- (c) "static-map-misses-scheme → use vended" implicit fallback — narrower and implicit; (a) is explicit and matches legacy precedence exactly.

### fe-core (`DefaultConnectorContext`)
Extract the vended-typed-map construction (already inline in `vendStorageCredentials`) into a private
helper, then use it from both methods (single source of truth, no drift between the BE-creds path and
the normalize path — they MUST agree: same token → same creds → same normalization):

```java
/** Build the vended StorageProperties typed map from a raw token (filter cloud props + createAll +
 *  index by Type), mirroring AbstractVendedCredentialsProvider. Returns null when the token is
 *  null/empty, yields no cloud props, or normalization throws — exactly the legacy provider's
 *  "return null -> Factory falls back to base" contract. */
private Map<StorageProperties.Type, StorageProperties> buildVendedStorageMap(Map<String,String> raw) {
    if (raw == null || raw.isEmpty()) return null;
    try {
        Map<String,String> filtered = CredentialUtils.filterCloudStorageProperties(raw);
        if (filtered.isEmpty()) return null;
        return StorageProperties.createAll(filtered).stream()
                .collect(Collectors.toMap(StorageProperties::getType, Function.identity()));
    } catch (Exception e) {
        LOG.warn("Failed to normalize vended credentials", e);
        return null;
    }
}

@Override public Map<String,String> vendStorageCredentials(Map<String,String> raw) {
    // Keep getBackendPropertiesFromStorageMap INSIDE a try so the fail-soft boundary is byte-preserved
    // vs the pre-refactor method (which wrapped the whole tail, incl. the BE-props call). Without this
    // outer try the refactor would shift that one call from fail-soft to fail-loud (latent — the
    // throwing branch is HdfsProperties.getBackendConfigProperties, unreachable for cloud STS tokens —
    // but we preserve exact semantics rather than rely on unreachability). [red-team S5b/gap3]
    try {
        Map<Type,StorageProperties> map = buildVendedStorageMap(raw);
        return map == null ? Collections.emptyMap() : CredentialUtils.getBackendPropertiesFromStorageMap(map);
    } catch (Exception e) {
        LOG.warn("Failed to normalize vended credentials", e);
        return Collections.emptyMap();
    }
}

@Override public String normalizeStorageUri(String rawUri, Map<String,String> rawVendedCredentials) {
    if (Strings.isNullOrEmpty(rawUri)) return rawUri;
    Map<Type,StorageProperties> vended = buildVendedStorageMap(rawVendedCredentials);
    Map<Type,StorageProperties> effective = (vended != null) ? vended : storagePropertiesSupplier.get();
    return LocationPath.of(rawUri, effective).toStorageLocation().toString();   // fail-loud, legacy parity
}

@Override public String normalizeStorageUri(String rawUri) {   // keep: delegates with no token
    return normalizeStorageUri(rawUri, null);
}
```

The extraction is **behavior-preserving for `vendStorageCredentials`**: the typed-map build is the same
filter/createAll/toMap, and the outer try keeps the BE-props call fail-soft, so the fail-soft boundary is
byte-identical to the pre-refactor method (red-team S5b confirmed the only risk was moving that call out
of the try; the outer try removes it). The 1-arg `normalizeStorageUri` becomes a delegate with a `null`
token → `effective == static` → **byte-identical to current behavior** (the 4 existing fe-core tests stay green).

### SPI (`fe-connector-spi/ConnectorContext`)
Add the overload as a `default` that ignores the token (other connectors have no vended creds), so it is
a no-op extension for every non-paimon connector:

```java
default String normalizeStorageUri(String rawUri, Map<String,String> rawVendedCredentials) {
    return normalizeStorageUri(rawUri);   // ignore token; falls to the existing default (returns rawUri)
}
```

### Connector (`PaimonScanPlanProvider`)
Thread the once-per-scan vended token to the normalize sites:
1. `normalizeUri(String rawUri, Map<String,String> vendedToken)` → `context.normalizeStorageUri(rawUri, vendedToken)` (null-context → rawUri, unchanged).
2. `buildNativeRange(...)` / `buildNativeRanges(...)`: add a `Map<String,String> vendedToken` parameter; pass it to both `normalizeUri` calls (data file + DV).
3. `planScanInternal`: compute `vendedToken = (context != null) ? extractVendedToken(table) : Collections.emptyMap();` **once** (next to the `cppReader` flag) and pass it into `buildNativeRanges` at the call site (`:404`).

`extractVendedToken(table)` is empty for non-REST (FileIO gate) → the 2-arg call degrades to the static
path → **non-REST scans are byte-unchanged**. It is computed **once per `planScan` invocation** (not per
file/sub-range — `validToken()` may refresh the token), separate from the existing
`getScanNodeProperties:558` extraction (two independent extractions; this is a pre-existing property of
the two-method plugin SPI, not introduced here). URI normalization is **invariant under token refresh**
— `validateAndNormalizeUri` consumes only scheme/bucket/key, never the access-key/secret/token — so the
two extractions can never disagree on the normalized URI (red-team gap5). It is NOT wrapped in
`executeAuthenticated` (parity: legacy did not wrap the FileIO/cred path; the existing
`getScanNodeProperties:558` call is also unwrapped). The pinned `resolveScanTable` table carries the same
`RESTTokenFileIO` reference as the base (verified: `AbstractFileStoreTable.copy` preserves `fileIO`), so
the token matches legacy's `source.getPaimonTable()` (red-team S5c).

## Implementation Plan
1. SPI: add the `normalizeStorageUri(uri, token)` default to `ConnectorContext`.
2. fe-core: add `buildVendedStorageMap` helper; refactor `vendStorageCredentials`; add 2-arg override; make 1-arg delegate.
3. Connector: thread `vendedToken` (steps 1–3 above).
4. Tests (below).
5. Build SPI → fe-core → connector; checkstyle; import-gate.

## Risk Analysis
- **Behavior change for non-REST**: none — empty token → static-map path, identical to today.
- **Behavior change for REST native**: was a hard throw → now normalizes via vended map (the fix). Vended
  *replaces* static (legacy parity); REST static is empty so no regression for any non-REST flavor.
- **Fail-loud preserved**: REST + bad/empty token → `buildVendedStorageMap` returns null → static (empty)
  → `LocationPath.of` throws (legacy also fails loud here). The normalize path stays fail-loud; the
  BE-creds path (`vendStorageCredentials`/`getBackendStorageProperties`) stays fail-soft — unchanged asymmetry.
- **Perf**: for REST scans the typed map is rebuilt per normalize call (per file + per DV + per sub-range)
  rather than once-per-scan as legacy did. The token is tiny; the empty-token short-circuit means non-REST
  pays nothing. Behavior is identical; only re-derivation frequency differs. Noted as a minor, accepted
  divergence (a once-per-scan cache would need extra SPI surface or an opaque handle — disproportionate to
  a BLOCKER hotfix; revisit only if profiling flags it).
- **Other connectors**: untouched (SPI default ignores the token; only paimon calls the 2-arg).

## Test Plan

### Unit Tests
**fe-core — `DefaultConnectorContextNormalizeUriTest`** (the actual bug & fix):
- `vendedRestCredentialsNormalizeUnderEmptyStaticMap`: context with an **empty** static supplier (the REST
  case) + a vended token carrying oss creds (`oss.access_key/secret_key/endpoint`) → `normalizeStorageUri(
  "oss://bkt/.../part-0.parquet", token)` returns `s3://bkt/.../part-0.parquet`. **This is the gap that hid
  the bug twice.** MUTATION: ignoring the token (old static-only path) → throws → red.
- `emptyTokenUnderEmptyStaticStillFailsLoud`: same empty-static context, **empty** token → `normalizeStorageUri(
  uri, emptyMap)` throws `RuntimeException` (proves the fix is the token, not a swallow; and that fail-loud
  is intact when there is genuinely no cred).
- `staticMapPathUnaffectedByEmptyToken`: oss-static context + empty token → still rewrites oss→s3 (regression
  guard for non-REST; the 2-arg must fold to the static path).
- Existing 4 tests (1-arg) remain unchanged (1-arg now delegates with null token).

**connector — `PaimonScanPlanProviderTest`** (the threading):
- Extend `RecordingConnectorContext`: override **only** the 2-arg `normalizeStorageUri(uri, token)` so it
  (a) sets `lastVendedToken = token`, (b) increments `normalizeCount` once, (c) does the oss→s3 rewrite;
  then make the existing 1-arg override **delegate** to `normalizeStorageUri(rawUri, null)` (single source;
  no recursion — the 2-arg does the rewrite directly, never calls the 1-arg). After the connector switches
  to the 2-arg call, the connector dispatches straight to this 2-arg override (NOT via the SPI default →
  1-arg), so `normalizeCount`/rewrite are driven by the 2-arg override. [red-team gap2]
- `buildNativeRangeThreadsVendedTokenToBothPaths`: call `buildNativeRange(file, dv, "parquet", emptyMap,
  vendedToken, 0L, 100L)` with a non-empty `vendedToken`; assert the context received that exact token map
  on the data-file **and** the DV normalize call (`lastVendedToken` equals it; `normalizeCount == 2`).
  MUTATION: passing an empty/null token, or dropping the token on the DV site → red.
- Update the **5** existing call sites broken by the signature change (pass `Collections.emptyMap()` as the
  new token arg; assertions unaffected — empty token folds to the unchanged path):
  - 3 `buildNativeRange` sites: `nativeRangeNormalizesBothDataAndDeletionVectorPaths` (`:270`),
    `nativeRangeWithoutDeletionVectorNormalizesOnlyDataPath` (`:294`), `nativeRangeWithoutContextPreservesRawPath` (`:314`).
  - 2 `buildNativeRanges` sites: `buildNativeRangesAttachesSameDeletionVectorToEverySubRange` (`:782`),
    `buildNativeRangesKeepsFileWholeWhenTargetNonPositive` (`:810`). [red-team gap1]

### E2E Tests
The positive `RESTTokenFileIO` token-extraction path needs a live REST stack and is **CI-gated**
(`enablePaimonTest=false`) — same as the existing `extractVendedToken` REST branch. Not run here; noted as
gated. The two unit layers (fe-core does the real normalization with a vended map; connector proves the
token is threaded to both sites) fully cover the offline-reachable surface.

## Files touched
- `fe/fe-connector/fe-connector-spi/.../spi/ConnectorContext.java` (add default overload)
- `fe/fe-core/.../connector/DefaultConnectorContext.java` (helper + 2-arg override + 1-arg delegate)
- `fe/fe-connector/fe-connector-paimon/.../PaimonScanPlanProvider.java` (thread token)
- `fe/fe-core/.../connector/DefaultConnectorContextNormalizeUriTest.java` (3 new cases)
- `fe/fe-connector/.../paimon/RecordingConnectorContext.java` (2-arg override capture; 1-arg delegates)
- `fe/fe-connector/.../paimon/PaimonScanPlanProviderTest.java` (1 new + 5 updated call sites)
