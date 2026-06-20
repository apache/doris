# Problem

P6 clean-room finding **C1** (MAJOR; BLOCKER if `minio.*` keying is supported in deployment;
classification: regression).

A `CREATE CATALOG ... PROPERTIES("type"="paimon", "minio.endpoint"=..., "minio.access_key"=...,
"minio.secret_key"=...)` keyed **purely** with `minio.*` property names no longer binds any storage
backend on this branch. Paimon read fails with `no file io for scheme s3`, and BE receives no
`location.AWS_*` credentials.

Legacy `MinioProperties` (`fe/fe-core/.../datasource/property/storage/MinioProperties.java`)
recognized:
`minio.endpoint / minio.region / minio.access_key / minio.secret_key / minio.session_token /
minio.connection.maximum / minio.connection.request.timeout / minio.connection.timeout /
minio.use_path_style / minio.force_parsing_by_standard_uri`
with region default `us-east-1` and tuning defaults `100 / 10000 / 10000`, and produced S3A Hadoop
config + AWS_* backend config via `AbstractS3CompatibleProperties`.

The new path sources storage **exclusively** from the typed `fe-filesystem` SPI. There is **no MinIO
provider**. Registered providers (ServiceLoader, verified via the eight
`META-INF/services/org.apache.doris.filesystem.spi.FileSystemProvider` files):
`OSS, Local, HDFS, COS, S3, Broker, Azure, OBS`. None recognizes a `minio.*` key.

# Root Cause

Two facts in the typed path combine to drop a pure-`minio.*` catalog:

1. **`S3FileSystemProvider.supports()` never matches `minio.*`.**
   `fe/fe-filesystem/fe-filesystem-s3/.../S3FileSystemProvider.java:45-73`. `supports()` checks
   `ACCESS_KEY_NAMES` / `ENDPOINT_NAMES` / `REGION_NAMES` / `ROLE_ARN_NAMES` /
   `CREDENTIALS_PROVIDER_TYPE_NAMES`. None of these arrays contain any `minio.*` key. The
   cloud-specific providers (OSS/COS/OBS) only match their endpoint domains (`aliyuncs.com` /
   `myqcloud.com` / `myhuaweicloud.com`) or explicit `provider`/`_STORAGE_TYPE_`. A bare MinIO
   endpoint (e.g. `http://127.0.0.1:9000`) matches none.

2. **No match → empty list, no throw (in `bindAll`, the path catalogs use).**
   `bindAllStorageProperties` (`fe/fe-core/.../fs/FileSystemFactory.java:119-142`) and the production
   `FileSystemPluginManager.bindAll` (`fe/fe-core/.../fs/FileSystemPluginManager.java:158-172`)
   iterate providers, `continue` on `!supports`, and return the accumulated list — empty when nothing
   matched. (`createFileSystem`/`getFileSystem` *do* throw on no-match, but catalog binding goes
   through `bindAll`, which does not.)

Empty `StorageProperties` list ⇒ paimon `PaimonScanPlanProvider`
(`fe/fe-connector/fe-connector-paimon/.../PaimonScanPlanProvider.java:617-624`) iterates an empty
`ctx.getStorageProperties()` ⇒ no `location.AWS_*` for BE; and the FE Hadoop-config map is never
populated with `fs.s3.impl` ⇒ paimon SDK has no FileIO for scheme `s3`.

**Per-key loss for a pure-`minio.*` catalog:** endpoint, region (default `us-east-1`), access_key,
secret_key, session_token, connection.maximum (100), connection.request.timeout (10000),
connection.timeout (10000), use_path_style (false), force_parsing_by_standard_uri (false).

# Design

## Decision: alias `minio.*` into the shared S3 provider/properties — NOT a dedicated provider.

**Recommendation: aliasing (the review's preferred option), with one caveat made explicit and
resolved below.** Both FE Hadoop config and BE creds for MinIO are byte-identical to plain S3A
(legacy `MinioProperties` had *zero* MinIO-specific `fs.*`/`AWS_*` keys — it inherited everything
from `AbstractS3CompatibleProperties`, exactly what `S3FileSystemProperties` already emits). MinIO is
literally "S3 with a custom endpoint." The only deltas are (a) the alias key prefix and (b) three
tuning defaults + the region default. Precedent: `CosFileSystemProvider`/`CosFileSystemProperties`
is a *dedicated* provider only because COS emits genuinely COS-specific Hadoop keys
(`fs.cosn.*`, `fs.cos.impl`) and uses a Tencent native SDK in `CosObjStorage`. MinIO emits **none** —
a dedicated provider would be a near-empty clone of S3, pure duplication for no behavioral gain.

**The one caveat — differing tuning defaults — and why it does not block aliasing.**
`ConnectorProperty` defaults are *field-level*, applied whenever no alias for that field is present
in the raw map (`ConnectorPropertiesUtils.bindConnectorProperties` only `field.set`s when a name
matched). They cannot be conditionalized on *which* alias matched. So I cannot make
`maxConnections` default to `50` for an `s3.*` catalog and `100` for a `minio.*` catalog purely by
adding aliases. Resolution: **add `minio.*` as aliases on the existing tuning fields and accept the
S3 defaults (50/3000/1000) for a `minio.*` catalog that omits the tuning keys.** Justification:

- The tuning values are connection-pool/timeout knobs; both sets are functional against MinIO. The
  legacy MinIO values (100/10000/10000) were never documented as required for correctness — they are
  a historical default, not a contract.
- A `minio.*` catalog that *explicitly* sets `minio.connection.maximum` etc. is honored exactly
  (the alias binds the value). Only the *unset* case differs, and only in pool size / timeouts.
- Conditionalizing the default would require post-bind logic ("if a `minio.*` alias matched and the
  tuning key was absent, override to 100/10000/10000") — added complexity in shared code, touching
  the s3 hot path, to preserve a non-contractual default. Not worth the blast-radius risk.

This is the single intentional, documented behavioral deviation from legacy MinIO. It is called out
in Risk Analysis and Open Questions for the main agent to ratify. **Region default `us-east-1` IS
preserved** — `S3FileSystemProperties.normalizeForLegacyS3Compatibility()` already derives
`region = DEFAULT_REGION ("us-east-1")` when an endpoint is set but region is blank
(`S3FileSystemProperties.java:360-362`), exactly matching legacy MinIO's `region="us-east-1"`
default behavior for the common endpoint-only case. (Field-level default of legacy is `us-east-1`
unconditionally; the SPI achieves the same effective value for endpoint-only configs and for
region-only/AWS-endpoint configs derives the real region — strictly better.)

## Ordering invariant (load-bearing for s3.* byte-parity)

`ConnectorPropertiesUtils.getMatchedPropertyName` (`ConnectorPropertiesUtils.java:96-108`) returns
the **first** name in the annotation's `names()` array that is present in the map. Therefore **all
`minio.*` aliases MUST be appended at the END of each field's existing `names()` list.** With
`minio.*` last, an `s3.*`-keyed (or `AWS_*`-keyed) catalog binds exactly as today even if it also
happened to carry a `minio.*` key — `s3.endpoint` outranks `minio.endpoint`. This is the mechanical
guarantee that the canonical `s3.*` path is byte-for-byte unchanged.

# Implementation Plan

Two files change. No new module, no new provider, no `META-INF/services` change.

## File 1 — `fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystemProperties.java`

Append `minio.*` aliases to the END of each field's `names()`. (Excerpts show current → proposed for
the affected fields only; nothing else in the file changes.)

`:86-90` endpoint
```java
@ConnectorProperty(names = {ENDPOINT, "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint",
        "glue.endpoint", "aws.glue.endpoint", "minio.endpoint"},
        ...
```

`:93-94` region (note `isRegionField = true` retained)
```java
@ConnectorProperty(names = {REGION, "AWS_REGION", "region", "REGION", "aws.region", "glue.region",
        "aws.glue.region", "iceberg.rest.signing-region", "rest.signing-region", "client.region",
        "minio.region"},
        ...
```

`:101-104` accessKey
```java
@ConnectorProperty(names = {ACCESS_KEY, "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY",
        "glue.access_key", "aws.glue.access-key",
        "client.credentials-provider.glue.access_key", "iceberg.rest.access-key-id",
        "s3.access-key-id", "minio.access_key"},
        ...
```

`:110-113` secretKey
```java
@ConnectorProperty(names = {SECRET_KEY, "AWS_SECRET_KEY", "secret_key", "SECRET_KEY",
        "glue.secret_key", "aws.glue.secret-key",
        "client.credentials-provider.glue.secret_key", "iceberg.rest.secret-access-key",
        "s3.secret-access-key", "minio.secret_key"},
        ...
```

`:120-121` sessionToken
```java
@ConnectorProperty(names = {SESSION_TOKEN, "AWS_TOKEN", "session_token",
        "s3.session-token", "iceberg.rest.session-token", "minio.session_token"},
        ...
```

`:152` maxConnections
```java
@ConnectorProperty(names = {MAX_CONNECTIONS, "AWS_MAX_CONNECTIONS", "minio.connection.maximum"},
        ...
```

`:158` requestTimeoutMs
```java
@ConnectorProperty(names = {REQUEST_TIMEOUT_MS, "AWS_REQUEST_TIMEOUT_MS",
        "minio.connection.request.timeout"},
        ...
```

`:164` connectionTimeoutMs
```java
@ConnectorProperty(names = {CONNECTION_TIMEOUT_MS, "AWS_CONNECTION_TIMEOUT_MS",
        "minio.connection.timeout"},
        ...
```

`:170` usePathStyle
```java
@ConnectorProperty(names = {USE_PATH_STYLE, "s3.path-style-access", "minio.use_path_style"},
        ...
```

**`force_parsing_by_standard_uri`:** `S3FileSystemProperties` has **no** such field today (the legacy
key only affected URI parsing in `S3PropertyUtils`, a fe-core concern; the SPI normalizes URIs via
`DefaultConnectorContext`). Do **not** add a new field — out of scope for C1 (no read-path use in the
typed model). Note as Open Question if URI parity for path-style MinIO is later reported.

## File 2 — `fe/fe-filesystem/fe-filesystem-s3/src/main/java/org/apache/doris/filesystem/s3/S3FileSystemProvider.java`

Append `"minio.*"` to the three detection arrays so a pure-`minio.*` map satisfies
`hasCredential && hasLocation` (`:64-72`). Add `minio.endpoint`/`minio.region` to location arrays and
`minio.access_key` to the credential array.

`:45-49` ACCESS_KEY_NAMES
```java
private static final String[] ACCESS_KEY_NAMES = {
        S3FileSystemProperties.ACCESS_KEY, "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY",
        "glue.access_key", "aws.glue.access-key",
        "client.credentials-provider.glue.access_key", "iceberg.rest.access-key-id",
        "s3.access-key-id", "minio.access_key"};
```

`:50-52` ENDPOINT_NAMES
```java
private static final String[] ENDPOINT_NAMES = {
        S3FileSystemProperties.ENDPOINT, "AWS_ENDPOINT", "endpoint", "ENDPOINT", "aws.endpoint",
        "glue.endpoint", "aws.glue.endpoint", "minio.endpoint"};
```

`:53-55` REGION_NAMES
```java
private static final String[] REGION_NAMES = {
        S3FileSystemProperties.REGION, "AWS_REGION", "region", "REGION", "aws.region", "glue.region",
        "aws.glue.region", "iceberg.rest.signing-region", "rest.signing-region", "client.region",
        "minio.region"};
```

This keeps `supports()` true for `minio.endpoint + minio.access_key + minio.secret_key`
(`hasCredential` via `minio.access_key`, `hasLocation` via `minio.endpoint`). Validation
(`requireTogether(accessKey, secretKey)`) still fires correctly because the binding resolves the
`minio.*` aliases into the typed fields before `validate()`.

# Risk Analysis

## Cross-connector blast radius

Consumers of `S3FileSystemProperties`/`S3FileSystemProvider` are **every** connector that reaches the
typed S3 path: iceberg, hive, hudi, paimon, plus fe-core load/backup/snapshot flows — they all go
through `bindAllStorageProperties` → `supports()` → `bind()`. The change touches only the alias
**lists** and the detection **arrays**; the emitted FE Hadoop config (`toHadoopConfigurationMap`,
`:285-316`) and BE map (`toFileSystemKv`, `:245-262`) code is **unchanged**.

## s3.* unchanged — proof

1. **Binding precedence**: `getMatchedPropertyName` returns the first present alias
   (`ConnectorPropertiesUtils.java:101-105`). New `minio.*` aliases are appended **last**, so for any
   map containing an `s3.*`/`AWS_*`/legacy-bare key, that key still matches first → identical bound
   field values → identical `toFileSystemKv`/`toHadoopConfigurationMap` output.
2. **No default changed**: field defaults (`DEFAULT_MAX_CONNECTIONS="50"`, etc.) are untouched, so an
   `s3.*` catalog that omits tuning keys gets `50/3000/1000` exactly as before. Regression-guarded by
   the existing `toMaps_emitS3TuningDefaultsWhenNotConfigured` test (S3FileSystemPropertiesTest.java
   :262-282), which already asserts the literal `50/3000/1000` and will fail if a default drifts.
3. **`supports()` for s3.* maps**: adding entries to the arrays can only make `supports()` return
   true for *more* inputs; it cannot turn a previously-true s3 map false. Existing
   `S3FileSystemProviderTest` cases remain green.

## Routing / disambiguation

`bindAll` collects **all** matching providers; `createFileSystem` uses the **first**. Could
`minio.*` cause a wrong/extra provider to match?

- **OSS/COS/OBS** match only on `aliyuncs.com` / `myqcloud.com` / `myhuaweicloud.com` endpoint
  substrings or explicit `provider`/`_STORAGE_TYPE_`/`fs.<x>.support`. A MinIO endpoint (e.g.
  `http://127.0.0.1:9000`) contains none of these ⇒ they do **not** match a pure-`minio.*` map.
  (Verified: OssFileSystemProvider:48-55, CosFileSystemProvider:48-55, ObsFileSystemProvider:48-55.)
  Note: these providers also read `s3.endpoint`/`AWS_ENDPOINT` aliases but still gate on the cloud
  domain substring — a `minio.endpoint` value pointing at, say, `aliyuncs.com` would (correctly) be
  treated as OSS, but that is operator misconfiguration, not a MinIO catalog.
- **Azure / HDFS / Local / Broker** key on `azure.*`/account keys, `dfs.*`/`hadoop.*`,
  `file://`/`_STORAGE_TYPE_=LOCAL`, `_STORAGE_TYPE_=BROKER` respectively — disjoint from `minio.*`.
- **Double-bind for legitimately multi-backend catalogs** (object store + HDFS) is the *intended*
  `bindAll` behavior and is unaffected: a `minio.* + dfs.*` catalog binds S3 (MinIO) + HDFS, exactly
  the legacy multi-backend semantics.

Conclusion: a pure-`minio.*` map matches **only** `S3FileSystemProvider`. No collision, no wrong
provider, no ambiguous double-bind.

## Differing tuning defaults

S3 defaults `50/3000/1000` vs legacy MinIO `100/10000/10000` (confirmed:
`S3Properties.java:129/136/143` = 50/3000/1000; `MinioProperties.java:75/84/93` = 100/10000/10000).
With aliasing, a `minio.*` catalog that omits tuning keys gets the **S3** defaults. This is the one
intentional deviation (see Design). It changes only connection-pool size / timeouts, never
correctness or credentials. Explicitly-set `minio.connection.*` values are honored. Documented in
Open Questions for ratification. A dedicated provider would preserve the legacy defaults but at the
cost of duplicating the entire S3 properties class for zero behavioral difference elsewhere — judged
not worth it.

# Test Plan

## Unit Tests

All in `fe/fe-filesystem/fe-filesystem-s3/src/test/java/org/apache/doris/filesystem/s3/`.

### `S3FileSystemProviderTest` — add

- `supports_acceptsPureMinioKeyedConfiguration`:
  map `{minio.endpoint=http://127.0.0.1:9000, minio.access_key=ak, minio.secret_key=sk}` ⇒
  `assertTrue(provider.supports(map))`. (This is the exact C1 reproduction; RED before the
  `ENDPOINT_NAMES`/`ACCESS_KEY_NAMES` edit.)
- `supports_acceptsMinioEndpointWithRegionOnly` (optional): `{minio.endpoint=..., minio.region=...,
  minio.access_key=ak, minio.secret_key=sk}` ⇒ true.

### `S3FileSystemProperties` MinIO binding test — new test class `MinioAliasS3FileSystemPropertiesTest` (or add to `S3FileSystemPropertiesTest`)

- `of_bindsPureMinioAliases`: input all `minio.*` keys (endpoint, access_key, secret_key,
  session_token, connection.maximum=200, connection.request.timeout=20000, connection.timeout=20000,
  use_path_style=true). Assert typed getters: `getEndpoint`/`getAccessKey`/`getSecretKey`/
  `getSessionToken`/`getMaxConnections`("200")/`getRequestTimeoutMs`("20000")/
  `getConnectionTimeoutMs`("20000")/`getUsePathStyle`("true").
- `of_minioEndpointOnly_appliesUsEast1RegionDefault`:
  `{minio.endpoint=http://127.0.0.1:9000, minio.access_key=ak, minio.secret_key=sk}` ⇒
  `assertEquals("us-east-1", props.getRegion())` (parity with legacy MinIO region default).
- `toHadoopConfigurationMap_forMinio_emitsS3aImplAndEndpoint`: from the endpoint-only map, assert
  `fs.s3.impl == org.apache.hadoop.fs.s3a.S3AFileSystem`, `fs.s3a.impl == ...S3AFileSystem`,
  `fs.s3a.endpoint == http://127.0.0.1:9000`, `fs.s3a.endpoint.region == us-east-1`,
  `fs.s3a.access.key == ak`, `fs.s3a.secret.key == sk`,
  `fs.s3a.path.style.access == <expected>`. (Covers FE-side `no file io for scheme s3` fix.)
- `toFileSystemKv_forMinio_emitsAwsBackendKeys`: assert `AWS_ENDPOINT`, `AWS_REGION` (`us-east-1`),
  `AWS_ACCESS_KEY`, `AWS_SECRET_KEY` present and correct. (Covers BE `location.AWS_*` fix — the
  values BE consumes via `PaimonScanPlanProvider:617-624`.)
- `of_minioOmittingTuning_appliesS3DefaultsNotLegacyMinioDefaults`: endpoint-only minio map ⇒ assert
  `getMaxConnections()=="50"`, `getRequestTimeoutMs()=="3000"`, `getConnectionTimeoutMs()=="1000"`.
  This **encodes the intentional deviation** so it cannot regress silently and documents WHY (legacy
  was 100/10000/10000; this asserts the deliberate S3-default behavior).
- `of_s3KeyOutranksMinioKey` (precedence guard): map carrying BOTH `s3.endpoint=https://A` and
  `minio.endpoint=http://B`, plus s3 ak/sk ⇒ `assertEquals("https://A", props.getEndpoint())`. This
  is the byte-parity regression guard for the s3 path (RED if minio aliases were prepended instead of
  appended).

### Existing regression guard (no change, must stay green)

`S3FileSystemPropertiesTest.toMaps_emitS3TuningDefaultsWhenNotConfigured` (:262-282) — proves the
s3.* default path is untouched. Re-run after the edit.

## E2E Tests (gated — do NOT run here)

- `regression-test/suites/external_table_p0/paimon/` paimon docker suite, gated by
  `enablePaimonTest=true` in `regression-conf.groovy`. A MinIO-warehouse paimon catalog created with
  `minio.*` properties should: (a) `SHOW DATABASES`/`SHOW TABLES` succeed (FE FileIO binds), and
  (b) `SELECT *` succeed (BE receives `location.AWS_*`). The pre-fix symptom is
  `no file io for scheme s3`. The fe-filesystem S3 module is exercised by every object-store external
  suite (iceberg/hive/hudi on S3/MinIO), so the broader external p0 set is the byte-parity safety net
  for the shared change.

# Open Questions

1. **Tuning-default deviation ratification.** — **RESOLVED 2026-06-18: PRESERVE legacy defaults.**
   The design's original "accept the deviation" recommendation rested on the claim that field-level
   defaults can't be conditionalized on which alias matched. An adversarial design red-team
   (`wf`/agent `adfda124…`) **refuted** that: the design's own cited `normalizeForLegacyS3Compatibility()`
   is a post-bind hook that already conditionalizes a field (region→us-east-1). Preserving the legacy
   MinIO tuning defaults (100/10000/10000) there is ~6 lines, gated on a `minio.*` raw key being
   present, so it never touches the canonical `s3.*` hot path. The review report (authoritative spec)
   explicitly required "preserve MinIO defaults: region us-east-1, tuning 100/10000/10000", so strict
   parity is the correct call and avoids a sign-off-requiring deviation. **Implemented** as
   `applyLegacyMinioTuningDefaults()` (gates on raw-key PRESENCE, not field-value-equals-default, so an
   explicit `minio.connection.maximum=50` is honored). Pinned by
   `of_minioOmittingTuning_appliesLegacyMinioTuningDefaults`.
2. **`minio.force_parsing_by_standard_uri` / path-style URI parsing.** Not modeled in the typed S3
   path (URI normalization moved to `DefaultConnectorContext`). C1 lists it as a lost key but no
   typed read-path consumes it. Confirm no path-style MinIO URI-parsing regression is in scope; if it
   is, that is a separate fix in the URI-normalization layer, not these two files.
3. **Should `minio.use_path_style` map default to `true`?** Legacy default was `false` (matching S3);
   the SPI keeps `false`. Many MinIO deployments need path-style addressing, but legacy also defaulted
   to `false`, so keeping `false` is strict parity. Flagging only because it is a common MinIO
   operational footgun, not a regression.
