# Problem

A Paimon catalog created through the new SPI connector with the **canonical Doris storage keys** silently loses every storage credential, so live reads against private S3/OSS buckets fail.

Two concrete live-reachable failures (paimon is in `SPI_READY_TYPES`):

1. **filesystem flavor + S3/OSS** (review path 9, Finding 9.1, BLOCKER 3/0/0). A user (and the shipped regression `test_paimon_s3.groovy:70-72`) passes the documented keys:
   ```
   'paimon.catalog.type'='filesystem',
   'warehouse'='s3://bucket/wh',
   's3.access_key'=..., 's3.secret_key'=..., 's3.endpoint'='s3.ap-east-1.amazonaws.com'
   ```
   `buildHadoopConfiguration` → `applyStorageConfig` recognizes none of `s3.access_key / s3.secret_key / s3.endpoint`. The resulting Hadoop `Configuration` has zero `fs.s3a.*` keys, so the Paimon FileSystem catalog hits S3 with no credentials → FE-side auth/access-denied exception at plan time.

2. **DLF flavor + OSS** (review path 9, Finding 9.2, BLOCKER 3/0/0; path 8 DLF). `requireOssStorageForDlf` passes when ANY `oss./fs.oss./paimon.fs.oss.` key is present, but `buildDlfHiveConf` then overlays storage only via the same `applyStorageConfig`. Canonical `oss.access_key / oss.secret_key / oss.endpoint / oss.region` are dropped, and `fs.oss.impl` (JindoOSS) is never set. The gate says "OSS configured" yet the HiveConf carries no usable OSS FileIO config → DLF/HMS catalog cannot read OSS data files.

Scope note (from review reproducibility lens): real-world symptom is an FE-side auth/access-denied exception during planning, not a literal "0 rows". Core claim (credentials dropped) holds.

# Root Cause (confirmed in current code)

`PaimonCatalogFactory.applyStorageConfig` is the single storage-translation seam, shared by `buildHadoopConfiguration` (filesystem/jdbc), `buildHmsHiveConf` (hms), and `buildDlfHiveConf` (dlf). It only recognizes a 4-prefix allow-list plus raw Hadoop keys:

- `fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonCatalogFactory.java:74-75` — `USER_STORAGE_PREFIXES = {"paimon.s3.", "paimon.s3a.", "paimon.fs.s3.", "paimon.fs.oss."}`.
- `PaimonCatalogFactory.java:328-340` — `applyStorageConfig`: for each prop, if it starts with one of the 4 `paimon.*` prefixes it is re-keyed to `fs.s3a.` + remainder; else if it starts with `fs./dfs./hadoop.` it is copied verbatim; **everything else (including `s3.access_key`, `oss.access_key`, `s3.endpoint`, `oss.endpoint`, `oss.region`, `AWS_*`) is dropped.**

The connector only ported the legacy **secondary** overlay (`AbstractPaimonProperties.normalizeS3Config` / `appendUserHadoopConfig`, fe-core `AbstractPaimonProperties.java:143-154`), which also only handles those same 4 `paimon.*` prefixes. It never ported the **primary** translation. In legacy the primary translation came from `StorageProperties.getHadoopStorageConfig()`, applied separately:

- **filesystem/hms**: `PaimonHMSMetaStoreProperties.buildHiveConfiguration` (fe-core `PaimonHMSMetaStoreProperties.java:80-84`) iterates `storagePropertiesList` and `conf.addResource(sp.getHadoopStorageConfig())` BEFORE the `appendUserHadoopConfig` overlay. The filesystem/jdbc path feeds the same `getOrderedStoragePropertiesList()` into `CatalogContext` (`PaimonExternalCatalog.createCatalog:147` → `initializeCatalog`).
- **dlf**: `PaimonAliyunDLFMetaStoreProperties.initializeCatalog` (fe-core `PaimonAliyunDLFMetaStoreProperties.java:90-95`) selects the OSS/OSS_HDFS `StorageProperties` and `ossProps.getHadoopStorageConfig().forEach(hiveConf::set)`.

For S3, `getHadoopStorageConfig` is `AbstractS3CompatibleProperties.appendS3HdfsProperties` (fe-core `AbstractS3CompatibleProperties.java:272-295`): from the canonical `s3.*`/`AWS_*` aliases it sets `fs.s3.impl`, `fs.s3a.impl`, `fs.s3a.endpoint`, `fs.s3a.endpoint.region`, `fs.s3.impl.disable.cache`, `fs.s3a.impl.disable.cache`, `fs.s3a.aws.credentials.provider`, `fs.s3a.access.key`, `fs.s3a.secret.key`, optional `fs.s3a.session.token`, and connection/path-style keys.

For OSS, it is `OSSProperties.initializeHadoopStorageConfig` (fe-core `OSSProperties.java:315-326`): the S3A block above PLUS `fs.oss.impl` (Jindo), `fs.AbstractFileSystem.oss.impl`, `fs.oss.accessKeyId`, `fs.oss.accessKeySecret`, optional `fs.oss.securityToken`, `fs.oss.endpoint`, `fs.oss.region`. The canonical OSS aliases are declared on `OSSProperties` fields (fe-core `OSSProperties.java:48-91`): endpoint = `{oss.endpoint, s3.endpoint, AWS_ENDPOINT, endpoint, dlf.endpoint, dlf.catalog.endpoint, fs.oss.endpoint}`, accessKey = `{oss.access_key, s3.access_key, ..., dlf.access_key, fs.oss.accessKeyId}`, etc.

So the connector's allow-list mismatches the keys Doris users actually pass (and the keys its own regression suite passes), and the credentials never reach the Paimon FileIO `Configuration`/`HiveConf`. Confirmed PaimonCatalogFactory imports zero `org.apache.doris.*` and currently sets none of the `fs.s3.impl`/`fs.s3a.impl`/credentials-provider keys.

# Design

Add a **canonical-key translation step** to `applyStorageConfig`, ported from legacy `appendS3HdfsProperties` + `OSSProperties.initializeHadoopStorageConfig`, keeping it fe-core-free (pure Map→setter, no `StorageProperties` import). The existing two branches (paimon.* re-key, raw fs./dfs./hadoop. passthrough) are **preserved unchanged** for backward compatibility — we only ADD recognition of the canonical aliases the connector currently drops.

Principles, matching existing style:

1. **Alias resolution via `firstNonBlank`** over the same alias lists legacy `@ConnectorProperty(names=...)` declared (literal string keys, mirroring how `DLF_*`/`HMS_URI` aliases are already declared as literal arrays in `PaimonConnectorProperties`). No fe-core types.
2. **S3A block** (both flavors): when an access key is resolvable, set `fs.s3a.access.key`/`fs.s3a.secret.key`/`fs.s3a.aws.credentials.provider=...SimpleAWSCredentialsProvider`, optional `fs.s3a.session.token`; always set `fs.s3.impl`/`fs.s3a.impl` + `disable.cache`; set `fs.s3a.endpoint` and `fs.s3a.endpoint.region` when present. This is the verbatim legacy `appendS3HdfsProperties` minus the FE-config-derived connection/timeout defaults (those are not credentials; the existing code already omits them, and Hadoop S3A has its own defaults — keep that minimal-change boundary).
3. **OSS block** (additive): when an OSS access key / endpoint / region is resolvable from canonical `oss.*` aliases, set the Jindo `fs.oss.*` keys (`fs.oss.impl`, `fs.AbstractFileSystem.oss.impl`, `fs.oss.accessKeyId`, `fs.oss.accessKeySecret`, optional `fs.oss.securityToken`, `fs.oss.endpoint`, `fs.oss.region`). Because the canonical OSS endpoint/key aliases overlap with `s3.*` (legacy `OSSProperties` shares them), the S3A block also gets populated from the same values — which is exactly legacy behavior (`OSSProperties.initializeHadoopStorageConfig` calls `super` = the S3A block first). This is desirable: it preserves the legacy "even for OSS we append S3 props for `s3://`-scheme back-compat" comment (fe-core `AbstractS3CompatibleProperties.java:266-269`).
4. **Precedence**: the existing `paimon.*` re-key and raw `fs./dfs./hadoop.` passthrough run AFTER the canonical translation, so an explicitly-passed `fs.s3a.access.key` or `paimon.s3.access-key` still wins (last-write). This matches legacy ordering: `addResource(getHadoopStorageConfig())` (canonical) THEN `appendUserHadoopConfig`/raw (paimon.*) overlay.
5. **Endpoint-from-region derivation is NOT ported** for the filesystem S3 case (legacy `setEndpointIfPossible` lives in `AbstractS3CompatibleProperties` and constructs from URL/region). The regression and documented config always pass an explicit endpoint; deriving it would require porting the S3/OSS endpoint-pattern machinery (large, fe-core-coupled). We set `fs.s3a.endpoint`/`fs.oss.endpoint` only when the user supplied one. For the **DLF** flavor, `buildDlfHiveConf` already derives the DLF *metastore* endpoint from region (`PaimonCatalogFactory.java:466-470`); the OSS *storage* endpoint for DLF should likewise be derivable — see Implementation Plan note (mirror `OSSProperties.getOssEndpoint` only inside the OSS block to keep DLF parity, since DLF users typically pass `dlf.region`/`oss.region` not `oss.endpoint`).

Helper method `firstNonBlank` already exists and is exactly the alias-priority primitive needed.

# Implementation Plan

All changes in `PaimonCatalogFactory.java` (one file; tests in a second). No SPI/fe-core change.

### 1. Add canonical alias constants (top of class, near `USER_STORAGE_PREFIXES`)

```java
// Canonical Doris storage aliases (ported from fe-core S3Properties / OSSProperties
// @ConnectorProperty names). Listed in legacy priority order. Kept as literal strings
// to avoid importing fe-core StorageProperties.
private static final String[] S3_ACCESS_KEY_ALIASES = {
        "s3.access_key", "AWS_ACCESS_KEY", "access_key", "ACCESS_KEY", "s3.access-key-id"};
private static final String[] S3_SECRET_KEY_ALIASES = {
        "s3.secret_key", "AWS_SECRET_KEY", "secret_key", "SECRET_KEY", "s3.secret-access-key"};
private static final String[] S3_SESSION_TOKEN_ALIASES = {
        "s3.session_token", "session_token", "s3.session-token", "AWS_TOKEN"};
private static final String[] S3_ENDPOINT_ALIASES = {
        "s3.endpoint", "AWS_ENDPOINT", "endpoint", "ENDPOINT"};
private static final String[] S3_REGION_ALIASES = {
        "s3.region", "AWS_REGION", "region", "REGION"};

private static final String[] OSS_ACCESS_KEY_ALIASES = {
        "oss.access_key", "fs.oss.accessKeyId", "dlf.access_key"};
private static final String[] OSS_SECRET_KEY_ALIASES = {
        "oss.secret_key", "fs.oss.accessKeySecret", "dlf.secret_key"};
private static final String[] OSS_SESSION_TOKEN_ALIASES = {
        "oss.session_token", "fs.oss.securityToken"};
private static final String[] OSS_ENDPOINT_ALIASES = {
        "oss.endpoint", "fs.oss.endpoint"};
private static final String[] OSS_REGION_ALIASES = {"oss.region", "dlf.region"};

private static final String S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
private static final String S3A_SIMPLE_CRED_PROVIDER =
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
// JindoOSS impls (literals; avoid the Aliyun compile dep, same pattern as appendDlfOptions).
private static final String JINDO_OSS_IMPL = "com.aliyun.jindodata.oss.JindoOssFileSystem";
private static final String JINDO_OSS_ABSTRACT_IMPL = "com.aliyun.jindodata.oss.JindoOSS";
```

Note: I deliberately list `s3.endpoint`/`s3.access_key` in the OSS aliases' *source* indirectly — legacy `OSSProperties` accepts `s3.*` as OSS aliases too. To keep this minimal and avoid double-population surprises, OSS detection keys off OSS-specific aliases (`oss.*`/`fs.oss.*`/`dlf.*`); when only `s3.*` keys are present the S3A block already covers them (legacy populates S3A regardless). This preserves the documented "append S3 props even for OSS" back-compat.

### 2. Rewrite `applyStorageConfig` to run canonical translation first, then existing logic

```java
private static void applyStorageConfig(Map<String, String> props, BiConsumer<String, String> setter) {
    applyCanonicalS3Config(props, setter);   // NEW: ported appendS3HdfsProperties
    applyCanonicalOssConfig(props, setter);  // NEW: ported OSSProperties.initializeHadoopStorageConfig OSS block
    // Existing behavior preserved (overlays canonical, last-write-wins = legacy ordering):
    props.forEach((key, value) -> {
        for (String prefix : USER_STORAGE_PREFIXES) {
            if (key.startsWith(prefix)) {
                setter.accept(FS_S3A_PREFIX + key.substring(prefix.length()), value);
                return;
            }
        }
        if (key.startsWith("fs.") || key.startsWith("dfs.") || key.startsWith("hadoop.")) {
            setter.accept(key, value);
        }
    });
}
```

`applyCanonicalS3Config` (ported from `appendS3HdfsProperties`, credentials-relevant subset):

```java
private static void applyCanonicalS3Config(Map<String, String> props, BiConsumer<String, String> setter) {
    String ak = firstNonBlank(props, S3_ACCESS_KEY_ALIASES);
    String sk = firstNonBlank(props, S3_SECRET_KEY_ALIASES);
    String endpoint = firstNonBlank(props, S3_ENDPOINT_ALIASES);
    String region = firstNonBlank(props, S3_REGION_ALIASES);
    String token = firstNonBlank(props, S3_SESSION_TOKEN_ALIASES);
    // Only emit S3A config when the user actually configured an S3-style storage key.
    if (ak == null && endpoint == null && region == null) {
        return;
    }
    setter.accept("fs.s3.impl", S3A_IMPL);
    setter.accept("fs.s3a.impl", S3A_IMPL);
    setter.accept("fs.s3.impl.disable.cache", "true");
    setter.accept("fs.s3a.impl.disable.cache", "true");
    if (StringUtils.isNotBlank(endpoint)) {
        setter.accept("fs.s3a.endpoint", endpoint);
    }
    if (StringUtils.isNotBlank(region)) {
        setter.accept("fs.s3a.endpoint.region", region);
    }
    if (StringUtils.isNotBlank(ak)) {
        setter.accept("fs.s3a.aws.credentials.provider", S3A_SIMPLE_CRED_PROVIDER);
        setter.accept("fs.s3a.access.key", ak);
        setter.accept("fs.s3a.secret.key", nullToEmpty(sk));
        if (StringUtils.isNotBlank(token)) {
            setter.accept("fs.s3a.session.token", token);
        }
    }
}
```

`applyCanonicalOssConfig` (ported from `OSSProperties.initializeHadoopStorageConfig` OSS block; only the OSS-specific aliases trigger it):

```java
private static void applyCanonicalOssConfig(Map<String, String> props, BiConsumer<String, String> setter) {
    String ak = firstNonBlank(props, OSS_ACCESS_KEY_ALIASES);
    String sk = firstNonBlank(props, OSS_SECRET_KEY_ALIASES);
    String endpoint = firstNonBlank(props, OSS_ENDPOINT_ALIASES);
    String region = firstNonBlank(props, OSS_REGION_ALIASES);
    String token = firstNonBlank(props, OSS_SESSION_TOKEN_ALIASES);
    if (ak == null && endpoint == null && region == null) {
        return;
    }
    setter.accept("fs.oss.impl", JINDO_OSS_IMPL);
    setter.accept("fs.AbstractFileSystem.oss.impl", JINDO_OSS_ABSTRACT_IMPL);
    if (StringUtils.isNotBlank(ak)) {
        setter.accept("fs.oss.accessKeyId", ak);
        setter.accept("fs.oss.accessKeySecret", nullToEmpty(sk));
    }
    if (StringUtils.isNotBlank(token)) {
        setter.accept("fs.oss.securityToken", token);
    }
    if (StringUtils.isNotBlank(endpoint)) {
        setter.accept("fs.oss.endpoint", endpoint);
    }
    if (StringUtils.isNotBlank(region)) {
        setter.accept("fs.oss.region", region);
    }
}
```

### 3. (Optional, DLF parity) OSS endpoint-from-region for DLF

Legacy DLF derives the OSS endpoint from region (`OSSProperties.getOssEndpoint(region, accessPublic)`). `buildDlfHiveConf` already computes `accessPublic` and derives the DLF metastore endpoint. To match DLF parity when a user passes only `dlf.region`/`oss.region` (no `oss.endpoint`), derive `fs.oss.endpoint = "oss-" + region + (accessPublic ? "" : "-internal") + ".aliyuncs.com"` inside `buildDlfHiveConf` after `applyStorageConfig`, only when `fs.oss.endpoint` is still unset. Keep this DLF-local (do not put region-derivation in the shared `applyCanonicalOssConfig`, since the filesystem S3/OSS flavor legacy required an explicit endpoint there). This is a small, additive overlay in `buildDlfHiveConf` and can be scoped out if we want the absolute minimal credential-only fix; flag it explicitly in the PR so the reviewer decides. Recommended to include for DLF Finding 9.2 completeness.

### Update Javadoc

Amend the `applyStorageConfig` and `buildHadoopConfiguration` Javadocs to state that canonical `s3.*`/`oss.*`/`AWS_*` aliases are now translated to `fs.s3a.*`/`fs.oss.*` (ported from legacy `appendS3HdfsProperties` + `OSSProperties.initializeHadoopStorageConfig`), in addition to the `paimon.*` re-key and raw passthrough.

# Risk Analysis

**Parity vs legacy**
- S3A block: ports the credential-bearing subset of `appendS3HdfsProperties` verbatim (impl/disable-cache/endpoint/region/credentials-provider/access/secret/token). Intentionally omits the connection/timeout/path-style keys (`fs.s3a.connection.maximum`, `...request.timeout`, `...path.style.access`) — those are not credentials and Hadoop S3A supplies its own defaults; the pre-fix code already set none of them, so omitting them is the minimal-change boundary, not a regression. If a deployment relied on `use_path_style`, that is a separate, pre-existing gap (call out in PR, defer).
- Endpoint-from-region/URL derivation (`setEndpointIfPossible`) is NOT ported for filesystem; documented config always supplies an explicit endpoint, and porting the endpoint-pattern engine would pull in fe-core-coupled regex machinery. DLF region-derivation handled locally (item 3).
- OSS block ports `OSSProperties.initializeHadoopStorageConfig` (Jindo impls + `fs.oss.*` credentials/endpoint/region). The Aliyun Jindo class names are hard-coded literals, matching the existing `appendDlfOptions` pattern that already hard-codes `com.aliyun.datalake...ProxyMetaStoreClient` to avoid the compile dep.

**Shared-code blast radius**
- `applyStorageConfig` is called by `buildHadoopConfiguration` (filesystem, jdbc), `buildHmsHiveConf` (hms), `buildDlfHiveConf` (dlf). Adding canonical translation there fixes filesystem S3/OSS (Finding 9.1) and DLF OSS (Finding 9.2) in one place, and also improves hms/jdbc when canonical keys are used — all strictly additive (previously dropped keys now translated).
- **Back-compat**: the existing `paimon.*` re-key and raw `fs./dfs./hadoop.` branches are unchanged and run AFTER the canonical translation, so any user already passing `paimon.s3.access-key` or a raw `fs.s3a.access.key` gets identical output (their explicit key overwrites, last-write-wins — matching legacy `addResource(...)` then `appendUserHadoopConfig` ordering). No existing test should change behavior.

**Edge cases**
- Anonymous / no-credential public buckets: when `ak` is null we still set `fs.s3.impl`/`fs.s3a.impl`/`disable.cache` and endpoint/region but no credentials provider — matches legacy (`appendS3HdfsProperties` only sets the provider/keys inside the `isNotBlank(accessKey)` guard).
- Mixed `s3.*` + `oss.*` keys: S3A block populated from s3 aliases, OSS block from oss aliases; both emitted, no collision (different key namespaces). Legacy does the same (OSS appends S3A then OSS keys).
- DLF gate interaction: `requireOssStorageForDlf` still runs first in `PaimonConnector.createCatalog`; with canonical `oss.*` now translated, the gate-passes-but-no-creds mismatch is closed.
- `s3.endpoint` is also a legacy OSS alias. Because OSS detection keys off `oss.*`/`fs.oss.*`/`dlf.*` only, a pure-`s3.*` config does NOT trigger the OSS Jindo block (correct — it is an S3 catalog); a pure-`oss.*` config triggers both blocks (correct — legacy OSS appends S3A too).

# Test Plan

## Unit Tests
New tests in `PaimonCatalogFactoryTest.java` (matching the existing offline, plain-map, no-Mockito style and the existing `buildHadoopConfigurationNormalizesS3PrefixesAndCopiesRawKeys` / `buildDlfHiveConf*` tests). Each FAILS before the fix (key absent / value null) and PASSES after, and each comment encodes WHY (credential reaches FileIO), not just WHAT — designed to catch a regression that re-drops the canonical keys.

1. `buildHadoopConfigurationTranslatesCanonicalS3Credentials` — the exact regression scenario. Input `props("s3.access_key","ak","s3.secret_key","sk","s3.endpoint","s3.ap-east-1.amazonaws.com")`. Assert `fs.s3a.access.key=ak`, `fs.s3a.secret.key=sk`, `fs.s3a.endpoint=s3.ap-east-1.amazonaws.com`, `fs.s3a.aws.credentials.provider` = SimpleAWSCredentialsProvider, `fs.s3a.impl` set, `fs.s3.impl.disable.cache=true`. INTENT comment: without these the Paimon FileSystem catalog reaches S3 anonymously and the documented `test_paimon_s3.groovy` config gets access-denied. MUTATION: dropping `s3.access_key` (current behavior) leaves `fs.s3a.access.key` null → test red.

2. `buildHadoopConfigurationTranslatesAwsEnvAliases` — input `AWS_ACCESS_KEY`/`AWS_SECRET_KEY`/`AWS_ENDPOINT`/`AWS_REGION`/`AWS_TOKEN`. Assert the same `fs.s3a.*` keys incl. `fs.s3a.session.token` and `fs.s3a.endpoint.region`. INTENT: legacy accepted the AWS_* alias family; verifies alias priority list, not just the primary key.

3. `buildHadoopConfigurationDoesNotEmitCredsProviderForAnonymousBucket` — input only `s3.endpoint`/`s3.region` (no keys). Assert `fs.s3a.endpoint`/`fs.s3a.endpoint.region` set, `fs.s3.impl` set, but `fs.s3a.access.key` and `fs.s3a.aws.credentials.provider` absent. INTENT: anonymous public-dataset parity (legacy guards the provider behind isNotBlank(accessKey)).

4. `buildHadoopConfigurationExplicitFsS3aKeyOverridesCanonical` — input both `s3.access_key=canon` and `fs.s3a.access.key=explicit`. Assert `fs.s3a.access.key=explicit`. INTENT: locks the legacy last-write ordering (raw passthrough overlays canonical); guards against a future refactor that reverses precedence and breaks power users.

5. `buildDlfHiveConfTranslatesCanonicalOssCredentials` — input `dlf.access_key`/`dlf.secret_key`/`dlf.endpoint`/`dlf.region` + `oss.access_key`/`oss.secret_key`/`oss.endpoint`/`oss.region`. Assert the 8 `dlf.catalog.*` keys still present AND `fs.oss.accessKeyId`/`fs.oss.accessKeySecret`/`fs.oss.endpoint`/`fs.oss.region`/`fs.oss.impl`(Jindo) set. INTENT: closes Finding 9.2 — gate-passes-but-no-OSS-creds; the assertion that `fs.oss.accessKeyId` is non-null is exactly what fails today.

6. `requireOssStorageForDlfThenBuildDlfHiveConfYieldsOssCreds` — integration of the gate + builder with canonical `oss.*` only (no `paimon.fs.oss.*`). First `assertDoesNotThrow(requireOssStorageForDlf(props))`, then assert `buildDlfHiveConf(props).get("fs.oss.accessKeyId")` non-null. INTENT: encodes the BLOCKER end-to-end — the gate and the translation must agree on the same key set.

7. (If item 3 of plan included) `buildDlfHiveConfDerivesOssEndpointFromRegion` — input `oss.region=cn-hangzhou`, no `oss.endpoint`, default access. Assert `fs.oss.endpoint=oss-cn-hangzhou-internal.aliyuncs.com`; with `dlf.access.public=true` assert public variant. INTENT: DLF parity with `OSSProperties.getOssEndpoint`.

Keep one explicit negative test untouched/extended: existing `buildHadoopConfigurationNormalizesS3PrefixesAndCopiesRawKeys` must still pass (confirms `paimon.s3.*` back-compat unchanged).

## E2E Tests
- `regression-test/suites/external_table_p0/paimon/test_paimon_s3.groovy` is the natural live verifier (already uses `s3.access_key/s3.secret_key/s3.endpoint` + `paimon.catalog.type=filesystem`). It is **live-only / CI-gated**: runs only when `enablePaimonTest=true` and real `AWSAK`/`AWSSK` credentials + a private S3 bucket are configured (`test_paimon_s3.groovy:60-61`). Cannot run in this offline environment (no creds, no bucket); it is the cutover live-e2e gate, consistent with the connector's own `buildHmsHiveConf`/`buildDlfHiveConf` notes that the live metastore=hive path "MUST be verified by live-e2e before cutover". No new e2e file needed; the offline UTs above are the deterministic regression guard. For DLF (Finding 9.2) a DLF live suite would be needed but requires Aliyun DLF + OSS credentials and the host hive-catalog-shade — defer to live cutover, same gating rationale.

---

# ✅ IMPL SUMMARY (2026-06-11)

**Status: DONE — build+UT green (PaimonCatalogFactoryTest 38 tests, 0 fail/err/skip; HEAD uncommitted).**

## Fix
One file changed for production (`PaimonCatalogFactory.java`), one for tests (`PaimonCatalogFactoryTest.java`). fe-core-free; `bash tools/check-connector-imports.sh` clean.

- Added canonical alias constant arrays (`S3_*_ALIASES`, `OSS_*_ALIASES`) + impl/provider literals (`S3A_IMPL`, `S3A_SIMPLE_CRED_PROVIDER`, `JINDO_OSS_IMPL`, `JINDO_OSS_ABSTRACT_IMPL`).
- `applyStorageConfig` now runs `applyCanonicalS3Config` + `applyCanonicalOssConfig` FIRST, then the pre-existing `paimon.*` re-key + raw `fs./dfs./hadoop.` passthrough (last-write-wins = legacy precedence). The two pre-existing branches are byte-unchanged.
- `applyCanonicalS3Config`: canonical `s3.*`/`AWS_*` → `fs.s3a.*` (impl/disable-cache always; endpoint/region when present; provider+access/secret/token only when an access key is present — anonymous parity).
- `applyCanonicalOssConfig`: canonical `oss.*`/`fs.oss.*`/`dlf.*` → Jindo `fs.oss.*`. Detection keys off OSS-specific aliases only, so a pure-`s3.*` config does not trigger the Jindo block.
- **Optional item 3 INCLUDED** (DLF parity, Finding 9.2 completeness): `buildDlfHiveConf` derives `fs.oss.endpoint` from `oss.region`/`dlf.region` when no explicit `oss.endpoint` was given (`oss-<region>[-internal].aliyuncs.com`, default non-public = `-internal`). Kept DLF-local.

## Tests (7 new, all fail-before / pass-after)
`buildHadoopConfigurationTranslatesCanonicalS3Credentials`, `…TranslatesAwsEnvAliases`, `…DoesNotEmitCredsProviderForAnonymousBucket`, `…ExplicitFsS3aKeyOverridesCanonical`, `buildDlfHiveConfTranslatesCanonicalOssCredentials`, `requireOssStorageForDlfThenBuildDlfHiveConfYieldsOssCreds`, `buildDlfHiveConfDerivesOssEndpointFromRegion`.

## Correction discovered during impl
The design's planned `assertNull(conf.get("fs.s3a.aws.credentials.provider"))` for the anonymous-bucket test is WRONG: Hadoop `Configuration` resolves a baked-in DEFAULT provider chain (`Temporary,Simple,Env,IAM`) from the hadoop-aws jar, so the key is never null. Production code is correct (it does not override the provider when the access key is blank). The assertion was changed to `assertNotEquals(SimpleAWSCredentialsProvider-single, …)` — which still catches the real mutation (dropping the `isNotBlank(ak)` guard → forcing Simple-only → breaks env/IAM fallback). access.key (no Hadoop default) is still asserted null.

## Live-e2e (gated, NOT run here)
`regression-test/.../paimon/test_paimon_s3.groovy` (filesystem+S3) and a DLF/OSS live suite — both need real creds + buckets; deferred to cutover live-e2e.
