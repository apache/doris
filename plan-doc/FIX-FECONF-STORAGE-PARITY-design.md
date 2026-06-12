# FIX-FECONF-STORAGE-PARITY — design

> Cluster: P8-1 / P8-2 / P8-3 / P8-4 / P9-2 / P9-3 (round-3 re-review). User-signed **FULL legacy parity**.
> Pure **connector-only** change (`PaimonCatalogFactory.java` + its test). No fe-core / SPI / BE.
> Design red-team (`wf_a6385c61-669`, 5 skeptics + completeness critic) ran **before** this doc; its findings
> are folded in below (each marked **[RT]**). S3-endpoint-from-region inclusion is **user-approved** (2026-06-12).

## Problem

The connector cannot import fe-core, so `PaimonCatalogFactory` rebuilds the FE-side Hadoop
`Configuration` / `HiveConf` from the raw property map with **literal** key logic (same pattern as the
existing `applyCanonicalS3Config` / `applyCanonicalOssConfig`). That reconstruction is **incomplete** vs
the legacy `*Properties` classes, so a paimon catalog on several storage backends fails FE-side
catalog/metadata access (the live `FileSystemCatalog` / `HiveCatalog` / `JdbcCatalog` cannot resolve the
storage FileIO). Consumers of the gap: `buildHadoopConfiguration` (filesystem, jdbc), `buildHmsHiveConf`
(hms), `buildDlfHiveConf` (dlf) — all route through `applyStorageConfig`.

Concrete gaps:
- **P8-1 / P8-3 (OSS)**: a region-only OSS catalog (no explicit `oss.endpoint`) gets no `fs.oss.endpoint`;
  and an OSS catalog gets none of the `fs.s3.impl` / `fs.s3a.*` base keys legacy emits (so `s3://`-over-OSS
  back-compat breaks).
- **P8-2 / P9-3 (S3 path-style / MinIO)**: `applyCanonicalS3Config` never emits `fs.s3a.path.style.access`
  nor the `fs.s3a.connection.maximum/request.timeout/timeout` tuning keys.
- **P9-2 (COS / OBS)**: there is **no** COS or OBS handling at all — a `cosn://` / `obs://` paimon catalog
  gets no `fs.cosn.*` / `fs.obs.*` keys.
- **P8-4 (HMS username)**: `buildHmsHiveConf` only copies the literal `hadoop.username`; a user who sets the
  `hive.metastore.username` alias has it land as an inert verbatim `hive.*` key, never reaching `hadoop.username`.
- **(user-approved, S3 endpoint-from-region)**: structurally identical to P8-1 — a region-only AWS-S3
  catalog gets no `fs.s3a.endpoint`. Legacy `S3Properties.getEndpointFromRegion` derives
  `https://s3.<region>.amazonaws.com`.

## Root Cause

`applyStorageConfig` runs only two canonical blocks (`applyCanonicalS3Config`, `applyCanonicalOssConfig`),
each emitting a **subset** of what the corresponding legacy `*Properties.initializeHadoopStorageConfig`
(plus its `super.appendS3HdfsProperties` base) emits, and there is **no COS/OBS block**. Legacy details
(parity references; do not modify these files):
- `AbstractS3CompatibleProperties.appendS3HdfsProperties` (the shared S3A base, inherited by S3/OSS/COS/OBS
  via `super`): `fs.s3.impl`, `fs.s3a.impl`, `fs.s3{,a}.impl.disable.cache=true`, `fs.s3a.endpoint`,
  `fs.s3a.endpoint.region`, creds (gated on `isNotBlank(accessKey)`), **`fs.s3a.connection.maximum`,
  `fs.s3a.connection.request.timeout`, `fs.s3a.connection.timeout`, `fs.s3a.path.style.access`**.
- **[RT — critic, missed by all skeptics]** the 4 tuning **defaults are per-subclass**:
  `S3Properties` = **50 / 3000 / 1000** (`S3Properties.java:129,136,143`; aliases incl. `AWS_MAX_CONNECTIONS`
  etc.), while `OSS/COS/OBS` = **100 / 10000 / 10000**. A single shared default would silently mis-tune S3.
- `OSSProperties` adds Jindo `fs.oss.*`; derives endpoint `oss-<region>[-internal].aliyuncs.com` from region
  when blank (`initNormalizeAndCheckProps:277-279` → `getOssEndpoint`, `dlfAccessPublic` default false →
  `-internal`).
- `COSProperties.initializeHadoopStorageConfig:177-181`: `fs.cos.impl`=S3AFileSystem, `fs.cosn.impl`=S3AFileSystem,
  and **unconditionally** `fs.cosn.bucket.region`, `fs.cosn.userinfo.secretId`, `fs.cosn.userinfo.secretKey`.
- `OBSProperties.initializeHadoopStorageConfig:194-205`: native `fs.obs.impl`/`fs.AbstractFileSystem.obs.impl`
  when `org.apache.hadoop.fs.obs.OBSFileSystem` is classpath-available, else `fs.obs.impl`=S3AFileSystem; plus
  **unconditional** `fs.obs.access.key`, `fs.obs.secret.key`, `fs.obs.endpoint`.
- **[RT — skeptic 2]** legacy selects exactly ONE backend via `guessIsMe` keyed on **endpoint/uri PATTERN**
  (COS=`myqcloud.com`, OBS=`myhuaweicloud.com`), NOT on scheme-prefixed keys. So a `cosn://` catalog
  configured with only `s3.endpoint=cos.<region>.myqcloud.com` (no `cos.*` key) is a real shape a
  scheme-key-only gate would miss.
- `HMSBaseProperties`: `@ConnectorProperty(names={"hive.metastore.username","hadoop.username"})` →
  `hiveConf.set(HADOOP_USER_NAME /*="hadoop.username"*/, hmsUserName)` (`:83-87,201-202`).

## Design

All changes live in `applyStorageConfig` and its callees. Introduce ONE shared helper and TWO new blocks,
extend the existing two blocks, and fix 4d. The raw `fs./dfs./hadoop.` passthrough stays **last**
(last-write-wins; existing `buildHadoopConfigurationExplicitFsS3aKeyOverridesCanonical` parity).

### Shared `applyS3aBaseConfig(setter, ak, sk, token, endpoint, region, maxConn, reqTimeout, connTimeout, pathStyle)`
Faithful port of `appendS3HdfsProperties`. **[RT — skeptic 4]** takes the creds AND the tuning as
**explicit caller-resolved params** (each block resolves from its OWN aliases with its OWN defaults — the
helper never re-resolves from props). Emits:
- unconditional: `fs.s3.impl`, `fs.s3a.impl`, `fs.s3{,a}.impl.disable.cache=true`.
- `fs.s3a.endpoint` / `fs.s3a.endpoint.region` — **conditional on `isNotBlank`** (documented deviation: legacy
  is unconditional via `checkNotNull`, but the connector has no `setRegionIfPossible` throw-guard; matches the
  current connector style).
- creds (gated on `isNotBlank(ak)`, anonymous-safe): `fs.s3a.aws.credentials.provider`=Simple,
  `fs.s3a.access.key`, `fs.s3a.secret.key`=`nullToEmpty(sk)`, `fs.s3a.session.token` (if token).
- unconditional tuning: `fs.s3a.connection.maximum`=maxConn, `…request.timeout`=reqTimeout,
  `…connection.timeout`=connTimeout, `fs.s3a.path.style.access`=pathStyle.

### `applyCanonicalS3Config` (P8-2, P9-3, + S3 endpoint-from-region)
Resolve S3 creds (existing aliases). Gate unchanged: `if (ak==null && endpoint==null && region==null) return;`
- **NEW (user-approved)**: `if (endpoint blank && region present) endpoint = "https://s3." + region + ".amazonaws.com";`
  (mirrors `S3Properties.getEndpointFromRegion:420`).
- Resolve tuning with **S3 defaults 50/3000/1000** from `{s3.connection.maximum, AWS_MAX_CONNECTIONS}` /
  `{s3.connection.request.timeout, AWS_REQUEST_TIMEOUT_MS}` / `{s3.connection.timeout, AWS_CONNECTION_TIMEOUT_MS}`;
  pathStyle default `false` from `{use_path_style, s3.path-style-access}`.
- `applyS3aBaseConfig(...)`.

### `applyCanonicalOssConfig` (P8-1, P8-3)
Resolve OSS creds (existing aliases). Gate unchanged.
- **NEW (4a)**: `if (endpoint blank && region present)` derive
  `endpoint = "oss-" + region + (publicAccess ? "" : "-internal") + ".aliyuncs.com"`, where
  `publicAccess = toBoolean(firstNonBlank(props, "dlf.access.public", "dlf.catalog.accessPublic"))` (default false).
  **[RT — skeptic 3]** this is the SAME derivation as the DLF-local block; therefore **REMOVE** the now-dead
  guarded block in `buildDlfHiveConf` (DLF still derives via this shared path; the move also—correctly—grants
  the **HMS** flavor the same legacy `OSSProperties.of()` derivation).
- Resolve tuning with **OSS defaults 100/10000/10000** (`{oss.connection.maximum, s3.connection.maximum}` etc.;
  pathStyle from `{oss.use_path_style, use_path_style, s3.path-style-access}`).
- **NEW (4a)**: `applyS3aBaseConfig(...)` (emit the S3A base for OSS).
- THEN the existing Jindo `fs.oss.*` block (kept as-is, incl. its existing `isNotBlank` guards — a pre-existing
  conditional-vs-unconditional deviation NOT in this fix's scope; after derivation `fs.oss.endpoint` now emits).

### `applyCanonicalCosConfig` (NEW, 4c)
COS aliases (from `COSProperties`): access `{cos.access_key, s3.access_key, s3.access-key-id, AWS_ACCESS_KEY,
access_key, ACCESS_KEY}`; secret `{cos.secret_key, s3.secret_key, s3.secret-access-key, AWS_SECRET_KEY,
secret_key, SECRET_KEY}`; token `{cos.session_token, s3.session_token, s3.session-token, session_token}`;
endpoint `{cos.endpoint, s3.endpoint, AWS_ENDPOINT, endpoint, ENDPOINT}`; region `{cos.region, s3.region,
AWS_REGION, region, REGION}`.
- **[RT — skeptic 2] Detect** = `anyKeyStartsWith(props, "cos.")` **OR** resolved endpoint contains
  `myqcloud.com` **OR** `warehouse` contains `myqcloud.com`. Gate: `if (!detected) return;`
- Tuning defaults 100/10000/10000 (`{cos.connection.*, s3.connection.*}`; pathStyle `{cos.use_path_style,
  use_path_style, s3.path-style-access}`).
- `applyS3aBaseConfig(...)` **FIRST** (super-first ordering), THEN **[RT — critic] unconditional**:
  `fs.cos.impl`=S3AFileSystem, `fs.cosn.impl`=S3AFileSystem, `fs.cosn.bucket.region`=`nullToEmpty(region)`,
  `fs.cosn.userinfo.secretId`=`nullToEmpty(ak)`, `fs.cosn.userinfo.secretKey`=`nullToEmpty(sk)`.

### `applyCanonicalObsConfig` (NEW, 4c)
OBS aliases (from `OBSProperties`, same shape as COS with `obs.` prefix). Detect = `anyKeyStartsWith(props,
"obs.")` OR resolved endpoint contains `myhuaweicloud.com` OR `warehouse` contains `myhuaweicloud.com`.
- Tuning defaults 100/10000/10000.
- `applyS3aBaseConfig(...)` FIRST, THEN **[RT — skeptic 5(i)]** native-vs-s3a by
  `isClassAvailable("org.apache.hadoop.fs.obs.OBSFileSystem")` (`Class.forName(name, false,
  PaimonCatalogFactory.class.getClassLoader())` — child-first delegates non-plugin classes to the host parent,
  so this answers the same question legacy did):
  - native → `fs.obs.impl`=`org.apache.hadoop.fs.obs.OBSFileSystem`, `fs.AbstractFileSystem.obs.impl`=`org.apache.hadoop.fs.obs.OBS`.
  - else → `fs.obs.impl`=S3AFileSystem.
  - **unconditional**: `fs.obs.access.key`=`nullToEmpty(ak)`, `fs.obs.secret.key`=`nullToEmpty(sk)`,
    `fs.obs.endpoint`=`nullToEmpty(endpoint)`.

### `applyStorageConfig` order
`applyCanonicalS3Config` → `applyCanonicalOssConfig` → `applyCanonicalCosConfig` → `applyCanonicalObsConfig`
→ raw passthrough (last). **[RT — skeptic 1]** when a `cos.*`/`myqcloud` catalog ALSO matches the S3 block
(shared `s3.endpoint`), the COS block runs AFTER S3 so its (identical) S3A base + the `fs.cosn.*` keys win
deterministically — matches legacy, which selects COS for that shape.

### 4d `buildHmsHiveConf` (P8-4)
Replace `copyIfPresent(props, hiveConf, "hadoop.username")` with
`String u = firstNonBlank(props, "hive.metastore.username", "hadoop.username"); if (isNotBlank(u))
hiveConf.set("hadoop.username", u);` (alias priority `hive.metastore.username` first; target key
`hadoop.username` == `HADOOP_USER_NAME`). This resolution must run **AFTER** `applyStorageConfig` — the raw
`hadoop.*` passthrough there would otherwise re-copy a literal `hadoop.username` and clobber the resolved
alias (caught by the username-priority test).

### 4e `buildHmsHiveConf` kerberos-ordering (folded in — pre-existing MAJOR, user-approved 2026-06-12)
Impl-verification (`wf_f90260cb-5e6`) found a **pre-existing** (B1, not introduced by this fix) clobber with
the SAME root cause as 4d: the kerberos block forced `hadoop.security.authentication=kerberos`, but
`applyStorageConfig`'s raw `hadoop.*` passthrough ran AFTER it and re-copied a user-supplied literal
`hadoop.security.authentication=simple` — leaving `auth=simple` with `sasl.enabled=true` (inconsistent
HiveConf, breaks the live GSSAPI handshake) for a **kerberized-HMS + simple-HDFS** catalog. Legacy
`HMSBaseProperties.checkAndInit` runs `initHadoopAuthenticator` LAST, so kerberos is authoritative. **Fix**:
relocate the entire kerberos-conditional block to AFTER `applyStorageConfig` (alongside the 4d username
block), mirroring legacy's ordering. The socket-timeout default + the `hive.*` service-principal stay correct
(neither is a `hadoop.*` passthrough key). User chose to fold this into the FIX-4 commit (same root cause,
same method).

### New small helpers
`firstNonBlankOrDefault(props, default, keys...)`; `anyKeyStartsWith(props, prefix)`;
`isClassAvailable(className)`; `containsToken(value, token)`.

## Implementation Plan
1. Add alias-array constants for S3 tuning, OSS tuning, and the full COS/OBS cred + tuning families.
2. Add `applyS3aBaseConfig` + the 4 small helpers.
3. Refactor `applyCanonicalS3Config` (S3 endpoint-from-region + tuning) and `applyCanonicalOssConfig`
   (endpoint-from-region + S3A base + tuning) to call the helper.
4. Add `applyCanonicalCosConfig` + `applyCanonicalObsConfig`; wire both into `applyStorageConfig`.
5. Remove the dead DLF-local OSS-endpoint derivation block from `buildDlfHiveConf`.
6. Fix 4d in `buildHmsHiveConf`.
7. Tests (below). Build connector-only; checkstyle; import-gate.

## Risk Analysis
- **DLF regression** [RT-3]: removing the DLF-local block — DLF still derives via the shared OSS block with the
  same `dlf.access.public` source. Verified by the 4 existing DLF tests (must stay green).
- **Existing S3 tests** [RT-4]: all 13 storage tests assert only old keys; new keys are additive, S3
  endpoint-from-region only triggers on a region-only-no-endpoint S3 case (none exist today). Passthrough-last
  preserved.
- **Wrong tuning defaults** [RT-critic]: mitigated by per-scheme defaults (50/3000/1000 for S3; 100/10000/10000
  for OSS/COS/OBS) + RED-first divergent-default tests.
- **Over-emission**: COS/OBS emit `fs.s3a.*` for `cosn://`/`obs://` — REQUIRED (those FS impls are S3A and read
  `fs.s3a.*`); inert extras (`fs.cosn.*` on an `s3://` catalog) match legacy.
- **Known residual (documented, out of scope)**: OSS endpoint-PATTERN detection (`aliyuncs.com`) is NOT added
  (pre-existing gap in the existing OSS block; no failing case; not in the approved cluster). The
  conditional-vs-unconditional `fs.s3a.endpoint/region` deviation is documented in a code comment.

## Test Plan

### Unit Tests (`PaimonCatalogFactoryTest`) — each is RED-before-GREEN (mutation noted)
- **S3 endpoint-from-region**: region-only S3 (no endpoint) → `fs.s3a.endpoint == https://s3.<region>.amazonaws.com`.
  Mutation: drop derivation → null.
- **S3 tuning defaults**: S3 catalog (endpoint+region, no conn keys) → `fs.s3a.connection.maximum==50`,
  `request.timeout==3000`, `connection.timeout==1000`, `path.style.access==false`. Mutation: shared 100/10000/10000 → red.
- **S3 path-style override**: `use_path_style=true` (and `s3.path-style-access=true`) → `fs.s3a.path.style.access==true`.
- **OSS endpoint-from-region** (filesystem AND hms flavor): `oss.region` only → `fs.oss.endpoint==oss-<region>-internal.aliyuncs.com`;
  with `dlf.access.public=true` → public form. Mutation: no derivation / wrong public-internal → red.
- **OSS S3A base**: OSS catalog → `fs.s3a.impl==S3AFileSystem` + `fs.s3a.connection.maximum==100`. Mutation: OSS block skips S3A base → red.
- **COS (cos.* keys, `cosn://`)**: `cos.access_key/secret_key/endpoint` → `fs.cosn.impl==S3AFileSystem`,
  `fs.cos.impl==S3AFileSystem`, `fs.cosn.userinfo.secretId==ak`, `fs.cosn.userinfo.secretKey==sk`,
  `fs.cosn.bucket.region==region`, AND `fs.s3a.endpoint==endpoint` + `fs.s3a.connection.maximum==100`.
- **COS pattern-detect (`s3.endpoint=cos…myqcloud.com`, no `cos.*` key)**: `fs.cosn.impl` present (the gate that a
  scheme-key-only design would miss). Mutation: cos.*-key-only gate → fs.cosn.impl null → red.
- **COS unconditional region**: COS catalog with NO region → `fs.cosn.bucket.region` present (`""`, not null).
- **OBS (obs.* keys, `obs://`)**: `obs.access_key/secret_key/endpoint=…myhuaweicloud.com` → `fs.obs.impl` present
  (native or s3a), `fs.obs.access.key==ak`, `fs.obs.secret.key==sk`, `fs.obs.endpoint==endpoint`, AND S3A base.
- **OBS pattern-detect (`s3.endpoint=…myhuaweicloud.com`, no `obs.*` key)**: `fs.obs.impl` present.
- **4d HMS username alias**: `hive.metastore.username=foo` (no `hadoop.username`) → `hadoop.username==foo`.
  Mutation: only literal-copy → null. Priority: both set → `hive.metastore.username` wins (this test caught a
  real ordering bug: the username resolution had to move AFTER the storage overlay or the raw `hadoop.*`
  passthrough clobbers it).
- **4e kerberos survives simple-HDFS passthrough**: `hive.metastore.authentication.type=kerberos` +
  `hadoop.security.authentication=simple` → `hadoop.security.authentication==kerberos` + `sasl.enabled==true`.
  Mutation: kerberos block before the storage overlay → clobbered to `simple` → red.
- **DLF unchanged**: existing 4 DLF tests stay green (regression guard for the block removal).

### E2E Tests
None added. Live coverage exists in `paimon_base_filesystem.groovy` (catalog_oss/cos/cosn/obs) +
`test_paimon_dlf_catalog.groovy` + `test_paimon_hms_catalog.groovy`, all CI-gated (`enablePaimonTest=false`).
Note as gated; do not claim executed.
