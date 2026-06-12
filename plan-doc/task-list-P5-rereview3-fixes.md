# Task list — P5 Paimon Round-3 re-review fixes

> Source: [reviews/P5-paimon-rereview3-2026-06-12.md](./reviews/P5-paimon-rereview3-2026-06-12.md).
> User-approved scope (2026-06-12): **P9-1 fix · P7-1 fix · P2-1 restore-reset · FE-config FULL legacy parity.**
> Execute each via the `step-by-step-fix` skill: design doc → impl → tests → **independent commit**.
> Keep **legacy `datasource/paimon/*` in-tree** as the parity reference until all fixes land (then B8 deletion).

## Commit hygiene (re-read before any `git add`)
- **Hard pre-req**: scrub `regression-test/conf/regression-conf.groovy` (plaintext Aliyun key) + remove scratch
  (`.audit-scratch/`, `conf.cmy/`, `META-INF/`, `*.bak`). **Path-whitelist `git add` — NEVER `git add -A`.**
- Each fix = one commit; message = `fix: <ID>` + root cause + solution + tests, trailing
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Build/verify (reuse)
- maven absolute `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`; verify via surefire XML + `MVN_EXIT` ([[doris-build-verify-gotchas]]).
- fe-core change → `-pl :fe-core -am`; SPI change → `-pl :fe-connector-api`/`:fe-connector-spi -am`.
- checkstyle: connector `mvn -pl :fe-connector-paimon checkstyle:check`; fe-core `mvn -pl :fe-core checkstyle:check`.
- import-gate: `bash tools/check-connector-imports.sh` (connector may import only `org.apache.doris.{thrift,connector,extension,filesystem}`).
- test harness: `RecordingConnectorContext` / `RecordingPaimonCatalogOps` / `FakePaimonTable` / `PaimonScanPlanProviderTest` / `PaimonIncrementalScanParamsTest` / `PaimonCatalogFactoryTest` / `DefaultConnectorContextNormalizeUriTest` (fe-core). live-e2e is CI-gated (`enablePaimonTest=false`) — note as gated, don't claim it ran.

---

## ✅ FIX-1 — `FIX-REST-VENDED-URI-NORMALIZE` (P9-1, **BLOCKER**) — **DONE** (commit `c376aba1264`)
> Design + adversarial red-team (DESIGN-SOUND): `FIX-REST-VENDED-URI-NORMALIZE-design.md`. SPI overload
> `normalizeStorageUri(uri, token)` + fe-core vended-overlay normalize (legacy "vended replaces static")
> + connector threads once-per-scan `extractVendedToken` to both native normalize sites. Verified:
> connector 42/0/0; fe-core NormalizeUri 7/0 (incl. 3 new), Vend 2/0; checkstyle 0; import-gate clean.
> Positive RESTTokenFileIO path E2E-gated. **Next: FIX-2.**
**Symptom**: `SELECT` over a Paimon **REST**-catalog table on **object storage** (oss/cos/obs/s3a),
native reader (ORC/Parquet, default) → FE planning throws `StoragePropertiesException: No storage
properties found for schema: oss`. Worked under legacy. Escape hatch: `force_jni_scanner=true`.

**Root cause**: native URI normalization uses the **static** catalog storage map, which is **empty by
design for REST** (`CatalogProperty.initStorageProperties:186-192` → `Maps.newHashMap()` when vended
creds enabled). Chain: `PaimonScanPlanProvider.normalizeUri:485-487` → `context.normalizeStorageUri`
→ `DefaultConnectorContext:203` `LocationPath.of(rawUri, staticSupplier.get(), normalize=true)`
(supplier = `PluginDrivenExternalCatalog:157-158`) → empty map → `findStorageProperties`==null → throw.
`shouldUseNativeReader:783` has **no flavor gate**, so REST native reads hit it. Called on the
data-file path (`buildNativeRange:439`) **and** the deletion-vector path (`:448`).

**Legacy parity ref**: `paimon/source/PaimonScanNode.java:171-176` re-derives a **vended-overlay** map
(`VendedCredentialsFactory.getStoragePropertiesMapWithVendedCredentials`) and uses it for
`LocationPath.of` at `:443` (data) / `:296` (DV).

**Fix approach**: route the **vended-overlay** storage map into normalization (legacy parity). The
connector already computes vended creds for the BE overlay (`DefaultConnectorContext.vendStorageCredentials:156-180`,
consumed at `PaimonScanPlanProvider:557-562`) — reuse that map.
- **Design decision (pick in design doc)**:
  (a) **[recommended]** add SPI overload `ConnectorContext.normalizeStorageUri(rawUri, Map<String,String> storageProps)`;
      connector passes the vended-merged map it already has at scan time. Explicit, matches legacy.
  (b) make the supplier vended-aware (harder — vended creds are per-token/dynamic, supplier is catalog-static).
  (c) fallback: when static map lacks the scheme entry, use the vended map. Narrower but implicit.
**Sites**: connector `PaimonScanPlanProvider.normalizeUri` + 2 call sites (`:439`, `:448`); fe-core
`DefaultConnectorContext.normalizeStorageUri:193-204`; SPI `fe-connector-api`/`-spi` if overload added.
**Tests**: `DefaultConnectorContextNormalizeUriTest` — add a **vended-REST** case (static map empty +
vended map carries an oss/s3 entry → normalize succeeds; this is the gap that hid the bug twice);
connector test for `buildNativeRange` data-file **and** DV under a vended context.
**Build**: SPI + fe-core + connector. **Commit**: `fix: FIX-REST-VENDED-URI-NORMALIZE`.
**Reconciliation note**: DV-025 deferred this exact corner to FIX-STATIC-CREDS-BE/FIX-REST-VENDED, but
those fixed cred-downflow, not `normalizeStorageUri`; deferral never closed → still live (report §D.1).

## ✅ FIX-2 — `FIX-JNI-FILE-FORMAT` (P7-1, MAJOR) — **DONE** (commit `2e845e88bf9`)
> Design: `FIX-JNI-FILE-FORMAT-design.md`. `buildJniScanRange`/`buildCountRange` now emit the real
> `defaultFileFormat` (not `"jni"`); `buildCountRange` gained the param (threaded from call site);
> Builder default `"jni"`→`""`. JNI routing gated by `paimon.split` presence (not the format string), so
> safe. Verified: connector 262/0/1skip (ScanPlanProvider 43/0); checkstyle 0; import-gate clean.
> **Next: FIX-3.**
**Root cause**: `PaimonScanPlanProvider.buildJniScanRange:610` and `buildCountRange:641` hardcode
`.fileFormat("jni")`; the correct `defaultFileFormat` (`= table.options().getOrDefault(FILE_FORMAT,"parquet")`,
computed at `:326-327`) is **passed into the methods and ignored**. `PaimonScanRange:186/244` then emits
`file_format="jni"`. BE `paimon_cpp_reader.cpp:397-411` **backfills** Paimon `FILE_FORMAT`/`MANIFEST_FORMAT`
from this field (guarded "if unset"); the comment says it exists to avoid the `manifest.format=avro`
default → with `"jni"` and an unset `manifest.format` the cpp reader gets an invalid format → manifest
read breaks.
**Legacy parity ref**: `paimon/source/PaimonScanNode.java:259,288` sets the real `"orc"/"parquet"`.
**Fix**: pass the already-available `defaultFileFormat` into `buildJniScanRange`/`buildCountRange`
instead of `"jni"` (and reconsider the `PaimonScanRange.Builder` default `:244`).
**Tests**: `PaimonScanPlanProviderTest` — assert JNI + count ranges carry the real format, not `"jni"`.
**Build**: connector only. No BE change. **Commit**: `fix: FIX-JNI-FILE-FORMAT`.
**Open (non-blocking)**: BE routing — whether a JNI-tagged split ever reaches the cpp reader vs the JNI
reader; fix is correctness-improving regardless.

## ▶ FIX-3 — `FIX-INCR-SCAN-RESET` (P2-1, MAJOR; was NIT in rereview2) — restore parity
**Root cause**: `PaimonIncrementalScanParams.java:222-265` deliberately strips legacy's defensive
null-reset (`PAIMON_SCAN_SNAPSHOT_ID=null`, `PAIMON_SCAN_MODE=null`). On a table that **persists**
`scan.*` options, the freshly-loaded base table inherits them and they're not reset before the
incremental-between window is applied → potential wrong @incr scan.
**Legacy parity ref**: `paimon/source/PaimonScanNode.java:840-846` seeds both nulls (re-asserts
`scan.mode=null` in the snapshot branch), applied via `baseTable.copy(getIncrReadParams())` `:896`.
**Fix**: re-add the null-reset of `scan.snapshot-id` + `scan.mode` before `table.copy(scanOptions)`.
- **Design decision**: the connector's `ConnectorMvccSnapshot.Builder.property()` **rejects null values**
  (why the keys were stripped originally). So thread the reset directly into the `table.copy(...)` map at
  `PaimonScanPlanProvider.resolveScanTable` (which can hold key→null), OR allow null specifically on the
  incremental options path. Decide in the design doc.
**Sites**: `PaimonIncrementalScanParams.java:222-265`; `PaimonConnectorMetadata.applySnapshot` /
`PaimonScanPlanProvider.resolveScanTable` (where `table.copy(scanOptions)` runs).
**Tests**: `PaimonIncrementalScanParamsTest` — assert the reset keys are present/applied for @incr.
**Build**: connector only. **Commit**: `fix: FIX-INCR-SCAN-RESET`.

## ▶ FIX-4 — `FIX-FECONF-STORAGE-PARITY` (cluster: P8-1/P8-2/P8-3/P8-4/P9-2/P9-3) — FULL legacy parity
**Root cause (shared)**: `PaimonCatalogFactory.buildHadoopConfiguration:390-394` rebuilds the FE-side
Hadoop `Configuration` from RAW props (the connector cannot import fe-core `OSSProperties`/`COSProperties`/
`OBSProperties`/`HMSBaseProperties`), and the reconstruction (`applyStorageConfig:412-426`,
`applyCanonicalS3Config:437-465`, `applyCanonicalOssConfig:475-499`, alias arrays `:87-106`) is
**incomplete** vs legacy. Affects filesystem/jdbc/HMS flavors → catalog/metadata access fails on the
missing backends. Constraint: replicate legacy key logic with **literals** (same pattern as existing
`applyCanonical*`), no fe-core import.
**Recommended split (clean independent commits)**:
- **4a `FIX-FECONF-OSS`** (P8-1, P8-3): emit `fs.oss.endpoint` derived from region when endpoint blank
  (replicate legacy `OSSProperties.getOssEndpoint` → `oss-<region>[-internal].aliyuncs.com`,
  ref `:277-279,314-326`); also emit the S3A keys for OSS that legacy emitted (`fs.s3.impl`/`fs.s3a.*`).
- **4b `FIX-FECONF-S3`** (P8-2, P9-3): emit `fs.s3a.path.style.access` from `use_path_style`/
  `s3.path-style-access` + connection/timeout keys (MinIO/path-style).
- **4c `FIX-FECONF-COS-OBS`** (P9-2): add `cos.*`/`obs.*` alias arrays + emit COS keys
  (`fs.cosn.impl`, `fs.cosn.userinfo.secretId/secretKey`, `fs.cosn.bucket.region`; ref `COSProperties:174-182`)
  and OBS keys (`fs.obs.impl`, `fs.AbstractFileSystem.obs.impl`, `fs.obs.access.key/secret.key`; ref `OBSProperties:194-204`).
- **4d `FIX-FECONF-HMS-USER`** (P8-4): emit `hive.metastore.username` alias for `hadoop.username` in `buildHmsHiveConf`.
**Tests**: `PaimonCatalogFactoryTest` — one case per backend (region-only OSS → `fs.oss.endpoint`;
COS props → `fs.cosn.*`; OBS → `fs.obs.*`; S3 path-style; HMS username alias).
**Build**: connector only (`PaimonCatalogFactory` is pure connector). **Commits**: 4a–4d (or one
`fix: FIX-FECONF-STORAGE-PARITY` if you prefer a single commit).

---

## Suggested order & dependencies
No hard deps. Suggested: **FIX-1 (BLOCKER)** → FIX-2 → FIX-3 → FIX-4a…4d.
FIX-1 & FIX-2 both edit `PaimonScanPlanProvider` (sequence to avoid churn). FIX-3 edits
`PaimonIncrementalScanParams`/scan-table copy. FIX-4 edits only `PaimonCatalogFactory` (independent).

## NOT in this fix scope — proposed deviations (confirm before B8 / final cleanup)
Accepted-as-deviation candidates (report §F/§G), pending explicit user sign-off:
- **MINOR**: P1-2 (split weight), P1-3 (EXPLAIN diag), P1-4 (CHAR LIKE pushdown), P1-5 (CAST conjunct drop),
  P1-6 (count→1 range), P4-1 (branch schema source), P5-1 (WITH_TIMEZONE extra-info), P6-1 (latest-schema
  cache key drops schema-id), P11-3 (nested struct comments on write), P12-1 (inert table-cache props).
- **NIT**: P3-2/P3-3 (error text), P4-3/P4-4 (branch non-FileStoreTable), P5-2 (sys-table live handle),
  P7-2 (native sub-split weight), P7-3 (VERBOSE delete-file counts), P10-1 (`.parq`→JNI),
  P10-2 (force-jni omits -1 entry), P12-2/P12-3 (dead residue), C-3 (MTMV sentinel filter).
- **C-1 (MINOR observability)** — scan-planning metrics + summary-profile timers dropped for every paimon
  query. Decide: restore (re-wire metric registry + profile timers in the plugin scan path) or accept.
- **uncheckedFallbacks** (need live confirmation): REFRESH TABLE/CATALOG → connector cache invalidation
  (no `invalidateTable` SPI; possible stale MVCC snapshot/handle); partitions-TVF auth + LATEST-only
  resolution; split-plan RPC outside `executeAuthenticated` (Kerberos); `PluginDrivenExternalCatalog:140`
  swallows authenticator-wiring exceptions.

## Follow-ups (after fixes)
- **D-057 re-scope** (report §D.3): the deferred `TablePartitionValues:162` prune-path sentinel residue
  does **not** affect paimon (MVCC override bypasses it). Re-scope the deferral to non-MVCC plugin
  connectors (maxcompute/es/jdbc); the base-class DATE-epoch + HIVE_DEFAULT paths (P11-1/P11-2) are a
  latent concern there, not paimon.
- **B8 legacy deletion**: R-1…R-7 enumerate the dead subtree. Deletion must preserve load-bearing
  dispatch ordering (`ShowPartitionsCommand:478-480`, R-4) and may proceed once the FE-config-parity
  fixes no longer need legacy `*Properties` as a reference.
