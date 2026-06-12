# FIX-FECONF-STORAGE-PARITY — summary

**Commit**: `f0210b51871` (fix). Cluster P8-1/P8-2/P8-3/P8-4 + P9-2/P9-3 + user-approved S3
endpoint-from-region + folded-in pre-existing kerberos-ordering MAJOR. **Connector-only** (no fe-core / SPI / BE).

## Problem
`PaimonCatalogFactory` rebuilds the FE-side Hadoop `Configuration` / `HiveConf` from raw props (the connector
cannot import fe-core), but the reconstruction was incomplete vs the legacy `*Properties`. Paimon catalogs on
OSS (region-only / s3://-over-OSS), COS, OBS, and MinIO/path-style failed FE-side catalog/metadata access; the
HMS `hive.metastore.username` alias never reached `hadoop.username`.

## Root Cause
`applyStorageConfig` ran only `applyCanonicalS3Config` + `applyCanonicalOssConfig`, each emitting a subset of
the legacy `initializeHadoopStorageConfig` (+ `super.appendS3HdfsProperties`) keys, with no COS/OBS block.
Notably the 4 S3A tuning keys were never emitted, the OSS endpoint-from-region derivation was DLF-local only,
and the HMS username alias was dropped.

## Fix
- Shared `applyS3aBaseConfig` helper (port of `appendS3HdfsProperties`) taking caller-resolved creds + tuning.
- **4a OSS**: endpoint-from-region (`oss-<region>[-internal].aliyuncs.com`, default `-internal`) moved into the
  shared OSS block (so filesystem + hms flavors get it); emit the S3A base for OSS; removed the dead DLF-local block.
- **4b S3**: `fs.s3a.path.style.access` + `connection.maximum/request.timeout/timeout`, with **per-backend
  defaults** — S3 `50/3000/1000` (+ `AWS_*` alias twins), OSS/COS/OBS `100/10000/10000`.
- **4c COS/OBS**: new blocks. Detection = `cos.`/`obs.` key OR endpoint/warehouse pattern
  (`myqcloud.com`/`myhuaweicloud.com`), mirroring legacy `guessIsMe`. Each emits the S3A base (the cosn/obs FS
  impl is `S3AFileSystem`, which reads `fs.s3a.*`) then the **unconditional** `fs.cosn.*` / `fs.obs.*` keys; OBS
  prefers native `OBSFileSystem` when classpath-available.
- **S3 endpoint-from-region** (user-approved): region-only AWS S3 → `https://s3.<region>.amazonaws.com`.
- **4d HMS username**: `firstNonBlank(hive.metastore.username, hadoop.username)` → `hadoop.username`, run AFTER
  the storage overlay so the raw `hadoop.*` passthrough can't clobber it.
- **4e kerberos-ordering** (folded-in pre-existing MAJOR): relocated the kerberos-conditional block to run AFTER
  the storage overlay, so a kerberized-HMS + simple-HDFS catalog keeps `auth=kerberos` (legacy
  `initHadoopAuthenticator`-last) instead of being clobbered to `simple` while `sasl.enabled=true`.

## Tests
`PaimonCatalogFactoryTest` **56/0/0** (15 new). The username-priority and kerberos-survives-simple-HDFS tests
are RED on the pre-move ordering (proof of fail-before; the kerberos clobber was empirically reproduced in
impl-review). Full `fe-connector-paimon` module green; checkstyle 0; import-gate clean. Live e2e
(`paimon_base_filesystem` catalog_oss/cos/cosn/obs, `test_paimon_dlf_catalog`, `test_paimon_hms_catalog`)
CI-gated (`enablePaimonTest=false`) — not run here.

## Method (meta)
Design red-team `wf_a6385c61-669` (5 skeptics + completeness critic) BEFORE coding caught: divergent per-backend
tuning defaults (S3 50/3000/1000 vs 100/10000/10000), endpoint-pattern detection (legacy detects COS/OBS by
endpoint pattern, not scheme key), and the unconditional `fs.cosn.*`/`fs.obs.*` requirement. Impl verification
`wf_f90260cb-5e6` confirmed byte-for-byte legacy key/alias/default fidelity (CLEAN) and surfaced the pre-existing
kerberos-ordering MAJOR (4e), which the user approved folding in.

## Result
All 4 round-3 user-approved fixes (FIX-1..FIX-4) complete. No fe-core/SPI/BE change. Known residual (documented,
out of scope): OSS endpoint-PATTERN detection (`aliyuncs.com`) not added to the existing OSS block (pre-existing,
no failing case); `fs.s3a.endpoint/region` emitted conditionally (connector lacks legacy's `checkNotNull`
throw-guard).
