# Task List — hudi BE-scan s3a:// failures (14 p2 suites)

Root cause: the new fe-connector hudi BE-scan path under-threads S3 storage config (parity gap vs
legacy `HudiScanNode`). Two independent wiring gaps, fixed independently. RCA: recon workflow
`wf_67162858-b79` + inline verification (this session, 2026-07-12).

- [x] FIX-hudi-s3a-native-scheme — native reader `[INVALID_ARGUMENT]Invalid S3 URI: s3a://...`:
      connector emits the raw HMS `s3a://` path into the native range `.path()`; must normalize
      `s3a://`→`s3://` via `context.normalizeStorageUri` (mirror iceberg/paimon `normalizeUri`).
      fe-connector-hudi only. JNI `THudiFileDesc` paths stay raw `s3a://` (S3AFileSystem wants s3a).
      **DONE** commit `a26eaf46b9f` (3 native sites incl. COW @incr; 25/25 UT, 0 checkstyle).
- [x] FIX-hudi-s3a-jni-creds — JNI Hudi scanner `NoAuthWithAWSException: No AWS Credentials`:
      `HudiScanPlanProvider.getScanNodeProperties` never emits the Hadoop-canonical `fs.s3a.*`
      creds under the `location.` prefix; add `storageHadoopConfig(context)` emission so BE's JNI
      Hadoop conf receives `fs.s3a.access.key/secret.key/endpoint`. fe-connector-hudi only.
      **DONE** (getScanNodeProperties emits translated fs.s3a.* under location.; +1 UT, 0 checkstyle).

E2E = the existing 14 failing suites under `regression-test/suites/external_table_p2/hudi/`
(no new suites needed; they are the regression gate). Run both fixes together, then re-run all 14.

Order: native-scheme → jni-creds (independent; either order works). TDD per fix, independent commit.
