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

- [x] FIX-hive-s3a-read — plain-hive object-store latent gap (found via the hudi red-team; user asked
      to fix it too). TWO gaps in fe-connector-hive: (1) native `.path()` not scheme-normalized
      (`splitFile`/`newRangeBuilder` + ACID paths) → `Invalid S3 URI`; (2) `getScanNodeProperties`
      emitted raw `s3.` aliases instead of BE-canonical `AWS_*` (legacy `getLocationProperties` emitted
      `getBackendStorageProperties()`) → private-bucket 403. Hive has no JNI → no fs.s3a. analog.
      **DONE** (normalizeNativeUri + canonical creds emission; +2 UT, full hive suite 328/328, 0 checkstyle).
      **Unexercised locally** (docker=HDFS; hive-s3 suites are p2/real-cloud) → unit-verified only,
      object-store e2e needs user's real s3/oss env.

E2E = the existing 14 failing suites under `regression-test/suites/external_table_p2/hudi/`
(no new suites needed; they are the regression gate). Run both hudi fixes together, then re-run all 14.
Hive: object-store e2e needs a real s3/oss hive table (docker is HDFS); HDFS hive suites are the parity guard.

Order: hudi native-scheme → hudi jni-creds → hive-s3a-read. TDD per fix, independent commit.
