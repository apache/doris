# Summary — FIX-hudi-s3a-jni-creds

## Problem
4 hudi p2 suites reading MOR-with-log slices via the JNI Hudi scanner (incremental,
schema_evolution, timetravel, snapshot) failed with
`NoAuthWithAWSException: No AWS Credentials provided ...` — and any `force_jni_scanner=true`
sub-query in the native suites.

## Root Cause
The BE JNI reader builds the Java scanner's Hadoop `Configuration` from
`TFileScanRangeParams.properties` (prefixing each key `hadoop_conf.`, stripped back by
`HadoopHudiJniScanner`). `HudiScanPlanProvider.getScanNodeProperties` emitted under `location.` only
(1) `getBackendStorageProperties()` — BE-native-canonical `AWS_*` (which `S3AFileSystem` ignores) and
(2) a raw passthrough of the catalog's `s3.` aliases (also ignored). It never emitted the
Hadoop-canonical `fs.s3a.access.key/secret.key/endpoint`. The translation (`storageHadoopConfig` →
`StorageProperties.toHadoopProperties()`) existed but was used only for the FE-side
`HoodieTableMetaClient` conf, not for the BE scan. Legacy `getLocationProperties` merged
`backendStorageProperties` + the translated hadoop props; the new path dropped the second half.

## Fix
Emit `storageHadoopConfig(context)` (the translated `fs.s3a.*`) under the `location.` prefix in
`getScanNodeProperties`, placed after the `AWS_*` canonical emission (1) and before the raw
passthrough (2) so a user-inline `fs.*`/`hadoop.*` key still wins (mirrors `buildHadoopConf`
precedence). Native reader is unaffected (it reads `AWS_*` only; extra `fs.s3a.*` are ignored, same as
the `s3.` aliases already emitted). One-line addition; `storageHadoopConfig` already null-guards.

## Tests
`HudiBackendDescriptorTest` +1: a context whose typed `StorageProperties` translate to `fs.s3a.*`,
with the catalog carrying only `s3.` aliases (the real failing scenario), asserts
`getScanNodeProperties` emits `location.fs.s3a.access.key`. Full fe-connector-hudi suite green,
0 checkstyle.

## Result
E2E (user-run): the 4 JNI-signature suites must stop throwing `NoAuthWithAWSException`, and
`force_jni_scanner=true` sub-queries in the native suites must succeed. Design red-team
`wf_4e4ec1d7-d4f` verdict SOUND (linchpin — HMS/hudi context storage IS bound — confirmed).
