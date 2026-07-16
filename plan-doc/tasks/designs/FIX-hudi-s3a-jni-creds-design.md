# FIX-hudi-s3a-jni-creds

## Problem

The 4 hudi p2 suites that read MOR-with-log slices via the JNI Hudi scanner (incremental,
schema_evolution, timetravel, snapshot) fail with:

```
[JNI_ERROR] failed to open hadoop hudi jni scanner: Cannot create a RecordReaderWrapper
 | CAUSED BY: AccessDeniedException: s3a://datalake/warehouse/.../xxx.parquet:
   org.apache.hadoop.fs.s3a.auth.NoAuthWithAWSException: No AWS Credentials provided by
   TemporaryAWSCredentialsProvider SimpleAWSCredentialsProvider EnvironmentVariableCredentialsProvider
   IAMInstanceCredentialsProvider
```

Also affects any `force_jni_scanner=true` sub-query in the native suites (e.g. test_hudi_catalog).

## Root Cause

The BE JNI reader builds the Java scanner's Hadoop `Configuration` from
`TFileScanRangeParams.properties`: for each entry it prefixes the key with `hadoop_conf.`
(`be/src/format/table/hudi_jni_reader.cpp:60-66`, `be/src/format_v2/jni/hudi_jni_reader.cpp:100-106`),
and `HadoopHudiJniScanner` strips `hadoop_conf.` back off into the `Configuration`
(`HadoopHudiJniScanner.java:124-127`). Those `properties` come from the connector's `location.`-prefixed
props (`PluginDrivenScanNode.getLocationProperties:603-613` → FILE_S3 branch of
`FileQueryScanNode`). So a connector property emitted as `location.fs.s3a.access.key` round-trips to a
Hadoop `Configuration` key `fs.s3a.access.key` in the JNI scanner.

`HudiScanPlanProvider.getScanNodeProperties` emits under `location.` only:
1. `context.getBackendStorageProperties()` — BE-**native**-canonical creds (`AWS_ACCESS_KEY` /
   `AWS_SECRET_KEY` / `AWS_ENDPOINT` / ...), which Hadoop's `S3AFileSystem` does NOT read.
2. a raw passthrough of the catalog's Doris aliases (`s3.access_key`, `s3.endpoint`, ...), which
   its own comment admits are "harmless ... ignored by both readers".

It never emits the Hadoop-canonical `fs.s3a.access.key/secret.key/endpoint`. Those are produced by
`storageHadoopConfig(context)` → `StorageProperties.toHadoopProperties()` (`HudiScanPlanProvider`
package-private helper, ~`:903-914`), but that map is used ONLY to build the FE-side
`HoodieTableMetaClient` conf (in `buildHadoopConf`), never emitted to BE. So the JNI `S3AFileSystem`
falls back to the default AWS provider chain with no keys → `NoAuthWithAWSException`. (The error
listing the DEFAULT chain, not a pinned `SimpleAWSCredentialsProvider`, confirms the JNI conf got
none of the catalog's s3 config.)

Legacy parity: legacy `getLocationProperties` merged `backendStorageProperties` THEN the Hadoop
properties (`fs.s3a.*`) — the new path dropped the second half for the scan (kept it only for the FE
metaClient).

## Design

Emit the Hadoop-canonical object-store config (`storageHadoopConfig(context)`, already defined and
already used for the metaClient) under the `location.` prefix in `getScanNodeProperties`, so BE's JNI
Hadoop `Configuration` receives `fs.s3a.access.key/secret.key/endpoint/path.style.access` etc.

Placement / precedence: emit it AFTER (1) `getBackendStorageProperties` (no key overlap — AWS_* vs
fs.s3a.*) and BEFORE (2) the raw passthrough loop, so a user-inline `fs.*`/`hadoop.*` key in the raw
catalog properties still overrides the translated value — matching the precedence in `buildHadoopConf`
(`storageHadoopConfig` first, inline `fs./dfs./hadoop.` keys override). `Map.put` last-wins gives this
ordering.

Safety for the native reader: the native FILE_S3 reader consumes AWS_* canonical keys and ignores the
extra `fs.s3a.*` keys — same as it already ignores the `s3.` aliases emitted today. So the addition is
JNI-enabling and native-neutral.

## Implementation Plan

`fe/fe-connector/fe-connector-hudi/src/main/java/org/apache/doris/connector/hudi/HudiScanPlanProvider.java`,
in `getScanNodeProperties`, between emission (1) and (2):

```java
//  (1b) Hadoop-canonical object-store config (fs.s3a.* / fs.oss.* / ... resolved hadoop.*/dfs.*)
//       translated from the catalog's typed StorageProperties, for the Hudi JNI reader's own Hadoop
//       FileSystem. S3AFileSystem reads ONLY fs.s3a.* — never the AWS_* canonical keys (native) nor
//       the s3. aliases — so without this a private s3a warehouse throws NoAuthWithAWSException in the
//       JNI scanner. Emitted BEFORE the inline passthrough (2) so a user-inline fs./hadoop. key still
//       wins (matches buildHadoopConf precedence).
storageHadoopConfig(context).forEach((k, v) -> props.put("location." + k, v));
```

`storageHadoopConfig(ConnectorContext)` already null-guards a null context (returns empty). No import
or signature change needed.

## Risk Analysis

- **Native reader confusion** by extra `fs.s3a.*` keys → none; native reads AWS_* only, ignores the
  rest (already true for the `s3.` aliases emitted today).
- **Key collision / wrong precedence** → resolved by placement before passthrough (2); user-inline
  `fs.*` wins, mirroring `buildHadoopConf`.
- **Non-S3 (HDFS/EMR) catalogs** → `storageHadoopConfig` yields resolved `hadoop.*/dfs.*` (not
  `fs.s3a.*`); already what the metaClient uses; no object-store keys leak.
- **Null context (offline UT)** → `storageHadoopConfig` returns empty; no-op.

## Test Plan

### Unit Tests (fe-connector-hudi)
- New: with a fake `ConnectorContext` whose `getStorageProperties().toHadoopProperties()` yields
  `fs.s3a.access.key/secret.key/endpoint`, assert `getScanNodeProperties` emits them under
  `location.fs.s3a.*`; and that a user-inline `fs.s3a.access.key` in raw properties overrides the
  translated value (precedence). Assert AWS_* canonical (native) keys are still present.
- `mvn -pl fe/fe-connector/fe-connector-hudi test`; checkstyle clean.

### E2E (user-run, docker-gated)
The 4 JNI-signature suites (incremental, schema_evolution, timetravel, snapshot) must stop throwing
`NoAuthWithAWSException`, and `force_jni_scanner=true` sub-queries in the native suites must succeed.
