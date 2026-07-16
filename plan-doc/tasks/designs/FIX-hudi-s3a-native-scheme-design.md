# FIX-hudi-s3a-native-scheme

## Problem

All hudi p2 suites that read COW/native slices from an `s3a://` (MinIO) warehouse fail with:

```
[INVALID_ARGUMENT]Invalid S3 URI: s3a://datalake/warehouse/regression_hudi.db/.../xxx.parquet
```

10 of the 14 failing suites (catalog, partition_prune, orc_tables, schema_change,
full_schema_change, timestamp, runtime_filter, mtmv, rewrite_mtmv, olap_rewrite_mtmv).

## Root Cause

BE's native S3 reader parses the file path via `S3URI::parse()` (`be/src/util/s3_uri.cpp:44-78`),
which accepts ONLY the schemes `s3`, `http`, `https`. The scheme `s3a` falls to the `else` at
line 70 → `Invalid S3 URI`. It fails at URI parse, before any credential/network use.

The new fe-connector hudi path hands BE the raw HMS location with the `s3a://` scheme intact.
There are **three** native `.path()` emitters in the connector (exhaustive grep), all raw `s3a://`:

- `HudiScanPlanProvider.collectCowSplits:420` → `.path(filePath)` where `filePath =
  baseFile.getPath()` (raw `s3a://`). [snapshot COW]
- `HudiScanPlanProvider.buildMorRange:490` → `.path(agencyPath)` (raw `s3a://` base file / first log).
  [snapshot MOR + incremental MOR]
- `COWIncrementalRelation.collectSplits:200` → `.path(baseFile)` (raw `s3a://` absolute path anchored
  on `metaClient.getBasePath()`). [**COW `@incr`**] — reached via `incrementalRanges`' COW branch
  (`HudiScanPlanProvider.java:576-578`) which returns `relation.collectSplits()` verbatim. Exercised by
  `test_hudi_incremental.groovy:89-90` (`set force_jni_scanner=false` → `isCow=true`). Found by the
  design red-team (`wf_4e4ec1d7-d4f`); missed in the first draft.

All three flow to BE identically:

- `PluginDrivenSplit.buildPath:78` wraps it via the **single-arg** `LocationPath.of(pathStr)`, which
  stores the raw string and never calls `validateAndNormalizeUri` (`LocationPath.java:163-169`).
- `FileQueryScanNode.createFileRangeDesc:568` ships that verbatim `s3a://` to `TFileRangeDesc.path`.
- `SchemaTypeMapper:56` maps `s3a`→`FILE_S3`, so BE picks the native S3 reader → rejects `s3a`.

The `s3a://`→`s3://` rewrite lives only in `S3PropertyUtils.validateAndNormalizeUri`
(`S3PropertyUtils.java:172-190`), reachable only via the *normalizing* `LocationPath.of` overload,
which the plugin split path bypasses.

Legacy parity: the still-present `HudiScanNode` built its snapshot splits via the normalizing 2-arg
overload `LocationPath.of(filePath, hmsTable.getStoragePropertiesMap())` (`HudiScanNode.java:425,590`),
which DID rewrite `s3a`→`s3`. The COW-incremental site is NOT covered by that parity argument: legacy
`COWIncrementalRelation.java:218` used the **single-arg** (non-normalizing) `LocationPath.of(baseFile)`,
so legacy COW `@incr` over `s3a` was latently broken too (a pre-existing gap, not a regression). Net:
this is a migration parity gap for the snapshot paths + a pre-existing latent gap for COW `@incr`,
both fixed here.

Sibling connectors already do the connector-side normalization the plugin path (correctly, per the
"fe-core holds no property parsing" rule) does NOT do:
- `IcebergScanPlanProvider:834-836` → `.path(normalizeUri(rawDataPath, vendedToken))`.
- `PaimonScanPlanProvider:547` → `.path(normalizeUri(file.path(), vendedToken))`.
Both delegate to the SPI seam `ConnectorContext.normalizeStorageUri` (`ConnectorContext.java:205`).
Hudi is the only object-store connector missing this call.

## Design

Mirror iceberg/paimon: normalize the **native** range `.path()` on the connector side via
`context.normalizeStorageUri`. Hudi is HMS-based (no REST vended credentials), so the single-arg
overload suffices — no `vendedToken`.

CRITICAL invariant: normalize ONLY the native range path field (`HudiScanRange.path`, which becomes
`TFileRangeDesc.path`, consumed by the native S3 reader). The JNI reader's `THudiFileDesc` paths
(`basePath` / `dataFilePath` / `deltaLogs`) MUST stay raw `s3a://` — Hadoop's `S3AFileSystem`
*wants* the `s3a` scheme. Those are set from separate builder args (`.basePath`, `.dataFilePath`,
`.deltaLogs`) and are untouched by this fix.

Normalizing the `.path()` field is safe even for a JNI-format slice: BE consumes `TFileRangeDesc.path`
via `S3URI` only for native-format ranges; for `FORMAT_JNI` it reads via `THudiFileDesc` and does not
parse the range path (proven by the JNI suites failing at `NoAuthWithAWSException` from `base_path`,
NOT at `Invalid S3 URI` from the range path). So normalizing the native path field unconditionally is
correct and harmless.

## Implementation Plan

`fe/fe-connector/fe-connector-hudi/src/main/java/org/apache/doris/connector/hudi/HudiScanPlanProvider.java`:

1. Add private instance helper (mirror paimon `normalizeUri`, single-arg):
   ```java
   private String normalizeNativeUri(String rawUri) {
       return context != null ? context.normalizeStorageUri(rawUri) : rawUri;
   }
   ```
   Null-context guard preserves offline unit tests (no live context).

2. `collectCowSplits` (instance): `.path(filePath)` → `.path(normalizeNativeUri(filePath))`.

3. `buildMorRange` (static) + `incrementalRanges` (static): thread a
   `java.util.function.UnaryOperator<String> nativePathNormalizer` param.
   - `buildMorRange`: `.path(agencyPath)` → `.path(nativePathNormalizer.apply(agencyPath))`.
   - `incrementalRanges`: accept the param, pass it to BOTH branches — the MOR branch's
     `buildMorRange` call AND the COW branch's `relation.collectSplits(...)` call.
   - Instance callers (`collectMorSplits:449`, `planScan`'s `incrementalRanges:258`) pass
     `this::normalizeNativeUri`.
   - Unit tests pass `UnaryOperator.identity()`.

   Rationale for a threaded function (not making the methods non-static): the methods are
   deliberately `static` + package-private for offline unit testing with hand-built `FileSlice`s
   (per their javadoc). Threading a `UnaryOperator<String>` keeps them static and lets tests pass
   `identity()`, mirroring how paimon threads `vendedToken` through its build methods.

4. COW-incremental third site: change the `IncrementalRelation.collectSplits()` interface method to
   `collectSplits(UnaryOperator<String> nativePathNormalizer)`.
   - `COWIncrementalRelation.collectSplits`: apply at `:200` → `.path(nativePathNormalizer.apply(baseFile))`.
   - `MORIncrementalRelation.collectSplits` (throws `UnsupportedOperationException`) and
     `EmptyIncrementalRelation.collectSplits` (returns `emptyList`): accept the param, ignore it (they
     emit no native ranges via `collectSplits`).
   - `incrementalRanges`' COW branch (`:577`): `ranges.addAll(relation.collectSplits(nativePathNormalizer))`.
   - Chosen over ctor-injection into `COWIncrementalRelation` because threading through
     `incrementalRanges` makes the wiring unit-testable at the existing fake-`IncrementalRelation` seam
     (the exact place the bug lived) and keeps the normalizer symmetric across COW + MOR.

Add import: `java.util.function.UnaryOperator` (to `HudiScanPlanProvider`, `IncrementalRelation`,
`COWIncrementalRelation`, `MORIncrementalRelation`, `EmptyIncrementalRelation`).

## Risk Analysis

- **JNI paths accidentally normalized** → would break MOR merge reads. Mitigated: only `.path()` is
  normalized; `.basePath/.dataFilePath/.deltaLogs` are distinct builder args, left raw. Guard with a
  UT asserting a JNI range's `THudiFileDesc` paths remain `s3a://` while the native `.path()` is `s3://`.
- **Non-s3a schemes** (hdfs://, plain paths) → `normalizeStorageUri` is a no-op / passthrough for
  non-S3-compatible schemes (same primitive paths that already work for iceberg/paimon on HDFS). No
  regression for the EMR/HDFS hudi config.
- **Null context (offline UT)** → helper returns raw; identity in tests. No NPE.
- **Test call-site churn** (`HudiIncrementalPlanScanTest`, ~8 refs) → mechanical `identity()` add;
  compile failure is the safety net if one is missed.

## Test Plan

### Unit Tests (fe-connector-hudi)
- New: assert `collectCowSplits` / `buildMorRange` native `.path()` is normalized `s3a://`→`s3://`
  when a fake `ConnectorContext.normalizeStorageUri` rewrites the scheme; and that a JNI slice's
  `THudiFileDesc.base_path/data_file_path/delta_logs` stay raw `s3a://`.
- Existing `HudiIncrementalPlanScanTest` (updated for the new param) must stay green.
- `mvn -pl fe/fe-connector/fe-connector-hudi test`; checkstyle clean.

### E2E (user-run, docker-gated)
The 10 native-signature suites under `regression-test/suites/external_table_p2/hudi/` must stop
throwing `Invalid S3 URI`. Full green also requires FIX-hudi-s3a-jni-creds (suites with
`force_jni_scanner=true` sub-queries, e.g. test_hudi_catalog, exercise both paths).
