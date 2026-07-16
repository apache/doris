# Summary — FIX-hudi-s3a-native-scheme

## Problem
10 hudi p2 suites reading COW / MOR-no-log slices from an `s3a://` (MinIO) warehouse failed with
BE `[INVALID_ARGUMENT]Invalid S3 URI: s3a://...`.

## Root Cause
BE's native S3 reader (`be/src/util/s3_uri.cpp` `S3URI::parse`) accepts only `s3`/`http`/`https`.
The new fe-connector hudi path shipped the raw HMS `s3a://` location straight to `TFileRangeDesc.path`
without the `s3a`→`s3` rewrite legacy `HudiScanNode` did via the 2-arg
`LocationPath.of(path, storagePropertiesMap)`. `PluginDrivenSplit.buildPath` uses the single-arg
(non-normalizing) `LocationPath.of`.

## Fix
Normalize the NATIVE range `.path()` on the connector side via
`ConnectorContext.normalizeStorageUri` (mirrors iceberg/paimon `normalizeUri`; fe-core parses no
properties). Applied at all THREE native emitters:
- `HudiScanPlanProvider.collectCowSplits` (snapshot COW)
- `HudiScanPlanProvider.buildMorRange` (snapshot + incremental MOR no-log native slice)
- `COWIncrementalRelation.collectSplits` (COW `@incr` — third site found by the design red-team
  `wf_4e4ec1d7-d4f`, missed in the first draft)

Threaded as a `UnaryOperator<String>` through `incrementalRanges` → `{collectSplits, buildMorRange}`
to keep the static package-private test seams. JNI `THudiFileDesc` base/data/delta-log paths stay raw
`s3a://` (Hadoop `S3AFileSystem` wants `s3a`). Null context (offline UT) preserves the raw URI.

## Tests
`HudiIncrementalPlanScanTest` +2 (COW `@incr` + MOR no-log native path `s3a`→`s3`); existing static
call sites threaded with `UnaryOperator.identity()`; `HudiIncrementalRelationTest` updated.
fe-connector-hudi **25/25 green, 0 checkstyle**.

## Result
Commit `a26eaf46b9f`. E2E (user-run): the 10 native-signature hudi p2 suites must stop throwing
`Invalid S3 URI`. Full green of the 14 also needs FIX-hudi-s3a-jni-creds (suites with
`force_jni_scanner=true` sub-queries hit both paths).
