# Summary — FIX-hive-s3a-read (native scheme + canonical creds)

## Problem
Plain-hive tables on an object-store warehouse (`s3a://`/`oss://`/`cos://`) would fail BE reads — the
same latent gap class that broke hudi. Unexercised locally (hive docker = `hdfs://`; hive-s3 suites are
p2/real-cloud). Fixed on user request after the hudi fixes.

## Root Cause (two gaps, fe-connector-hive only)
1. **Scheme**: `splitFile`→`newRangeBuilder:587` `.path(filePath)` shipped raw `s3a://` to
   `TFileRangeDesc.path`; BE `S3URI` accepts only `s3/http/https`. (Legacy normalized via 2-arg
   `LocationPath.of`; hive's own WRITE path already used `normalizeStorageUri`.)
2. **Creds (legacy regression)**: `getScanNodeProperties` emitted only raw `s3.`/`fs.` aliases; BE's
   native reader reads only canonical `AWS_ACCESS_KEY/SECRET/ENDPOINT` (`s3_util.cpp:146-148`) → private
   bucket 403s. Legacy `getLocationProperties()` returned `getBackendStorageProperties()` (canonical);
   the new path dropped it. (Hive has no JNI scanner → no `fs.s3a.*`/JNI analog.)

## Fix
- `normalizeNativeUri(raw) = context != null ? context.normalizeStorageUri(raw) : raw`; applied to the
  native data-file path in `splitFile`, and to the ACID BE-facing paths (`acidLocation` +
  `encodeDeleteDeltas` delete-delta dir, threaded a `UnaryOperator<String>`). Files are still LISTED with
  the raw scheme (Hadoop wants `s3a`); only BE-facing copies normalized.
- `getScanNodeProperties` emits `context.getBackendStorageProperties()` (canonical) under `location.`
  before the raw passthrough (inline `fs.`/`hadoop.` still wins). Restores legacy behavior.
fe-core untouched; no source-specific code.

## Tests
`HiveScanBatchModeTest` +2 (RED-able): range path `s3a`→`s3` via `planScanForPartitionBatch`; canonical
`AWS_*` emission. **Full fe-connector-hive suite 328/328 green, 0 checkstyle** (regression guard for the
creds change over the HDFS-based suite).

## Result
Design `plan-doc/tasks/designs/FIX-hive-s3a-read-design.md`. **Unit-verified only** — object-store e2e
needs the user's real s3/oss hive env (docker is HDFS). HDFS parity: canonical emission = what legacy
sent, so the HDFS docker hive suites should stay green (final proof = user's docker run).
