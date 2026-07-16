# FIX-hive-s3a-read (native scheme + canonical creds)

## Problem
Plain-hive tables on an object-store warehouse (`s3a://`/`oss://`/`cos://`/`obs://`) would fail
BE-side reads — the same class of latent gap that broke hudi. Found while fixing hudi; the design
red-team flagged `HiveScanPlanProvider` passes raw HMS paths with no scheme normalization.
**Unexercised locally**: the hive docker regression uses `hdfs://` (no S3URI, no s3 creds); the only
hive-s3 suites (`test_hive_write_insert_s3`, `hive_on_hms_and_dlf`) are p2/external (real cloud),
not run in local docker. So both gaps below are latent.

## Root Cause — TWO gaps in `HiveScanPlanProvider` (fe-connector-hive only)

### Gap 1 — native path scheme not normalized
BE's native S3 reader (`be/src/util/s3_uri.cpp` `S3URI::parse`) accepts only `s3`/`http`/`https`.
`splitFile` → `newRangeBuilder:587` `.path(filePath)` ships the raw `s3a://` (etc.) path verbatim to
`TFileRangeDesc.path`. Legacy `HiveScanNode` normalized via the 2-arg
`LocationPath.of(path, storagePropertiesMap)`. Hive's OWN write path already uses
`context.normalizeStorageUri` (`HiveWritePlanProvider:155`) — the read path just never got it. Also
affects the ACID full-transactional BE-facing paths (partition location + delete-delta directories).

### Gap 2 — BE-canonical credentials not emitted (worse; a legacy regression)
`getScanNodeProperties` emitted under `location.` ONLY the raw catalog aliases (`s3.`/`fs.`/`hadoop.`
via `isLocationProperty`). BE's native FILE_S3 reader reads ONLY `AWS_ACCESS_KEY`/`AWS_SECRET_KEY`/
`AWS_ENDPOINT` (`s3_util.cpp:146-148`), never `s3.access_key` — so a private bucket 403s. Legacy
`HiveScanNode.getLocationProperties()` returned `hmsTable.getBackendStorageProperties()` (the
canonical `AWS_*`); the new path dropped it. (Hive has **no JNI scanner** — grep for `FORMAT_JNI`
empty — so there is no `fs.s3a.*`/JNI-creds analog of the hudi Fix B.)

## Design
Mirror the hudi fixes + hive's own write-path pattern + legacy parity:

- **Gap 1**: add `normalizeNativeUri(raw) = context != null ? context.normalizeStorageUri(raw) : raw`.
  Normalize `filePath` at the top of `splitFile` (covers the regular + ACID data-file paths — both
  callers route through it). Normalize the ACID BE-facing paths at their emit sites: `acidLocation`
  (partition location) and the delete-delta directory inside `encodeDeleteDeltas` (threaded a
  `UnaryOperator<String>`). The connector still LISTS files with the raw scheme (Hadoop
  `S3AFileSystem` wants `s3a`); only the BE-facing copies are normalized.
- **Gap 2**: emit `context.getBackendStorageProperties()` (canonical `AWS_*` / resolved
  `hadoop.*`/`dfs.*`) under `location.` BEFORE the existing raw passthrough, so a user-inline
  `fs.`/`hadoop.` key still wins. Keep the raw passthrough (harmless `s3.` aliases + inline keys).
  This restores exactly what legacy emitted.

fe-core untouched (no property parsing; no source-specific code). Non-object-store schemes
(`hdfs://`) pass through `normalizeStorageUri` unchanged, and for HDFS catalogs
`getBackendStorageProperties()` yields resolved `hadoop.*/dfs.*` (what legacy sent) — so the
HDFS-backed docker suites are parity-preserved.

## Risk Analysis
- **HDFS regression** (reference connector, 300+ tests on HDFS): the canonical emission adds resolved
  `hadoop.*/dfs.*` for HDFS catalogs — identical to what legacy `getLocationProperties()` sent, and
  the raw passthrough (already present, already green) is retained. Parity argument → low risk;
  final proof is the user's docker HDFS run (unit tests can't drive BE).
- **Cannot e2e-validate object-store locally** (docker = HDFS; s3 suites = p2 real-cloud). Verified at
  the connector level with unit tests only; end-to-end needs the user's real s3/oss hive setup.
- **ACID-on-object-store** is a rare untested corner; normalization is safe (s3:// is accepted; if BE
  doesn't parse a given acid path via S3URI the rewrite is a harmless no-op).
- Existing tests asserting `getScanNodeProperties` output: full-suite run is the guard.

## Test Plan
### Unit (fe-connector-hive, `HiveScanBatchModeTest`)
- `nativeScanRangePathNormalizedS3aToS3`: drive `planScanForPartitionBatch` with an `s3a://` file +
  a context normalizing `s3a`→`s3`; assert the range `.getPath()` is `s3://`.
- `scanNodePropertiesEmitsCanonicalCredsForNativeReader`: a context whose
  `getBackendStorageProperties()` yields `AWS_ACCESS_KEY`, catalog carrying only the `s3.` alias;
  assert `location.AWS_ACCESS_KEY` emitted (and the raw alias still forwarded).
- Full fe-connector-hive suite must stay green (regression guard for the creds change).

### E2E (user-run; needs real s3/oss hive)
A hive table on an `s3a://`/`oss://` warehouse: native read stops throwing `Invalid S3 URI`, and a
private bucket no longer 403s. HDFS hive suites must remain green (parity).
