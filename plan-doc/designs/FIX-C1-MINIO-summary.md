# Summary — FIX-C1-MINIO (P6 finding C1)

## Problem
A catalog keyed purely with legacy `minio.*` storage properties (`minio.endpoint` / `minio.access_key`
/ `minio.secret_key` / …) was **unbindable** on the SPI branch. The typed `fe-filesystem` storage SPI
has no MinIO provider, and `S3FileSystemProvider.supports()` / `S3FileSystemProperties` recognized no
`minio.*` key → `bindAllStorageProperties` returned empty (no throw) → empty Hadoop map (no
`fs.s3.impl`) on FE catalog-create ("no file io for scheme s3") and empty `location.AWS_*` on BE
(native paimon read failed). MAJOR (BLOCKER if a deployment keys catalogs with `minio.*`).

## Root Cause
The 2026-06-14 `applyCanonicalMinioConfig` work (in the old `PaimonCatalogFactory.applyStorageConfig`
path) was obsoleted by the move to the typed storage SPI and never carried into this branch. Legacy
`MinioProperties` was just "S3 with a custom endpoint" — it inherited pure S3A config from
`AbstractS3CompatibleProperties` and emitted **zero** MinIO-specific `fs.*`/`AWS_*` keys; only its
alias prefix (`minio.*`), its region default (`us-east-1`), and its tuning defaults
(`100/10000/10000`, vs S3's `50/3000/1000`) differed.

## Fix
Alias `minio.*` into the **shared** `fe-filesystem-s3` module (no dedicated provider — that would be a
near-empty S3 clone):
- `S3FileSystemProperties.java`: appended `minio.*` aliases (endpoint/region/access_key/secret_key/
  session_token/connection.maximum/connection.request.timeout/connection.timeout/use_path_style) at
  the **end** of each field's `names()`. First-alias-wins (`ConnectorPropertiesUtils.getMatchedPropertyName`)
  → canonical `s3.*`/`AWS_*` keys still outrank → `s3.*` path byte-for-byte unchanged.
- `S3FileSystemProvider.java`: added `minio.access_key`/`minio.endpoint`/`minio.region` to the
  detection arrays so a pure-`minio.*` map satisfies `supports()` (`hasCredential && hasLocation`).
- **Tuning-default parity (key decision):** added gated `applyLegacyMinioTuningDefaults()` in the
  post-bind `normalizeForLegacyS3Compatibility()` hook. When a `minio.*` raw key is present and a
  tuning knob is unset under **any** alias, restore the legacy MinIO default (100/10000/10000).
  Gated on raw-key PRESENCE (not field-value-equals-default), so an explicit `minio.connection.maximum=50`
  is honored; gated on `minio.*` presence, so the canonical `s3.*` path is untouched. Region default
  `us-east-1` was already preserved by the existing endpoint-only normalize branch.

Decision rationale: the design's first pass proposed *accepting* the tuning deviation; an adversarial
design red-team refuted the "can't conditionalize defaults" premise (the region-default precedent is
the same post-bind mechanism), and the review spec explicitly required preserving the MinIO tuning
defaults → PRESERVE. (See `FIX-C1-MINIO-design.md` §Open Questions.)

## Tests
`fe-filesystem-s3` (FE UT):
- `S3FileSystemProviderTest.supports_acceptsPureMinioKeyedConfiguration` — pure `minio.*` map binds.
- `S3FileSystemPropertiesTest.of_bindsPureMinioAliasesAndHonorsExplicitTuning` — all aliases bind;
  explicit tuning honored.
- `…of_minioEndpointOnly_appliesUsEast1RegionDefaultAndEmitsS3aAndAwsKeys` — region default +
  FE `fs.s3.impl`/`fs.s3a.*` + BE `AWS_*`.
- `…of_minioOmittingTuning_appliesLegacyMinioTuningDefaults` — pins 100/10000/10000 preservation.
- `…of_s3KeyOutranksMinioKeyForSameField` — precedence/byte-parity guard.
- Existing `toMaps_emitS3TuningDefaultsWhenNotConfigured` (pure `s3.*` → 50/3000/1000) still green.

All fail on revert (verified by adversarial impl-verification). E2E (`enablePaimonTest`-gated MinIO
warehouse paimon catalog with `minio.*` props) NOT run here.

## Result
`mvn -pl :fe-filesystem-s3 -am test -Dtest=S3FileSystemPropertiesTest,S3FileSystemProviderTest`:
**28 tests run, 0 failures, 0 errors** (19 properties + 9 provider), BUILD SUCCESS, checkstyle clean
(runs in validate). Docker e2e NOT run (CI-gated).
