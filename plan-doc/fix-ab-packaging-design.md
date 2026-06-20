# Problem

CI 968994 (commit `3d93f195eff`), `Doris_External_Regression`. The self-contained paimon
plugin zip (`fe-connector-paimon/target/doris-fe-connector-paimon.zip`) is missing two
runtime jars, producing two failure classes:

- **Class A** — every `s3://`-warehouse paimon catalog fails to create:
  `UnsupportedSchemeException: Could not find a file io implementation for scheme 's3'`,
  with the deeper Hadoop-S3A cause
  `SdkClientException: ... ApplyUserAgentInterceptor does not implement the interface ... ExecutionInterceptor`.
  6 direct tests (test_paimon_s3, test_paimon_minio, test_paimon_schema_change,
  test_paimon_char_varchar_type, test_paimon_full_schema_change, test_paimon_jdbc_catalog)
  + 18 "Unknown database" collateral (swallowed at `ExternalCatalog.buildDbForInit():914`).
- **Class B** — `obs://` paimon catalog fails:
  `class org.apache.hadoop.fs.obs.OBSFileSystem cannot be cast to class org.apache.hadoop.fs.FileSystem`
  (`OBSFileSystem` in loader `'app'`, `FileSystem` in the plugin `ChildFirstClassLoader`).
  1 test (paimon_base_filesystem).

# Root Cause

Verified directly against the built zip + `output/fe/lib` + `mvn dependency:tree`:

- **A:** the plugin bundles `hadoop-aws-3.4.2` + `s3-2.29.52` + `sdk-core-2.29.52` but NOT
  `s3-transfer-manager`. The SPI resource `software/amazon/awssdk/services/s3/execution.interceptors`
  (listing `software.amazon.awssdk.transfer.s3.internal.ApplyUserAgentInterceptor`) lives ONLY in
  `s3-transfer-manager.jar` — `s3.jar` carries none. The plugin's child-first `sdk-core`
  `ClasspathInterceptorChainFactory` finds no child copy of the resource, so
  `ChildFirstClassLoader.getResources` fails open to the **parent** `s3-transfer-manager` (present in
  `output/fe/lib`), loading its `ApplyUserAgentInterceptor` which implements the **parent** `sdk-core`'s
  `ExecutionInterceptor` — a different `Class` than the child's → `isAssignableFrom` fails →
  `SdkClientException` → S3A unusable → paimon FileIO fallback fails → "no file io for scheme s3" →
  catalog init throws → swallowed at `buildDbForInit():914` → empty db list → "Unknown database".
- **B:** the plugin does NOT bundle `hadoop-huaweicloud`, so `OBSFileSystem` resolves from the parent
  `'app'` classpath while the plugin's `FileSystem` is child-first → cross-loader `ClassCastException`.
  Exactly the same shape the hadoop-aws bundling already fixed for `s3a`.

# Design

Complete the self-contained bundle (the documented RC-3 strategy; the pom comment even says
"STS/assumed-role would need `software.amazon.awssdk:sts` added the same way"). Two one-dependency
additions to `fe/fe-connector/fe-connector-paimon/pom.xml`; the assembly (`plugin-zip.xml`) bundles all
compile/runtime deps except its explicit excludes, so no descriptor change is needed.

- **A:** add `software.amazon.awssdk:s3-transfer-manager` (BOM-managed `${awssdk.version}` = 2.29.52,
  matching the bundled s3/sdk-core), child-first. Puts the `execution.interceptors` resource +
  `ApplyUserAgentInterceptor` in the child loader → resolves against the child `sdk-core`.
- **B:** add `com.huaweicloud:hadoop-huaweicloud` (managed version `3.1.1-hw-46`, scope `compile`,
  jackson-databind excluded — all inherited from fe-core dependencyManagement). The `-hw-46` jar is a
  fat jar self-containing both `OBSFileSystem` AND the OBS SDK (`com/obs/*`, 1703 classes), so a single
  child-first jar makes OBS self-consistent; no separate `esdk-obs` dep needed.

# Implementation Plan

1. Insert the `hadoop-huaweicloud` dep after the `hadoop-aws` block (FIX-B), with an explanatory comment
   mirroring the hadoop-aws rationale.
2. Insert the `s3-transfer-manager` dep after the `apache-client` dep (FIX-A), with a comment explaining
   the interceptor cross-loader skew.
3. Two independent commits (per branch convention: each RC its own commit).

# Risk Analysis

- `mvn dependency:tree -am` (succeeded): both resolve at the expected versions; single
  `hadoop-common:3.4.2` (plugin's direct depth-1 copy wins mediation over hadoop-huaweicloud's
  transitive copy → no duplicate `FileSystem`); **no new thrift** introduced (A/B are thrift-neutral;
  thrift is FIX-C's concern).
- esdk-obs static collision with the parent's `esdk-obs-java-optimised` is theoretically possible but
  the `-hw-46` jar self-contains its own `com.obs.*` child-first, so the child is self-consistent.
  Docker-gated.
- All of A/B are **docker-gated** (`enablePaimonTest=true`): they pass local UT and the local zip-build
  proves bundling, but only the docker external suite reproduces/verifies the runtime classloader paths.

# Test Plan

## Unit Tests
None — pure packaging. Existing fe-connector-paimon UT must stay green (no code change).

## E2E Tests
Existing suites are the coverage; no new suite needed:
- A: external_table_p0/paimon/{test_paimon_s3, test_paimon_minio, test_paimon_schema_change,
  test_paimon_char_varchar_type, test_paimon_full_schema_change, test_paimon_jdbc_catalog} + the 18
  "Unknown database" suites should go green.
- B: external_table_p0/paimon/paimon_base_filesystem (obs:// branch).

## Local verification (pre-commit)
- `mvn dependency:tree -am` clean (done).
- `mvn -pl fe-connector/fe-connector-paimon -am package` then
  `unzip -l target/doris-fe-connector-paimon.zip | grep -E 's3-transfer-manager|hadoop-huaweicloud'`
  must show both jars in `lib/`.

## Runtime gate
Docker external regression with `enablePaimonTest=true` (the real gate; cannot be reproduced locally).
