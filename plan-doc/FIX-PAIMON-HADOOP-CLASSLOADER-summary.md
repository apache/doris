# FIX-PAIMON-HADOOP-CLASSLOADER — summary

## Problem
~39 of 42 failed suites in the TeamCity external run (`af2037`): the Paimon connector plugin's Hadoop classloading
split-brained (`Could not initialize class SecurityUtil` · `S3AFileSystem cannot be cast to FileSystem` ·
`DNSDomainNameResolver not DomainNameResolver` · `ServiceConfigurationError: NullScanFileSystem not a subtype` ·
cascade `Unknown database 'X'`).

## Root Cause
The plugin runs child-first with `org.apache.hadoop` NOT parent-first, and bundled `hadoop-common`/`hadoop-client-api`
but NOT `hadoop-aws`. So `FileSystem`/`SecurityUtil` loaded child-first while `S3AFileSystem` resolved from the parent
`app` loader → cross-loader cast + permanent `SecurityUtil.<clinit>` poison. `buildHadoopConfiguration` built a bare
`new Configuration()` (parent context CL) and the connector never pinned the thread-context CL (unlike JDBC/HMS).

## Fix
Self-contained plugin (no parent-leaning — aligns with fe-core dropping hadoop/hive-catalog-shade after full migration):
1. `fe-connector-paimon/pom.xml`: add `hadoop-aws` (the only missing FS impl — `S3AFileSystem`; `DistributedFileSystem`
   already came from the transitive `hadoop-client-api`).
2. `PaimonCatalogFactory.buildHadoopConfiguration`: `conf.setClassLoader(PaimonCatalogFactory.class.getClassLoader())`
   → `Configuration.getClass("fs.<scheme>.impl")` resolves the FS impl from the plugin loader.
3. `PaimonConnector.createCatalogFromContext` (single chokepoint, all flavors): pin thread-context CL to the plugin
   loader around `executeAuthenticated(...)` → `FileSystem` ServiceLoader + `SecurityUtil`/DNS init resolve child.
   Mirrors `JdbcConnectorClient`/`ThriftHmsClient`.

## Tests
- Build: `-pl :fe-connector-paimon -am package` → BUILD SUCCESS; all connector UTs 0 fail / 0 error.
- Packaging assertion: plugin `lib/` now contains `hadoop-aws-3.4.2.jar`; `S3AFileSystem` present in child.
- Checkstyle (connector) + connector import-gate: clean.
- Adversarial review: headline "BLOCKER" rested on a false premise (pre-fix `af2037` log read as post-fix); two valid
  sub-findings folded in (dropped `hadoop-hdfs`; documented AWS SDK v2 single-copy parent resolution).
- **Final runtime proof is the docker external paimon suite (CI-gated, `enablePaimonTest`) — not run locally.**

## Result
Implemented + locally verified (build/UT/packaging/style). Resolves the dominant cluster (39 suites incl. 3 non-Paimon
collateral) pending CI e2e confirmation.
