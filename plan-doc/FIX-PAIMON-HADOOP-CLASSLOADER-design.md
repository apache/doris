# FIX-PAIMON-HADOOP-CLASSLOADER — design

## Problem
On the TeamCity external pipeline (build `af2037`), ~39 of 42 failed suites trace to the Paimon connector
plugin's Hadoop classloading. Surfaces (all the same bug):
- `java.lang.NoClassDefFoundError: Could not initialize class org.apache.hadoop.security.SecurityUtil`
- `class org.apache.hadoop.fs.s3a.S3AFileSystem cannot be cast to class org.apache.hadoop.fs.FileSystem`
  `(S3AFileSystem in loader 'app'; FileSystem in loader ...ChildFirstClassLoader@665e9a87)`
- `java.lang.RuntimeException: class org.apache.hadoop.net.DNSDomainNameResolver not org.apache.hadoop.net.DomainNameResolver`
- `java.util.ServiceConfigurationError: org.apache.hadoop.fs.FileSystem: org.apache.hadoop.hive.ql.io.NullScanFileSystem not a subtype`
- cascade: `listDatabaseNames` throws → swallowed at `ExternalCatalog:914` → `Unknown database 'X'`

Evidence: regression log markers (42) and FE log (`fe/log/fe.log` 76× cast / 56× SecurityUtil / 30× DNS).
First poison `fe.log:104928` via `PaimonConnector.createCatalogFromContext → CatalogFactory.createCatalog →
HadoopFileIO → FileSystem.get → SecurityUtil.<clinit>`.

## Root Cause
The Paimon plugin runs under `org.apache.doris.extension.loader.ChildFirstClassLoader`. Its parent-first allowlist
(`DEFAULT_PARENT_FIRST_PACKAGES` + connector `CONNECTOR_PARENT_FIRST_PREFIXES = {"org.apache.doris.connector.",
"org.apache.doris.filesystem."}`) does **not** include `org.apache.hadoop`, so all `org.apache.hadoop.*` is child-first.

The plugin pom (`fe-connector-paimon/pom.xml:92-95`) bundles `hadoop-common` at compile scope but **not**
`hadoop-aws` / `hadoop-hdfs`. So at runtime the plugin `lib/` has `FileSystem`/`SecurityUtil`/`Configuration`/
`DomainNameResolver` (child) but NOT `S3AFileSystem`/`DistributedFileSystem`. When paimon's `HadoopFileIO` does
`FileSystem.get()` on the warehouse (`s3://warehouse/wh`, `hdfs://...`), Hadoop reflectively resolves the impl:
- `Configuration.getClass("fs.s3a.impl")` → child miss → parent `app` loader → parent's `S3AFileSystem` → cast to
  child `FileSystem` → ClassCastException.
- `FileSystem.loadFileSystems()` → `ServiceLoader.load(FileSystem.class)` uses the **thread-context CL** (= parent
  `app`) → finds parent service providers incl. hive's `NullScanFileSystem` → "not a subtype" of child `FileSystem`.
- `SecurityUtil.<clinit>` → `DomainNameResolverFactory` → `Configuration.getClass(...DNSDomainNameResolver)` resolves
  across loaders → `DNSDomainNameResolver not DomainNameResolver`; the failed static init poisons `SecurityUtil`
  JVM-permanently → every later Hadoop-touching test (incl. 3 non-Paimon: iceberg/remote_doris/describe).

`PaimonCatalogFactory.buildHadoopConfiguration():457` builds a bare `new Configuration()` (captures the parent
thread-context CL as `Configuration.classLoader`), and `PaimonConnector` never pins the thread-context CL — unlike the
JDBC (`JdbcConnectorClient:222-241`) and HMS (`ThriftHmsClient:252-258`) connectors, which already pin it to their own
plugin loader around native calls.

## Design
Make the Paimon plugin **self-contained** for the Hadoop filesystem layer (own its full closure inside its own
child-first loader), rather than delegating hadoop to the parent. This matches the migration end-state where
`hive-catalog-shade` and fe-core's Hadoop stack are removed (user-confirmed 2026-06-12): a parent-first / `provided`
approach would break the moment fe-core sheds hadoop. Three coordinated changes:

1. **Packaging** (`fe-connector-paimon/pom.xml`): add **`hadoop-aws`** only (groupId+artifactId; version
   `${hadoop.version}`=3.4.2 + exclusions inherited from `fe/pom.xml` dependencyManagement). Empirically, the plugin
   lib already bundles `hadoop-client-api-3.4.2.jar` (transitive) carrying `FileSystem` / hdfs `DistributedFileSystem`
   / `SecurityUtil` / `DNSDomainNameResolver`; the ONLY FS impl missing from the child was `S3AFileSystem`
   (`hadoop-aws`). So adding `hadoop-aws` completes the child closure. (Earlier draft also added `hadoop-hdfs` — dropped:
   in Hadoop 3.x `DistributedFileSystem` lives in `hadoop-hdfs-client`/`hadoop-client-api`, NOT `hadoop-hdfs`, so it was
   dead weight + extra duplication.) `S3AFileSystem`'s AWS SDK v2 classes (`software.amazon.awssdk.*`, the heavy
   `:bundle` excluded in depMgmt) are not in the child and resolve from the FE parent classpath as a SINGLE copy → a
   cross-loader reference but not a split (only one copy exists). `hive-common` stays bundled (child);
   `org.apache.hadoop` stays child-first; no parent-first list change.

2. **Configuration classloader** (`PaimonCatalogFactory.buildHadoopConfiguration`): after `new Configuration()`, call
   `conf.setClassLoader(PaimonCatalogFactory.class.getClassLoader())`. Forces `Configuration.getClass("fs.<scheme>.impl")`
   to resolve FS impls from the plugin loader (handles the `fs.s3a.impl`/`fs.hdfs.impl` config-driven path).

3. **Thread-context CL pin** (`PaimonConnector.createCatalogFromContext`, the single chokepoint for all 5 flavors):
   wrap the `executeAuthenticated(() -> CatalogFactory.createCatalog(...))` call in a
   `setContextClassLoader(getClass().getClassLoader())` / restore-in-`finally`, mirroring `JdbcConnectorClient`. Makes
   the `FileSystem` ServiceLoader and `SecurityUtil`/DNS static init resolve from the plugin loader. The one-time class
   resolutions + `SecurityUtil.<clinit>` happen here (first FS touch), so pinning creation suffices for the observed
   failures; later FS ops reuse the cached `FileSystem` / already-loaded classes.

Why all three: #1 alone → ServiceLoader still reads parent (thread-context) service files → ServiceConfigurationError.
#3 alone → child lacks `S3AFileSystem` → "No FileSystem for scheme s3a". #2 covers the config-driven `getClass` path
that uses `Configuration.classLoader` (not the thread-context CL). Together: single-loader resolution.

## Implementation Plan
- `fe/fe-connector/fe-connector-paimon/pom.xml`: add the two deps next to `hadoop-common` with an explanatory comment.
- `PaimonCatalogFactory.java` (`buildHadoopConfiguration`, ~457): `conf.setClassLoader(...)` + comment.
- `PaimonConnector.java` (`createCatalogFromContext`, 192-198): context-CL pin try/finally + comment.

## Risk Analysis
- **Plugin size**: `hadoop-aws` pulls the AWS SDK (the heavy `bundle` is already excluded in depMgmt). Acceptable — it
  is the intended self-contained-plugin architecture.
- **hms/dlf flavors** (not in the failing set; cutover-gated): the context-CL pin in `createCatalogFromContext` applies
  to them too, but it is the correct plugin behavior (child-first still falls back to parent for the host-provided
  Thrift metastore client). No `HiveConf` classloader change (#2 is scoped to `buildHadoopConfiguration`, filesystem/jdbc only). Flag for hms/dlf cutover e2e.
- **Version skew**: `${hadoop.version}` mirrors fe-core, so the bundled FS impls match `hadoop-common`/paimon expectations.
- **Local verification gap**: the full fix is only provable by the docker external suite (CI-gated). Local proof =
  build + assert plugin `lib/` contents + connector UTs + the mechanism analysis above.

## Test Plan
### Unit Tests
- `PaimonCatalogFactoryTest` must stay green; the existing `buildHadoopConfiguration*` test still asserts S3 prefix /
  raw-key behavior (setClassLoader is orthogonal). Optionally add an assertion that
  `buildHadoopConfiguration(props).getClassLoader() == PaimonCatalogFactory.class.getClassLoader()`.
### E2E Tests
- No new suite — the existing `external_table_p0/paimon/*` (39 suites) ARE the regression coverage; they must go green
  in the docker external pipeline (`enablePaimonTest=true`, CI). Packaging assertion (lib/ contains hadoop-aws,
  `S3AFileSystem` resolvable in child) is verified at build time.

## Adversarial review (2026-06-12)
A skeptical reviewer returned "INSUFFICIENT/BLOCKER", but its headline rested on a **false premise**: it read the
`af2037` CI `fe.log` (the PRE-fix baseline, plugin `jarCount=143`, no hadoop-aws) as if it were a post-fix run, so the
recurring `S3AFileSystem cannot be cast` it cites is the *original* bug, not proof the fix fails. Its "loader-global
static cache" argument is also off — `FileSystem.SERVICE_FILE_SYSTEMS` / `Configuration.CACHE_CLASSES` are statics of
the *child's* `FileSystem`/`Configuration` class (per-loader), and are populated correctly under the #3 pin. Its
recommended remedy (`org.apache.hadoop.` parent-first) is the parent-leaning approach the user explicitly rejected
given the fe-core-hadoop-removal end-state. Two sub-findings WERE valid and folded in: (DEFECT 4) `hadoop-hdfs` does not
carry `DistributedFileSystem` → dropped; (DEFECT 3) AWS SDK v2 not in child → documented as single-copy parent
resolution (interim) + end-state follow-up. Post-creation FS access (planScan/rowCount) reuses the catalog's pinned
`SerializableConfiguration` (classLoader=child), so it is covered by #2 without needing its own pin.

## Verification boundary
Local proof = build green + all connector UTs pass + plugin lib contains `hadoop-aws`/`S3AFileSystem` + mechanism
analysis. The full runtime proof (the cross-loader split is gone end-to-end) is the docker external paimon suite, which
is CI-gated (`enablePaimonTest=false` locally) — NOT claimed to have run locally.
