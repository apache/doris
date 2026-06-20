# Task list — CI external-catalog regression fixes (TeamCity build `af2037`)

> Source: TeamCity external pipeline run on `af2037cf13b39b5877fdca1ad3e11c9a4873724f` (parent of HEAD `761a77049ce`).
> Logs: `/mnt/disk1/yy/tmp/64445_af2037cf13b39b5877fdca1ad3e11c9a4873724f_external/`.
> Full RCA (this session): 42 failed suites / 560. One dominant root cause (Paimon plugin Hadoop
> classloader split) = 39 suites + one real branch regression (`SHOW CREATE TABLE` plugin-props leak) = 1.
> **HEAD does NOT fix either** — `git log af2037..HEAD` over loader/pom/assembly/Env.java is empty.
> Execute each via the `step-by-step-fix` skill: design doc → impl → tests → **independent commit**.

## Commit hygiene (re-read before any `git add`)
- **Hard pre-req**: do NOT commit `regression-test/conf/regression-conf.groovy` (plaintext Aliyun key) or scratch
  (`.audit-scratch/`, `conf.cmy/`, `META-INF/`, `*.bak`, `plan-doc/reviews/*` if scratch). **Path-whitelist `git add` — NEVER `git add -A`.**
- Each fix = one commit; message = `fix: <ID>` + root cause + solution + tests, trailing
  `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`.

## Build / verify
- maven absolute `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> -am -Dmaven.build.cache.enabled=false -DfailIfNoTests=false`; verify via surefire XML + `MVN_EXIT` ([[doris-build-verify-gotchas]]).
- FIX-PAIMON-HADOOP-CLASSLOADER: connector-only — `-pl :fe-connector-paimon -am package`; then **unzip the plugin zip and assert `lib/` now contains `hadoop-aws-*.jar` + `hadoop-hdfs-*.jar`** (the packaging half of the fix). Run `PaimonCatalogFactoryTest`.
- FIX-SHOWCREATE-PLUGIN-PROPS: fe-core — `-pl :fe-core -am`; checkstyle `mvn -pl :fe-core checkstyle:check`.
- import-gate (connector edits): `bash tools/check-connector-imports.sh`.
- **live paimon e2e is CI-gated (`enablePaimonTest=false` locally)** — the docker external suite is the FINAL gate; note as CI-run, do NOT claim it ran locally.

---

## ✅ FIX-1 — `FIX-PAIMON-HADOOP-CLASSLOADER` (dominant, 39 suites) — IMPLEMENTED (uncommitted)
> Design+summary: `FIX-PAIMON-HADOOP-CLASSLOADER-{design,summary}.md`. 3 edits (pom add `hadoop-aws` only;
> `buildHadoopConfiguration` `conf.setClassLoader`; `createCatalogFromContext` context-CL pin). Verified: connector
> build SUCCESS + UTs 0/0; plugin lib has `hadoop-aws`/`S3AFileSystem`; checkstyle + import-gate clean. Adversarial
> review "BLOCKER" was a false premise (pre-fix `af2037` log); dropped `hadoop-hdfs` per a valid sub-finding. Runtime
> e2e CI-gated. **NEXT: commit (path-whitelist) after user review.**
**Symptom**: nearly every `external_table_p0/paimon/*` suite + 3 non-Paimon collateral (iceberg/remote_doris/describe) fail with one of:
`Could not initialize class org.apache.hadoop.security.SecurityUtil` · `S3AFileSystem cannot be cast to FileSystem` ·
`DNSDomainNameResolver not DomainNameResolver` · `ServiceConfigurationError: NullScanFileSystem not a subtype` ·
cascade `Unknown database 'X'`.
**Root cause**: the Paimon plugin (`ChildFirstClassLoader`, `org.apache.hadoop` NOT parent-first) bundles `hadoop-common`
(`fe-connector-paimon/pom.xml:92-95`, compile) but NOT `hadoop-aws`/`hadoop-hdfs`. So `FileSystem`/`SecurityUtil` load
child-first while `S3AFileSystem`/`DistributedFileSystem`/`DNSDomainNameResolver` resolve from the parent `app` loader →
cross-loader split; `SecurityUtil.<clinit>` then poisons JVM-permanently. `buildHadoopConfiguration():457` does a bare
`new Configuration()` (captures parent context CL); the connector never pins the context CL (JDBC/HMS connectors do).
**Fix (self-contained plugin; aligns with future fe-core hadoop/hive-shade removal — do NOT lean on parent)**:
  1. pom: add `hadoop-aws` + `hadoop-hdfs` (managed version/exclusions) so the child owns the full FS closure.
  2. `buildHadoopConfiguration`: `conf.setClassLoader(PaimonCatalogFactory.class.getClassLoader())` → `Configuration.getClass("fs.s3a.impl")` resolves child.
  3. `createCatalogFromContext` (single chokepoint, all flavors): pin thread-context CL to the plugin loader around `executeAuthenticated(...)` → `FileSystem` ServiceLoader + `SecurityUtil`/DNS resolve child. Mirror `JdbcConnectorClient:222-241`.
**Design**: `plan-doc/FIX-PAIMON-HADOOP-CLASSLOADER-design.md`

## ✅ FIX-2 — `FIX-SHOWCREATE-PLUGIN-PROPS` (regression, 1 suite + credential leak) — IMPLEMENTED (uncommitted)
> Design+summary: `FIX-SHOWCREATE-PLUGIN-PROPS-{design,summary}.md`. 1 edit (Env.java engine-type gate). Verified:
> fe-core compile SUCCESS + checkstyle clean; adversarial review VERDICT SOUND. **NEXT: commit (path-whitelist) after user review.**
**Symptom**: `test_nereids_refresh_catalog` — `SHOW CREATE TABLE` on a **JDBC** external table emits `LOCATION ''` +
`PROPERTIES(... "password"=... )` vs expected `ENGINE=JDBC_EXTERNAL_TABLE;`.
**Root cause**: branch commit `98a73bf7692` (D-046 paimon parity) added LOCATION+PROPERTIES rendering to the **shared**
`PLUGIN_EXTERNAL_TABLE` branch (`Env.java:4946-4959`), gated only on `!properties.isEmpty()`. JDBC/ES/Trino plugin tables
have non-empty props (incl. credentials) → wrongly rendered. Legacy = comment-only.
**Fix**: scope the LOCATION+PROPERTIES emission to the connector engine type that legacy rendered it (paimon /
file-table), not all plugin tables. Do NOT rebaseline the `.out` (would bake leaked creds into expected output).
**Design**: `plan-doc/FIX-SHOWCREATE-PLUGIN-PROPS-design.md`

---

## Out of scope (flagged — NOT this branch's regressions)
- `test_hive_ctas_to_doris`: upstream PR **#63794** (commit `3538497d40b`, ancestor of `af2037`) decoupled
  `enable_insert_strict`/`enable_strict_cast`; an over-length CTAS that used to fail now succeeds → stale `.out`
  assertion (`assertTrue(false)`). Not caused by catalog-spi; would fail on master too.
- `test_streaming_job_cdc_stream_postgres_latest_alter_cred`: Postgres logical-replication timing flake (timed out
  19:47, before any poisoning); branch never touched CDC code.
