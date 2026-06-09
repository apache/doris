# P4-T06d — FIX-READ-DESC — focused implementation design

> Issue: FIX-READ-DESC (阶段 1 blocker of P4-T06d, MaxCompute 翻闸 gap-fix).
> Parent design: `plan-doc/tasks/designs/P4-cutover-fix-design.md` §`### FIX-READ-DESC`
> (incl. its `#### 🔎 对抗 critic — verdict: sound` block). This doc is implementation-ready
> and resolves every correction/gap the critic raised. Date: 2026-06-07.

## Problem

After the `max_compute` cutover (T06b), a `max_compute` catalog instantiates as
`PluginDrivenExternalCatalog`. Any `SELECT` over a MaxCompute external table goes through
`PluginDrivenExternalTable.toThrift()` (`fe/fe-core/.../datasource/PluginDrivenExternalTable.java:249`),
which calls `metadata.buildTableDescriptor(...)`. `MaxComputeConnectorMetadata` does **not**
override it, so it hits the SPI default (`ConnectorTableOps.buildTableDescriptor`,
`fe/fe-connector/fe-connector-api/.../ConnectorTableOps.java:146-151`) which returns `null`.
fe-core then falls back to a `TTableType.SCHEMA_TABLE` descriptor **with no `mcTable`**
(`PluginDrivenExternalTable.java:257`).

BE then static_casts unconditionally to `MaxComputeTableDescriptor`
(`be/src/exec/scan/file_scanner.cpp:1069-1070` for `table_format_type=="max_compute"`), but the
real object is a `SchemaTableDescriptor` → type confusion → crash / garbage endpoint/project/quota/
credentials. Legacy worked; cutover breaks it. Severity: **blocker**.

## Root Cause

Direct cause: `MaxComputeConnectorMetadata` lacks a `buildTableDescriptor` override (unlike
`JdbcConnectorMetadata.java:182-217` / `EsConnectorMetadata.java:121-131`). The dispatch +
SPI hook + null fallback in fe-core are correct and generic; the fix belongs in the MC connector.

## Design (decisions B1–B4)

- **B1** — Add `@Override public org.apache.doris.thrift.TTableDescriptor buildTableDescriptor(...)`
  to `MaxComputeConnectorMetadata` with the SAME signature as the SPI default. Build a `TMCTable`
  and call `setEndpoint(endpoint)`, `setQuota(quota)`, `setProject(dbName)`, `setTable(remoteName)`,
  `setProperties(properties)`. `project`/`table` use the **remote-name params** (`dbName`,
  `remoteName` are already remote at the call site — see B-registrations OQ-7). Do **not** set
  region/access_key/secret_key/public_access/odps_url/tunnel_url (legacy leaves them unset /
  deprecated — mirror that; credentials flow through the `properties` map).
- **B2** — Construct
  `new org.apache.doris.thrift.TTableDescriptor(tableId, TTableType.MAX_COMPUTE_TABLE, numCols, 0, tableName, dbName)`
  then `setMcTable(tMcTable)`. The **6th ctor arg (descriptor dbName field) = remote `dbName` param**,
  mirroring legacy `MaxComputeExternalTable.toThrift:318-319` which passes `dbName` there. BE does
  NOT read this field for MC reads (JNI scanner uses `TMCTable.project/table`), so it is harmless,
  but we mirror legacy faithfully (this diverges from jdbc/es which pass `""` — recorded below).
- **B3** — Extend `MaxComputeConnectorMetadata` ctor with
  `private final String endpoint; private final String quota; private final Map<String,String> properties;`
  (reuse existing `java.util.Map` import) + corresponding ctor params; assign them. Update
  `MaxComputeDorisConnector.getMetadata` to pass `endpoint, quota, properties`. These fields are
  assigned in `doInit()` and `getMetadata()` calls `ensureInitialized()` first, so they are non-null
  at construction time.
- **B4** — Style: match the jdbc/es override exactly — fully-qualified `org.apache.doris.thrift.*`
  names, **no new thrift imports**. The connector import-gate only forbids fe-core internal packages
  and only scans `^import` lines in `src/main/java`; fully-qualified usage trips neither it nor
  Checkstyle. The only reused import is `java.util.Map` (already present).

## Implementation Plan (per file)

1. `fe/fe-connector/fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java`
   - Add three final fields `endpoint`, `quota`, `properties` and extend the ctor with the three
     params; assign them.
   - Add `@Override buildTableDescriptor(...)` per B1/B2 (fully-qualified thrift names).
2. `fe/fe-connector/fe-connector-maxcompute/.../MaxComputeDorisConnector.java`
   - `getMetadata`: `new MaxComputeConnectorMetadata(odps, structureHelper, defaultProject, endpoint, quota, properties)`.
3. No changes to BE, thrift, fe-core, or any other connector.

Gate: only the connector is touched → `mvn ... -pl :fe-connector-maxcompute` (no `-pl :fe-core`).

## Risk

- Low: pure new override + ctor passthrough. No fe-core dispatch / BE / thrift / other-connector
  change. jdbc/es/trino unaffected (own override or null fallback).
- Keep-set untouched: legacy `MaxComputeExternalTable.toThrift` stays (unused under cutover; removed
  in Batch D). No ordering conflict with this fix.
- BE `descriptors.cpp:289-320` reads region/access_key/... without `__isset` guards, but since we set
  the whole `mcTable`, unset fields default to empty strings (not UB) — identical to legacy, which also
  does not set them.

## Test Plan

- **UT** — new `MaxComputeBuildTableDescriptorTest` in
  `fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/`.
  Plain JUnit 5 (no fe-core dep, no Mockito). Construct `MaxComputeConnectorMetadata` directly with
  `null` odps/structureHelper (ctor only assigns; `buildTableDescriptor` never dereferences them) and
  real endpoint/quota/properties/defaultProject. Call `buildTableDescriptor(null, tableId, tableName,
  dbName, remoteName, numCols, catalogId)` and assert: (1) result != null; (2)
  `getTableType()==MAX_COMPUTE_TABLE`; (3) `isSetMcTable()`; (4) `mcTable.getEndpoint()/getQuota()/
  getProject()==dbName/getTable()==remoteName/getProperties()` equal inputs. Comment encodes WHY: BE
  static_casts to `MaxComputeTableDescriptor` and reads these as the auth/addressing contract — a
  SCHEMA_TABLE/null fallback crashes BE (Rule 9). The test FAILS if the override returns null or a
  SCHEMA_TABLE descriptor.
- **E2E (user-run, live ODPS)** — under cutover, run `test_external_catalog_maxcompute.groovy` /
  `test_max_compute_all_type.groovy` `SELECT` with column projection (a real-data SELECT, not just
  `count(*)`, per critic gap) against existing `.out` baselines.
- **Build note (both modules).** Run these UTs with `-am` (e.g.
  `mvn -f fe/pom.xml -pl :fe-core -am -DfailIfNoTests=false -Dtest=PluginDrivenExternalTableEngineTest test`).
  Without `-am`, sibling SNAPSHOT artifacts (incl. the connector-api jar) resolve from a stale
  `~/.m2`, causing `NoClassDefFoundError: ConnectorTransaction`. The `-am` reactor build also requires
  `-DfailIfNoTests=false` so the `-Dtest=` filter does not fail upstream modules with "No tests were
  executed".

## Parity & boundary registrations

- **OQ-7 — project/table use remote names (intentional fix).** The SPI read session itself uses
  remote names: `PluginDrivenScanNode` builds the table handle from `db.getRemoteName()/table
  .getRemoteName()`, and the JNI scanner does `requireNonNull(project)` + `odps.setDefaultProject(
  project)`. So the descriptor MUST carry remote names to stay consistent with the read session;
  reverting to legacy local names would diverge from the SPI read session. This is the correct,
  not merely tolerable, choice (same family as DDL-P3/DDL-C2 remote-name fixes). Note: for the
  actual data read the descriptor `project/table` are largely vestigial — real addressing uses the
  FE-prebuilt serialized scan session — but they must still be the remote names for consistency and
  to satisfy the BE `MaxComputeTableDescriptor` contract.
- **6th ctor arg dbName choice.** We pass the **remote `dbName` param** (mirrors legacy
  `MaxComputeExternalTable.toThrift:318-319`), NOT `""` as jdbc/es do. BE does not read
  `TTableDescriptor.dbName` for MC reads, so it is harmless; we choose legacy-faithful over
  jdbc/es-uniform here, and record the deliberate divergence from the jdbc/es style.
- **UT coverage boundary (now closed — two-sided).** Coverage is split across two modules:
  1. The connector UT (`MaxComputeBuildTableDescriptorTest`) asserts the override's OWN output. It
     CANNOT reach the fe-core `PluginDrivenExternalTable.toThrift` call site (cross-module; this
     module has no fe-core dependency).
  2. The fe-core call site is now covered by
     `PluginDrivenExternalTableEngineTest#testToThriftPassesRemoteNamesAndNumColsToBuildTableDescriptor`
     (`fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalTableEngineTest.java`).
     It uses a Mockito-mocked `ConnectorMetadata` with an `ArgumentCaptor` on `buildTableDescriptor`,
     drives `table.toThrift()` (stubbing only the two Env-backed methods it traverses —
     `makeSureInitialized()` and `getFullSchema()` — plus a `TestablePluginCatalog.getConnector()`
     override that bypasses Env-backed catalog init), and asserts the captured args:
     `dbName == "REMOTE_DB"` (≠ local `mydb`), `remoteName == "REMOTE_TBL"` (≠ local `mytbl`), and
     `numCols == schema.size()`. Local names differ from remote names so a regression that passes
     local names (or a wrong numCols) FAILS the test (verified by a temporary mutation:
     `db.getRemoteName()→db.getFullName()` / `getRemoteName()→getName()` / `size()→size()+1`
     produced `expected: <REMOTE_DB> but was: <mydb>`). The previous "e2e-only" claim is superseded:
     the call-site wiring is now automated; e2e still covers the live-ODPS end-to-end read.
- **time_zone note.** The BE JNI scanner requires `time_zone`, but BE injects it via the jni_reader
  framework (`jni_reader.cpp:151` `_scanner_params.emplace("time_zone", _state->timezone())`) for all
  JNI scanners, NOT via the descriptor. So this fix neither needs nor touches `time_zone`. Recorded so
  a future change to descriptor properties does not wrongly assume the descriptor must carry it.
