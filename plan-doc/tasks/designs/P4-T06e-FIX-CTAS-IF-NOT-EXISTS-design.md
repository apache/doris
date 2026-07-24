# Problem

After the MaxCompute SPI cutover, a `max_compute` catalog is a `PluginDrivenExternalCatalog`.
Its `createTable(CreateTableInfo)` override **unconditionally returns `false`** and
**unconditionally writes the create-table edit log** — even when the statement carried
`IF NOT EXISTS` and the target table already exists (the connector silently no-op'd it).

The return value is load-bearing for CTAS:

- `CreateTableCommand.run` (CTAS branch) at `CreateTableCommand.java:103`:
  `if (Env.getCurrentEnv().createTable(this.createTableInfo)) { return; }`
- `Env.createTable` (`Env.java:3749-3752`) returns `catalogIf.createTable(info)` directly —
  the override's return value flows straight up.

So `CREATE TABLE IF NOT EXISTS t AS SELECT ...` against an **already-existing** `t` returns
`false`, the CTAS does **not** short-circuit, and the command proceeds to build and run an
`INSERT INTO` the pre-existing table. This is a **silent data change** (DG-6 / F33), not the
benign edit-log redundancy it was previously triaged as (old DDL-C5, minor). The redundant
edit log is a secondary defect (one extra `OP_CREATE_TABLE` per IF-NOT-EXISTS hit).

The `Env.createTable` contract is explicit (`Env.java` Javadoc, just above `:3749`):
> `@return if CreateTableStmt.isIfNotExists is true, return true if table already exists otherwise return false`

The override violates this contract.

# Root Cause

`PluginDrivenExternalCatalog.createTable` overrides the base path and does its **own** edit
log — it never calls `super`/`ExternalCatalog.createTable`. So the fix lives entirely in this
override.

Confirmed cutover evidence — `PluginDrivenExternalCatalog.java:263-300`:

```
263  @Override
264  public boolean createTable(CreateTableInfo createTableInfo) throws UserException {
265      makeSureInitialized();
272      ExternalDatabase<? extends ExternalTable> db = getDbNullable(createTableInfo.getDbName());
273      if (db == null) { throw new DdlException("Failed to get database: ..."); }
277      ConnectorSession session = buildConnectorSession();
278      ConnectorCreateTableRequest request = CreateTableInfoToConnectorRequestConverter
279              .convert(createTableInfo, db.getRemoteName());
280      try {
281          connector.getMetadata(session).createTable(session, request);   // no-ops on existing+ifNotExists
282      } catch (DorisConnectorException e) { throw new DdlException(e.getMessage(), e); }
285      ... persistInfo = new org.apache.doris.persist.CreateTableInfo(getName(), dbName, tableName);
290      Env.getCurrentEnv().getEditLog().logCreateTable(persistInfo);          // ALWAYS written
296      getDbForReplay(...).ifPresent(d -> d.resetMetaCacheNames());           // ALWAYS reset
299      return false;                                                          // ALWAYS false  <-- BUG
300  }
```

The connector confirms the no-op semantics — `MaxComputeConnectorMetadata.createTable`
(`MaxComputeConnectorMetadata.java:331-345`):

```
337  if (structureHelper.tableExist(odps, dbName, tableName)) {
338      if (request.isIfNotExists()) {
339          LOG.info("create table[{}.{}] which already exists", dbName, tableName);
340          return;                      // <-- existing + IF NOT EXISTS: silent no-op
341      }
343      throw new DorisConnectorException("Table '" + tableName + "' already exists ...");
344  }                                    // <-- existing + NOT ifNotExists: already errors
```

So today: existing-table + `IF NOT EXISTS` → connector returns normally → override falls
through to `logCreateTable` + `resetMetaCacheNames` + `return false`. The `false` is the
regression; the edit log + cache reset are wasted work in that case.

`isIfNotExists` is correctly plumbed end-to-end (so the override can read it):
`CreateTableInfo.isIfNotExists()` exists (`CreateTableInfo.java:356`) and the converter
forwards it (`CreateTableInfoToConnectorRequestConverter.java:70` `.ifNotExists(info.isIfNotExists())`).

# Parity Reference

Legacy `MaxComputeMetadataOps.createTableImpl` — `MaxComputeMetadataOps.java:166-249`
(the exact branch being mirrored, `:178-197`):

```
166  public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
172      ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
173      if (db == null) { throw new UserException("Failed to get database: ..."); }
178      // 2. Check if table exists in remote
179      if (tableExist(db.getRemoteName(), tableName)) {
180          if (createTableInfo.isIfNotExists()) {
181              LOG.info("create table[{}] which already exists", tableName);
182              return true;                                  // <-- returns TRUE, before any SDK create
183          } else {
184              ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
185          }
186      }
188      // 3. Check if table exists in local (case sensitivity issue)
189      ExternalTable dorisTable = db.getTableNullable(tableName);
190      if (dorisTable != null) {
191          if (createTableInfo.isIfNotExists()) {
192              LOG.info("create table[{}] which already exists", tableName);
193              return true;                                  // <-- returns TRUE
194          } else {
195              ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
196          }
197      }
        ... // 4-8: validate + build schema + SDK create
248      return false;                                         // <-- new table: returns FALSE
249  }
```

And the editlog gate — base `ExternalCatalog.createTable` (`ExternalCatalog.java:1055-1080`),
which the **legacy** metadataOps path runs through:

```
1063  boolean res = metadataOps.createTable(createTableInfo);
1064  if (!res) {                          // <-- editlog ONLY when a NEW table was created
1071      Env.getCurrentEnv().getEditLog().logCreateTable(info);
1074  }
1075  return res;
```

Net legacy semantics to mirror:
1. existing + `IF NOT EXISTS` → `return true`, **no** SDK create, **no** editlog, (legacy also
   never invokes `afterCreateTable`/cache reset because the `!res` branch is skipped).
2. existing + **not** `IF NOT EXISTS` → `ERR_TABLE_EXISTS_ERROR` (a `DdlException`).
3. new → SDK create, `return false`, editlog written, cache reset.

# Design

Chosen direction (per the issue, honored): **NO SPI change.** Fix lives entirely inside the
`PluginDrivenExternalCatalog.createTable` override by adding the existence pre-check that
mirrors legacy `createTableImpl:178-197`, and branching the return value / side effects on it.

Existence check — mirror legacy's **two** probes:
- **Remote**: `connector.getMetadata(session).getTableHandle(session, db.getRemoteName(), tableName).isPresent()`.
  This is the legacy `tableExist(db.getRemoteName(), tableName)` analog. We reuse the existing
  SPI `getTableHandle` (`ConnectorTableOps.java:36`, default `Optional.empty()`) rather than
  adding a method — it is already overridden by MaxCompute (`MaxComputeConnectorMetadata.java:111`,
  backed by `structureHelper.tableExist`) and is the **same** method `dropTable` already uses in
  this class, so the pattern is in-house. The table name is passed **raw** (not remote-resolved),
  exactly as legacy `:179` and as the existing override's documented convention
  (`:270`: "table name is intentionally NOT remote-resolved").
- **Local**: `db.getTableNullable(tableName) != null` (legacy `:189`, the case-sensitivity guard).

Why `getTableHandle` and not a new `tableExists` SPI: MaxCompute *does* expose a public
`tableExists(session, dbName, tableName)` on its impl (`MaxComputeConnectorMetadata.java:105`),
but that method is **not** on the `ConnectorMetadata`/`ConnectorTableOps` SPI surface (no api-module
declaration — grep-confirmed), so it is not callable from fe-core through the connector interface
without an additive SPI change. `getTableHandle` *is* on the SPI with a safe `Optional.empty()`
default, so it is the zero-SPI-change, zero-other-connector-break path and matches Rule 2/Rule 3.

Branching:
- `exists && createTableInfo.isIfNotExists()` → `return true`; **skip** the connector
  `createTable` call (it would only no-op), **skip** `logCreateTable`, **skip**
  `resetMetaCacheNames`. (Mirrors legacy branch 1 + the `!res` editlog gate.)
- `exists && !isIfNotExists()` → **delegate the error to the connector** (do not add an FE-side
  throw). Rationale below.
- else (new) → unchanged: connector `createTable`, `logCreateTable`, `resetMetaCacheNames`,
  `return false`.

**Decision — non-`IF NOT EXISTS` existing-table error path (Rule 7 / Rule 2):**
Legacy raises `ERR_TABLE_EXISTS_ERROR` FE-side at `:184/:195`. The cutover connector already
raises on this case (`MaxComputeConnectorMetadata.java:343`,
`"Table '...' already exists in database '...'"`), which the override wraps to `DdlException`
at `:282-283`. To keep the change **minimal and surgical** and avoid a second source of truth
for "already exists", we do **not** add an FE-side `ErrorReport.reportDdlException` for the
existing-table check. Instead:
- We only short-circuit (skip the connector call) for the `IF NOT EXISTS` hit.
- For `exists && !isIfNotExists()` we let control fall through to the existing
  `connector.createTable(...)` call, which throws → `DdlException` (today's behavior, unchanged).

This means the FE-side existence probe is used **only** to decide the `IF NOT EXISTS`
short-circuit; the not-`IF NOT EXISTS` error stays exactly as it is today. Trade-off surfaced:
the legacy error code (`ERR_TABLE_EXISTS_ERROR`, MySQL 1050) differs from the connector's
generic `DdlException` message. That message divergence already exists on the cutover branch
today and is **out of scope** for this data-change fix; restoring the exact error code would
add FE-side error machinery for no behavioral parity benefit beyond the message text. Flagged
for cleanup, not fixed here. (If the orchestrator wants exact-message parity, see Open
Question.)

Also update the stale Javadoc at `:256-261` that claims the override "conservatively assumes
creation happened and writes the edit log" — that statement is now false for the IF-NOT-EXISTS
existing-table path.

# Implementation Plan

All production changes in **one** file. One test file changed. No signature changes anywhere.

### 1. `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java`

**(a) Replace the stale Javadoc paragraph at `:257-261`** (the "void SPI / conservatively
assumes creation happened" paragraph) with one that documents the new IF-NOT-EXISTS parity:

> The SPI `createTable` is `void` and the override has no `metadataOps`; this method therefore
> mirrors legacy `MaxComputeMetadataOps.createTableImpl`: when the table already exists and
> `IF NOT EXISTS` was given it returns `true` and skips the connector create + edit log + cache
> reset (so CTAS short-circuits instead of INSERTing into the existing table); otherwise it
> creates the table, writes the edit log, resets the cache, and returns `false`.

**(b) Insert the existence pre-check** after the session is built (after `:277`) and before the
converter/connector call. Method signature unchanged
(`public boolean createTable(CreateTableInfo createTableInfo) throws UserException`):

```java
ConnectorSession session = buildConnectorSession();
ConnectorMetadata metadata = connector.getMetadata(session);

// Mirror legacy MaxComputeMetadataOps.createTableImpl:178-197 -- probe both the remote
// (connector) and the local FE cache for an existing table. On IF NOT EXISTS this lets CTAS
// short-circuit (Env.createTable contract: return true when the table already exists), so a
// "CREATE TABLE IF NOT EXISTS ... AS SELECT" does NOT fall through to an INSERT into the
// pre-existing table. The table name is intentionally NOT remote-resolved (legacy parity).
boolean exists = metadata.getTableHandle(session, db.getRemoteName(),
        createTableInfo.getTableName()).isPresent()
        || db.getTableNullable(createTableInfo.getTableName()) != null;
if (exists && createTableInfo.isIfNotExists()) {
    LOG.info("create table[{}.{}.{}] which already exists; skipping (IF NOT EXISTS)",
            getName(), createTableInfo.getDbName(), createTableInfo.getTableName());
    return true;
}

ConnectorCreateTableRequest request = CreateTableInfoToConnectorRequestConverter
        .convert(createTableInfo, db.getRemoteName());
try {
    metadata.createTable(session, request);   // existing + !ifNotExists throws here -> DdlException (unchanged)
} catch (DorisConnectorException e) {
    throw new DdlException(e.getMessage(), e);
}
```

Reuse the `metadata` local for both the existence probe and the create call (replaces the
inline `connector.getMetadata(session)` at the old `:281`) — one `getMetadata` call, consistent
with how `dropTable` in this class already holds a `metadata` reference.

The new-table tail (`persistInfo` build, `logCreateTable`, `resetMetaCacheNames`, the existing
`LOG.info`, `return false`) is **unchanged**.

**Imports:** `ConnectorMetadata` (`org.apache.doris.connector.api.ConnectorMetadata`) — confirm
it is already imported (the class already uses `connector.getMetadata(...)`); if the local-var
type triggers an import, add it in CustomImportOrder position. No other new imports
(`getTableHandle`/`isIfNotExists`/`getTableNullable` are all on already-imported types).

### 2. `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`

Add tests in the `// ==================== CREATE TABLE ====================` section (see Test
Plan). No changes to the `TestablePluginCatalog` harness are required — it already exposes
`getDbNullable`/`getDbForReplay`/`resetMetaCacheNames` and mocks `metadata`. The only addition
is stubbing `db.getTableNullable(...)` and `metadata.getTableHandle(...)` per-test.

No call-site changes anywhere (no signature change).

# Blast Radius

**Other connectors (es/jdbc/hive/hudi/iceberg/paimon/trino): ZERO break — proven.**
- No SPI signature change (Rule: additive-or-none). The fix calls only `getTableHandle`, which
  is an *existing* `ConnectorTableOps` default returning `Optional.empty()`
  (`ConnectorTableOps.java:36-40`).
- This override (`PluginDrivenExternalCatalog.createTable`) is reached **only** by
  plugin-driven catalogs. jdbc/es/trino external catalogs route create-table through
  `ExternalCatalog.createTable` → `metadataOps.createTable` (their `metadataOps` is non-null);
  they never enter this override. (The test file's class Javadoc confirms: plugin catalogs have
  `metadataOps == null`.)
- For a hypothetical future full-adopter connector that does **not** override `getTableHandle`,
  `exists` collapses to `db.getTableNullable(...) != null` (FE-cache only). That is strictly
  *more* conservative than legacy MaxCompute (it just may miss a remote-only table that the FE
  cache hasn't seen), and never regresses the new-table path. No connector is broken.

**Callers of the changed method:**
- `Env.createTable` (`Env.java:3749-3752`) → `catalogIf.createTable(info)`: now receives `true`
  on the IF-NOT-EXISTS existing-table case. This is exactly the contract `Env.createTable`'s
  own Javadoc promises — the caller `CreateTableCommand.java:103` `if (createTable(...)) return;`
  now short-circuits as intended. No code change at the call site; behavior is the fix.
- Plain (non-CTAS) `CREATE TABLE IF NOT EXISTS` on an existing table: `CreateTableCommand.run`
  non-CTAS branch (`:91`) calls `Env.createTable` and ignores the return — behavior is now
  "no editlog, no SDK call" instead of "redundant editlog"; strictly an improvement, no visible
  user effect.

**Existing tests whose assertions must change: NONE (they are preserved, not changed).**
- `testCreateTablePassesRemoteDbNameToConverter` (`:315-341`): stubs neither `getTableNullable`
  nor `getTableHandle`. With the harness, `db` is a Mockito mock → `getTableNullable` returns
  `null`, and `metadata.getTableHandle(...)` returns `Optional.empty()` (Mockito default) →
  `exists == false` → the new-table path runs unchanged → `convert(info, "DB1")` still invoked.
  **Stays green.**
- `testCreateTableInvalidatesDbCacheUsingLocalNames` (`:353-389`): same — `exists == false` →
  `metadata.createTable` called, editlog written with local names, `resetMetaCacheNames` on the
  replay db. **Stays green.** (This is the regression guard for the new-table / common path.)
- `testCreateTableMissingDbThrows` (`:343-351`): `db == null` branch is untouched. **Stays green.**
- All `createDb`/`dropDb`/`dropTable` tests: untouched code paths.

So we **add** assertions for the existing-table path; we **change** none.

# Risk Analysis

- **Extra remote round-trip on every CREATE TABLE.** `getTableHandle` for MaxCompute calls
  `structureHelper.tableExist` *and* `getOdpsTable` (`MaxComputeConnectorMetadata.java:113-121`)
  — one extra ODPS metadata fetch per create. Legacy `createTableImpl` did the same `tableExist`
  probe (`:179`), so this is **parity, not a new cost** (legacy's `tableExist` was a remote
  call too). The `getOdpsTable` inside `getTableHandle` is marginally heavier than a bare
  existence check, but CREATE TABLE is rare and not latency-sensitive; acceptable. (Avoiding it
  would require a `tableExists` SPI method — rejected per the No-SPI-change directive.)
- **Short-circuit skips the connector's own `IF NOT EXISTS` no-op.** We now never call
  `connector.createTable` on the existing+ifNotExists path. The connector's branch
  (`:337-341`) was *also* a pure no-op in that case, so no behavior is lost; we just decide it
  FE-side (required to get the correct `return true`).
- **Local-vs-remote existence divergence.** If the FE cache is stale (table exists remotely but
  not in cache), `getTableHandle` still catches it (remote probe). If it exists in cache but not
  remotely (dropped out-of-band), `getTableNullable` catches it → we `return true` and skip
  create. Legacy did the identical OR (`:179` remote OR `:189` local), so this is exact parity.
- **Error-message divergence on `exists && !IF NOT EXISTS`** (DdlException text vs legacy
  `ERR_TABLE_EXISTS_ERROR`) — pre-existing on the cutover branch, explicitly out of scope,
  flagged for cleanup. Fail-loud is preserved (it still throws). Rule 12 satisfied.
- **Mutation safety / no silent skip.** The new-table path is byte-for-byte the old behavior;
  the only added branch is guarded by `exists && isIfNotExists()`. No path silently succeeds
  that previously failed.

# Test Plan

## Unit Tests

File: `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`
Class: `PluginDrivenExternalCatalogDdlRoutingTest` (add to the CREATE TABLE section).

**Test 1 — `testCreateTableIfNotExistsExistingTableReturnsTrueAndSkipsAllSideEffects`**
- Arrange: `db = mockExternalDatabase()`, `db.getRemoteName()` → `"DB1"`,
  `catalog.dbNullableResult = db`; `info.isIfNotExists()` → `true`,
  `info.getDbName()` → `"db1"`, `info.getTableName()` → `"t1"`; stub
  `metadata.getTableHandle(session, "DB1", "t1")` → `Optional.of(mock(ConnectorTableHandle.class))`.
- Act: `boolean res = catalog.createTable(info);`
- Assert:
  - `assertTrue(res, ...)` — WHY: a `false` here makes `CreateTableCommand:103` not short-circuit,
    so CTAS INSERTs into the already-existing table (silent data change, DG-6). This is the
    core regression guard.
  - `verify(metadata, never()).createTable(any(), any())` — WHY: the connector create must be
    skipped on the IF-NOT-EXISTS hit (it would only no-op; calling it is wasted + masks intent).
  - `verify(mockEditLog, never()).logCreateTable(any())` — WHY: legacy writes editlog only when a
    NEW table was created (`ExternalCatalog.java:1064` `if (!res)`); a redundant `OP_CREATE_TABLE`
    on an existing table pollutes the journal.
  - `assertEquals(0, catalog.resetMetaCacheNamesCount)` *(or* `verify(replayDb, never()).resetMetaCacheNames()`
    if a replay db is stubbed)* — WHY: no metadata changed, so no cache invalidation; legacy's
    `afterCreateTable` runs only on the `!res` branch.

**Test 2 — `testCreateTableIfNotExistsExistingLocalTableReturnsTrue`** (local-cache arm of the OR)
- Arrange: `db.getRemoteName()` → `"DB1"`; `metadata.getTableHandle(session, "DB1", "t1")`
  → `Optional.empty()` (remote says absent); `db.getTableNullable("t1")` → `mock(ExternalTable.class)`
  (FE cache has it); `info.isIfNotExists()` → `true`.
- Act/Assert: `assertTrue(res)`, `verify(metadata, never()).createTable(...)`,
  `verify(mockEditLog, never()).logCreateTable(...)`.
- WHY: legacy checks BOTH remote (`:179`) and local (`:189`); this guards the local arm so a
  refactor that drops the `getTableNullable` probe (keeping only `getTableHandle`) goes red —
  it encodes the case-sensitivity / stale-remote parity, not just "exists".

**Test 3 (new-table regression guard) — covered by EXISTING
`testCreateTableInvalidatesDbCacheUsingLocalNames` (`:353-389`).** It already asserts the
new-table path: `metadata.createTable` called, editlog written with local names,
`resetMetaCacheNames` on replay db. Its implicit return value is `false`. We rely on it as the
"new table still creates + logs" guard (no duplication, Rule 2). Optionally add one explicit
line to that test: `assertFalse(catalog.createTable(info))` capture — but since it already
locks the side effects, adding a 4th near-identical test is redundant.

**MUTATION CHECK (Rule 9):**
- One-line production revert: change the new branch back to the original unconditional tail —
  i.e. delete the `if (exists && createTableInfo.isIfNotExists()) { return true; }` block (so the
  method always falls through to `logCreateTable` + `resetMetaCacheNames` + `return false`).
  → **Test 1 goes red** on every assertion (`assertTrue(res)` first: gets `false`;
  `verify(never()).logCreateTable` fails: editlog written; `resetMetaCacheNamesCount` becomes 1).
  This is precisely the DG-6 production bug, so the test cannot pass while the bug is present.
- Second mutation: change `exists = getTableHandle(...).isPresent() || getTableNullable(...) != null`
  to drop the `|| getTableNullable(...) != null` arm. → **Test 2 goes red** (remote stub is
  empty, so `exists` becomes `false`, falls into the new-table path, `createTable` gets called).
  Encodes the local-cache parity intent.

## E2E Tests

`regression-test/suites/...` — the truth-gate is a live ODPS run (CI-skipped, per the standing
e2e policy; no e2e is exercised in CI). Intent to verify when a live ODPS env is available:

1. `CREATE TABLE IF NOT EXISTS mc_existing AS SELECT * FROM src;` against a pre-existing
   `mc_existing` → asserts the table's row count / contents are **unchanged** (no INSERT
   occurred) and the statement returns OK. This is the end-to-end form of the silent-data-change
   regression. (Pre-fix: rows from `src` get appended.)
2. `CREATE TABLE IF NOT EXISTS mc_new AS SELECT * FROM src;` for a fresh `mc_new` → table is
   created and populated (new-table path intact).

These belong alongside the existing MaxCompute CTAS / DDL suites; CI will skip the live-ODPS
suite. Note (Rule 12): UT alone cannot prove the absence of the downstream INSERT against a real
table — UT proves `createTable` returns `true` and the CTAS command's `if (...) return;`
short-circuits; the live e2e is what confirms no rows were written.
