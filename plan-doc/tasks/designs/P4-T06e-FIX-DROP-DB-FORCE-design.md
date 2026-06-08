# Problem

`DROP DATABASE <db> FORCE` on a `max_compute` catalog no longer cascades the table
drops after the SPI cutover. The legacy `MaxComputeMetadataOps.dropDbImpl` enumerated
the remote tables and dropped each one before deleting the schema when `force==true`;
the plugin-driven path silently discards the `force` flag. On a non-empty schema this
degrades `DROP DB FORCE` to a non-FORCE drop — ODPS `schemas().delete()` does not
auto-cascade (the very existence of the legacy enumerate-loop proves it), so the drop
either fails outright or leaves residue. Silently dropping the user's `force` intent
also violates Rule 12 (fail loud).

Issue id: **P2-5 FIX-DROP-DB-FORCE** (clean-room re-review DG-3 / findings F22, F27).

# Root Cause

Confirmed against the code on branch `catalog-spi-05`:

**Cutover path (force dropped):**
- `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java:337-355`
  `dropDb(String dbName, boolean ifExists, boolean force)` accepts `force` but never
  forwards it. At **:348** it calls the 3-arg SPI:
  `connector.getMetadata(session).dropDatabase(session, dbName, ifExists)`. The Javadoc
  at **:332-335** explicitly self-documents the gap: *"The SPI carries no `force`;
  cascade semantics, if any, are left to the connector, so `force` is intentionally not
  forwarded."* — but the connector does NOT cascade either (below).
- `fe/fe-connector/fe-connector-api/.../ConnectorSchemaOps.java:54-59`
  the SPI only declares `default void dropDatabase(session, dbName, ifExists)` — there
  is no `force`/cascade parameter, so the flag cannot even reach the connector.
- `fe/fe-connector/fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java:415-420`
  `dropDatabase(session, dbName, ifExists)` is just
  `structureHelper.dropDb(odps, dbName, ifExists)` — no table enumeration, no cleanup.
  `ProjectSchemaTableHelper.dropDb` (`McStructureHelper.java:195`) calls
  `schemas().delete()`; `ProjectTableHelper.dropDb` (`:323-328`) throws "not supported"
  (namespace-schema=false has no schemas to drop).

**Effect:** with `force=true` on a non-empty schema, the cutover issues a bare
`schemas().delete()` against a schema that still has tables → ODPS rejects / residue.

# Parity Reference

Legacy code being mirrored —
`fe/fe-core/src/main/java/org/apache/doris/datasource/maxcompute/MaxComputeMetadataOps.java:132-157`:

```java
public void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException {
    ExternalDatabase<?> dorisDb = dorisCatalog.getDbNullable(dbName);
    if (dorisDb == null) {
        if (ifExists) { LOG.info(...); return; }
        else { ErrorReport.reportDdlException(ERR_DB_DROP_EXISTS, dbName); }
    }
    if (force) {                                                       // <-- cascade gate
        List<String> remoteTableNames = listTableNames(dorisDb.getRemoteName());
        for (String remoteTableName : remoteTableNames) {
            ExternalTable tbl = null;
            try {
                tbl = (ExternalTable) dorisDb.getTableOrDdlException(remoteTableName);
            } catch (DdlException e) { LOG.warn(...); continue; }       // <-- skip-on-fail
            dropTableImpl(tbl, true);                                   // drop each table first
        }
    }
    dorisCatalog.getMcStructureHelper().dropDb(odps, dbName, ifExists); // THEN delete schema
}
```

Two parity facts that bound the fix:
1. **Enumerate-then-drop ordering**: every table is dropped (with `ifExists=true`)
   BEFORE `dropDb` deletes the schema. This is the behavior we must restore.
2. **FE-cache effect of the legacy force path is db-level ONLY**: legacy emits NO
   per-table editlog and NO per-table `afterDropTable` for the cascaded tables — the
   only FE bookkeeping is the single db-level `afterDropDb → unregisterDatabase`
   (`MaxComputeMetadataOps.java:160-162`) plus the single `logDropDb`
   (`ExternalCatalog.dropDb`). Therefore pushing the cascade entirely into the
   connector (no per-table FE editlog) is exactly faithful to legacy — the plugin
   path already emits the one `logDropDb` + `unregisterDatabase`
   (`PluginDrivenExternalCatalog.java:352-353`), which is the complete legacy FE
   bookkeeping.

# Design

**Chosen direction (honoring the user's decision): extend the SPI `dropDatabase` with
`force` and push the cascade into the connector — NOT an FE-side cascade.**

Why this is correct and minimal:
- The cascade is inherently a remote-storage operation (enumerate ODPS tables, delete
  each via the ODPS `tables().delete()` already used by `MaxComputeConnectorMetadata.dropTable`).
  Pushing it into the connector keeps fe-core generic and confines MaxCompute-specific
  semantics to the MaxCompute connector — matching how the cutover already routes
  CREATE/DROP TABLE/DB through the SPI.
- An FE-side cascade would force fe-core to enumerate + per-table editlog, which is
  EXTRA bookkeeping legacy never did (legacy cascade is editlog-silent per table) — so
  the connector-side approach is *also* the closer parity match, not just the simpler one.
- **Additive-default SPI overload** (the established pattern used by P0-1/2/3 and
  P1-4's 6-arg `planScan`): add a new 4-arg `dropDatabase(session, dbName, ifExists,
  boolean force)` with a `default` body that delegates to the existing 3-arg form. The
  other six connectors (es/jdbc/hive/hudi/iceberg/paimon/trino) never override
  `ConnectorSchemaOps.dropDatabase` (verified: only MaxCompute does) and never call the
  SPI form (they go through their own `metadataOps`), so they are ZERO-break: the
  default 4-arg simply forwards to their inherited (or absent) 3-arg behavior.
- Only `MaxComputeConnectorMetadata` overrides the 4-arg to cascade. The cascade is
  gated by `if (force)`; non-force preserves today's behavior verbatim.

# Implementation Plan

Ordered, file-by-file. SPI change first (api module), then connector, then fe-core
caller, then tests.

### 1. SPI: add additive 4-arg overload
`fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorSchemaOps.java`

After the existing 3-arg `dropDatabase` (ends at :59), add:

```java
/**
 * Drops the specified database, cascading to its tables when {@code force} is true.
 * Default delegates to the non-cascading 3-arg form, so connectors that do not
 * support cascade keep their current behavior with zero change.
 */
default void dropDatabase(ConnectorSession session,
        String dbName, boolean ifExists, boolean force) {
    dropDatabase(session, dbName, ifExists);
}
```

Keep the existing 3-arg `dropDatabase` unchanged (it is the delegation target and is
still used by the default).

### 2. Connector: override the 4-arg to cascade
`fe/fe-connector/fe-connector-maxcompute/src/main/java/org/apache/doris/connector/maxcompute/MaxComputeConnectorMetadata.java:415-420`

Decision on the existing 3-arg override: **fold the 3-arg into the 4-arg** to avoid two
methods that both touch `dropDb`. Replace the current 3-arg `dropDatabase` override with
the 4-arg override; the inherited `default` 3-arg will delegate into it. Concretely,
change the existing override signature from
`dropDatabase(ConnectorSession session, String dbName, boolean ifExists)` to
`dropDatabase(ConnectorSession session, String dbName, boolean ifExists, boolean force)`
and add the cascade:

```java
@Override
public void dropDatabase(ConnectorSession session, String dbName,
        boolean ifExists, boolean force) {
    if (force) {
        // ODPS schemas().delete() does NOT auto-cascade; enumerate and drop each
        // table first (mirrors legacy MaxComputeMetadataOps.dropDbImpl force branch).
        for (String tableName : structureHelper.listTableNames(odps, dbName)) {
            try {
                structureHelper.dropTable(odps, dbName, tableName, true);
            } catch (OdpsException e) {
                throw new DorisConnectorException("Failed to drop MaxCompute table '"
                        + tableName + "' during force-drop of database '" + dbName
                        + "': " + e.getMessage(), e);
            }
        }
    }
    structureHelper.dropDb(odps, dbName, ifExists);
    LOG.info("dropped MaxCompute database {} (force={})", dbName, force);
}
```

Helper signatures confirmed present and already used in this class:
- `structureHelper.listTableNames(odps, dbName)` — used at `MaxComputeConnectorMetadata.java:102`.
- `structureHelper.dropTable(odps, dbName, tableName, true)` — used at `:398`
  (single-table `dropTable`); declared `throws OdpsException`
  (`McStructureHelper.java:73-74`), hence the try/catch wrapping into
  `DorisConnectorException` exactly as the existing single-table `dropTable` override
  does at `:399-401`.
- `structureHelper.dropDb(odps, dbName, ifExists)` — the existing terminal call (`:418`).

Note on legacy skip-on-fail (`continue`): legacy logs+continues if it can't *resolve*
a Doris table wrapper, but its actual remote drop (`dropTableImpl`) is NOT swallowed.
Here there is no FE table-wrapper resolution step (we enumerate remote names directly),
so the only failure point is the remote `dropTable`, which we surface as
`DorisConnectorException` (fail loud, Rule 12) rather than silently continuing — this is
stricter than legacy only for the genuinely-failing-remote-drop case, which is the
correct fail-loud behavior. No imports change (`OdpsException`, `DorisConnectorException`
already imported, confirmed at `:25` and `:37`).

### 3. fe-core caller: forward `force`
`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalCatalog.java:348`

Change:
```java
connector.getMetadata(session).dropDatabase(session, dbName, ifExists);
```
to:
```java
connector.getMetadata(session).dropDatabase(session, dbName, ifExists, force);
```

Also update the now-stale Javadoc at `:329-335` — replace the "force is intentionally
not forwarded" sentence with: "`force` is forwarded to the connector, which performs the
table cascade (mirroring legacy `MaxComputeMetadataOps.dropDbImpl`)." The surrounding
FE bookkeeping (`logDropDb` at :352, `unregisterDatabase` at :353) is unchanged — that
is the complete legacy db-level FE effect.

### 4. FE routing test: update 3-arg stubs to 4-arg + add force-forwarding tests
`fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`

The FE caller will now call the 4-arg SPI, so the existing Mockito verify/doThrow stubs
that reference the 3-arg `dropDatabase` must move to the 4-arg form:
- **:139** `Mockito.verify(metadata).dropDatabase(session, "db1", false)` →
  `Mockito.verify(metadata).dropDatabase(session, "db1", false, false)`.
- **:151** `Mockito.verify(metadata, Mockito.never()).dropDatabase(Mockito.any(), Mockito.any(), Mockito.anyBoolean())`
  → add a 4th matcher: `..., Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean())`.
- **:167** `Mockito.doThrow(...).when(metadata).dropDatabase(Mockito.any(), Mockito.any(), Mockito.anyBoolean())`
  → `..., Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean())`.

Add two new tests in the DROP DATABASE section (mock `ConnectorMetadata`, so the default
delegation is irrelevant — we assert the exact 4-arg call):
- `testDropDbForceForwardsForceTrueToConnector` — `dropDb("db1", false, true)` then
  `verify(metadata).dropDatabase(session, "db1", false, true)`.
- `testDropDbNonForceForwardsForceFalseToConnector` — `dropDb("db1", false, false)` then
  `verify(metadata).dropDatabase(session, "db1", false, false)`.

### 5. Connector cascade test (NEW)
`fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/MaxComputeConnectorMetadataDropDbTest.java`

The maxcompute test module has junit-jupiter but **NO mockito** (confirmed: no mockito
in `fe-connector-maxcompute/pom.xml`, connector parent, or api pom; no test uses
`org.mockito`). So the test uses a **hand-written recording fake `McStructureHelper`**
(implements the interface, records call order) — matching how
`MaxComputeBuildTableDescriptorTest` constructs the metadata offline with `null` odps.
The cascade code never dereferences `odps` (it only passes it to the fake helper), so
`null` odps is safe. See Test Plan for the exact tests.

# Blast Radius

**SPI overriders of `ConnectorSchemaOps.dropDatabase`** (grep across `fe/fe-connector/`):
only two files — `ConnectorSchemaOps.java` (declaration) and
`MaxComputeConnectorMetadata.java` (the sole override). The other six connectors
(es/jdbc/hive/hudi/iceberg/paimon/trino) do NOT override it and do NOT call the SPI form
(their DROP DB goes through `metadataOps.dropDb` /
`ExternalCatalog.dropDb:1037` / `PaimonMetadataOps.java:158` / `HiveMetadataOps.java:162`,
none of which touch `ConnectorSchemaOps`). The new 4-arg is a `default` that delegates to
the 3-arg, so:
- es/jdbc/hive/hudi/iceberg/paimon/trino: ZERO source change, ZERO behavior change
  (they never reach this method; even if they did, the default preserves 3-arg behavior).
- MaxCompute: only connector whose behavior changes, and only on the `force==true`
  branch (non-force is byte-for-byte the prior `dropDb` call).

**Production callers of the SPI `dropDatabase`**: exactly one —
`PluginDrivenExternalCatalog.java:348` (the FE caller being updated). No other
production call site exists (the many `dropDatabase` hits in the grep are Hive/Glue/Datalake
metastore *clients*, an unrelated method on a different interface).

**Tests whose assertions must change (signature change forces this):**
- `PluginDrivenExternalCatalogDdlRoutingTest.java:139,151,167` — the three 3-arg
  `dropDatabase` stubs/verifies, as itemized in Implementation Plan step 4. These are
  compile-or-verify breaks caused directly by the FE caller switching to the 4-arg form;
  they are mandatory.

No SPI method is removed; the 3-arg `dropDatabase` stays (it is the delegation target).
No fe-core public signature changes — `PluginDrivenExternalCatalog.dropDb` already had
`force` in its signature.

# Risk Analysis

1. **Cross-module rebuild ordering.** SPI lives in `fe-connector-api`. A signature
   *addition* (additive default) does not break binary compat for the existing 3-arg
   callers, but the new call site in fe-core and the new override in maxcompute both
   reference the 4-arg, so api + maxcompute + fe-core must be rebuilt together
   (`-am`). Mitigation: build order in Test Plan.

2. **dbName local-vs-remote name resolution (pre-existing, out of scope).** Legacy
   enumerates via `dorisDb.getRemoteName()` and drops via `dorisTable.getRemoteName()`,
   whereas `PluginDrivenExternalCatalog.dropDb` passes the **local** `dbName` straight to
   the SPI (it does NOT remote-resolve, unlike the `dropTable`/`createTable` overrides at
   :390/:279). The cascade therefore enumerates against whatever name the FE passes. For
   a non-name-mapped catalog (the common case) local==remote and behavior is correct.
   For a name-mapped catalog this is a latent bug — but it is **identical to and
   inherited from the already-shipped non-force 3-arg path** (which also passes local
   dbName to `dropDb`). Per Rule 3 (surgical) this fix does NOT widen scope to remote-
   resolve dbName; doing so would change the existing non-force path too. Surfaced as an
   open question (see below) and flagged for the DG-3/DG-4 DB-DDL triage batch.

3. **Partial cascade on mid-loop failure.** If table N's remote drop throws, tables
   1..N-1 are already gone and the schema is NOT deleted (we throw before `dropDb`).
   This is a fail-loud partial state — but it matches legacy's exposure (legacy's
   `dropTableImpl` could equally throw mid-loop) and is the correct behavior vs silently
   leaving residue. The DdlException surfaces to the user.

4. **namespace-schema=false mode.** `ProjectTableHelper.dropDb` throws "not supported"
   (`McStructureHelper.java:323-328`). With `force=true` in that mode, we now enumerate
   + dropTable first, then hit the same "not supported" on `dropDb`. Net behavior is the
   same terminal error as today (CREATE/DROP DB is unsupported without namespace-schema);
   the extra table drops before the throw are harmless because that mode realistically
   isn't used for DROP DB at all. No regression vs current.

5. **Live ODPS truth-gate.** UT verifies routing + cascade ordering with fakes/mocks;
   it cannot verify ODPS actually rejects non-empty `schemas().delete()`. That remains
   the standing live-e2e truth gate (CI-skip).

# Test Plan

## Unit Tests

### A. FE routing — `PluginDrivenExternalCatalogDdlRoutingTest` (fe-core)
File: `fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenExternalCatalogDdlRoutingTest.java`
Class: `PluginDrivenExternalCatalogDdlRoutingTest`

- **(edit) testDropDbRoutesToConnectorAndUnregisters** (:134) — change the :139 verify
  to `dropDatabase(session, "db1", false, false)`. WHY: the FE caller now uses the 4-arg
  SPI; this keeps the existing routing+unregister assertion valid against the new signature.
- **(edit) testDropDbIfExistsWhenMissingIsNoop** (:145) — change :151 `never()` matcher
  to the 4-arg form. WHY: keeps "missing db + IF EXISTS never touches the connector"
  meaningful against the new signature.
- **(edit) testDropDbWrapsConnectorException** (:163) — change :167 `doThrow...when`
  matcher to the 4-arg form. WHY: keeps "connector failure → DdlException" wrapping
  guarded against the new signature.
- **(new) testDropDbForceForwardsForceTrueToConnector** — `catalog.dbNullableResult =
  mock; catalog.dropDb("db1", false, true);` then
  `verify(metadata).dropDatabase(session, "db1", false, true)`. WHY (Rule 9): the
  regression is that `force` was silently dropped (Rule 12 violation / lost cascade);
  this asserts the user's `FORCE` intent actually reaches the connector.
- **(new) testDropDbNonForceForwardsForceFalseToConnector** — same but `force=false`
  → `verify(metadata).dropDatabase(session, "db1", false, false)`. WHY: guards that the
  fix does NOT spuriously cascade a non-FORCE drop (over-correction would be a new bug).

MUTATION check: reverting `PluginDrivenExternalCatalog.java:348` to pass a hardcoded
`false` (or back to the 3-arg form) makes **testDropDbForceForwardsForceTrueToConnector**
go red (verify sees `force=false`/no 4-arg call, not `true`).

### B. Connector cascade — `MaxComputeConnectorMetadataDropDbTest` (fe-connector-maxcompute, NEW)
File: `fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/MaxComputeConnectorMetadataDropDbTest.java`
Class: `MaxComputeConnectorMetadataDropDbTest`

Harness: NO mockito in this module → use a hand-written recording fake
`RecordingStructureHelper implements McStructureHelper` that (a) returns a fixed table
list from `listTableNames`, (b) appends `"dropTable:<name>"` to an ordered `List<String>`
log on each `dropTable`, (c) appends `"dropDb:<db>"` on `dropDb`. Construct
`new MaxComputeConnectorMetadata(null /*odps*/, fake, "proj", "ep", "quota", emptyMap)`
(same offline pattern as `MaxComputeBuildTableDescriptorTest`). Call the 4-arg
`dropDatabase` directly.

- **forceTrueCascadesAllTablesBeforeDroppingSchema** — fake `listTableNames` returns
  `["t1","t2"]`; call `dropDatabase(session, "db1", false, true)`; assert the recorded
  log equals `["dropTable:t1", "dropTable:t2", "dropDb:db1"]` (per-table drops, in order,
  all BEFORE the schema delete). WHY (Rule 9): encodes the legacy parity requirement that
  ODPS does NOT auto-cascade, so every table must be dropped first — the exact regression
  DG-3 describes. MUTATION: removing the `if (force) { ...enumerate/dropTable... }` block
  in `MaxComputeConnectorMetadata.dropDatabase` makes this red (log becomes just
  `["dropDb:db1"]`).
- **forceFalseDoesNotEnumerateOrDropTables** — fake `listTableNames` returns
  `["t1","t2"]` (would-be tables present); call `dropDatabase(session, "db1", false,
  false)`; assert the log equals `["dropDb:db1"]` (no `dropTable`, no enumeration). WHY:
  guards that non-FORCE never cascades — a regression in the other direction (always
  cascading) would silently delete tables on a plain DROP DB. MUTATION: changing the gate
  from `if (force)` to unconditional makes this red.
- **forceTrueOnEmptySchemaJustDropsDb** — fake `listTableNames` returns `[]`; call with
  `force=true`; assert log equals `["dropDb:db1"]`. WHY: FORCE on an empty schema must
  behave like a plain drop (no spurious table calls) — confirms the loop is a no-op when
  there are no tables.
- **forceTrueSurfacesRemoteDropFailureAsConnectorException** — fake `dropTable` throws
  `OdpsException` for `t2`; assert `dropDatabase(session,"db1",false,true)` throws
  `DorisConnectorException` whose message contains `t2`, AND that `dropDb` was NOT
  recorded (schema not deleted after a failed cascade). WHY (Rule 12, fail-loud): a
  failing remote drop must not be swallowed and must abort before deleting the schema —
  the opposite of the silent-degradation regression. MUTATION: swallowing the
  `OdpsException` (catch+continue without rethrow) makes this red.

## E2E Tests

No new e2e file is strictly required for UT-level parity, but the standing truth-gate is
live ODPS (CI-skip). If an e2e suite is added it belongs under
`regression-test/suites/external_table_p2/maxcompute/` (mirroring existing MC suites):
- `test_mc_drop_db_force` — create a `max_compute` schema, create ≥2 tables, run
  `DROP DATABASE <db> FORCE`, assert it succeeds and the schema + tables are gone;
  and that `DROP DATABASE <db>` (non-force) on a non-empty schema fails. CI-skip
  (requires real ODPS credentials/quota); this is the only layer that proves ODPS
  `schemas().delete()` truly rejects a non-empty schema, which the loop exists to avoid.
