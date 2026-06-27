// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.RefreshManager;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.constraint.ConstraintManager;
import org.apache.doris.catalog.info.BranchOptions;
import org.apache.doris.catalog.info.ColumnPosition;
import org.apache.doris.catalog.info.CreateOrReplaceBranchInfo;
import org.apache.doris.catalog.info.CreateOrReplaceTagInfo;
import org.apache.doris.catalog.info.DropBranchInfo;
import org.apache.doris.catalog.info.DropTagInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.info.TagOptions;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.BranchChange;
import org.apache.doris.connector.api.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.api.ddl.DropRefChange;
import org.apache.doris.connector.api.ddl.TagChange;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.ddl.CreateTableInfoToConnectorRequestConverter;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.persist.EditLog;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Tests for {@link PluginDrivenExternalCatalog}'s DDL overrides (createDb / dropDb /
 * dropTable) added by P4-T06c, and the cache-invalidation fix to the existing
 * createTable override.
 *
 * <p><b>Why these tests matter:</b> after the MaxCompute SPI cutover (T06b), a
 * {@code max_compute} catalog is a {@link PluginDrivenExternalCatalog} whose
 * {@code metadataOps} is always {@code null}. Without these overrides every DDL
 * would hit the base class and throw "… is not supported for catalog". These tests
 * lock in that DDL is routed to the connector SPI instead, that connector failures
 * are surfaced as {@link DdlException} (caller contract), that the SPI's missing
 * {@code ifNotExists}/{@code ifExists} semantics are enforced FE-side, and that the
 * FE metadata cache is invalidated after each op so the change is visible on the
 * same FE — exactly what the legacy {@code MaxComputeMetadataOps.afterX()} hooks did.</p>
 */
public class PluginDrivenExternalCatalogDdlRoutingTest {

    private MockedStatic<Env> mockedEnv;
    private EditLog mockEditLog;
    private RefreshManager mockRefreshManager;
    private ConstraintManager mockConstraintManager;
    private Connector connector;
    private ConnectorMetadata metadata;
    private ConnectorSession session;
    private TestablePluginCatalog catalog;

    @BeforeEach
    public void setUp() {
        connector = Mockito.mock(Connector.class);
        metadata = Mockito.mock(ConnectorMetadata.class);
        session = Mockito.mock(ConnectorSession.class);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);

        // Construct with the real Env singleton (the constructor is Env-safe), then
        // activate the static Env mock so the DDL overrides' edit-log writes are no-ops.
        catalog = new TestablePluginCatalog(connector);
        catalog.sessionMock = session;

        Env mockEnv = Mockito.mock(Env.class);
        mockEditLog = Mockito.mock(EditLog.class);
        mockRefreshManager = Mockito.mock(RefreshManager.class);
        mockConstraintManager = Mockito.mock(ConstraintManager.class);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        Mockito.when(mockEnv.getEditLog()).thenReturn(mockEditLog);
        Mockito.when(mockEnv.getRefreshManager()).thenReturn(mockRefreshManager);
        Mockito.when(mockEnv.getConstraintManager()).thenReturn(mockConstraintManager);
    }

    @AfterEach
    public void tearDown() {
        if (mockedEnv != null) {
            mockedEnv.close();
        }
    }

    // ==================== CREATE DATABASE ====================

    @Test
    public void testCreateDbRoutesToConnectorAndInvalidatesCache() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put("k", "v");

        catalog.createDb("db1", false, props);

        Mockito.verify(metadata).createDatabase(session, "db1", props);
        Mockito.verify(mockEditLog).logCreateDb(Mockito.any());
        Assertions.assertEquals(1, catalog.resetMetaCacheNamesCount,
                "createDb must invalidate the catalog db-name cache (legacy afterCreateDb parity)");
    }

    @Test
    public void testCreateDbIfNotExistsShortCircuitsWhenDbExists() throws Exception {
        catalog.dbNullableResult = Mockito.mock(ExternalDatabase.class);

        catalog.createDb("db1", true, new HashMap<>());

        Mockito.verify(metadata, Mockito.never()).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateDb(Mockito.any());
        Assertions.assertEquals(0, catalog.resetMetaCacheNamesCount);
    }

    @Test
    public void testCreateDbWrapsConnectorException() {
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.createDb("db1", false, new HashMap<>()));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    @Test
    public void testCreateDbIfNotExistsSkipsWhenRemoteExistsAndConnectorSupportsCreate() throws Exception {
        catalog.dbNullableResult = null; // FE-cache miss
        Mockito.when(metadata.supportsCreateDatabase()).thenReturn(true);
        Mockito.when(metadata.databaseExists(session, "db1")).thenReturn(true);

        catalog.createDb("db1", true, new HashMap<>());

        // WHY (Rule 9): DG-4 regression -- a db that exists REMOTELY but is not yet in this FE's
        // cache must make CREATE DATABASE IF NOT EXISTS a clean no-op (legacy createDbImpl consulted
        // the remote databaseExist), NOT surface a remote "already exists" error. A mutation that
        // removes the remote precheck calls createDatabase/logCreateDb -> these never() asserts red.
        Mockito.verify(metadata).databaseExists(session, "db1");
        Mockito.verify(metadata, Mockito.never()).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateDb(Mockito.any());
        Assertions.assertEquals(0, catalog.resetMetaCacheNamesCount);
    }

    @Test
    public void testCreateDbIfNotExistsCreatesWhenRemoteAbsent() throws Exception {
        catalog.dbNullableResult = null; // FE-cache miss
        Mockito.when(metadata.supportsCreateDatabase()).thenReturn(true);
        Mockito.when(metadata.databaseExists(session, "db1")).thenReturn(false); // absent remotely
        Map<String, String> props = new HashMap<>();

        catalog.createDb("db1", true, props);

        // WHY: remote-absent must still create + editlog + cache reset -- proves the fix did not
        // degrade IF NOT EXISTS into "never create". Paired with the test above (exists<->absent),
        // this pins both sides of legacy createDbImpl's existence branch.
        Mockito.verify(metadata).databaseExists(session, "db1");
        Mockito.verify(metadata).createDatabase(session, "db1", props);
        Mockito.verify(mockEditLog).logCreateDb(Mockito.any());
        Assertions.assertEquals(1, catalog.resetMetaCacheNamesCount);
    }

    @Test
    public void testCreateDbIfNotExistsBypassesPrecheckWhenConnectorLacksCreateSupport() throws Exception {
        catalog.dbNullableResult = null; // FE-cache miss
        // supportsCreateDatabase() defaults to false on the mock -- the connector cannot create
        // databases (jdbc/es/trino). databaseExists is intentionally NOT stubbed: it must never
        // be consulted (the && short-circuits on the capability gate).
        Map<String, String> props = new HashMap<>();

        catalog.createDb("db1", true, props);

        // WHY (Rule 9): the capability gate keeps jdbc/es/trino byte-identical -- a connector that
        // cannot create databases must fall through to createDatabase ("not supported" in
        // production), and the && must short-circuit so the remote databaseExists query is never
        // even issued. MUTATION: dropping the `supportsCreateDatabase() &&` gate makes databaseExists
        // get consulted here -> the never().databaseExists verify goes red (createDatabase still runs
        // because databaseExists defaults to false; the gate's job is to skip the remote probe).
        Mockito.verify(metadata, Mockito.never()).databaseExists(Mockito.any(), Mockito.any());
        Mockito.verify(metadata).createDatabase(session, "db1", props);
    }

    // ==================== DROP DATABASE ====================

    @Test
    public void testDropDbRoutesToConnectorAndUnregisters() throws Exception {
        catalog.dbNullableResult = Mockito.mock(ExternalDatabase.class);

        catalog.dropDb("db1", false, false);

        Mockito.verify(metadata).dropDatabase(session, "db1", false, false);
        Mockito.verify(mockEditLog).logDropDb(Mockito.any());
        Assertions.assertEquals("db1", catalog.unregisteredDb,
                "dropDb must remove the db from the cache (legacy afterDropDb parity)");
    }

    @Test
    public void testDropDbIfExistsWhenMissingIsNoop() throws Exception {
        catalog.dbNullableResult = null; // db not present

        catalog.dropDb("missing", true, false);

        Mockito.verify(metadata, Mockito.never())
                .dropDatabase(Mockito.any(), Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean());
        Assertions.assertNull(catalog.unregisteredDb);
    }

    @Test
    public void testDropDbMissingWithoutIfExistsThrows() {
        catalog.dbNullableResult = null;

        Assertions.assertThrows(DdlException.class, () -> catalog.dropDb("missing", false, false));
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void testDropDbWrapsConnectorException() {
        catalog.dbNullableResult = Mockito.mock(ExternalDatabase.class);
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).dropDatabase(Mockito.any(), Mockito.any(), Mockito.anyBoolean(), Mockito.anyBoolean());

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.dropDb("db1", false, false));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    @Test
    public void testDropDbForceForwardsForceTrueToConnector() throws Exception {
        catalog.dbNullableResult = Mockito.mock(ExternalDatabase.class);

        catalog.dropDb("db1", false, true);

        // WHY (Rule 9 / Rule 12): the regression (DG-3) is that the user's FORCE intent was
        // silently dropped at the FE→SPI boundary, so DROP DB FORCE stopped cascading table
        // drops. This asserts force=true actually reaches the connector. A mutation reverting
        // PluginDrivenExternalCatalog.dropDb to the 3-arg / hardcoded-false call makes it red.
        Mockito.verify(metadata).dropDatabase(session, "db1", false, true);
    }

    @Test
    public void testDropDbNonForceForwardsForceFalseToConnector() throws Exception {
        catalog.dbNullableResult = Mockito.mock(ExternalDatabase.class);

        catalog.dropDb("db1", false, false);

        // WHY: guards that the fix does NOT over-correct into always-cascading -- a plain
        // (non-FORCE) DROP DB must forward force=false so the connector never deletes tables.
        Mockito.verify(metadata).dropDatabase(session, "db1", false, false);
    }

    // ==================== DROP TABLE ====================
    // FIX-DDL-REMOTE: dropTable now resolves the local db/table names to their REMOTE (ODPS)
    // names (via getDbNullable + db.getTableNullable + getRemoteDbName/getRemoteName) before
    // calling the connector, mirroring base ExternalCatalog.dropTable / legacy
    // MaxComputeMetadataOps.dropTableImpl. Every drop test therefore stubs dbNullableResult and
    // db.getTableNullable; edit log / cache invalidation still use the LOCAL names.

    @Test
    public void testDropTableResolvesRemoteNamesRoutesAndUnregisters() throws Exception {
        // local db1.t1 maps to remote DB1.TBL1 (name mapping enabled).
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();   // resolution db (getDbNullable)
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        // Distinct replay db: locks that cache invalidation uses the getDbForReplay lookup, NOT
        // the resolution db (a refactor routing unregister through the resolution db must go red).
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);

        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));

        catalog.dropTable("db1", "t1", false, false, false, false, false, false);

        // WHY: the connector must receive the REMOTE names so name-mapped catalogs hit the real
        // ODPS object; a mutation that passes the local "db1"/"t1" makes this verify red.
        Mockito.verify(metadata).getTableHandle(session, "DB1", "TBL1");
        Mockito.verify(metadata).dropTable(session, handle);
        // WHY: edit log + cache invalidation MUST use the LOCAL names -- followers replay the
        // persisted DropInfo and the on-FE cache is keyed by local name. A mutation building
        // DropInfo / looking up getDbForReplay with the remote names must turn these red.
        ArgumentCaptor<org.apache.doris.persist.DropInfo> dropInfo =
                ArgumentCaptor.forClass(org.apache.doris.persist.DropInfo.class);
        Mockito.verify(mockEditLog).logDropTable(dropInfo.capture());
        Assertions.assertEquals("db1", dropInfo.getValue().getDb(),
                "edit-log DropInfo must carry the LOCAL db name for follower replay");
        Assertions.assertEquals("t1", dropInfo.getValue().getTableName(),
                "edit-log DropInfo must carry the LOCAL table name for follower replay");
        Assertions.assertEquals("db1", catalog.lastGetDbForReplayArg,
                "cache invalidation must look up the LOCAL db name");
        Mockito.verify(replayDb).unregisterTable("t1");
        Mockito.verify(db, Mockito.never()).unregisterTable(Mockito.anyString());
    }

    @Test
    public void testDropTableMissingDbThrowsEvenWithIfExists() {
        catalog.dbNullableResult = null; // db not present

        // WHY: mirror base ExternalCatalog.dropTable -- a missing db ALWAYS throws, even with
        // IF EXISTS (only a missing TABLE honors IF EXISTS). A mutation that ifExists-gates the
        // db==null branch makes this test red.
        Assertions.assertThrows(DdlException.class,
                () -> catalog.dropTable("missing", "t1", false, false, false, true, false, false));
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void testDropTableIfExistsWhenMissingTableIsNoop() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.doReturn(null).when(db).getTableNullable("missing");
        catalog.dbNullableResult = db;

        catalog.dropTable("db1", "missing", false, false, false, true, false, false);

        // Table missing + IF EXISTS => no-op; the connector is never even consulted.
        Mockito.verifyNoInteractions(metadata);
        Mockito.verify(mockEditLog, Mockito.never()).logDropTable(Mockito.any());
    }

    @Test
    public void testDropTableMissingTableWithoutIfExistsThrows() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.doReturn(null).when(db).getTableNullable("missing");
        catalog.dbNullableResult = db;

        Assertions.assertThrows(DdlException.class,
                () -> catalog.dropTable("db1", "missing", false, false, false, false, false, false));
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void testDropTableHandleAbsentAfterLocalResolveIsNoopWithIfExists() throws Exception {
        // FE cache has the table (resolves locally), but it was dropped out-of-band remotely:
        // getTableHandle returns empty. IF EXISTS must still no-op.
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.empty());

        catalog.dropTable("db1", "t1", false, false, false, true, false, false);

        Mockito.verify(metadata).getTableHandle(session, "DB1", "TBL1");
        Mockito.verify(metadata, Mockito.never()).dropTable(Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logDropTable(Mockito.any());
    }

    @Test
    public void testDropTableHandleAbsentAfterLocalResolveThrowsWithoutIfExists() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.empty());

        Assertions.assertThrows(DdlException.class,
                () -> catalog.dropTable("db1", "t1", false, false, false, false, false, false));
        Mockito.verify(metadata, Mockito.never()).dropTable(Mockito.any(), Mockito.any());
    }

    @Test
    public void testDropTableWrapsConnectorException() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;

        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).dropTable(session, handle);

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.dropTable("db1", "t1", false, false, false, false, false, false));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    // ==================== RENAME TABLE ====================
    // renameTable resolves the SOURCE by REMOTE names (like dropTable) and passes the new name through
    // (legacy renameTableImpl parity); afterExternalRename does the cache fix (unregister old + reset names)
    // + constraintManager rename + createForRenameTable editlog, all with LOCAL names for follower replay.

    @Test
    public void testRenameTableResolvesRemoteSourceRoutesAndFixesCache() throws Exception {
        // local db1.t1 maps to remote DB1.TBL1 (name mapping enabled).
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        // Distinct replay db: locks that the cache fix uses the getDbForReplay lookup (LOCAL name), not the
        // resolution db.
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));

        catalog.renameTable("db1", "t1", "t2");

        // WHY: the connector must receive the REMOTE source names + the new name; a mutation passing the
        // local "db1"/"t1" makes this verify red.
        Mockito.verify(metadata).getTableHandle(session, "DB1", "TBL1");
        Mockito.verify(metadata).renameTable(session, handle, "t2");
        // WHY (Rule 9): cache fix + constraint + editlog MUST use LOCAL names (followers replay the
        // createForRenameTable entry and the cache is keyed by local name). A mutation using remote names
        // for the bookkeeping turns these red.
        Assertions.assertEquals("db1", catalog.lastGetDbForReplayArg,
                "cache fix must look up the LOCAL db name");
        Mockito.verify(replayDb).unregisterTable("t1");
        Mockito.verify(replayDb).resetMetaCacheNames();
        ArgumentCaptor<TableNameInfo> oldName = ArgumentCaptor.forClass(TableNameInfo.class);
        ArgumentCaptor<TableNameInfo> newName = ArgumentCaptor.forClass(TableNameInfo.class);
        Mockito.verify(mockConstraintManager).renameTable(oldName.capture(), newName.capture());
        Assertions.assertEquals("t1", oldName.getValue().getTbl());
        Assertions.assertEquals("t2", newName.getValue().getTbl());
        ArgumentCaptor<ExternalObjectLog> logCap = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(mockEditLog).logRefreshExternalTable(logCap.capture());
        Assertions.assertEquals("db1", logCap.getValue().getDbName());
        Assertions.assertEquals("t1", logCap.getValue().getTableName());
        Assertions.assertEquals("t2", logCap.getValue().getNewTableName());
    }

    @Test
    public void testRenameTableMissingDbThrows() {
        catalog.dbNullableResult = null;

        Assertions.assertThrows(DdlException.class, () -> catalog.renameTable("missing", "t1", "t2"));
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void testRenameTableMissingTableThrows() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.doReturn(null).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;

        Assertions.assertThrows(DdlException.class, () -> catalog.renameTable("db1", "t1", "t2"));
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void testRenameTableWrapsConnectorExceptionAndSkipsBookkeeping() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));
        Mockito.doThrow(new DorisConnectorException("boom")).when(metadata).renameTable(session, handle, "t2");

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.renameTable("db1", "t1", "t2"));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
        // WHY: a remote rename failure must abort BEFORE any bookkeeping (no editlog, no constraint rename),
        // so the FE cache + constraints stay consistent with the unchanged remote.
        Mockito.verify(mockEditLog, Mockito.never()).logRefreshExternalTable(Mockito.any());
        Mockito.verifyNoInteractions(mockConstraintManager);
    }

    // ==================== CREATE TABLE ====================
    // FIX-DDL-REMOTE: createTable now resolves the local db name to its REMOTE (ODPS) name (via
    // getDbNullable + db.getRemoteName()) and passes THAT to the converter; the table name is
    // intentionally NOT remote-resolved (legacy parity). Edit log / cache invalidation still use
    // the local names.

    @Test
    public void testCreateTablePassesRemoteDbNameToConverter() throws UserException {
        // local db1 maps to remote DB1.
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        catalog.dbNullableResult = db;
        catalog.dbForReplayResult = Optional.of(db);

        try (MockedStatic<CreateTableInfoToConnectorRequestConverter> conv =
                Mockito.mockStatic(CreateTableInfoToConnectorRequestConverter.class)) {
            ConnectorCreateTableRequest req = Mockito.mock(ConnectorCreateTableRequest.class);
            conv.when(() -> CreateTableInfoToConnectorRequestConverter.convert(Mockito.any(), Mockito.any()))
                    .thenReturn(req);
            CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
            Mockito.when(info.getDbName()).thenReturn("db1");
            Mockito.when(info.getTableName()).thenReturn("t1");

            catalog.createTable(info);

            // WHY: the converter (and thus the connector) must receive the REMOTE db name "DB1",
            // not the local "db1", so name-mapped catalogs address the real ODPS schema. We assert
            // on the SECOND argument actually passed to convert() -- NOT on req.getDbName(), which
            // would be vacuous here because the converter is mocked and returns a stub unaffected
            // by the dbName argument. A mutation that passes info.getDbName() makes this red.
            conv.verify(() -> CreateTableInfoToConnectorRequestConverter.convert(info, "DB1"));
        }
    }

    @Test
    public void testCreateTableMissingDbThrows() {
        catalog.dbNullableResult = null; // db not present
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("missing");

        Assertions.assertThrows(DdlException.class, () -> catalog.createTable(info));
        Mockito.verifyNoInteractions(metadata);
    }

    @Test
    public void testCreateTableInvalidatesDbCacheUsingLocalNames() throws UserException {
        // remote DB1 != local db1, so the LOCAL-name assertions below are meaningful.
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        catalog.dbNullableResult = db;
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);

        try (MockedStatic<CreateTableInfoToConnectorRequestConverter> conv =
                Mockito.mockStatic(CreateTableInfoToConnectorRequestConverter.class)) {
            ConnectorCreateTableRequest req = Mockito.mock(ConnectorCreateTableRequest.class);
            conv.when(() -> CreateTableInfoToConnectorRequestConverter.convert(Mockito.any(), Mockito.any()))
                    .thenReturn(req);
            CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
            Mockito.when(info.getDbName()).thenReturn("db1");
            Mockito.when(info.getTableName()).thenReturn("t1");

            catalog.createTable(info);

            Mockito.verify(metadata).createTable(session, req);
            // WHY: edit log MUST carry the LOCAL names (followers replay this persist entry), even
            // though the connector got the remote "DB1". A mutation persisting db.getRemoteName()
            // must turn these red.
            ArgumentCaptor<org.apache.doris.persist.CreateTableInfo> persist =
                    ArgumentCaptor.forClass(org.apache.doris.persist.CreateTableInfo.class);
            Mockito.verify(mockEditLog).logCreateTable(persist.capture());
            Assertions.assertEquals("db1", persist.getValue().getDbName(),
                    "edit-log CreateTableInfo must carry the LOCAL db name for follower replay");
            Assertions.assertEquals("t1", persist.getValue().getTblName(),
                    "edit-log CreateTableInfo must carry the LOCAL table name for follower replay");
            // Cache invalidation must look up the LOCAL db name and act on the replay db.
            Assertions.assertEquals("db1", catalog.lastGetDbForReplayArg,
                    "cache invalidation must look up the LOCAL db name");
            Mockito.verify(replayDb).resetMetaCacheNames();
        }
    }

    @Test
    public void testCreateTableIfNotExistsExistingRemoteTableReturnsTrueAndSkipsSideEffects() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        catalog.dbNullableResult = db;
        // Distinct replay db: production resets the cache via getDbForReplay(...).resetMetaCacheNames()
        // on the REPLAY db object (NOT catalog.resetMetaCacheNames()), so we must assert on it.
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "t1")).thenReturn(Optional.of(handle));
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("db1");
        Mockito.when(info.getTableName()).thenReturn("t1");
        Mockito.when(info.isIfNotExists()).thenReturn(true);

        boolean res = catalog.createTable(info);

        // WHY (Rule 9 / DG-6): returning false here makes CreateTableCommand:103 not short-circuit,
        // so CTAS (CREATE TABLE IF NOT EXISTS ... AS SELECT) runs an INSERT into the pre-existing
        // table -- a SILENT DATA CHANGE. The fix must return true and skip create/editlog/cache-reset.
        Assertions.assertTrue(res,
                "IF NOT EXISTS on an existing table must return true so CTAS short-circuits (no INSERT)");
        Mockito.verify(metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
        Mockito.verify(replayDb, Mockito.never()).resetMetaCacheNames();
    }

    @Test
    public void testCreateTableIfNotExistsExistingLocalTableReturnsTrue() throws Exception {
        // Remote says absent (getTableHandle empty) but the FE cache HAS it -- the local arm of the
        // legacy OR (createTableImpl:189, the case-sensitivity / stale-remote guard).
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        Mockito.doReturn(Mockito.mock(ExternalTable.class)).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        Mockito.when(metadata.getTableHandle(session, "DB1", "t1")).thenReturn(Optional.empty());
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("db1");
        Mockito.when(info.getTableName()).thenReturn("t1");
        Mockito.when(info.isIfNotExists()).thenReturn(true);

        boolean res = catalog.createTable(info);

        // WHY: legacy checks BOTH remote AND local; this pins the local arm so a refactor that drops
        // the `|| db.getTableNullable(...) != null` probe (keeping only getTableHandle) goes red.
        Assertions.assertTrue(res, "existing local table + IF NOT EXISTS must return true");
        Mockito.verify(metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
    }

    @Test
    public void testCreateTableExistingRemoteTableWithoutIfNotExistsReportsErrno1050() {
        // FIX-R1-TABLE: a table that exists REMOTELY but is absent from this FE's cache (stale cache /
        // other-FE / external create), created without IF NOT EXISTS, must be rejected with MySQL errno
        // 1050 (ERR_TABLE_EXISTS_ERROR / SQLSTATE 42S01) -- legacy parity ({Paimon,MaxCompute}MetadataOps
        // both report 1050 for the remote arm). The connector is NOT consulted (the FE short-circuits).
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        catalog.dbNullableResult = db;
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "t1")).thenReturn(Optional.of(handle));
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("db1");
        Mockito.when(info.getTableName()).thenReturn("t1");
        Mockito.when(info.isIfNotExists()).thenReturn(false);

        // WHY (Rule 9 / Rule 12): pre-fix the bridge gated the 1050 report on localExists only, so a
        // remote-ONLY conflict fell through to metadata.createTable and surfaced a GENERIC DdlException
        // (errno ERR_UNKNOWN_ERROR = 0) -- silently dropping the documented MySQL 1050 contract some
        // ORMs branch on. The fix reports 1050 at the FE before the connector. MUTATION: restoring the
        // `if (localExists)` guard makes this remote-only case (localExists=false) fall through -> errno
        // reverts to 0 and metadata.createTable IS called -> the errno assertion AND the never().createTable
        // verify both go red.
        DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.createTable(info));
        Assertions.assertEquals(ErrorCode.ERR_TABLE_EXISTS_ERROR, ex.getMysqlErrorCode(),
                "remote-existing table without IF NOT EXISTS must surface MySQL errno 1050 (legacy parity)");
        Assertions.assertTrue(ex.getMessage().contains("already exists"));
        Mockito.verify(metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
    }

    @Test
    public void testCreateTableLocalConflictWithoutIfNotExistsRejects() throws Exception {
        // Remote says ABSENT (getTableHandle empty) but the FE cache HAS the table -- the local arm of the
        // legacy remote-then-local probe (PaimonMetadataOps.performCreateTable:206-214). Under
        // lower_case_meta_names a case-variant name folds onto an existing local table while the
        // case-sensitive remote has no such table. Legacy throws ERR_TABLE_EXISTS_ERROR here; the bridge
        // must NOT fall through to metadata.createTable, which would CREATE a duplicate remote table
        // (silent metadata corruption).
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        Mockito.doReturn(Mockito.mock(ExternalTable.class)).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        Mockito.when(metadata.getTableHandle(session, "DB1", "t1")).thenReturn(Optional.empty());

        try (MockedStatic<CreateTableInfoToConnectorRequestConverter> conv =
                Mockito.mockStatic(CreateTableInfoToConnectorRequestConverter.class)) {
            ConnectorCreateTableRequest req = Mockito.mock(ConnectorCreateTableRequest.class);
            conv.when(() -> CreateTableInfoToConnectorRequestConverter.convert(Mockito.any(), Mockito.any()))
                    .thenReturn(req);
            CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
            Mockito.when(info.getDbName()).thenReturn("db1");
            Mockito.when(info.getTableName()).thenReturn("t1");
            Mockito.when(info.isIfNotExists()).thenReturn(false);

            // WHY (Rule 9 / Rule 12): a local-ONLY conflict without IF NOT EXISTS must be REJECTED at the FE
            // level with MySQL errno 1050 (ERR_TABLE_EXISTS_ERROR), never handed to connector.createTable
            // (which would create a duplicate remote table under lower_case_meta_names case-folding). Paired
            // with testCreateTableExistingRemoteTableWithoutIfNotExistsReportsErrno1050, this pins that the
            // existence rejection covers EITHER arm: MUTATION re-narrowing the report to the remote arm only
            // (e.g. `if (remoteExists)`) lets this local-only case (remoteExists=false) fall through ->
            // createTable called + errno reverts to ERR_UNKNOWN_ERROR -> the asserts below go red.
            DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.createTable(info));
            Assertions.assertEquals(ErrorCode.ERR_TABLE_EXISTS_ERROR, ex.getMysqlErrorCode(),
                    "local-cache conflict without IF NOT EXISTS must surface MySQL errno 1050");
            Assertions.assertTrue(ex.getMessage().contains("already exists"));
            Mockito.verify(metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
            Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
        }
    }

    // ==================== COLUMN EVOLUTION (B2) ====================
    // The 6 column-op overrides resolve the connector handle by REMOTE names (like dropTable), convert the
    // Doris Column/ColumnPosition to the neutral SPI types, dispatch, wrap DorisConnectorException as
    // DdlException, and run afterExternalDdl (editlog with LOCAL names + RefreshManager.refreshTableInternal
    // re-resolving by REMOTE names). PluginDriven has no metadataOps, so without these overrides the base ops
    // would throw "not supported".

    @Test
    public void testAddColumnRoutesConvertsAndLogsRefresh() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.addColumn(table, nullableIntColumn("age"), ColumnPosition.FIRST);

        ArgumentCaptor<ConnectorColumn> colCap = ArgumentCaptor.forClass(ConnectorColumn.class);
        ArgumentCaptor<ConnectorColumnPosition> posCap = ArgumentCaptor.forClass(ConnectorColumnPosition.class);
        Mockito.verify(metadata).addColumn(Mockito.eq(session), Mockito.eq(handle),
                colCap.capture(), posCap.capture());
        Assertions.assertEquals("age", colCap.getValue().getName());
        // WHY: position FIRST must be neutralized to ConnectorColumnPosition.FIRST (toConnectorPosition); a
        // mutation dropping the isFirst() branch makes this red.
        Assertions.assertTrue(posCap.getValue().isFirst());
        // WHY (Rule 9): the editlog MUST carry the LOCAL names for follower replay (base
        // logRefreshExternalTable parity); a mutation persisting the remote names turns these red.
        ArgumentCaptor<ExternalObjectLog> logCap = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(mockEditLog).logRefreshExternalTable(logCap.capture());
        Assertions.assertEquals("db1", logCap.getValue().getDbName());
        Assertions.assertEquals("t1", logCap.getValue().getTableName());
    }

    @Test
    public void testAddColumnsRoutesConvertedList() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.addColumns(table, Arrays.asList(nullableIntColumn("a"), nullableIntColumn("b")));

        ArgumentCaptor<java.util.List<ConnectorColumn>> cap = ArgumentCaptor.forClass(java.util.List.class);
        Mockito.verify(metadata).addColumns(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        Assertions.assertEquals(2, cap.getValue().size());
        Assertions.assertEquals("a", cap.getValue().get(0).getName());
        Assertions.assertEquals("b", cap.getValue().get(1).getName());
    }

    @Test
    public void testDropColumnRoutes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.dropColumn(table, "age");

        Mockito.verify(metadata).dropColumn(session, handle, "age");
        Mockito.verify(mockEditLog).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testRenameColumnRoutes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.renameColumn(table, "old", "new");

        Mockito.verify(metadata).renameColumn(session, handle, "old", "new");
    }

    @Test
    public void testModifyColumnRoutesWithAfterPosition() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.modifyColumn(table, nullableIntColumn("age"), new ColumnPosition("id"));

        ArgumentCaptor<ConnectorColumnPosition> posCap = ArgumentCaptor.forClass(ConnectorColumnPosition.class);
        Mockito.verify(metadata).modifyColumn(Mockito.eq(session), Mockito.eq(handle),
                Mockito.any(ConnectorColumn.class), posCap.capture());
        // WHY: AFTER <col> must be neutralized to ConnectorColumnPosition.after(col); a mutation that drops
        // the afterColumn or flips it to FIRST makes these red.
        Assertions.assertFalse(posCap.getValue().isFirst());
        Assertions.assertEquals("id", posCap.getValue().getAfterColumn());
    }

    @Test
    public void testReorderColumnsRoutes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.reorderColumns(table, Arrays.asList("b", "a"));

        Mockito.verify(metadata).reorderColumns(session, handle, Arrays.asList("b", "a"));
    }

    @Test
    public void testColumnOpNullPositionConvertedToNull() throws Exception {
        ExternalTable table = mockAlterTable();
        stubAlterHandle();

        catalog.addColumn(table, nullableIntColumn("age"), null);

        ArgumentCaptor<ConnectorColumnPosition> posCap = ArgumentCaptor.forClass(ConnectorColumnPosition.class);
        Mockito.verify(metadata).addColumn(Mockito.any(), Mockito.any(),
                Mockito.any(ConnectorColumn.class), posCap.capture());
        // WHY: a null ColumnPosition (no position clause) must stay null across the SPI (toConnectorPosition
        // null-guard); a mutation returning FIRST/after for null would change append semantics.
        Assertions.assertNull(posCap.getValue());
    }

    @Test
    public void testColumnOpRefreshesTableCacheViaRefreshManager() throws Exception {
        ExternalTable table = mockAlterTable();
        stubAlterHandle();
        // afterExternalDdl re-resolves the cached table by the REMOTE names (legacy IcebergMetadataOps.refreshTable
        // parity), then calls RefreshManager.refreshTableInternal — the cache-invalidation the base column op
        // delegated into metadataOps and PluginDriven (metadataOps == null) must reproduce explicitly.
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        ExternalTable cached = Mockito.mock(ExternalTable.class);
        Mockito.doReturn(Optional.of(cached)).when(replayDb).getTableForReplay("TBL1");
        catalog.dbForReplayResult = Optional.of(replayDb);

        catalog.dropColumn(table, "age");

        // WHY (Rule 9 / BLOCKER-2): the base column ops do NOT invalidate the cache themselves — they delegate it
        // into metadataOps.refreshTable -> RefreshManager.refreshTableInternal. A helper that only writes the
        // editlog (the literal "copy the base op" reading) would SILENTLY lose cache invalidation after every
        // connector-driven schema change. These asserts pin that the refresh actually runs, re-resolving by the
        // REMOTE names. A mutation dropping the refreshTableInternal call goes red.
        Assertions.assertEquals("DB1", catalog.lastGetDbForReplayArg,
                "afterExternalDdl must re-resolve the cached table by the REMOTE db name (legacy parity)");
        Mockito.verify(replayDb).getTableForReplay("TBL1");
        Mockito.verify(mockRefreshManager)
                .refreshTableInternal(Mockito.eq(replayDb), Mockito.eq(cached), Mockito.anyLong());
    }

    @Test
    public void testColumnOpHandleAbsentThrows() {
        ExternalTable table = mockAlterTable();
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.empty());

        Assertions.assertThrows(DdlException.class, () -> catalog.dropColumn(table, "age"));
        Mockito.verify(metadata, Mockito.never()).dropColumn(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testColumnOpWrapsConnectorException() {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).dropColumn(session, handle, "age");

        DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.dropColumn(table, "age"));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
    }

    // Branch/tag ALTERs resolve the handle by REMOTE names (like the column ops), neutralize the nereids info
    // type to the SPI carrier (ConnectorBranchTagConverter), wrap a DorisConnectorException as a DdlException,
    // and run afterExternalDdl (editlog with LOCAL names + refreshTableInternal). PluginDriven has no
    // metadataOps, so without these overrides the base ops would throw "branching operation is not supported".

    @Test
    public void testCreateOrReplaceBranchRoutesConvertsAndRefreshes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        catalog.dbForReplayResult = Optional.of(mockExternalDatabase());

        CreateOrReplaceBranchInfo info = new CreateOrReplaceBranchInfo("b1", true, false, true,
                new BranchOptions(Optional.of(42L), Optional.of(86400000L),
                        Optional.of(5), Optional.of(172800000L)));
        catalog.createOrReplaceBranch(table, info);

        // WHY (Rule 9): the converter must map every field of the nereids info/options to the neutral carrier
        // (incl. the legacy retain->maxSnapshotAge / numSnapshots->minSnapshotsToKeep / retention->maxRefAge
        // mapping). A mutation dropping any field makes one of these asserts red.
        ArgumentCaptor<BranchChange> cap = ArgumentCaptor.forClass(BranchChange.class);
        Mockito.verify(metadata).createOrReplaceBranch(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        BranchChange b = cap.getValue();
        Assertions.assertEquals("b1", b.getName());
        Assertions.assertTrue(b.isCreate());
        Assertions.assertFalse(b.isReplace());
        Assertions.assertTrue(b.isIfNotExists());
        Assertions.assertEquals(42L, b.getSnapshotId().longValue());
        Assertions.assertEquals(86400000L, b.getMaxSnapshotAgeMs().longValue());
        Assertions.assertEquals(5, b.getMinSnapshotsToKeep().intValue());
        Assertions.assertEquals(172800000L, b.getMaxRefAgeMs().longValue());
        // WHY: branch/tag share the column-op bookkeeping (afterExternalDdl) — editlog with LOCAL names + cache
        // refresh re-resolving by REMOTE names. A mutation dropping the bookkeeping turns these red.
        ArgumentCaptor<ExternalObjectLog> logCap = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(mockEditLog).logRefreshExternalTable(logCap.capture());
        Assertions.assertEquals("db1", logCap.getValue().getDbName());
        Assertions.assertEquals("t1", logCap.getValue().getTableName());
        Assertions.assertEquals("DB1", catalog.lastGetDbForReplayArg,
                "afterExternalDdl must re-resolve the cached table by the REMOTE db name");
    }

    @Test
    public void testCreateOrReplaceBranchEmptyOptionsConvertToNulls() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        catalog.dbForReplayResult = Optional.of(mockExternalDatabase());

        catalog.createOrReplaceBranch(table, new CreateOrReplaceBranchInfo("b1", true, false, false,
                BranchOptions.EMPTY));

        ArgumentCaptor<BranchChange> cap = ArgumentCaptor.forClass(BranchChange.class);
        Mockito.verify(metadata).createOrReplaceBranch(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        BranchChange b = cap.getValue();
        // An absent SQL option must become a null carrier field (== "leave the snapshot/retention untouched").
        Assertions.assertNull(b.getSnapshotId());
        Assertions.assertNull(b.getMaxSnapshotAgeMs());
        Assertions.assertNull(b.getMinSnapshotsToKeep());
        Assertions.assertNull(b.getMaxRefAgeMs());
    }

    @Test
    public void testCreateOrReplaceBranchWrapsConnectorException() {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).createOrReplaceBranch(Mockito.eq(session), Mockito.eq(handle), Mockito.any());

        DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.createOrReplaceBranch(table,
                new CreateOrReplaceBranchInfo("b1", true, false, false, BranchOptions.EMPTY)));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
        // A remote failure must abort BEFORE bookkeeping (no editlog).
        Mockito.verify(mockEditLog, Mockito.never()).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testCreateOrReplaceTagRoutesConvertsAndRefreshes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        catalog.dbForReplayResult = Optional.of(mockExternalDatabase());

        catalog.createOrReplaceTag(table, new CreateOrReplaceTagInfo("v1", false, true, false,
                new TagOptions(Optional.of(9L), Optional.of(99000L))));

        ArgumentCaptor<TagChange> cap = ArgumentCaptor.forClass(TagChange.class);
        Mockito.verify(metadata).createOrReplaceTag(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        TagChange t = cap.getValue();
        Assertions.assertEquals("v1", t.getName());
        Assertions.assertFalse(t.isCreate());
        Assertions.assertTrue(t.isReplace());
        Assertions.assertEquals(9L, t.getSnapshotId().longValue());
        Assertions.assertEquals(99000L, t.getMaxRefAgeMs().longValue());
        Mockito.verify(mockEditLog).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testDropBranchRoutesConvertsAndRefreshes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        catalog.dbForReplayResult = Optional.of(mockExternalDatabase());

        catalog.dropBranch(table, new DropBranchInfo("b1", true));

        ArgumentCaptor<DropRefChange> cap = ArgumentCaptor.forClass(DropRefChange.class);
        Mockito.verify(metadata).dropBranch(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        Assertions.assertEquals("b1", cap.getValue().getName());
        Assertions.assertTrue(cap.getValue().isIfExists());
        Mockito.verify(mockEditLog).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testDropTagRoutesConvertsAndRefreshes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        catalog.dbForReplayResult = Optional.of(mockExternalDatabase());

        catalog.dropTag(table, new DropTagInfo("v1", false));

        ArgumentCaptor<DropRefChange> cap = ArgumentCaptor.forClass(DropRefChange.class);
        Mockito.verify(metadata).dropTag(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        Assertions.assertEquals("v1", cap.getValue().getName());
        Assertions.assertFalse(cap.getValue().isIfExists());
        Mockito.verify(mockEditLog).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testBranchTagHandleAbsentThrows() {
        ExternalTable table = mockAlterTable();
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.empty());

        Assertions.assertThrows(DdlException.class, () -> catalog.dropTag(table, new DropTagInfo("v1", false)));
        Mockito.verify(metadata, Mockito.never())
                .dropTag(Mockito.any(), Mockito.any(), Mockito.any());
    }

    // ==================== helpers ====================

    /** A mock external table whose LOCAL names are db1.t1 and REMOTE names DB1.TBL1 (name mapping enabled). */
    private ExternalTable mockAlterTable() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getDbName()).thenReturn("db1");
        Mockito.when(table.getName()).thenReturn("t1");
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        return table;
    }

    /** Stubs the connector handle resolution for the REMOTE names of {@link #mockAlterTable()}. */
    private ConnectorTableHandle stubAlterHandle() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));
        return handle;
    }

    /** A nullable INT Doris column (iceberg add/modify reject non-nullable adds). */
    private static Column nullableIntColumn(String name) {
        return new Column(name, Type.INT, false, null, true, null, "");
    }

    @SuppressWarnings("unchecked")
    private ExternalDatabase<? extends ExternalTable> mockExternalDatabase() {
        return (ExternalDatabase<? extends ExternalTable>) Mockito.mock(ExternalDatabase.class);
    }

    /**
     * Testable subclass: injects a mock connector, neutralizes init machinery, and
     * makes the FE-cache hooks observable so DDL routing + cache invalidation can be
     * asserted without a full Doris environment.
     */
    private static class TestablePluginCatalog extends PluginDrivenExternalCatalog {
        ConnectorSession sessionMock;
        ExternalDatabase<? extends ExternalTable> dbNullableResult;
        Optional<ExternalDatabase<? extends ExternalTable>> dbForReplayResult = Optional.empty();
        int resetMetaCacheNamesCount;
        String unregisteredDb;
        // Records the arg passed to getDbForReplay so tests can assert the cache-invalidation
        // lookup uses the LOCAL db name (follower-replay parity), not the remote-resolved one.
        String lastGetDbForReplayArg;

        TestablePluginCatalog(Connector initial) {
            super(1L, "test-catalog", null, testProps(), "", initial);
            this.initialized = true;
        }

        @Override
        protected void initLocalObjectsImpl() {
            // no-op: connector is injected via constructor; skip txn-manager/auth setup.
        }

        @Override
        public ConnectorSession buildConnectorSession() {
            return sessionMock;
        }

        @Override
        public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
            return dbNullableResult;
        }

        @Override
        public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(String dbName) {
            lastGetDbForReplayArg = dbName;
            return dbForReplayResult;
        }

        @Override
        public void resetMetaCacheNames() {
            resetMetaCacheNamesCount++;
        }

        @Override
        public void unregisterDatabase(String dbName) {
            unregisteredDb = dbName;
        }

        private static Map<String, String> testProps() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "test");
            return props;
        }
    }
}
