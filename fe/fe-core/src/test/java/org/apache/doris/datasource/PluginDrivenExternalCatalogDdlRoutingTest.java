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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.Connector;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.ddl.ConnectorCreateTableRequest;
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
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        Mockito.when(mockEnv.getEditLog()).thenReturn(mockEditLog);
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

    // ==================== helpers ====================

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
