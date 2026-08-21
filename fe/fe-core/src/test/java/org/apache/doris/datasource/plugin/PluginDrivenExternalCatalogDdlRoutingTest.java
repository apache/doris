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

package org.apache.doris.datasource.plugin;

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
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.Util;
import org.apache.doris.connector.ddl.CreateTableInfoToConnectorRequestConverter;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.ConnectorColumn;
import org.apache.doris.connector.spi.ConnectorMetadata;
import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.ConnectorStatementScope;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.ddl.BranchChange;
import org.apache.doris.connector.spi.ddl.ConnectorColumnPosition;
import org.apache.doris.connector.spi.ddl.ConnectorCreateTableRequest;
import org.apache.doris.connector.spi.ddl.DropRefChange;
import org.apache.doris.connector.spi.ddl.PartitionFieldChange;
import org.apache.doris.connector.spi.ddl.TagChange;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalMetaCacheMgr;
import org.apache.doris.datasource.ExternalRowCountCache;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.log.ExternalObjectLog;
import org.apache.doris.nereids.trees.plans.commands.info.AddPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropPartitionFieldOp;
import org.apache.doris.nereids.trees.plans.commands.info.ReplacePartitionFieldOp;
import org.apache.doris.persist.CreateDbInfo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.TruncateTableInfo;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.util.concurrent.MoreExecutors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/** Tests connector-routed external DDL, persistence, and local cache invalidation. */
public class PluginDrivenExternalCatalogDdlRoutingTest {

    private static final String CATALOG_NAME = "test-catalog";
    private static final String DATABASE_NAME = "db1";
    private static final long DATABASE_ID = Util.genIdByName(CATALOG_NAME, DATABASE_NAME);
    private static final long TABLE_ID = Util.genIdByName(CATALOG_NAME, DATABASE_NAME, "t1");
    private MockedStatic<Env> mockedEnv;
    private Env mockEnv;
    private EditLog mockEditLog;
    private RefreshManager mockRefreshManager;
    private ConstraintManager mockConstraintManager;
    private ExternalMetaCacheMgr mockMetaCacheMgr;
    private Connector connector;
    private ConnectorMetadata metadata;
    private ConnectorSession session;
    private TestablePluginCatalog catalog;

    @BeforeEach
    public void setUp() {
        connector = Mockito.mock(Connector.class);
        metadata = Mockito.mock(ConnectorMetadata.class);
        session = Mockito.mock(ConnectorSession.class);
        Mockito.when(session.getStatementScope()).thenReturn(ConnectorStatementScope.NONE);
        Mockito.when(connector.getMetadata(Mockito.any())).thenReturn(metadata);
        Mockito.when(metadata.fromRemoteDatabaseName(Mockito.eq(session), Mockito.anyString()))
                .thenAnswer(inv -> inv.getArgument(1));
        Mockito.when(metadata.fromRemoteTableName(Mockito.eq(session), Mockito.anyString(), Mockito.anyString()))
                .thenAnswer(inv -> inv.getArgument(2));

        // Construct with the real Env singleton (the constructor is Env-safe), then
        // activate the static Env mock so the DDL overrides' edit-log writes are no-ops.
        catalog = new TestablePluginCatalog(connector);
        catalog.sessionMock = session;

        mockEnv = Mockito.mock(Env.class);
        mockEditLog = Mockito.mock(EditLog.class);
        mockRefreshManager = Mockito.mock(RefreshManager.class);
        mockConstraintManager = Mockito.mock(ConstraintManager.class);
        mockMetaCacheMgr = Mockito.mock(ExternalMetaCacheMgr.class);
        mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(mockEnv);
        Mockito.when(mockEnv.getEditLog()).thenReturn(mockEditLog);
        Mockito.when(mockEnv.getRefreshManager()).thenReturn(mockRefreshManager);
        Mockito.when(mockEnv.getConstraintManager()).thenReturn(mockConstraintManager);
        Mockito.when(mockEnv.getExtMetaCacheMgr()).thenReturn(mockMetaCacheMgr);
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
        Assertions.assertEquals(1, catalog.resetMetaCacheNamesCount);
        InOrder order = Mockito.inOrder(connector, mockMetaCacheMgr);
        order.verify(connector).invalidateDb("db1");
        order.verify(mockMetaCacheMgr).invalidateDb(
                1L, Util.genIdByName("test-catalog", "db1"), "db1");
    }

    @Test
    public void testCreateDbPersistsCanonicalLocalName() throws Exception {
        Mockito.when(metadata.fromRemoteDatabaseName(session, "REMOTE_DB")).thenReturn("local_db");

        catalog.createDb("REMOTE_DB", false, new HashMap<>());

        ArgumentCaptor<CreateDbInfo> log = ArgumentCaptor.forClass(CreateDbInfo.class);
        Mockito.verify(mockEditLog).logCreateDb(log.capture());
        Assertions.assertEquals("local_db", log.getValue().getDbName());
        Mockito.verify(connector).invalidateDb("REMOTE_DB");
        Mockito.verify(mockMetaCacheMgr).invalidateDb(
                1L, Util.genIdByName("test-catalog", "local_db"), "local_db");
    }

    @Test
    public void testCreateDbIfNotExistsRefreshesCachesWhenDbExists() throws Exception {
        catalog.dbNullableResult = mockExternalDatabase();
        long constraintMetadataBaseline = catalog.snapshotConstraintMetadata();

        catalog.createDb("db1", true, new HashMap<>());

        Mockito.verify(metadata, Mockito.never()).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateDb(Mockito.any());
        Mockito.verify(connector).invalidateDb("DB1");
        Assertions.assertEquals(1, catalog.resetMetaCacheNamesCount);
        Mockito.verify(mockMetaCacheMgr).invalidateDb(
                1L, Util.genIdByName("test-catalog", "db1"), "db1");
        Assertions.assertNotEquals(
                constraintMetadataBaseline, catalog.snapshotConstraintMetadata());
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
    public void testCreateDbIfNotExistsSkipsWhenRemoteExists() throws Exception {
        catalog.dbNullableResult = null; // FE-cache miss
        Mockito.when(metadata.databaseExists(session, "db1")).thenReturn(true);

        catalog.createDb("db1", true, new HashMap<>());

        Mockito.verify(metadata).databaseExists(session, "db1");
        Mockito.verify(metadata, Mockito.never()).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateDb(Mockito.any());
        Assertions.assertEquals(1, catalog.resetMetaCacheNamesCount);
        Mockito.verify(mockMetaCacheMgr).invalidateDb(
                1L, Util.genIdByName("test-catalog", "db1"), "db1");
    }

    @Test
    public void testCreateDbIfNotExistsCreatesWhenRemoteAbsent() throws Exception {
        catalog.dbNullableResult = null; // FE-cache miss
        Mockito.when(metadata.databaseExists(session, "db1")).thenReturn(false); // absent remotely
        Map<String, String> props = new HashMap<>();

        catalog.createDb("db1", true, props);

        Mockito.verify(metadata).databaseExists(session, "db1");
        Mockito.verify(metadata).createDatabase(session, "db1", props);
        Mockito.verify(mockEditLog).logCreateDb(Mockito.any());
        Assertions.assertEquals(1, catalog.resetMetaCacheNamesCount);
    }

    @Test
    public void testCreateDbIfNotExistsLosingTheCreateRaceReturnsWithoutEditLog() throws Exception {
        Mockito.when(metadata.databaseExists(session, "db1")).thenReturn(false, true);
        Mockito.doThrow(new DorisConnectorException("database already exists"))
                .when(metadata).createDatabase(session, "db1", Collections.emptyMap());

        catalog.createDb("db1", true, Collections.emptyMap());

        Mockito.verify(metadata, Mockito.times(2)).databaseExists(session, "db1");
        Mockito.verify(mockEditLog, Mockito.never()).logCreateDb(Mockito.any());
        Mockito.verify(connector).invalidateDb("db1");
        Mockito.verify(mockMetaCacheMgr).invalidateDb(
                1L, Util.genIdByName("test-catalog", "db1"), "db1");
    }

    @Test
    public void testCreateDbIfNotExistsSucceedsWhenConnectorCannotCreateButDbExists() throws Exception {
        catalog.dbNullableResult = null; // FE-cache miss
        // A connector that cannot create databases (jdbc/es/trino): createDatabase throws the SPI
        // default, but the db is already there remotely.
        Mockito.when(metadata.databaseExists(session, "db1")).thenReturn(true);
        Mockito.doThrow(new DorisConnectorException("CREATE DATABASE not supported"))
                .when(metadata).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());

        catalog.createDb("db1", true, new HashMap<>());

        Mockito.verify(metadata).databaseExists(session, "db1");
        Mockito.verify(metadata, Mockito.never()).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateDb(Mockito.any());
    }

    @Test
    public void testCreateDbIfNotExistsStillReachesConnectorWhenDbAbsent() throws Exception {
        catalog.dbNullableResult = null; // FE-cache miss
        // databaseExists defaults to false on the mock: a connector that answers neither question is
        // byte-identical to before -- it falls through to createDatabase ("not supported").
        Mockito.doThrow(new DorisConnectorException("CREATE DATABASE not supported"))
                .when(metadata).createDatabase(Mockito.any(), Mockito.any(), Mockito.any());

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.createDb("db1", true, new HashMap<>()));

        Assertions.assertTrue(ex.getMessage().contains("CREATE DATABASE not supported"));
    }

    // ==================== DROP DATABASE ====================

    @Test
    public void testDropDbRoutesToConnectorAndUnregisters() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("db1"); // non-mapped: LOCAL == REMOTE
        catalog.dbNullableResult = db;

        catalog.dropDb("db1", false, false);

        Mockito.verify(metadata).dropDatabase(session, "db1", false, false);
        Mockito.verify(mockEditLog).logDropDb(Mockito.any());
        Assertions.assertEquals("db1", catalog.unregisteredDb,
                "dropDb must remove the db from the cache (legacy afterDropDb parity)");
        Mockito.verify(mockMetaCacheMgr).invalidateDb(1L, DATABASE_ID, "db1");
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
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("db1"); // non-mapped: LOCAL == REMOTE
        catalog.dbNullableResult = db;

        catalog.dropDb("db1", false, true);

        Mockito.verify(metadata).dropDatabase(session, "db1", false, true);
    }

    @Test
    public void testDropDbNonForceForwardsForceFalseToConnector() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("db1"); // non-mapped: LOCAL == REMOTE
        catalog.dbNullableResult = db;

        catalog.dropDb("db1", false, false);

        Mockito.verify(metadata).dropDatabase(session, "db1", false, false);
    }

    @Test
    public void testDropDbResolvesRemoteNameRoutesAndUnregisters() throws Exception {
        // local "db1" maps to remote "REMOTE_DB1" (name mapping enabled).
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("REMOTE_DB1");
        catalog.dbNullableResult = db;

        catalog.dropDb("db1", false, true);

        Mockito.verify(metadata).dropDatabase(session, "REMOTE_DB1", false, true);
        // Connector calls use remote names; edit-log and FE cache identities stay local.
        ArgumentCaptor<org.apache.doris.persist.DropDbInfo> dropDbInfo =
                ArgumentCaptor.forClass(org.apache.doris.persist.DropDbInfo.class);
        Mockito.verify(mockEditLog).logDropDb(dropDbInfo.capture());
        Assertions.assertEquals("db1", dropDbInfo.getValue().getDbName(),
                "edit-log DropDbInfo must carry the LOCAL db name for follower replay");
        Assertions.assertEquals("db1", catalog.unregisteredDb,
                "cache invalidation must use the LOCAL db name");
        Mockito.verify(connector).invalidateDb("REMOTE_DB1");
    }

    // ==================== DROP TABLE ====================
    @Test
    public void testDropTableKeepsExactCleanupAfterDatabaseCacheEviction() throws Exception {
        // local db1.t1 maps to remote DB1.TBL1 (name mapping enabled).
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();   // resolution db (getDbNullable)
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t1");
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("T1");
        catalog.dbNullableResult = db;
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);

        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));
        Mockito.doAnswer(inv -> {
            catalog.dbForReplayResult = Optional.empty();
            return null;
        }).when(metadata).dropTable(session, handle);

        catalog.dropTable("DB1", "T1", false, false, false, false, false, false);

        Mockito.verify(metadata).getTableHandle(session, "DB1", "TBL1");
        Mockito.verify(metadata).dropTable(session, handle);
        ArgumentCaptor<org.apache.doris.persist.DropInfo> dropInfo =
                ArgumentCaptor.forClass(org.apache.doris.persist.DropInfo.class);
        Mockito.verify(mockEditLog).logDropTable(dropInfo.capture());
        Assertions.assertEquals("db1", dropInfo.getValue().getDb(),
                "edit-log DropInfo must carry the LOCAL db name for follower replay");
        Assertions.assertEquals("t1", dropInfo.getValue().getTableName(),
                "edit-log DropInfo must carry the LOCAL table name for follower replay");
        Assertions.assertEquals("db1", catalog.lastGetDbForReplayArg);
        Mockito.verify(replayDb, Mockito.never()).unregisterTable(Mockito.anyString());
        Mockito.verify(db, Mockito.never()).unregisterTable(Mockito.anyString());
        Mockito.verify(mockMetaCacheMgr).invalidateTable(1L, DATABASE_ID, "db1", TABLE_ID, "t1");
        // Connector caches are keyed by the remote identity.
        Mockito.verify(connector).invalidateTable("DB1", "TBL1");
    }

    @Test
    public void testDropTableMissingDbThrowsEvenWithIfExists() {
        catalog.dbNullableResult = null; // db not present

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
    public void testDropTableHandleAbsentAfterLocalResolveCleansLocalStateWithIfExists() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t1");
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.empty());

        catalog.dropTable("db1", "t1", false, false, false, true, false, false);

        Mockito.verify(metadata).getTableHandle(session, "DB1", "TBL1");
        Mockito.verify(metadata, Mockito.never()).dropTable(Mockito.any(), Mockito.any());
        Mockito.verify(connector).invalidateTable("DB1", "TBL1");
        Mockito.verify(mockEditLog).logDropTable(Mockito.any());
        Mockito.verify(mockMetaCacheMgr).invalidateTable(1L, DATABASE_ID, "db1", TABLE_ID, "t1");
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
        Mockito.verify(connector, Mockito.never()).invalidateTable(Mockito.any(), Mockito.any());
    }

    @Test
    public void testDropTableRoutesViewToDropViewAndUnregisters() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable view = Mockito.mock(ExternalTable.class);
        Mockito.when(view.getName()).thenReturn("v1");
        Mockito.when(view.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(view.getRemoteName()).thenReturn("V1");
        Mockito.doReturn(view).when(db).getTableNullable("v1");
        catalog.dbNullableResult = db;
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);
        Mockito.when(metadata.viewExists(session, "DB1", "V1")).thenReturn(true);

        catalog.dropTable("db1", "v1", false, false, false, false, false, false);

        Mockito.verify(metadata).dropView(session, "DB1", "V1");
        Mockito.verify(metadata, Mockito.never()).getTableHandle(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(metadata, Mockito.never()).dropTable(Mockito.any(), Mockito.any());
        ArgumentCaptor<org.apache.doris.persist.DropInfo> dropInfo =
                ArgumentCaptor.forClass(org.apache.doris.persist.DropInfo.class);
        Mockito.verify(mockEditLog).logDropTable(dropInfo.capture());
        Assertions.assertEquals("db1", dropInfo.getValue().getDb(),
                "edit-log DropInfo must carry the LOCAL db name for follower replay");
        Assertions.assertEquals("v1", dropInfo.getValue().getTableName(),
                "edit-log DropInfo must carry the LOCAL view name for follower replay");
        Assertions.assertEquals("db1", catalog.lastGetDbForReplayArg);
        Mockito.verify(replayDb).unregisterTable("v1");
        Mockito.verify(connector).invalidateTable("DB1", "V1");
    }

    @Test
    public void testDropViewWrapsConnectorException() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable view = Mockito.mock(ExternalTable.class);
        Mockito.when(view.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(view.getRemoteName()).thenReturn("V1");
        Mockito.doReturn(view).when(db).getTableNullable("v1");
        catalog.dbNullableResult = db;
        Mockito.when(metadata.viewExists(session, "DB1", "V1")).thenReturn(true);
        Mockito.doThrow(new DorisConnectorException("boom")).when(metadata).dropView(session, "DB1", "V1");

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.dropTable("db1", "v1", false, false, false, false, false, false));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
        Mockito.verify(mockEditLog, Mockito.never()).logDropTable(Mockito.any());
    }

    // ==================== RENAME TABLE ====================
    @Test
    public void testRenameTableKeepsCanonicalExactCleanupAfterDatabaseCacheEviction() throws Exception {
        // local db1.t1 maps to remote DB1.TBL1 (name mapping enabled).
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t1");
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        Mockito.when(db.canonicalLocalTableNameFromRemote("REMOTE_T2")).thenReturn("local_t2");
        catalog.dbNullableResult = db;
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));
        Mockito.doAnswer(inv -> {
            catalog.dbForReplayResult = Optional.empty();
            return null;
        }).when(metadata).renameTable(session, handle, "REMOTE_T2");

        catalog.renameTable("db1", "t1", "REMOTE_T2");

        Mockito.verify(metadata).getTableHandle(session, "DB1", "TBL1");
        Mockito.verify(metadata).renameTable(session, handle, "REMOTE_T2");
        Assertions.assertEquals("db1", catalog.lastGetDbForReplayArg);
        Mockito.verify(replayDb, Mockito.never()).invalidateTableRename(Mockito.anyString(), Mockito.anyString());
        ArgumentCaptor<TableNameInfo> oldName = ArgumentCaptor.forClass(TableNameInfo.class);
        ArgumentCaptor<TableNameInfo> newName = ArgumentCaptor.forClass(TableNameInfo.class);
        Mockito.verify(mockConstraintManager).renameTable(oldName.capture(), newName.capture());
        Assertions.assertEquals("t1", oldName.getValue().getTbl());
        Assertions.assertEquals("local_t2", newName.getValue().getTbl());
        ArgumentCaptor<ExternalObjectLog> logCap = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(mockEditLog).logRefreshExternalTable(logCap.capture());
        Assertions.assertEquals("db1", logCap.getValue().getDbName());
        Assertions.assertEquals("t1", logCap.getValue().getTableName());
        Assertions.assertEquals("local_t2", logCap.getValue().getNewTableName());
        // The destination has no pre-rename mapping, so its remote name is the requested new name.
        Mockito.verify(connector).invalidateTable("DB1", "TBL1");
        Mockito.verify(connector).invalidateTable("DB1", "REMOTE_T2");
        Mockito.verify(mockMetaCacheMgr).invalidateTableRename(
                1L, DATABASE_ID, "db1", TABLE_ID, "t1",
                Util.genIdByName("test-catalog", "db1", "local_t2"), "local_t2");
    }

    @Test
    public void testRenameResolvesDestinationIdentityBeforeRemoteMutation() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        Mockito.when(db.canonicalLocalTableNameFromRemote("TBL2"))
                .thenThrow(new IllegalStateException("name mapping failed"));
        catalog.dbNullableResult = db;
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));

        Assertions.assertThrows(IllegalStateException.class,
                () -> catalog.renameTable("db1", "t1", "TBL2"));

        Mockito.verify(metadata, Mockito.never()).renameTable(Mockito.any(), Mockito.any(), Mockito.anyString());
        Mockito.verify(mockEditLog, Mockito.never()).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testRenameTableUsesStructuralSourceInvalidationAndDestinationBarrier() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t1");
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(table).when(db).getTableNullable("t1");
        catalog.dbNullableResult = db;
        ExternalDatabase<? extends ExternalTable> cachedDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(cachedDb);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));

        catalog.renameTable("db1", "t1", "t2");

        long destinationId = Util.genIdByName("test-catalog", "db1", "t2");
        InOrder order = Mockito.inOrder(mockEditLog, connector, cachedDb, mockMetaCacheMgr);
        order.verify(mockEditLog).logRefreshExternalTable(Mockito.any());
        order.verify(connector).invalidateTable("DB1", "TBL1");
        order.verify(connector).invalidateTable("DB1", "t2");
        order.verify(cachedDb).invalidateTableRename("t1", "t2");
        order.verify(mockMetaCacheMgr).invalidateTableRename(
                1L, DATABASE_ID, "db1", TABLE_ID, "t1", destinationId, "t2");
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
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));
        Mockito.doThrow(new DorisConnectorException("boom")).when(metadata).renameTable(session, handle, "t2");

        DdlException ex = Assertions.assertThrows(DdlException.class,
                () -> catalog.renameTable("db1", "t1", "t2"));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
        Mockito.verify(mockEditLog, Mockito.never()).logRefreshExternalTable(Mockito.any());
        Mockito.verifyNoInteractions(mockConstraintManager);
        Mockito.verify(connector, Mockito.never()).invalidateTable(Mockito.any(), Mockito.any());
    }

    // ==================== CREATE TABLE ====================
    @Test
    public void testCreateTablePassesRemoteDbNameToConverter() throws UserException {
        // local db1 maps to remote DB1.
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
            ArgumentCaptor<org.apache.doris.persist.CreateTableInfo> persist =
                    ArgumentCaptor.forClass(org.apache.doris.persist.CreateTableInfo.class);
            Mockito.verify(mockEditLog).logCreateTable(persist.capture());
            Assertions.assertEquals("db1", persist.getValue().getDbName(),
                    "edit-log CreateTableInfo must carry the LOCAL db name for follower replay");
            Assertions.assertEquals("t1", persist.getValue().getTblName(),
                    "edit-log CreateTableInfo must carry the LOCAL table name for follower replay");
            Mockito.verify(mockMetaCacheMgr).invalidateTable(
                    1L, DATABASE_ID, "db1", Util.genIdByName("test-catalog", "db1", "t1"), "t1");
            Mockito.verify(connector).invalidateTable("DB1", "t1");
        }
    }

    @Test
    public void testCreateTablePersistsCanonicalLocalTableName() throws UserException {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        Mockito.when(db.canonicalLocalTableNameFromRemote("REMOTE_T1")).thenReturn("local_t1");
        catalog.dbNullableResult = db;

        try (MockedStatic<CreateTableInfoToConnectorRequestConverter> converter =
                Mockito.mockStatic(CreateTableInfoToConnectorRequestConverter.class)) {
            ConnectorCreateTableRequest request = Mockito.mock(ConnectorCreateTableRequest.class);
            CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
            Mockito.when(info.getDbName()).thenReturn("db1");
            Mockito.when(info.getTableName()).thenReturn("REMOTE_T1");
            converter.when(() -> CreateTableInfoToConnectorRequestConverter.convert(info, "DB1"))
                    .thenReturn(request);

            catalog.createTable(info);

            Mockito.verify(db).getTableNullable("local_t1");
            ArgumentCaptor<org.apache.doris.persist.CreateTableInfo> log =
                    ArgumentCaptor.forClass(org.apache.doris.persist.CreateTableInfo.class);
            Mockito.verify(mockEditLog).logCreateTable(log.capture());
            Assertions.assertEquals("db1", log.getValue().getDbName());
            Assertions.assertEquals("local_t1", log.getValue().getTblName());
            Mockito.verify(connector).invalidateTable("DB1", "REMOTE_T1");
            Mockito.verify(mockMetaCacheMgr).invalidateTable(
                    1L, DATABASE_ID, "db1", Util.genIdByName("test-catalog", "db1", "local_t1"), "local_t1");
        }
    }

    @Test
    public void testCreateTableIfNotExistsEvictsPreexistingRowCount() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        catalog.dbNullableResult = db;
        catalog.dbForReplayResult = Optional.of(db);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "t1")).thenReturn(Optional.of(handle));
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("db1");
        Mockito.when(info.getTableName()).thenReturn("t1");
        Mockito.when(info.isIfNotExists()).thenReturn(true);

        long tableId = Util.genIdByName("test-catalog", "db1", "t1");
        AtomicLong rowCount = new AtomicLong(100L);
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.fetchRowCountWithMetaCache(false)).thenAnswer(inv -> rowCount.get());
        ExternalMetaCacheMgr realMetaCacheMgr = new ExternalMetaCacheMgr(true);
        ExternalRowCountCache rowCountCache = new ExternalRowCountCache(
                MoreExecutors.newDirectExecutorService());
        Deencapsulation.setField(realMetaCacheMgr, "rowCountCache", rowCountCache);
        Mockito.when(mockEnv.getExtMetaCacheMgr()).thenReturn(realMetaCacheMgr);

        try (MockedStatic<StatisticsUtil> statisticsUtil = Mockito.mockStatic(StatisticsUtil.class)) {
            statisticsUtil.when(() -> StatisticsUtil.findTable(1L, DATABASE_ID, tableId)).thenReturn(table);
            Assertions.assertEquals(100L, rowCountCache.getCachedRowCount(1L, DATABASE_ID, tableId, false));

            rowCount.set(200L);
            boolean result = catalog.createTable(info);

            Assertions.assertTrue(result);
            Mockito.verify(metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
            Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
            Mockito.verify(connector).invalidateTable("DB1", "t1");
            Assertions.assertEquals(200L, rowCountCache.getCachedRowCount(1L, DATABASE_ID, tableId, false));
        }
    }

    @Test
    public void testCreateTableIfNotExistsLosingTheCreateRaceReturnsTrueWithoutEditLog() throws Exception {
        // The existence probe and the remote create are not atomic: here the probe says ABSENT and the
        // connector then fails because a concurrent creator won the race in between.
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        catalog.dbNullableResult = db;
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        catalog.dbForReplayResult = Optional.of(replayDb);
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        // absent on the pre-probe, present on the post-failure re-probe.
        Mockito.when(metadata.getTableHandle(session, "DB1", "t1"))
                .thenReturn(Optional.empty(), Optional.of(handle));
        Mockito.doThrow(new DorisConnectorException("table already exists"))
                .when(metadata).createTable(Mockito.any(), Mockito.any());
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("db1");
        Mockito.when(info.getTableName()).thenReturn("t1");
        Mockito.when(info.isIfNotExists()).thenReturn(true);

        boolean res = catalog.createTable(info);

        Assertions.assertTrue(res, "losing the create race under IF NOT EXISTS must return true");
        Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
        Mockito.verify(connector).invalidateTable("DB1", "t1");
        Mockito.verify(mockMetaCacheMgr).invalidateTable(
                1L, DATABASE_ID, "db1", Util.genIdByName("test-catalog", "db1", "t1"), "t1");
    }

    @Test
    public void testCreateTableWithoutIfNotExistsLosingTheCreateRaceStillThrows() {
        // Same race, but the user did NOT write IF NOT EXISTS: the failure must surface, never be
        // laundered into a success by the re-probe.
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        catalog.dbNullableResult = db;
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "t1"))
                .thenReturn(Optional.empty(), Optional.of(handle));
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).createTable(Mockito.any(), Mockito.any());
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("db1");
        Mockito.when(info.getTableName()).thenReturn("t1");
        Mockito.when(info.isIfNotExists()).thenReturn(false);

        DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.createTable(info));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
        Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
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
        long constraintMetadataBaseline = catalog.snapshotConstraintMetadata();

        boolean res = catalog.createTable(info);

        Assertions.assertTrue(res, "existing local table + IF NOT EXISTS must return true");
        Mockito.verify(metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
        Mockito.verify(mockMetaCacheMgr).invalidateTable(
                1L, DATABASE_ID, "db1", Util.genIdByName("test-catalog", "db1", "t1"), "t1");
        Assertions.assertNotEquals(
                constraintMetadataBaseline, catalog.snapshotConstraintMetadata());
    }

    @Test
    public void testCreateTableExistingRemoteTableWithoutIfNotExistsReportsErrno1050() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        catalog.dbNullableResult = db;
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "t1")).thenReturn(Optional.of(handle));
        CreateTableInfo info = Mockito.mock(CreateTableInfo.class);
        Mockito.when(info.getDbName()).thenReturn("db1");
        Mockito.when(info.getTableName()).thenReturn("t1");
        Mockito.when(info.isIfNotExists()).thenReturn(false);

        DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.createTable(info));
        Assertions.assertEquals(ErrorCode.ERR_TABLE_EXISTS_ERROR, ex.getMysqlErrorCode(),
                "remote-existing table without IF NOT EXISTS must surface MySQL errno 1050 (legacy parity)");
        Assertions.assertTrue(ex.getMessage().contains("already exists"));
        Mockito.verify(metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
        Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
    }

    @Test
    public void testCreateTableLocalConflictWithoutIfNotExistsRejects() throws Exception {
        // A case-folded local name may conflict even when the case-sensitive remote name is absent.
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

            DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.createTable(info));
            Assertions.assertEquals(ErrorCode.ERR_TABLE_EXISTS_ERROR, ex.getMysqlErrorCode(),
                    "local-cache conflict without IF NOT EXISTS must surface MySQL errno 1050");
            Assertions.assertTrue(ex.getMessage().contains("already exists"));
            Mockito.verify(metadata, Mockito.never()).createTable(Mockito.any(), Mockito.any());
            Mockito.verify(mockEditLog, Mockito.never()).logCreateTable(Mockito.any());
        }
    }

    // ==================== TRUNCATE TABLE ====================

    @Test
    public void testTruncateTablePersistsCanonicalLocalNames() throws Exception {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getId()).thenReturn(DATABASE_ID);
        ExternalTable table = mockAlterTable();
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(db.getFullName()).thenReturn("db1");
        Mockito.doReturn(table).when(db).getTableNullable("T1");
        catalog.dbNullableResult = db;
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));

        catalog.truncateTable("DB1", "T1", null, false, "");

        Mockito.verify(metadata).truncateTable(session, handle, null);
        ArgumentCaptor<TruncateTableInfo> log = ArgumentCaptor.forClass(TruncateTableInfo.class);
        Mockito.verify(mockEditLog).logTruncateTable(log.capture());
        Assertions.assertEquals("db1", log.getValue().getDb());
        Assertions.assertEquals("t1", log.getValue().getTable());
        Mockito.verify(mockRefreshManager).refreshTableInternal(Mockito.eq(db), Mockito.eq(table), Mockito.anyLong());
    }

    @Test
    public void testReplayTruncateUsesNormalRefreshWhenTableObjectIsCached() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        ExternalTable table = mockAlterTable();
        Mockito.doReturn(Optional.of(table)).when(db).getTableForReplay("t1");
        catalog.dbForReplayResult = Optional.of(db);
        TruncateTableInfo info = new TruncateTableInfo("test-catalog", "db1", "t1", null, 123L);

        catalog.replayTruncateTable(info);

        Mockito.verify(mockRefreshManager).refreshTableInternal(db, table, 123L);
        Mockito.verifyNoInteractions(mockMetaCacheMgr);
    }

    @Test
    public void testReplayTruncateInvalidatesByCanonicalNameWhenTableObjectIsCold() {
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        Mockito.doReturn(Optional.empty()).when(db).getTableForReplay("t1");
        catalog.dbForReplayResult = Optional.of(db);

        catalog.replayTruncateTable(new TruncateTableInfo("test-catalog", "db1", "t1", null, 123L));

        InOrder order = Mockito.inOrder(connector, mockMetaCacheMgr);
        order.verify(connector).invalidateDb("DB1");
        order.verify(mockMetaCacheMgr).invalidateTable(
                1L, Util.genIdByName("test-catalog", "db1"), "db1",
                Util.genIdByName("test-catalog", "db1", "t1"), "t1");
    }

    @Test
    public void testReplayTruncateInvalidatesAllConnectorCachesWhenDatabaseObjectIsCold() {
        catalog.dbForReplayResult = Optional.empty();

        catalog.replayTruncateTable(new TruncateTableInfo("test-catalog", "db1", "t1", null, 123L));

        InOrder order = Mockito.inOrder(connector, mockMetaCacheMgr);
        order.verify(connector).invalidateAll();
        order.verify(mockMetaCacheMgr).invalidateTable(
                1L, Util.genIdByName("test-catalog", "db1"), "db1",
                Util.genIdByName("test-catalog", "db1", "t1"), "t1");
    }

    // ==================== EDIT-LOG REPLAY (follower / observer propagation) ====================
    @Test
    public void testReplayDropTableInvalidatesConnectorOnFollower() {
        // local db1.t1 maps to remote DB1.TBL1; the table is still in the replay cache when the drop replays.
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        Mockito.when(replayDb.getRemoteName()).thenReturn("DB1");
        ExternalTable cached = Mockito.mock(ExternalTable.class);
        Mockito.when(cached.getRemoteName()).thenReturn("TBL1");
        Mockito.doReturn(Optional.of(cached)).when(replayDb).getTableForReplay("t1");
        catalog.dbForReplayResult = Optional.of(replayDb);

        catalog.replayDropTable("db1", "t1");

        Mockito.verify(connector).invalidateTable("DB1", "TBL1");
        Mockito.verify(replayDb).unregisterTable("t1");
    }

    @Test
    public void testReplayDropTableDoesNotInitializeColdDatabase() {
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        Mockito.when(replayDb.isInitialized()).thenReturn(false);
        Mockito.doReturn(Optional.empty()).when(replayDb).getTableForReplay("t1");
        catalog.dbForReplayResult = Optional.of(replayDb);

        catalog.replayDropTable("db1", "t1");

        Mockito.verify(replayDb, Mockito.never()).unregisterTable(Mockito.anyString());
        verifyDeterministicTableInvalidation("db1", "t1");
    }

    @Test
    public void testReplayDropTableInvalidatesDatabaseConnectorCacheWhenTableObjectIsCold() {
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        Mockito.when(replayDb.getRemoteName()).thenReturn("DB1");
        Mockito.doReturn(Optional.empty()).when(replayDb).getTableForReplay("t1");
        catalog.dbForReplayResult = Optional.of(replayDb);

        catalog.replayDropTable("db1", "t1");

        Mockito.verify(connector).invalidateDb("DB1");
        Mockito.verify(replayDb).unregisterTable("t1");
    }

    @Test
    public void testReplayDropTableInvalidatesAllConnectorCachesWhenDatabaseObjectIsCold() {
        catalog.dbForReplayResult = Optional.empty();

        catalog.replayDropTable("db1", "t1");

        Mockito.verify(connector).invalidateAll();
        verifyDeterministicTableInvalidation("db1", "t1");
    }

    @Test
    public void testReplayDropTableUninitializedCatalogSkipsInvalidate() {
        // Replay must not initialize a catalog solely to invalidate connector state.
        catalog.setInitializedForTest(false);
        catalog.dbForReplayResult = Optional.empty();

        catalog.replayDropTable("db1", "t1");

        Mockito.verifyNoInteractions(connector);
        verifyDeterministicTableInvalidation("db1", "t1");
    }

    @Test
    public void testReplayDropDbInvalidatesConnectorOnFollower() {
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        Mockito.when(replayDb.getRemoteName()).thenReturn("DB1");
        catalog.dbForReplayResult = Optional.of(replayDb);

        catalog.replayDropDb("db1");

        Mockito.verify(connector).invalidateDb("DB1");
        Assertions.assertEquals("db1", catalog.unregisteredDb);
        verifyDeterministicDatabaseInvalidation("db1");
    }

    @Test
    public void testReplayDropDbInvalidatesAllWhenDatabaseObjectIsCold() {
        catalog.dbForReplayResult = Optional.empty();

        catalog.replayDropDb("db1");

        Mockito.verify(connector).invalidateAll();
        Assertions.assertEquals("db1", catalog.unregisteredDb);
        verifyDeterministicDatabaseInvalidation("db1");
    }

    @Test
    public void testReplayCreateTableInvalidatesConnectorOnFollower() {
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        Mockito.when(replayDb.getRemoteName()).thenReturn("DB1");
        catalog.dbForReplayResult = Optional.of(replayDb);

        catalog.replayCreateTable("db1", "t1");

        Mockito.verify(connector).invalidateDb("DB1");
        verifyDeterministicTableInvalidation("db1", "t1");
    }

    @Test
    public void testReplayCreateTableInvalidatesAllWhenDatabaseObjectIsCold() {
        catalog.dbForReplayResult = Optional.empty();

        catalog.replayCreateTable("db1", "t1");

        Mockito.verify(connector).invalidateAll();
        verifyDeterministicTableInvalidation("db1", "t1");
    }

    @Test
    public void testReplayCreateDbInvalidatesDatabaseCaches() {
        catalog.replayCreateDb("db1");

        Assertions.assertEquals(1, catalog.resetMetaCacheNamesCount);
        Mockito.verify(connector).invalidateAll();
        verifyDeterministicDatabaseInvalidation("db1");
    }

    // ==================== COLUMN EVOLUTION ====================

    @Test
    public void testAddColumnRoutesConvertsAndLogsRefresh() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        long constraintMetadataBaseline = catalog.snapshotConstraintMetadata();

        catalog.addColumn(table, nullableIntColumn("age"), ColumnPosition.FIRST);

        ArgumentCaptor<ConnectorColumn> colCap = ArgumentCaptor.forClass(ConnectorColumn.class);
        ArgumentCaptor<ConnectorColumnPosition> posCap = ArgumentCaptor.forClass(ConnectorColumnPosition.class);
        Mockito.verify(metadata).addColumn(Mockito.eq(session), Mockito.eq(handle),
                colCap.capture(), posCap.capture());
        Assertions.assertEquals("age", colCap.getValue().getName());
        Assertions.assertTrue(posCap.getValue().isFirst());
        ArgumentCaptor<ExternalObjectLog> logCap = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(mockEditLog).logRefreshExternalTable(logCap.capture());
        Assertions.assertEquals("db1", logCap.getValue().getDbName());
        Assertions.assertEquals("t1", logCap.getValue().getTableName());
        Assertions.assertNotEquals(
                constraintMetadataBaseline, catalog.snapshotConstraintMetadata());
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
        Assertions.assertNull(posCap.getValue());
    }

    @Test
    public void testColumnOpRefreshesTableCacheViaRefreshManager() throws Exception {
        ExternalTable table = mockAlterTable();
        stubAlterHandle();
        ExternalDatabase<? extends ExternalTable> replayDb = mockExternalDatabase();
        ExternalTable cached = Mockito.mock(ExternalTable.class);
        Mockito.doReturn(replayDb).when(cached).getDb();
        Mockito.doReturn(Optional.of(cached)).when(replayDb).getTableForReplay("t1");
        catalog.dbForReplayResult = Optional.of(replayDb);

        catalog.dropColumn(table, "age");

        Assertions.assertEquals("db1", catalog.lastGetDbForReplayArg);
        Mockito.verify(replayDb).getTableForReplay("t1");
        Mockito.verify(mockRefreshManager)
                .refreshTableInternal(Mockito.eq(replayDb), Mockito.eq(cached), Mockito.anyLong());
    }

    @Test
    public void testColumnOpRefreshesTransientTableWhenSharedCacheIsCold() throws Exception {
        ExternalTable transientTable = mockAlterTable();
        stubAlterHandle();

        catalog.dropColumn(transientTable, "age");

        Assertions.assertEquals("db1", catalog.lastGetDbForReplayArg);
        Mockito.verify(mockRefreshManager).refreshTableInternal(
                Mockito.eq(transientTable.getDb()), Mockito.eq(transientTable), Mockito.anyLong());
    }

    @Test
    public void testColumnOpHandleAbsentThrows() {
        ExternalTable table = mockAlterTable();
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.empty());

        Assertions.assertThrows(DdlException.class, () -> catalog.dropColumn(table, "age"));
        Mockito.verify(metadata, Mockito.never()).dropColumn(Mockito.any(), Mockito.any(), Mockito.any());
    }

    @Test
    public void testColumnOpWrapsConnectorException() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        long constraintMetadataBaseline = catalog.snapshotConstraintMetadata();
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).dropColumn(session, handle, "age");

        DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.dropColumn(table, "age"));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
        Assertions.assertThrows(DdlException.class, () -> {
            try (ExternalCatalog.ConstraintMetadataReadGuard ignored =
                    catalog.lockConstraintMetadata(constraintMetadataBaseline)) {
                Assertions.fail("stale constraint metadata snapshot must be rejected");
            }
        });
        try (ExternalCatalog.ConstraintMetadataReadGuard ignored =
                catalog.lockConstraintMetadata(catalog.snapshotConstraintMetadata())) {
            Assertions.assertNotNull(ignored);
        }
    }

    @Test
    public void testCreateOrReplaceBranchRoutesConvertsAndRefreshes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        long constraintMetadataBaseline = catalog.snapshotConstraintMetadata();

        CreateOrReplaceBranchInfo info = new CreateOrReplaceBranchInfo("b1", true, false, true,
                new BranchOptions(Optional.of(42L), Optional.of(86400000L),
                        Optional.of(5), Optional.of(172800000L)));
        catalog.createOrReplaceBranch(table, info);

        ArgumentCaptor<BranchChange> cap = ArgumentCaptor.forClass(BranchChange.class);
        Mockito.verify(metadata).createOrReplaceBranch(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        Assertions.assertEquals(
                constraintMetadataBaseline, catalog.snapshotConstraintMetadata());
        BranchChange b = cap.getValue();
        Assertions.assertEquals("b1", b.getName());
        Assertions.assertTrue(b.isCreate());
        Assertions.assertFalse(b.isReplace());
        Assertions.assertTrue(b.isIfNotExists());
        Assertions.assertEquals(42L, b.getSnapshotId().longValue());
        Assertions.assertEquals(86400000L, b.getMaxSnapshotAgeMs().longValue());
        Assertions.assertEquals(5, b.getMinSnapshotsToKeep().intValue());
        Assertions.assertEquals(172800000L, b.getMaxRefAgeMs().longValue());
        ArgumentCaptor<ExternalObjectLog> logCap = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(mockEditLog).logRefreshExternalTable(logCap.capture());
        Assertions.assertEquals("db1", logCap.getValue().getDbName());
        Assertions.assertEquals("t1", logCap.getValue().getTableName());
    }

    @Test
    public void testCreateOrReplaceBranchEmptyOptionsConvertToNulls() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

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
        Mockito.verify(mockEditLog, Mockito.never()).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testCreateOrReplaceTagRoutesConvertsAndRefreshes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

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

    @Test
    public void testAddPartitionFieldRoutesConvertsAndRefreshes() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.addPartitionField(table, new AddPartitionFieldOp("bucket", 8, "id", "id_b"));

        ArgumentCaptor<PartitionFieldChange> cap = ArgumentCaptor.forClass(PartitionFieldChange.class);
        Mockito.verify(metadata).addPartitionField(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        PartitionFieldChange c = cap.getValue();
        Assertions.assertEquals("bucket", c.getTransformName());
        Assertions.assertEquals(8, c.getTransformArg().intValue());
        Assertions.assertEquals("id", c.getColumnName());
        Assertions.assertEquals("id_b", c.getPartitionFieldName());
        Assertions.assertNull(c.getOldColumnName());
        ArgumentCaptor<ExternalObjectLog> logCap = ArgumentCaptor.forClass(ExternalObjectLog.class);
        Mockito.verify(mockEditLog).logRefreshExternalTable(logCap.capture());
        Assertions.assertEquals("db1", logCap.getValue().getDbName());
        Assertions.assertEquals("t1", logCap.getValue().getTableName());
    }

    @Test
    public void testDropPartitionFieldRoutesByName() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.dropPartitionField(table, new DropPartitionFieldOp("p_id"));

        ArgumentCaptor<PartitionFieldChange> cap = ArgumentCaptor.forClass(PartitionFieldChange.class);
        Mockito.verify(metadata).dropPartitionField(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        Assertions.assertEquals("p_id", cap.getValue().getPartitionFieldName());
        Assertions.assertNull(cap.getValue().getColumnName());
        Mockito.verify(mockEditLog).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testReplacePartitionFieldRoutesMapsOldAndNew() throws Exception {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();

        catalog.replacePartitionField(table,
                new ReplacePartitionFieldOp("p", null, null, null, "bucket", 4, "id", "p2"));

        ArgumentCaptor<PartitionFieldChange> cap = ArgumentCaptor.forClass(PartitionFieldChange.class);
        Mockito.verify(metadata).replacePartitionField(Mockito.eq(session), Mockito.eq(handle), cap.capture());
        PartitionFieldChange c = cap.getValue();
        Assertions.assertEquals("bucket", c.getTransformName());
        Assertions.assertEquals(4, c.getTransformArg().intValue());
        Assertions.assertEquals("id", c.getColumnName());
        Assertions.assertEquals("p2", c.getPartitionFieldName());
        Assertions.assertEquals("p", c.getOldPartitionFieldName());
        Mockito.verify(mockEditLog).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testPartitionFieldWrapsConnectorException() {
        ExternalTable table = mockAlterTable();
        ConnectorTableHandle handle = stubAlterHandle();
        Mockito.doThrow(new DorisConnectorException("boom"))
                .when(metadata).addPartitionField(Mockito.eq(session), Mockito.eq(handle), Mockito.any());

        DdlException ex = Assertions.assertThrows(DdlException.class, () -> catalog.addPartitionField(table,
                new AddPartitionFieldOp(null, null, "id", null)));
        Assertions.assertTrue(ex.getMessage().contains("boom"));
        Mockito.verify(mockEditLog, Mockito.never()).logRefreshExternalTable(Mockito.any());
    }

    @Test
    public void testPartitionFieldHandleAbsentThrows() {
        ExternalTable table = mockAlterTable();
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.empty());

        Assertions.assertThrows(DdlException.class,
                () -> catalog.addPartitionField(table, new AddPartitionFieldOp(null, null, "id", null)));
        Mockito.verify(metadata, Mockito.never())
                .addPartitionField(Mockito.any(), Mockito.any(), Mockito.any());
    }

    // ==================== helpers ====================

    /** A mock external table whose LOCAL names are db1.t1 and REMOTE names DB1.TBL1 (name mapping enabled). */
    private ExternalTable mockAlterTable() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getDbName()).thenReturn("db1");
        Mockito.when(table.getName()).thenReturn("t1");
        Mockito.when(table.getRemoteDbName()).thenReturn("DB1");
        Mockito.when(table.getRemoteName()).thenReturn("TBL1");
        ExternalDatabase<? extends ExternalTable> db = mockExternalDatabase();
        Mockito.doReturn(db).when(table).getDb();
        return table;
    }

    /** Stubs the connector handle resolution for the REMOTE names of {@link #mockAlterTable()}. */
    private ConnectorTableHandle stubAlterHandle() {
        ConnectorTableHandle handle = Mockito.mock(ConnectorTableHandle.class);
        Mockito.when(metadata.getTableHandle(session, "DB1", "TBL1")).thenReturn(Optional.of(handle));
        return handle;
    }

    /** A nullable INT Doris column (iceberg add/modify reject non-nullable adds). */
    private void verifyDeterministicDatabaseInvalidation(String dbName) {
        Mockito.verify(mockMetaCacheMgr).invalidateDb(
                1L, Util.genIdByName("test-catalog", dbName), dbName);
    }

    private void verifyDeterministicTableInvalidation(String dbName, String tableName) {
        Mockito.verify(mockMetaCacheMgr).invalidateTable(
                1L, Util.genIdByName("test-catalog", dbName), dbName,
                Util.genIdByName("test-catalog", dbName, tableName), tableName);
    }

    private static Column nullableIntColumn(String name) {
        return new Column(name, Type.INT, false, null, true, null, "");
    }

    @SuppressWarnings("unchecked")
    private ExternalDatabase<? extends ExternalTable> mockExternalDatabase() {
        ExternalDatabase<? extends ExternalTable> db =
                (ExternalDatabase<? extends ExternalTable>) Mockito.mock(ExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("db1");
        Mockito.when(db.getRemoteName()).thenReturn("DB1");
        Mockito.when(db.getId()).thenReturn(DATABASE_ID);
        Mockito.when(db.isInitialized()).thenReturn(true);
        Mockito.when(db.canonicalLocalTableNameFromRemote(Mockito.anyString()))
                .thenAnswer(inv -> inv.getArgument(0));
        return db;
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
        public ConnectorSession buildCrossStatementSession() {
            return buildConnectorSession();
        }

        @Override
        public ExternalDatabase<? extends ExternalTable> getDbNullable(String dbName) {
            return dbNullableResult;
        }

        @Override
        public Optional<ExternalDatabase<? extends ExternalTable>> getDbForReplay(String dbName) {
            lastGetDbForReplayArg = dbName;
            return dbForReplayResult.filter(db -> dbName.equals(db.getFullName()));
        }

        @Override
        public void resetMetaCacheNames() {
            resetMetaCacheNamesCount++;
            super.resetMetaCacheNames();
        }

        @Override
        public void unregisterDatabase(String dbName) {
            unregisteredDb = dbName;
            super.unregisterDatabase(dbName);
        }

        private static Map<String, String> testProps() {
            Map<String, String> props = new HashMap<>();
            props.put("type", "test");
            return props;
        }
    }
}
